//! Participant side of the consensus connection.

use std::io;
use std::net::SocketAddr;

use heph::actor;

use futures_util::future::{select, Either};
use futures_util::io::{AsyncReadExt, AsyncWriteExt};
use heph::actor::context::ThreadSafe;
use heph::actor_ref::{ActorRef, NoResponse};
use heph::net::TcpStream;
use heph::rt::options::ActorOptions;
use heph::supervisor::SupervisorStrategy;
use log::warn;
use serde::{Deserialize, Serialize};

use crate::buffer::Buffer;
use crate::db::{self, AddBlobResponse, RemoveBlobResponse};
use crate::peer::coordinator;
use crate::peer::Peers;
use crate::Key;

// TODO: better logging.

/// Actor that accepts messages from [`coordinator::relay`] over the `stream` and
/// starts a [`consensus`] actor for each run of the consensus algorithm.
///
/// [`coordinator::relay`]: super::coordinator::relay
pub async fn dispatcher(
    mut ctx: actor::Context<Response, ThreadSafe>,
    mut stream: TcpStream,
    mut buf: Buffer,
    peers: Peers,
    db_ref: ActorRef<db::Message>,
) -> io::Result<()> {
    let remote = read_server_address(&mut stream, &mut buf).await?;
    write_peers(&mut stream, &mut buf, &peers).await?;

    // FIXME: this loops for ever.
    loop {
        match select(ctx.receive_next(), buf.read_from(&mut stream)).await {
            Either::Left((msg, _)) => {
                // Received a message.
                write_response(&mut stream, &mut buf, msg).await?;
            }
            Either::Right((Ok(_), _)) => {
                // Read one or more requests from the stream.
                read_requests(&mut ctx, &remote, &mut buf, &db_ref)?;
            }
            // Read error.
            Either::Right((Err(err), _)) => return Err(err),
        }
    }
}

/// Reads the address of the `coordinator::server` from `stream`.
async fn read_server_address(stream: &mut TcpStream, buf: &mut Buffer) -> io::Result<SocketAddr> {
    loop {
        // TODO: put `Deserializer` outside the loop.
        let mut de = serde_json::Deserializer::from_slice(buf.as_bytes());
        match SocketAddr::deserialize(&mut de) {
            Ok(address) => return Ok(address),
            Err(ref err) if err.is_eof() => {}
            Err(err) => return Err(err.into()),
        }
        buf.read_from(&mut *stream).await?;
    }
}

/// Write all peer addresses in `peers` to `stream`.
async fn write_peers(stream: &mut TcpStream, buf: &mut Buffer, peers: &Peers) -> io::Result<()> {
    // 45 bytes of space per address (max size of a IPv6 address) + 2 for the
    // quotes (JSON string) and another 2 for the slice.
    let mut wbuf = buf.split_write(peers.len() * (45 + 2) + 2).1;
    serde_json::to_writer(&mut wbuf, &peers.addresses())?;
    stream.write_all(wbuf.as_bytes()).await
}

/// Writes `response` to `stream`.
async fn write_response<'b>(
    stream: &mut TcpStream,
    buf: &mut Buffer,
    response: Response,
) -> io::Result<()> {
    let mut wbuf = buf.split_write(MAX_RES_SIZE).1;
    serde_json::to_writer(&mut wbuf, &response)?;
    stream.write_all(&wbuf.as_bytes()).await
}

/// Read one or more requests in the `buf`fer and start a [`consensus`] actor
/// for each.
fn read_requests(
    ctx: &mut actor::Context<Response, ThreadSafe>,
    remote: &SocketAddr,
    buf: &mut Buffer,
    db_ref: &ActorRef<db::Message>,
) -> io::Result<()> {
    // TODO: reuse the `Deserializer`, it allocates scratch memory.
    let mut de = serde_json::Deserializer::from_slice(buf.as_bytes()).into_iter::<Request>();

    for result in de.next() {
        match result {
            Ok(request) => {
                let response = RpcResponse {
                    id: request.id,
                    actor_ref: Some(ctx.actor_ref()),
                };

                // Start a new consensus actor to handle the request.
                let consensus = consensus as fn(_, _, _, _, _) -> _;
                ctx.spawn(
                    |err| {
                        warn!("consensus actor failed: {}", err);
                        SupervisorStrategy::Stop
                    },
                    consensus,
                    (*remote, request.kind, response, db_ref.clone()),
                    ActorOptions::default(),
                );
            }
            Err(err) if err.is_eof() => break,
            Err(err) => {
                // TODO: return an error here to the coordinator in case of an
                // syntax error.
                return Err(err.into());
            }
        }
    }

    let bytes_processed = de.byte_offset();
    buf.processed(bytes_processed);
    Ok(())
}

/// Owned version of [`coordinator::Request`].
///
/// [`coordinator::Request`]: super::coordinator::Request
#[derive(Debug, Deserialize)]
pub struct Request {
    id: usize,
    kind: RequestKind,
}

/// Owned version of [`coordinator::RequestKind`].
///
/// [`coordinator::RequestKind`]: super::coordinator::RequestKind
#[derive(Debug, Deserialize)]
pub enum RequestKind {
    /// Same as [`coordinator::RequestKind::AddBlob`].
    ///
    /// [`coordinator::RequestKind::AddBlob`]: super::coordinator::RequestKind::AddBlob
    AddBlob(Key),
    /// Same as [`coordinator::RequestKind::RemoveBlob`].
    ///
    /// [`coordinator::RequestKind::RemoveBlob`]: super::coordinator::RequestKind::RemoveBlob
    RemoveBlob(Key),
}

// This is here to ensure the types don't diverge.
impl From<coordinator::Request<'_>> for Request {
    fn from(req: coordinator::Request<'_>) -> Request {
        use coordinator::RequestKind::*;
        Request {
            id: req.id,
            kind: match req.kind {
                AddBlob(key) => RequestKind::AddBlob(key.clone()),
                RemoveBlob(key) => RequestKind::RemoveBlob(key.clone()),
            },
        }
    }
}

/// Maximum size of [`Response`].
// TODO: this.
const MAX_RES_SIZE: usize = 1000;

/// Response message send from [`dispatcher`] to [`coordinator::relay`].
///
/// [`coordinator::relay`]: super::coordinator::relay
#[derive(Debug, Deserialize, Serialize)]
pub struct Response {
    pub request_id: usize,
    pub response: ResponseKind,
}

/// Response details.
#[derive(Debug, Deserialize, Serialize)]
pub enum ResponseKind {
    /// Vote to commit to the consensus algorithm.
    Commit,
    /// Vote to abort the consensus algorithm.
    Abort,
    /// Something when wrong: fail the consensus algorithm.
    Fail,
}

/// Response for the [`dispatcher`] that get relay to the [`coordinator::relay`].
///
/// If this is dropped with calling [`respond`] it will send
/// [`ResponseKind::Fail`].
///
/// [`respond`]: RpcResponse::respond
pub struct RpcResponse {
    id: usize,
    actor_ref: Option<ActorRef<Response>>,
}

impl RpcResponse {
    /// Respond with `response`.
    fn respond(mut self, response: ResponseKind) {
        if let Some(actor_ref) = self.actor_ref.take() {
            let _ = actor_ref.send(Response {
                request_id: self.id,
                response,
            });
        }
    }
}

impl Drop for RpcResponse {
    fn drop(&mut self) {
        if let Some(actor_ref) = self.actor_ref.take() {
            // Let the dispatcher know we failed to it can be relayed to the
            // coordinator.
            let _ = actor_ref.send(Response {
                request_id: self.id,
                response: ResponseKind::Fail,
            });
        }
    }
}

/// Actor that runs a single consensus algorithm run.
pub async fn consensus(
    mut ctx: actor::Context<!, ThreadSafe>,
    remote: SocketAddr,
    request: RequestKind,
    response: RpcResponse,
    db_ref: ActorRef<db::Message>,
) -> io::Result<()> {
    match request {
        RequestKind::AddBlob(key) => {
            // TODO: reuse stream and buffer.
            let mut stream = TcpStream::connect(&mut ctx, remote)?;
            let mut buf = Buffer::new();

            let blob_len = if let Some((blob_key, blob)) = read_blob(&mut stream, &mut buf).await? {
                if key != blob_key {
                    // Something went wrong sending the key.
                    response.respond(ResponseKind::Fail);
                    return Ok(());
                }
                blob.len()
            } else {
                // We can't store a key for which we can't get the bytes.
                response.respond(ResponseKind::Abort);
                return Ok(());
            };

            match db_ref.rpc(&mut ctx, (buf, blob_len)) {
                Ok(rpc) => match rpc.await {
                    // FIXME: commit to the query at some point.
                    Ok((AddBlobResponse::Query(_query), ..)) => {
                        response.respond(ResponseKind::Commit)
                    }
                    Ok((AddBlobResponse::AlreadyStored(..), ..)) => {
                        response.respond(ResponseKind::Abort)
                    }
                    // Storage actor failed.
                    Err(NoResponse) => response.respond(ResponseKind::Fail),
                },
                // Storage actor failed.
                Err(..) => response.respond(ResponseKind::Fail),
            }
        }
        RequestKind::RemoveBlob(key) => match db_ref.rpc(&mut ctx, key) {
            Ok(rpc) => match rpc.await {
                // FIXME: commit to the query at some point.
                Ok(RemoveBlobResponse::Query(_query)) => response.respond(ResponseKind::Commit),
                Ok(RemoveBlobResponse::NotStored(..)) => response.respond(ResponseKind::Abort),
                // Storage actor failed.
                Err(NoResponse) => response.respond(ResponseKind::Fail),
            },
            // Storage actor failed.
            Err(..) => response.respond(ResponseKind::Fail),
        },
    }

    Ok(())
}

/// Read a blob (as returned by the [`coordinator::server`]) from `stream`.
async fn read_blob<'b>(
    stream: &mut TcpStream,
    buf: &'b mut Buffer,
) -> io::Result<Option<(Key, &'b [u8])>> {
    // TODO: this is wasteful, a read system call for 8 bytes?!
    let mut length_buf = [0; 8];
    stream.read(&mut length_buf[..]).await?;
    let blob_length = u64::from_be_bytes(length_buf);
    if blob_length == 0 {
        return Ok(None);
    }

    let mut calc = Key::calculator(stream);
    // TODO: put max on this.
    buf.reserve_atleast(blob_length as usize);
    let mut read_bytes: usize = 0;
    while read_bytes < blob_length as usize {
        read_bytes += buf.read_from(&mut calc).await?;
    }
    Ok(Some((
        calc.finish(),
        &buf.as_bytes()[..blob_length as usize],
    )))
}
