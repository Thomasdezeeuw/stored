//! Participant side of the consensus connection.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::time::SystemTime;

use futures_util::future::{select, Either};
use futures_util::io::{AsyncReadExt, AsyncWriteExt};
use heph::actor;
use heph::actor::context::ThreadSafe;
use heph::actor_ref::{ActorRef, NoResponse};
use heph::net::TcpStream;
use heph::rt::options::ActorOptions;
use heph::supervisor::SupervisorStrategy;
use log::warn;
use serde::{Deserialize, Serialize};

use crate::buffer::Buffer;
use crate::db::{self, AddBlobResponse};
use crate::peer::{coordinator, ConsensusId, Peers};
use crate::storage::AddBlob;
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
    let mut running = HashMap::new();

    // FIXME: this loops for ever.
    loop {
        match select(ctx.receive_next(), buf.read_from(&mut stream)).await {
            Either::Left((msg, _)) => {
                // Received a message.
                write_response(&mut stream, &mut buf, msg, &mut running).await?;
            }
            Either::Right((Ok(_), _)) => {
                // Read one or more requests from the stream.
                read_requests(&mut ctx, &remote, &mut buf, &db_ref, &mut running)?;
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
    _running: &mut HashMap<ConsensusId, ActorRef<ConsensusResult>>,
) -> io::Result<()> {
    // TODO: remove actor_ref from running.
    // TODO: only do this after the algorithm has completed.
    //running.remove(response.consensus_id);

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
    running: &mut HashMap<ConsensusId, ActorRef<ConsensusResult>>,
) -> io::Result<()> {
    // TODO: reuse the `Deserializer`, it allocates scratch memory.
    let mut de = serde_json::Deserializer::from_slice(buf.as_bytes()).into_iter::<Request>();

    for result in de.next() {
        match result {
            Ok(request) => {
                let (key, operation) = match request.kind {
                    RequestKind::AddBlob(key) => (key, ConsensusOperation::AddBlob),
                    RequestKind::CommitAddBlob(key, timestamp) => {
                        if let Some(actor_ref) = running.get(&request.consensus_id) {
                            // Relay the message to the correct acort.
                            let msg = ConsensusResult::Commit(key, timestamp);
                            if let Err(..) = actor_ref.send(msg) {
                                // In case we fail we send ourself a message to
                                // relay to the coordinator that the actor
                                // failed.
                                let response = Response {
                                    request_id: request.id,
                                    response: ResponseKind::Fail,
                                };
                                let _ = ctx.actor_ref().send(response);
                            }
                        }

                        continue;
                    }
                    RequestKind::RemoveBlob(key) => (key, ConsensusOperation::RemoveBlob),
                };

                let consensus_id = request.consensus_id;
                let responder = RpcResponder {
                    id: request.id,
                    actor_ref: ctx.actor_ref(),
                    phase: ConsensusPhase::init(),
                };
                let request = ConsensusRequest {
                    operation,
                    key,
                    remote: *remote,
                };

                // Start a new consensus actor to handle the request.
                let consensus = consensus as fn(_, _, _, _) -> _;
                let actor_ref = ctx.spawn(
                    |err| {
                        warn!("consensus actor failed: {}", err);
                        SupervisorStrategy::Stop
                    },
                    consensus,
                    (request, responder, db_ref.clone()),
                    ActorOptions::default(),
                );
                if let Some(actor_ref) = running.insert(consensus_id, actor_ref) {
                    warn!(
                        "received conflict consensus ids: stopping both: consensus_id={}",
                        consensus_id.0
                    );
                    let _ = actor_ref.send(ConsensusResult::Abort);
                    if let Some(actor_ref) = running.remove(&consensus_id) {
                        let _ = actor_ref.send(ConsensusResult::Abort);
                    }
                }
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
    consensus_id: ConsensusId,
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
    /// Same as [`coordinator::RequestKind::CommitAddBlob`].
    ///
    /// [`coordinator::RequestKind::CommitAddBlob`]: super::coordinator::RequestKind::CommitAddBlob
    CommitAddBlob(Key, SystemTime),
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
            consensus_id: req.consensus_id,
            kind: match req.kind {
                AddBlob(key) => RequestKind::AddBlob(key.clone()),
                CommitAddBlob(key, timestamp) => {
                    RequestKind::CommitAddBlob(key.clone(), timestamp.clone())
                }
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

/// Responder for the [`dispatcher`] that relays the message to the
/// [`coordinator::relay`].
///
/// If this is dropped without completing the consensus algorithm it will send
/// [`ResponseKind::Fail`].
///
/// [`respond`]: RpcResponder::respond
pub struct RpcResponder {
    id: usize,
    actor_ref: ActorRef<Response>,
    phase: ConsensusPhase,
}

enum ConsensusPhase {
    One,
    Two,
    Complete,
    Failed,
}

impl ConsensusPhase {
    /// Returns the initial consensus phase.
    const fn init() -> ConsensusPhase {
        ConsensusPhase::One
    }

    /// Advance to the next consensus phase.
    fn next(&mut self) {
        match self {
            ConsensusPhase::One => *self = ConsensusPhase::Two,
            ConsensusPhase::Two => *self = ConsensusPhase::Complete,
            ConsensusPhase::Complete => {
                unreachable!("can't go into the next phase after completion")
            }
            ConsensusPhase::Failed => unreachable!("can't go into the next phase after failing"),
        }
    }

    /// Mark the consensus run as failed.
    fn failed(&mut self) {
        *self = ConsensusPhase::Failed;
    }

    /// Returns `true` if the consensus run was complete.
    fn is_complete(&self) -> bool {
        matches!(self, ConsensusPhase::Failed | ConsensusPhase::Complete)
    }
}

impl RpcResponder {
    /// Respond with `response`.
    ///
    /// # Panics
    ///
    /// This panics if its called more then once or after calling it with
    /// [`ResponseKind::Abort`].
    fn respond(&mut self, response: ResponseKind) {
        if let ResponseKind::Commit = response {
            self.phase.next()
        } else {
            self.phase.failed()
        }

        let _ = self.actor_ref.send(Response {
            request_id: self.id,
            response,
        });
    }
}

impl Drop for RpcResponder {
    fn drop(&mut self) {
        if !self.phase.is_complete() {
            // If we didn't respond in a phase let the dispatcher know we failed
            // to it can be relayed to the coordinator.
            let _ = self.actor_ref.send(Response {
                request_id: self.id,
                response: ResponseKind::Fail,
            });
        }
    }
}

#[derive(Debug)]
pub struct ConsensusRequest {
    /// The blob to operate on.
    key: Key,
    /// The kind of operation the consensus is for.
    operation: ConsensusOperation,
    /// The address of the coordinator peer, e.g. used to retrieve the blob to
    /// store.
    remote: SocketAddr,
}

#[derive(Debug, Copy, Clone)]
pub enum ConsensusOperation {
    AddBlob,
    RemoveBlob,
}

/// Result of the consensus algorithm after the first phase.
pub enum ConsensusResult {
    Commit(Key, SystemTime),
    Abort,
}

/// Actor that runs a single consensus algorithm run.
pub async fn consensus(
    ctx: actor::Context<ConsensusResult, ThreadSafe>,
    request: ConsensusRequest,
    responder: RpcResponder,
    db_ref: ActorRef<db::Message>,
) -> io::Result<()> {
    match request.operation {
        ConsensusOperation::AddBlob => consensus_add_blob(ctx, request, responder, db_ref).await,
        ConsensusOperation::RemoveBlob => {
            consensus_remove_blob(ctx, request, responder, db_ref).await
        }
    }
}

/// Run the consensus algorithm for adding a blob.
async fn consensus_add_blob(
    mut ctx: actor::Context<ConsensusResult, ThreadSafe>,
    request: ConsensusRequest,
    mut responder: RpcResponder,
    mut db_ref: ActorRef<db::Message>,
) -> io::Result<()> {
    debug_assert!(matches!(request.operation, ConsensusOperation::AddBlob));

    // TODO: reuse stream and buffer.
    // TODO: stream large blob to a file directly? Preallocating disk space in
    // the data file?
    let mut stream = TcpStream::connect(&mut ctx, request.remote)?;
    let mut buf = Buffer::new();

    // TODO: add timeout.
    let blob_len = if let Some((blob_key, blob)) = read_blob(&mut stream, &mut buf).await? {
        if request.key != blob_key {
            // Something went wrong sending the key.
            responder.respond(ResponseKind::Abort);
            return Ok(());
        }
        blob.len()
    } else {
        // We can't store a key for which we can't get the bytes.
        responder.respond(ResponseKind::Abort);
        return Ok(());
    };

    // Phase one: storing the blob, readying it to be added to the database.
    let query = match add_blob_to_db(&mut ctx, &mut db_ref, buf, blob_len).await {
        Status::Continue(query) => {
            responder.respond(ResponseKind::Commit);
            query
        }
        Status::Return(response) => {
            responder.respond(response);
            return Ok(());
        }
    };

    // Phase two: commit or abort the query.
    // TODO: add timeout.
    let response = match ctx.receive_next().await {
        ConsensusResult::Commit(key, timestamp) => {
            // Check if the keys match.
            if request.key != key {
                Err(())
            } else {
                commit_blob_to_db(&mut ctx, &mut db_ref, query, timestamp).await
            }
        }
        ConsensusResult::Abort => abort_add_blob(&mut ctx, &mut db_ref, query).await,
    };
    let response = match response {
        Ok(()) => ResponseKind::Commit,
        Err(()) => ResponseKind::Abort,
    };
    responder.respond(response);
    return Ok(());
}

async fn consensus_remove_blob(
    _ctx: actor::Context<ConsensusResult, ThreadSafe>,
    request: ConsensusRequest,
    _responder: RpcResponder,
    _db_ref: ActorRef<db::Message>,
) -> io::Result<()> {
    debug_assert!(matches!(request.operation, ConsensusOperation::RemoveBlob));

    todo!("remove blob")

    /*
    match db_ref.rpc(&mut ctx, request.key) {
        Ok(rpc) => match rpc.await {
            // FIXME: commit to the query at some point.
            Ok(RemoveBlobResponse::Query(_query)) => response.respond(ResponseKind::Commit),
            Ok(RemoveBlobResponse::NotStored(..)) => response.respond(ResponseKind::Abort),
            // Storage actor failed.
            Err(NoResponse) => response.respond(ResponseKind::Abort),
        },
        // Storage actor failed.
        Err(..) => response.respond(ResponseKind::Abort),
    }
    */
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

/// Status of processing a request.
enum Status<T> {
    /// Continue processing the request.
    Continue(T),
    /// Early return in case a condition is not method, e.g. a blob is too large
    /// to store.
    Return(ResponseKind),
}

/// Add blob in `buf` to the database.
async fn add_blob_to_db(
    ctx: &mut actor::Context<ConsensusResult, ThreadSafe>,
    db_ref: &mut ActorRef<db::Message>,
    buf: Buffer,
    blob_length: usize,
) -> Status<AddBlob> {
    match db_ref.rpc(ctx, (buf, blob_length)) {
        Ok(rpc) => match rpc.await {
            Ok((AddBlobResponse::Query(query), ..)) => Status::Continue(query),
            Ok((AddBlobResponse::AlreadyStored(..), ..)) => Status::Return(ResponseKind::Abort),
            // Storage actor failed.
            Err(NoResponse) => Status::Return(ResponseKind::Abort),
        },
        // Storage actor failed.
        Err(..) => Status::Return(ResponseKind::Abort),
    }
}

/// Commit to adding the blob to the database.
async fn commit_blob_to_db(
    ctx: &mut actor::Context<ConsensusResult, ThreadSafe>,
    db_ref: &mut ActorRef<db::Message>,
    query: AddBlob,
    timestamp: SystemTime,
) -> Result<(), ()> {
    match db_ref.rpc(ctx, (query, timestamp)) {
        Ok(rpc) => match rpc.await {
            Ok(..) => Ok(()),
            // Storage actor failed.
            Err(NoResponse) => Err(()),
        },
        // Storage actor failed.
        Err(..) => Err(()),
    }
}

/// Abort adding the blob to the database.
async fn abort_add_blob(
    ctx: &mut actor::Context<ConsensusResult, ThreadSafe>,
    db_ref: &mut ActorRef<db::Message>,
    query: AddBlob,
) -> Result<(), ()> {
    match db_ref.rpc(ctx, query) {
        Ok(rpc) => match rpc.await {
            Ok(..) => Ok(()),
            // Storage actor failed.
            Err(NoResponse) => Err(()),
        },
        // Storage actor failed.
        Err(..) => Err(()),
    }
}
