//! Coordinator side of the consensus connection.

use std::collections::HashMap;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::time::SystemTime;

use futures_util::future::{select, Either};
use futures_util::io::AsyncWriteExt;
use heph::actor;
use heph::actor::context::ThreadSafe;
use heph::actor_ref::{ActorRef, RpcMessage, RpcResponse};
use heph::net::TcpStream;
use log::{debug, error, warn};
use serde::{Deserialize, Serialize};

use crate::buffer::{Buffer, WriteBuffer};
use crate::peer::participant::{Response, ResponseKind};
use crate::peer::{ConsensusId, Peers, PARTICIPANT_MAGIC};
use crate::storage::{BlobEntry, PAGE_SIZE};
use crate::{db, Key};

/// Message relayed to the peer the [`relay`] is connected to.
#[derive(Debug)]
pub enum RelayMessage {
    /// Add the blob with [`Key`].
    ///
    /// Returns the peer's time at which the blob was added.
    AddBlob(RpcMessage<(ConsensusId, Key), Result<SystemTime, RelayError>>),
    /// Commit to adding blob with [`Key`] at the provided timestamp.
    CommitAddBlob(RpcMessage<(ConsensusId, Key, SystemTime), Result<(), RelayError>>),
    /// Remove the blob with [`Key`].
    ///
    /// Returns the peer's time at which the blob was added.
    RemoveBlob(RpcMessage<(ConsensusId, Key), Result<SystemTime, RelayError>>),
    /*
    /// Commit to removing blob with [`Key`].
    CommitRemoveBlob(RpcMessage<(ConsensusId, Key, SystemTime), Result<(), RelayError>>),
    */
}

/// Error return to [`RelayMessage`].
#[derive(Debug)]
pub enum RelayError {
    /// Peer wants to abort the operation.
    Abort,
    /// Peer failed.
    Failed,
}

/// Macro to allow `concat!` to used in creating the `doc` attribute.
macro_rules! doc_comment {
    ($doc: expr, $( $tt: tt )*) => {
        #[doc = $doc]
        $($tt)*
    };
}

/// Macro to create stand-alone types for [`RelayMessage`] variants.
/// This is required because the most of them have the same request type
/// ([`Key`]) when using RPC.
// TODO: maybe add something like this to Heph?
macro_rules! msg_types {
    ($name: ident ($inner_type1: ty, $inner_type2: ty) -> $return_type: ty) => {
        msg_types!(struct $name, $inner_type1, $inner_type2, stringify!($name));
        msg_types!(impl $name, $name, $return_type, 0, 1);
    };
    ($name: ident ($inner_type: ty) -> $return_type: ty) => {
        msg_types!(struct $name, $inner_type, stringify!($name));
        msg_types!(impl $name, $name, $return_type, 0);
    };
    (struct $name: ident, $inner_type: ty, $doc: expr) => {
        doc_comment! {
            concat!("Message type to use with [`ActorRef::rpc`] for [`RelayMessage::", $doc, "`]."),
            #[derive(Debug, Clone)]
            pub(super) struct $name(pub $inner_type);
        }
    };
    (struct $name: ident, $inner_type1: ty, $inner_type2: ty, $doc: expr) => {
        doc_comment! {
            concat!("Message type to use with [`ActorRef::rpc`] for [`RelayMessage::", $doc, "`]."),
            #[derive(Debug, Clone)]
            pub(super) struct $name(pub $inner_type1, pub $inner_type2);
        }
    };
    (impl $name: ident, $ty: ty, $return_type: ty, $( $field: tt ),*) => {
        impl From<RpcMessage<(ConsensusId, $ty), Result<$return_type, RelayError>>> for RelayMessage {
            fn from(msg: RpcMessage<(ConsensusId, $ty), Result<$return_type, RelayError>>) -> RelayMessage {
                RelayMessage::$name(RpcMessage {
                    request: (msg.request.0, $( (msg.request.1).$field ),* ),
                    response: msg.response,
                })
            }
        }
    };
}

msg_types!(AddBlob(Key) -> SystemTime);
msg_types!(CommitAddBlob(Key, SystemTime) -> ());
msg_types!(RemoveBlob(Key) -> SystemTime);

/// Enum to collect all possible [`RpcResponse`]s from [`RelayMessage`].
#[derive(Debug)]
enum RelayResponse {
    /// Response for [`RelayMessage::AddBlob`].
    AddBlob(RpcResponse<Result<SystemTime, RelayError>>),
    /// Response for [`RelayMessage::CommitAddBlob`].
    CommitAddBlob(RpcResponse<Result<(), RelayError>>),
    /// Response for [`RelayMessage::RemoveBlob`].
    RemoveBlob(RpcResponse<Result<SystemTime, RelayError>>),
}

/// Actor that relays messages to a [`participant::dispatcher`] actor running on
/// the `remote` node.
///
/// [`participant::dispatcher`]: super::participant::dispatcher
pub async fn relay(
    mut ctx: actor::Context<RelayMessage, ThreadSafe>,
    remote: SocketAddr,
    peers: Peers,
    server: SocketAddr,
) -> io::Result<()> {
    let mut responses = HashMap::new();
    let mut req_id = 0;
    let mut buf = Buffer::new();

    let mut stream = start_participant_connection(&mut ctx, remote, &mut buf, &server).await?;

    read_known_peers(&mut ctx, &mut stream, &mut buf, &peers, server).await?;

    // FIXME: this loops for ever.
    loop {
        match select(ctx.receive_next(), buf.read_from(&mut stream)).await {
            Either::Left((msg, _)) => {
                // Received a message to relay.
                let req_id = next_id(&mut req_id);
                let wbuf = buf.split_write(MAX_REQ_SIZE).1;
                write_message(&mut stream, wbuf, &mut responses, req_id, msg).await?;
            }
            Either::Right((Ok(_), _)) => {
                // Read one or more requests from the stream.
                relay_responses(&mut responses, &mut buf)?;
            }
            // Read error.
            Either::Right((Err(err), _)) => return Err(err),
        }

        next_id(&mut req_id);
    }
}

/// Start a participant connection to `remote` address.
async fn start_participant_connection(
    ctx: &mut actor::Context<RelayMessage, ThreadSafe>,
    remote: SocketAddr,
    buf: &mut Buffer,
    server: &SocketAddr,
) -> io::Result<TcpStream> {
    debug!("connecting to participant: remote_address={}", remote);
    let mut stream = TcpStream::connect(ctx, remote)?;

    // Need space for the magic bytes and a IPv6 address (max. 45 bytes).
    let mut wbuf = buf.split_write(PARTICIPANT_MAGIC.len() + 45).1;

    // The connection magic to request the node to act as participant.
    let n = wbuf.write(PARTICIPANT_MAGIC)?;
    assert_eq!(n, PARTICIPANT_MAGIC.len());

    // The address of the `coordinator::server`.
    serde_json::to_writer(&mut wbuf, server)?;

    stream.write_all(wbuf.as_bytes()).await.map(|()| stream)
}

/// Read the known peers from the `stream`, starting a new [`relay`] actor for
/// each and adding it to `peers`.
async fn read_known_peers(
    ctx: &mut actor::Context<RelayMessage, ThreadSafe>,
    stream: &mut TcpStream,
    buf: &mut Buffer,
    peers: &Peers,
    server: SocketAddr,
) -> io::Result<()> {
    loop {
        buf.read_from(&mut *stream).await?;
        // TODO: put `Deserializer` outside the loop.
        let mut de = serde_json::Deserializer::from_slice(buf.as_bytes());
        match Vec::<SocketAddr>::deserialize(&mut de) {
            Ok(addresses) => {
                peers.extend(ctx, &addresses, server);
                return Ok(());
            }
            Err(ref err) if err.is_eof() => continue,
            Err(err) => return Err(err.into()),
        }
    }
}

fn next_id(id: &mut usize) -> usize {
    let i = *id;
    *id += 1;
    i
}

/// Writes `msg` to `stream`.
async fn write_message<'b>(
    stream: &mut TcpStream,
    mut wbuf: WriteBuffer<'b>,
    responses: &mut HashMap<usize, RelayResponse>,
    id: usize,
    msg: RelayMessage,
) -> io::Result<()> {
    let (consensus_id, kind) = match &msg {
        RelayMessage::AddBlob(RpcMessage {
            request: (consensus_id, key),
            ..
        }) => (consensus_id, RequestKind::AddBlob(key)),
        RelayMessage::CommitAddBlob(RpcMessage {
            request: (consensus_id, key, timestamp),
            ..
        }) => (consensus_id, RequestKind::CommitAddBlob(key, timestamp)),
        RelayMessage::RemoveBlob(RpcMessage {
            request: (consensus_id, key),
            ..
        }) => (consensus_id, RequestKind::RemoveBlob(key)),
    };

    let request = Request {
        id,
        consensus_id: *consensus_id,
        kind,
    };
    serde_json::to_writer(&mut wbuf, &request)?;
    stream.write_all(&wbuf.as_bytes()).await?;

    match msg {
        RelayMessage::AddBlob(RpcMessage { response, .. }) => {
            responses.insert(id, RelayResponse::AddBlob(response));
        }
        RelayMessage::CommitAddBlob(RpcMessage { response, .. }) => {
            responses.insert(id, RelayResponse::CommitAddBlob(response));
        }
        RelayMessage::RemoveBlob(RpcMessage { response, .. }) => {
            responses.insert(id, RelayResponse::RemoveBlob(response));
        }
    }
    Ok(())
}

/// Read one or more requests in the `buf`fer and relay the responses to the
/// correct actor in [`responses`].
fn relay_responses(
    responses: &mut HashMap<usize, RelayResponse>,
    buf: &mut Buffer,
) -> io::Result<()> {
    // TODO: reuse the `Deserializer`, it allocates scratch memory.
    let mut de = serde_json::Deserializer::from_slice(buf.as_bytes()).into_iter::<Response>();

    for result in de.next() {
        match result {
            Ok(response) => match responses.remove(&response.request_id) {
                Some(relay) => match relay {
                    RelayResponse::AddBlob(relay) | RelayResponse::RemoveBlob(relay) => {
                        // Convert the response into the type required for
                        // `RelayResponse`.
                        let response = match response.response {
                            // FIXME: get timestamp form peer.
                            ResponseKind::Commit => Ok(SystemTime::now()),
                            ResponseKind::Abort => Err(RelayError::Abort),
                            ResponseKind::Fail => Err(RelayError::Failed),
                        };
                        let _ = relay.respond(response);
                    }
                    RelayResponse::CommitAddBlob(relay) => {
                        // Convert the response into the type required for
                        // `RelayResponse`.
                        let response = match response.response {
                            ResponseKind::Commit => Ok(()),
                            ResponseKind::Abort => Err(RelayError::Abort),
                            ResponseKind::Fail => Err(RelayError::Failed),
                        };
                        let _ = relay.respond(response);
                    }
                },
                None => {
                    // TODO: improve logging.
                    warn!("got an unexpected response");
                    continue;
                }
            },
            Err(err) if err.is_eof() => break,
            Err(err) => return Err(err.into()),
        }
    }

    let bytes_processed = de.byte_offset();
    buf.processed(bytes_processed);
    Ok(())
}

/// Maximum size of [`Request`].
// TODO: this.
const MAX_REQ_SIZE: usize = 1000;

/// Request message send from [`relay`] to [`participant::dispatcher`].
///
/// [`participant::dispatcher`]: super::participant::dispatcher
#[derive(Debug, Serialize)]
pub struct Request<'a> {
    pub id: usize,
    pub consensus_id: ConsensusId,
    pub kind: RequestKind<'a>,
}

/// Request details.
#[derive(Debug, Serialize)]
pub enum RequestKind<'a> {
    /// Add the blob with [`Key`].
    // TODO: provide the length so we can pre-allocate a buffer at the
    // participant?
    AddBlob(&'a Key),
    /// Commit to add the blob.
    CommitAddBlob(&'a Key, &'a SystemTime),
    /// Remove the blob with [`Key`].
    RemoveBlob(&'a Key),
}

/// Length send if the blob is not found.
const NO_BLOB: [u8; 8] = 0u64.to_be_bytes();

/// Actor that serves the [`participant::consensus`] actor in retrieving
/// uncommited blobs.
///
/// Expects to read [`Key`]s (as bytes) on the `stream`, responding with
/// [`Blob`]s (length as `u64`, following by the bytes). If the returned length
/// is 0 the blob is not found (either never stored or removed).
///
/// [`participant::consensus`]: crate::peer::participant::consensus
/// [`Blob`]: crate::storage::Blob
pub async fn server(
    // Having `Response` here as message makes no sense, but its required
    // because `participant::dispatcher` has it as message. It should be `!`.
    mut ctx: actor::Context<Response, ThreadSafe>,
    mut stream: TcpStream,
    mut buf: Buffer,
    db_ref: ActorRef<db::Message>,
) -> io::Result<()> {
    loop {
        while buf.len() >= Key::LENGTH {
            let key = Key::from_bytes(&buf.as_bytes()[..Key::LENGTH]);
            match db_ref.rpc(&mut ctx, key.clone()) {
                Ok(rpc) => match rpc.await {
                    Ok(Some(BlobEntry::Stored(blob))) => {
                        if blob.len() > PAGE_SIZE {
                            // If the blob is large(-ish) we'll prefetch it from
                            // disk to improve performance.
                            // TODO: benchmark this with large(-ish) blobs.
                            let _ = blob.prefetch();
                        }

                        // TODO: use vectored I/O here.
                        let length: [u8; 8] = (blob.len() as u64).to_be_bytes();
                        stream.write_all(&length).await?;
                        stream.write_all(blob.bytes()).await?;
                    }
                    Ok(Some(BlobEntry::Removed(_))) | Ok(None) => {
                        // By writing a length of 0 we indicate the blob is not
                        // found.
                        stream.write_all(&NO_BLOB).await?;
                    }
                    Err(err) => {
                        error!("error making RPC call to database: {}", err);
                        // Forcefully close the connection to force the client to
                        // recognise the error.
                        return Ok(());
                    }
                },
                Err(err) => {
                    // See error handling above for rationale.
                    error!("error making RPC call to database: {}", err);
                    return Ok(());
                }
            }
        }

        match buf.read_from(&mut stream).await {
            Ok(0) => return Ok(()),
            Ok(_) => {}
            Err(err) => return Err(err),
        }
    }
}
