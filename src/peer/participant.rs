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
use log::{debug, error, trace, warn};
use serde::{Deserialize, Serialize};

use crate::db::{self, AddBlobResponse};
use crate::error::Describe;
use crate::peer::{coordinator, ConsensusId, Peers, COORDINATOR_MAGIC};
use crate::storage::StoreBlob;
use crate::{Buffer, Key};

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
    server: SocketAddr,
) -> crate::Result<()> {
    debug!("starting participant dispatcher: server_address={}", server);

    // Read the address at which the peer is listening for peer connections.
    let remote = read_server_address(&mut stream, &mut buf).await?;

    // Add it to the list of known peers.
    peers.spawn(&mut ctx, remote, server);

    write_peers(&mut stream, &mut buf, &peers).await?;
    let mut running = HashMap::new();

    loop {
        match select(ctx.receive_next(), buf.read_from(&mut stream)).await {
            Either::Left((msg, _)) => {
                debug!("participant dispatcher received a message: {:?}", msg);
                // Received a message.
                write_response(&mut stream, &mut buf, msg, &mut running).await?;
            }
            Either::Right((Ok(0), _)) => {
                debug!("peer coordinator closed connection");
                while let Some(msg) = ctx.try_receive_next() {
                    // Write any response left in our mailbox.
                    // TODO: ignore errors here since the other side is already
                    // disconnected?
                    debug!("participant dispatcher received a message: {:?}", msg);
                    write_response(&mut stream, &mut buf, msg, &mut running).await?;
                }
                return Ok(());
            }
            Either::Right((Ok(_), _)) => {
                // Read one or more requests from the stream.
                read_requests(&mut ctx, &remote, &mut buf, &db_ref, &mut running)?;
            }
            // Read error.
            Either::Right((Err(err), _)) => return Err(err.describe("reading from socket")),
        }
    }
}

/// Reads the address of the `coordinator::server` from `stream`.
async fn read_server_address(
    stream: &mut TcpStream,
    buf: &mut Buffer,
) -> crate::Result<SocketAddr> {
    trace!("participant dispatch reading peer's server address");
    loop {
        // TODO: put `Deserializer` outside the loop.
        // We use the `StreamDeserializer` here because we need the
        // `byte_offset` below.
        let mut iter = serde_json::Deserializer::from_slice(buf.as_bytes()).into_iter();
        match iter.next() {
            Some(Ok(address)) => {
                let bytes_processed = iter.byte_offset();
                buf.processed(bytes_processed);
                return Ok(address);
            }
            Some(Err(ref err)) if err.is_eof() => {}
            Some(Err(err)) => {
                return Err(io::Error::from(err).describe("deserializing peer's server address"))
            }
            None => {} // Continue reading more.
        }
        let n = buf
            .read_from(&mut *stream)
            .await
            .map_err(|err| err.describe("reading peer's server address"))?;
        if n == 0 {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof)
                .describe("reading peer's server address"));
        }
    }
}

/// Write all peer addresses in `peers` to `stream`.
async fn write_peers(stream: &mut TcpStream, buf: &mut Buffer, peers: &Peers) -> crate::Result<()> {
    trace!("participant dispatch writing all known peers to connection");
    // 45 bytes of space per address (max size of a IPv6 address) + 2 for the
    // quotes (JSON string) and another 2 for the slice.
    let mut wbuf = buf.split_write(peers.len() * (45 + 2) + 2).1;
    serde_json::to_writer(&mut wbuf, &peers.addresses())
        .map_err(|err| io::Error::from(err).describe("serializing peers"))?;

    stream
        .write_all(wbuf.as_bytes())
        .await
        .map_err(|err| err.describe("writing known peers to socket"))
}

/// Writes `response` to `stream`.
async fn write_response<'b>(
    stream: &mut TcpStream,
    buf: &mut Buffer,
    response: Response,
    _running: &mut HashMap<ConsensusId, ActorRef<ConsensusResultRequest>>,
) -> crate::Result<()> {
    trace!("participant dispatch writing response: {:?}", response);
    // TODO: remove actor_ref from running.
    // TODO: only do this after the algorithm has completed.
    //running.remove(response.consensus_id);

    let mut wbuf = buf.split_write(MAX_RES_SIZE).1;
    serde_json::to_writer(&mut wbuf, &response)
        .map_err(|err| io::Error::from(err).describe("serializing response"))?;

    stream
        .write_all(&wbuf.as_bytes())
        .await
        .map_err(|err| err.describe("writing response to socket"))
}

/// Read one or more requests in the `buf`fer and start a [`consensus`] actor
/// for each.
fn read_requests(
    ctx: &mut actor::Context<Response, ThreadSafe>,
    remote: &SocketAddr,
    buf: &mut Buffer,
    db_ref: &ActorRef<db::Message>,
    running: &mut HashMap<ConsensusId, ActorRef<ConsensusResultRequest>>,
) -> crate::Result<()> {
    // TODO: reuse the `Deserializer`, it allocates scratch memory.
    let mut de = serde_json::Deserializer::from_slice(buf.as_bytes()).into_iter::<Request>();

    while let Some(result) = de.next() {
        match result {
            Ok(request) => {
                debug!("participant dispatcher received a request: {:?}", request);

                let (key, operation) = match request.kind {
                    RequestKind::AddBlob(key) => (key, ConsensusOperation::StoreBlob),
                    RequestKind::CommitStoreBlob(key, timestamp) => {
                        if let Some(actor_ref) = running.get(&request.consensus_id) {
                            // Relay the message to the correct actor.
                            let msg = ConsensusResultRequest {
                                id: request.id,
                                result: ConsensusResult::Commit(key, timestamp),
                            };
                            if let Err(..) = actor_ref.send(msg) {
                                // In case we fail we send ourself a message to
                                // relay to the coordinator that the actor
                                // failed.
                                let response = Response {
                                    request_id: request.id,
                                    response: ResponseKind::Fail,
                                };
                                // We can always send ourselves a message.
                                ctx.actor_ref().send(response).unwrap();
                            }
                        }

                        continue;
                    }
                    RequestKind::RemoveBlob(key) => (key, ConsensusOperation::RemoveBlob),
                };

                let consensus_id = request.consensus_id;

                if let Some(actor_ref) = running.remove(&consensus_id) {
                    warn!(
                        "received conflict consensus ids: stopping both: consensus_id={}",
                        consensus_id.0
                    );
                    let msg = ConsensusResultRequest {
                        id: request.id,
                        result: ConsensusResult::Abort,
                    };
                    if let Err(err) = actor_ref.send(msg.clone()) {
                        warn!("failed to send consensus result to actor: {}", err);
                    }
                    continue;
                }

                // Start a new consensus actor to handle the request.
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
                debug!(
                    "participant dispatcher starting consensus actor: request={:?}, response={:?}",
                    request, responder
                );
                let consensus = consensus as fn(_, _, _, _) -> _;
                let actor_ref = ctx.spawn(
                    |err| {
                        warn!("consensus actor failed: {}", err);
                        SupervisorStrategy::Stop
                    },
                    consensus,
                    (request, responder, db_ref.clone()),
                    ActorOptions::default().mark_ready(),
                );
                // Checked above that we don't have duplicates.
                let _ = running.insert(consensus_id, actor_ref);
            }
            Err(err) if err.is_eof() => break,
            Err(err) => {
                // TODO: return an error here to the coordinator in case of an
                // syntax error.
                return Err(io::Error::from(err).describe("deserializing peer request"));
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
    /// Same as [`coordinator::RequestKind::CommitStoreBlob`].
    ///
    /// [`coordinator::RequestKind::CommitStoreBlob`]: super::coordinator::RequestKind::CommitStoreBlob
    CommitStoreBlob(Key, SystemTime),
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
                CommitStoreBlob(key, timestamp) => {
                    RequestKind::CommitStoreBlob(key.clone(), *timestamp)
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
#[derive(Debug)]
pub struct RpcResponder {
    id: usize,
    actor_ref: ActorRef<Response>,
    phase: ConsensusPhase,
}

#[derive(Debug)]
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
    pub fn respond(&mut self, response: ResponseKind) {
        if let ResponseKind::Commit = response {
            self.phase.next()
        } else {
            self.phase.failed()
        }

        let response = Response {
            request_id: self.id,
            response,
        };
        self.send_response(response);
    }

    /// Send `response`.
    fn send_response(&mut self, response: Response) {
        trace!(
            "responding to participant dispatcher: response={:?}",
            response
        );
        if let Err(err) = self.actor_ref.send(response) {
            warn!("failed to respond to the peer dispatcher: {}", err);
        }
    }
}

impl Drop for RpcResponder {
    fn drop(&mut self) {
        if !self.phase.is_complete() {
            // If we didn't respond in a phase let the dispatcher know we failed
            // to it can be relayed to the coordinator.
            let response = Response {
                request_id: self.id,
                response: ResponseKind::Fail,
            };
            self.send_response(response);
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
    StoreBlob,
    RemoveBlob,
}

#[derive(Debug, Clone)]
pub struct ConsensusResultRequest {
    /// This is the [`Request.id`], needed to update [`RpcResponder.id`] to
    /// ensure the response is send in response to the correct request so the
    /// coordinator can process it.
    id: usize,
    result: ConsensusResult,
}

/// Result of the consensus algorithm after the first phase.
#[derive(Debug, Clone)]
pub enum ConsensusResult {
    Commit(Key, SystemTime),
    Abort,
}

/// Actor that runs a single consensus algorithm run.
pub async fn consensus(
    ctx: actor::Context<ConsensusResultRequest, ThreadSafe>,
    request: ConsensusRequest,
    responder: RpcResponder,
    db_ref: ActorRef<db::Message>,
) -> crate::Result<()> {
    debug!("consensus actor started: request={:?}", request);

    match request.operation {
        ConsensusOperation::StoreBlob => {
            consensus_store_blob(ctx, request, responder, db_ref).await
        }
        ConsensusOperation::RemoveBlob => {
            consensus_remove_blob(ctx, request, responder, db_ref).await
        }
    }
}

/// Run the consensus algorithm for storing a blob.
async fn consensus_store_blob(
    mut ctx: actor::Context<ConsensusResultRequest, ThreadSafe>,
    request: ConsensusRequest,
    mut responder: RpcResponder,
    mut db_ref: ActorRef<db::Message>,
) -> crate::Result<()> {
    debug_assert!(matches!(request.operation, ConsensusOperation::StoreBlob));

    // TODO: reuse stream and buffer.
    // TODO: stream large blob to a file directly? Preallocating disk space in
    // the data file?
    debug!(
        "connecting to coordinator server: remote_address={}",
        request.remote
    );
    let mut stream = TcpStream::connect(&mut ctx, request.remote)
        .map_err(|err| err.describe("creating connect to peer server"))?;
    let mut buf = Buffer::new();

    // TODO: use write_vectored_all.
    trace!("writing connection magic");
    stream
        .write_all(COORDINATOR_MAGIC)
        .await
        .map_err(|err| err.describe("writing magic string"))?;
    trace!(
        "requesting key from coordinator server: key={}",
        request.key
    );
    stream
        .write_all(request.key.as_bytes())
        .await
        .map_err(|err| err.describe("writing request key"))?;

    // TODO: add timeout.
    let blob_len = if let Some((blob_key, blob)) = read_blob(&mut stream, &mut buf).await? {
        if request.key != blob_key {
            // Something went wrong sending the key.
            error!(
                "coordinator server responded with incorrect blob: want_key={}, got_blob_key={}",
                request.key, blob_key
            );
            responder.respond(ResponseKind::Abort);
            return Ok(());
        }
        blob.len()
    } else {
        // We can't store a key for which we can't get the bytes.
        error!(
            "couldn't get blob, voting to abort consensus: key={}",
            request.key
        );
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
    let result_request = ctx.receive_next().await;
    // Update the `RpcResponder.id` to ensure we're responding to the correct
    // request.
    responder.id = result_request.id;
    let response = match result_request.result {
        ConsensusResult::Commit(key, timestamp) => {
            // Check if the keys match.
            if request.key != key {
                error!(
                    "consensus tried to commit with incorrect key: want_key={}, consensus_key={}",
                    request.key, key
                );
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
    Ok(())
}

async fn consensus_remove_blob(
    _ctx: actor::Context<ConsensusResultRequest, ThreadSafe>,
    request: ConsensusRequest,
    _responder: RpcResponder,
    _db_ref: ActorRef<db::Message>,
) -> crate::Result<()> {
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
) -> crate::Result<Option<(Key, &'b [u8])>> {
    trace!("reading blob from coordinator server");
    // TODO: this is wasteful, a read system call for 8 bytes?!
    // NOTE: doing this because of the `KeyCalculator` below.
    let mut length_buf = [0; 8];
    let mut read_bytes: usize = 0;
    while read_bytes < length_buf.len() {
        let n = stream
            .read(&mut length_buf[..])
            .await
            .map_err(|err| err.describe("reading blob length"))?;
        if n == 0 {
            debug!("coordinator server disconnected connection to consensus actor");
            // TODO: handle this better?
            return Ok(None);
        }
        read_bytes += n;
    }
    let blob_length = u64::from_be_bytes(length_buf);
    trace!(
        "read blob length from coordinator server: length={}",
        blob_length
    );
    if blob_length == 0 {
        return Ok(None);
    }

    let mut calc = Key::calculator(stream);
    // TODO: put a maximum on this.
    buf.reserve_atleast(blob_length as usize);
    let mut read_bytes: usize = 0;
    while read_bytes < blob_length as usize {
        let n = buf
            .read_from(&mut calc)
            .await
            .map_err(|err| err.describe("reading blob"))?;
        if n == 0 {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof).describe("reading blob"));
        }
        read_bytes += n;
    }
    let key = calc.finish();
    trace!(
        "read blob from coordinator server: key={}, length={}",
        key,
        blob_length
    );
    Ok(Some((key, &buf.as_bytes()[..blob_length as usize])))
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
    ctx: &mut actor::Context<ConsensusResultRequest, ThreadSafe>,
    db_ref: &mut ActorRef<db::Message>,
    buf: Buffer,
    blob_length: usize,
) -> Status<StoreBlob> {
    trace!("consensus actor adding blob: length={}", blob_length);
    let view = buf.view(blob_length);
    match db_ref.rpc(ctx, view) {
        Ok(rpc) => match rpc.await {
            Ok((AddBlobResponse::Query(query), ..)) => Status::Continue(query),
            // FIXME: make a distinction here in already stored?
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
    ctx: &mut actor::Context<ConsensusResultRequest, ThreadSafe>,
    db_ref: &mut ActorRef<db::Message>,
    query: StoreBlob,
    timestamp: SystemTime,
) -> Result<(), ()> {
    trace!(
        "consensus actor committing to adding blob: query={:?}",
        query
    );
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
    ctx: &mut actor::Context<ConsensusResultRequest, ThreadSafe>,
    db_ref: &mut ActorRef<db::Message>,
    query: StoreBlob,
) -> Result<(), ()> {
    trace!("consensus actor aborting adding blob: query={:?}", query);
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
