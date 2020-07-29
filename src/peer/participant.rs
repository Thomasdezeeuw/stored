//! Participant side of the consensus connection.

use heph::ActorRef;
use log::{debug, warn};

use crate::peer::{ConsensusVote, Response};

/// Responder for the [`dispatcher`] that relays the message to the
/// [`coordinator::relay`].
///
/// If this is dropped without completing the consensus algorithm it will send
/// [`ConsensusVote::Fail`].
///
///[`coordinator::relay`]: crate::peer::coordinator::relay
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
    /// [`ConsensusVote::Abort`].
    pub fn respond(&mut self, vote: ConsensusVote) {
        if let ConsensusVote::Commit(..) = vote {
            self.phase.next()
        } else {
            self.phase.failed()
        }

        self.send_response(Response {
            request_id: self.id,
            vote,
        });
    }

    /// Send `response` to this dispatcher.
    fn send_response(&mut self, response: Response) {
        debug!("responding to dispatcher: response={:?}", response);
        if let Err(err) = self.actor_ref.send(response) {
            warn!("failed to respond to the dispatcher: {}", err);
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
                vote: ConsensusVote::Fail,
            };
            self.send_response(response);
        }
    }
}

pub mod dispatcher {
    //! Module with the [dispatcher actor].
    //!
    //! [dispatcher actor]: actor()

    use std::collections::HashMap;
    use std::io;
    use std::net::SocketAddr;
    use std::time::Duration;

    use futures_util::future::{select, Either};
    use futures_util::io::AsyncWriteExt;
    use fxhash::FxBuildHasher;
    use heph::actor::context::ThreadSafe;
    use heph::net::TcpStream;
    use heph::rt::options::ActorOptions;
    use heph::supervisor::SupervisorStrategy;
    use heph::timer::Deadline;
    use heph::{actor, ActorRef};
    use log::{debug, trace, warn};

    use crate::error::Describe;
    use crate::peer::participant::consensus::{self, VoteResult};
    use crate::peer::participant::{ConsensusPhase, RpcResponder};
    use crate::peer::{
        ConsensusId, ConsensusVote, Operation, Peers, Request, Response, EXIT_COORDINATOR,
        EXIT_PARTICIPANT, PARTICIPANT_CONSENSUS_ID,
    };
    use crate::{db, Buffer};

    /// Timeout used for I/O between peers.
    const IO_TIMEOUT: Duration = Duration::from_secs(2);

    /// An estimate of the largest size of an [`Response`] in bytes.
    const MAX_RES_SIZE: usize = 100;

    /// Function to change the log target and module for the warning message,
    /// the [`peer::switcher`] has little to do with the error.
    pub(crate) async fn run_actor(
        ctx: actor::Context<Response, ThreadSafe>,
        stream: TcpStream,
        buf: Buffer,
        peers: Peers,
        db_ref: ActorRef<db::Message>,
        server: SocketAddr,
        remote: SocketAddr,
    ) {
        if let Err(err) = actor(ctx, stream, buf, peers, db_ref, server).await {
            warn!(
                "participant dispatcher failed: {}: remote_address={}, server_address={}",
                err, remote, server
            );
        }
    }

    /// Actor that accepts messages from [`coordinator::relay`] over the
    /// `stream` and starts a [`consensus`] actor for each run of the consensus
    /// algorithm.
    ///
    /// [`coordinator::relay`]: crate::peer::coordinator::relay
    /// [`consensus`]: super::consensus::actor()
    pub async fn actor(
        mut ctx: actor::Context<Response, ThreadSafe>,
        mut stream: TcpStream,
        mut buf: Buffer,
        peers: Peers,
        db_ref: ActorRef<db::Message>,
        server: SocketAddr,
    ) -> crate::Result<()> {
        debug!("starting participant dispatcher: server_address={}", server);

        // Read the address at which the peer is listening for peer connections.
        let remote = read_server_address(&mut ctx, &mut stream, &mut buf).await?;
        // Add it to the list of known peers.
        peers.spawn(&mut ctx, remote, server);

        write_peers(&mut ctx, &mut stream, &mut buf, &peers).await?;
        let mut running = HashMap::with_hasher(FxBuildHasher::default());

        // TODO: close connection cleanly, sending `EXIT_PARTICIPANT`.

        loop {
            match select(ctx.receive_next(), buf.read_from(&mut stream)).await {
                Either::Left((msg, _)) => {
                    debug!("participant dispatcher received a message: {:?}", msg);
                    write_response(&mut ctx, &mut stream, &mut buf, msg).await?;
                }
                Either::Right((Ok(0), _)) => {
                    return Err(io::Error::from(io::ErrorKind::UnexpectedEof)
                        .describe("coordinator relay closed connection unexpectedly"));
                }
                Either::Right((Ok(_), _)) => {
                    // Read one or more requests from the stream.
                    let close =
                        read_requests(&mut ctx, &remote, &mut buf, &db_ref, &peers, &mut running)?;
                    if close {
                        debug!("coordinator relay closing connection");
                        while let Some(msg) = ctx.try_receive_next() {
                            debug!("participant dispatcher received a message: {:?}", msg);
                            write_response(&mut ctx, &mut stream, &mut buf, msg).await?;
                        }
                        return stream
                            .write_all(EXIT_PARTICIPANT)
                            .await
                            .map_err(|err| err.describe("writing exit message"));
                    }
                }
                // Read error.
                Either::Right((Err(err), _)) => return Err(err.describe("reading from connection")),
            }
        }
    }

    /// Reads the address of the `coordinator::server` from `stream`.
    async fn read_server_address(
        ctx: &mut actor::Context<Response, ThreadSafe>,
        stream: &mut TcpStream,
        buf: &mut Buffer,
    ) -> crate::Result<SocketAddr> {
        trace!("participant dispatch reading peer's server address");
        loop {
            // NOTE: because we didn't create `buf` it could be its already
            // holding a request, so try to deserialise before reading.

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

            match Deadline::timeout(ctx, IO_TIMEOUT, buf.read_from(&mut *stream)).await {
                Ok(0) => {
                    return Err(io::Error::from(io::ErrorKind::UnexpectedEof)
                        .describe("reading peer's server address"));
                }
                Ok(..) => {}
                Err(err) => return Err(err.describe("reading peer's server address")),
            }
        }
    }

    /// Write all peer addresses in `peers` to `stream`.
    async fn write_peers(
        ctx: &mut actor::Context<Response, ThreadSafe>,
        stream: &mut TcpStream,
        buf: &mut Buffer,
        peers: &Peers,
    ) -> crate::Result<()> {
        trace!("participant dispatch writing all known peers to connection");
        let addresses = peers.addresses();
        // 45 bytes of space per address (max size of a IPv6 address) + 2 for the
        // quotes (JSON string) and another 2 for the slice.
        let mut wbuf = buf.split_write(addresses.len() * (45 + 2) + 2).1;
        serde_json::to_writer(&mut wbuf, &addresses)
            .map_err(|err| io::Error::from(err).describe("serializing peers addresses"))?;
        match Deadline::timeout(ctx, IO_TIMEOUT, stream.write_all(wbuf.as_bytes())).await {
            Ok(()) => Ok(()),
            Err(err) => Err(err.describe("writing known peers")),
        }
    }

    /// Writes `response` to `stream`.
    async fn write_response<'b>(
        ctx: &mut actor::Context<Response, ThreadSafe>,
        stream: &mut TcpStream,
        buf: &mut Buffer,
        response: Response,
    ) -> crate::Result<()> {
        trace!("participant dispatch writing response: {:?}", response);
        let mut wbuf = buf.split_write(MAX_RES_SIZE).1;
        serde_json::to_writer(&mut wbuf, &response)
            .map_err(|err| io::Error::from(err).describe("serializing response"))?;
        match Deadline::timeout(ctx, IO_TIMEOUT, stream.write_all(wbuf.as_bytes())).await {
            Ok(()) => Ok(()),
            Err(err) => Err(err.describe("writing response")),
        }
    }

    /// Read one or more requests in the `buf`fer and start a [`consensus`] actor
    /// for each.
    ///
    /// Return `Ok(true)` if the coordinator wants to close the connection.
    /// Returns `Ok(false)` if more requests are to be expected.
    fn read_requests(
        ctx: &mut actor::Context<Response, ThreadSafe>,
        remote: &SocketAddr,
        buf: &mut Buffer,
        db_ref: &ActorRef<db::Message>,
        peers: &Peers,
        running: &mut HashMap<ConsensusId, ActorRef<VoteResult>, FxBuildHasher>,
    ) -> crate::Result<bool> {
        // TODO: reuse the `Deserializer`, it allocates scratch memory.
        let mut de = serde_json::Deserializer::from_slice(buf.as_bytes()).into_iter::<Request>();

        while let Some(result) = de.next() {
            match result {
                Ok(request) => handle_request(ctx, remote, db_ref, peers, running, request),
                Err(err) if err.is_eof() => break,
                Err(err) => {
                    let bytes_processed = de.byte_offset();
                    buf.processed(bytes_processed);

                    if buf.as_bytes() == EXIT_COORDINATOR {
                        // Participant wants to close the connection.
                        buf.processed(EXIT_COORDINATOR.len());
                        return Ok(true);
                    } else {
                        // TODO: return an error here to the coordinator in case of
                        // an syntax error.
                        return Err(io::Error::from(err).describe("deserialising peer request"));
                    }
                }
            }
        }

        let bytes_processed = de.byte_offset();
        buf.processed(bytes_processed);
        Ok(false)
    }

    /// Handle a single request.
    fn handle_request(
        ctx: &mut actor::Context<Response, ThreadSafe>,
        remote: &SocketAddr,
        db_ref: &ActorRef<db::Message>,
        peers: &Peers,
        running: &mut HashMap<ConsensusId, ActorRef<VoteResult>, FxBuildHasher>,
        request: Request,
    ) {
        debug!("received a request: {:?}", request);
        if request.consensus_id == PARTICIPANT_CONSENSUS_ID {
            let msg = consensus::Message::Peer {
                key: request.key,
                op: request.op,
            };
            peers.send_participant_consensus(msg);
            return;
        }

        // TODO: DRY this.
        match request.op {
            Operation::AddBlob => {
                let consensus_id = request.consensus_id;
                if let Some(actor_ref) = running.remove(&consensus_id) {
                    warn!(
                        "received conflicting consensus ids, stopping both: consensus_id={}",
                        consensus_id
                    );
                    let msg = VoteResult {
                        request_id: request.id,
                        key: request.key, // NOTE: this is the wrong key.
                        result: ConsensusVote::Abort,
                    };
                    // If we fail to send the actor already stopped, so that's
                    // fine.
                    let _ = actor_ref.send(msg);
                    return;
                }

                let responder = RpcResponder {
                    id: request.id,
                    actor_ref: ctx.actor_ref(),
                    phase: ConsensusPhase::init(),
                };
                debug!(
                    "participant dispatcher starting store blob consensus actor: request={:?}, response={:?}",
                    request, responder
                );
                let consensus = consensus::store_blob_actor as fn(_, _, _, _, _, _) -> _;
                let actor_ref = ctx.spawn(
                    |err| {
                        warn!("store blob consensus actor failed: {}", err);
                        SupervisorStrategy::Stop
                    },
                    consensus,
                    (
                        db_ref.clone(),
                        peers.clone(),
                        *remote,
                        request.key,
                        responder,
                    ),
                    ActorOptions::default().mark_ready(),
                );
                // Checked above that we don't have duplicates.
                let _ = running.insert(consensus_id, actor_ref);
            }
            Operation::CommitStoreBlob(timestamp) => {
                if let Some(actor_ref) = running.get(&request.consensus_id) {
                    // Relay the message to the correct actor.
                    let msg = VoteResult {
                        request_id: request.id,
                        key: request.key,
                        result: ConsensusVote::Commit(timestamp),
                    };
                    if let Err(..) = actor_ref.send(msg) {
                        warn!("failed to send to consensus actor for commit request: request_id={}, consensus_id={}",
                            request.id, request.consensus_id);
                        // In case we fail we send ourself a message to relay to
                        // the coordinator that the actor failed.
                        let response = Response {
                            request_id: request.id,
                            vote: ConsensusVote::Fail,
                        };
                        // We can always send ourselves a message.
                        ctx.actor_ref().send(response).unwrap();
                    }
                } else {
                    warn!("can't find consensus actor for commit request: request_id={}, consensus_id={}",
                        request.id, request.consensus_id);
                    let response = Response {
                        request_id: request.id,
                        vote: ConsensusVote::Fail,
                    };
                    // We can always send ourselves a message.
                    ctx.actor_ref().send(response).unwrap();
                }
            }
            Operation::AbortStoreBlob => {
                if let Some(actor_ref) = running.remove(&request.consensus_id) {
                    // Relay the message to the correct actor.
                    let msg = VoteResult {
                        request_id: request.id,
                        key: request.key,
                        result: ConsensusVote::Abort,
                    };
                    if let Err(..) = actor_ref.send(msg) {
                        warn!("failed to send to consensus actor for abort request: request_id={}, consensus_id={}",
                            request.id, request.consensus_id);
                        // In case we fail we send ourself a message to relay to
                        // the coordinator that the actor failed.
                        let response = Response {
                            request_id: request.id,
                            vote: ConsensusVote::Fail,
                        };
                        // We can always send ourselves a message.
                        ctx.actor_ref().send(response).unwrap();
                    }
                } else {
                    warn!("can't find consensus actor for abort request: request_id={}, consensus_id={}",
                        request.id, request.consensus_id);
                    let response = Response {
                        request_id: request.id,
                        vote: ConsensusVote::Fail,
                    };
                    // We can always send ourselves a message.
                    ctx.actor_ref().send(response).unwrap();
                }
            }
            Operation::StoreCommitted(timestamp) => {
                if let Some(actor_ref) = running.remove(&request.consensus_id) {
                    // Relay the message to the correct actor.
                    let msg = VoteResult {
                        request_id: request.id,
                        key: request.key,
                        result: ConsensusVote::Commit(timestamp),
                    };
                    if let Err(..) = actor_ref.send(msg) {
                        warn!("failed to send to consensus actor for committed request: request_id={}, consensus_id={}",
                            request.id, request.consensus_id);
                    }
                } else {
                    warn!("can't find consensus actor for committed request: request_id={}, consensus_id={}",
                        request.id, request.consensus_id);
                }
            }
            Operation::RemoveBlob => {
                let consensus_id = request.consensus_id;
                if let Some(actor_ref) = running.get(&consensus_id) {
                    warn!(
                        "received conflicting consensus ids, stopping both: consensus_id={}",
                        consensus_id
                    );
                    let msg = VoteResult {
                        request_id: request.id,
                        key: request.key, // NOTE: this is the wrong key.
                        result: ConsensusVote::Abort,
                    };
                    // If we fail to send the actor already stopped, so that's
                    // fine.
                    let _ = actor_ref.send(msg);
                    return;
                }

                let responder = RpcResponder {
                    id: request.id,
                    actor_ref: ctx.actor_ref(),
                    phase: ConsensusPhase::init(),
                };
                debug!(
                    "participant dispatcher starting remove blob consensus actor: request={:?}, response={:?}",
                    request, responder
                );
                let consensus = consensus::remove_blob_actor as fn(_, _, _, _, _, _) -> _;
                let actor_ref = ctx.spawn(
                    |err| {
                        warn!("remove blob consensus actor failed: {}", err);
                        SupervisorStrategy::Stop
                    },
                    consensus,
                    (
                        db_ref.clone(),
                        peers.clone(),
                        *remote,
                        request.key,
                        responder,
                    ),
                    ActorOptions::default().mark_ready(),
                );
                // Checked above that we don't have duplicates.
                let _ = running.insert(consensus_id, actor_ref);
            }
            Operation::CommitRemoveBlob(timestamp) => {
                if let Some(actor_ref) = running.get(&request.consensus_id) {
                    // Relay the message to the correct actor.
                    let msg = VoteResult {
                        request_id: request.id,
                        key: request.key,
                        result: ConsensusVote::Commit(timestamp),
                    };
                    if let Err(..) = actor_ref.send(msg) {
                        warn!("failed to send to consensus actor for commit request: request_id={}, consensus_id={}",
                            request.id, request.consensus_id);
                        // In case we fail we send ourself a message to relay to
                        // the coordinator that the actor failed.
                        let response = Response {
                            request_id: request.id,
                            vote: ConsensusVote::Fail,
                        };
                        // We can always send ourselves a message.
                        ctx.actor_ref().send(response).unwrap();
                    }
                } else {
                    warn!("can't find consensus actor for commit request: request_id={}, consensus_id={}",
                        request.id, request.consensus_id);
                    let response = Response {
                        request_id: request.id,
                        vote: ConsensusVote::Fail,
                    };
                    // We can always send ourselves a message.
                    ctx.actor_ref().send(response).unwrap();
                }
            }
            Operation::AbortRemoveBlob => {
                if let Some(actor_ref) = running.remove(&request.consensus_id) {
                    // Relay the message to the correct actor.
                    let msg = VoteResult {
                        request_id: request.id,
                        key: request.key,
                        result: ConsensusVote::Abort,
                    };
                    if let Err(..) = actor_ref.send(msg) {
                        warn!("failed to send to consensus actor for abort request: request_id={}, consensus_id={}",
                            request.id, request.consensus_id);
                        // In case we fail we send ourself a message to relay to
                        // the coordinator that the actor failed.
                        let response = Response {
                            request_id: request.id,
                            vote: ConsensusVote::Fail,
                        };
                        // We can always send ourselves a message.
                        ctx.actor_ref().send(response).unwrap();
                    }
                } else {
                    warn!("can't find consensus actor for abort request: request_id={}, consensus_id={}",
                        request.id, request.consensus_id);
                    let response = Response {
                        request_id: request.id,
                        vote: ConsensusVote::Fail,
                    };
                    // We can always send ourselves a message.
                    ctx.actor_ref().send(response).unwrap();
                }
            }
            Operation::RemoveCommitted(timestamp) => {
                if let Some(actor_ref) = running.remove(&request.consensus_id) {
                    // Relay the message to the correct actor.
                    let msg = VoteResult {
                        request_id: request.id,
                        key: request.key,
                        result: ConsensusVote::Commit(timestamp),
                    };
                    if let Err(..) = actor_ref.send(msg) {
                        warn!("failed to send to consensus actor for committed request: request_id={}, consensus_id={}",
                            request.id, request.consensus_id);
                    }
                } else {
                    warn!("can't find consensus actor for committed request: request_id={}, consensus_id={}",
                        request.id, request.consensus_id);
                }
            }
        }
    }
}

pub mod consensus {
    //! Module with consensus actor.

    use std::collections::HashMap;
    use std::convert::TryInto;
    use std::net::SocketAddr;
    use std::time::{Duration, SystemTime};

    use futures_util::future::{select, Either};
    use futures_util::io::AsyncWriteExt;
    use fxhash::FxBuildHasher;
    use heph::actor::context::ThreadSafe;
    use heph::net::TcpStream;
    use heph::rt::RuntimeAccess;
    use heph::timer::{Deadline, Timer};
    use heph::{actor, ActorRef};
    use log::{debug, error, info, trace, warn};

    use crate::error::Describe;
    use crate::op::{abort_query, add_blob, commit_query, prep_remove_blob, Outcome};
    use crate::peer::participant::RpcResponder;
    use crate::peer::server::{
        BLOB_LENGTH_LEN, DATE_TIME_LEN, METADATA_LEN, NO_BLOB, REQUEST_BLOB,
    };
    use crate::peer::{ConsensusVote, Operation, Peers, COORDINATOR_MAGIC};
    use crate::storage::{Query, RemoveBlob, StoreBlob};
    use crate::{db, Buffer, Key};

    /// Timeout used for I/O between peers.
    const IO_TIMEOUT: Duration = Duration::from_secs(2);

    /// Timeout used for waiting for the result of the census (in each phase).
    const RESULT_TIMEOUT: Duration = Duration::from_secs(5);

    /// Result of the consensus vote.
    #[derive(Debug)]
    pub struct VoteResult {
        /// Id of the request that send the result (so that the dispatcher can
        /// reply to the correct request).
        pub(super) request_id: usize,
        /// The blob to operate on.
        pub(super) key: Key,
        /// Result of the vote.
        pub(super) result: ConsensusVote,
    }

    impl VoteResult {
        /// Returns `true` if the result is to commit.
        fn is_committed(&self) -> bool {
            matches!(self.result, ConsensusVote::Commit(..))
        }
    }

    /// Actor that runs the consensus algorithm for storing a blob.
    pub async fn store_blob_actor(
        mut ctx: actor::Context<VoteResult, ThreadSafe>,
        mut db_ref: ActorRef<db::Message>,
        peers: Peers,
        remote: SocketAddr,
        key: Key,
        mut responder: RpcResponder,
    ) -> crate::Result<()> {
        debug!(
            "store blob consensus actor started: key={}, remote_address={}",
            key, remote
        );

        // TODO: reuse stream and buffer.
        // TODO: stream large blob to a file directly? Preallocating disk space in
        // the data file?
        debug!(
            "connecting to coordinator server: remote_address={}",
            remote
        );
        let mut stream = TcpStream::connect(&mut ctx, remote)
            .map_err(|err| err.describe("creating connect to peer server"))?;
        let mut buf = Buffer::new();

        trace!("writing connection magic");
        match Deadline::timeout(&mut ctx, IO_TIMEOUT, stream.write_all(COORDINATOR_MAGIC)).await {
            Ok(()) => {}
            Err(err) => return Err(err.describe("writing connection magic")),
        }

        let read_blob = read_blob(&mut ctx, &mut stream, &mut buf, &key);
        let blob_len = match read_blob.await {
            Ok(Some((blob_key, blob))) if key == blob_key => blob.len(),
            Ok(Some((blob_key, ..))) => {
                error!(
                    "coordinator server responded with incorrect blob, voting to abort consensus: want_key={}, got_blob_key={}",
                    key, blob_key
                );
                responder.respond(ConsensusVote::Abort);
                return Ok(());
            }
            Ok(None) => {
                error!(
                    "couldn't get blob from coordinator server, voting to abort consensus: key={}",
                    key
                );
                responder.respond(ConsensusVote::Abort);
                return Ok(());
            }
            Err(err) => return Err(err),
        };

        // Phase one: storing the blob, readying it to be added to the database.
        let query = match add_blob(&mut ctx, &mut db_ref, &mut buf, blob_len).await {
            Ok(Outcome::Continue(query)) => {
                responder.respond(ConsensusVote::Commit(SystemTime::now()));
                query
            }
            Ok(Outcome::Done(..)) => {
                info!(
                    "blob already stored, voting to abort consensus: key={}",
                    key
                );
                responder.respond(ConsensusVote::Abort);
                return Ok(());
            }
            Err(()) => {
                warn!(
                    "failed to add blob to storage, voting to abort consensus: key={}",
                    key
                );
                responder.respond(ConsensusVote::Abort);
                return Ok(());
            }
        };
        // We checked the blob's key in `read_blob`.
        debug_assert_eq!(key, *query.key());

        // Phase two: commit or abort the query.
        let timer = Timer::timeout(&mut ctx, RESULT_TIMEOUT);
        let f = select(ctx.receive_next(), timer);
        let vote_result = match f.await {
            Either::Left((vote_result, ..)) => {
                // Update the `RpcResponder.id` to ensure we're responding to
                // the correct request.
                responder.id = vote_result.request_id;
                vote_result
            }
            Either::Right(..) => {
                warn!(
                    "failed to get consensus result in time, running peer conensus: key={}",
                    query.key()
                );
                peers.uncommitted_stored(query);
                return Ok(());
            }
        };

        let response = match vote_result.result {
            ConsensusVote::Commit(timestamp) => {
                commit_query(&mut ctx, &mut db_ref, query, timestamp)
                    .await
                    .map(Some)
            }
            ConsensusVote::Abort | ConsensusVote::Fail => abort_query(&mut ctx, &mut db_ref, query)
                .await
                .map(|_| None),
        };

        let timestamp = match response {
            Ok(timestamp) => {
                let response = ConsensusVote::Commit(SystemTime::now());
                responder.respond(response);
                match timestamp {
                    Some(timestamp) => timestamp,
                    // If the query is aborted we're done.
                    None => return Ok(()),
                }
            }
            Err(()) => {
                responder.respond(ConsensusVote::Abort);
                return Ok(());
            }
        };

        let timer = Timer::timeout(&mut ctx, RESULT_TIMEOUT);
        let f = select(ctx.receive_next(), timer);
        match f.await {
            // Coordinator committed, so we're done.
            Either::Left((vote, ..)) if vote.is_committed() => Ok(()),
            Either::Left(..) | Either::Right(..) => {
                // If the coordinator didn't send us a message that it (and all
                // other participants) committed we could be in an invalid
                // state, where some participants committed and some didn't. We
                // use peer consensus to get us back into an ok state.
                warn!(
                    "failed to get committed message from coordinator, \
                    running peer conensus: key={}",
                    key
                );
                // We're committed, let all other participants know.
                peers.share_stored_commitment(key, timestamp);
                // TODO: check if all peers committed?
                Ok(())
            }
        }
    }

    /// Read a blob (as returned by the [`coordinator::server`]) from `stream`.
    async fn read_blob<'b, M, K>(
        ctx: &mut actor::Context<M, K>,
        stream: &mut TcpStream,
        buf: &'b mut Buffer,
        request_key: &Key,
    ) -> crate::Result<Option<(Key, &'b [u8])>>
    where
        K: RuntimeAccess,
    {
        trace!(
            "requesting key from coordinator server: key={}",
            request_key
        );
        debug_assert!(buf.is_empty());

        // TODO: use vectored I/O here. See
        // https://github.com/rust-lang/futures-rs/pull/2181.
        // Write the request for the key.
        match Deadline::timeout(ctx, IO_TIMEOUT, stream.write_all(&[REQUEST_BLOB])).await {
            Ok(()) => {}
            Err(err) => return Err(err.describe("writing request key")),
        }
        match Deadline::timeout(ctx, IO_TIMEOUT, stream.write_all(request_key.as_bytes())).await {
            Ok(()) => {}
            Err(err) => return Err(err.describe("writing request key")),
        }

        // Don't want to include the length of the blob in the key calculation.
        let mut calc = Key::calculator_skip(stream, METADATA_LEN);

        // Read at least the metadata of the blob.
        let f = Deadline::timeout(ctx, IO_TIMEOUT, buf.read_n_from(&mut calc, METADATA_LEN));
        match f.await {
            Ok(()) => {}
            Err(err) => return Err(err.describe("reading blob length")),
        }

        // Safety: we just read enough bytes, so the marking processed and
        // indexing won't panic.
        buf.processed(DATE_TIME_LEN); // We don't care about the timestamp.
        let blob_length_bytes = buf.as_bytes()[..BLOB_LENGTH_LEN].try_into().unwrap();
        let blob_length = u64::from_be_bytes(blob_length_bytes);
        buf.processed(BLOB_LENGTH_LEN);
        trace!(
            "read blob length from coordinator server: length={}",
            blob_length
        );

        if blob_length == u64::from_be_bytes(NO_BLOB) {
            return Ok(None);
        }

        if (buf.len() as u64) < blob_length {
            // Haven't read entire blob yet.
            let want_n = blob_length - buf.len() as u64;
            let read_n = buf.read_n_from(&mut calc, want_n as usize);
            match Deadline::timeout(ctx, IO_TIMEOUT, read_n).await {
                Ok(()) => {}
                Err(err) => return Err(err.describe("reading blob")),
            }
        }

        let key = calc.finish();
        trace!(
            "read blob from coordinator server: key={}, length={}",
            key,
            blob_length
        );
        // Safety: just read `blob_length` bytes, so this won't panic.
        Ok(Some((key, &buf.as_bytes()[..blob_length as usize])))
    }

    /// Actor that runs the consensus algorithm for removing a blob.
    // TODO: DRY with store_blob_actor.
    // FIXME: use consensus id in logging.
    pub async fn remove_blob_actor(
        mut ctx: actor::Context<VoteResult, ThreadSafe>,
        mut db_ref: ActorRef<db::Message>,
        peers: Peers,
        remote: SocketAddr,
        key: Key,
        mut responder: RpcResponder,
    ) -> crate::Result<()> {
        debug!(
            "remove blob consensus actor started: key={}, remote_address={}",
            key, remote
        );

        // Phase one: removing the blob, readying it to be removed from the database.
        let query = match prep_remove_blob(&mut ctx, &mut db_ref, key.clone()).await {
            Ok(Outcome::Continue(query)) => {
                responder.respond(ConsensusVote::Commit(SystemTime::now()));
                query
            }
            Ok(Outcome::Done(..)) => {
                // FIXME: if we're not synced this can happen, but we should
                // continue.
                info!(
                    "blob already removed/not stored, voting to abort consensus: key={}",
                    key
                );
                responder.respond(ConsensusVote::Abort);
                return Ok(());
            }
            Err(()) => {
                warn!(
                    "failed to prepare storage to remove blob, voting to abort consensus: key={}",
                    key
                );
                responder.respond(ConsensusVote::Abort);
                return Ok(());
            }
        };
        debug_assert_eq!(key, *query.key());

        // Phase two: commit or abort the query.
        let timer = Timer::timeout(&mut ctx, RESULT_TIMEOUT);
        let f = select(ctx.receive_next(), timer);
        let vote_result = match f.await {
            Either::Left((vote_result, ..)) => {
                // Update the `RpcResponder.id` to ensure we're responding to
                // the correct request.
                responder.id = vote_result.request_id;
                vote_result
            }
            Either::Right(..) => {
                warn!(
                    "failed to get consensus result in time, running peer conensus: key={}",
                    query.key()
                );
                peers.uncommitted_removed(query);
                return Ok(());
            }
        };

        let response = match vote_result.result {
            ConsensusVote::Commit(timestamp) => {
                commit_query(&mut ctx, &mut db_ref, query, timestamp)
                    .await
                    .map(Some)
            }
            ConsensusVote::Abort | ConsensusVote::Fail => abort_query(&mut ctx, &mut db_ref, query)
                .await
                .map(|()| None),
        };

        let timestamp = match response {
            Ok(timestamp) => {
                let response = ConsensusVote::Commit(SystemTime::now());
                responder.respond(response);
                match timestamp {
                    Some(timestamp) => timestamp,
                    // If the query is aborted we're done.
                    None => return Ok(()),
                }
            }
            Err(()) => {
                responder.respond(ConsensusVote::Abort);
                return Ok(());
            }
        };

        let timer = Timer::timeout(&mut ctx, RESULT_TIMEOUT);
        let f = select(ctx.receive_next(), timer);
        match f.await {
            // Coordinator committed, so we're done.
            Either::Left((vote, ..)) if vote.is_committed() => Ok(()),
            Either::Left(..) | Either::Right(..) => {
                // If the coordinator didn't send us a message that it (and all
                // other participants) committed we could be in an invalid
                // state, where some participants committed and some didn't. We
                // use peer consensus to get us back into an ok state.
                warn!(
                    "failed to get committed message from coordinator, \
                    running peer conensus: key={}",
                    key
                );
                // We're committed, let all other participants know.
                peers.share_removed_commitment(key, timestamp);
                // TODO: check if all peers committed?
                Ok(())
            }
        }
    }

    /// Message for the [participant consensus actor].
    ///
    /// [participant consensus actor]: actor()
    #[derive(Debug)]
    pub enum Message {
        /// Store query that is uncommitted, where the coordinator failed.
        UncommittedStore(StoreBlob),
        /// Remove query that is uncommitted, where the coordinator failed.
        UncommittedRemove(RemoveBlob),
        /// Message from a peer node.
        Peer { key: Key, op: Operation },
    }

    /// Result for a 2PC as defined by the peers.
    struct PeerResult {
        /// If some at least a single participant committed with the timestamp.
        committed: Option<SystemTime>,
        /// Number of results.
        count: usize,
    }

    enum StorageQuery {
        Store(StoreBlob),
        Remove(RemoveBlob),
    }

    /// Actor that handles participant consensus for this node.
    pub async fn actor(
        mut ctx: actor::Context<Message, ThreadSafe>,
        mut db_ref: ActorRef<db::Message>,
    ) -> Result<(), !> {
        // Queries from local consensus actor where the coordinator failed.
        let mut queries = HashMap::with_hasher(FxBuildHasher::default());
        // Results from peers for consensus queries where the coordinator
        // failed.
        let mut peer_results: HashMap<_, PeerResult, _> =
            HashMap::with_hasher(FxBuildHasher::default());

        loop {
            let msg = ctx.receive_next().await;
            debug!("participant consensus received a message: {:?}", msg);

            match msg {
                Message::UncommittedStore(query) => {
                    if let Some(result) = peer_results.get(query.key()) {
                        if let Some(timestamp) = result.committed {
                            // Don't care about the result, can't handle it
                            // here.
                            let _ = commit_query(&mut ctx, &mut db_ref, query, timestamp).await;
                            continue;
                        }
                    }

                    // TODO: deal with duplicates.
                    queries.insert(query.key().to_owned(), StorageQuery::Store(query));
                }
                Message::UncommittedRemove(query) => {
                    if let Some(result) = peer_results.get(query.key()) {
                        if let Some(timestamp) = result.committed {
                            // Don't care about the result, can't handle it
                            // here.
                            let _ = commit_query(&mut ctx, &mut db_ref, query, timestamp).await;
                            continue;
                        }
                    }

                    // TODO: deal with duplicates.
                    queries.insert(query.key().to_owned(), StorageQuery::Remove(query));
                }
                Message::Peer { key, op } => {
                    use Operation::*;
                    let res = peer_results
                        .entry(key.clone())
                        .and_modify(|res| match op {
                            CommitStoreBlob(timestamp)
                            | StoreCommitted(timestamp)
                            | CommitRemoveBlob(timestamp)
                            | RemoveCommitted(timestamp) => res.committed = Some(timestamp),
                            // TODO: declare the query as failed at some point.
                            _ => res.count += 1,
                        })
                        .or_insert_with(|| match op {
                            CommitStoreBlob(timestamp)
                            | StoreCommitted(timestamp)
                            | CommitRemoveBlob(timestamp)
                            | RemoveCommitted(timestamp) => PeerResult {
                                committed: Some(timestamp),
                                count: 0,
                            },
                            _ => PeerResult {
                                committed: None,
                                count: 1,
                            },
                        });

                    // If one of the peers has committed we also want to commit
                    // the query.
                    if let Some(timestamp) = res.committed {
                        if let Some(query) = queries.remove(&key) {
                            match query {
                                StorageQuery::Store(query) => {
                                    // Don't care about the result, can't handle
                                    // it here.
                                    let _ =
                                        commit_query(&mut ctx, &mut db_ref, query, timestamp).await;
                                }
                                StorageQuery::Remove(query) => {
                                    // Don't care about the result, can't handle
                                    // it here.
                                    let _ =
                                        commit_query(&mut ctx, &mut db_ref, query, timestamp).await;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
