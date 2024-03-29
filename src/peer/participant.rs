//! Participant side of the consensus connection.

use heph::ActorRef;
use log::{debug, warn};

use crate::peer::{ConsensusVote, RequestId, Response};

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
    id: RequestId,
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
        if let Err(err) = self.actor_ref.try_send(response) {
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
    use std::time::SystemTime;

    use futures_util::future::{select, Either};
    use fxhash::FxBuildHasher;
    use heph::actor::context::ThreadSafe;
    use heph::net::TcpStream;
    use heph::rt::options::ActorOptions;
    use heph::supervisor::{NoSupervisor, SupervisorStrategy};
    use heph::timer::Deadline;
    use heph::{actor, ActorRef};
    use log::{debug, trace, warn};

    use crate::error::Describe;
    use crate::passport::Passport;
    use crate::peer::participant::consensus::{self, VoteResult};
    use crate::peer::participant::{ConsensusPhase, RpcResponder};
    use crate::peer::{
        ConsensusId, ConsensusVote, Operation, Peers, Request, Response, EXIT_COORDINATOR,
        EXIT_PARTICIPANT, PARTICIPANT_CONSENSUS_ID,
    };
    use crate::{db, timeout, Buffer};

    /// An estimate of the largest size of an [`Response`] in bytes.
    const MAX_RES_SIZE: usize = 100;

    /// Function to change the log target and module for the warning message,
    /// the [`peer::switcher`] has little to do with the error.
    pub(crate) async fn run_actor(
        ctx: actor::Context<Response, ThreadSafe>,
        stream: TcpStream,
        address: SocketAddr,
        buf: Buffer,
        peers: Peers,
        db_ref: ActorRef<db::Message>,
    ) {
        if let Err(err) = actor(ctx, stream, address, buf, peers, db_ref).await {
            warn!(
                "participant dispatcher failed: {}: remote_address=\"{}\"",
                err, address
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
        stream_address: SocketAddr,
        mut buf: Buffer,
        peers: Peers,
        db_ref: ActorRef<db::Message>,
    ) -> crate::Result<()> {
        debug!("starting participant dispatcher");

        // Read the address at which the peer is listening for peer connections.
        let remote_server =
            read_server_address(&mut ctx, &mut stream, &stream_address, &mut buf).await?;

        // Add it to the list of known peers.
        peers.spawn(&mut ctx, remote_server);

        write_peers(&mut ctx, &mut stream, &mut buf, &peers).await?;
        let mut running = HashMap::with_hasher(FxBuildHasher::default());

        // TODO: close connection cleanly, sending `EXIT_PARTICIPANT`.

        loop {
            buf.move_to_start(true);
            match select(ctx.receive_next(), stream.recv(&mut buf)).await {
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
                    #[rustfmt::skip]
                    let close = read_requests(&mut ctx, &remote_server, &mut buf, &db_ref, &peers, &mut running)?;
                    if close {
                        debug!("coordinator relay closing connection");
                        while let Some(msg) = ctx.try_receive_next() {
                            debug!("participant dispatcher received a message: {:?}", msg);
                            write_response(&mut ctx, &mut stream, &mut buf, msg).await?;
                        }
                        return stream
                            .send_all(EXIT_PARTICIPANT)
                            .await
                            .map_err(|err| err.describe("writing exit message"));
                    }
                }
                // Read error.
                Either::Right((Err(err), _)) => return Err(err.describe("reading from connection")),
            }
        }
    }

    /// Reads the port of the `coordinator::server` from `stream`, return the
    /// address for it based on `remote`.
    async fn read_server_address(
        ctx: &mut actor::Context<Response, ThreadSafe>,
        stream: &mut TcpStream,
        remote: &SocketAddr,
        buf: &mut Buffer,
    ) -> crate::Result<SocketAddr> {
        trace!("reading peer's server port");
        loop {
            // NOTE: because we didn't create `buf` it could be its already
            // holding a request, so try to deserialise before reading.

            // TODO: put `Deserializer` outside the loop.
            // We use the `StreamDeserializer` here because we need the
            // `byte_offset` below.
            let mut iter = serde_json::Deserializer::from_slice(buf.as_slice()).into_iter();
            match iter.next() {
                Some(Ok(port)) => {
                    let bytes_processed = iter.byte_offset();
                    buf.processed(bytes_processed);
                    return Ok(SocketAddr::new(remote.ip(), port));
                }
                // Continue to reading below.
                None => {}
                Some(Err(ref err)) if err.is_eof() => {}
                Some(Err(err)) => {
                    return Err(io::Error::from(err).describe("deserializing peer's server port"))
                }
            }

            match Deadline::timeout(ctx, timeout::PEER_READ, stream.recv(&mut *buf)).await {
                Ok(0) => {
                    return Err(io::Error::from(io::ErrorKind::UnexpectedEof)
                        .describe("reading peer's server port"));
                }
                Ok(..) => {}
                Err(err) => return Err(err.describe("reading peer's server port")),
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
        trace!("participant dispatcher writing all known peers to connection");
        let addresses = peers.addresses();
        // 45 bytes of space per address (max size of a IPv6 address) + 2 for
        // the quotes (JSON string) + 1 for the list separator (,) and another 2
        // for the slice.
        let mut wbuf = buf.split_write(addresses.len() * (45 + 2 + 1) + 2).1;
        serde_json::to_writer(&mut wbuf, &addresses)
            .map_err(|err| io::Error::from(err).describe("serializing peers addresses"))?;
        match Deadline::timeout(ctx, timeout::PEER_WRITE, stream.send_all(wbuf.as_slice())).await {
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
        trace!("participant dispatcher writing response: {:?}", response);
        let mut wbuf = buf.split_write(MAX_RES_SIZE).1;
        serde_json::to_writer(&mut wbuf, &response)
            .map_err(|err| io::Error::from(err).describe("serializing response"))?;
        match Deadline::timeout(ctx, timeout::PEER_WRITE, stream.send_all(wbuf.as_slice())).await {
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
        if buf.as_slice() == EXIT_COORDINATOR {
            // Participant wants to close the connection.
            buf.processed(EXIT_COORDINATOR.len());
            return Ok(true);
        }

        // TODO: reuse the `Deserializer`, it allocates scratch memory.
        let mut de = serde_json::Deserializer::from_slice(buf.as_slice()).into_iter::<Request>();

        while let Some(result) = de.next() {
            match result {
                Ok(request) => handle_request(ctx, remote, db_ref, peers, running, request),
                Err(err) if err.is_eof() => break,
                Err(err) => {
                    let bytes_processed = de.byte_offset();
                    buf.processed(bytes_processed);

                    // TODO: return an error here to the coordinator in case of
                    // an syntax error.
                    return Err(io::Error::from(err).describe("deserialising peer request"));
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
            let msg = if let Operation::StoreCommitted(timestamp) = request.op {
                consensus::Message::PeerCommittedStore(request.key, timestamp)
            } else if let Operation::RemoveCommitted(timestamp) = request.op {
                consensus::Message::PeerCommittedRemove(request.key, timestamp)
            } else {
                // This shouldn't happen, but just in case we might as well log
                // it.
                warn!("dropping useless peer request: {:?}", request);
                return;
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
                        "received conflicting consensus ids, failing both: consensus_id={}",
                        consensus_id
                    );
                    let msg = VoteResult {
                        request_id: request.id,
                        key: request.key, // NOTE: this is the wrong key.
                        result: ConsensusVote::Fail,
                    };
                    // If we fail to send the actor already stopped, so that's
                    // fine.
                    let _ = actor_ref.try_send(msg);
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
                let remote = *remote;
                let key = request.key.clone();
                let args = (
                    db_ref.clone(),
                    peers.clone(),
                    remote,
                    request.key,
                    responder,
                );
                let actor_ref = ctx.spawn(
                    move |err: crate::Error<(io::Error, Passport)>| {
                        warn!("store blob consensus actor failed: {}: {}: remote_address=\"{}\", key=\"{}\", passport={}",
                            err.description(), err.error(), remote, key, err.context());
                        SupervisorStrategy::Stop
                    },
                    consensus,
                    args,
                    ActorOptions::default(),
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
                    if let Err(..) = actor_ref.try_send(msg) {
                        warn!("failed to send to consensus actor for commit request: request_id={}, consensus_id={}",
                            request.id, request.consensus_id);
                        // In case we failed, we send ourself a message to relay
                        // to the coordinator that the actor failed.
                        let response = Response {
                            request_id: request.id,
                            vote: ConsensusVote::Fail,
                        };
                        // We can always send ourselves a message.
                        ctx.actor_ref().try_send(response).unwrap();
                    }
                } else {
                    warn!("can't find consensus actor for commit request: request_id={}, consensus_id={}",
                        request.id, request.consensus_id);
                    let response = Response {
                        request_id: request.id,
                        vote: ConsensusVote::Fail,
                    };
                    // We can always send ourselves a message.
                    ctx.actor_ref().try_send(response).unwrap();
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
                    if let Err(..) = actor_ref.try_send(msg) {
                        // If we can't send a message to the actor it is likely
                        // that the actor failed and caused the consensus run to
                        // fail as well. In any case the consensus run will
                        // be/is already aborted.
                        let response = Response {
                            request_id: request.id,
                            vote: ConsensusVote::Commit(SystemTime::now()),
                        };
                        // We can always send ourselves a message.
                        ctx.actor_ref().try_send(response).unwrap();
                    }
                } else {
                    warn!("can't find consensus actor for abort request: request_id={}, consensus_id={}",
                        request.id, request.consensus_id);
                    let response = Response {
                        request_id: request.id,
                        vote: ConsensusVote::Fail,
                    };
                    // We can always send ourselves a message.
                    ctx.actor_ref().try_send(response).unwrap();
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
                    if let Err(..) = actor_ref.try_send(msg) {
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
                        "received conflicting consensus ids, failing both: consensus_id={}",
                        consensus_id
                    );
                    let msg = VoteResult {
                        request_id: request.id,
                        key: request.key, // NOTE: this is the wrong key.
                        result: ConsensusVote::Fail,
                    };
                    // If we fail to send the actor already stopped, so that's
                    // fine.
                    let _ = actor_ref.try_send(msg);
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
                let remote = *remote;
                let args = (
                    db_ref.clone(),
                    peers.clone(),
                    remote,
                    request.key,
                    responder,
                );
                let actor_ref = ctx.spawn(NoSupervisor, consensus, args, ActorOptions::default());
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
                    if let Err(..) = actor_ref.try_send(msg) {
                        warn!("failed to send to consensus actor for commit request: request_id={}, consensus_id={}",
                            request.id, request.consensus_id);
                        // In case we failed, we send ourself a message to relay
                        // to the coordinator that the actor failed.
                        let response = Response {
                            request_id: request.id,
                            vote: ConsensusVote::Fail,
                        };
                        // We can always send ourselves a message.
                        ctx.actor_ref().try_send(response).unwrap();
                    }
                } else {
                    warn!("can't find consensus actor for commit request: request_id={}, consensus_id={}",
                        request.id, request.consensus_id);
                    let response = Response {
                        request_id: request.id,
                        vote: ConsensusVote::Fail,
                    };
                    // We can always send ourselves a message.
                    ctx.actor_ref().try_send(response).unwrap();
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
                    if let Err(..) = actor_ref.try_send(msg) {
                        // If we can't send a message to the actor it is likely
                        // that the actor failed and caused the consensus run to
                        // fail as well. In any case the consensus run will
                        // be/is already aborted.
                        let response = Response {
                            request_id: request.id,
                            vote: ConsensusVote::Commit(SystemTime::now()),
                        };
                        // We can always send ourselves a message.
                        ctx.actor_ref().try_send(response).unwrap();
                    }
                } else {
                    warn!("can't find consensus actor for abort request: request_id={}, consensus_id={}",
                        request.id, request.consensus_id);
                    let response = Response {
                        request_id: request.id,
                        vote: ConsensusVote::Fail,
                    };
                    // We can always send ourselves a message.
                    ctx.actor_ref().try_send(response).unwrap();
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
                    if let Err(..) = actor_ref.try_send(msg) {
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

    use std::collections::hash_map::Entry;
    use std::collections::HashMap;
    use std::convert::TryInto;
    use std::io::{self, IoSlice};
    use std::net::SocketAddr;
    use std::slice;
    use std::time::{Duration, Instant, SystemTime};

    use futures_util::future::{select, Either};
    use futures_util::io::AsyncWriteExt;
    use fxhash::FxBuildHasher;
    use heph::actor::context::ThreadSafe;
    use heph::net::TcpStream;
    use heph::timer::{Deadline, Timer};
    use heph::{actor, rt, ActorRef};
    use log::{debug, error, info, trace, warn};

    use crate::db::{self, db_error};
    use crate::error::Describe;
    use crate::net::tcp_connect_retry;
    use crate::op::{
        abort_query, add_blob, commit_query, prep_remove_blob, retrieve_store_blob_query,
        stream_add_blob, Outcome, StreamResult,
    };
    use crate::passport::{Event, Passport, Uuid};
    use crate::peer::participant::RpcResponder;
    use crate::peer::server::{
        BLOB_LENGTH_LEN, DATE_TIME_LEN, METADATA_LEN, NO_BLOB, REQUEST_BLOB,
    };
    use crate::peer::{ConsensusVote, Peers, RequestId, COORDINATOR_MAGIC};
    use crate::storage::{BlobAlreadyStored, DateTime, Query, RemoveBlob, StoreBlob, StreamBlob};
    use crate::{timeout, Buffer, Key};

    /// Minimum blob size to stream the blob.
    const MIN_STREAM_BLOB_SIZE: u64 = 4 * 1024; // 4kB.

    /// Result of the consensus vote.
    #[derive(Debug)]
    pub struct VoteResult {
        /// Id of the request that send the result (so that the dispatcher can
        /// reply to the correct request).
        pub(super) request_id: RequestId,
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
    ) -> Result<(), crate::Error<(io::Error, Passport)>> {
        let mut passport = Passport::new();
        debug!(
            "store blob consensus actor started: \
                request_id=\"{}\", key=\"{}\", remote_address=\"{}\"",
            passport.id(),
            key,
            remote
        );

        let prepare = prepare_store_blob(&mut ctx, &mut db_ref, &mut passport, key.clone(), remote);
        let query = match prepare.await {
            Ok(Ok(query)) => {
                responder.respond(ConsensusVote::Commit(SystemTime::now()));
                passport.mark(Event::ConsensusPhaseOneComplete);
                query
            }
            Ok(Err(PrepareError::BlobAlreadyStored)) => {
                info!(
                    "blob already stored, voting to abort consensus: \
                        request_id=\"{}\", key=\"{}\"",
                    passport.id(),
                    key
                );
                responder.respond(ConsensusVote::Abort);
                return Ok(());
            }
            Ok(Err(PrepareError::FailedToRetrieveBlob)) => {
                passport.mark(Event::ConsensusFailed);
                error!(
                    "couldn't get blob from coordinator server, failing consensus: \
                        request_id=\"{}\", key=\"{}\", passport={}",
                    passport.id(),
                    key,
                    passport
                );
                // As this shouldn't happen we don't vote to abort, but to fail
                // it.
                responder.respond(ConsensusVote::Fail);
                return Ok(());
            }
            Ok(Err(PrepareError::IncorrectKey)) => {
                passport.mark(Event::ConsensusFailed);
                error!(
                    "coordinator server responded with incorrect blob, failing consensus: \
                        request_id=\"{}\", key=\"{}\", passport={}",
                    passport.id(),
                    key,
                    passport
                );
                // As this shouldn't happen we don't vote to abort, but to fail
                // it.
                responder.respond(ConsensusVote::Fail);
                return Ok(());
            }
            Err(err) => return Err(err.with(passport)),
        };

        // Phase two: commit or abort the query.
        let timer = Timer::timeout(&mut ctx, timeout::PEER_CONSENSUS);
        let f = select(ctx.receive_next(), timer);
        let vote_result = match f.await {
            Either::Left((vote_result, ..)) => {
                // Update the `RpcResponder.id` to ensure we're responding to
                // the correct request.
                responder.id = vote_result.request_id;
                passport.mark(Event::ConsensusPhaseOneResults);
                vote_result
            }
            Either::Right(..) => {
                passport.mark(Event::ConsensusFailed);
                warn!(
                    "failed to get consensus result in time, running participant consensus: \
                        request_id=\"{}\", key=\"{}\", passport={}",
                    passport.id(),
                    key,
                    passport
                );
                peers.uncommitted_stored(query);
                return Ok(());
            }
        };

        let result = match vote_result.result {
            ConsensusVote::Commit(timestamp) => {
                match commit_query(&mut ctx, &mut db_ref, &mut passport, query, timestamp).await {
                    Ok(got_timestamp) if got_timestamp == timestamp => Ok(Some(timestamp)),
                    // If the timestamp is different it means the blob was
                    // already store, so we'll want to abort this run.
                    Ok(..) | Err(()) => Err(()),
                }
            }
            ConsensusVote::Abort | ConsensusVote::Fail => {
                abort_query(&mut ctx, &mut db_ref, &mut passport, query)
                    .await
                    .map(|_| None)
            }
        };

        let timestamp = match result {
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
                responder.respond(ConsensusVote::Fail);
                return Ok(());
            }
        };

        let timer = Timer::timeout(&mut ctx, timeout::PEER_CONSENSUS);
        let f = select(ctx.receive_next(), timer);
        match f.await {
            // Coordinator committed, so we're done.
            Either::Left((vote, ..)) if vote.is_committed() => {
                passport.mark(Event::ConsensusPhaseTwoResults);
                info!(
                    "completed store blob consensus: \
                        request_id=\"{}\", key=\"{}\", runtime=\"{:?}\", passport={}",
                    passport.id(),
                    key,
                    passport.elapsed(),
                    passport,
                );
                Ok(())
            }
            Either::Left(..) | Either::Right(..) => {
                // If the coordinator didn't send us a message that it (and all
                // other participants) committed we could be in an invalid
                // state, where some participants committed and some didn't. We
                // use participant consensus to get us back into an ok state.
                warn!(
                    "failed to get committed message from coordinator, running participant consensus: \
                        request_id=\"{}\", key=\"{}\", runtime=\"{:?}\", passport={}",
                    passport.id(),
                    key,
                    passport.elapsed(),
                    passport,
                );
                // We're committed, let all other participants know.
                peers.share_stored_commitment(key, timestamp);
                // TODO: check if all peers committed?
                Ok(())
            }
        }
    }

    /// Error returned by [`prepare_store_blob`].
    enum PrepareError {
        BlobAlreadyStored,
        FailedToRetrieveBlob,
        IncorrectKey,
    }

    async fn prepare_store_blob<'b, M, K>(
        ctx: &mut actor::Context<M, K>,
        db_ref: &mut ActorRef<db::Message>,
        passport: &mut Passport,
        key: Key,
        remote: SocketAddr,
    ) -> crate::Result<Result<StoreBlob, PrepareError>>
    where
        actor::Context<M, K>: rt::Access,
    {
        match retrieve_store_blob_query(ctx, db_ref, passport, key.clone()).await {
            // Already a query in progress, reuse that.
            Ok(Ok(Some(query))) => return Ok(Ok(query)),
            // No query in progress, so we can proceed.
            Ok(Ok(None)) => {}
            // Blob is already stored.
            Ok(Err(BlobAlreadyStored)) => return Ok(Err(PrepareError::BlobAlreadyStored)),
            // Database error.
            Err(()) => return Err(db_error()),
        }

        // TODO: reuse stream and buffer.
        let mut stream = peer_connect(ctx, passport, remote).await?;
        let mut buf = Buffer::new();

        let blob_length = match request_blob(ctx, &mut stream, passport, &mut buf, &key).await? {
            Some(blob_length) => blob_length,
            None => return Ok(Err(PrepareError::FailedToRetrieveBlob)),
        };

        match blob_length {
            blob_length if blob_length <= MIN_STREAM_BLOB_SIZE => {
                read_blob(ctx, &mut stream, passport, &mut buf, blob_length).await?;

                // NOTE: `add_blob` will advance the buffer for us.
                match add_blob(ctx, db_ref, passport, &mut buf, blob_length as usize).await {
                    Ok(Outcome::Continue(query)) if *query.key() == key => Ok(Ok(query)),
                    // Key of the blob we retrieve doesn't match.
                    Ok(Outcome::Continue(_)) => Ok(Err(PrepareError::IncorrectKey)),
                    // Blob is already stored.
                    Ok(Outcome::Done(blob_key)) if blob_key == key => {
                        Ok(Err(PrepareError::BlobAlreadyStored))
                    }
                    Ok(Outcome::Done(_)) => Ok(Err(PrepareError::IncorrectKey)),
                    Err(()) => Err(db_error()),
                }
            }
            blob_length => {
                let write = move |mut stream_blob: Box<StreamBlob>| {
                    // First copy over all the contents of the blob in the
                    // buffer to the data file.
                    buf.copy_to(&mut *stream_blob);

                    async move {
                        // If the entire blob wasn't in the buffer, read the
                        // remaining bytes from the stream.
                        let bytes_left = stream_blob.bytes_left();
                        if bytes_left > 0 {
                            stream
                                .recv_n(&mut *stream_blob, bytes_left)
                                .await
                                .map(|()| stream_blob)
                        } else {
                            // Entire blob was in the buffer already, so we're
                            // done.
                            Ok(stream_blob)
                        }
                    }
                };

                match stream_add_blob(ctx, db_ref, passport, blob_length as usize, write).await {
                    StreamResult::Ok(Outcome::Continue(query)) if *query.key() == key => {
                        Ok(Ok(query))
                    }
                    // Key of the blob we retrieve doesn't match.
                    StreamResult::Ok(Outcome::Continue(_)) => Ok(Err(PrepareError::IncorrectKey)),
                    // Blob is already stored.
                    StreamResult::Ok(Outcome::Done(blob_key)) if blob_key == key => {
                        Ok(Err(PrepareError::BlobAlreadyStored))
                    }
                    StreamResult::Ok(Outcome::Done(_)) => Ok(Err(PrepareError::IncorrectKey)),
                    StreamResult::IoErr(err) => Err(err.describe("streaming blob")),
                    StreamResult::DbError => Err(db_error()),
                }
            }
        }
    }

    /// Connect to peer at `remote` address.
    async fn peer_connect<M, K>(
        ctx: &mut actor::Context<M, K>,
        passport: &mut Passport,
        remote: SocketAddr,
    ) -> crate::Result<TcpStream>
    where
        actor::Context<M, K>: rt::Access,
    {
        debug!(
            "connecting to coordinator server: request_id=\"{}\", remote_address=\"{}\"",
            passport.id(),
            remote
        );
        // We use a low retry value and number of retries because we should
        // already be connected to the peer, so we known it up and running.
        let mut stream = tcp_connect_retry(ctx, remote, Duration::from_millis(100), 3)
            .await
            .map_err(|err| err.describe("connecting to peer server"))?;

        trace!("writing connection magic: request_id=\"{}\"", passport.id());
        let write = stream.send_all(COORDINATOR_MAGIC);
        match Deadline::timeout(ctx, timeout::PEER_WRITE, write).await {
            Ok(()) => {}
            Err(err) => return Err(err.describe("writing connection magic")),
        }

        // We only send small requests.
        if let Err(err) = stream.set_nodelay(true) {
            warn!(
                "failed to set no delay, continuing: {}: request_id=\"{}\", remote_address=\"{}\"",
                err,
                passport.id(),
                remote
            );
        }

        passport.mark(Event::ConnectedToPeerServer);
        Ok(stream)
    }

    /// Request blob from `stream`.
    ///
    /// Returns the length of the blob, or `None` if the blob is not found. If
    /// the blob is found `buf` might contain part of it.
    async fn request_blob<'b, M, K>(
        ctx: &mut actor::Context<M, K>,
        stream: &mut TcpStream,
        passport: &mut Passport,
        buf: &mut Buffer,
        key: &Key,
    ) -> crate::Result<Option<u64>>
    where
        actor::Context<M, K>: rt::Access,
    {
        trace!(
            "requesting key from coordinator server: request_id=\"{}\", key=\"{}\"",
            passport.id(),
            key
        );

        // Write the request for the key.
        let bufs = &mut [
            IoSlice::new(slice::from_ref(&REQUEST_BLOB)),
            IoSlice::new(key.as_bytes()),
        ];
        match Deadline::timeout(ctx, timeout::PEER_WRITE, stream.write_all_vectored(bufs)).await {
            Ok(()) => {}
            Err(err) => return Err(err.describe("writing blob request")),
        }
        passport.mark(Event::WrittenRequestBlobRequest);

        // Read at least the metadata of the blob.
        buf.reserve_atleast(METADATA_LEN);
        let write = stream.recv_n(&mut *buf, METADATA_LEN);
        match Deadline::timeout(ctx, timeout::PEER_READ, write).await {
            Ok(()) => {}
            Err(err) => return Err(err.describe("reading blob length")),
        }
        passport.mark(Event::ReadRequestBlobResponse);

        // Safety: we just read enough bytes, so the marking processed and
        // indexing won't panic.
        let timestamp =
            DateTime::from_bytes(&buf.as_slice()[..DATE_TIME_LEN]).unwrap_or(DateTime::INVALID);

        let blob_length_bytes = buf.as_slice()[DATE_TIME_LEN..DATE_TIME_LEN + BLOB_LENGTH_LEN]
            .try_into()
            .unwrap();
        let blob_length = u64::from_be_bytes(blob_length_bytes);
        buf.processed(METADATA_LEN);
        trace!(
            "read blob length from coordinator server: request_id=\"{}\", blob_length={}",
            passport.id(),
            blob_length
        );

        // NOTE: We allow invalid timestamps as uncommited blobs will have an
        // invalid timestamp.
        if timestamp.is_removed() || blob_length == u64::from_be_bytes(NO_BLOB) {
            Ok(None)
        } else {
            Ok(Some(blob_length))
        }
    }

    /// Reads a blob from `stream` into `buf`.
    async fn read_blob<M, K>(
        ctx: &mut actor::Context<M, K>,
        stream: &mut TcpStream,
        passport: &mut Passport,
        buf: &mut Buffer,
        blob_length: u64,
    ) -> crate::Result<()>
    where
        actor::Context<M, K>: rt::Access,
    {
        if (buf.len() as u64) < blob_length {
            // Haven't read entire blob yet.
            let want_n = blob_length - buf.len() as u64;
            buf.reserve_atleast(want_n as usize);
            let read_n = stream.recv_n(buf, want_n as usize);
            match Deadline::timeout(ctx, timeout::peer_read(want_n), read_n).await {
                Ok(()) => {}
                Err(err) => return Err(err.describe("reading blob")),
            }
            passport.mark(Event::ReadRequestBlobResponseBlob);
        }

        trace!(
            "read blob from coordinator server: request_id=\"{}\", blob_length={}",
            passport.id(),
            blob_length
        );
        Ok(())
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
    ) -> Result<(), !> {
        let mut passport = Passport::new();
        debug!(
            "remove blob consensus actor started: request_id=\"{}\", key=\"{}\", remote_address=\"{}\"",
            passport.id(),
            key,
            remote
        );

        // Phase one: removing the blob, readying it to be removed from the database.
        let query = match prep_remove_blob(&mut ctx, &mut db_ref, &mut passport, key.clone()).await
        {
            Ok(Outcome::Continue(query)) => {
                responder.respond(ConsensusVote::Commit(SystemTime::now()));
                passport.mark(Event::ConsensusPhaseOneComplete);
                query
            }
            Ok(Outcome::Done(..)) => {
                passport.mark(Event::ConsensusFailed);
                // FIXME: if we're not synced this can happen, but we should
                // continue.
                info!(
                    "blob already removed/not stored, voting to abort consensus: \
                        request_id=\"{}\", key=\"{}\", passport={}",
                    passport.id(),
                    key,
                    passport
                );
                responder.respond(ConsensusVote::Abort);
                return Ok(());
            }
            Err(()) => {
                passport.mark(Event::ConsensusFailed);
                warn!(
                    "failed to prepare storage to remove blob, failing consensus: \
                        request_id=\"{}\", key=\"{}\", passport={}",
                    passport.id(),
                    key,
                    passport
                );
                responder.respond(ConsensusVote::Fail);
                return Ok(());
            }
        };
        debug_assert_eq!(key, *query.key());

        // Phase two: commit or abort the query.
        let timer = Timer::timeout(&mut ctx, timeout::PEER_CONSENSUS);
        let f = select(ctx.receive_next(), timer);
        let vote_result = match f.await {
            Either::Left((vote_result, ..)) => {
                // Update the `RpcResponder.id` to ensure we're responding to
                // the correct request.
                responder.id = vote_result.request_id;
                passport.mark(Event::ConsensusPhaseOneResults);
                vote_result
            }
            Either::Right(..) => {
                passport.mark(Event::ConsensusFailed);
                warn!(
                    "failed to get consensus result in time, running participant consensus: \
                        request_id=\"{}\", key=\"{}\", passport={}",
                    passport.id(),
                    query.key(),
                    passport
                );
                peers.uncommitted_removed(query);
                return Ok(());
            }
        };

        let result = match vote_result.result {
            ConsensusVote::Commit(timestamp) => {
                commit_query(&mut ctx, &mut db_ref, &mut passport, query, timestamp)
                    .await
                    .map(Some)
            }
            ConsensusVote::Abort | ConsensusVote::Fail => {
                abort_query(&mut ctx, &mut db_ref, &mut passport, query)
                    .await
                    .map(|()| None)
            }
        };

        let timestamp = match result {
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
                responder.respond(ConsensusVote::Fail);
                return Ok(());
            }
        };

        let timer = Timer::timeout(&mut ctx, timeout::PEER_CONSENSUS);
        let f = select(ctx.receive_next(), timer);
        match f.await {
            // Coordinator committed, so we're done.
            Either::Left((vote, ..)) if vote.is_committed() => {
                passport.mark(Event::ConsensusPhaseTwoResults);
                info!(
                    "completed remove blob consensus: \
                        request_id=\"{}\", key=\"{}\", runtime=\"{:?}\", passport={}",
                    passport.id(),
                    key,
                    passport.elapsed(),
                    passport,
                );
                Ok(())
            }
            Either::Left(..) | Either::Right(..) => {
                // If the coordinator didn't send us a message that it (and all
                // other participants) committed we could be in an invalid
                // state, where some participants committed and some didn't. We
                // use participant consensus to get us back into an ok state.
                warn!(
                    "failed to get committed message from coordinator, running participant consensus: \
                        request_id=\"{}\", key=\"{}\", runtime=\"{:?}\", passport={}",
                    passport.id(),
                    key,
                    passport.elapsed(),
                    passport,
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
        // From local actors:
        /// Store query that is uncommitted, where the coordinator failed.
        UncommittedStore(StoreBlob),
        /// Remove query that is uncommitted, where the coordinator failed.
        UncommittedRemove(RemoveBlob),

        // From remote peers:
        /// Peer committed to storing blob with `key`.
        PeerCommittedStore(Key, SystemTime),
        /// Peer committed to removing blob with `key`.
        PeerCommittedRemove(Key, SystemTime),
    }

    /// State of a participant consensus run.
    enum ConsensusState {
        /// Currently no peer indicated that they committed. The `Instant` is
        /// the time the store was last updated, used as timeout before removing
        /// it. NOTE: this is **not** the timestamp used to commit to the query.
        UndecidedStore(StoreBlob, Instant),
        /// If a peer participant received a commit message, before the local
        /// consensus actor send us a message, we want to keep track of it in
        /// case the local consensus actor decides to run the participant
        /// consensus. We don't want to miss a commit message from a peer after
        /// all.
        CommittedStore(SystemTime),
        /// Same as `UndecidedStore`, but for remove storage query.
        UndecidedRemove(RemoveBlob, Instant),
        /// Same as `CommittedStore`, but for remove.
        CommittedRemove(SystemTime),
    }

    impl ConsensusState {
        /// Unwrap `ConsensusState::UndecidedStore`.
        #[track_caller]
        fn unwrap_undecided_store(self) -> StoreBlob {
            match self {
                ConsensusState::UndecidedStore(query, ..) => query,
                _ => unreachable!("tried to unwrap ConsensusState::UndecidedStore"),
            }
        }

        /// Unwrap `ConsensusState::CommittedStore`.
        #[track_caller]
        fn unwrap_committed_store(self) -> SystemTime {
            match self {
                ConsensusState::CommittedStore(timestamp) => timestamp,
                _ => unreachable!("tried to unwrap ConsensusState::CommittedStore"),
            }
        }

        /// Unwrap `ConsensusState::UndecidedRemove`.
        #[track_caller]
        fn unwrap_undecided_remove(self) -> RemoveBlob {
            match self {
                ConsensusState::UndecidedRemove(query, ..) => query,
                _ => unreachable!("tried to unwrap ConsensusState::UndecidedRemove"),
            }
        }

        /// Unwrap `ConsensusState::CommittedRemove`.
        #[track_caller]
        fn unwrap_committed_remove(self) -> SystemTime {
            match self {
                ConsensusState::CommittedRemove(timestamp) => timestamp,
                _ => unreachable!("tried to unwrap ConsensusState::CommittedRemove"),
            }
        }
    }

    /// Actor that handles participant consensus for this node.
    pub async fn actor(
        mut ctx: actor::Context<Message, ThreadSafe>,
        mut db_ref: ActorRef<db::Message>,
    ) -> Result<(), !> {
        // States of all ongoing participant consensus runs.
        let mut states = HashMap::with_hasher(FxBuildHasher::default());
        // TODO: add a cleanup routine that removes `Undecided*` values from the
        // `states` map.

        // TODO: use and log the passport properly.
        let mut passport = Passport::empty();
        loop {
            let msg = ctx.receive_next().await;
            passport.reset();
            passport.set_id(Uuid::new());
            debug!("participant consensus received a message: {:?}", msg);

            match msg {
                Message::UncommittedStore(query) => {
                    let entry = states.entry(query.key().clone());
                    let state = ConsensusState::UndecidedStore(query, Instant::now());
                    update_state(&mut ctx, &mut db_ref, &mut passport, entry, state).await
                }
                Message::UncommittedRemove(query) => {
                    let entry = states.entry(query.key().clone());
                    let state = ConsensusState::UndecidedRemove(query, Instant::now());
                    update_state(&mut ctx, &mut db_ref, &mut passport, entry, state).await
                }
                Message::PeerCommittedStore(key, timestamp) => {
                    let entry = states.entry(key);
                    let state = ConsensusState::CommittedStore(timestamp);
                    update_state(&mut ctx, &mut db_ref, &mut passport, entry, state).await
                }
                Message::PeerCommittedRemove(key, timestamp) => {
                    let entry = states.entry(key);
                    let state = ConsensusState::CommittedRemove(timestamp);
                    update_state(&mut ctx, &mut db_ref, &mut passport, entry, state).await
                }
            }
        }
    }

    /// Update an `entry` with the new `state`. If current state (`entry`) and
    /// `state` match (e.g. a undecided store blob query and a commit to store
    /// blob) it will commit to the query.
    async fn update_state(
        ctx: &mut actor::Context<Message, ThreadSafe>,
        db_ref: &mut ActorRef<db::Message>,
        passport: &mut Passport,
        entry: Entry<'_, Key, ConsensusState>,
        state: ConsensusState,
    ) {
        use ConsensusState::*;
        match entry {
            Entry::Occupied(mut entry) => match (state, entry.get_mut()) {
                // Adding a store blob query.
                (UndecidedStore(q, la), UndecidedStore(query, last_update)) => {
                    // It seems the 2PC was retried before all peers shared
                    // there results. Best we can do is update the timestamp.
                    debug_assert_eq!(query.key(), q.key());
                    *last_update = la;
                }
                (UndecidedStore(query, ..), CommittedStore(..)) => {
                    // Previously received a message from a peer how committed
                    // to storing the blob, so no so will we.
                    let (key, state) = entry.remove_entry();
                    debug_assert_eq!(key, *query.key());
                    let timestamp = state.unwrap_committed_store();
                    if let Err(()) = commit_query(ctx, db_ref, passport, query, timestamp).await {
                        // TODO: add passport id?
                        // FIXME: still need to add the blob to storage...
                        warn!("failed to commit to store query: key={}", key);
                    }
                }
                (state @ UndecidedStore(..), UndecidedRemove(..))
                | (state @ UndecidedStore(..), CommittedRemove(..)) => {
                    #[rustfmt::skip]
                    warn!("discarding previous remove participant consensus: key={}", entry.key());
                    entry.insert(state);
                }
                // Peer is committed to storing a blob.
                (CommittedStore(timestamp), UndecidedStore(..)) => {
                    let (key, state) = entry.remove_entry();
                    let query = state.unwrap_undecided_store();
                    debug_assert_eq!(key, *query.key());
                    if let Err(()) = commit_query(ctx, db_ref, passport, query, timestamp).await {
                        // TODO: add passport id?
                        // FIXME: still need to add the blob to storage...
                        warn!("failed to commit to store query: key={}", key);
                    }
                }
                (CommittedStore(t), CommittedStore(timestamp)) => {
                    if t != *timestamp {
                        // FIXME: Oh no, differing timestamps!
                        error!("Received different timestamps to commit store blob query");
                    }
                }
                (state @ CommittedStore(..), UndecidedRemove(..))
                | (state @ CommittedStore(..), CommittedRemove(..)) => {
                    #[rustfmt::skip]
                    warn!("discarding previous remove participant consensus: key={}", entry.key());
                    entry.insert(state);
                }

                // Adding a remove blob query.
                // NOTE: this follows the same structure as above, see it for
                // comments etc.
                (UndecidedRemove(q, la), UndecidedRemove(query, last_update)) => {
                    debug_assert_eq!(query.key(), q.key());
                    *last_update = la;
                }
                (UndecidedRemove(query, ..), CommittedRemove(..)) => {
                    let (key, state) = entry.remove_entry();
                    debug_assert_eq!(key, *query.key());
                    let timestamp = state.unwrap_committed_remove();
                    if let Err(()) = commit_query(ctx, db_ref, passport, query, timestamp).await {
                        // TODO: add passport id?
                        // FIXME: still need to add the blob to storage...
                        warn!("failed to commit to remove query: key={}", key);
                    }
                }
                (state @ UndecidedRemove(..), UndecidedStore(..))
                | (state @ UndecidedRemove(..), CommittedStore(..)) => {
                    #[rustfmt::skip]
                    warn!("discarding previous store participant consensus: key={}", entry.key());
                    entry.insert(state);
                }
                // Peer is committed to removing a blob.
                (CommittedRemove(timestamp), UndecidedRemove(..)) => {
                    let (key, state) = entry.remove_entry();
                    let query = state.unwrap_undecided_remove();
                    debug_assert_eq!(key, *query.key());
                    if let Err(()) = commit_query(ctx, db_ref, passport, query, timestamp).await {
                        // TODO: add passport id?
                        // FIXME: still need to add the blob to storage...
                        warn!("failed to commit to store query: key={}", key);
                    }
                }
                (CommittedRemove(t), CommittedRemove(timestamp)) => {
                    if t != *timestamp {
                        // FIXME: Oh no, differing timestamps!
                        error!("Received different timestamps to commit remove blob query");
                    }
                }
                (state @ CommittedRemove(..), UndecidedStore(..))
                | (state @ CommittedRemove(..), CommittedStore(..)) => {
                    #[rustfmt::skip]
                    warn!("discarding previous store participant consensus: key={}", entry.key());
                    entry.insert(state);
                }
            },
            Entry::Vacant(entry) => {
                // If its the first entry we simply insert it.
                entry.insert(state);
            }
        }
    }
}
