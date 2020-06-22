//! Participant side of the consensus connection.

use heph::ActorRef;
use log::{debug, warn};

use crate::peer::{ConsensusVote, Response};

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
    use crate::peer::{ConsensusId, ConsensusVote, Operation, Peers, Request, Response};
    use crate::{db, Buffer};

    /// Timeout used for I/O between peers.
    const IO_TIMEOUT: Duration = Duration::from_secs(2);

    /// An estimate of the largest size of an [`Response`] in bytes.
    const MAX_RES_SIZE: usize = 100;

    /// Actor that accepts messages from [`coordinator::relay`] over the
    /// `stream` and starts a [`consensus`] actor for each run of the consensus
    /// algorithm.
    ///
    /// [`coordinator::relay`]: crate::peer::coordinator::relay
    /// [`consensus`]: super::consensus::
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
        let mut running = HashMap::new();

        loop {
            match select(ctx.receive_next(), buf.read_from(&mut stream)).await {
                Either::Left((msg, _)) => {
                    debug!("participant dispatcher received a message: {:?}", msg);
                    write_response(&mut ctx, &mut stream, &mut buf, msg).await?;
                }
                Either::Right((Ok(0), _)) => {
                    debug!("peer coordinator closed connection");
                    while let Some(msg) = ctx.try_receive_next() {
                        // Write any response left in our inbox.
                        // TODO: ignore errors here since the other side is
                        // already disconnected?
                        debug!("participant dispatcher received a message: {:?}", msg);
                        write_response(&mut ctx, &mut stream, &mut buf, msg).await?;
                    }
                    return Ok(());
                }
                Either::Right((Ok(_), _)) => {
                    // Read one or more requests from the stream.
                    read_requests(&mut ctx, &remote, &mut buf, &db_ref, &mut running)?;
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
                Ok(Ok(0)) => {
                    return Err(io::Error::from(io::ErrorKind::UnexpectedEof)
                        .describe("reading peer's server address"));
                }
                Ok(Ok(..)) => {}
                Ok(Err(err)) => return Err(err.describe("reading peer's server address")),
                Err(err) => {
                    return Err(
                        io::Error::from(err).describe("timeout reading peer's server address")
                    );
                }
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
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(err.describe("writing known peers")),
            Err(err) => Err(io::Error::from(err).describe("timeout writing known peers")),
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
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(err.describe("writing response")),
            Err(err) => Err(io::Error::from(err).describe("timeout writing response")),
        }
    }

    /// Read one or more requests in the `buf`fer and start a [`consensus`] actor
    /// for each.
    fn read_requests(
        ctx: &mut actor::Context<Response, ThreadSafe>,
        remote: &SocketAddr,
        buf: &mut Buffer,
        db_ref: &ActorRef<db::Message>,
        running: &mut HashMap<ConsensusId, ActorRef<VoteResult>>,
    ) -> crate::Result<()> {
        // TODO: reuse the `Deserializer`, it allocates scratch memory.
        let mut de = serde_json::Deserializer::from_slice(buf.as_bytes()).into_iter::<Request>();

        while let Some(result) = de.next() {
            match result {
                Ok(request) => handle_request(ctx, remote, db_ref, running, request),
                Err(err) if err.is_eof() => break,
                Err(err) => {
                    // TODO: return an error here to the coordinator in case of
                    // an syntax error.
                    return Err(io::Error::from(err).describe("deserializing peer request"));
                }
            }
        }

        let bytes_processed = de.byte_offset();
        buf.processed(bytes_processed);
        Ok(())
    }

    /// Handle a single request.
    fn handle_request(
        ctx: &mut actor::Context<Response, ThreadSafe>,
        remote: &SocketAddr,
        db_ref: &ActorRef<db::Message>,
        running: &mut HashMap<ConsensusId, ActorRef<VoteResult>>,
        request: Request,
    ) {
        debug!("received a request: {:?}", request);

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
                let request = consensus::Request {
                    key: request.key,
                    remote: *remote,
                };
                debug!(
                    "participant dispatcher starting store blob consensus actor: request={:?}, response={:?}",
                    request, responder
                );
                let consensus = consensus::store_blob_actor as fn(_, _, _, _) -> _;
                let actor_ref = ctx.spawn(
                    |err| {
                        warn!("store blob consensus actor failed: {}", err);
                        SupervisorStrategy::Stop
                    },
                    consensus,
                    (request, responder, db_ref.clone()),
                    ActorOptions::default().mark_ready(),
                );
                // Checked above that we don't have duplicates.
                let _ = running.insert(consensus_id, actor_ref);
            }
            Operation::CommitStoreBlob(timestamp) => {
                if let Some(actor_ref) = running.remove(&request.consensus_id) {
                    // Relay the message to the correct actor.
                    let msg = VoteResult {
                        request_id: request.id,
                        key: request.key,
                        result: ConsensusVote::Commit(timestamp),
                    };
                    if let Err(..) = actor_ref.send(msg) {
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
            Operation::RemoveBlob => {
                // FIXME: Implement this.
                todo!("Implement remove blob");
            }
        }
    }
}

pub mod consensus {
    //! Module with the [consensus actor].
    //!
    //! [consensus actor]: actor()

    use std::convert::TryInto;
    use std::io;
    use std::net::SocketAddr;
    use std::time::{Duration, SystemTime};

    use futures_util::io::AsyncWriteExt;
    use heph::actor::context::ThreadSafe;
    use heph::net::TcpStream;
    use heph::rt::RuntimeAccess;
    use heph::timer::{Deadline, Timer};
    use heph::{actor, ActorRef};
    use log::{debug, error, info, trace, warn};

    use crate::error::Describe;
    use crate::op::store::Success;
    use crate::peer::coordinator::server::{BLOB_LENGTH_LEN, NO_BLOB};
    use crate::peer::participant::RpcResponder;
    use crate::peer::{ConsensusVote, COORDINATOR_MAGIC};
    use crate::{db, op, Buffer, Key};

    /// Timeout used for I/O between peers.
    const IO_TIMEOUT: Duration = Duration::from_secs(2);

    /// Timeout used for waiting for the result of the census (in each phase).
    const RESULT_TIMEOUT: Duration = Duration::from_secs(5);

    /// Request to start a consensus algorithm.
    #[derive(Debug)]
    pub struct Request {
        /// The blob to operate on.
        pub(super) key: Key,
        /// The address of the coordinator peer, e.g. used to retrieve the blob
        /// to store.
        pub(super) remote: SocketAddr,
    }

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

    /// Actor that runs the consensus algorithm for storing a blob.
    pub async fn store_blob_actor(
        mut ctx: actor::Context<VoteResult, ThreadSafe>,
        request: Request,
        mut responder: RpcResponder,
        mut db_ref: ActorRef<db::Message>,
    ) -> crate::Result<()> {
        debug!(
            "store blob consensus actor started: key={}, remote={}",
            request.key, request.remote
        );

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

        trace!("writing connection magic");
        match Deadline::timeout(&mut ctx, IO_TIMEOUT, stream.write_all(COORDINATOR_MAGIC)).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(err.describe("writing connection magic")),
            Err(err) => {
                return Err(io::Error::from(err).describe("timeout writing connection magic"))
            }
        }

        let read_blob = read_blob(&mut ctx, &mut stream, &mut buf, &request.key);
        let blob_len = match read_blob.await {
            Ok(Some((blob_key, blob))) if request.key == blob_key => blob.len(),
            Ok(Some((blob_key, ..))) => {
                error!(
                    "coordinator server responded with incorrect blob, voting to abort consensus: want_key={}, got_blob_key={}",
                    request.key, blob_key
                );
                responder.respond(ConsensusVote::Abort);
                return Ok(());
            }
            Ok(None) => {
                error!(
                    "couldn't get blob from coordinator server, voting to abort consensus: key={}",
                    request.key
                );
                responder.respond(ConsensusVote::Abort);
                return Ok(());
            }
            Err(err) => return Err(err),
        };

        // Phase one: storing the blob, readying it to be added to the database.
        let query = match op::store::add_blob(&mut ctx, &mut db_ref, &mut buf, blob_len).await {
            Ok(Success::Continue(query)) => {
                responder.respond(ConsensusVote::Commit(SystemTime::now()));
                query
            }
            Ok(Success::Done(..)) => {
                info!(
                    "blob already stored, voting to abort consensus: key={}",
                    request.key
                );
                responder.respond(ConsensusVote::Abort);
                return Ok(());
            }
            Err(()) => {
                warn!(
                    "failed to add blob to storage, voting to abort consensus: key={}",
                    request.key
                );
                responder.respond(ConsensusVote::Abort);
                return Ok(());
            }
        };

        // Phase two: commit or abort the query.
        let recv_msg = Timer::timeout(&mut ctx, RESULT_TIMEOUT).wrap(ctx.receive_next());
        let vote_result = match recv_msg.await {
            Ok(vote_result) => {
                // Update the `RpcResponder.id` to ensure we're responding to
                // the correct request.
                responder.id = vote_result.request_id;
                vote_result
            }
            Err(err) => {
                return Err(io::Error::from(err).describe("timeout waiting for consensus result"))
            }
        };

        let response = if request.key.ne(query.key()) {
            error!(
                "received an incorrect key in consensus run: want_key={}, consensus_key={}",
                request.key,
                query.key()
            );
            Err(())
        } else {
            match vote_result.result {
                ConsensusVote::Commit(timestamp) => {
                    op::store::commit(&mut ctx, &mut db_ref, query, timestamp).await
                }
                ConsensusVote::Abort | ConsensusVote::Fail => {
                    op::store::abort(&mut ctx, &mut db_ref, query).await
                }
            }
        };
        let response = match response {
            Ok(()) => ConsensusVote::Commit(SystemTime::now()),
            Err(()) => ConsensusVote::Abort,
        };
        responder.respond(response);
        Ok(())
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

        // Write the request for the key.
        match Deadline::timeout(ctx, IO_TIMEOUT, stream.write_all(request_key.as_bytes())).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(err.describe("writing request key")),
            Err(err) => return Err(io::Error::from(err).describe("timeout writing request key")),
        }

        // Don't want to include the length of the blob in the key calculation.
        let mut calc = Key::calculator_skip(stream, BLOB_LENGTH_LEN);

        // Read at least the length of the blob.
        let f = Deadline::timeout(ctx, IO_TIMEOUT, buf.read_n_from(&mut calc, BLOB_LENGTH_LEN));
        match f.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(err.describe("reading blob length")),
            Err(err) => return Err(io::Error::from(err).describe("timeout reading blob length")),
        }

        // Safety: we just read at least `BLOB_LENGTH_LEN` bytes, so this won't
        // panic.
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
                Ok(Ok(())) => {}
                Ok(Err(err)) => return Err(err.describe("reading blob")),
                Err(err) => return Err(io::Error::from(err).describe("timeout reading blob")),
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

    /// Not implemented.
    pub async fn remove_blob_actor(
        _ctx: actor::Context<VoteResult, ThreadSafe>,
        _request: Request,
        _responder: RpcResponder,
        _db_ref: ActorRef<db::Message>,
    ) -> crate::Result<()> {
        // FIXME: implement this.
        todo!("remove blob")
    }
}
