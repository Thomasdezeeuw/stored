//! Types, functions and actors related to peer interaction.

use std::fmt;
use std::future::Future;
use std::mem::replace;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{self, Poll};
use std::time::{Duration, SystemTime};

use heph::actor::context::{ThreadLocal, ThreadSafe};
use heph::actor::messages::Start;
use heph::actor::{self, NewActor};
use heph::actor_ref::{ActorGroup, ActorRef, NoResponse, Rpc, RpcMessage, SendError};
use heph::net::tcp;
use heph::rt::options::{ActorOptions, Priority};
use heph::supervisor::NoSupervisor;
use heph::timer::Timer;
use heph::{restart_supervisor, Runtime};
use log::{debug, warn};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::error::Describe;
use crate::storage::{RemoveBlob, StoreBlob};
use crate::util::CountDownLatch;
use crate::{config, db, Key};

// TODO: Run syncs after we disconnected from a peer.

pub mod coordinator;
pub mod participant;
pub mod server;
pub mod sync;

#[cfg(test)]
mod tests;

use coordinator::relay;
use participant::consensus;

/// Start the all actors related to peer interaction.
pub fn start(
    runtime: &mut Runtime,
    config: config::Distributed,
    db_ref: ActorRef<db::Message>,
    start_http_ref: Arc<RwLock<ActorGroup<Start>>>,
    start_sync: Arc<CountDownLatch>,
) -> crate::Result<Peers> {
    match config.replicas {
        config::Replicas::All => {}
        _ => unimplemented!("currently only replicas = 'all' is supported"),
    }

    let relay = consensus::actor as fn(_, _) -> _;
    let participant_ref =
        runtime.spawn(NoSupervisor, relay, db_ref.clone(), ActorOptions::default());
    let peers = Peers::new(participant_ref, start_sync.clone());

    start_listener(
        runtime,
        db_ref.clone(),
        config.peer_address,
        peers.clone(),
        config.peer_address,
    )?;
    start_relays(runtime, config.peers.0, config.peer_address, peers.clone());

    // NOTE: the synchronisation actor only starts once all peers are connected
    // and all worker threads are started, as defined by the `start_sync` latch.
    start_sync_actor(runtime, db_ref, start_http_ref, start_sync, peers.clone());

    Ok(peers)
}

restart_supervisor!(ListenerSupervisor, "peer listener", (), 2);

/// Start the peer listener, listening for incoming TCP connections from its
/// peers starting a new [switcher actor] for each connection.
///
/// [switcher actor]: switcher::actor
fn start_listener(
    runtime: &mut Runtime,
    db_ref: ActorRef<db::Message>,
    address: SocketAddr,
    peers: Peers,
    server: SocketAddr,
) -> crate::Result<()> {
    debug!("starting peer listener: address=\"{}\"", address);

    let switcher = (switcher::actor as fn(_, _, _, _, _, _) -> _)
        .map_arg(move |(stream, remote)| (stream, remote, peers.clone(), db_ref.clone(), server));
    let server_actor = tcp::Server::setup(address, NoSupervisor, switcher, ActorOptions::default())
        .map_err(|err| err.describe("creating peer listener"))?;
    let supervisor = ListenerSupervisor::new(());
    let options = ActorOptions::default().with_priority(Priority::HIGH);
    let server_ref = runtime
        .try_spawn(supervisor, server_actor, (), options)
        .map_err(|err| err.describe("spawning peer server"))?;
    runtime.receive_signals(server_ref.try_map());

    Ok(())
}

/// Start a [`coordinator::relay`] actor for each address in `peer_addresses`.
fn start_relays(
    runtime: &mut Runtime,
    mut peer_addresses: Vec<SocketAddr>,
    server: SocketAddr,
    peers: Peers,
) {
    peer_addresses.sort_unstable();
    peer_addresses.dedup();

    for peer_address in peer_addresses {
        if peer_address == server {
            // Don't want to add ourselves.
            continue;
        }

        debug!(
            "starting relay actor for peer: remote_address=\"{}\"",
            peer_address
        );
        let args = (peer_address, peers.clone(), server);
        let supervisor = coordinator::relay::Supervisor::new(peer_address, peers.clone(), server);
        let relay = coordinator::relay::actor as fn(_, _, _, _) -> _;
        let options = ActorOptions::default()
            .with_priority(Priority::HIGH)
            .mark_ready();
        let relay_ref = runtime.spawn(supervisor, relay, args, options);

        peers.add(Peer {
            actor_ref: relay_ref,
            address: peer_address,
            is_connected: false,
        });
    }
}

/// Spawn the peer synchronisation actor.
fn start_sync_actor(
    runtime: &mut Runtime,
    db_ref: ActorRef<db::Message>,
    start_http_ref: Arc<RwLock<ActorGroup<Start>>>,
    start: Arc<CountDownLatch>,
    peers: Peers,
) {
    debug!("spawning peer synchronisation actor");
    let actor = sync::actor as fn(_, _, _, _, _) -> _;
    let args = (db_ref, start_http_ref, start, peers);
    let options = ActorOptions::default()
        .with_priority(Priority::HIGH)
        .mark_ready();
    runtime.spawn(NoSupervisor, actor, args, options);
}

/// Exit message send by coordinator for a clean shutdown.
const EXIT_COORDINATOR: &[u8] = b"EXIT COORDINATOR";
/// Exit message send by participant for a clean shutdown.
const EXIT_PARTICIPANT: &[u8] = b"EXIT PARTICIPANT";

/// Id of a consensus algorithm run.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[repr(transparent)]
pub struct ConsensusId(usize);

impl fmt::Display for ConsensusId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Consensus id used to mark a message as meant for participant consensus.
pub const PARTICIPANT_CONSENSUS_ID: ConsensusId = ConsensusId(usize::max_value());

/// Request message send from [`coordinator::relay`] to
/// [`participant::dispatcher`].
#[derive(Debug, Deserialize, Serialize)]
pub struct Request {
    /// Unique id for this request.
    pub id: usize,
    /// Id of the consensus run (there may be multiple requests per consensus
    /// run).
    pub consensus_id: ConsensusId,
    /// The key to operate on.
    pub key: Key,
    /// The kind of operation.
    pub op: Operation,
}

/// Kind of operation to execute.
#[derive(Debug, Deserialize, Serialize)]
pub enum Operation {
    /// Add the blob (phase one in storing it).
    // TODO: provide the length so we can pre-allocate a buffer at the
    // participant?
    AddBlob,
    /// Commit to storing the blob.
    CommitStoreBlob(SystemTime),
    /// Abort storing the blob.
    AbortStoreBlob,
    /// Coordinator committed to storing the blob.
    StoreCommitted(SystemTime),

    /// Remove the blob (phase one in removing it).
    RemoveBlob,
    /// Commit to removing the blob.
    CommitRemoveBlob(SystemTime),
    /// Abort removing the blob.
    AbortRemoveBlob,
    /// Coordinator committed to removing the blob.
    RemoveCommitted(SystemTime),
}

/// Response message send from [`participant::dispatcher`] to
/// [`coordinator::relay`].
#[derive(Debug, Deserialize, Serialize)]
pub struct Response {
    /// Response to request with this id.
    pub request_id: usize,
    /// The vote of the peer.
    pub vote: ConsensusVote,
}

/// How the peer voted.
#[derive(Debug, Deserialize, Serialize)]
pub enum ConsensusVote {
    /// Vote to commit to the consensus algorithm.
    Commit(SystemTime),
    /// Vote to abort the consensus algorithm.
    Abort,
    /// Something when wrong, effectively a vote to abort not a direct one.
    ///
    /// This can happen when for example the [`participant::consensus`] actor
    /// fails.
    Fail,
}

// TODO: replace `Peers` with `ActorGroup`.
//
// TODO: remove disconnected/failed peers from `Peers`.

/// Collection of connections peers.
#[derive(Clone)]
pub struct Peers {
    inner: Arc<PeersInner>,
}

struct PeersInner {
    peers: RwLock<Vec<Peer>>,
    /// Unique id for each consensus run.
    consensus_id: AtomicUsize,
    /// Count down latch that gets decreased once a (previously unconnected)
    /// peer is connected.
    peers_connected: Option<Arc<CountDownLatch>>,
    /// Actor reference to the [participant consensus actor].
    ///
    /// [participant consensus actor]: consensus::actor
    participant_ref: Option<ActorRef<consensus::Message>>,
}

#[derive(Debug, Clone)]
struct Peer {
    actor_ref: ActorRef<relay::Message>,
    address: SocketAddr,
    is_connected: bool,
}

impl Peers {
    /// Create an empty collection of peers, can't grow.
    pub fn empty() -> Peers {
        Peers {
            inner: Arc::new(PeersInner {
                peers: RwLock::new(Vec::new()),
                consensus_id: AtomicUsize::new(0),
                peers_connected: None,
                participant_ref: None,
            }),
        }
    }

    /// Create a new peer collections.
    pub fn new(
        participant_ref: ActorRef<consensus::Message>,
        peers_connected: Arc<CountDownLatch>,
    ) -> Peers {
        Peers {
            inner: Arc::new(PeersInner {
                peers: RwLock::new(Vec::new()),
                consensus_id: AtomicUsize::new(0),
                peers_connected: Some(peers_connected),
                participant_ref: Some(participant_ref),
            }),
        }
    }

    /// Returns `true` if the collection is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.peers.read().is_empty()
    }

    /// Start a consensus algorithm to store blob with `key`.
    pub fn add_blob<M>(
        &self,
        ctx: &mut actor::Context<M, ThreadLocal>,
        key: Key,
    ) -> (ConsensusId, PeerRpc<SystemTime>) {
        let id = self.new_consensus_id();
        let rpc = self.rpc(ctx, id, coordinator::relay::AddBlob(key));
        (id, rpc)
    }

    /// Commit to storing a blob.
    pub fn commit_to_store_blob<M>(
        &self,
        ctx: &mut actor::Context<M, ThreadLocal>,
        id: ConsensusId,
        key: Key,
        timestamp: SystemTime,
    ) -> PeerRpc<()> {
        self.rpc(ctx, id, coordinator::relay::CommitStoreBlob(key, timestamp))
    }

    /// Abort storing a blob.
    pub fn abort_store_blob<M>(
        &self,
        ctx: &mut actor::Context<M, ThreadLocal>,
        id: ConsensusId,
        key: Key,
    ) -> PeerRpc<()> {
        self.rpc(ctx, id, coordinator::relay::AbortStoreBlob(key))
    }

    /// Committed to storing a blob.
    pub fn committed_store_blob(&self, id: ConsensusId, key: Key, timestamp: SystemTime) {
        self.send(coordinator::relay::CoordinatorCommittedStore(
            id, key, timestamp,
        ))
    }

    /// Start a consensus algorithm to remove blob with `key`.
    pub fn remove_blob<M>(
        &self,
        ctx: &mut actor::Context<M, ThreadLocal>,
        key: Key,
    ) -> (ConsensusId, PeerRpc<SystemTime>) {
        let id = self.new_consensus_id();
        let rpc = self.rpc(ctx, id, coordinator::relay::RemoveBlob(key));
        (id, rpc)
    }

    /// Commit to removing a blob.
    pub fn commit_to_remove_blob<M>(
        &self,
        ctx: &mut actor::Context<M, ThreadLocal>,
        id: ConsensusId,
        key: Key,
        timestamp: SystemTime,
    ) -> PeerRpc<()> {
        self.rpc(
            ctx,
            id,
            coordinator::relay::CommitRemoveBlob(key, timestamp),
        )
    }

    /// Abort removing a blob.
    pub fn abort_remove_blob<M>(
        &self,
        ctx: &mut actor::Context<M, ThreadLocal>,
        id: ConsensusId,
        key: Key,
    ) -> PeerRpc<()> {
        self.rpc(ctx, id, coordinator::relay::AbortRemoveBlob(key))
    }

    /// Committed to removing a blob.
    pub fn committed_remove_blob(&self, id: ConsensusId, key: Key, timestamp: SystemTime) {
        self.send(coordinator::relay::CoordinatorCommittedRemove(
            id, key, timestamp,
        ))
    }

    /// Share with the peers to commit to storing the blob with `key` at
    /// `timestamp`.
    pub fn share_stored_commitment(&self, key: Key, timestamp: SystemTime) {
        self.send(coordinator::relay::ShareCommitmentStored(key, timestamp))
    }

    /// Same as `share_stored_commitment` but for a remove query.
    pub fn share_removed_commitment(&self, key: Key, timestamp: SystemTime) {
        self.send(coordinator::relay::ShareCommitmentRemoved(key, timestamp))
    }

    /// 2PC participant is uncommitted the `query` and the coordinator timed
    /// out. Send it to the participant consensus actor.
    pub fn uncommitted_stored(&self, query: StoreBlob) {
        self.send_participant_consensus(consensus::Message::UncommittedStore(query))
    }

    /// Same as `uncommitted_stored`, but with a remove query.
    pub fn uncommitted_removed(&self, query: RemoveBlob) {
        self.send_participant_consensus(consensus::Message::UncommittedRemove(query))
    }

    /// Send a message to the [participant consensus actor].
    ///
    /// [participant consensus actor]: consensus::actor
    pub(crate) fn send_participant_consensus(&self, msg: consensus::Message) {
        match self.inner.participant_ref.as_ref() {
            Some(participant_ref) if participant_ref.send(msg).is_ok() => {}
            _ => warn!("failed to send message to participant consensus actor"),
        }
    }

    /// Rpc with all peers in the collection.
    fn rpc<M, Req, Res>(
        &self,
        ctx: &mut actor::Context<M, ThreadLocal>,
        id: ConsensusId,
        request: Req,
    ) -> PeerRpc<Res>
    where
        relay::Message: From<RpcMessage<(ConsensusId, Req), Result<Res, relay::Error>>>,
        Req: Clone,
    {
        let rpcs = self
            .inner
            .peers
            .read()
            .iter()
            .map(|peer| {
                let status = match peer.actor_ref.rpc(ctx, (id, request.clone())) {
                    Ok(rpc) => RpcStatus::InProgress(rpc),
                    Err(SendError) => {
                        warn!(
                            "failed to send message to peer relay: address=\"{}\"",
                            peer.address
                        );
                        RpcStatus::Done(Err(relay::Error::Failed))
                    }
                };
                SinglePeerRpc {
                    status,
                    address: peer.address,
                }
            })
            .collect();
        let timer = Timer::timeout(ctx, PEER_TIMEOUT);
        PeerRpc { rpcs, timer }
    }

    /// Send a message to all peers in the collection.
    fn send<Req>(&self, request: Req)
    where
        relay::Message: From<Req>,
        Req: Clone,
    {
        for peer in self.inner.peers.read().iter() {
            if let Err(err) = peer.actor_ref.send(request.clone()) {
                warn!(
                    "failed to send message to peer relay: {}: address=\"{}\"",
                    err, peer.address
                );
            }
        }
    }

    fn new_consensus_id(&self) -> ConsensusId {
        // This wraps around, which is OK.
        ConsensusId(self.inner.consensus_id.fetch_add(1, Ordering::AcqRel))
    }

    /// Get a private copy of all currently known peer addresses.
    pub fn addresses(&self) -> Vec<SocketAddr> {
        self.inner
            .peers
            .read()
            .iter()
            .map(|peer| peer.address)
            .collect()
    }

    /// Attempts to spawn a [`coordinator::relay`] actor for `peer_address`..
    fn spawn<M>(
        &self,
        ctx: &mut actor::Context<M, ThreadSafe>,
        peer_address: SocketAddr,
        server: SocketAddr,
    ) {
        // If we get the known peers addresses from a peer it could be that we
        // (this node) it a known peer, but we don't want to add ourselves to
        // this list.
        if peer_address == server {
            return;
        }

        // NOTE: we don't lock while spawning
        if self.known(&peer_address) {
            return;
        }

        // TODO: DRY with code in `start_relays`: need a trait for spawning
        // in Heph.
        debug!(
            "starting relay actor for peer: remote_address=\"{}\"",
            peer_address
        );
        let args = (peer_address, self.clone(), server);
        let supervisor = coordinator::relay::Supervisor::new(peer_address, self.clone(), server);
        let relay = coordinator::relay::actor as fn(_, _, _, _) -> _;
        let options = ActorOptions::default()
            .with_priority(Priority::HIGH)
            .mark_ready();
        let relay_ref = ctx.spawn(supervisor, relay, args, options);
        self.add(Peer {
            actor_ref: relay_ref,
            address: peer_address,
            is_connected: false,
        });
    }

    /// Calls [`spawn`] for each address in `peer_addresses`.
    ///
    /// [`spawn`]: Peers::spawn
    fn spawn_many<M>(
        &self,
        ctx: &mut actor::Context<M, ThreadSafe>,
        peer_addresses: &[SocketAddr],
        server: SocketAddr,
    ) {
        // NOTE: we don't lock the entire collection up front here because we
        // don't want to hold the lock for too long.
        for peer_address in peer_addresses.iter().copied() {
            self.spawn(ctx, peer_address, server);
        }
    }

    /// Attempts to add `peer` to the collection.
    fn add(&self, peer: Peer) {
        let mut peers = self.inner.peers.write();
        if !known_peer(&peers, &peer.address) {
            if let Some(peers_connected) = self.inner.peers_connected.as_ref() {
                // Wait until this peer is connected.
                peers_connected.increase();
            }
            peers.push(peer);
        } else {
            // TODO: kill the relay?
        }
    }

    /// Remove the `address` from the known peers, e.g. when the actor failed
    /// too many times and doesn't get restarted anymore.
    ///
    /// # Notes
    ///
    /// Silently ignores the error if no peer was known with `address`.
    fn remove(&self, address: &SocketAddr) {
        let mut peers = self.inner.peers.write();
        if let Some(pos) = peers.iter().position(|peer| peer.address == *address) {
            if let Some(peers_connected) = self.inner.peers_connected.as_ref() {
                peers_connected.decrease();
            }

            peers.remove(pos);
        }
    }

    /// Returns `true` if a peer at `address` is already in the collection.
    fn known(&self, address: &SocketAddr) -> bool {
        known_peer(&*self.inner.peers.read(), address)
    }

    /// Mark the peer with `address` as connected.
    fn connected(&self, address: &SocketAddr) {
        let mut peers = self.inner.peers.write();
        let peer = peers.iter_mut().find(|peer| peer.address == *address);

        // TODO: deal with `peer = None`.
        if let Some(peer) = peer {
            if !peer.is_connected {
                peer.is_connected = true;

                if let Some(peers_connected) = self.inner.peers_connected.as_ref() {
                    peers_connected.decrease();
                }
            }
        }
    }

    /// Returns `true` if all peers connected.
    pub(crate) fn all_connected(&self) -> bool {
        self.inner.peers.read().iter().all(|peer| peer.is_connected)
    }
}

impl fmt::Debug for Peers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Peers")
            .field("peers", &self.inner.peers)
            .field("consensus_id", &self.inner.consensus_id)
            .finish()
    }
}

fn known_peer(peers: &[Peer], address: &SocketAddr) -> bool {
    peers.iter().any(|peer| peer.address == *address)
}

const PEER_TIMEOUT: Duration = Duration::from_secs(5);

/// [`Future`] implementation that makes RPCs with one or more peers, see
/// [`Peers`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct PeerRpc<Res> {
    rpcs: Box<[SinglePeerRpc<Res>]>,
    timer: Timer,
}
impl<Res> PeerRpc<Res> {
    fn timeout_peers(&self) -> DisplayTimedoutPeers<Res> {
        DisplayTimedoutPeers { peers: self }
    }
}

/// `PeerRpc` data for a single peer.
// TODO: better name.
#[derive(Debug)]
struct SinglePeerRpc<Res> {
    status: RpcStatus<Res>,
    address: SocketAddr,
}

impl<Res> SinglePeerRpc<Res> {
    /// Returns `true` if the status is `InProgress`.
    fn in_progress(&self) -> bool {
        matches!(self.status, RpcStatus::InProgress(..))
    }
}

/// Status of a [`Rpc`] future in [`PeerRpc`].
#[derive(Debug)]
enum RpcStatus<Res> {
    /// Future is in progress.
    InProgress(Rpc<Result<Res, relay::Error>>),
    /// Future has returned a result.
    Done(Result<Res, relay::Error>),
    /// Future has returned and its result has been taken.
    Completed,
}

impl<Res> Future for PeerRpc<Res> {
    type Output = Vec<Result<Res, relay::Error>>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut task::Context<'_>) -> Poll<Self::Output> {
        // Check all the statuses.
        let mut has_pending = false;
        for peer_rpc in self.rpcs.iter_mut() {
            if let RpcStatus::InProgress(rpc) = &mut peer_rpc.status {
                match Pin::new(rpc).poll(ctx) {
                    Poll::Ready(result) => {
                        // Map no response to a failed response.
                        let result = match result {
                            Ok(result) => result,
                            Err(NoResponse) => Err(relay::Error::Failed),
                        };
                        drop(replace(&mut peer_rpc.status, RpcStatus::Done(result)));
                    }
                    Poll::Pending => has_pending = true,
                }
            }
        }

        // If not all futures have returned and the timer hasn't passed we'll
        // try again later.
        if has_pending && !self.timer.has_passed() {
            return Poll::Pending;
        } else if has_pending {
            warn!("peer RPC timed out: failed_peers={}", self.timeout_peers());
        }

        // If we reached this point all statuses should be `Done`.
        let results = self
            .rpcs
            .iter_mut()
            .map(|peer_rpc| {
                match replace(&mut peer_rpc.status, RpcStatus::Completed) {
                    RpcStatus::Done(result) => result,
                    // If the peer hasn't responded yet and we're here it means the
                    // timeout has passed, we'll consider there vote as failed.
                    _ => Err(relay::Error::Failed),
                }
            })
            .collect();
        Poll::Ready(results)
    }
}

struct DisplayTimedoutPeers<'a, Res> {
    peers: &'a PeerRpc<Res>,
}

impl<'a, Res> fmt::Display for DisplayTimedoutPeers<'a, Res> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut iter = self
            .peers
            .rpcs
            .iter()
            .filter_map(|peer| peer.in_progress().then_some(peer.address));
        f.write_str("[")?;
        if let Some(address) = iter.next() {
            address.fmt(f)?;
        }
        for address in iter {
            f.write_str(", ")?;
            address.fmt(f)?;
        }
        f.write_str("]")
    }
}

// Magic bytes indicate the kind of connection.
pub const COORDINATOR_MAGIC: &[u8] = b"Stored coordinate\0";
const PARTICIPANT_MAGIC: &[u8] = b"Stored participate";
const MAGIC_LENGTH: usize = 18;

pub mod switcher {
    //! Module with the [switcher actor].
    //!
    //! [switcher actor]: actor()

    use std::io;
    use std::net::SocketAddr;
    use std::time::Duration;

    use futures_util::io::AsyncWriteExt;
    use heph::actor::context::ThreadSafe;
    use heph::net::TcpStream;
    use heph::timer::Deadline;
    use heph::{actor, ActorRef};
    use log::{debug, warn};

    use crate::peer::{
        participant, server, Peers, Response, COORDINATOR_MAGIC, MAGIC_LENGTH, PARTICIPANT_MAGIC,
    };
    use crate::{db, Buffer};

    const MAGIC_ERROR_MSG: &[u8] = b"incorrect connection magic";

    const TIMEOUT: Duration = Duration::from_secs(5);

    /// Actor that acts as switcher between acting as a coordinator server or
    /// participant.
    pub async fn actor(
        // The `Response` type is a bit awkward, but required for
        // `participant::dispatcher`.
        mut ctx: actor::Context<Response, ThreadSafe>,
        mut stream: TcpStream,
        remote: SocketAddr,
        peers: Peers,
        db_ref: ActorRef<db::Message>,
        server: SocketAddr,
    ) -> Result<(), !> {
        debug!("accepted peer connection: remote_address=\"{}\"", remote);
        let mut buf = Buffer::new();

        let read_n = buf.read_n_from(&mut stream, MAGIC_LENGTH);
        match Deadline::timeout(&mut ctx, TIMEOUT, read_n).await {
            Ok(()) => {}
            Err(ref err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                warn!(
                    "closing connection: missing connection magic: remote_address=\"{}\", local_address=\"{}\"",
                    remote, server
                );
                // We don't really care if we didn't write all the bytes here,
                // the connection is invalid anyway.
                if let Err(err) = stream.write(MAGIC_ERROR_MSG).await {
                    warn!(
                        "error writing error response: {}: remote_address=\"{}\", local_address=\"{}\"",
                        err, remote, server
                    );
                }
                return Ok(());
            }
            Err(err) => {
                warn!(
                    "closing connection: error reading connection magic: {}: remote_address=\"{}\", local_address=\"{}\"",
                    err, remote, server
                );
                return Ok(());
            }
        }

        match &buf.as_bytes()[..MAGIC_LENGTH] {
            COORDINATOR_MAGIC => {
                buf.processed(MAGIC_LENGTH);
                server::run_actor(ctx, stream, buf, db_ref, server, remote).await;
                Ok(())
            }
            PARTICIPANT_MAGIC => {
                buf.processed(MAGIC_LENGTH);
                participant::dispatcher::run_actor(ctx, stream, buf, peers, db_ref, server, remote)
                    .await;
                Ok(())
            }
            _ => {
                warn!(
                    "closing connection: incorrect connection magic: remote_address=\"{}\", local_address=\"{}\"",
                    remote, server,
                );
                // We don't really care if we didn't write all the bytes here,
                // the connection is invalid anyway.
                if let Err(err) = stream.write(MAGIC_ERROR_MSG).await {
                    warn!(
                        "error writing error response: {}: remote_address=\"{}\", local_address=\"{}\"",
                        err, remote, server
                    );
                }
                Ok(())
            }
        }
    }
}
