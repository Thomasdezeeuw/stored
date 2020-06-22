//! Module with the type related to peer interaction.

use std::fmt;
use std::future::Future;
use std::mem::replace;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::task::{self, Poll};
use std::time::{Duration, SystemTime};

use heph::actor::context::{ThreadLocal, ThreadSafe};
use heph::actor::{self, NewActor};
use heph::actor_ref::{ActorRef, NoResponse, Rpc, RpcMessage, SendError};
use heph::net::tcp;
use heph::rt::options::{ActorOptions, Priority};
use heph::supervisor::RestartSupervisor;
use heph::timer::Timer;
use heph::Runtime;
use log::{debug, warn};
use serde::{Deserialize, Serialize};

use crate::error::Describe;
use crate::{config, db, Key};

pub mod coordinator;
pub mod participant;

#[cfg(test)]
mod tests;

use coordinator::relay;

/// Start the all actors related to peer interaction.
pub fn start(
    runtime: &mut Runtime,
    config: config::Distributed,
    db_ref: ActorRef<db::Message>,
) -> crate::Result<Peers> {
    match config.replicas {
        config::Replicas::All => {}
        _ => unimplemented!("currently only replicas = 'all' is supported"),
    }

    let peers = Peers::empty();

    start_listener(
        runtime,
        db_ref,
        config.peer_address,
        peers.clone(),
        config.peer_address,
    )?;
    start_relays(runtime, config.peers.0, config.peer_address, peers.clone());

    // TODO: sync already stored blobs.

    Ok(peers)
}

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
    debug!("starting peer listener: address={}", address);

    let switcher = (switcher::actor as fn(_, _, _, _, _, _) -> _)
        .map_arg(move |(stream, remote)| (stream, remote, peers.clone(), db_ref.clone(), server));
    let server_actor = tcp::Server::setup(
        address,
        switcher::supervisor,
        switcher,
        ActorOptions::default(),
    )
    .map_err(|err| err.describe("creating new tcp::Server"))?;
    let supervisor = RestartSupervisor::new("consensus listener", ());
    let options = ActorOptions::default().with_priority(Priority::HIGH);
    let _ = runtime
        .try_spawn(supervisor, server_actor, (), options)
        .map_err(|err| err.describe("spawning peer server"))?;
    // FIXME: receive signals.
    //runtime.receive_signals(server_ref.try_map());

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
            "starting relay actor for peer: remote_address={}",
            peer_address
        );
        let args = (peer_address, peers.clone(), server);
        let supervisor = RestartSupervisor::new("coordinator::relay", args.clone());
        let relay = coordinator::relay::actor as fn(_, _, _, _) -> _;
        let options = ActorOptions::default()
            .with_priority(Priority::HIGH)
            .mark_ready();
        let relay_ref = runtime.spawn(supervisor, relay, args, options);

        peers.add(Peer {
            actor_ref: relay_ref,
            address: peer_address,
        });
    }
}

/// Id of a consensus algorithm run.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[repr(transparent)]
pub struct ConsensusId(usize);

impl fmt::Display for ConsensusId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

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
    /// Remove the blob.
    RemoveBlob,
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
}

#[derive(Debug, Clone)]
struct Peer {
    actor_ref: ActorRef<relay::Message>,
    address: SocketAddr,
}

impl Peers {
    /// Create an empty collection of peers.
    pub fn empty() -> Peers {
        Peers {
            inner: Arc::new(PeersInner {
                peers: RwLock::new(Vec::new()),
                consensus_id: AtomicUsize::new(0),
            }),
        }
    }

    /// Returns `true` if the collection is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.peers.read().unwrap().is_empty()
    }

    /// Start a consensus algorithm to add blob with `key`.
    pub fn add_blob<M>(
        &self,
        ctx: &mut actor::Context<M, ThreadLocal>,
        key: Key,
    ) -> (ConsensusId, PeerRpc<SystemTime>) {
        let id = self.new_consensus_id();
        let rpc = self.rpc(ctx, id, coordinator::relay::AddBlob(key));
        (id, rpc)
    }

    /// Commit to add blob.
    pub fn commit_to_add_blob<M>(
        &self,
        ctx: &mut actor::Context<M, ThreadLocal>,
        id: ConsensusId,
        key: Key,
        timestamp: SystemTime,
    ) -> PeerRpc<()> {
        self.rpc(ctx, id, coordinator::relay::CommitStoreBlob(key, timestamp))
    }

    /*
    pub fn remove_blob<M>(
        &self,
        ctx: &mut actor::Context<M, ThreadLocal>,
        key: Key,
    ) -> (ConsensusId, PeerRpc<SystemTime>) {
        let id = self.new_consensus_id();
        let self.rpc(coordinator::RemoveBlob(key));
        (id, rpc)
    }
    */

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
            .unwrap()
            .iter()
            .map(|peer| {
                let status = match peer.actor_ref.rpc(ctx, (id, request.clone())) {
                    Ok(rpc) => RpcStatus::InProgress(rpc),
                    Err(SendError) => {
                        warn!(
                            "failed to send message to peer relay: address={}",
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

    fn new_consensus_id(&self) -> ConsensusId {
        // This wraps around, which is OK.
        ConsensusId(self.inner.consensus_id.fetch_add(1, Ordering::AcqRel))
    }

    /// Get a private copy of all known peer addresses.
    fn addresses(&self) -> Vec<SocketAddr> {
        self.inner
            .peers
            .read()
            .unwrap()
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
            "starting relay actor for peer: remote_address={}",
            peer_address
        );
        let args = (peer_address, self.clone(), server);
        let supervisor = RestartSupervisor::new("coordinator::relay", args.clone());
        let relay = coordinator::relay::actor as fn(_, _, _, _) -> _;
        let options = ActorOptions::default()
            .with_priority(Priority::HIGH)
            .mark_ready();
        let relay_ref = ctx.spawn(supervisor, relay, args, options);
        self.add(Peer {
            actor_ref: relay_ref,
            address: peer_address,
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
        let mut peers = self.inner.peers.write().unwrap();
        if !known_peer(&peers, &peer.address) {
            peers.push(peer);
        } else {
            // TODO: kill the relay?
        }
    }

    /// Returns `true` if a peer at `address` is already in the collection.
    fn known(&self, address: &SocketAddr) -> bool {
        known_peer(&*self.inner.peers.read().unwrap(), address)
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
struct SinglePeerRpc<Res> {
    status: RpcStatus<Res>,
    address: SocketAddr,
}

impl<Res> SinglePeerRpc<Res> {
    fn is_done(&self) -> bool {
        matches!(self.status, RpcStatus::Done(..))
    }
}

/// Status of a [`Rpc`] future in [`PeerRpc`].
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
            warn!(
                "peer RPC timed out: timed_out_peers={}",
                self.timeout_peers()
            );
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
        let iter = self
            .peers
            .rpcs
            .iter()
            .filter_map(|peer| peer.is_done().then_some(peer.address));
        f.debug_list().entries(iter).finish()
    }
}

// Magic bytes indicate the kind of connection.
const COORDINATOR_MAGIC: &[u8] = b"Stored coordinate\0";
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
    use heph::{actor, ActorRef, SupervisorStrategy};
    use log::{debug, warn};

    use crate::error::Describe;
    use crate::peer::{
        coordinator, participant, Peers, Response, COORDINATOR_MAGIC, MAGIC_LENGTH,
        PARTICIPANT_MAGIC,
    };
    use crate::{db, Buffer};

    const MAGIC_ERROR_MSG: &[u8] = b"incorrect connection magic";

    const TIMEOUT: Duration = Duration::from_secs(5);

    /// Supervisor for [`actor`].
    pub fn supervisor<Args>(err: crate::Error) -> SupervisorStrategy<Args> {
        warn!("switcher actor failed: {}", err);
        SupervisorStrategy::Stop
    }

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
    ) -> crate::Result<()> {
        debug!("accepted peer connection: remote_address={}", remote);
        // TODO: 8k is a bit large for `coordinator::server`, only needs to read
        // 1 key at a time.
        let mut buf = Buffer::new();

        let read_n = buf.read_n_from(&mut stream, MAGIC_LENGTH);
        match Deadline::timeout(&mut ctx, TIMEOUT, read_n).await {
            Ok(Ok(())) => {}
            Ok(Err(ref err)) if err.kind() == io::ErrorKind::UnexpectedEof => {
                warn!(
                    "closing connection: missing connection magic: remote_address={}",
                    remote
                );
                // We don't really care if we didn't write all the bytes here,
                // the connection is invalid anyway.
                return stream
                    .write(MAGIC_ERROR_MSG)
                    .await
                    .map(|_| ())
                    .map_err(|err| err.describe("writing error response"));
            }
            Ok(Err(err)) => return Err(err.describe("reading connection magic")),
            Err(err) => {
                return Err(io::Error::from(err).describe("timeout reading connection magic"))
            }
        }

        match &buf.as_bytes()[..MAGIC_LENGTH] {
            COORDINATOR_MAGIC => {
                buf.processed(MAGIC_LENGTH);
                // Don't log the error as a problem in the switch actor.
                if let Err(err) = coordinator::server::actor(ctx, stream, buf, db_ref).await {
                    warn!("coordinator server failed: {}", err);
                }
                Ok(())
            }
            PARTICIPANT_MAGIC => {
                buf.processed(MAGIC_LENGTH);
                // Don't log the error as a problem in the switch actor.
                let res =
                    participant::dispatcher::actor(ctx, stream, buf, peers, db_ref, server).await;
                if let Err(err) = res {
                    warn!("participant dispatcher failed: {}", err);
                }
                Ok(())
            }
            _ => {
                warn!(
                    "closing connection: incorrect connection magic: remote_address={}",
                    remote
                );
                // We don't really care if we didn't write all the bytes here,
                // the connection is invalid anyway.
                stream
                    .write(MAGIC_ERROR_MSG)
                    .await
                    .map(|_| ())
                    .map_err(|err| err.describe("writing error response"))
            }
        }
    }
}
