//! Module with the type related to peer interaction.

use std::future::Future;
use std::mem::replace;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::task::{self, Poll};
use std::time::SystemTime;
use std::{fmt, io};

use futures_util::io::AsyncWriteExt;
use heph::actor::context::{ThreadLocal, ThreadSafe};
use heph::actor::{self, NewActor};
use heph::actor_ref::{ActorRef, NoResponse, Rpc, RpcMessage, SendError};
use heph::net::{tcp, TcpStream};
use heph::rt::options::{ActorOptions, Priority};
use heph::rt::{self, Runtime};
use heph::supervisor::{RestartSupervisor, SupervisorStrategy};
use log::{debug, warn};
use serde::{Deserialize, Serialize};

use crate::{config, db, Buffer, Key};

pub mod coordinator;
pub mod participant;

use coordinator::{RelayError, RelayMessage};

/// Start the all actors related to peer interaction.
pub fn start(
    runtime: &mut Runtime,
    config: config::Distributed,
    db_ref: ActorRef<db::Message>,
) -> Result<Peers, rt::Error<io::Error>> {
    match config.replicas {
        config::Replicas::Majority => {}
        _ => unimplemented!("currently only replicas = 'all' is supported"),
    }

    let peers = Peers::empty();

    // Setup consensus server.
    let peers2 = peers.clone();
    let switcher = (switcher as fn(_, _, _, _, _) -> _)
        .map_arg(move |(stream, address)| (stream, address, peers2.clone(), db_ref.clone()));
    let server_actor = tcp::Server::setup(
        config.peer_address,
        |err| {
            warn!("switcher actor failed: {}", err);
            SupervisorStrategy::Stop
        },
        switcher,
        ActorOptions::default(),
    )?;
    let supervisor = RestartSupervisor::new("consensus HTTP server", ());
    let options = ActorOptions::default().with_priority(Priority::HIGH);
    let _ = runtime.try_spawn(supervisor, server_actor, (), options)?;
    // FIXME: receive signals.
    //runtime.receive_signals(server_ref.try_map());

    for peer_address in config.peers.0 {
        let args = (peer_address, peers.clone(), config.peer_address);
        let supervisor = RestartSupervisor::new("coordinator::relay", args.clone());
        let relay = coordinator::relay as fn(_, _, _, _) -> _;
        let options = ActorOptions::default().with_priority(Priority::HIGH);
        let relay_ref = runtime.spawn(supervisor, relay, args, options);

        peers.add(Peer {
            actor_ref: relay_ref,
            address: peer_address,
        });
    }

    Ok(peers)
}

// TODO: replace `Peers` with `ActorGroup`.

/// Id of a consensus algorithm run.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[repr(transparent)]
pub struct ConsensusId(usize);

/// Collection of connections peers.
#[derive(Clone)]
pub struct Peers {
    inner: Arc<PeersInner>,
}

impl fmt::Debug for Peers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Peers")
            .field("peers", &self.inner.peers)
            .field("consensus_id", &self.inner.consensus_id)
            .finish()
    }
}

struct PeersInner {
    peers: RwLock<Vec<Peer>>,
    /// Unique id for each consensus run.
    consensus_id: AtomicUsize,
}

#[derive(Debug, Clone)]
struct Peer {
    actor_ref: ActorRef<RelayMessage>,
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

    /// Returns the number of peers in the collection.
    fn len(&self) -> usize {
        self.inner.peers.read().unwrap().len()
    }

    /// Start a consensus algorithm to add blob with `key`.
    pub fn add_blob<M>(
        &self,
        ctx: &mut actor::Context<M, ThreadLocal>,
        key: Key,
    ) -> (ConsensusId, PeerRpc<SystemTime>) {
        let id = self.new_consensus_id();
        let rpc = self.rpc(ctx, id, coordinator::AddBlob(key));
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
        self.rpc(ctx, id, coordinator::CommitAddBlob(key, timestamp))
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
        RelayMessage: From<RpcMessage<(ConsensusId, Req), Result<Res, RelayError>>>,
        Req: Clone,
    {
        let rpcs = self
            .inner
            .peers
            .read()
            .unwrap()
            .iter()
            .map(
                |peer| match peer.actor_ref.rpc(ctx, (id, request.clone())) {
                    Ok(rpc) => RpcStatus::InProgress(rpc),
                    Err(SendError) => RpcStatus::Done(Err(RelayError::Failed)),
                },
            )
            .collect();
        PeerRpc { rpcs }
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

    /// Attempts to add multiple peers.
    ///
    /// Its spawns a new [`coordinator::relay`] for each address in `addresses`,
    /// which are not in the collection already.
    fn extend<M>(
        &self,
        ctx: &mut actor::Context<M, ThreadSafe>,
        addresses: &[SocketAddr],
        server: SocketAddr,
    ) {
        // NOTE: we don't lock the entire collection up front here because we
        // don't want to hold the lock for too long.

        for address in addresses.into_iter().copied() {
            if self.known(&address) {
                continue;
            }

            // TODO: DRY with code in `peer::start`: need a trait for spawning
            // in Heph.
            let args = (address, self.clone(), server);
            let supervisor = RestartSupervisor::new("coordinator::relay", args.clone());
            let relay = coordinator::relay as fn(_, _, _, _) -> _;
            let options = ActorOptions::default().with_priority(Priority::HIGH);
            let relay_ref = ctx.spawn(supervisor, relay, args, options);
            self.add(Peer {
                actor_ref: relay_ref,
                address,
            });
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

fn known_peer(peers: &[Peer], address: &SocketAddr) -> bool {
    peers.iter().any(|peer| peer.address == *address)
}

pub struct PeerRpc<Res> {
    rpcs: Box<[RpcStatus<Res>]>,
}

/// Status of a [`Rpc`] future in [`PeerRpc`].
enum RpcStatus<Res> {
    /// Future is in progress.
    InProgress(Rpc<Result<Res, RelayError>>),
    /// Future has returned a result.
    Done(Result<Res, RelayError>),
    /// Future has returned and its result has been taken.
    Completed,
}

impl<Res> RpcStatus<Res> {
    /// Take the result.
    ///
    /// # Panics
    ///
    /// This will panic if the status is not `Done`.
    fn take(&mut self) -> Result<Res, RelayError> {
        match replace(self, RpcStatus::Completed) {
            RpcStatus::Done(result) => result,
            _ => unreachable!("folled `PeerRpc` after completion"),
        }
    }
}

impl<Res> Future for PeerRpc<Res> {
    type Output = Vec<Result<Res, RelayError>>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut task::Context<'_>) -> Poll<Self::Output> {
        // Check all the statuses.
        for status in self.rpcs.iter_mut() {
            match status {
                RpcStatus::InProgress(rpc) => match Pin::new(rpc).poll(ctx) {
                    Poll::Ready(result) => {
                        // Map no response to a failed response.
                        let result = match result {
                            Ok(result) => result,
                            Err(NoResponse) => Err(RelayError::Failed),
                        };
                        drop(replace(status, RpcStatus::Done(result)));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                // RpcStatus::Done(..) => {}, // Continue.
                // RpcStatus::Complete => unreachable!(),
                _ => {}
            }
        }

        // If we reached this point all statuses should be `Done`.
        let results = self.rpcs.iter_mut().map(|status| status.take()).collect();
        Poll::Ready(results)
    }
}

const COORDINATOR_MAGIC: &[u8] = b"Stored coordinate\0";
const PARTICIPANT_MAGIC: &[u8] = b"Stored participate";
const MAGIC_LENGTH: usize = 18;

const MAGIC_ERROR_MSG: &[u8] = b"invalid connection magic";

/// Actor that acts as switcher between acting as a coordinator server or
/// participant.
pub async fn switcher(
    // The `Response` type is a bit awkward, but required for
    // participant::dispatcher.
    ctx: actor::Context<participant::Response, ThreadSafe>,
    mut stream: TcpStream,
    remote: SocketAddr,
    peers: Peers,
    db_ref: ActorRef<db::Message>,
) -> io::Result<()> {
    debug!("accepted peer connection: remote_address={}", remote);
    // TODO: 8k is a bit large for `coordinator::server`, only needs to read 1
    // key at a time.
    let mut buf = Buffer::new();

    let mut bytes_read = 0;
    while bytes_read < MAGIC_LENGTH {
        bytes_read += buf.read_from(&mut stream).await?;
    }

    assert_eq!(MAGIC_LENGTH, COORDINATOR_MAGIC.len());
    assert_eq!(MAGIC_LENGTH, PARTICIPANT_MAGIC.len());
    match &buf.as_bytes()[..MAGIC_LENGTH] {
        COORDINATOR_MAGIC => {
            buf.processed(MAGIC_LENGTH);
            if let Err(err) = coordinator::server(ctx, stream, buf, db_ref).await {
                warn!("coordinator server failed: {}", err);
            }
            // Don't log the error as a problem in the switch part.
            Ok(())
        }
        PARTICIPANT_MAGIC => {
            buf.processed(MAGIC_LENGTH);
            if let Err(err) = participant::dispatcher(ctx, stream, buf, peers, db_ref).await {
                warn!("participant dispatcher failed: {}", err);
            }
            // Don't log the error as a problem in the switch part.
            Ok(())
        }
        _ => {
            warn!(
                "closing connection: incorrect connection magic: remote_address={}",
                remote
            );
            stream.write(MAGIC_ERROR_MSG).await.map(|_| ())
        }
    }
}
