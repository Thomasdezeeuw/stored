//! Module with the type related to peer interaction.

use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use futures_util::io::AsyncWriteExt;
use heph::actor::context::ThreadSafe;
use heph::actor::{self, NewActor};
use heph::actor_ref::ActorRef;
use heph::net::{tcp, TcpStream};
use heph::rt::options::{ActorOptions, Priority};
use heph::rt::{self, Runtime};
use heph::supervisor::{RestartSupervisor, SupervisorStrategy};
use log::{debug, warn};

use crate::{config, db, Buffer};

pub mod coordinator;
pub mod participant;

use coordinator::RelayMessage;

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

/// Collection of [`Peer`]s.
#[derive(Clone, Debug)]
pub struct Peers {
    peers: Arc<RwLock<Vec<Peer>>>,
}

impl Peers {
    fn empty() -> Peers {
        Peers {
            peers: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Returns `true` if the collection is empty.
    pub fn is_empty(&self) -> bool {
        self.peers.read().unwrap().is_empty()
    }

    /// Returns the number of peers in the collection.
    fn len(&self) -> usize {
        self.peers.read().unwrap().len()
    }

    /// Get a private copy of all known peer addresses.
    fn addresses(&self) -> Vec<SocketAddr> {
        self.peers
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
        // NOTE: we don't lock here because we don't want to hold the lock for
        // too long, we just use `Peers::add`.

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
        let mut peers = self.peers.write().unwrap();
        if !known_peer(&peers, &peer.address) {
            peers.push(peer);
        } else {
            // TODO: kill the relay?
        }
    }

    /// Returns `true` if a peer at `address` is already in the collection.
    fn known(&self, address: &SocketAddr) -> bool {
        known_peer(&*self.peers.read().unwrap(), address)
    }
}

fn known_peer(peers: &[Peer], address: &SocketAddr) -> bool {
    peers.iter().any(|peer| peer.address == *address)
}

#[derive(Debug, Clone)]
pub struct Peer {
    actor_ref: ActorRef<RelayMessage>,
    address: SocketAddr,
}

impl Peer {
    /// Returns a reference to the actor reference.
    pub fn actor_ref(&self) -> &ActorRef<RelayMessage> {
        &self.actor_ref
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
