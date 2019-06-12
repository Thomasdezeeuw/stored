use std::net::SocketAddr;
use std::io;

use futures::io::{AsyncReadExt, AsyncWriteExt};
use heph::actor_ref::{ActorRef, Sync};
use heph::log::REQUEST_TARGET;
use heph::net::tcp::{TcpListener, TcpListenerError, TcpStream};
use heph::system::options::Priority;
use heph::{actor, NewActor, ActorOptions, ActorSystemRef, SupervisorStrategy};
use log::{error, info};

use crate::cache::{self, Cache};

/// Options used in [`setup`].
pub struct Options {
    pub cache: Cache,
    pub cache_ref: ActorRef<cache::Message, Sync>,
    pub address: SocketAddr,
}

/// Add a `TcpListener` to the system.
pub fn setup(system_ref: &mut ActorSystemRef, options: Options) -> io::Result<()> {
    let Options { cache, cache_ref, address } = options;

    let conn_actor = (conn_actor as fn(_, _, _, _, _) -> _) // Ugh.
        .map_arg(move |(stream, address)|
            (stream, address, cache.clone(), cache_ref.clone()));

    let listener = TcpListener::new(conn_supervisor, conn_actor, ActorOptions {
        priority: Priority::LOW,
        .. ActorOptions::default()
    });
    system_ref.try_spawn(listener_supervisor, listener, address, ActorOptions::default())
        .map(|_| ())
}

fn listener_supervisor(err: TcpListenerError<!>) -> SupervisorStrategy<SocketAddr> {
    error!("error accepting connection: {}", err);
    SupervisorStrategy::Stop
}

fn conn_supervisor(err: io::Error) -> SupervisorStrategy<(TcpStream, SocketAddr)> {
    error!("error handling connection: {}", err);
    SupervisorStrategy::Stop
}

async fn conn_actor(
    _ctx: actor::Context<!>,
    mut stream: TcpStream,
    address: SocketAddr,
    _cache: Cache,
    _cache_ref: ActorRef<cache::Message, Sync>,
) -> io::Result<()> {
    info!(target: REQUEST_TARGET, "accepted connection: address={}", address);

    let mut buf = [0; 4096];

    let bytes = stream.read(&mut buf).await?;
    let buf = &buf[0..bytes];

    stream.write_all(&buf).await?;

    Ok(())
}