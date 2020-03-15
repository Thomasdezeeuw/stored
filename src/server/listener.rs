use std::io;
use std::net::SocketAddr;

use heph::net::tcp::{self, TcpStream};
use heph::rt::options::Priority;
use heph::{actor, ActorOptions, NewActor, RuntimeRef, Supervisor, SupervisorStrategy};
use log::{error, warn};

use crate::server::cache::Cache;

/// Options used in [`setup`].
pub struct Options {
    pub cache: Cache,
    pub address: SocketAddr,
}

/// Spawns a TCP listener.
pub fn setup(runtime: &mut RuntimeRef, options: Options) -> io::Result<()> {
    let Options { cache, address } = options;

    let conn_actor = (conn_actor as fn(_, _, _, _) -> _) // Ugh.
        .map_arg(move |(stream, address)| (stream, address, cache.clone()));

    // FIXME: do this before starting the runtime, see example 2_my_ip.
    let listener = tcp::Server::setup(
        address,
        conn_supervisor,
        conn_actor,
        ActorOptions::default(),
    )?;
    let options = ActorOptions::default().with_priority(Priority::LOW);

    runtime
        .try_spawn(ServerSupervisor, listener, (), options)
        .map(|actor_ref| runtime.receive_signals(actor_ref.try_map()))
}

/// Our supervisor for the TCP server.
#[derive(Copy, Clone, Debug)]
struct ServerSupervisor;

impl<S, NA> Supervisor<tcp::ServerSetup<S, NA>> for ServerSupervisor
where
    S: Supervisor<NA> + Clone + 'static,
    NA: NewActor<Argument = (TcpStream, SocketAddr), Error = !> + Clone + 'static,
{
    fn decide(&mut self, err: tcp::ServerError<!>) -> SupervisorStrategy<()> {
        use tcp::ServerError::*;
        match err {
            // When we hit an error accepting a connection we'll drop the old
            // listener and create a new one.
            Accept(err) => {
                warn!("error accepting new connection: {}", err);
                SupervisorStrategy::Restart(())
            }
            NewActor::<!>(_) => unreachable!(),
        }
    }

    fn decide_on_restart_error(&mut self, err: io::Error) -> SupervisorStrategy<()> {
        // If we can't create a new listener we'll try to restart it.
        warn!("error restarting the TCP server: {}", err);
        SupervisorStrategy::Restart(())
    }

    fn second_restart_error(&mut self, err: io::Error) {
        error!("error restarting the TCP server a second time: {}", err);
    }
}

/// Our supervisor for the connection actor.
///
/// Since we can't create a new TCP connection all this supervisor does is log
/// the error and signal to stop the actor.
fn conn_supervisor(err: io::Error) -> SupervisorStrategy<(TcpStream, SocketAddr)> {
    error!("error handling connection: {}", err);
    SupervisorStrategy::Stop
}

async fn conn_actor(
    _ctx: actor::Context<!>,
    _stream: TcpStream,
    _address: SocketAddr,
    _cache: Cache,
) -> io::Result<()> {
    todo!("connection actor")
}
