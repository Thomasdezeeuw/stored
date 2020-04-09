//! Module with server's supervisors.

use std::io;
use std::net::SocketAddr;

use heph::net::{tcp, TcpStream};
use heph::{NewActor, Supervisor, SupervisorStrategy};
use log::error;

/// TCP/HTTP server supervisor.
///
/// Attempts to restart the server once, stops it the second time.
pub struct ServerSupervisor;

impl<S, NA> Supervisor<tcp::ServerSetup<S, NA>> for ServerSupervisor
where
    S: Supervisor<NA> + Clone + 'static,
    NA: NewActor<Argument = (TcpStream, SocketAddr), Error = !> + Clone + 'static,
{
    fn decide(&mut self, err: tcp::ServerError<!>) -> SupervisorStrategy<()> {
        use tcp::ServerError::*;
        match err {
            Accept(err) => {
                error!("error accepting new connection: {}", err);
                SupervisorStrategy::Restart(())
            }
            NewActor::<!>(_) => unreachable!(),
        }
    }

    fn decide_on_restart_error(&mut self, err: io::Error) -> SupervisorStrategy<()> {
        error!("error restarting the HTTP server: {}", err);
        SupervisorStrategy::Stop
    }

    fn second_restart_error(&mut self, err: io::Error) {
        error!("error restarting the actor a second time: {}", err);
    }
}

/// Supervisor for the [`http::actor`]
///
/// [`http::actor`]: crate::http::actor()
///
/// Logs the error and stops the actor.
pub fn http_supervisor(err: io::Error) -> SupervisorStrategy<(TcpStream, SocketAddr)> {
    error!("error handling HTTP connection: {}", err);
    SupervisorStrategy::Stop
}
