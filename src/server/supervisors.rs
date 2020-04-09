//! Module with server's supervisors.

use std::io;
use std::net::SocketAddr;
use std::path::Path;

use heph::actor::sync::SyncActor;
use heph::net::{tcp, TcpStream};
use heph::supervisor::SyncSupervisor;
use heph::{NewActor, Supervisor, SupervisorStrategy};
use log::{error, info};

use crate::storage::Storage;

/// Supervisor for the [`db::actor`].
///
/// [`db::actor`]: crate::server::actors::db::actor
///
/// It logs the error and tries to reopen the database, restarting the actor if
/// successful.
pub struct DbSupervisor(Box<Path>);

impl DbSupervisor {
    /// Create a new `DbSupervisor`.
    pub const fn new(path: Box<Path>) -> DbSupervisor {
        DbSupervisor(path)
    }
}

impl<A> SyncSupervisor<A> for DbSupervisor
where
    A: SyncActor<Argument = Storage, Error = io::Error>,
{
    fn decide(&mut self, err: io::Error) -> SupervisorStrategy<Storage> {
        error!("error operating on database: {}", err);
        info!("attempting to reopen database");
        match Storage::open(&self.0) {
            Ok(storage) => {
                info!("successfully reopened database, restarting database actor");
                SupervisorStrategy::Restart(storage)
            }
            Err(err) => {
                // FIXME: shutdown the entire server somehow? Maybe by sending
                // the TCP server a shutdown message?
                error!(
                    "failed to reopen database, not restarting database actor: {}",
                    err
                );
                SupervisorStrategy::Stop
            }
        }
    }
}

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
