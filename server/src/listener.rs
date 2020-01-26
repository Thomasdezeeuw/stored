use std::io;
use std::sync::Arc;
use std::net::SocketAddr;

use heph::log::REQUEST_TARGET;
use heph::net::tcp::{self, TcpStream};
use heph::rt::options::Priority;
use heph::{Supervisor, actor, ActorRef, ActorOptions, RuntimeRef, NewActor, SupervisorStrategy};
use log::{warn, debug, error, info, trace};

use coeus_common::buffer::Buffer;
use coeus_common::{parse, serialise};

use crate::{Key, cache};
use crate::cache::Cache;

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

    let listener = tcp::Server::setup(address, conn_supervisor, conn_actor, ActorOptions::default())?;

    runtime
        .try_spawn(ServerSupervisor, listener, (), ActorOptions::default().with_priority(Priority::LOW))
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
        // If we can't create a new listener we'll stop.
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

/*
fn listener_supervisor(err: tcp::ServerError<!>) -> SupervisorStrategy<()> {
    error!("error accepting connection: {}", err);
    SupervisorStrategy::Stop
}

fn conn_supervisor(err: io::Error) -> SupervisorStrategy<(TcpStream, SocketAddr)> {
    error!("error handling connection: {}", err);
    SupervisorStrategy::Stop
}
*/

enum Message {
    CacheFound(cache::Value),
    CacheNotFound,
    CacheOk,
}

/*
impl From<Message> for cache::Response {
    fn from(msg: Message) -> cache::Response {
        use cache::Response::*;
        use Message::*;
        match msg {
            CacheFound(value) => Found(value),
            CacheNotFound => NotFound,
            CacheOk => Ok,
        }
    }
}

impl From<cache::Response> for Message {
    fn from(msg: cache::Response) -> Message {
        use cache::Response::*;
        match msg {
            Found(value) => Message::CacheFound(value),
            NotFound => Message::CacheNotFound,
            Ok => Message::CacheOk,
        }
    }
}
*/

async fn conn_actor(
    mut ctx: actor::Context<Message>,
    mut stream: TcpStream,
    address: SocketAddr,
    mut cache: Cache,
) -> io::Result<()> {
    todo!()

    /*
    use Message::*;
    info!(target: REQUEST_TARGET, "accepted connection: address={}", address);

    let mut buf = Buffer::new();

    loop {
        debug!("reading from connection");
        let bytes_read = buf.read_from(&mut stream).await?;
        trace!("read bytes: {}", bytes_read);
        if bytes_read == 0 {
            // Client closed the connection.
            return Ok(())
        }

        match parse::request(buf.as_bytes()) {
            Ok((request, request_length)) => {
                debug!("successfully parsed request: {:?}", request);
                let self_ref = ctx.actor_ref().upgrade(ctx.runtime()).unwrap().map();
                match request {
                    parse::Request::Store(value) => {
                        let key = Key::for_value(value);
                        let value = Arc::from(value);
                        cache <<= cache::Message::Store(key.clone(), value, self_ref);

                        match ctx.receive_next().await {
                            CacheOk => {
                                trace!("writing store response");
                                let response = serialise::Response::Store(&key);
                                response.write_to(&mut stream).await?
                            },
                            _ => unreachable!("received unexpected message"),
                        }
                    },
                    parse::Request::Retrieve(key) => {
                        cache <<= cache::Message::Retrieve(key.clone(), self_ref);

                        match ctx.receive_next().await {
                            CacheFound(value) => {
                                let response = serialise::Response::Value(&*value);
                                trace!("writing value response");
                                response.write_to(&mut stream).await?;
                            },
                            CacheNotFound => {
                                trace!("writing value not found response");
                                serialise::Response::ValueNotFound.write_to(&mut stream).await?
                            },
                            _ => unreachable!("received unexpected message"),
                        }
                    },
                    parse::Request::Remove(key) => {
                        cache <<= cache::Message::Remove(key.clone(), self_ref);

                        match ctx.receive_next().await {
                            CacheOk => {
                                trace!("writing ok response");
                                serialise::Response::Ok.write_to(&mut stream).await?
                            },
                            _ => unreachable!("received unexpected message"),
                        }
                    },
                }

                buf.processed(request_length)
            },
            Err(parse::Error::Incomplete) => {
                debug!("incomplete request");
            },
            Err(parse::Error::InvalidType) => {
                debug!("error parsing request: invalid request type");
                serialise::Response::InvalidRequestType.write_to(&mut stream).await?
            },
        }
    }
    */
}
