use std::io;
use std::sync::Arc;
use std::net::SocketAddr;

use heph::log::REQUEST_TARGET;
use heph::net::tcp::{self, TcpStream};
use heph::system::options::Priority;
use heph::{actor, ActorOptions, ActorSystemRef, NewActor, SupervisorStrategy};
use heph::actor_ref::{ActorRef, Sync};
use log::{debug, error, info, trace};

use coeus_common::buffer::Buffer;
use coeus_common::{parse, serialise};

use crate::{Key, cache};

/// Options used in [`setup`].
pub struct Options {
    pub cache_ref: ActorRef<Sync<cache::Message>>,
    pub address: SocketAddr,
}

/// Add a `TcpListener` to the system.
pub fn setup(system_ref: &mut ActorSystemRef, options: Options) -> io::Result<()> {
    let Options { cache_ref, address } = options;

    let conn_actor = (conn_actor as fn(_, _, _, _) -> _) // Ugh.
        .map_arg(move |(stream, address)| (stream, address, cache_ref.clone()));

    let listener = tcp::setup_server(
        conn_supervisor,
        conn_actor,
        ActorOptions::default().with_priority(Priority::LOW),
    );

    info!("listening on address: {}", address);
    system_ref
        .try_spawn(listener_supervisor, listener, address, ActorOptions::default())
        .map(|_| ())
}

fn listener_supervisor(err: tcp::ServerError<!>) -> SupervisorStrategy<SocketAddr> {
    error!("error accepting connection: {}", err);
    SupervisorStrategy::Stop
}

fn conn_supervisor(err: io::Error) -> SupervisorStrategy<(TcpStream, SocketAddr)> {
    error!("error handling connection: {}", err);
    SupervisorStrategy::Stop
}

enum Message {
    CacheFound(cache::Value),
    CacheNotFound,
    CacheOk,
}

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

async fn conn_actor(
    mut ctx: actor::Context<Message>,
    mut stream: TcpStream,
    address: SocketAddr,
    mut cache: ActorRef<Sync<cache::Message>>,
) -> io::Result<()> {
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
                let self_ref = ctx.actor_ref().upgrade(ctx.system_ref()).unwrap().map();
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
}
