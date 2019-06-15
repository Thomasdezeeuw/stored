use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use heph::log::REQUEST_TARGET;
use heph::net::tcp::{TcpListener, TcpListenerError, TcpStream};
use heph::system::options::Priority;
use heph::{actor, ActorOptions, ActorSystemRef, NewActor, SupervisorStrategy};
use log::{debug, error, info, trace};

use coeus_common::{parse, serialise};

use crate::cache::CacheRef;
use crate::buffer::Buffer;

/// Options used in [`setup`].
pub struct Options {
    pub cache: CacheRef,
    pub address: SocketAddr,
}

/// Add a `TcpListener` to the system.
pub fn setup(system_ref: &mut ActorSystemRef, options: Options) -> io::Result<()> {
    let Options { cache, address } = options;

    let conn_actor = (conn_actor as fn(_, _, _, _) -> _) // Ugh.
        .map_arg(move |(stream, address)| (stream, address, cache.clone()));

    let listener = TcpListener::new(
        conn_supervisor,
        conn_actor,
        ActorOptions {
            priority: Priority::LOW,
            ..ActorOptions::default()
        },
    );

    info!("listening on address: {}", address);
    system_ref
        .try_spawn(listener_supervisor, listener, address, ActorOptions::default())
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
    _: actor::Context<!>,
    mut stream: TcpStream,
    address: SocketAddr,
    mut cache: CacheRef,
) -> io::Result<()> {
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
                match request {
                    parse::Request::Store(value) => {
                        let key = cache.async_add(Arc::from(value));
                        let response = serialise::Response::Store(&key);
                        trace!("writing store response");
                        response.write_to(&mut stream).await?
                    },
                    parse::Request::Retrieve(key) => {
                        if let Some(value) = cache.get(key) {
                            let response = serialise::Response::Value(&*value);
                            trace!("writing value response");
                            response.write_to(&mut stream).await?
                        } else {
                            trace!("writing value not found response");
                            serialise::Response::ValueNotFound.write_to(&mut stream).await?
                        }
                    },
                    parse::Request::Remove(key) => {
                        cache.async_remove(key.clone());
                        trace!("writing ok response");
                        serialise::Response::Ok.write_to(&mut stream).await?
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
