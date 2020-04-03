//! Module with the HTTP actor.

use std::io;
use std::net::SocketAddr;
use std::time::Instant;

use log::{debug, error, info};

use heph::log::request;
use heph::net::TcpStream;
use heph::{actor, ActorRef};

use crate::server::http::{Connection, Request, Response, ResponseKind};

use super::db::{self, AddBlobResponse};

/// Actor that handles a single TCP `stream`.
///
/// Returns any and all I/O errors.
pub async fn actor(
    mut ctx: actor::Context<!>,
    stream: TcpStream,
    address: SocketAddr,
    mut db_ref: ActorRef<db::Message>,
) -> io::Result<()> {
    let start = Instant::now();
    debug!("accepted connection: address={}", address);
    let mut conn = Connection::new(stream);

    // TODO: pipeline.
    // check response.should_close -> close connection.

    let request = match conn.read_header().await {
        Ok(request) => request,
        Err(err) => {
            debug!("HTTP request error: {}", err);
            let response = Response::for_error(err)?;
            // Write the error response and close the connection. We don't want
            // to support pipelining here because we haven't read the body.
            conn.write_response(&response).await?;
            request!(
                "processed invalid request: status_code={}, address={}, elapsed={:?}",
                response.status_code().0,
                address,
                start.elapsed()
            );
            return conn.close();
        }
    };

    let method = request.method();
    let is_head = request.is_head();
    let response = match request {
        Request::Post(size_hint) => {
            info!(
                "POST request to store blob: content_length={}, address={}",
                size_hint, address
            );

            // TODO: get this from a configuration.
            const MAX_SIZE: usize = 1024 * 1024; // 1MB.
            if size_hint <= MAX_SIZE {
                // TODO: put a timeout on reading this and response with a 408:
                // https://tools.ietf.org/html/rfc7231#section-6.5.7.
                let mut body = conn.read_body(size_hint).await?;
                let blob = body.take();

                if blob.is_empty() {
                    Response::new(false, ResponseKind::BadRequest("Can't store empty blob"))
                } else if blob.len() < size_hint {
                    Response::new(false, ResponseKind::BadRequest("Incomplete blob"))
                } else {
                    match db_ref.rpc(&mut ctx, (blob, size_hint)) {
                        Ok(rpc) => match rpc.await {
                            Ok(AddBlobResponse::Query(query, mut buffer)) => {
                                buffer.processed(size_hint);
                                body.replace(buffer);

                                match db_ref.rpc(&mut ctx, query) {
                                    Ok(rpc) => match rpc.await {
                                        Ok(key) => Response::new(false, ResponseKind::Stored(key)),
                                        Err(err) => {
                                            error!(
                                                "error waiting for RPC response from database: {}",
                                                err
                                            );
                                            Response::new(false, ResponseKind::ServerError)
                                        }
                                    },
                                    Err(err) => {
                                        error!("error making RPC call to database: {}", err);
                                        Response::new(false, ResponseKind::ServerError)
                                    }
                                }
                            }
                            Ok(AddBlobResponse::AlreadyPresent(key)) => {
                                Response::new(false, ResponseKind::Stored(key))
                            }
                            Err(err) => {
                                error!("error waiting for RPC response from database: {}", err);
                                Response::new(false, ResponseKind::ServerError)
                            }
                        },
                        Err(err) => {
                            error!("error making RPC call to database: {}", err);
                            Response::new(false, ResponseKind::ServerError)
                        }
                    }
                }
            } else {
                Response::new(false, ResponseKind::TooLargePayload)
            }
        }
        Request::Get(key) | Request::Head(key) => {
            info!(
                "{} request for blob: key={}, address={}",
                if is_head { "HEAD" } else { "GET" },
                key,
                address
            );

            // TODO: deal with the request body.

            match db_ref.rpc(&mut ctx, key) {
                Ok(rpc) => match rpc.await {
                    Ok(Some(blob)) => Response::new(is_head, ResponseKind::Ok(blob)),
                    Ok(None) => Response::new(is_head, ResponseKind::NotFound),
                    Err(err) => {
                        error!("error waiting for RPC response from database: {}", err);
                        Response::new(is_head, ResponseKind::ServerError)
                    }
                },
                Err(err) => {
                    error!("error making RPC call to database: {}", err);
                    Response::new(is_head, ResponseKind::ServerError)
                }
            }
        }
        Request::Delete(key) => {
            info!(
                "DELETE request to delete blob: key={}, address={}",
                key, address
            );
            // TODO: implement this.
            error!("TODO: delete: key={}", key);
            Response::new(false, ResponseKind::ServerError)
        }
    };

    conn.write_response(&response).await?;
    // TODO: add HTTP request path.
    request!(
        "processed request: method={} status_code={}, address={}, elapsed={:?}",
        method,
        response.status_code().0,
        address,
        start.elapsed()
    );
    conn.close()
}
