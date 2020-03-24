//! Module with the HTTP actor.

// TODO: support pipelining.

use std::io;
use std::net::SocketAddr;
use std::time::Instant;

use log::{debug, error, info, warn};

use heph::log::request;
use heph::net::TcpStream;
use heph::{actor, ActorRef};

use crate::server::http::{Connection, Request, RequestError, Response};

use super::db;

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

    use RequestError::*;
    let request = match conn.read_header().await {
        Ok(request) => request,
        Err(err) => {
            warn!("HTTP request error: {}", err);

            let response = match err {
                // We just return I/O error, can't do much with them.
                Io(err) => return Err(err),
                // HTTP parsing errors.
                Parse(httparse::Error::HeaderName) => Response::BadRequest("invalid header name"),
                Parse(httparse::Error::HeaderValue) => Response::BadRequest("invalid header value"),
                Parse(httparse::Error::NewLine)
                | Parse(httparse::Error::Status)
                | Parse(httparse::Error::Token)
                | Parse(httparse::Error::Version) => {
                    Response::BadRequest("invalid HTTP request format")
                }
                Parse(httparse::Error::TooManyHeaders) => Response::TooManyHeaders,
                // 404 Not found.
                InvalidRoute => Response::NotFound,
                // Always need a Content-Length header for `Post` requests.
                MissingContentLength | InvalidContentLength => Response::NoContentLength,
                // Invalid key format in "/blob/$key" path.
                InvalidKey(_err) => Response::InvalidKey,
            };

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
    let response = match request {
        Request::Post(size_hint) => {
            info!(
                "request to store blob: size={}, address={}",
                size_hint, address
            );
            // TODO: implement this.
            todo!("TODO: Post: size_hint={}", size_hint);
        }
        Request::Get(key) => {
            info!("request for blob: key={}, address={}", key, address);

            // TODO: deal with the request body.

            match db_ref.rpc(&mut ctx, key) {
                Ok(rpc) => match rpc.await {
                    Ok(Some(blob)) => Response::Ok(blob),
                    Ok(None) => Response::NotFound,
                    Err(err) => {
                        error!("error waiting for RPC response from database: {}", err);
                        Response::ServerError
                    }
                },
                Err(err) => {
                    error!("error making RPC call to database: {}", err);
                    Response::ServerError
                }
            }
        }
        Request::Head(key) => {
            info!("head request for blob: key={}, address={}", key, address);

            // TODO: deal with the request body.

            // This is the same as for `Request::Get`, but returns NoBody
            // responses.
            match db_ref.rpc(&mut ctx, key) {
                Ok(rpc) => match rpc.await {
                    Ok(Some(blob)) => Response::OkNobody(blob),
                    Ok(None) => Response::NotFoundNoBody,
                    Err(err) => {
                        error!("error waiting for RPC response from database: {}", err);
                        Response::ServerErrorNoBody
                    }
                },
                Err(err) => {
                    error!("error making RPC call to database: {}", err);
                    Response::ServerErrorNoBody
                }
            }
        }
        Request::Delete(key) => {
            info!("request to delete blob: key={}, address={}", key, address);
            // TODO: implement this.
            todo!("TODO: delete: key={}", key);
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
