//! Controller is the core of the store that controls users connected to the
//! store.

use std::fmt;
use std::time::{Duration, Instant};

use log::{as_debug, as_display, debug, info, warn};

use crate::protocol::{IsFatal, Protocol, Request, Response};
use crate::storage;

/// Controller configuration.
pub trait Config {
    /// Read timeout, passed to [`Protocol::next_request`].
    fn read_timeout(&self) -> Duration;

    /// Write timeout, passed to [`Protocol::reply`] and
    /// [`Protocol::reply_to_error`].
    fn write_timeout(&self) -> Duration;
}

/// Actor that controls a user connected using `protocol` trying to access
/// `storage`.
///
/// `source` is used in logging and should be socket address or similar.
pub async fn actor<C, P, S, I>(
    config: C,
    mut protocol: P,
    storage: S,
    source: I,
) -> Result<(), Error<S::Error, P::ResponseError>>
where
    C: Config,
    P: Protocol,
    S: storage::Read,
    S::Blob: fmt::Debug, // Needed for logging of response (for now).
    I: fmt::Display,
{
    let accepted = Instant::now();
    debug!(source = as_display!(source); "accepted connection");

    loop {
        let request = match protocol.next_request(config.read_timeout()).await {
            Ok(Some(request)) => request,
            Ok(None) => break, // Done.
            Err(err) => {
                let is_fatal = err.is_fatal();
                warn!(source = as_display!(source), fatal = as_display!(is_fatal);
                    "error reading next request: {err}");
                match protocol.reply_to_error(err, config.write_timeout()).await {
                    Ok(()) if is_fatal => break,
                    Ok(()) => continue,
                    Err(err) => {
                        return Err(Error {
                            description: "writing error response",
                            kind: ErrorKind::Protocol(err),
                        })
                    }
                }
            }
        };

        let start = Instant::now();
        let response = match &request {
            Request::AddBlob(..) => {
                todo!("AddBlob: need storage::Write access");
            }
            Request::RemoveBlob(..) => {
                todo!("RemoveBlob: need storage::Write access");
            }
            Request::GetBlob(key) => match storage.lookup(key) {
                Some(blob) => Response::Blob(blob),
                None => Response::BlobNotFound,
            },
            Request::CointainsBlob(key) => match storage.contains(key) {
                true => Response::ContainsBlob,
                false => Response::BlobNotFound,
            },
        };

        // TODO: improve logging of request and response.
        let elapsed = start.elapsed();
        info!(target: "request",
            source = as_display!(source),
            request = as_debug!(request),
            response = as_debug!(response),
            elapsed = as_debug!(elapsed);
            "processed request");

        match protocol.reply(response, config.write_timeout()).await {
            Ok(()) => {}
            Err(err) => {
                return Err(Error {
                    description: "writing response",
                    kind: ErrorKind::Protocol(err),
                })
            }
        }
    }

    // No more requests.
    let elapsed = accepted.elapsed();
    debug!(source = as_display!(source), elapsed = as_debug!(elapsed); "dropping connection");
    Ok(())
}

/// Error returned by [`actor`].
#[derive(Debug)]
pub struct Error<SE, PE> {
    /// Description of the operation that returned the error.
    description: &'static str,
    kind: ErrorKind<SE, PE>,
}

/// Error kind for [`Error`].
#[derive(Debug)]
enum ErrorKind<SE, PE> {
    /// Storage error.
    Storage(SE),
    /// Protocol error.
    Protocol(PE),
}

impl<SE, PE> std::error::Error for Error<SE, PE>
where
    SE: std::error::Error + 'static,
    PE: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            ErrorKind::Storage(err) => Some(err),
            ErrorKind::Protocol(err) => Some(err),
        }
    }
}

impl<SE, PE> fmt::Display for Error<SE, PE>
where
    SE: fmt::Display,
    PE: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ErrorKind::Storage(err) => write!(f, "{}: {}", self.description, err),
            ErrorKind::Protocol(err) => write!(f, "{}: {}", self.description, err),
        }
    }
}
