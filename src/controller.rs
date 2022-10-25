//! Controller is the core of the store that controls users connected to the
//! store.

use std::fmt;
use std::time::{Duration, Instant};

use log::{as_debug, as_display, debug, error, info, warn};

use crate::key::Key;
use crate::protocol::{Protocol, Request, Response};
use crate::storage::{AddError, Storage};
use crate::{Error, IsFatal};

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
    mut storage: S,
    source: I,
) -> Result<(), Error<P::ResponseError>>
where
    C: Config,
    P: Protocol,
    S: Storage,
    S::Error: fmt::Display,
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
                            source: err,
                        })
                    }
                }
            }
        };

        let start = Instant::now();
        let request_info = RequestInfo::from(&request);
        let response = match request {
            Request::AddBlob(blob) => match storage.add_blob(blob).await {
                Ok(key) => Response::Added(key),
                Err(AddError::AlreadyStored(key)) => Response::AlreadyStored(key),
                Err(AddError::Err(err)) => {
                    error!("failed to store blob: {err}");
                    Response::Error
                }
            },
            Request::RemoveBlob(key) => match storage.remove_blob(key).await {
                Ok(true) => Response::BlobRemoved,
                Ok(false) => Response::BlobNotFound,
                Err(err) => {
                    error!("failed to remove blob: {err}");
                    Response::Error
                }
            },
            Request::GetBlob(key) => match storage.lookup(key).await {
                Ok(Some(blob)) => Response::Blob(blob),
                Ok(None) => Response::BlobNotFound,
                Err(err) => {
                    error!("failed to retrieve blob: {err}");
                    Response::Error
                }
            },
            Request::CointainsBlob(key) => match storage.contains(key).await {
                Ok(true) => Response::ContainsBlob,
                Ok(false) => Response::BlobNotFound,
                Err(err) => {
                    error!("failed to check if storage contains blob: {err}");
                    Response::Error
                }
            },
        };

        let elapsed = start.elapsed();
        info!(target: "request",
            source = as_display!(source),
            request = as_display!(request_info),
            response = as_display!(response),
            elapsed = as_debug!(elapsed);
            "processed request");

        match protocol.reply(response, config.write_timeout()).await {
            Ok(()) => {}
            Err(err) => {
                return Err(Error {
                    description: "writing response",
                    source: err,
                })
            }
        }
    }

    let elapsed = accepted.elapsed();
    debug!(source = as_display!(source), elapsed = as_debug!(elapsed); "dropping connection");
    Ok(())
}

/// Information logged about a request.
///
/// See [`Request`].
enum RequestInfo {
    AddBlob,
    RemoveBlob(Key),
    GetBlob(Key),
    CointainsBlob(Key),
}

impl From<&Request<'_>> for RequestInfo {
    fn from(request: &Request) -> RequestInfo {
        match request {
            Request::AddBlob(..) => RequestInfo::AddBlob,
            Request::RemoveBlob(key) => RequestInfo::RemoveBlob(key.clone()),
            Request::GetBlob(key) => RequestInfo::GetBlob(key.clone()),
            Request::CointainsBlob(key) => RequestInfo::CointainsBlob(key.clone()),
        }
    }
}

impl fmt::Display for RequestInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RequestInfo::AddBlob => f.write_str("add blob"),
            RequestInfo::RemoveBlob(key) => write!(f, "remove {key}"),
            RequestInfo::GetBlob(key) => write!(f, "get {key}"),
            RequestInfo::CointainsBlob(key) => write!(f, "contains {key}"),
        }
    }
}
