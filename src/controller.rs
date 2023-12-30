//! Controller is the core of the store that controls users connected to the
//! store.
//!
//! The controller runs as an [`actor`](actor()), which is generic over the
//! [`Protocol`] and [`Storage`]. The controller can be configured using
//! [`Config`].

use std::fmt;
use std::time::{Duration, Instant};

use heph::actor;
use heph::supervisor::SupervisorStrategy;
use heph_rt::timer::{DeadlinePassed, Timer};
use heph_rt::util::either;
use log::{as_debug, as_display, debug, error, info, warn};

use crate::key::Key;
use crate::protocol::{IsFatal, Protocol, Request, Response};
use crate::storage::{AddError, Storage};

/// Controller configuration.
pub trait Config {
    /// Read timeout.
    fn read_timeout(&self) -> Duration;

    /// Write timeout.
    fn write_timeout(&self) -> Duration;
}

/// Default implementation for [`Config`].
pub struct DefaultConfig;

impl Config for DefaultConfig {
    fn read_timeout(&self) -> Duration {
        Duration::from_secs(60)
    }

    fn write_timeout(&self) -> Duration {
        Duration::from_secs(30)
    }
}

/// Actor that controls a user connected using `protocol` trying to access
/// `storage`.
///
/// `source` is used in logging and should be socket address or similar.
pub async fn actor<C, P, S, RT>(
    ctx: actor::Context<!, RT>,
    config: C,
    mut protocol: P,
    mut storage: S,
) -> Result<(), Error<P::ResponseError>>
where
    C: Config,
    P: Protocol,
    S: Storage,
    S::Error: fmt::Display,
    RT: heph_rt::Access + Clone,
{
    let accepted = Instant::now();
    let source = match protocol.source().await {
        Ok(source) => source,
        Err(err) => return Err(Error::new("getting source of client", err)),
    };
    debug!(source = as_display!(source); "accepted connection");

    loop {
        let timer = Timer::after(ctx.runtime_ref().clone(), config.read_timeout());
        let request = match either(protocol.next_request(), timer).await {
            Ok(Ok(Some(request))) => request,
            Ok(Ok(None)) => break, // Done.
            Ok(Err(err)) => {
                let is_fatal = err.is_fatal();
                warn!(source = as_display!(source), fatal = as_display!(is_fatal);
                    "error reading next request: {err}");
                let timer = Timer::after(ctx.runtime_ref().clone(), config.write_timeout());
                match either(protocol.reply_to_error(err), timer).await {
                    Ok(Ok(())) if is_fatal => break,
                    Ok(Ok(())) => continue,
                    Ok(Err(err)) => return Err(Error::new("writing error response", err)),
                    Err(DeadlinePassed) => {
                        warn!(source = as_display!(source); "timed out writing error response");
                        break;
                    }
                }
            }
            Err(DeadlinePassed) => {
                warn!(source = as_display!(source); "timed out reading next request");
                break;
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
                Ok(false) => Response::BlobNotRemoved,
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
            Request::ContainsBlob(key) => match storage.contains(key).await {
                Ok(true) => Response::ContainsBlob,
                Ok(false) => Response::NotContainBlob,
                Err(err) => {
                    error!("failed to check if storage contains blob: {err}");
                    Response::Error
                }
            },
            Request::BlobStored => Response::ContainsBlobs(storage.len()),
        };

        let elapsed = start.elapsed();
        info!(target: "request", source = as_display!(source), request = as_display!(request_info),
            response = as_display!(response), elapsed = as_debug!(elapsed); "processed request");

        let timer = Timer::after(ctx.runtime_ref().clone(), config.write_timeout());
        match either(protocol.reply(response), timer).await {
            Ok(Ok(())) => {} // On to the next request.
            Ok(Err(err)) => return Err(Error::new("writing response", err)),
            Err(DeadlinePassed) => {
                warn!(source = as_display!(source); "timed out writing response");
                break;
            }
        }
    }

    let elapsed = accepted.elapsed();
    debug!(source = as_display!(source), elapsed = as_debug!(elapsed); "dropping connection");
    Ok(())
}

/// Controller error.
#[derive(Debug)]
pub struct Error<E> {
    /// Description of the operation.
    description: &'static str,
    /// Underlying protocol error.
    source: E,
}

impl<E> Error<E> {
    const fn new(description: &'static str, source: E) -> Error<E> {
        Error {
            description,
            source,
        }
    }
}

impl<E> fmt::Display for Error<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.description, self.source)
    }
}

/// [`Supervisor`] for the controller [`actor`](actor()) that logs the error and
/// stops the actor.
///
/// [`Supervisor`]: heph::supervisor::Supervisor
pub fn supervisor<A, E>(err: Error<E>) -> SupervisorStrategy<A>
where
    E: fmt::Display,
{
    error!("error handling connection: {err}");
    SupervisorStrategy::Stop
}

/// Information logged about a request.
///
/// See [`Request`].
enum RequestInfo {
    AddBlob,
    RemoveBlob(Key),
    GetBlob(Key),
    ContainsBlob(Key),
    BlobStored,
}

impl From<&Request<'_>> for RequestInfo {
    fn from(request: &Request) -> RequestInfo {
        match request {
            Request::AddBlob(..) => RequestInfo::AddBlob,
            Request::RemoveBlob(key) => RequestInfo::RemoveBlob(key.clone()),
            Request::GetBlob(key) => RequestInfo::GetBlob(key.clone()),
            Request::ContainsBlob(key) => RequestInfo::ContainsBlob(key.clone()),
            Request::BlobStored => RequestInfo::BlobStored,
        }
    }
}

impl fmt::Display for RequestInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RequestInfo::AddBlob => f.write_str("add blob"),
            RequestInfo::RemoveBlob(key) => write!(f, "remove {key}"),
            RequestInfo::GetBlob(key) => write!(f, "get {key}"),
            RequestInfo::ContainsBlob(key) => write!(f, "contains {key}"),
            RequestInfo::BlobStored => f.write_str("amount of blobs stored"),
        }
    }
}
