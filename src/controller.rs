//! Controller is the core of the store that controls users connected to the
//! store.

use std::fmt;
use std::time::Instant;

use log::{as_debug, as_display, debug, info};

use crate::protocol::{Protocol, Request, Response};
use crate::{storage, Describe, Error};

/// Actor that controls a user connected using `protocol` trying to access
/// `storage`.
///
/// `source` is used in logging and should be socket address or similar.
pub async fn actor<P, S, I, E>(mut protocol: P, storage: S, source: I) -> Result<(), Error<E>>
where
    P: Protocol<Error = E>,
    S: storage::Read<Error = E>,
    S::Blob: fmt::Debug, // Needed for logging of response (for now).
    I: fmt::Display,
{
    let accepted = Instant::now();
    debug!(source = as_display!(source); "accepted connection");

    while let Some(request) = protocol
        .next_request()
        .await
        .describe("reading next request")?
    {
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

        protocol
            .reply(response)
            .await
            .describe("writing response")?;
    }

    // No more requests.
    let elapsed = accepted.elapsed();
    debug!(source = as_display!(source), elapsed = as_debug!(elapsed); "dropping connection");
    Ok(())
}
