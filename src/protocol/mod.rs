//! Define how to interact with connected clients.
//!
//! The [`Protocol`] trait defines the interactions with the client via a
//! certain protocol, e.g. HTTP or RESP. This is used by the [`controller`] to
//! implement the client connection logic.
//!
//! The [`Request`] and [`Response`] type define the the incoming and outgoing
//! messages respectively. Both can be received/send using the `Protocol`.
//!
//! See the sub-modules for concrete implementations:
//!  * [HTTP](http).
//!  * [RESP](resp).
//!
//! [`controller`]: crate::controller

use std::fmt;
use std::future::Future;

use crate::key::Key;
use crate::storage::Blob;

pub mod resp;
pub use resp::Resp;

/// Protocol that defines how to interact with connected clients.
pub trait Protocol {
    /// Return the source of the client.
    ///
    /// # Errors
    ///
    /// The Error is considered fatal.
    fn source(&mut self) -> impl Future<Output = Result<Self::Source, Self::ResponseError>>;

    /// Source of the client.
    ///
    /// For TCP connections this will be the IP address.
    type Source: fmt::Display;

    /// Read the next request.
    ///
    /// # Errors
    ///
    /// If this return an error it will be passed to
    /// [`Protocol::reply_to_error`]. If the error is [fatal] processing will
    /// stop.
    ///
    /// [fatal]: IsFatal::is_fatal
    fn next_request<'a>(
        &'a mut self,
    ) -> impl Future<Output = Result<Option<Request<'a>>, Self::RequestError>>;

    /// Error returned by [`Protocol::next_request`].
    ///
    /// If the [`IsFatal`] implementation of this error returns true the
    /// connection is considered broken and will no longer be used after
    /// [`Protocol::reply_to_error`] is called.
    type RequestError: IsFatal + fmt::Display;

    /// Reply to a request with `response`.
    fn reply<B>(
        &mut self,
        response: Response<B>,
    ) -> impl Future<Output = Result<(), Self::ResponseError>>
    where
        B: Blob;

    /// Reply to a (broken) request with `error`.
    fn reply_to_error(
        &mut self,
        error: Self::RequestError,
    ) -> impl Future<Output = Result<(), Self::ResponseError>>;

    /// Error returned by [`Protocol::reply`] and [`Protocol::reply_to_error`].
    ///
    /// This error is considered fatal for the connection and will stop further
    /// processing.
    type ResponseError: fmt::Display;
}

/// Request read by a [`Protocol`] implementation.
#[derive(Debug)]
pub enum Request<'a> {
    /// Add a blob to the storage.
    AddBlob(&'a [u8]),
    /// Remove the blob with key.
    RemoveBlob(Key),
    /// Get blob with key.
    GetBlob(Key),
    /// Check if a blob with key exists.
    ContainsBlob(Key),
    /// Check the number of blobs stored.
    BlobStored,
}

/// Response to a [`Request`], generic over the blob type `B`.
#[derive(Debug)]
pub enum Response<B> {
    /// Blob has been added.
    ///
    /// Response to [`Request::AddBlob`].
    Added(Key),
    /// Blob is already stored.
    ///
    /// Response to [`Request::AddBlob`].
    AlreadyStored(Key),
    /// Blob has been removed.
    ///
    /// Response to [`Request::RemoveBlob`].
    BlobRemoved,
    /// Blob was **not** removed, as it's not stored.
    ///
    /// Response to [`Request::RemoveBlob`].
    BlobNotRemoved,
    /// Blob was retrieved.
    ///
    /// Response to [`Request::GetBlob`].
    Blob(B),
    /// Blob is not found, e.g. when removing or getting it.
    ///
    /// Response to [`Request::GetBlob`].
    BlobNotFound,
    /// Store contains the blob.
    ///
    /// Response to [`Request::ContainsBlob`].
    ContainsBlob,
    /// Store does **not** contain the blob.
    ///
    /// Response to [`Request::ContainsBlob`].
    NotContainBlob,
    /// The amount of blobs stored.
    ///
    /// Response to [`Request::BlobStored`].
    ContainsBlobs(usize),
    /// Server error occurred, no detail is specified, but an error is logged.
    /// This is not an error from normal processing, something bad happened.
    ///
    /// Can be in response to any request.
    Error,
}

/// Used in logging.
impl<B> fmt::Display for Response<B> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Response::Added(key) => write!(f, "added {key}"),
            Response::AlreadyStored(key) => write!(f, "already stored {key}"),
            Response::BlobRemoved => f.write_str("blob removed"),
            Response::BlobNotRemoved => f.write_str("blob not removed"),
            Response::Blob(..) => f.write_str("found blob"),
            Response::ContainsBlob => f.write_str("contains blob"),
            Response::NotContainBlob => f.write_str("does not contain blob"),
            Response::BlobNotFound => f.write_str("blob not found"),
            Response::Error => f.write_str("server error"),
            Response::ContainsBlobs(amount) => write!(f, "stored {amount} blobs"),
        }
    }
}

/// Whether or not an error is fatal.
pub trait IsFatal {
    /// If this returns true the component is considered broken and will no
    /// longer be used.
    fn is_fatal(&self) -> bool;
}
