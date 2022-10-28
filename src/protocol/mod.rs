//! Define how to interact with connected clients.

use std::fmt;
use std::future::Future;
use std::time::Duration;

use crate::key::Key;
use crate::storage::Blob;
use crate::IsFatal;

pub mod resp;

/// Protocol that defines how to interact with connected clients.
pub trait Protocol {
    /// Read the next request.
    ///
    /// # Errors
    ///
    /// If this return an error it will be passed to
    /// [`Protocol::reply_to_error`]. If the error is [fatal] processing will
    /// stop.
    ///
    /// [fatal]: IsFatal::is_fatal
    fn next_request<'a>(&'a mut self, timeout: Duration) -> Self::NextRequest<'a>;

    /// [`Future`] behind [`Protocol::next_request`].
    type NextRequest<'a>: Future<Output = Result<Option<Request<'a>>, Self::RequestError>> + 'a
    where
        Self: 'a;

    /// Error returned by [`Protocol::next_request`].
    ///
    /// If the [`IsFatal`] implementation of this error returns true the
    /// connection is considered broken and will no longer be used after
    /// [`Protocol::reply_to_error`] is called.
    type RequestError: IsFatal + fmt::Display;

    /// Reply to a request with `response`.
    fn reply<'a, B>(&'a mut self, response: Response<B>, timeout: Duration) -> Self::Reply<'a, B>
    where
        B: Blob + 'a;

    /// [`Future`] behind [`Protocol::reply`].
    type Reply<'a, B>: Future<Output = Result<(), Self::ResponseError>> + 'a
    where
        Self: 'a,
        B: Blob + 'a;

    /// Reply to a (broken) request with `error`.
    fn reply_to_error<'a>(
        &'a mut self,
        error: Self::RequestError,
        timeout: Duration,
    ) -> Self::ReplyWithError<'a>;

    /// [`Future`] behind [`Protocol::reply`].
    type ReplyWithError<'a>: Future<Output = Result<(), Self::ResponseError>> + 'a
    where
        Self: 'a;

    /// Error returned by [`Protocol::reply`] and [`Protocol::reply_to_error`].
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
    CointainsBlob(Key),
}

/// Response to a [`Request`], generic over the blob type `B`.
#[derive(Debug)]
pub enum Response<B> {
    /// Blob has been added.
    Added(Key),
    /// Blob is already stored.
    AlreadyStored(Key),
    /// Blob has been removed.
    BlobRemoved,
    /// Blob was retrieved.
    Blob(B),
    /// Store contains the blob.
    ContainsBlob,
    /// Blob is not found, e.g. when removing or getting it.
    BlobNotFound,
    /// Server error occurred, no detail is specified, but an error is logged.
    /// This is not an error from normal processing, something bad happened.
    Error,
}

impl<B> fmt::Display for Response<B> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Response::Added(key) => write!(f, "added {key}"),
            Response::AlreadyStored(key) => write!(f, "already stored {key}"),
            Response::BlobRemoved => f.write_str("blob removed"),
            Response::Blob(..) => f.write_str("found blob"),
            Response::ContainsBlob => f.write_str("contains blob"),
            Response::BlobNotFound => f.write_str("blob not found"),
            Response::Error => f.write_str("server error"),
        }
    }
}

/// Connection abstraction.
pub trait Connection {
    /// I/O error, usually [`std::io::Error`].
    ///
    /// This error is always considered fatal.
    type Error: fmt::Display;

    /// Read data into `buf`.
    fn read_into<'a>(&'a mut self, buf: Vec<u8>, timeout: Duration) -> Self::Read<'a>;

    /// [`Future`] behind [`Connection::read_into`].
    type Read<'a>: Future<Output = Result<Vec<u8>, Self::Error>> + 'a
    where
        Self: 'a;
}
