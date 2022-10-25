//! Define how to interact with connected clients.

use std::future::Future;
use std::time::Duration;
use std::{fmt, io};

use crate::key::Key;
use crate::storage::Blob;

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
    /// Server error occured, no detail is specified, but an error is logged.
    /// This is not an error from normal processing, something bad happened.
    Error,
}

/// Whether or not an error is fatal.
pub trait IsFatal {
    /// If this returns true the connection is considered broken and will no
    /// longer be used after [`Protocol::reply_to_error`] is called.
    fn is_fatal(&self) -> bool;
}

/// Connection abstraction.
// FIXME: make this async.
pub trait Connection {
    /// Read data into `buf`.
    fn read_into(&mut self, buf: Vec<u8>, timeout: Duration) -> io::Result<Vec<u8>>;
}
