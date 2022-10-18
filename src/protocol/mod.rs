//! Define how to interact with connected clients.

use std::fmt;
use std::future::Future;
use std::time::Duration;

use crate::key::Key;
use crate::storage::Blob;

/// Protocol that defines how to interact with connected clients.
pub trait Protocol {
    type Error: fmt::Display;

    /// Read the next request.
    fn next_request<'a>(&'a mut self, timeout: Duration) -> Self::NextRequest<'a>;

    /// [`Future`] behind [`Protocol::next_request`].
    type NextRequest<'a>: Future<Output = Result<Option<Request<'a>>, Self::Error>> + 'a
    where
        Self: 'a;

    /// Reply to a request with `response`.
    fn reply<'a, B>(&'a mut self, response: Response<B>, timeout: Duration) -> Self::Reply<'a, B>
    where
        B: Blob + 'a;

    /// [`Future`] behind [`Protocol::reply`].
    type Reply<'a, B>: Future<Output = Result<(), Self::Error>> + 'a
    where
        Self: 'a,
        B: Blob + 'a;
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
pub enum Response<B: Blob> {
    /// Blob has been added.
    Added,
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
