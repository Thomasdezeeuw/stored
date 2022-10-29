//! Define how to interact with connected clients.

use std::future::Future;
use std::io::IoSlice;
use std::time::Duration;
use std::{fmt, io};

use heph_rt::net::TcpStream;

use crate::key::Key;
use crate::storage::Blob;
use crate::IsFatal;

pub mod resp;
pub use resp::Resp;

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
    /// Response to [`Request::CointainsBlob`].
    ContainsBlob,
    /// Store does **not** contain the blob.
    ///
    /// Response to [`Request::CointainsBlob`].
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

    /// Write data from `buf`.
    fn write<'a>(&'a mut self, buf: &'a [u8], timeout: Duration) -> Self::Write<'a>;

    /// [`Future`] behind [`Connection::write`].
    type Write<'a>: Future<Output = Result<(), Self::Error>> + 'a
    where
        Self: 'a;

    /// Write data from `buf`.
    fn write_vectored<'a>(
        &'a mut self,
        bufs: &'a mut [IoSlice<'a>],
        timeout: Duration,
    ) -> Self::WriteVectored<'a>;

    /// [`Future`] behind [`Connection::write`].
    type WriteVectored<'a>: Future<Output = Result<(), Self::Error>> + 'a
    where
        Self: 'a;
}

impl<C> Connection for &mut C
where
    C: Connection,
{
    type Error = C::Error;

    fn read_into<'a>(&'a mut self, buf: Vec<u8>, timeout: Duration) -> Self::Read<'a> {
        (&mut **self).read_into(buf, timeout)
    }

    type Read<'a> = C::Read<'a>
    where
        Self: 'a;

    fn write<'a>(&'a mut self, buf: &'a [u8], timeout: Duration) -> Self::Write<'a> {
        (&mut **self).write(buf, timeout)
    }

    type Write<'a> = C::Write<'a>
    where
        Self: 'a;

    fn write_vectored<'a>(
        &'a mut self,
        bufs: &'a mut [IoSlice<'a>],
        timeout: Duration,
    ) -> Self::WriteVectored<'a> {
        (&mut **self).write_vectored(bufs, timeout)
    }

    type WriteVectored<'a> = C::WriteVectored<'a>
    where
        Self: 'a;
}

impl Connection for TcpStream {
    type Error = io::Error;

    fn read_into<'a>(&'a mut self, mut buf: Vec<u8>, timeout: Duration) -> Self::Read<'a> {
        // FIXME: add timeout.
        async move { self.recv(&mut buf).await.map(|_| buf) }
    }

    type Read<'a> = impl Future<Output = Result<Vec<u8>, Self::Error>> + 'a
    where
        Self: 'a;

    fn write<'a>(&'a mut self, buf: &'a [u8], timeout: Duration) -> Self::Write<'a> {
        // FIXME: add timeout.
        async move { self.send_all(buf).await }
    }

    type Write<'a> = impl Future<Output = Result<(), Self::Error>> + 'a
    where
        Self: 'a;

    fn write_vectored<'a>(
        &'a mut self,
        bufs: &'a mut [IoSlice<'a>],
        timeout: Duration,
    ) -> Self::WriteVectored<'a> {
        // FIXME: add timeout.
        async move { self.send_vectored_all(bufs).await }
    }

    type WriteVectored<'a> = impl Future<Output = Result<(), Self::Error>> + 'a
    where
        Self: 'a;
}
