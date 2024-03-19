//! Storage implementations.
//!
//! The [`Storage`] trait defines the behaviour of the storage. This is used by
//! the [`controller`] to execute client requests.
//!
//! See the sub-modules for concrete implementations:
//!  * [in-memory](mem).
//!  * [on-disk](disk).
//!
//! The [`Blob`] trait defines specialised behaviour for the type used a blob.
//!
//! [`controller`]: crate::controller

use std::async_iter::AsyncIterator;
use std::future::Future;
use std::{fmt, io};

use heph_rt::io::{Buf, Write};

use crate::key::Key;

pub mod index;

pub mod mem;
pub use mem::new as new_in_memory;

pub mod disk;
pub use disk::open as open_on_disk;

/// Storage implementation.
pub trait Storage {
    /// Blob type used by the implementation.
    type Blob: Blob;

    /// Error used by the storage.
    type Error: fmt::Display;

    /// Returns the number of blobs stored.
    fn len(&self) -> usize;

    /// Returns the [`Blob`] corresponding to `key`, if stored.
    fn lookup(&self, key: Key) -> impl Future<Output = Result<Option<Self::Blob>, Self::Error>>;

    /// Returns `true` if the storage contains a blob corresponding to `key`,
    /// `false` otherwise.
    fn contains(&self, key: Key) -> impl Future<Output = Result<bool, Self::Error>>;

    /// Add `blob` to the storage.
    fn add_blob(&mut self, blob: &[u8])
        -> impl Future<Output = Result<Key, AddError<Self::Error>>>;

    /// Remove the blob with `key` from storage.
    ///
    /// Returns `true` if the blob was previously stored, `false` otherwise.
    fn remove_blob(&mut self, key: Key) -> impl Future<Output = Result<bool, Self::Error>>;
}

/// Error returned by [`Storage::add_blob`].
#[derive(Debug)]
pub enum AddError<E> {
    /// Blob is already stored.
    AlreadyStored(Key),
    /// Other, storage specific, error.
    Err(E),
}

/// Trait to represent a BLOB (Binary Large OBject).
pub trait Blob {
    /// Length of the blob in bytes.
    fn len(&self) -> usize;

    /// Write the blob with `header` and `trailer` to `connection`.
    fn write<H, T, C>(
        self,
        header: H,
        trailer: T,
        connection: &mut C,
    ) -> impl Future<Output = io::Result<(H, T)>>
    where
        H: Buf,
        T: Buf,
        C: Write;

    /// Iterator of the blob's bytes.
    // NOTE: this is mainly here for the HTTP implementation which doesn't have
    // direct access to the underlying stream.
    type BlobBytes: AsyncIterator<Item: Buf> + 'static;

    /// Returns an asynchronous iterator that iterator over buffers that
    /// represent parts of the blob.
    fn bytes(self) -> Self::BlobBytes;
}
