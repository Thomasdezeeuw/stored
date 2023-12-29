//! Storage implementations.
//!
//! The [`Storage`] trait defines the behaviour of the storage. This is used by
//! the [`controller`] to execute client requests.
//!
//! See the sub-modules for concrete implementations:
//!  * [in-memory](mem).
//!
//! The [`Blob`] trait defines specialised behaviour for the type used a blob.
//!
//! [`controller`]: crate::controller

use std::future::Future;
use std::io;

use heph_rt::io::{Buf, Write};

use crate::key::Key;

pub mod mem;
pub use mem::new as new_in_memory;

/// Trait to represent a BLOB (Binary Large OBject).
pub trait Blob {
    /// Length of the blob in bytes.
    fn len(&self) -> usize;

    /// Write the blob with `header` and `trailer` to `connection`.
    ///
    /// The `write` call will attempt to use the most efficient I/O possible,
    /// ranging from `sendfile(2)` to vectored I/O.
    fn write<H, T, C>(
        self,
        header: H,
        trailer: T,
        connection: C,
    ) -> impl Future<Output = Result<(H, T), io::Error>>
    where
        H: Buf,
        T: Buf,
        C: Write;
}

/// Storage implementation.
pub trait Storage {
    /// Blob type used by the implementation.
    type Blob: Blob;

    /// Error used by the storage.
    type Error;

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
