//! Storage implementations.

use std::future::Future;
use std::io;

use heph_rt::io::Write;

use crate::key::Key;

pub mod mem;
pub use mem::new as new_in_memory;

/// Trait to represent a BLOB (Binary Large OBject).
pub trait Blob {
    /// Length of the blob in bytes.
    fn len(&self) -> usize;

    /// Write the blob with `header` and `trailer` to `connection`.
    fn write<'a, C>(
        &'a self,
        header: &'a [u8],
        trailer: &'a [u8],
        connection: C,
    ) -> impl Future<Output = Result<(), io::Error>> + 'a
    where
        C: Write + 'a;
}

/// Storage implementation.
pub trait Storage {
    /// Blob type returned.
    type Blob: Blob;

    /// Error used by the storage, often this will be [`std::io::Error`].
    type Error;

    /// Returns the number of blobs stored.
    fn len(&self) -> usize;

    /// Returns the number of bytes of data stored.
    fn data_size(&self) -> u64;

    /// Returns the total size of the storage file(s).
    ///
    /// # Notes
    ///
    /// This may **not** match the file size of the data file as data may be
    /// preallocated.
    fn total_size(&self) -> u64;

    /// Returns the [`Blob`] corresponding to `key`, if stored.
    fn lookup<'a>(&'a self, key: Key) -> Self::Lookup<'a>;

    /// [`Future`] behind [`Storage::lookup`].
    type Lookup<'a>: Future<Output = Result<Option<Self::Blob>, Self::Error>> + 'a
    where
        Self: 'a;

    /// Returns `true` if the storage contains a blob corresponding to `key`,
    /// `false` otherwise.
    fn contains<'a>(&'a self, key: Key) -> Self::Contains<'a>;

    /// [`Future`] behind [`Storage::contains`].
    type Contains<'a>: Future<Output = Result<bool, Self::Error>> + 'a
    where
        Self: 'a;

    /// Add `blob` to the storage.
    fn add_blob<'a>(&'a mut self, blob: &[u8]) -> Self::AddBlob<'a>;

    /// [`Future`] behind [`Storage::add_blob`].
    type AddBlob<'a>: Future<Output = Result<Key, AddError<Self::Error>>> + 'a
    where
        Self: 'a;

    /// Remove the blob with `key` from storage.
    ///
    /// Returns `true` if the blob was previously stored, `false` otherwise.
    fn remove_blob<'a>(&'a mut self, key: Key) -> Self::RemoveBlob<'a>;

    /// [`Future`] behind [`Storage::remove_blob`].
    type RemoveBlob<'a>: Future<Output = Result<bool, Self::Error>> + 'a
    where
        Self: 'a;
}

/// Error returned by [`Storage::add_blob`].
#[derive(Debug)]
pub enum AddError<E> {
    /// Blob is already stored.
    AlreadyStored(Key),
    /// Other, storage specific, error.
    Err(E),
}
