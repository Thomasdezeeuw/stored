//! Storage implementations.

use std::future::Future;

use crate::key::Key;

pub mod mem;
pub use mem::new as new_in_memory;

/// Trait to represent a BLOB (Binary Large OBject).
pub trait Blob {
    /// Length of the blob in bytes.
    fn len(&self) -> usize;
}

/// Write access to the storage.
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
    fn lookup(&self, key: Key) -> Self::Lookup;

    /// [`Future`] behind [`Storage::lookup`].
    type Lookup: Future<Output = Result<Option<Self::Blob>, Self::Error>>;

    /// Returns `true` if the storage contains a blob corresponding to `key`,
    /// `false` otherwise.
    fn contains(&self, key: Key) -> Self::Contains;

    /// [`Future`] behind [`Storage::contains`].
    type Contains: Future<Output = Result<bool, Self::Error>>;

    /// Add `blob` to the storage.
    fn add_blob(&mut self, blob: &[u8]) -> Self::AddBlob;

    /// [`Future`] behind [`Storage::add_blob`].
    type AddBlob: Future<Output = Result<Key, AddError<Self::Error>>>;

    /// Remove the blob with `key` from storage.
    ///
    /// Returns `true` if the blob was previously stored, `false` otherwise.
    fn remove_blob(&mut self, key: Key) -> Self::RemoveBlob;

    /// [`Future`] behind [`Storage::remove_blob`].
    type RemoveBlob: Future<Output = Result<bool, Self::Error>>;
}

/// Error returned by [`Write::add_blob`].
#[derive(Debug)]
pub enum AddError<E> {
    /// Blob is already stored.
    AlreadyStored(Key),
    /// Other, storage specific, error.
    Err(E),
}
