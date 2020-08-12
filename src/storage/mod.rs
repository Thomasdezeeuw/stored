//! Module with the `Storage` handle to a database.
//!
//! # Design
//!
//! The database is split into two files: the data file and the index file. Both
//! files are append-only logs [^1]. The index file determines what blobs are
//! actually in the database, this means that even though the bytes might be in
//! the data file it doesn't mean that those bytes are in the database. The
//! index file has the final say in what blobs are and aren't in the database.
//! The idea of splitting the storing of values (blobs) from the keys in
//! different files is taking from the paper [WiscKey: Separating Keys from
//! Values in SSD-conscious Storage] by Lanyue Lu et al.
//!
//! [WiscKey: Separating Keys from Values in SSD-conscious Storage]: https://www.usenix.org/conference/fast16/technical-sessions/presentation/lu
//!
//! [^1]: Not really, but pretend like they are for now.
//!
//! ## Storing blobs
//!
//! Storing new blobs to the store is done in two phases. First, by appending
//! the blob's bytes to the data file. Note that in this phase we don't yet
//! ensure the blob is fully synced to disk. Second, we fully sync the blob to
//! the data file and add a new entry to the index file, ensuring the entry is
//! also fully synced. Only after those two steps is a blob considered stored.
//!
//! In the code this is done by first calling [`Storage::add_blob`]. This adds
//! the blob's bytes to the data file and returns a [`StoreBlob`] query. This
//! query can then be committed (by calling [`Storage::commit`]) or aborted (by
//! calling [`Storage::abort`]). If the query is committed an entry is added to
//! the index file, ensuring the blob is stored in the database. Only after the
//! query is committed the blob can be looked up (using [`Storage::lookup`]).
//! However if the query is aborted, or never used again, no index entry will be
//! created for the blob and it will thus not be stored in the database. The
//! bytes stored in the data file for the blob will be left in place.
//!
//! ## Removing blobs
//!
//! Just like storing blobs, removing blobs is done in two phases. In the first
//! phase the in-memory hash map is checked if the blob is present. If the blob
//! is present it returns a [`RemoveBlob`] query, if its not present it returns
//! [`RemoveResult::NotStored`].
//!
//! In the second phase, if the query is committed, the time of blob's entry in
//! the index file will be overwritten with a removed time. Next the in-memory
//! hash map is updated to mark the blob as removed. The other fields, mainly
//! the `offset` and `length` fields, are not changed. These fields will be used
//! to remove the blob's bytes from the data file at a later stage. If the query
//! is aborted nothing happens, as nothing was done.
//!
//! ## In case of failures
//!
//! Any database storage implementation should be resistant against failures of
//! various kinds. One assumption this implementation makes is that the
//! underlying storage is reliable, that is if bytes are written and synced to
//! disk we expect those bytes stored properly and to be returned (read) as is,
//! without corruption. File systems implement various methods to avoid and
//! recover from corruption, for that reason we don't and instead depend on the
//! file system to take of this for us. Note however that is possible to
//! validate the database, [see below](#validating-the-database).
//!
//! As per the documentation above storing blobs is a two step process. If there
//! is a failure in step one, for example if the application crashes before the
//! all bytes are written to disk, it means that we have bytes in the data file
//! which don't have an entry in the index file. This is fine, the index file
//! determines what blobs are in the database, and as there is no entry in the
//! index that points to these possibly invalid bytes the database is not
//! corrupted.
//!
//! If there is a failure is step two we need to make a choose: either dropping
//! a (possibly) corrupt entry or trying to restore it. TODO: make and implement
//! this choose.
//!
//! ## Append-only log, but not really
//!
//! In an ideal world the data and index files would actually be append-only
//! logs. However we don't live in an ideal world. In this less than ideal world
//! we also need to remove blobs, complying with laws such as GDPR. For this
//! reason we need to invalidate the entry in the index file and overwrite the
//! bytes in the data file, ensuring they can't be read anymore. TODO: implement
//! this.
//!
//! # Validating the database
//!
//! Each file in the database, that is the index and data files, start with a
//! magic header that is checked each time the database is opened to ensure the
//! correct files are opened.
//!
//! Since all entries hold the key for the blob each blob can validated using the
//! key as checksum. This will point out any corruptions and could help in
//! restoring them. TODO: implement this.

// TODO: benchmark the following flags to mmap:
// - MAP_HUGETLB, with:
//   - MAP_HUGE_2MB, or
//   - MAP_HUGE_1GB
// - MAP_POPULATE

// TODO: add some safeguard to ensure the `StoreBlob` and `RemoveBlob` queries
// can only be applied to the correct `Storage`? Important when the database
// actor is restarted.

// TODO: use the invalid bit in `Datetime.subsec_nanos` to indicate that the
// storing the blob was aborted? That would make it easier for the cleanup.

// TODO: if an index entry is detected, or if the index file length is incorrect
// should we validate the database?

// TODO: benchmarks using `MAP_PRIVATE` and `PROT_READ` for already written
// data (e.g. read-only at that point) in `Data`.

// TODO: see if some of the assertions can be debug assertions.

// TODO: DRY the various `MmapSlice` variants.

use std::cmp::max;
use std::collections::hash_map::{self, HashMap};
use std::convert::TryInto;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{self, Read, Write};
use std::iter::FusedIterator;
use std::marker::PhantomData;
use std::mem::{align_of, replace, size_of, MaybeUninit};
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::ptr::{self, NonNull};
use std::sync::atomic::{self, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};
use std::{fmt, slice};

use fxhash::FxBuildHasher;
use log::{error, trace, warn};

use crate::key::Key;

#[cfg(test)]
use std::thread;

mod validate;

#[cfg(test)]
mod tests;

pub use validate::{validate, validate_storage, Corruption};

/// The size of a single page.
// TODO: ensure this is correct on all architectures. Tested in
// `data::page_size` test in the tests module.
pub const PAGE_SIZE: usize = 1 << PAGE_BITS; // 4096.
const PAGE_BITS: usize = 12;

/// Number of pages to pre-allocate for the [`Data`] file.
const DATA_PRE_ALLOC_PAGES: usize = 16;
const DATA_PRE_ALLOC_BYTES: usize = DATA_PRE_ALLOC_PAGES * PAGE_SIZE; // 65,536 bytes.

/// Magic header strings for the data and index files.
const DATA_MAGIC: &[u8] = b"Stored data v01\0"; // Null padded to 16 bytes.
const INDEX_MAGIC: &[u8] = b"Stored index v01";

/// A Binary Large OBject (BLOB).
#[derive(Clone)]
pub struct Blob {
    mmap_slice: MmapSlice<u8>,
    /// Date at which the `Blob` was created/stored.
    created: SystemTime,
}

impl Blob {
    /// Returns the bytes that make up the `Blob`.
    pub fn bytes<'b>(&'b self) -> &'b [u8] {
        self.mmap_slice.as_slice()
    }

    /// Returns the length of `Blob`.
    pub fn len(&self) -> usize {
        self.mmap_slice.len()
    }

    /// Returns the time at which the blob was stored.
    pub fn created_at(&self) -> SystemTime {
        self.created
    }

    /// Prefetch the bytes that make up this `Blob`.
    ///
    /// # Notes
    ///
    /// If this returns an error it doesn't mean the bytes are inaccessible.
    pub fn prefetch(&self) -> io::Result<()> {
        // TODO: benchmark at what size it makes sense to use this.
        if self.len() > PAGE_SIZE {
            self.mmap_slice
                .madvise(libc::MADV_SEQUENTIAL | libc::MADV_WILLNEED)
        } else {
            Ok(())
        }
    }
}

impl fmt::Debug for Blob {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Blob")
            .field("bytes", &self.bytes())
            .field("created", &self.created)
            .finish()
    }
}

/// A Binary Large OBject (BLOB) that is **not** committed to [`Storage`].
#[derive(Clone)]
pub struct UncommittedBlob {
    mmap_slice: MmapSlice<u8>,
    /// Offset in the data file.
    offset: u64,
}

impl UncommittedBlob {
    /// Returns the bytes that make up the `UncommittedBlob`.
    pub fn bytes<'b>(&'b self) -> &'b [u8] {
        self.mmap_slice.as_slice()
    }

    /// Returns the length of `UncommittedBlob`.
    pub fn len(&self) -> usize {
        self.mmap_slice.len()
    }

    /// Prefetch the bytes that make up this `UncommittedBlob`.
    ///
    /// # Notes
    ///
    /// If this returns an error it doesn't mean the bytes are inaccessible.
    pub fn prefetch(&self) -> io::Result<()> {
        // TODO: benchmark at what size it makes sense to use this.
        if self.len() > PAGE_SIZE {
            self.mmap_slice
                .madvise(libc::MADV_SEQUENTIAL | libc::MADV_WILLNEED)
        } else {
            Ok(())
        }
    }
}

impl fmt::Debug for UncommittedBlob {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("UncommittedBlob")
            .field("bytes", &self.bytes())
            .finish()
    }
}

/// Handle to a database.
#[derive(Debug)]
pub struct Storage {
    index: Index,
    /// All blobs currently in [`Index`] and [`Data`].
    ///
    /// # Safety
    ///
    /// `blobs` must be declared before `data` because it must be dropped before
    /// `data`.
    // TODO: use different hashing algorithm.
    blobs: HashMap<Key, (EntryIndex, BlobEntry), FxBuildHasher>,
    /// Uncommitted blobs.
    ///
    /// The `usize` is the number of [`StoreBlob`]s adding the blob.
    uncommitted: HashMap<Key, (usize, UncommittedBlob), FxBuildHasher>,
    /// Length of `blob` that are not [`BlobEntry::Removed`].
    length: usize,
    /// # Safety
    ///
    /// Must outlive `blobs`, the lifetime `'s` refers to this.
    data: Data,
}

/// Blob entry in the [`Storage`].
#[derive(Clone, Debug)]
pub enum BlobEntry {
    /// Blob is stored.
    Stored(Blob),
    /// Blob was removed at the provided time.
    Removed(SystemTime),
}

impl Storage {
    /// Open a database.
    ///
    /// `path` must be a directory with the `index` and `data` files.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Storage> {
        let path = path.as_ref();

        // Ensure the directory exists.
        trace!("creating directories for database: path={}", path.display());
        create_dir_all(path)?;

        let data = Data::open(path.join("data"))?;
        let mut index = Index::open(path.join("index"))?;

        // All blobs currently in the database.
        let entries = index.entries()?;
        // Length of stored (i.e. not removed) blobs.
        let mut length = 0;
        let blobs = entries
            .iter()
            .filter_map(|(index, entry)| {
                let res = match entry.modified_time() {
                    ModifiedTime::Created(time) => data
                        .slice(entry.offset(), entry.length() as usize)
                        .map(|mmap_slice| {
                            length += 1;
                            BlobEntry::Stored(Blob {
                                mmap_slice,
                                created: time,
                            })
                        })
                        .ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidData, "incorrect index entry")
                        }),
                    ModifiedTime::Removed(time) => Ok(BlobEntry::Removed(time)),
                    ModifiedTime::Invalid => return None,
                };
                Some(res.map(|blob_entry| (entry.key().clone(), (index, blob_entry))))
            })
            .collect::<io::Result<_>>()?;
        drop(entries);

        Ok(Storage {
            data,
            index,
            blobs,
            uncommitted: HashMap::with_hasher(FxBuildHasher::default()),
            length,
        })
    }

    /// Returns the number of blobs stored in the database.
    pub fn len(&self) -> usize {
        self.length
    }

    /// Returns the number of bytes of data stored.
    ///
    /// # Notes
    ///
    /// While `Storage` is opened this will **not** match the file size of the
    /// data file.
    pub fn data_size(&self) -> u64 {
        self.data.used_bytes()
    }

    /// Returns the size of the index file in bytes.
    pub fn index_size(&self) -> u64 {
        self.index.file_length()
    }

    /// Returns the total size of the database file (data and index).
    pub fn total_size(&self) -> u64 {
        self.data_size() + self.index_size()
    }

    /// Returns a reference to the `Blob` corresponding to `key`, if stored.
    pub fn lookup(&self, key: &Key) -> Option<BlobEntry> {
        self.blobs
            .get(key)
            .map(|(_, blob_entry)| blob_entry.clone())
    }

    /// Returns `true` if the storage contains (either stored or removed) a blob
    /// corresponding to `key`, `false` otherwise.
    pub fn contains(&self, key: &Key) -> bool {
        self.blobs.contains_key(key)
    }

    /// Returns `true` if a blob corresponding to `key` is stored in the
    /// storage, `false` otherwise.
    pub fn is_stored(&self, key: &Key) -> bool {
        matches!(self.blobs.get(key), Some((.., BlobEntry::Stored(..))))
    }

    /// Lookup an uncommitted blob.
    pub fn lookup_uncommitted(&self, key: &Key) -> Option<UncommittedBlob> {
        self.uncommitted.get(key).map(|(_, blob)| blob.clone())
    }

    /// Returns an iterator over the `Entry`'s `Key`s.
    pub fn keys(&mut self) -> io::Result<Keys> {
        self.index.entries().map(|slice| Keys { slice })
    }

    /// Add `blob` to the storage.
    ///
    /// Only after the returned [query] is [committed] is the blob stored in the
    /// database.
    ///
    /// # Notes
    ///
    /// There is an implicit lifetime between the returned [`StoreBlob`] query
    /// and this `Storage`. The query can only be commit or aborted to this
    /// `Storage`, the query may however safely outlive `Storage`.
    ///
    /// [query]: StoreBlob
    /// [committed]: Storage::commit
    pub fn add_blob(&mut self, blob: &[u8]) -> AddResult {
        let key = Key::for_blob(blob);
        trace!("adding blob: key={} length={}", key, blob.len());

        // Can't have double entries.
        if let Some((_, BlobEntry::Stored(_))) = self.blobs.get(&key) {
            return AddResult::AlreadyStored(key);
        }

        // Check if another query is already running to add the blob.
        if let Some((count, _)) = self.uncommitted.get_mut(&key) {
            // We can reuse the same query.
            *count += 1;
            return AddResult::Ok(StoreBlob { key });
        }

        // First add the blob to the data file. If something happens the blob
        // will be written (or not), but its not *in* the database as the index
        // defines what is in the database.
        match self.data.add_blob(blob) {
            Ok((mmap_slice, offset)) => {
                let uncommitted_blob = UncommittedBlob { mmap_slice, offset };
                self.uncommitted.insert(key.clone(), (1, uncommitted_blob));
                AddResult::Ok(StoreBlob { key })
            }
            Err(err) => AddResult::Err(err),
        }
    }

    /// Remove the blob with `key` from the database.
    ///
    /// Only after the returned [query] is [committed] is the blob removed from
    /// the database.
    ///
    /// # Notes
    ///
    /// There is an implicit lifetime between the returned [`RemoveBlob`] query
    /// and this `Storage`. The query can only be commit or aborted to this
    /// `Storage`, the query may however safely outlive `Storage`.
    ///
    /// [query]: RemoveBlob
    /// [committed]: Storage::commit
    pub fn remove_blob(&mut self, key: Key) -> RemoveResult {
        trace!("removing blob: key={}", key);
        match self.blobs.get(&key) {
            Some((_, BlobEntry::Stored(_))) => RemoveResult::Ok(RemoveBlob { key }),
            Some((_, BlobEntry::Removed(time))) => RemoveResult::NotStored(Some(*time)),
            None => RemoveResult::NotStored(None),
        }
    }

    /// Commit to `query`.
    pub fn commit<Q>(&mut self, query: Q, timestamp: SystemTime) -> io::Result<SystemTime>
    where
        Q: Query,
    {
        query.commit(self, timestamp)
    }

    /// Abort `query`.
    pub fn abort<Q>(&mut self, query: Q) -> io::Result<()>
    where
        Q: Query,
    {
        query.abort(self)
    }

    /// This is equal to [`Storage::add_blob`] followed by [committing] to the
    /// [`StoreBlob`] query. Used to synchronise blobs.
    ///
    /// [committing]: Storage::commit
    pub fn store_blob(&mut self, blob: &[u8], created_at: SystemTime) -> io::Result<SystemTime> {
        let key = Key::for_blob(blob);
        trace!(
            "storing blob: key={}, length={}, created_at={:?}",
            key,
            blob.len(),
            created_at
        );

        // Can't have double entries.
        if let Some((_, BlobEntry::Stored(blob))) = self.blobs.get(&key) {
            return Ok(blob.created_at());
        }

        // First add the blob to the data file. If something happens the blob
        // will be written (or not), but its not *in* the database as the index
        // defines what is in the database.
        let (mmap_slice, offset) = self.data.add_blob(blob)?;
        let uncommitted_blob = UncommittedBlob { mmap_slice, offset };
        let query = StoreBlob { key: key.clone() };
        let (entry_index, blob) =
            query.create_entry(&mut self.index, created_at, uncommitted_blob)?;
        self.blobs
            .insert(key, (entry_index, BlobEntry::Stored(blob)));
        self.length += 1;
        Ok(created_at)
    }

    pub fn store_removed_blob(
        &mut self,
        key: Key,
        removed_at: SystemTime,
    ) -> io::Result<SystemTime> {
        trace!(
            "storing removed blob: key={}, removed_at={:?}",
            key,
            removed_at
        );

        if let Some((entry_index, blob_entry)) = self.blobs.get_mut(&key) {
            if let BlobEntry::Removed(time) = blob_entry {
                // TODO: should we use the minimum time?
                Ok(*time)
            } else {
                let res = self
                    .index
                    .mark_as_removed(*entry_index, removed_at)
                    .map(|()| {
                        *blob_entry = BlobEntry::Removed(removed_at);
                        removed_at
                    });
                self.length -= 1;
                res
            }
        } else {
            let entry = Entry {
                key,
                offset: 0,
                length: 0,
                time: ModifiedTime::Removed(removed_at).into(),
            };
            self.index.add_entry(&entry).map(|entry_index| {
                self.blobs
                    .insert(entry.key, (entry_index, BlobEntry::Removed(removed_at)));
                removed_at
            })
        }
    }
}

/// Result returned by [`Storage::add_blob`].
#[derive(Debug)]
pub enum AddResult {
    /// Blob was successfully stored, but not yet added to the index nor
    /// database.
    Ok(StoreBlob),
    /// Blob is already stored.
    AlreadyStored(Key),
    /// I/O error.
    Err(io::Error),
}

/// Result returned by [`Storage::remove_blob`].
#[derive(Debug)]
pub enum RemoveResult {
    /// Blob is prepared to be removed, but not yet removed from the database.
    Ok(RemoveBlob),
    /// Key is not stored in the database.
    NotStored(Option<SystemTime>),
}

/// a `Query` is a partially prepared storage operation.
///
/// An example is [`StoreBlob`] that already has the data of the blob stored,
/// but the blob itself isn't yet in the database. Only after [committing] will
/// the blob be stored (and thus accessible).
///
/// [committing]: Query::commit
pub trait Query {
    /// Returns the query for this query.
    fn key(&self) -> &Key;

    /// Commit to the query.
    ///
    /// Returns the time at which the operation was completed. This is the same
    /// time as the provided `timestamp`, unless the operation was previously
    /// already completed. For example if while removing a blob, the blob was
    /// already removed, this returns the time at which the blob was removed
    /// first.
    fn commit(self, storage: &mut Storage, timestamp: SystemTime) -> io::Result<SystemTime>;

    /// Abort the query.
    fn abort(self, storage: &mut Storage) -> io::Result<()>;
}

/// A [`Query`] to [store a blob] to the [`Storage`].
///
/// [store a blob]: Storage::add_blob
#[derive(Debug)]
pub struct StoreBlob {
    // TODO: maybe add `slice: MmapSlice<u8>`? to get access to the complete
    // value, useful to support streaming.
    key: Key,
}

impl StoreBlob {
    /// Create an `Entry` in `storage.index`.
    ///
    /// This ensures that the blob for which the entry is created is synced to
    /// disk.
    fn create_entry(
        self,
        index: &mut Index,
        created_at: SystemTime,
        blob: UncommittedBlob,
    ) -> io::Result<(EntryIndex, Blob)> {
        // Ensure the data is synced to disk.
        blob.mmap_slice.sync()?;

        // The data is already stored so we can add the blob to the
        // index.
        let index_entry = Entry::new(self.key, blob.offset, blob.len() as u32, created_at);
        let entry_index = index.add_entry(&index_entry)?;

        // Now that the data and index entry are stored we can insert
        // the blob into our database.
        Ok((
            entry_index,
            Blob {
                mmap_slice: blob.mmap_slice,
                created: created_at,
            },
        ))
    }

    /// Remove blob bytes.
    fn remove_blob_bytes(self, _storage: &mut Storage) -> io::Result<()> {
        trace!("removing unused blob bytes: key={}", self.key);
        /* TODO: cleanup the unused bytes.
        // We add an entry with an invalid time to indicate the bytes are
        // unused.
        let index_entry = Entry::new(self.key, self.offset, self.length, DateTime::INVALID);
        storage.index.add_entry(&index_entry).map(|_| ())
        */
        Ok(())
    }
}

impl Query for StoreBlob {
    fn key(&self) -> &Key {
        &self.key
    }

    fn commit(self, storage: &mut Storage, created_at: SystemTime) -> io::Result<SystemTime> {
        trace!(
            "committing to storing blob: key={}, created_at={:?}",
            self.key,
            created_at
        );
        let uncommitted_blob = match storage.uncommitted.remove(&self.key) {
            Some((_, uncommitted_blob)) => uncommitted_blob,
            None => match storage.lookup(&self.key) {
                Some(BlobEntry::Stored(blob)) => return Ok(blob.created_at()),
                _ => unreachable!("committing StoreBlob without preparing it"),
            },
        };

        debug_assert_eq!(
            storage
                .data
                .slice(uncommitted_blob.offset, uncommitted_blob.mmap_slice.length)
                .unwrap()
                .as_slice(),
            uncommitted_blob.mmap_slice.as_slice()
        );

        use hash_map::Entry::*;
        match storage.blobs.entry(self.key.clone()) {
            Occupied(mut entry) => match entry.get_mut() {
                (_, BlobEntry::Stored(blob)) => {
                    trace!("blob already committed: key={}", self.key);
                    let created_at = blob.created_at();
                    // This should never happen, but just in case it does we
                    // don't want to modify the existing entry.
                    self.remove_blob_bytes(storage).map(|()| created_at)
                }
                entry @ (_, BlobEntry::Removed(_)) => {
                    // First add our new index entry.
                    let (entry_index, blob) =
                        self.create_entry(&mut storage.index, created_at, uncommitted_blob)?;
                    let (old_entry_index, _) =
                        replace(entry, (entry_index, BlobEntry::Stored(blob)));
                    storage.length += 1;

                    // Next mark the old index entry as invalid.
                    storage
                        .index
                        .mark_as_invalid(old_entry_index)
                        .map(|()| created_at)
                }
            },
            Vacant(entry) => {
                let (entry_index, blob) =
                    self.create_entry(&mut storage.index, created_at, uncommitted_blob)?;
                entry.insert((entry_index, BlobEntry::Stored(blob)));
                storage.length += 1;
                Ok(created_at)
            }
        }
    }

    fn abort(self, storage: &mut Storage) -> io::Result<()> {
        trace!("aborting storing blob: key={}", self.key);
        use hash_map::Entry::*;
        match storage.uncommitted.entry(self.key.clone()) {
            Occupied(mut entry) => match entry.get_mut() {
                (1, _) => {
                    // We're the only `StoreBlob` query that has access to these
                    // bytes, so we can safely remove them.
                    entry.remove();
                }
                (count, _) => {
                    // Another `StoreBlob` is still using the bytes.
                    *count -= 1;
                    return Ok(());
                }
            },
            // Blob already committed by another query.
            Vacant(..) => return Ok(()),
        }

        self.remove_blob_bytes(storage)
    }
}

/// A [`Query`] to [remove a blob] to the [`Storage`].
///
/// [remove a blob]: Storage::remove_blob
#[derive(Debug)]
pub struct RemoveBlob {
    key: Key,
}

impl Query for RemoveBlob {
    fn key(&self) -> &Key {
        &self.key
    }

    fn commit(self, storage: &mut Storage, removed_at: SystemTime) -> io::Result<SystemTime> {
        trace!(
            "committing to removing blob: key={}, removed_at={:?}",
            self.key,
            removed_at
        );
        let res = if let Some((entry_index, blob_entry)) = storage.blobs.get_mut(&self.key) {
            if let BlobEntry::Removed(time) = blob_entry {
                // The blob was removed in between the time the query was
                // created and commit; we'll keep using the original time.
                // TODO: should we use the minimum time?
                return Ok(*time);
            } else {
                storage
                    .index
                    .mark_as_removed(*entry_index, removed_at)
                    .map(|()| {
                        *blob_entry = BlobEntry::Removed(removed_at);
                        removed_at
                    })
            }
        } else {
            unreachable!("trying to commit to removing a blob that was never stored");
        };
        storage.length -= 1;
        res
    }

    fn abort(self, _storage: &mut Storage) -> io::Result<()> {
        trace!("aborting removing blob: key={}", self.key);
        // We don't have to do anything as we didn't change anything.
        Ok(())
    }
}

/// Handle for a data file.
#[derive(Debug)]
struct Data {
    /// Data file opened for reading and writing in append-only mode (for
    /// storing new blobs).
    ///
    /// Note: the seek position is unlikely to be correct after opening.
    file: File,
    /// Number of bytes used in the file.
    used_bytes: u64,
    /// Number of bytes `fallocate`d in the file and `mmap`-ed in `areas`.
    ///
    /// # Safety
    ///
    /// Must always be page-aligned!
    mmaped_bytes: u64,
    /// Non-overlapping memory mapped (`mmap(2)`-ed) areas.
    ///
    /// See `check` for notes about safety.
    areas: Vec<MmapAreaControl>,
}

impl Data {
    /// Protections used in calls to `mmap(2)`.
    const PROTECTIONS: libc::c_int = libc::PROT_READ | libc::PROT_WRITE;
    /// Flags used in calls to `mmap(2)`.
    const FLAGS: libc::c_int = libc::MAP_SHARED;

    /// Open a `Data` file.
    fn open<P: AsRef<Path>>(path: P) -> io::Result<Data> {
        let path = path.as_ref();

        trace!("opening data file: path={}", path.display());
        let mut data = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)
            .map(|file| Data {
                file,
                used_bytes: 0,
                mmaped_bytes: 0,
                areas: Vec::new(),
            })?;
        lock(&mut data.file)?;

        let metadata = data.file.metadata()?;
        data.used_bytes = metadata.len();
        if data.used_bytes == 0 {
            // New file so we need to add our magic.
            trace!("new data file, writing magic: file={:?}", data.file);
            data.file.write_all(&DATA_MAGIC)?;
            data.used_bytes = DATA_MAGIC.len() as u64;
        } else {
            // Existing file; we'll check if it has the magic header.
            let mut magic = [0; DATA_MAGIC.len()];
            let read_bytes = data.file.read(&mut magic)?;
            if read_bytes != DATA_MAGIC.len() || magic != DATA_MAGIC {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "missing magic header in data file",
                ));
            }
        }

        data.fallocate(data.used_bytes as usize)?;
        data.check();
        Ok(data)
    }

    /// Returns the used bytes of the file.
    fn used_bytes(&self) -> u64 {
        self.used_bytes
    }

    /// Add `blob` at the end of the data file.
    ///
    /// Returns the blob as `mmap(2)`-ed memory, backed by the data file, and
    /// the offset in the data file where the blob starts.
    ///
    /// # Notes
    ///
    /// The blob is not synced to disk, call [`sync`] on the returned slice to
    /// ensure that! Can't store an empty blob!
    ///
    /// [`sync`]: MmapSlice::sync
    fn add_blob(&mut self, blob: &[u8]) -> io::Result<(MmapSlice<u8>, u64)> {
        let offset = self.used_bytes;
        let mut slice = self.reserve(blob.len())?;
        slice.copy_from_slice(0, blob);
        // Let the OS we want to sync it to disk at a later stage.
        slice.start_sync()?;
        // Safety: `new_area` allocate `mmap` areas with `PROT_READ`.
        unsafe { Ok((slice.assume_init().immutable(), offset)) }
    }

    /// Reserve `length` bytes to write into.
    ///
    /// Returns a `mmap`-ed memory area that is backed by the data file.
    fn reserve(&mut self, length: usize) -> io::Result<MmapSliceMut<MaybeUninit<u8>>> {
        if self.used_bytes + length as u64 > self.mmaped_bytes {
            // Don't have enough space, allocate some more.
            self.fallocate(length)?;
        }

        // Safety: we ensure above that we have space in the last area, so this
        // `unwrap` is safe.
        let area = self.areas.last_mut().unwrap();
        let in_area_offset = self.used_bytes - area.file_offset();
        self.used_bytes += length as u64;
        unsafe { Ok(area.slice_mut_unchecked(in_area_offset as usize, length)) }
    }

    /// Allocate a new area of `length` bytes in the data file and add the newly
    /// allocated space to `mmap`-ed areas.
    ///
    /// `length` gets increase (if needed) to align to [`DATA_PRE_ALLOC_BYTES`].
    fn fallocate(&mut self, length: usize) -> io::Result<()> {
        // Always allocate at least `DATA_PRE_ALLOC_BYTES` bytes.
        let length = min_fallocate_size(length);

        // Allocate more space on disk.
        let new_file_length = self.mmaped_bytes + length as u64;
        trace!(
            "growing data file: file={:?}, old_length = {}, new_length={}",
            self.file,
            self.mmaped_bytes,
            new_file_length
        );
        fallocate(self.file.as_raw_fd(), new_file_length as libc::off_t)?;

        // Try to grow the last `mmap`ed area.
        if let Some(area) = self.areas.last_mut() {
            if area.try_grow_by(length, Self::PROTECTIONS, Self::FLAGS, &self.file)? {
                // Success.
                self.mmaped_bytes = new_file_length;
                return Ok(());
            }
        }
        // Mark the unused bytes from the last area as used. We can't use the
        // bytes as the blob is too large to fit into the leftover `mmap` area.
        // This space can be reclaimed later when compacting.
        // Note: we need to use `max` here as the first call (in `new`)
        // `mmap_bytes` will be 0 and `used_bytes` >= `INDEX_MAGIC.len()`.
        self.used_bytes = max(self.mmaped_bytes, self.used_bytes);
        // Safety: `fallocate_size` ensures that the length is page aligned.
        unsafe { self.new_area(length) }
    }

    /// Create a new `mmap` area.
    ///
    /// Adds this area to `self.area`.
    ///
    /// # Safety
    ///
    /// The data file must be atleast `self.mmaped_bytes + length` bytes long.
    /// `length` and `self.mmaped_bytes` must be page aligned.
    unsafe fn new_area(&mut self, length: libc::size_t) -> io::Result<()> {
        let area = MmapAreaControl::new(
            length,
            Self::PROTECTIONS,
            Self::FLAGS,
            &self.file,
            self.mmaped_bytes as libc::off_t,
        )?;
        self.areas.push(area);
        self.mmaped_bytes += length as u64;
        Ok(())
    }

    /// Returns the address for the blob at `offset`, with `length`. Or `None`
    /// if the combination is invalid.
    ///
    /// # Notes
    ///
    /// The returned address lifetime is tied to the `Data` struct.
    fn slice(&self, offset: u64, length: usize) -> Option<MmapSlice<u8>> {
        debug_assert!(
            offset >= DATA_MAGIC.len() as u64,
            "offset inside magic header"
        );
        self.areas
            .iter()
            .find_map(|area| area.slice(offset, length))
    }

    /// Run all safety checks.
    ///
    /// Checks the following:
    ///
    /// * Ensures all `MmapArea.mmap_address`es are page aligned (per `PAGE_SIZE`).
    /// * All `MmapArea.offset`s are positive.
    /// * `self.areas` is sorted by `MmapArea.offset`.
    /// * All `MmapArea.offset`s are valid: `(0..D).sum(length) == offset`,
    ///   for each D in `self.areas` (all `Mmap.offset`s are the sum of the
    ///   mmaped so far).
    /// * The total length matches the length of all mmaped slices: `self.length
    ///   == self.areas.sum(length)`.
    ///
    /// # Notes
    ///
    /// This is only run with `debug_assertions` on, i.e. its a no-op in release
    /// mode.
    fn check(&self) {
        if cfg!(debug_assertions) {
            let mut total_length: u64 = 0;
            let mut last_offset: u64 = 0;
            for control in &self.areas {
                let area = control.area();
                assert!(
                    is_page_aligned(area.mmap_address.as_ptr() as usize),
                    //area.mmap_address.as_ptr() as usize % PAGE_SIZE == 0,
                    "incorrect mmap address alignment"
                );
                assert!(
                    area.mmap_offset as u64 <= DATA_MAGIC.len() as u64,
                    "incorrect offset"
                );
                assert!(
                    area.mmap_offset as u64 >= last_offset,
                    "mmaped areas not sorted by offset"
                );
                assert_eq!(
                    area.mmap_offset as u64, total_length,
                    "incorrect offset for mmap area"
                );
                last_offset = area.mmap_offset as u64;
                total_length += area.mmap_length as u64;
            }
            assert!(total_length >= self.used_bytes, "incorrect used_bytes");
        }
    }
}

/// Returns the size for `length` to allocate. Ensure the length is aligned to
/// `DATA_PRE_ALLOC_BYTES`.
const fn min_fallocate_size(length: usize) -> usize {
    length + (DATA_PRE_ALLOC_BYTES - (length % DATA_PRE_ALLOC_BYTES))
}

impl Drop for Data {
    fn drop(&mut self) {
        // Shrink the file back to the actual used bytes.
        // NOTE: this doesn't happen in case of a crash and that is OK. We'll
        // end up with a gap of unused bytes, but that will be cleaned-up by the
        // compacting phase.
        if let Err(err) = ftruncate(self.file.as_raw_fd(), self.used_bytes as libc::off_t) {
            error!(
                "failed to truncate file: {}: file={:?}, current_length={}, actual_length={}",
                err, self.file, self.mmaped_bytes, self.used_bytes
            );
        }
    }
}

/// Handle for an index file.
#[derive(Debug)]
struct Index {
    /// Index file opened for reading (for use in `entries`) and writing in
    /// append-only mode for adding entries (`add_entry`).
    file: File,
    /// Current length of the file.
    ///
    /// Must always be number of entries * `size_of::<Entry>()` +
    /// `INDEX_MAGIC.len()`. See [`len`] method.
    length: u64,
}

impl Index {
    /// Open an index file.
    fn open<P: AsRef<Path>>(path: P) -> io::Result<Index> {
        let path = path.as_ref();
        trace!("opening index file: path={}", path.display());
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            // NOTE: we should use `O_APPEND` here, but Linux has a bug with
            // `pwrite` where it will just ignore the offset provided and
            // instead just append the bytes at the end of the file. This breaks
            // `Index::overwrite_date` forcing us to use `O_RDWR` instead. This
            // puts more importance on `lock`ing the file, but that is broken in
            // various scenarios as well (e.g. see
            // https://www.sqlite.org/lockingv3.html#how_to_corrupt), so great
            // stuff all-round!
            .write(true)
            .open(path)?;
        lock(&mut file)?;

        let metadata = file.metadata()?;
        let mut length = metadata.len();
        if length == 0 {
            // New file so we need to add our magic.
            trace!("new index file, writing magic");
            file.write_all(&INDEX_MAGIC)?;
            length += INDEX_MAGIC.len() as u64;
        } else {
            // Existing file; we'll check if it has the magic header and correct
            // length.
            let mut magic = [0; INDEX_MAGIC.len()];
            let read_bytes = file.read(&mut magic)?;
            if read_bytes != INDEX_MAGIC.len() || magic != INDEX_MAGIC {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "missing magic header in index file",
                ));
            } else if (length as usize - INDEX_MAGIC.len()) % size_of::<Entry>() != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "incorrect index file size",
                ));
            }
        }

        Ok(Index { file, length })
    }

    /// Returns the total file length.
    fn file_length(&self) -> u64 {
        self.length
    }

    /// Returns the number of entries in the index.
    fn len(&self) -> usize {
        (self.length as usize - INDEX_MAGIC.len()) / size_of::<Entry>()
    }

    /// Returns an iterator for all entries remaining in the `Index`. If the
    /// `Index` was just `open`ed this means all entries in the entire index
    /// file.
    fn entries(&mut self) -> io::Result<MmapSliceOwned<Entry>> {
        let slice = MmapSliceOwned::new(
            self.len(),
            libc::PROT_READ,
            libc::MAP_PRIVATE,
            &self.file,
            INDEX_MAGIC.len(),
        )?;

        // For performance let the OS known we're going to read the entries so
        // it can prefetch the pages from disk.
        // Note that we don't care about the result as its just an advise to the
        // OS, if t can't comply we'll continue on.
        if let Err(err) = slice.madvise(libc::MADV_SEQUENTIAL | libc::MADV_WILLNEED) {
            warn!("madvise failed, continuing: {}", err);
        }

        Ok(slice)
    }

    /// Add a new `entry` to the `Index`.
    fn add_entry(&mut self, entry: &Entry) -> io::Result<EntryIndex> {
        debug_assert!(
            (entry.offset >= DATA_MAGIC.len() as u64) ||
            // Entries for removed blob are allowed an offset of 0.
            (entry.offset == 0 && entry.length == 0),
            "offset inside magic header"
        );
        // Safety: because `u8` doesn't have any invalid bit patterns this is
        // OK. We're also ensured at least `size_of::<Entry>` bytes are valid.
        let bytes: &[u8] =
            unsafe { slice::from_raw_parts(entry as *const _ as *const _, size_of::<Entry>()) };
        trace!("adding entry to index file: entry={:?}", entry);
        self.file
            .write_all(&bytes)
            .and_then(|()| self.file.sync_all())
            .map(|()| {
                let entry_index = EntryIndex(
                    ((self.length - (INDEX_MAGIC.len() as u64)) / (bytes.len() as u64)) as usize,
                );
                self.length += bytes.len() as u64;
                // Ensure we the file size is correct.
                debug_assert_eq!(self.file.metadata().unwrap().len(), self.length);
                entry_index
            })
    }

    /// Marks the `Entry` at index `at` as removed at `removed_at` time.
    fn mark_as_removed(&mut self, at: EntryIndex, removed_at: SystemTime) -> io::Result<()> {
        trace!(
            "marking entry in index file as removed: entry_index={:?}",
            at
        );
        // The date at which the blob was removed.
        let date = DateTime::from(removed_at).mark_removed();
        self.overwrite_date(at, &date)
    }

    /// Marks the `Entry` at index `at` as invalid.
    fn mark_as_invalid(&mut self, at: EntryIndex) -> io::Result<()> {
        trace!(
            "marking entry in index file as invalid: entry_index={:?}",
            at
        );
        self.overwrite_date(at, &DateTime::INVALID)
    }

    /// Overwrite the date of the `Entry` at index `at` with `date`.
    fn overwrite_date(&mut self, at: EntryIndex, date: &DateTime) -> io::Result<()> {
        debug_assert!(
            ((at.0 as u64) * (size_of::<Entry>() as u64) + (size_of::<Entry>() as u64))
                < self.length,
            "index outside of index file"
        );

        // NOTE: this is only correct because `Entry` and `DateTime` have the
        // `#[repr(C)]` attribute and thus the layout is fixed.
        let offset = at.offset() + ((size_of::<Entry>() - size_of::<DateTime>()) as u64);
        self.file
            .write_all_at(date.as_bytes(), offset)
            .and_then(|_| {
                // Ensure we didn't change the file size.
                debug_assert_eq!(self.file.metadata().unwrap().len(), self.length);
                self.file.sync_all()
            })
    }
}

impl MmapSliceOwned<Entry> {
    /// Returns an iterator over the `Entry`s and its index into the [`Index`]
    /// file.
    fn iter(
        &self,
    ) -> impl Iterator<Item = (EntryIndex, &Entry)> + ExactSizeIterator + FusedIterator {
        self.as_slice()
            .iter()
            .enumerate()
            .map(|(idx, entry)| (EntryIndex(idx), entry))
    }
}

/// Iterator of the [`Key`]s in the [`Storage`].
pub struct Keys {
    slice: MmapSliceOwned<Entry>,
}

impl fmt::Debug for Keys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.slice.as_slice().fmt(f)
    }
}

impl<'a> IntoIterator for &'a Keys {
    type Item = &'a Key;
    type IntoIter = impl Iterator<Item = Self::Item> + ExactSizeIterator + FusedIterator;

    fn into_iter(self) -> Self::IntoIter {
        self.slice.as_slice().iter().map(|entry| &entry.key)
    }
}

/// Index of an [`Entry`] in the [`Index`] file.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
struct EntryIndex(usize);

impl EntryIndex {
    /// Returns the offset of the [`Entry`] into the [`Index`] file in bytes.
    fn offset(self) -> u64 {
        INDEX_MAGIC.len() as u64 + (self.0 as u64 * size_of::<Entry>() as u64)
    }
}

/// Entry in the [`Index`].
///
/// The layout of the `Entry` is fixed as it must be loaded from disk.
///
/// # Notes
///
/// Integers in `Entry` (and `DateTime`) are stored in big-endian format on
/// disk. Use the getters (e.g. `offset`) to get the value in native endian.
#[repr(C)]
#[derive(Eq, PartialEq)]
struct Entry {
    /// Key for the blob.
    key: Key,
    /// Offset into the data file.
    ///
    /// If this is `u64::MAX` the blob has been removed.
    offset: u64,
    /// Length of the blob in bytes.
    length: u32,
    /// Time at which the blob is created or removed.
    time: DateTime,
}

impl Entry {
    /// Create a new `Entry` formatted correctly to be stored on disk, i.e.
    /// integer set to use big-endian.
    fn new<T>(key: Key, offset: u64, length: u32, created_at: T) -> Entry
    where
        T: Into<DateTime>,
    {
        Entry {
            key,
            offset: u64::from_ne_bytes(offset.to_be_bytes()),
            length: u32::from_ne_bytes(length.to_be_bytes()),
            // `From<SystemTime> for DateTime` always returns a created at time.
            time: created_at.into(),
        }
    }

    /// Returns the `Key` for the entry.
    fn key(&self) -> &Key {
        &self.key
    }

    /// Returns the offset for the entry, in native endian.
    fn offset(&self) -> u64 {
        u64::from_be_bytes(self.offset.to_ne_bytes())
    }

    /// Returns the length for the entry, in native endian.
    fn length(&self) -> u32 {
        u32::from_be_bytes(self.length.to_ne_bytes())
    }

    /// Returns the time at which this entry was created.
    fn modified_time(&self) -> ModifiedTime {
        self.time.into()
    }
}

impl fmt::Debug for Entry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Entry")
            .field("key", &self.key)
            .field("offset", &u64::from_be_bytes(self.offset.to_ne_bytes()))
            .field("length", &u32::from_be_bytes(self.length.to_ne_bytes()))
            .field("time", &self.time)
            .finish()
    }
}

/// Layout stable date-time format.
///
/// This is essentially a [`Duration`] since [unix epoch] with a stable on-disk
/// layout.
///
/// [unix epoch]: std::time::SystemTime::UNIX_EPOCH
///
/// # Notes
///
/// Can't represent times before Unix epoch.
/// Integers are stored in big-endian format on disk **and in memory**.
#[repr(C, packed)] // Packed to reduce the size of `Index`.
#[derive(Copy, Clone, Eq, PartialEq)]
pub struct DateTime {
    /// Number of seconds since Unix epoch.
    seconds: u64,
    /// The first 30 bit make up the number of nano sub-seconds, always less
    /// then 1 billion (the number of nanoseconds in a second).
    ///
    /// The last bit (31st) is the `REMOVED_BIT` and if set marks the `DateTime`
    /// as a removed at time (`ModifiedTime::Removed`).
    subsec_nanos: u32,
}

impl DateTime {
    /// Bit that is set in `subsec_nanos` if the `DateTime` is represented as an
    /// removed time.
    const REMOVED_BIT: u32 = 1 << 31;

    /// Bit that is set in `subsec_nanos` if the `DateTime` is invalid.
    const INVALID_BIT: u32 = 1 << 30;

    /// `DateTime` that represents an invalid time.
    pub const INVALID: DateTime = DateTime {
        seconds: 0,
        subsec_nanos: u32::from_be_bytes(DateTime::INVALID_BIT.to_ne_bytes()),
    };

    /// Returns `true` if the `REMOVED_BIT` is set.
    pub fn is_removed(&self) -> bool {
        let subsec_nanos = u32::from_be_bytes(self.subsec_nanos.to_ne_bytes());
        subsec_nanos & DateTime::REMOVED_BIT != 0
    }

    /// Returns `true` if the time is invalid.
    pub fn is_invalid(&self) -> bool {
        const NANOS_PER_SEC: u32 = 1_000_000_000;
        let subsec_nanos = u32::from_be_bytes(self.subsec_nanos.to_ne_bytes());
        (subsec_nanos & !DateTime::REMOVED_BIT) > NANOS_PER_SEC
    }

    /// Returns the same `DateTime`, but as removed at
    /// (`ModifiedTime::Removed`).
    #[must_use]
    pub fn mark_removed(mut self) -> Self {
        let mut subsec_nanos = u32::from_be_bytes(self.subsec_nanos.to_ne_bytes());
        subsec_nanos |= DateTime::REMOVED_BIT;
        self.subsec_nanos = u32::from_ne_bytes(subsec_nanos.to_be_bytes());
        self
    }

    /// Convert a slice of bytes of into `Datetime`.
    ///
    /// Returns `None` is not of length `size_of::<DateTime>()` or if the bytes
    /// are invalid.
    pub fn from_bytes<'a>(bytes: &'a [u8]) -> Option<DateTime> {
        bytes.get(8..).and_then(|subsec_bytes| {
            subsec_bytes
                .try_into()
                .ok()
                .map(u32::from_ne_bytes)
                .and_then(|subsec_nanos| {
                    // Safety: indexed [8..] above, so we know that this is safe.
                    bytes[0..8].try_into().ok().map(|seconds| DateTime {
                        seconds: u64::from_ne_bytes(seconds),
                        subsec_nanos,
                    })
                })
        })
    }

    /// Returns the `DateTime` as bytes.
    pub fn as_bytes<'a>(&'a self) -> &'a [u8] {
        // Safety: because `u8` doesn't have any invalid bit patterns this is
        // OK. We're also ensured at least `size_of::<DateTime>` bytes are
        // valid.
        unsafe { slice::from_raw_parts(self as *const DateTime as *const _, size_of::<DateTime>()) }
    }
}

impl From<SystemTime> for DateTime {
    /// # Notes
    ///
    /// If `time` is before Unix epoch this will return an empty `DateTime`,
    /// i.e. this same time as Unix epoch.
    ///
    /// This always returns a created at (`ModifiedTime::CreateAt`) time.
    fn from(time: SystemTime) -> DateTime {
        let elapsed = time
            .duration_since(SystemTime::UNIX_EPOCH)
            // We can't represent times before Unix epoch.
            .unwrap_or_else(|_| Duration::new(0, 0));

        DateTime {
            seconds: u64::from_ne_bytes(elapsed.as_secs().to_be_bytes()),
            subsec_nanos: u32::from_ne_bytes(elapsed.subsec_nanos().to_be_bytes()),
        }
    }
}

/// Time at which a blob was created or removed.
#[derive(Eq, PartialEq, Debug)]
pub(crate) enum ModifiedTime {
    /// Blob was created at this time.
    Created(SystemTime),
    /// Blob was removed at this time.
    Removed(SystemTime),
    /// Blob was invalid, e.g. when adding of the blob was aborted.
    Invalid,
}

impl From<ModifiedTime> for DateTime {
    /// # Notes
    ///
    /// If `time` is before Unix epoch this will return an empty `DateTime`,
    /// i.e. this same time as Unix epoch.
    fn from(time: ModifiedTime) -> DateTime {
        let (time, is_removed) = match time {
            ModifiedTime::Created(time) => (time, false),
            ModifiedTime::Removed(time) => (time, true),
            ModifiedTime::Invalid => return DateTime::INVALID,
        };
        let time: DateTime = time.into();
        if is_removed {
            time.mark_removed()
        } else {
            time
        }
    }
}

impl Into<ModifiedTime> for DateTime {
    fn into(self) -> ModifiedTime {
        if self.is_invalid() {
            ModifiedTime::Invalid
        } else {
            let seconds = u64::from_be_bytes(self.seconds.to_ne_bytes());
            let subsec_nanos = u32::from_be_bytes(self.subsec_nanos.to_ne_bytes());
            let subsec_nanos = subsec_nanos & !DateTime::REMOVED_BIT;
            let elapsed = Duration::new(seconds, subsec_nanos);
            let time = SystemTime::UNIX_EPOCH + elapsed;
            if self.is_removed() {
                ModifiedTime::Removed(time)
            } else {
                ModifiedTime::Created(time)
            }
        }
    }
}

impl fmt::Debug for DateTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DateTime")
            .field("seconds", &u64::from_be_bytes(self.seconds.to_ne_bytes()))
            .field(
                "subsec_nanos",
                &u32::from_be_bytes(self.subsec_nanos.to_ne_bytes()),
            )
            .field("is_removed", &self.is_removed())
            .field("is_invalid", &self.is_invalid())
            .finish()
    }
}

/// Control structure for a `mmap` area, see [`MmapArea`].
///
/// TODO: update below.
/// This has mutable access to all fields, except for the `ref_count`, of the
/// `MmapArea` it points to, as only a single `MmapAreaControl` may control the
/// `MmapArea`. However operations on the `ref_count` must still use atomic
/// operations, as `MmapLifetime` also has readable access to it.
#[derive(Debug)]
struct MmapAreaControl {
    ptr: NonNull<MmapArea>,
}

/// Lifetime reference to ensure `MmapArea` outlives anything that point to
/// it.
///
/// This lifetime only has access to `MmapArea.ref_count` (using atomics) and
/// must drop it once the count is zero (see the `Drop` impl).
#[derive(Debug)]
pub(crate) struct MmapLifetime {
    ptr: NonNull<MmapArea>,
}

impl MmapAreaControl {
    /// Create a new `MmapAreaControl`.
    fn new(
        length: libc::size_t,
        protection: libc::c_int,
        flags: libc::c_int,
        file: &File,
        offset: libc::off_t,
    ) -> io::Result<MmapAreaControl> {
        trace!(
            "allocating new `mmap`-area: file={:?}, offset={}, length={}",
            file,
            offset,
            length
        );
        let fd = file.as_raw_fd();
        mmap(ptr::null_mut(), length, protection, flags, fd, offset).map(|mmap_address| {
            let area = Box::new(MmapArea {
                mmap_address,
                mmap_length: length,
                mmap_offset: offset,
                ref_count: AtomicUsize::new(1),
            });
            MmapAreaControl {
                ptr: NonNull::from(Box::leak(area)),
            }
        })
    }

    /// Attempts to grow the `mmap`ed area by `grow_by_length` bytes. Returns
    /// `Ok(false)` if the area can't grow (e.g. if the page after this area is
    /// already being used). Returns `Ok(true)` if the area was successfully
    /// grown. And an error if one is hit.
    ///
    /// # Safety
    ///
    /// `file` must be the same after as the `MmapAreaControl` was created with.
    fn try_grow_by(
        &mut self,
        grow_by_length: libc::size_t,
        protection: libc::c_int,
        flags: libc::c_int,
        file: &File,
    ) -> io::Result<bool> {
        let area = self.area();
        if !needs_next_page(area.mmap_length, grow_by_length) {
            // Can grow inside the same page.
            return Ok(true);
        }

        // If we need another page we need to reserve it to ensure that we're
        // not overwriting a mapping that we don't own.
        match reserve_next_pages(area, grow_by_length) {
            Ok(true) => {}
            // Failed to reserve or an error, can't grow.
            other => return other,
        }

        // We successfully reserved up to `grow_by_length % PAGE_SIZE` pages. So
        // it is safe to overwrite our own mapping using `MAP_FIXED`.
        let new_length = area.mmap_length + grow_by_length;
        let fd = file.as_raw_fd();
        // FIXME: use `mremap(2)` on Linux.
        let new_address = mmap(
            area.mmap_address.as_ptr(),
            new_length,
            protection,
            flags | libc::MAP_FIXED, // Force the same address.
            fd,
            area.mmap_offset,
        )?;

        // Address and offset should remain unchanged.
        assert_eq!(
            new_address.as_ptr(),
            area.mmap_address.as_ptr(),
            "remapping the mmap area changed the address"
        );
        // Update `mmap`-ed length.
        // Safety: Only `MmapAreaControl` has access to `MmapArea`'s field other
        // than the `ref_count`, so this shouldn't cause a data race.
        // FIXME: I think we might need an `UnsafeCell` around `mmap_length` for
        // this.
        unsafe {
            self.ptr.as_mut().mmap_length = new_length;
        }
        Ok(true)
    }

    /// Returns the length of the `mmap` area.
    fn len(&self) -> usize {
        self.area().mmap_length
    }

    /// Returns the offset in the file of the `mmap` area.
    fn file_offset(&self) -> u64 {
        self.area().mmap_offset as u64
    }

    /// Returns the slice starting at `file_offset` containing `length`
    /// elements, if in this area. Otherwise this returns `None`.
    fn slice<T>(&self, file_offset: u64, length: usize) -> Option<MmapSlice<T>> {
        let area = self.area();
        let area_offset = area.mmap_offset as u64;
        if file_offset >= area_offset {
            let in_area_offset = (file_offset - area_offset) as usize;
            if area.mmap_length >= length + in_area_offset {
                // Safety: just checked if the slice is in the area.
                return Some(unsafe { self.slice_unchecked(in_area_offset, length) });
            }
        }
        None
    }

    /// Create a new [`MmapSlice`]. Starting at `offset` (**in bytes, relative
    /// to this area**) and `length` elements.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the area is long enough to create the slice.
    unsafe fn slice_unchecked<T>(&self, offset: usize, length: usize) -> MmapSlice<T> {
        assert!(offset <= self.len());
        assert!(self.len() - offset >= length * size_of::<T>());

        // Safety: we're ensured the pointer is null-null, so
        // `NonNull::new_unchecked` is safe. The caller must ensure the offset
        // and length are correct (see assertion above).
        let address =
            NonNull::new_unchecked(self.area().mmap_address.cast::<u8>().as_ptr().add(offset))
                .cast::<T>();
        let lifetime = self.create_lifetime();
        MmapSlice {
            address,
            length,
            lifetime,
        }
    }

    /// Same as [`slice_unchecked`], but returns a mutable slice ([`MmapSliceMut`]).
    ///
    /// # Safety
    ///
    /// See [`slice_unchecked`] for all safety requirements.
    unsafe fn slice_mut_unchecked<T>(&self, offset: usize, length: usize) -> MmapSliceMut<T> {
        assert!(offset <= self.len());
        assert!(self.len() - offset >= length * size_of::<T>());

        // Safety: see `slice_unchecked`.
        let address =
            NonNull::new_unchecked(self.area().mmap_address.cast::<u8>().as_ptr().add(offset))
                .cast::<T>();
        let lifetime = self.create_lifetime();
        MmapSliceMut {
            address,
            length,
            lifetime,
        }
    }

    /// Create a new lifetime structure for the `MmapArea`.
    fn create_lifetime(&self) -> MmapLifetime {
        // See `Arc::Clone` why relaxed ordering is sufficient here.
        self.area().ref_count.fetch_add(1, Ordering::Relaxed);
        MmapLifetime {
            ptr: self.ptr.cast(),
        }
    }
}

/// Returns `true` if for growing by `grow_by_length` bytes we need another
/// page.
const fn needs_next_page(area_length: libc::size_t, grow_by_length: libc::size_t) -> bool {
    // The number of bytes used in last page of the mmap area.
    let used_last_page = area_length % PAGE_SIZE;
    // If our last page is fully used (0 bytes used in last page) or the blob
    // doesn't fit in this last page we need another page for the blob.
    used_last_page == 0 || (PAGE_SIZE - used_last_page) < grow_by_length
}

/// Attempts to reverse the pages after `area`.
///
/// Returns `true` if successful,
/// false if we couldn't reserve the area or an error otherwise.
///
/// If this returns `true` a mapping of enough pages (to accommodate `size`)
/// exists at the next page aligned address after `area`, which can be safely
/// overwritten.
fn reserve_next_pages(area: &MmapArea, size: libc::size_t) -> io::Result<bool> {
    // The address of the next page to reserve, aligned to the page size.
    let end_address = area.mmap_address.as_ptr() as usize + area.mmap_length;
    let reserve_address = if is_page_aligned(end_address) {
        // If end_address is already page aligned it means we filled the entire
        // previous page.
        end_address as *mut libc::c_void
    } else {
        // Otherwise we need to find the end of this page, which is the start of
        // the page we want to reserve.
        next_page_aligned(end_address) as *mut libc::c_void
    };
    debug_assert!(is_page_aligned(reserve_address as usize));

    let actual_address = mmap(
        // Hint to the OS we want our area here at this address, if possible
        // most OSes will grant it to us or return a different address if not.
        reserve_address,
        size,
        libc::PROT_NONE,
        libc::MAP_PRIVATE | libc::MAP_ANON,
        -1,
        0,
    )?;

    if actual_address.as_ptr() == reserve_address {
        // We've created a mapping at the desired address so our reservation was
        // successful. We don't unmap the area as the caller will overwrite it.
        // Otherwise we have a data race between unmapping and the caller
        // overwriting the area, while another thread is also using `mmap`.
        Ok(true)
    } else {
        // Couldn't reserve the pages, unmap the mapping we created as we're not
        // going to use them.
        munmap(actual_address.as_ptr(), PAGE_SIZE).map(|()| false)
    }
}

/// Code shared between `MmapAreaControl` and `MmapLifetime`.
macro_rules! mmap_lifetime {
    ( $( $name: ident ),+ ) => {
        $(
        impl $name {
            /// Get a reference to the `MmapArea`.
            fn area<'a>(&'a self) -> &'a MmapArea {
                // Safety: the `MmapArea` must be alive at least as long as this
                // `MmapLifetime` (its the whole point of this type).
                unsafe { self.ptr.as_ref() }
            }
        }

        impl Clone for $name {
            fn clone(&self) -> $name {
                // See `Arc::Clone` why relaxed ordering is sufficient here.
                self.area().ref_count.fetch_add(1, Ordering::Relaxed);
                $name { ptr: self.ptr }
            }
        }

        impl Drop for $name {
            fn drop(&mut self) {
                // See `Arc::drop` to see why this is safe.
                if self.area().ref_count.fetch_sub(1, Ordering::Release) != 1 {
                    return;
                }
                atomic::fence(Ordering::Acquire);

                // Safety: `MmapArea` was allocated using `Box`, see
                // `MmapArea::new`. We checked the `ref_count` field above so
                // we're also ensured we're the last one with access.
                unsafe { drop(Box::from_raw(self.ptr.as_ptr())) }
            }
        }

        // Safety: the `mmap` allocated area can be safely accessed from
        // different threads.
        unsafe impl Send for $name {}
        unsafe impl Sync for $name {}
        )+
    };
}

mmap_lifetime!(MmapAreaControl, MmapLifetime);

/// A `mmap`ed area.
///
/// In this struct there are two kinds of values: used in the call to `mmap` and
/// the values used in respect to the `Data.file`.
/// * `mmap_address` and `mmap_length` are the values used in the call to
///   `mmap(2)` and can be used to `unmap(2)` it.
/// * `offset` and `length` are relative to the `Data.file`.
#[derive(Debug)]
pub struct MmapArea {
    /// Mmap address. Safety: must be the `mmap` returned address.
    mmap_address: NonNull<libc::c_void>,
    /// Mmap allocation length. Safety: must be length used in `mmap`.
    mmap_length: libc::size_t,
    /// Offset in the file.
    mmap_offset: libc::off_t,
    /// Reference count, shared between one or more `MmapLifetime`s. Only once
    /// this is zero this should be dropped.
    ref_count: AtomicUsize,
}

impl Drop for MmapArea {
    fn drop(&mut self) {
        assert!(self.ref_count.load(Ordering::Relaxed) == 0);
        // Safety: both `mmap_address` and `mmap_length` are used in the call to
        // `mmap(2)`, so its safe to use in the call to `munmap`.
        if let Err(err) = munmap(self.mmap_address.as_ptr(), self.mmap_length) {
            // We can't really handle the error properly here so we'll log it
            // and if we're testing (and not panicking) we'll panic on it.
            error!("error unmapping data: {}", err);
            #[cfg(test)]
            if !thread::panicking() {
                panic!("error unmapping data: {}", err);
            }
        }
    }
}

/// A slice backed by `mmap(2)`.
#[derive(Debug)]
struct MmapSliceOwned<T> {
    /// Mmap address. Safety: must be the `mmap` returned address. May be
    /// dangling if `mmap_length` is 0, in which case the address is not
    /// unmapped.
    mmap_address: NonNull<libc::c_void>,
    /// Mmap allocation length. Safety: must be length used in `mmap`.
    mmap_length: libc::size_t,
    /// Offset from `address` to start the slice.
    offset: usize,
    slice: PhantomData<[T]>,
}

impl<T> MmapSliceOwned<T> {
    /// Returns an empty slice.
    const fn empty() -> MmapSliceOwned<T> {
        MmapSliceOwned {
            mmap_address: NonNull::dangling(),
            mmap_length: 0,
            offset: 0,
            slice: PhantomData,
        }
    }

    /// Length is the number of elements, not bytes.
    fn new(
        length: usize,
        protection: libc::c_int,
        flags: libc::c_int,
        file: &File,
        offset: usize,
    ) -> io::Result<MmapSliceOwned<T>> {
        if length == 0 {
            return Ok(MmapSliceOwned::empty());
        }

        // Mmap offset needs to be page aligned.
        let mmap_offset = prev_page_aligned(offset as usize) as libc::off_t;
        let mmap_length = offset - mmap_offset as usize + (length * size_of::<T>());
        let offset: usize = offset - mmap_offset as usize;
        let mmap_address = mmap(
            ptr::null_mut(),
            mmap_length,
            protection,
            flags,
            file.as_raw_fd(),
            mmap_offset,
        )?;

        Ok(MmapSliceOwned {
            mmap_address,
            mmap_length,
            offset,
            slice: PhantomData,
        })
    }

    /// Returns the length of the slice.
    fn len(&self) -> usize {
        (self.mmap_length - self.offset) / size_of::<T>()
    }

    /// Call `madvise`
    fn madvise(&self, advise: libc::c_int) -> io::Result<()> {
        // Checking `mmap_length` alone here is fine because if the `length`
        // passed to `new` is 0 `mmap_length` will also be set to 0.
        if self.mmap_length == 0 {
            Ok(())
        } else {
            madvise(
                self.mmap_address.as_ptr() as *mut _,
                self.mmap_length as usize,
                advise,
            )
        }
    }

    /// Turns the this `MmapSlice` into a Rust slice.
    fn as_slice(&self) -> &[T] {
        // Mmap needs to be page aligned, so we allow an offset to support
        // arbitrary ranges.
        let address = unsafe {
            self.mmap_address
                .cast::<u8>()
                .as_ptr()
                .add(self.offset)
                .cast::<T>()
        };
        // Note: this works with `NonNull::dangling` pointers if `mmap_length`
        // is 0.
        unsafe { slice::from_raw_parts(address.cast(), self.len()) }
    }
}

// Safety: the `mmap` allocated area can be safely accessed from different
// threads.
unsafe impl<T: Send> Send for MmapSliceOwned<T> {}
unsafe impl<T: Sync> Sync for MmapSliceOwned<T> {}

impl<T> Drop for MmapSliceOwned<T> {
    fn drop(&mut self) {
        if self.mmap_length != 0 {
            // Safety: both `address` and `length` are used in the call to `mmap`.
            if let Err(err) = munmap(self.mmap_address.as_ptr(), self.mmap_length) {
                // We can't really handle the error properly here so we'll log
                // it and if we're testing (and not panicking) we'll panic on
                // it.
                error!("error unmapping data: {}", err);
                #[cfg(test)]
                if !thread::panicking() {
                    #[cfg(test)]
                    panic!("error unmapping data: {}", err);
                }
            }
        }
    }
}

/// A slice backed by `mmap(2)`.
///
/// The `mmap`-ed memory is managed by [`MmapAreaControl`], but this has a
/// separate lifetime to it.
#[derive(Debug, Clone)]
struct MmapSlice<T> {
    address: NonNull<T>,
    length: usize,
    lifetime: MmapLifetime,
}

// Safety: the `mmap` allocated area can be safely accessed from different
// threads.
unsafe impl<T: Send> Send for MmapSlice<T> {}
unsafe impl<T: Sync> Sync for MmapSlice<T> {}

impl<T> MmapSlice<T> {
    /// Returns the length of the slice.
    fn len(&self) -> usize {
        self.length
    }

    /// Turns the this `MmapSlice` into a Rust slice.
    fn as_slice(&self) -> &[T] {
        check_address(self.address.as_ptr(), self.length);
        unsafe { slice::from_raw_parts(self.address.as_ptr(), self.length) }
    }

    /// Call `madvise`
    fn madvise(&self, advise: libc::c_int) -> io::Result<()> {
        madvise(
            // Not required to be page aligned.
            self.address.as_ptr() as *mut _,
            self.length as usize,
            advise,
        )
    }

    /// Call `msync(2)` with `MS_SYNC`. Blocks until syncing is complete.
    fn sync(&self) -> io::Result<()> {
        unaligned_msync(self.address.as_ptr() as *mut _, self.length, libc::MS_SYNC)
    }
}

/// A slice backed by `mmap(2)`.
///
/// The `mmap`-ed memory is managed by [`MmapAreaControl`], but this has a
/// separate lifetime to it.
#[derive(Debug)]
struct MmapSliceMut<T> {
    address: NonNull<T>,
    length: usize,
    lifetime: MmapLifetime,
}

// Safety: the `mmap` allocated area can be safely accessed from different
// threads.
unsafe impl<T: Send> Send for MmapSliceMut<T> {}
unsafe impl<T: Sync> Sync for MmapSliceMut<T> {}

impl<T> MmapSliceMut<T> {
    /// Changes the mutable slice into an immutable one.
    ///
    /// # Safety
    ///
    /// Caller must ensure the underlying `mmap` area is created with
    /// `PROT_READ`.
    unsafe fn immutable(self) -> MmapSlice<T> {
        MmapSlice {
            address: self.address,
            length: self.length,
            lifetime: self.lifetime,
        }
    }

    /// Call `msync(2)` with `MS_ASYNC`. Returns immediately.
    fn start_sync(&self) -> io::Result<()> {
        unaligned_msync(self.address.as_ptr() as *mut _, self.length, libc::MS_ASYNC)
    }
}

impl<T> MmapSliceMut<MaybeUninit<T>> {
    /// See [`MaybeUninit::assume_init`].
    unsafe fn assume_init(self) -> MmapSliceMut<T> {
        MmapSliceMut {
            address: self.address.cast(),
            length: self.length,
            lifetime: self.lifetime,
        }
    }

    /// Copy from `src` into this slice, starting at `offset` (in elements).
    ///
    /// # Panics
    ///
    /// Panics if
    ///  * `offset` is larger then this slice.
    ///  * `src` + offset is longer than the slice.
    fn copy_from_slice(&mut self, offset: usize, src: &[T])
    where
        T: Copy,
    {
        check_address(self.address.as_ptr(), self.length);
        // Safety: we check if we own the memory after the offset.
        assert!(self.length >= offset);
        let dst_address = unsafe { self.address.as_ptr().add(offset) };
        assert!(src.len() <= self.length - offset);
        // Safety: src is available for reading, dst for writing and we checked
        // the length above.
        unsafe {
            ptr::copy_nonoverlapping(src.as_ptr(), dst_address as *mut _, src.len());
        }
    }
}

/// Check the `address` and `length` before creating a slice from it.
fn check_address<T>(address: *const T, length: usize) {
    assert!(length % size_of::<T>() == 0, "length incorrect");
    assert!(
        address as usize % align_of::<T>() == 0,
        "alignment incorrect"
    );
}

/// Returns true if `address` is page aligned.
const fn is_page_aligned(address: usize) -> bool {
    address.trailing_zeros() >= PAGE_BITS as u32
}

/// Returns the page-aligned address of the `address`. The returned address will
/// always be <= then `address`.
const fn prev_page_aligned(address: usize) -> usize {
    address & !((1 << PAGE_BITS) - 1)
}

/// Returns an aligned address to the next page relative to `address`.
const fn next_page_aligned(address: usize) -> usize {
    (address + PAGE_SIZE) & !((1 << PAGE_BITS) - 1)
}

/// `mmap(2)` system call.
fn mmap(
    addr: *mut libc::c_void,
    len: libc::size_t,
    protection: libc::c_int,
    flags: libc::c_int,
    fd: libc::c_int,
    offset: libc::off_t,
) -> io::Result<NonNull<libc::c_void>> {
    debug_assert!(len != 0);
    debug_assert!(is_page_aligned(addr as usize)); // Null is also page aligned.
    debug_assert!(is_page_aligned(offset as usize)); // 0 is also page aligned.
    let addr = unsafe { libc::mmap(addr, len, protection, flags, fd, offset) };
    if addr == libc::MAP_FAILED {
        Err(io::Error::last_os_error())
    } else {
        // Safety: if `mmap(2)` doesn't return `MAP_FAILED` the address will
        // always be non null.
        Ok(unsafe { NonNull::new_unchecked(addr) })
    }
}

/// `munmap(2)` system call.
fn munmap(addr: *mut libc::c_void, len: libc::size_t) -> io::Result<()> {
    debug_assert!(len != 0);
    debug_assert!(!addr.is_null());
    debug_assert!(is_page_aligned(addr as usize));
    if unsafe { libc::munmap(addr, len) } != 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// `madvise(2)` system call.
fn madvise(addr: *mut libc::c_void, len: libc::size_t, advise: libc::c_int) -> io::Result<()> {
    debug_assert!(len != 0);
    if unsafe { libc::madvise(addr, len, advise) != 0 } {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// Aligns `address` and corrects `length` before a call to [`msync`].
fn unaligned_msync(
    address: *mut libc::c_void,
    length: libc::size_t,
    flags: libc::c_int,
) -> io::Result<()> {
    let address = address as usize;
    let aligned_address = prev_page_aligned(address);
    let total_length = address - aligned_address + length;
    msync(aligned_address as *mut _, total_length, flags)
}

/// `msync(2)` system call.
fn msync(addr: *mut libc::c_void, len: libc::size_t, flags: libc::c_int) -> io::Result<()> {
    debug_assert!(len != 0);
    debug_assert!(!addr.is_null());
    debug_assert!(is_page_aligned(addr as usize));
    if unsafe { libc::msync(addr, len, flags) } != 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// Lock `file` using `flock(2)`.
///
/// # Notes
///
/// Once the `file` is dropped it's automatically unlocked.
fn lock(file: &mut File) -> io::Result<()> {
    trace!("locking file: file={:?}", file);
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::WouldBlock {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "database already in used",
            ))
        } else {
            Err(err)
        }
    } else {
        Ok(())
    }
}

/// `fallocate(2)` system call.
#[cfg(any(target_os = "android", target_os = "linux",))]
fn fallocate(fd: libc::c_int, new_len: libc::off_t) -> io::Result<()> {
    // Allocates more disk space (mode = 0), starting at offset 0.
    if unsafe { libc::fallocate(fd, 0, 0, new_len) } == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// Emulated `fallocate(2)` system call.
#[cfg(any(target_os = "ios", target_os = "macos",))]
fn fallocate(fd: libc::c_int, new_len: libc::off_t) -> io::Result<()> {
    // `ftruncate(2)` can actually grow the file. Alternatively we could use
    // `fcntl(2)` with `F_PREALLOCATE`, however that doesn't seem to increase
    // the file size correctly, possibly in the metadata or something. In any
    // case the result is that any access to the `mmap`-ed memory in this newly
    // created area will cause `SIGBUS`.
    ftruncate(fd, new_len)
}

/// `ftruncate(2)` system call.
fn ftruncate(fd: libc::c_int, new_len: libc::off_t) -> io::Result<()> {
    if unsafe { libc::ftruncate(fd, new_len) } == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}
