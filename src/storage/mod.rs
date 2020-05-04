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
//! ## Adding blobs
//!
//! Adding new blobs to the database is done in two phases. First, by appending
//! the blob's bytes to the data file. Note that in this phase we don't yet
//! ensure the blob is fully synced to disk. Second, we fully sync the blob to
//! the data and add a new entry to the index file, ensuring its fully synced.
//! Only after those two steps is a blob stored in the database.
//!
//! In the code this is done by first calling [`Storage::add_blob`]. This add
//! the blob's bytes to the datafile and returns a [`AddBlob`] query. This query
//! can then be committed (by calling [`Storage::commit`]) or aborted (by
//! calling [`Storage::abort`]). If the query is committed an entry is added to
//! the index file, ensuring the blob is stored in the database. Only after the
//! query is commited the blob can be looked up (using [`Storage::lookup`]).
//! However if the query is aborted, or never used again, no index entry will be
//! created for the blob and it will thus not be stored in the database. The
//! bytes stored in the data file for the blob will be left in place.
//!
//! ## Removing blobs
//!
//! Just like adding new blobs, removing blobs is done in two phases. In the
//! first phase the in-memory hash map is checked if the blob is present. If the
//! blob is present it returns a [`RemoveBlob`] query, if its not present it
//! returns [`RemoveResult::NotStored`].
//!
//! In the second phase, if the query is commited, the time of blob's entry in
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
//! As documentation above storing blobs is a two step process. If there is a
//! failure in step one, for example if the application crashes before the all
//! bytes are written to disk, it means that we have bytes in the data file
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

// TODO: add some safeguard to ensure the `AddBlob` and `RemoveBlob` queries can
// only be applied to the correct `Storage`? Important when the database actor
// is restarted.

// TODO: use the invalid bit in `Datetime.subsec_nanos` to indicate that the
// storing the blob was aborted? That would make it easier for the cleanup.

use std::cell::UnsafeCell;
use std::collections::hash_map::{self, HashMap};
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{self, Read, Write};
use std::iter::FusedIterator;
use std::marker::PhantomData;
use std::mem::{replace, size_of};
use std::ops::{Deref, DerefMut};
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::ptr::{self, NonNull};
use std::sync::atomic::{self, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};
use std::{fmt, slice, thread};

use log::error;

use crate::Key;

mod validate;

#[cfg(test)]
mod tests;

pub use validate::{validate, validate_storage, Corruption};

/// Magic header strings for the data and index files.
const DATA_MAGIC: &[u8] = b"Stored data v01\0"; // Null padded to 16 bytes.
const INDEX_MAGIC: &[u8] = b"Stored index v01";

/// A Binary Large OBject (BLOB).
#[derive(Clone)]
pub struct Blob {
    /// **The lifetime is a lie!** However the `lifetime` field ensures that the
    /// bytes are alive.
    bytes: &'static [u8],
    /// Date at which the `Blob` was created/added.
    created: SystemTime,
    /// Ensures that `mmap`ed data in `Data` doesn't get freed before this
    /// `Blob`, as that would invalidate the `bytes` field.
    lifetime: MmapLifetime,
}

impl Blob {
    /// Returns the bytes that make up the `Blob`.
    pub fn bytes<'b>(&'b self) -> &'b [u8] {
        // This is safe because the lifetime `'b` can't outlive the lifetime of
        // this `Blob`.
        self.bytes
    }

    /// Returns the length of `Blob`.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Returns the time at which the blob was added.
    pub fn created_at(&self) -> SystemTime {
        self.created
    }

    /// Prefetch the bytes that make up this `Blob`.
    ///
    /// # Notes
    ///
    /// If this returns an error it doesn't mean the bytes are inaccessible.
    pub fn prefetch(&self) -> io::Result<()> {
        madvise(
            self.bytes.as_ptr() as *mut _,
            self.bytes.len(),
            libc::MADV_SEQUENTIAL | libc::MADV_WILLNEED,
        )
    }
}

impl fmt::Debug for Blob {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Blob")
            .field("bytes", &self.bytes)
            .field("created", &self.created)
            .finish()
    }
}

/// Handle to a database.
#[derive(Debug)]
pub struct Storage {
    index: Index,
    /// All blobs currently in `Index` and `Data`. The lifetime `'s` refers to
    /// the `mmap`ed areas in `Data`.
    ///
    /// # Safety
    ///
    /// `blobs` must be declared before `data`because it must be dropped before
    /// `data`.
    // TODO: use different hashing algorithm.
    blobs: HashMap<Key, (EntryIndex, BlobEntry)>,
    /// Length of `blob` that are not `BlobEntry::Removed`.
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
                        .address_for(entry.offset(), entry.length())
                        .map(|(address, lifetime)| {
                            // Safety: this is safe because we have `lifetime`
                            // which ensures the bytes live until its dropped.
                            let bytes = unsafe {
                                slice::from_raw_parts(address.as_ptr(), entry.length() as usize)
                            };
                            length += 1;
                            BlobEntry::Stored(Blob {
                                bytes,
                                created: time,
                                lifetime,
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
            length,
        })
    }

    /// Returns the number of blobs stored in the database.
    pub fn len(&self) -> usize {
        self.length
    }

    /// Returns the number of bytes of data stored.
    pub fn data_size(&self) -> u64 {
        self.data.file_length()
    }

    /// Returns the size of the index file in bytes.
    pub fn index_size(&self) -> u64 {
        self.index.file_length()
    }

    /// Returns the total size of the database file (data and index).
    pub fn total_size(&self) -> u64 {
        self.data_size() + self.index_size()
    }

    /// Returns a reference to the `Blob` corresponding to the key, if stored.
    pub fn lookup(&self, key: &Key) -> Option<BlobEntry> {
        self.blobs
            .get(key)
            .map(|(_, blob_entry)| blob_entry.clone())
    }

    /// Add `blob` to the database.
    ///
    /// Only after the returned [query] is [committed] is the blob stored in the
    /// database.
    ///
    /// # Notes
    ///
    /// There is an implicit lifetime between the returned [`AddBlob`] query and
    /// this `Storage`. The query can only be commit or aborted to this
    /// `Storage`, the query may however safely outlive `Storage`.
    ///
    /// [query]: AddBlob
    /// [committed]: Storage::commit
    pub fn add_blob(&mut self, blob: &[u8]) -> AddResult {
        let key = Key::for_blob(blob);

        // Can't have double entries.
        match self.blobs.get(&key) {
            Some((_, BlobEntry::Stored(_))) => return AddResult::AlreadyStored(key),
            _ => {}
        }

        // First add the blob to the data file. If something happens the blob
        // will be written (or not), but its not *in* the database as the index
        // defines what is in the database.
        match self.data.add_blob(blob) {
            Ok((offset, address, lifetime)) => AddResult::Ok(AddBlob {
                key,
                offset,
                length: blob.len() as u32,
                address,
                lifetime,
            }),
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
        match self.blobs.get(&key) {
            Some((_, BlobEntry::Stored(_))) => RemoveResult::Ok(RemoveBlob { key }),
            Some((_, BlobEntry::Removed(time))) => RemoveResult::NotStored(Some(*time)),
            None => RemoveResult::NotStored(None),
        }
    }

    /// Commit to `query`.
    pub fn commit<Q>(&mut self, query: Q, arg: Q::Arg) -> io::Result<Q::Return>
    where
        Q: Query,
    {
        query.commit(self, arg)
    }

    /// Abort `query`.
    pub fn abort<Q>(&mut self, query: Q) -> io::Result<()>
    where
        Q: Query,
    {
        query.abort(self)
    }
}

/// Result returned by [`Storage::add_blob`].
pub enum AddResult {
    /// Blob was successfully stored, but not yet added to the index nor
    /// database.
    Ok(AddBlob),
    /// Blob is already stored.
    AlreadyStored(Key),
    /// I/O error.
    Err(io::Error),
}

/// Result returned by [`Storage::remove_blob`].
pub enum RemoveResult {
    /// Blob is prepared to be removed, but not yet removed from the database.
    Ok(RemoveBlob),
    /// Key is not stored in the database.
    NotStored(Option<SystemTime>),
}

/// a `Query` is a partially prepared storage operation.
///
/// An example is [`AddBlob`] that already has the data of the blob stored, but
/// the blob itself isn't yet in the database. Only after [committing] will the
/// blob be stored (and thus accessible).
///
/// [committing]: Query::commit
pub trait Query {
    /// Argument provided when [committing] to a `Query`.
    ///
    /// [committing]: Storage::commit
    type Arg;

    /// Type returned after a `Query` is [commited].
    ///
    /// [commited]: Storage::commit
    type Return;

    /// Commit to the query.
    fn commit(self, storage: &mut Storage, arg: Self::Arg) -> io::Result<Self::Return>;

    /// Abort the query.
    fn abort(self, storage: &mut Storage) -> io::Result<()>;
}

/// A [`Query`] to [add a blob] to the [`Storage`].
///
/// [add a blob]: Storage::add_blob
pub struct AddBlob {
    key: Key,
    offset: u64,
    length: u32,
    address: NonNull<u8>,
    lifetime: MmapLifetime,
}

impl AddBlob {
    /// Create an `Entry` in `storage.index`.
    ///
    /// This ensures that the blob for which the entry is created is synced to
    /// disk.
    fn create_entry(
        self,
        data: &mut Data,
        index: &mut Index,
        created_at: SystemTime,
    ) -> io::Result<(EntryIndex, Key, Blob)> {
        // Ensure the data is synced to disk.
        data.sync(self.offset, self.length)?;

        // The data is already stored so we can add the blob to the
        // index.
        let index_entry = Entry::new(self.key.clone(), self.offset, self.length, created_at);
        let entry_index = index.add_entry(&index_entry)?;

        // Now that the data and index entry are stored we can insert
        // the blob into our database.
        Ok((
            entry_index,
            self.key,
            Blob {
                // Safety: `Data`'s `MmapArea`s outlive `blobs` in `Storage`
                // because of the `lifetime` added below.
                bytes: unsafe {
                    slice::from_raw_parts(self.address.as_ptr(), self.length as usize)
                },
                created: created_at,
                lifetime: self.lifetime,
            },
        ))
    }
}

impl Query for AddBlob {
    type Arg = SystemTime;
    type Return = Key;

    fn commit(self, storage: &mut Storage, created_at: SystemTime) -> io::Result<Self::Return> {
        debug_assert_eq!(
            storage
                .data
                .address_for(self.offset, self.length)
                .unwrap()
                .0,
            self.address
        );

        use hash_map::Entry::*;
        match storage.blobs.entry(self.key.clone()) {
            Occupied(mut entry) => match entry.get_mut() {
                (_, BlobEntry::Stored(_)) => {
                    // If the blob has already been added we don't want to modify
                    // it.
                    let key = self.key.clone();
                    return self.abort(storage).map(|()| key);
                }
                entry @ (_, BlobEntry::Removed(_)) => {
                    // First add our new index entry.
                    let (entry_index, key, blob) =
                        self.create_entry(&mut storage.data, &mut storage.index, created_at)?;
                    let (old_entry_index, _) =
                        replace(entry, (entry_index, BlobEntry::Stored(blob)));
                    storage.length += 1;

                    // Next mark the old index entry as invalid.
                    storage.index.mark_as_invalid(old_entry_index).map(|()| key)
                }
            },
            Vacant(entry) => {
                let (entry_index, key, blob) =
                    self.create_entry(&mut storage.data, &mut storage.index, created_at)?;
                entry.insert((entry_index, BlobEntry::Stored(blob)));
                storage.length += 1;
                Ok(key)
            }
        }
    }

    fn abort(self, _storage: &mut Storage) -> io::Result<()> {
        // Note: this is also called by `commit` is the blob is already in the
        // database when committing.
        Ok(())

        /* TODO: cleanup the unused bytes.
        // We add an entry with an invalid time to indicate the bytes are
        // unused.
        let index_entry = Entry::new(self.key, self.offset, self.length, DateTime::INVALID);
        storage.index.add_entry(&index_entry).map(|_| ())
        */
    }
}

// Safety: the `lifetime` field ensures the `address` remains valid.
unsafe impl Send for AddBlob {}

/// A [`Query`] to [remove a blob] to the [`Storage`].
///
/// [remove a blob]: Storage::remove_blob
pub struct RemoveBlob {
    key: Key,
}

impl Query for RemoveBlob {
    type Arg = SystemTime;
    /// Returns the time at which the blob is actually removed. This is the same
    /// time as the provided [`Arg`]ument, unless the blob was already removed,
    /// in which case its the time at which the blob was originally removed.
    ///
    /// [`Arg`]: Query::Arg
    type Return = SystemTime;

    fn commit(self, storage: &mut Storage, removed_at: SystemTime) -> io::Result<Self::Return> {
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
            unreachable!("trying to remove a blob that was never stored");
        };
        storage.length -= 1;
        res
    }

    fn abort(self, _storage: &mut Storage) -> io::Result<()> {
        // We don't have to do anything as we didn't change anything.
        Ok(())
    }
}

/// Handle for a data file.
#[derive(Debug)]
struct Data {
    /// Data file opened for reading and writing in append-only mode (for
    /// adding new blobs).
    ///
    /// Note: the seek position is unlikely to be correct after opening.
    file: File,
    /// Current length of the file and all `mmap`ed areas.
    length: u64,
    /// Number of bytes synced to disk.
    synced_length: u64,
    /// `mmap`ed areas.
    /// See `check` for notes about safety.
    areas: Vec<MmapAreaControl>,
}

/// The size of a single page, used in probing `mmap`ing memory.
// TODO: ensure this is correct on all architectures. Tested in
// `data::page_size` test in the tests module.
pub const PAGE_SIZE: usize = 1 << 12; // 4096.
const PAGE_BITS: usize = 12;

/// Control structure for a `mmap` area, see [`MmapArea`].
///
/// This has mutable access to all fields, except for the `ref_count`, of the
/// `MmapArea` it points to, as only a single `MmapAreaControl` may control the
/// `MmapArea`. However operations on the `ref_count` must still use atomic
/// operations, as `MmapLifetime` also has readable access to it.
struct MmapAreaControl {
    ptr: NonNull<UnsafeCell<MmapArea>>,
}

// Safety: the `mmap` allocated area can be safely accessed from different
// threads.
unsafe impl Send for MmapAreaControl {}

impl MmapAreaControl {
    /// Create a new `MmapArea`, with a single `MmapAreaControl` pointing to it
    /// and zero or more `MmapLifetime`.
    fn new(
        mmap_address: NonNull<libc::c_void>,
        mmap_length: libc::size_t,
        offset: u64,
        length: libc::size_t,
    ) -> MmapAreaControl {
        let ptr = Box::new(UnsafeCell::new(MmapArea {
            mmap_address,
            mmap_length,
            offset,
            length,
            ref_count: AtomicUsize::new(1),
        }));

        MmapAreaControl {
            ptr: NonNull::from(Box::leak(ptr)),
        }
    }

    /// Create a new lifetime structure for the `MmapArea`.
    fn create_lifetime(&self) -> MmapLifetime {
        // See `Arc::Clone` why relaxed ordering is sufficient here.
        self.deref().ref_count.fetch_add(1, Ordering::Relaxed);
        MmapLifetime {
            ptr: self.ptr.cast(),
        }
    }
}

impl Deref for MmapAreaControl {
    type Target = MmapArea;

    fn deref(&self) -> &Self::Target {
        // Safety: see docs of `MmapAreaControl`.
        unsafe { &*self.ptr.as_ref().get() }
    }
}

impl DerefMut for MmapAreaControl {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // Safety: see docs of `MmapAreaControl`.
        unsafe { &mut *self.ptr.as_mut().get() }
    }
}

impl fmt::Debug for MmapAreaControl {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.deref().fmt(f)
    }
}

impl Drop for MmapAreaControl {
    fn drop(&mut self) {
        unsafe {
            // See `Arc::drop` to see why this is safe.
            if self.deref().ref_count.fetch_sub(1, Ordering::Release) != 1 {
                return;
            }
            atomic::fence(Ordering::Acquire);
            drop(Box::from_raw(self.ptr.as_ptr()));
        }
    }
}

/// Lifetime reference to ensure `MmapArea` outlives any `Blob`s that point to
/// them.
struct MmapLifetime {
    ptr: NonNull<MmapArea>,
}

impl Drop for MmapLifetime {
    fn drop(&mut self) {
        unsafe {
            // See `MmapAreaControl`.
            if self.ptr.as_ref().ref_count.fetch_sub(1, Ordering::Release) != 1 {
                return;
            }
            atomic::fence(Ordering::Acquire);
            drop(Box::from_raw(self.ptr.as_ptr()));
        }
    }
}

impl Clone for MmapLifetime {
    fn clone(&self) -> MmapLifetime {
        // See `Arc::Clone` why relaxed ordering is sufficient here.
        unsafe { self.ptr.as_ref().ref_count.fetch_add(1, Ordering::Relaxed) };
        MmapLifetime { ptr: self.ptr }
    }
}

// Safety: the `mmap` allocated area can be safely accessed from different
// threads.
unsafe impl Send for MmapLifetime {}

/// A `mmap`ed area.
///
/// In this struct there are two kinds of values: used in the call to `mmap` and
/// the values used in respect to the `Data.file`.
/// * `mmap_address` and `mmap_length` are the values used in the call to
///   `mmap(2)` and can be used to `unmap(2)` it.
/// * `offset` and `length` are relative to the `Data.file`.
#[derive(Debug)]
struct MmapArea {
    /// Mmap address. Safety: must be the `mmap` returned address.
    mmap_address: NonNull<libc::c_void>,
    /// Mmap allocation length. Safety: must be length used in `mmap`.
    mmap_length: libc::size_t,

    /// Absolute offset in the file. NOTE: **not** the offset used in the call
    /// to `mmap`, as that must be page aligned.
    offset: u64,
    /// Length actually used of the mmap allocation, relative to `offset`.
    /// `mmap_length` might be larger due to the page alignment requirement for
    /// the offset.
    length: libc::size_t,

    /// Reference count, shared between one `MmapAreaControl` and zero or more
    /// `MmapLifetime`s in `Blob`s. Only once this is zero this should be
    /// dropped.
    ref_count: AtomicUsize,
}

impl MmapArea {
    /// Returns `true` if the blob at `offset` with `length` is in this area.
    fn in_area(&self, offset: u64, length: u32) -> bool {
        let area_offset = self.offset as u64;
        area_offset <= offset && (offset + length as u64) <= (area_offset + self.length as u64)
    }

    /// Return a pointer to the `mmap`ed area at `offset`.
    ///
    /// # Safety
    ///
    /// `offset` must be: `MmapArea.offset > offset < (MmapArea.offset +
    /// MmapArea.length)`
    fn offset(&self, offset: u64) -> NonNull<u8> {
        // The offset in the `mmap`ed area.
        let relative_offset = offset - self.offset;

        // The ensure that the offset into the file is page aligned we might
        // having overlapping bytes at the start of this area, we need to ignore
        // those.
        let ignore_bytes = self.mmap_length - self.length;

        let address = unsafe {
            self.mmap_address
                .as_ptr()
                .add(ignore_bytes + relative_offset as usize)
        };
        NonNull::new(address as *mut u8).unwrap()
    }

    /// Attempts to grow the `mmap`ed area by `length` bytes. Returns `Ok(None)`
    /// if the area can't grow (e.g. if the page after this area is already
    /// being used). Returns `Ok(Some(adress))`, where address is the starting
    /// address at which the area is grown, if the area was successfully grown.
    fn try_grow_by(
        &mut self,
        length: libc::size_t,
        fd: libc::c_int,
    ) -> io::Result<Option<NonNull<u8>>> {
        let can_grow = if needs_next_page(self.mmap_length, length) {
            // If we need another page we need to reserve it to ensure that
            // we're not overwriting a mapping that we don't own.
            reserve_next_page(&self)?
        } else {
            // Can grow inside the same page.
            true
        };

        if can_grow {
            // If we successfully reserved the next page, or we can grow
            // within the next page, it is safe to overwrite our own mapping
            // using `MAP_FIXED`.
            let new_length = self.mmap_length + length;

            let aligned_offset = if is_page_aligned(self.offset as usize) {
                self.offset
            } else {
                prev_page_aligned(self.offset as usize) as u64
            };
            let res = mmap(
                self.mmap_address.as_ptr(),
                new_length,
                libc::PROT_READ,
                libc::MAP_PRIVATE | libc::MAP_FIXED, // Force the same address.
                fd,
                aligned_offset as libc::off_t,
            );

            if let Ok(new_address) = res {
                // Address and offset should remain unchanged.
                assert_eq!(
                    new_address,
                    self.mmap_address.as_ptr(),
                    "remapping the mmap area changed the address"
                );
                // Update `mmap` and effective length.
                self.mmap_length = new_length;
                self.length += length;
                // The blob is located in the last `length` bytes.
                let offset = self.offset as u64 + (self.length - length) as u64;
                let blob_ptr = self.offset(offset);
                return Ok(Some(blob_ptr));
            }
        }

        Ok(None)
    }
}

impl Data {
    /// Open a `Data` file.
    fn open<P: AsRef<Path>>(path: P) -> io::Result<Data> {
        let mut data = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)
            .map(|file| Data {
                file,
                length: 0,
                synced_length: 0,
                areas: Vec::new(),
            })?;
        lock(&mut data.file)?;

        let metadata = data.file.metadata()?;
        let mut length = metadata.len() as libc::size_t;
        data.synced_length = data.length;
        if length == 0 {
            // New file so we need to add our magic.
            data.file.write_all(&DATA_MAGIC)?;
            length = DATA_MAGIC.len();
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

        data.new_area(length)?;
        data.check();
        Ok(data)
    }

    /// Returns the total file length.
    fn file_length(&self) -> u64 {
        self.length
    }

    /// Add `blob` at the end of the data log.
    /// Returns the stored blob's offset in the data file and its `mmap`ed
    /// address.
    ///
    /// # Notes
    ///
    /// The blob is not synced to disk, call [`sync`] to ensure that!
    /// Can't store an empty blob!
    fn add_blob(&mut self, blob: &[u8]) -> io::Result<(u64, NonNull<u8>, MmapLifetime)> {
        assert!(!blob.is_empty(), "tried to store an empty blob");
        // First add the blob to the file.
        self.file.write_all(blob)?;

        // Next grow our `mmap`ed area(s).
        let offset = self.length;
        let (address, lifetime) = self.grow_by(blob.len())?;
        self.check();
        Ok((offset as u64, address, lifetime))
    }

    /// Sync the file to disk up to at least `offset` bytes.
    fn sync(&mut self, offset: u64, length: u32) -> io::Result<()> {
        if self.synced_length >= (offset + length as u64) {
            // Already synced.
            Ok(())
        } else {
            self.file
                .sync_all()
                .map(|()| self.synced_length = self.length)
        }
    }

    /// Grows the `mmap`ed data by `length` bytes.
    ///
    /// This either grows the last `mmap`ed area, or allocates a new one.
    /// Returns the starting address at which the area is grown, i.e. where the
    /// blob is mmaped into memory.
    fn grow_by(&mut self, length: libc::size_t) -> io::Result<(NonNull<u8>, MmapLifetime)> {
        // Try to grow the last `mmap`ed area.
        if let Some(area) = self.areas.last_mut() {
            if let Some(address) = area.try_grow_by(length, self.file.as_raw_fd())? {
                // Update total `Data` length .
                self.length += length as u64;
                return Ok((address, area.create_lifetime()));
            }
        }

        // If we've failed to grow the last `mmap`ed area, or didn't yet have
        // one, we'll allocate a new area.
        self.new_area(length)
    }

    /// Create a new `mmap` area of `length` bytes, using the offset from the
    /// last mmap area (or 0).
    ///
    /// Returns the starting address of the added blob. Note that this can
    /// different from the address of the `mmap`ed area (last
    /// `MmapArea.address`), as the blob offset might not be page aligned.
    fn new_area(&mut self, length: libc::size_t) -> io::Result<(NonNull<u8>, MmapLifetime)> {
        // Get the file offset from the last `mmap`ed area.
        let offset = self
            .areas
            .last()
            .map(|area| area.offset + area.length as u64)
            .unwrap_or(0);

        // Offset must be page aligned. This means we can have overlapping
        // sections in the entire mmaped area, but that is ok.
        let (aligned_offset, offset_alignment_diff) = if is_page_aligned(offset as usize) {
            // Already page aligned, neat! No overlapping areas.
            (offset, 0)
        } else {
            // Offset is not page aligned, so we mmap the previous page again to
            // ensure the blob can be read from continuous memory.
            let aligned_offset = prev_page_aligned(offset as usize) as u64;
            let offset_alignment_diff = offset - aligned_offset;
            (aligned_offset, offset_alignment_diff as usize)
        };

        // Account for the overlapping area to page align the offset.
        let aligned_length = length + offset_alignment_diff;

        let address = mmap(
            ptr::null_mut(),
            aligned_length,
            libc::PROT_READ,
            libc::MAP_PRIVATE,
            self.file.as_raw_fd(),
            aligned_offset as libc::off_t,
        )?;

        // Inform the OS that our access pattern are likely to be random, this
        // way it doesn't load pages we won't need. We can use `Blob::prefetch`
        // to inform the OS when we do need pages.
        let _ = madvise(address, aligned_length, libc::MADV_RANDOM);

        // Safety: `mmap` doesn't return a null address.
        let address = NonNull::new(address).unwrap();
        let area = MmapAreaControl::new(address, aligned_length, offset, length);
        let blob_address = area.offset(offset as u64);
        let lifetime = area.create_lifetime();
        self.areas.push(area);

        // Not counting the overlapping length!
        self.length += length as u64;
        Ok((blob_address, lifetime))
    }

    /// Returns the address for the blob at `offset`, with `length`. Or `None`
    /// if the combination is invalid.
    ///
    /// # Notes
    ///
    /// The returned address lifetime is tied to the `Data` struct.
    fn address_for(&self, offset: u64, length: u32) -> Option<(NonNull<u8>, MmapLifetime)> {
        debug_assert!(
            offset >= DATA_MAGIC.len() as u64,
            "offset inside magic header"
        );
        self.areas
            .iter()
            .find(|area| area.in_area(offset, length))
            .map(|area| (area.offset(offset), area.create_lifetime()))
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
            for area in &self.areas {
                assert!(
                    area.mmap_address.as_ptr() as usize % PAGE_SIZE == 0,
                    "incorrect mmap address alignment"
                );
                assert!(area.offset <= DATA_MAGIC.len() as u64, "incorrect offset");
                assert!(
                    area.offset >= last_offset,
                    "mmaped areas not sorted by offset"
                );
                assert_eq!(
                    area.offset as u64, total_length,
                    "incorrect offset for mmap area"
                );
                assert!(
                    area.length <= area.mmap_length,
                    "mmap area smaller the MmapArea.length"
                );
                last_offset = area.offset;
                total_length += area.length as u64;
            }
            assert_eq!(self.length, total_length, "incorrect total length");
        }
    }
}

/// Returns `true` if for growing by `grow_by_length` bytes we need another
/// page.
fn needs_next_page(area_length: libc::size_t, grow_by_length: libc::size_t) -> bool {
    // The number of bytes used in last page of the mmap area.
    let used_last_page = area_length % PAGE_SIZE;
    // If our last page is fully used (0 bytes used in last page) or the blob
    // doesn't fit in this last page we need another page for the blob.
    used_last_page == 0 || (PAGE_SIZE - used_last_page) < grow_by_length
}

/// Attempts to reverse the page after `area`. Returns `true` if successful,
/// false or an error otherwise.
///
/// If this return `true` a mapping of a single page exists at the next page
/// aligned address after `area`, which can be safely overwritten.
fn reserve_next_page(area: &MmapArea) -> io::Result<bool> {
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
        PAGE_SIZE,
        libc::PROT_NONE,
        libc::MAP_PRIVATE | libc::MAP_ANON,
        -1,
        0,
    )?;

    if actual_address == reserve_address {
        // We've created a mapping at the desired address so our reservation was
        // successful. We don't unmap the area as the caller will overwrite it.
        // Otherwise we have a data race between unmapping and the caller
        // overwriting the area, while another thread is also using `mmap`.
        Ok(true)
    } else {
        // Couldn't reserve the page, unmap the mapping we created as we're not
        // going to use it.
        munmap(actual_address, PAGE_SIZE).map(|()| false)
    }
}

/// Returns true if `address` is page aligned.
const fn is_page_aligned(address: usize) -> bool {
    address.trailing_zeros() >= PAGE_BITS as u32
}

/// Returns an aligned address to the next page relative to `address`.
const fn next_page_aligned(address: usize) -> usize {
    (address + PAGE_SIZE) & !((1 << PAGE_BITS) - 1)
}

/// Returns an aligned address to the previous page relative to `address`.
/// Used in determining the `offset` used in calls to `mmap(2)`.
fn prev_page_aligned(address: usize) -> usize {
    address.saturating_sub(PAGE_SIZE) & !((1 << PAGE_BITS) - 1)
}

impl Drop for MmapArea {
    fn drop(&mut self) {
        debug_assert!(self.ref_count.load(Ordering::Relaxed) == 0);
        // Safety: both `address` and `length` are used in the call to `mmap`.
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

/// Handle for an index file.
#[derive(Debug)]
struct Index {
    /// Index file opened for reading (for use in `entries`) and writing in
    /// append-only mode for adding entries (`add_entry`).
    file: File,
    /// Current length of the file.
    length: u64,
}

impl Index {
    /// Open an index file.
    fn open<P: AsRef<Path>>(path: P) -> io::Result<Index> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        lock(&mut file)?;

        let metadata = file.metadata()?;
        let mut length = metadata.len();
        if length == 0 {
            // New file so we need to add our magic.
            file.write_all(&INDEX_MAGIC)?;
            length += INDEX_MAGIC.len() as u64;
        } else {
            // Existing file; we'll check if it has the magic header.
            let mut magic = [0; INDEX_MAGIC.len()];
            let read_bytes = file.read(&mut magic)?;
            if read_bytes != INDEX_MAGIC.len() || magic != INDEX_MAGIC {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "missing magic header in index file",
                ));
            }
        }

        Ok(Index { file, length })
    }

    /// Returns the total file length.
    fn file_length(&self) -> u64 {
        self.length
    }

    /// Returns an iterator for all entries remaining in the `Index`. If the
    /// `Index` was just `open`ed this means all entries in the entire index
    /// file.
    fn entries<'i>(&'i mut self) -> io::Result<MmapSlice<'i, Entry>> {
        let mmap_length = self.length as libc::size_t;
        let indices_length = mmap_length - INDEX_MAGIC.len();
        if indices_length == 0 {
            return Ok(MmapSlice {
                address: ptr::null_mut(),
                length: 0,
                offset: 0,
                slice: PhantomData,
            });
        }

        if indices_length % size_of::<Entry>() != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "incorrect index file size",
            ));
        }

        let mmap_address = mmap(
            ptr::null_mut(),
            mmap_length,
            libc::PROT_READ,
            libc::MAP_PRIVATE,
            self.file.as_raw_fd(),
            0,
        )?;

        // For performance let the OS known we're going to read the entries
        // so it can prefetch the pages from disk.
        // Note that we don't care about the result as its just an advise to the
        // OS, if t can't comply we'll continue on.
        let _ = madvise(
            mmap_address,
            mmap_length,
            libc::MADV_SEQUENTIAL | libc::MADV_WILLNEED,
        );

        Ok(MmapSlice {
            address: mmap_address,
            length: mmap_length,
            offset: INDEX_MAGIC.len(),
            slice: PhantomData,
        })
    }

    /// Add a new `entry` to the `Index`.
    fn add_entry(&mut self, entry: &Entry) -> io::Result<EntryIndex> {
        debug_assert!(
            entry.offset >= DATA_MAGIC.len() as u64,
            "offset inside magic header"
        );
        // Safety: because `u8` doesn't have any invalid bit patterns this is
        // OK. We're also ensured at least `size_of::<Entry>` bytes are valid.
        let bytes: &[u8] =
            unsafe { slice::from_raw_parts(entry as *const _ as *const _, size_of::<Entry>()) };
        self.file
            .write_all(&bytes)
            .and_then(|()| self.file.sync_all())
            .map(|()| {
                let entry_index = EntryIndex(
                    ((self.length - (INDEX_MAGIC.len() as u64)) / (bytes.len() as u64)) as usize,
                );
                self.length += bytes.len() as u64;
                entry_index
            })
    }

    /// Marks the `Entry` at index `at` as removed at `removed_at` time.
    fn mark_as_removed(&mut self, at: EntryIndex, removed_at: SystemTime) -> io::Result<()> {
        // The date at which the blob was removed.
        let date = DateTime::from(removed_at).mark_removed();
        self.overwrite_date(at, &date)
    }

    /// Marks the `Entry` at index `at` as invalid.
    fn mark_as_invalid(&mut self, at: EntryIndex) -> io::Result<()> {
        self.overwrite_date(at, &DateTime::INVALID)
    }

    /// Overwrite the date of the `Entry` at index `at` with `date`.
    fn overwrite_date(&mut self, at: EntryIndex, date: &DateTime) -> io::Result<()> {
        debug_assert!(
            ((at.0 as u64) * (size_of::<Entry>() as u64) + (size_of::<Entry>() as u64))
                < self.length,
            "index outside of index file"
        );

        // Safety: because `u8` doesn't have any invalid bit patterns this is
        // OK. We're also ensured at least `size_of::<DateTime>` bytes are valid.
        let bytes: &[u8] = unsafe {
            slice::from_raw_parts(date as *const DateTime as *const _, size_of::<DateTime>())
        };

        // NOTE: this is only correct because `Entry` and `DateTime` have the
        // `#[repr(C)]` attribute and thus the layout is fixed.
        let offset = at.offset() + ((size_of::<Entry>() - size_of::<DateTime>()) as u64);
        self.file
            .write_all_at(&bytes, offset)
            .and_then(|()| self.file.sync_all())
    }
}

/// A slice backed by `mmap(2)`.
struct MmapSlice<'a, T> {
    /// Mmap address. Safety: must be the `mmap` returned address, may be null
    /// in which case the address is not unmapped. If null `length` must be 0.
    address: *mut libc::c_void,
    /// Mmap allocation length. Safety: must be length used in `mmap`.
    length: libc::size_t,
    /// Offset from `address` to start the slice returned by `Deref`.
    offset: usize,
    slice: PhantomData<&'a [T]>,
}

impl<'a, T> Deref for MmapSlice<'a, T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        // Mmap needs to be page aligned, so we allow an offset to support
        // arbitrary ranges.
        let address = unsafe { self.address.add(self.offset) };
        let length = self.length - self.offset;
        debug_assert!(length % size_of::<T>() == 0, "length incorrect");
        unsafe { slice::from_raw_parts(address as *const _, length / size_of::<T>()) }
    }
}

impl<'a> MmapSlice<'a, Entry> {
    /// Returns an iterator over the `Entry`s and its index into the [`Index`]
    /// file.
    fn iter(
        &self,
    ) -> impl Iterator<Item = (EntryIndex, &Entry)> + ExactSizeIterator + FusedIterator {
        self.deref()
            .iter()
            .enumerate()
            .map(|(idx, entry)| (EntryIndex(idx), entry))
    }
}

impl<'a, T> Drop for MmapSlice<'a, T> {
    fn drop(&mut self) {
        // If the index file was empty `address` will be null as we can't create
        // a mmap with length 0.
        if !self.address.is_null() {
            // Safety: both `address` and `length` are used in the call to `mmap`.
            if let Err(err) = munmap(self.address, self.length) {
                // We can't really handle the error properly here so we'll log
                // it and if we're testing (and not panicking) we'll panic on
                // it.
                error!("error unmapping data: {}", err);
                if !thread::panicking() {
                    #[cfg(test)]
                    panic!("error unmapping data: {}", err);
                }
            }
        } else {
            debug_assert!(self.length == 0);
        }
    }
}

fn mmap(
    addr: *mut libc::c_void,
    len: libc::size_t,
    protection: libc::c_int,
    flags: libc::c_int,
    fd: libc::c_int,
    offset: libc::off_t,
) -> io::Result<*mut libc::c_void> {
    assert!(len > 0);
    assert!(is_page_aligned(addr as usize)); // Null is also page aligned.
    assert!(is_page_aligned(offset as usize)); // 0 is also page aligned.
    let addr = unsafe { libc::mmap(addr, len, protection, flags, fd, offset) };
    if addr == libc::MAP_FAILED {
        Err(io::Error::last_os_error())
    } else {
        Ok(addr)
    }
}

fn munmap(addr: *mut libc::c_void, len: libc::size_t) -> io::Result<()> {
    assert!(len != 0);
    assert!(!addr.is_null());
    assert!(is_page_aligned(addr as usize));
    if unsafe { libc::munmap(addr, len) } != 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn madvise(addr: *mut libc::c_void, len: libc::size_t, advice: libc::c_int) -> io::Result<()> {
    debug_assert!(len != 0);
    if unsafe { libc::madvise(addr, len, advice) != 0 } {
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
struct DateTime {
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
    const INVALID: DateTime = DateTime {
        seconds: 0,
        subsec_nanos: u32::from_be_bytes(DateTime::INVALID_BIT.to_ne_bytes()),
    };

    /// Returns `true` if the `REMOVED_BIT` is set.
    fn is_removed(&self) -> bool {
        let subsec_nanos = u32::from_be_bytes(self.subsec_nanos.to_ne_bytes());
        subsec_nanos & DateTime::REMOVED_BIT != 0
    }

    /// Returns `true` if the time is invalid.
    fn is_invalid(&self) -> bool {
        const NANOS_PER_SEC: u32 = 1_000_000_000;
        let subsec_nanos = u32::from_be_bytes(self.subsec_nanos.to_ne_bytes());
        (subsec_nanos & !DateTime::REMOVED_BIT) > NANOS_PER_SEC
    }

    /// Returns the same `DateTime`, but as removed at
    /// (`ModifiedTime::Removed`).
    fn mark_removed(mut self) -> Self {
        let mut subsec_nanos = u32::from_be_bytes(self.subsec_nanos.to_ne_bytes());
        subsec_nanos |= DateTime::REMOVED_BIT;
        self.subsec_nanos = u32::from_ne_bytes(subsec_nanos.to_be_bytes());
        self
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
enum ModifiedTime {
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
