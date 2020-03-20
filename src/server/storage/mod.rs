//! Module with the `Storage` handle to a database.
//!
//! # Design
//!
//! The database is split into two files: the data file and the index file. Both
//! files are append-only logs [^1]. The index file determines what blobs are
//! actually in the database, this means that even though the bytes might be in
//! the data file it doesn't mean that those bytes are in the database. The
//! index file has the final say in what blobs are and aren't in the database.
//!
//! [^1]: Not really, but pretend like they are for now.
//!
//! ## Adding blobs
//!
//! Adding new blobs to the database is done in two phases. First, by appending
//! the blob's bytes to the data file, ensuring its fully stored on (synced to)
//! disk. Second, adding a new entry to the index file, again ensuring its fully
//! synced. Only after those two steps is a blob stored in the database.
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
//!
//! ## Remove blobs
//!
//! TODO: document and implement this.
//!
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
//!
//! # Validating the database
//!
//! Since all entries hold the key for the blob each blob can validated using the
//! key as checksum. This will point out any corruptions and could help in
//! restoring them. TODO: implement this.

// TODO: add a magic string and version at the start of the index and data
// files? To ensure we're opening a correct file?

// FIXME: endian of integers. Currently moving an index from little- to
// big-endian is invalid.

// TODO: benchmark the following flags to mmap:
// - MAP_HUGETLB, with:
//   - MAP_HUGE_2MB, or
//   - MAP_HUGE_1GB
// - MAP_POPULATE

use std::collections::hash_map::{self, HashMap};
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{self, Write};
use std::iter::FusedIterator;
use std::mem::{align_of, size_of};
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::ptr::{self, NonNull};
use std::time::{Duration, SystemTime};
use std::{slice, thread};

use log::error;

use crate::Key;

#[cfg(test)]
mod tests;

/// A Binary Large OBject (BLOB).
#[derive(Debug, Clone)]
pub struct Blob<'s> {
    bytes: &'s [u8],
    created: SystemTime,
}

impl<'s> Blob<'s> {
    /// Returns the bytes that make up the `Blob`.
    pub fn bytes(&self) -> &[u8] {
        self.bytes
    }

    /// Returns the time at which the blob was added.
    pub fn created_at(&self) -> SystemTime {
        self.created
    }
}

/// Handle to a database.
#[derive(Debug)]
pub struct Storage<'s> {
    index: Index,
    /// All blobs currently in `Index` and `Data`. The lifetime `'s` refers to
    /// the `mmap`ed areas in `Data`.
    ///
    /// # Safety
    ///
    /// `blobs` must be declared before `data`because it must be dropped before
    /// `data`.
    // TODO: use different hashing algorithm.
    blobs: HashMap<Key, Blob<'s>>,
    /// # Safety
    ///
    /// Must outlive `blobs`, the lifetime `'s` refers to this.
    data: Data,
}

impl<'s> Storage<'s> {
    /// Open a database.
    ///
    /// `path` must be a directory with the `index` and `data` files.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Storage<'s>> {
        let path = path.as_ref();

        // Ensure the directory exists.
        create_dir_all(path)?;

        let data = Data::open(path.join("data"))?;

        let mut index = Index::open(path.join("index"))?;
        let entries = index.entries()?;

        // Add all blobs currently in the database.
        let mut blobs = HashMap::with_capacity(entries.len());
        blobs.extend(entries.map(|entry| {
            let address = data
                .address_for(entry.offset, entry.length)
                // TODO: handle this better. Think should happen. It it possible
                // that there are more blobs in `Data` then the `Index`
                // suggests.
                .expect("missing blobs from data file");

            // Safety: this is safe because `Data` outlives `blobs`, see
            // `Storage.blobs` docs.
            let bytes = unsafe { slice::from_raw_parts(address.as_ptr(), entry.length as usize) };

            let blob = Blob {
                bytes,
                created: entry.created.into(),
            };
            (entry.key.clone(), blob)
        }));

        Ok(Storage { data, index, blobs })
    }

    /// Returns the number of blobs stored in the database.
    pub fn len(&self) -> usize {
        self.blobs.len()
    }

    /// Returns the number of bytes of data stored.
    pub fn data_size(&self) -> u64 {
        self.data.file_length() as u64
    }

    /// Returns the size of the index in bytes.
    pub fn index_size(&self) -> u64 {
        self.len() as u64 * size_of::<Entry>() as u64
    }

    /// Returns the total size of the database (data and index).
    pub fn total_size(&self) -> u64 {
        self.data_size() + self.index_size()
    }

    /// Returns a reference to the `Blob` corresponding to the key, if stored.
    pub fn lookup(&self, key: &Key) -> Option<Blob<'s>> {
        self.blobs.get(key).cloned()
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
    pub fn add_blob<'b>(&mut self, blob: &'b [u8]) -> AddResult {
        let key = Key::for_blob(blob);

        // Can't have double entries.
        if self.blobs.contains_key(&key) {
            debug_assert_eq!(self.lookup(&key).unwrap().bytes(), blob);
            return AddResult::AlreadyPresent(key);
        }

        // First add the blob to the data file. If something happens the blob
        // will be written (or not), but its not *in* the database as the index
        // defines what is in the database.
        match self.data.add_blob(blob) {
            Ok((offset, address)) => AddResult::Ok(AddBlob {
                key,
                offset,
                length: blob.len() as u32,
                address,
            }),
            Err(err) => AddResult::Err(err),
        }
    }

    /// Commit to `query`.
    pub fn commit<Q>(&mut self, query: Q) -> io::Result<Q::Return>
    where
        Q: Query,
    {
        query.commit(self)
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
    AlreadyPresent(Key),
    /// I/O error.
    Err(io::Error),
}

/// a `Query` is a partially prepared storage operation.
///
/// An example is [`AddBlob`] that already has the data of the blob stored, but
/// the blob itself isn't yet in the database. Only after [committing] will the
/// blob be stored (and thus accessible).
///
/// [committing]: Query::commit
pub trait Query {
    /// Type returned after a `Query` is [commited].
    ///
    /// [commited]: Storage::commit
    type Return;

    /// Commit to the query.
    fn commit(self, storage: &mut Storage) -> io::Result<Self::Return>;

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
}

impl Query for AddBlob {
    type Return = Key;

    fn commit(self, storage: &mut Storage) -> io::Result<Self::Return> {
        use hash_map::Entry::*;
        match storage.blobs.entry(self.key.clone()) {
            Occupied(_) => {
                // If the blob has already been added we don't want to modify
                // it.
                // TODO: mark the blob bytes as unused and clean them up.
            }
            Vacant(entry) => {
                // The data is already stored so we can add the blob to the
                // index.
                let index_entry = Entry {
                    key: self.key.clone(),
                    offset: self.offset,
                    length: self.length,
                    // TODO: should this be provided so that all stores have the
                    // same created time?
                    created: DateTime::now(),
                };
                storage.index.add_entry(&index_entry)?;

                // Now that the data and index entry are stored we can insert
                // the blob into our database.
                debug_assert_eq!(
                    storage.data.address_for(self.offset, self.length),
                    Some(self.address)
                );
                let blob = Blob {
                    // Safety: `Data` must outlive `blobs` in `Storage`.
                    bytes: unsafe {
                        slice::from_raw_parts(self.address.as_ptr(), self.length as usize)
                    },
                    created: index_entry.created.into(),
                };
                entry.insert(blob);
            }
        }

        Ok(self.key)
    }

    fn abort(self, _storage: &mut Storage) -> io::Result<()> {
        // Since the blob isn't in the index, it also isn't in the database.
        // TODO: cleanup the unused bytes.
        Ok(())
    }
}

/// Handle for a data file.
#[derive(Debug)]
struct Data {
    /// Data file opened for reading and writing in append-only mode (for
    /// adding new blobs).
    file: File,
    /// Current length of the file and all `mmap`ed areas.
    length: u64,
    /// `mmap`ed areas.
    /// See `check` for notes about safety.
    areas: Vec<MmapArea>,
}

/// The size of a single page, used in probing `mmap`ing memory.
// TODO: ensure this is correct on all architectures. Tested in
// `data::page_size` test in the tests module.
const PAGE_SIZE: usize = 1 << 12; // 4096.
const PAGE_BITS: usize = 12;

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
    // TODO: just make this a `u64`?
    offset: libc::off_t,
    /// Length actually used of the mmap allocation, relative to `offset`.
    /// `mmap_length` might be larger due to the page alignment requirement for
    /// the offset.
    length: libc::size_t,
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
        let relative_offset = offset as libc::off_t - self.offset;
        assert!(
            relative_offset >= 0,
            "want offset: {}, absolute offset: {}, relative offset: {}",
            offset,
            self.offset,
            relative_offset
        );

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
                areas: Vec::new(),
            })?;

        let metadata = data.file.metadata()?;
        let length = metadata.len() as libc::size_t;
        if length != 0 {
            data.new_area(length)?;
        }

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
    fn add_blob(&mut self, blob: &[u8]) -> io::Result<(u64, NonNull<u8>)> {
        assert!(!blob.is_empty(), "tried to store an empty blob");
        // First add the blob to the file.
        self.file
            .write_all(blob)
            .and_then(|()| self.file.sync_all())?;

        // Next grow our `mmap`ed area(s).
        let offset = self.length;
        let address = self.grow_by(blob.len())?;
        self.check();
        Ok((offset as u64, address))
    }

    /// Grows the `mmap`ed data by `length` bytes.
    ///
    /// This either grows the last `mmap`ed area, or allocates a new one.
    /// Returns the starting address at which the area is grown, i.e. where the
    /// blob is mmaped into memory.
    fn grow_by(&mut self, length: libc::size_t) -> io::Result<NonNull<u8>> {
        // Try to grow the last `mmap`ed area.
        if let Some(area) = self.areas.last_mut() {
            let can_grow = if needs_next_page(area.mmap_length, length) {
                // If we need another page we need to reserve it to ensure that
                // we're not overwriting a mapping that we don't own.
                reserve_next_page(&area)?
            } else {
                // Can grow inside the same page.
                true
            };

            if can_grow {
                // If we successfully reserved the next page, or we can grow
                // within the next page it is safe to overwrite our own mapping
                // using `MAP_FIXED`.
                let new_length = area.mmap_length + length;

                let aligned_offset = if is_page_aligned(area.offset as usize) {
                    area.offset
                } else {
                    prev_page_aligned(area.offset as usize) as libc::off_t
                };
                let res = mmap(
                    area.mmap_address.as_ptr(),
                    new_length,
                    libc::PROT_READ,
                    libc::MAP_PRIVATE | libc::MAP_FIXED, // Force the same address.
                    self.file.as_raw_fd(),
                    aligned_offset,
                );

                if let Ok(new_address) = res {
                    // Address and offset should remain unchanged.
                    assert_eq!(
                        new_address,
                        area.mmap_address.as_ptr(),
                        "remapping the mmap area changed the address"
                    );
                    // Update `mmap` and effective length.
                    area.mmap_length = new_length;
                    area.length += length;
                    // Update total `Data` length .
                    self.length += length as u64;
                    // The blob is located in the last `length` bytes.
                    let offset = area.offset as u64 + (area.length - length) as u64;
                    let blob_ptr = area.offset(offset);
                    return Ok(blob_ptr);
                }
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
    fn new_area(&mut self, length: libc::size_t) -> io::Result<NonNull<u8>> {
        // Get the file offset from the last `mmap`ed area.
        let offset = self
            .areas
            .last()
            .map(|area| area.offset + area.length as libc::off_t)
            .unwrap_or(0);

        // Offset must be page aligned. This means we can have overlapping
        // section in the entire mmaped area, but that is ok.
        let (aligned_offset, offset_alignment_diff) = if is_page_aligned(offset as usize) {
            // Already page aligned, neat! No overlapping areas.
            (offset, 0)
        } else {
            // Offset is not page aligned, so we mmap the previous page again to
            // ensure the blob can be read from continuous memory.
            let aligned_offset = prev_page_aligned(offset as usize) as libc::off_t;
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
            aligned_offset,
        )?;

        // Safety: `mmap` doesn't return a null address.
        let address = NonNull::new(address).unwrap();
        let area = MmapArea {
            mmap_address: address,
            mmap_length: aligned_length,
            offset,
            length,
        };
        let blob_address = area.offset(offset as u64);
        self.areas.push(area);

        // Not counting the overlapping length!
        self.length += length as u64;
        Ok(blob_address)
    }

    /// Returns the address for the blob at `offset`, with `length`. Or `None`
    /// if the combination is invalid.
    ///
    /// # Notes
    ///
    /// The returned address lifetime is tied to the `Data` struct.
    fn address_for(&self, offset: u64, length: u32) -> Option<NonNull<u8>> {
        // TODO: return slice? What lifetime would that have?
        self.areas
            .iter()
            .find(|area| area.in_area(offset, length))
            .map(|area| area.offset(offset))
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
            let mut last_offset: libc::off_t = -1;
            for area in &self.areas {
                assert!(
                    area.mmap_address.as_ptr() as usize % PAGE_SIZE == 0,
                    "invalid mmap address alignment, maybe invalid PAGE_SIZE?"
                );
                assert!(area.offset >= 0, "negative offset");
                assert!(
                    area.offset > last_offset,
                    "mmaped areas not sorted by offset"
                );
                assert_eq!(
                    area.offset as u64, total_length,
                    "invalid offset for mmap area"
                );
                assert!(
                    area.length <= area.mmap_length,
                    "mmap area smaller the MmapArea.length"
                );
                last_offset = area.offset;
                total_length += area.length as u64;
            }
            assert_eq!(self.length, total_length, "invalid total length");
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
    let reserve_address: *mut libc::c_void = if is_page_aligned(end_address) {
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
        // Safety: both `address` and `length` are used in the call to `mmap`.
        if let Err(err) = munmap(self.mmap_address.as_ptr(), self.mmap_length) {
            // We can't really handle the error properly here so we'll log it
            // and if we're testing (and not panicking) we'll panic on it.
            error!("error unmapping data: {}", err);
            if !thread::panicking() {
                #[cfg(test)]
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
}

impl Index {
    /// Open an index file.
    fn open<P: AsRef<Path>>(path: P) -> io::Result<Index> {
        OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)
            .map(|file| Index { file })
    }

    /// Returns an iterator for all entries remaining in the `Index`. If the
    /// `Index` was just `open`ed this means all entries in the entire index
    /// file.
    fn entries<'i>(&'i mut self) -> io::Result<Entries<'i>> {
        let metadata = self.file.metadata()?;
        let length = metadata.len() as libc::size_t;
        if length == 0 {
            // Can't call mmap with `length = 0`.
            return Ok(Entries {
                address: ptr::null_mut(),
                length: 0,
                iter: [].iter(),
            });
        }

        if length % size_of::<Entry>() != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid index size: is it corrupted?",
            ));
        }

        let address = mmap(
            ptr::null_mut(),
            length,
            libc::PROT_READ,
            libc::MAP_PRIVATE,
            self.file.as_raw_fd(),
            0,
        )?;

        // For performance let the OS known we're going to read the entries
        // so it can prefetch the pages from disk.
        madvise(address, length, libc::MADV_SEQUENTIAL | libc::MADV_WILLNEED)?;

        let slice: &'i [Entry] = mmap_slice(address, length);
        Ok(Entries {
            address,
            length,
            iter: slice.iter(),
        })
    }

    /// Add a new `entry` to the `Index`.
    fn add_entry(&mut self, entry: &Entry) -> io::Result<()> {
        // Safety: because `u8` doesn't have any invalid bit patterns this is
        // OK. We're also ensured at least `size_of::<Entry>` bytes are valid.
        let bytes: &[u8] =
            unsafe { slice::from_raw_parts(entry as *const _ as *const _, size_of::<Entry>()) };
        self.file
            .write_all(&bytes)
            .and_then(|()| self.file.sync_all())
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
        return Err(io::Error::last_os_error());
    } else {
        Ok(())
    }
}

/// Converts an `address` and `length` returned from `mmap` into a slice of `T`.
///
/// # Safety
///
/// The lifetime `'a` must not outlive `address`.
fn mmap_slice<'a, T>(address: *mut libc::c_void, length: libc::size_t) -> &'a [T] {
    // Safety: we're ensuring alignment and the OS ensured the length
    // for us.
    assert!(
        address as usize % align_of::<T>() == 0,
        "mmap address not properly aligned"
    );
    assert!(length % size_of::<T>() == 0, "mmap length invalid");
    unsafe { slice::from_raw_parts(address as *const _, length / size_of::<T>()) }
}

/// Iterator that all entries from an [`Index`].
///
/// # Notes
///
/// Lifetime `'i` is connected to `Index`.
#[derive(Debug)]
struct Entries<'i> {
    /// Mmap address. Safety: must be the `mmap` returned address, may be null
    /// in which case the address is not unmapped. If null `length` must be 0.
    address: *mut libc::c_void,
    /// Mmap allocation length. Safety: must be length used in `mmap`.
    length: libc::size_t,
    /// Iterator based on a slice, so we don't have to do the heavy lifting.
    /// This points to the `mmap`ed memory `address`.
    iter: std::slice::Iter<'i, Entry>,
}

impl<'i> Iterator for Entries<'i> {
    type Item = &'i Entry;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<'i> FusedIterator for Entries<'i> {}

impl<'i> ExactSizeIterator for Entries<'i> {
    fn len(&self) -> usize {
        self.iter.len()
    }
}

impl<'i> Drop for Entries<'i> {
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

/// Entry in the [`Index`].
///
/// The layout of the `Entry` is fixed as it must be loaded from disk.
#[repr(C)]
#[derive(Eq, PartialEq, Debug)]
struct Entry {
    /// Key for the blob.
    key: Key,
    /// Offset into the data file.
    ///
    /// If this is `u64::MAX` the blbo has been removed.
    offset: u64,
    /// Length of the blob in bytes.
    length: u32,
    /// Time at which the blob is created.
    created: DateTime,
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
#[repr(C, packed)] // Packed to reduce the size of `Index`.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
struct DateTime {
    /// Number of seconds since Unix epoch.
    seconds: u64,
    /// Number of nano sub-seconds, always less then 1 billion (the number of
    /// nanoseconds in a second).
    subsec_nanos: u32,
}

impl DateTime {
    /// Returns the current time as `DateTime`.
    fn now() -> DateTime {
        SystemTime::now().into()
    }
}

impl From<SystemTime> for DateTime {
    /// # Notes
    ///
    /// If `time` is before Unix epoch this will return an empty `DateTime`,
    /// i.e. this same time as Unix epoch.
    fn from(time: SystemTime) -> DateTime {
        let elapsed = time
            .duration_since(SystemTime::UNIX_EPOCH)
            // We can't represent times before Unix epoch.
            .unwrap_or_else(|_| Duration::new(0, 0));

        DateTime {
            seconds: elapsed.as_secs(),
            subsec_nanos: elapsed.subsec_nanos(),
        }
    }
}

impl Into<SystemTime> for DateTime {
    fn into(self) -> SystemTime {
        let elapsed = Duration::new(self.seconds, self.subsec_nanos);
        SystemTime::UNIX_EPOCH + elapsed
    }
}
