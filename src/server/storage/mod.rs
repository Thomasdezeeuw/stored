#![allow(dead_code)] // FIXME: use this.

// FIXME: endian of integers. Currently moving an index from little- to
// big-endian is invalid.

// TODO: benchmark the following flags to mmap:
// - MAP_HUGETLB, with:
//   - MAP_HUGE_2MB, or
//   - MAP_HUGE_1GB
// - MAP_POPULATE

use std::fs::{File, OpenOptions};
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

struct Data {
    /// Data file opened for reading (for use in `entries`) and writing in
    /// append-only mode for adding entries (`add_entry`).
    file: File,
    /// Current length of the file and all `mmap`ed areas.
    length: libc::size_t,
    /// `mmap`ed areas.
    /// See `check` for notes about safety.
    areas: Vec<MmapArea>,
}

/// The size of a single page, used in `mmap`ing memory.
// TODO: ensure this is correct on all architectures.
const PAGE_SIZE: usize = 1 << 12; // 4096.
const PAGE_BITS: usize = 12;

struct MmapArea {
    /// Mmap address. Safety: must be the `mmap` returned address.
    address: NonNull<libc::c_void>,
    /// Absolute offset in the file.
    offset: libc::off_t,
    /// Mmap allocation length. Safety: must be length used in `mmap`.
    length: libc::size_t,
}

impl MmapArea {
    /// Returns `true` if the value at `offset` with `length` is in this area.
    fn in_area(&self, offset: u64, length: u32) -> bool {
        let area_offset = self.offset as u64;
        area_offset <= offset && (offset + length as u64) <= (area_offset + self.length as u64)
    }
}

impl Data {
    /// Open a data file.
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
            data.grow_to(length)?;
        }

        data.check();
        Ok(data)
    }

    /// Add `value` at the end of the data log.
    /// Returns its offset in the file and its `mmap`ed address.
    fn add_value(&mut self, value: &[u8]) -> io::Result<(u64, NonNull<u8>)> {
        assert!(!value.is_empty(), "tried to store an empty value");
        // First add the value to the file.
        self.file
            .write_all(value)
            .and_then(|()| self.file.sync_all())?;

        // Next grow our `mmap`ed area(s).
        let offset = self.length;
        let address = self.grow_to(value.len())?;
        self.check();
        Ok((offset as u64, address))
    }

    /// Grows the `mmap`ed data by `grow_by_length` bytes.
    fn grow_to(&mut self, grow_by_length: libc::size_t) -> io::Result<NonNull<u8>> {
        // Try to grow the last `mmap`ed area.
        if let Some(area) = self.areas.last_mut() {
            // The number of bytes used in last page of our mmap area.
            let used_last_page = area.length % PAGE_SIZE;

            // If our last page is fully used or the value doesn't fit in this
            // last page we need to ensure that no other mapping is using the
            // next page.
            let can_grow = if used_last_page == 0 || (PAGE_SIZE - used_last_page) < grow_by_length {
                // Probe the next page to see if is being used.
                // Probe address is the next page (`+ PAGE_SIZE`), aligned
                // to the page (`>> PAGE_BITS << PAGE_BITS`).
                let probe_address: *mut libc::c_void =
                    (((area.address.as_ptr() as usize + PAGE_SIZE) >> PAGE_BITS) << PAGE_BITS)
                        as *mut _;

                let actual_address = mmap(
                    probe_address,
                    PAGE_SIZE,
                    libc::PROT_NONE,
                    libc::MAP_PRIVATE | libc::MAP_ANON,
                    -1,
                    0,
                )?;

                if actual_address == probe_address {
                    // We can grow the page safely as we now own the next page
                    // as well. The next call to mmap below will overwrite the
                    // probe mapping so we don't have to unmap it.
                    true
                } else {
                    // Our probe failed, we need to create another area.
                    munmap(actual_address, PAGE_SIZE)?;
                    false
                }
            } else {
                // If the value does fit in the last page we can safely
                // overwrite our own mapping.
                true
            };

            if can_grow {
                let new_length = area.length + grow_by_length;
                let res = mmap(
                    area.address.as_ptr(),
                    new_length,
                    libc::PROT_READ,
                    libc::MAP_PRIVATE | libc::MAP_FIXED, // Force the same address.
                    self.file.as_raw_fd(),
                    area.offset,
                );

                if let Ok(new_address) = res {
                    // address and offset should remain unchanged.
                    assert_eq!(
                        new_address,
                        area.address.as_ptr(),
                        "remapping the mmap area changed the address"
                    );
                    area.length = new_length;
                    self.length += grow_by_length;
                    let value_ptr = unsafe {
                        (area.address.as_ptr() as *mut u8).add(area.length - grow_by_length)
                    };
                    return Ok(NonNull::new(value_ptr).unwrap());
                }
            }
        }

        // If we've failed to grow the last `mmap`ed area, or didn't yet have
        // one, we'll allocate a new area.

        let offset = self
            .areas
            .last()
            .map(|area| area.offset + area.length as libc::off_t)
            .unwrap_or(0);
        let address = mmap(
            ptr::null_mut(),
            grow_by_length,
            libc::PROT_READ,
            libc::MAP_PRIVATE,
            self.file.as_raw_fd(),
            offset,
        )?;

        self.areas.push(MmapArea {
            // Safety: `mmap` doesn't return a null address.
            address: unsafe { NonNull::new_unchecked(address) },
            offset: offset,
            length: grow_by_length,
        });

        self.length += grow_by_length;
        Ok(NonNull::new(address as *mut u8).unwrap())
    }

    /// Returns the address for the value at `offset`, with `length`.
    ///
    /// # Notes
    ///
    /// The returned address lifetime is tied to the `Data` struct.
    fn address_for(&self, offset: u64, length: u32) -> Option<NonNull<u8>> {
        // TODO: return slice? What lifetime would that have?
        self.areas
            .iter()
            .find(|area| area.in_area(offset, length))
            .map(|area| {
                let relative_offset = offset as libc::off_t - area.offset;
                assert!(relative_offset >= 0);
                let address = unsafe { area.address.as_ptr().add(relative_offset as usize) };
                assert!(!address.is_null());
                NonNull::new(address as *mut u8).unwrap()
            })
    }

    /// Run all safety checks.
    ///
    /// Checks the following:
    ///
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
            let mut total_length: libc::size_t = 0;
            let mut last_offset: libc::off_t = -1;
            for area in &self.areas {
                assert!(
                    area.address.as_ptr() as usize % PAGE_SIZE == 0,
                    "invalid mmap address alignment, maybe invalid PAGE_SIZE?"
                );
                assert!(area.offset >= 0, "negative offset");
                assert!(
                    area.offset > last_offset,
                    "mmaped areas not sorted by offset"
                );
                assert_eq!(
                    area.offset as libc::size_t, total_length,
                    "invalid offset for mmap area"
                );
                last_offset = area.offset;
                total_length += area.length;
            }
            assert_eq!(self.length, total_length, "invalid total length");
        }
    }
}

impl Drop for MmapArea {
    fn drop(&mut self) {
        // Safety: both `address` and `length` are used in the call to `mmap`.
        if let Err(err) = munmap(self.address.as_ptr(), self.length) {
            error!("error unmapping data: {}", err);
            if !thread::panicking() {
                #[cfg(test)]
                panic!("error unmapping data: {}", err);
            }
        }
    }
}

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
    /// `Index` was just opened this means all entries in the entire index file.
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
    let addr = unsafe { libc::mmap(addr, len, protection, flags, fd, offset) };
    if addr == libc::MAP_FAILED {
        Err(io::Error::last_os_error())
    } else {
        Ok(addr)
    }
}

fn munmap(addr: *mut libc::c_void, len: libc::size_t) -> io::Result<()> {
    if unsafe { libc::munmap(addr, len) } != 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn madvise(addr: *mut libc::c_void, len: libc::size_t, advice: libc::c_int) -> io::Result<()> {
    if unsafe { libc::madvise(addr, len, advice) != 0 } {
        return Err(io::Error::last_os_error());
    } else {
        Ok(())
    }
}

/// Converts an `address` and `length` from `mmap` into a slice of `T`.
///
/// # Notes
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

/// Iterator that all entries reads from an [`Index`].
#[derive(Debug)]
pub(crate) struct Entries<'i> {
    /// Mmap address. Safety: must be the `mmap` returned address, may be null
    /// in which case the address is not unmapped.
    address: *mut libc::c_void,
    /// Mmap allocation length. Safety: must be length used in `mmap`.
    length: libc::size_t,
    /// Iterator based on a slice, so we don't have to do the heavy lifting.
    /// This points to the `mmap`ed memory.
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
        if !self.address.is_null() {
            // Safety: both `address` and `length` are used in the call to `mmap`.
            if let Err(err) = munmap(self.address, self.length) {
                error!("error unmapping data: {}", err);
                if !thread::panicking() {
                    #[cfg(test)]
                    panic!("error unmapping data: {}", err);
                }
            }
        }
    }
}

/// Entry in the [`Index`].
///
/// The layout of the `Entry` is fixed as it must be loaded from disk.
#[repr(C)]
#[derive(Eq, PartialEq, Debug)]
pub(crate) struct Entry {
    /// Key for the value.
    key: Key,
    /// Offset into the data file.
    ///
    /// If this is `u64::MAX` the value has been removed.
    offset: u64,
    /// Length of the value in bytes.
    length: u32,
    /// Time at which the value is created.
    created: DateTime,
}

/// Layout stable date-time format.
///
/// This is essential a [`Duration`] since [unix epoch].
///
/// [unix epoch]: std::time::SystemTime::UNIX_EPOCH
///
/// # Notes
///
/// Can't represent times before Unix epoch.
#[repr(C, packed)] // Packed to reduce the size of `Index`.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) struct DateTime {
    /// Number of seconds since Unix epoch.
    seconds: u64,
    /// Number of nano sub-seconds, always less then 1 billion (the number of
    /// nanoseconds in a second).
    subsec_nanos: u32,
}

impl DateTime {
    /// Returns the current time as `DateTime`.
    pub(crate) fn now() -> DateTime {
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
