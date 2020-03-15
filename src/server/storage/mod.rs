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
use std::time::{Duration, SystemTime};
use std::{ptr, slice};

use log::error;

use crate::Key;

#[cfg(test)]
mod tests;

pub(crate) struct Index {
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
            libc::MAP_PRIVATE, // TODO: look into MAP_POPULATE on Linux.
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
    /// in which can the address in not unmapped.
    address: *mut libc::c_void,
    /// Mmap allocation length. Safety: must be length used in `mmap`.
    length: libc::size_t,
    /// Iterator based on a slice, so we don't have to do the heavy lifting.
    /// This points to the `mmap`-ed memory.
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
            if unsafe { libc::munmap(self.address, self.length) } != 0 {
                let err = io::Error::last_os_error();
                error!("error unmapping index: {}", err);
                #[cfg(test)]
                panic!("error unmapping index: {}", err);
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
