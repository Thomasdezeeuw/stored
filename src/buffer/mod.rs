//! I/O buffer and related types.
//!
//! The main type is [`Buffer`], which is to be used as buffer for reading.
//! After the entire request is read the buffer can be split, using
//! [`Buffer::split_write`], to create a [`WriteBuffer`]. The `WriteBuffer` can
//! be used to buffer writes to a connection. This way we can use a single
//! buffer for both reading and writing to and from a connection.
//!
//! # Notes
//!
//! Most types in this module, such as [`Buffer`], can use the alternative flag
//! (`{:#?}`) when debug printing. It will try to print the buffer as an UTF-8
//! string, defaulting to the raw bytes if the bytes are not a valid UTF-8
//! string.

use std::cmp::min;
use std::io::{self, IoSlice, Write};
use std::mem::MaybeUninit;
use std::str::from_utf8;
use std::{fmt, ptr, slice};

use heph::net::Bytes;

#[cfg(test)]
mod tests;

/// Size used as initial buffer size.
const INITIAL_BUF_SIZE: usize = 4 * 1024;

/// Minimum number of processed bytes before the data is moved to the start of
/// the buffer.
const MIN_SIZE_MOVE: usize = 512;

/// Buffer, *you know what a buffer is*.
///
/// This buffer buffers reads (that is what a buffer does), but also keeps track
/// of the bytes processed which is useful in combination when parsing a
/// request.
pub struct Buffer {
    data: Vec<u8>,
    /// The number of bytes already processed in `data`.
    processed: usize,
}

impl Buffer {
    /// Create a empty `Buffer`.
    pub const fn empty() -> Buffer {
        Buffer {
            data: Vec::new(),
            processed: 0,
        }
    }

    /// Create a new `Buffer` with a already allocated memory.
    pub fn new() -> Buffer {
        Buffer {
            data: Vec::with_capacity(INITIAL_BUF_SIZE),
            processed: 0,
        }
    }

    /// Reserves an allocation that can read a buffer of at least `length`
    /// bytes, including the unprocessed bytes already in the buffer.
    pub fn reserve_atleast(&mut self, length: usize) {
        if length > self.capacity_left() {
            // Need more capacity, try removing the already processed bytes
            // first.
            self.move_to_start(true);
            if length > self.capacity_left() {
                // Still need more.
                self.data.reserve(length);
            }
        }
    }

    /// Returns the number of unprocessed, read bytes.
    pub fn len(&self) -> usize {
        self.data.len() - self.processed
    }

    /// Returns `true` if there are no unprocessed, read bytes.
    pub fn is_empty(&self) -> bool {
        self.data.len() == self.processed
    }

    /// Returns the unprocessed, read bytes.
    pub fn as_slice(&self) -> &[u8] {
        // NOTE: also see `split`.
        &self.data[self.processed..]
    }

    /// Copies all bytes currently in the buffer (up to what fits into `buf`)
    /// into `buf`, and marks the bytes as processed. Returns the number of
    /// bytes copied.
    pub fn copy_to<B>(&mut self, mut buf: B) -> usize
    where
        B: Bytes,
    {
        let dst = buf.as_bytes();
        let len = min(self.len(), dst.len());
        // Safety: both the src and dst pointers are good. And we've ensured
        // that the length is correct, not overwriting data we don't own or
        // reading data we don't own.
        unsafe { ptr::copy_nonoverlapping(self.as_slice().as_ptr(), dst.as_mut_ptr().cast(), len) }
        self.processed(len);
        // Safety: just copied the bytes above.
        unsafe { buf.update_length(len) }
        len
    }

    /// Split the buffer into used bytes and unused bytes. This is equivalent to
    /// calling `as_slice` and `Bytes::as_bytes` (but that isn't allowed due to
    /// the lifetime restrictions).
    fn split<'b>(&'b mut self) -> (&'b [u8], &'b mut [MaybeUninit<u8>]) {
        // NOTE: also see `as_slice` and `Bytes::as_bytes`.
        assert!(self.data.len() >= self.processed);
        // Safety: since the two slices don't overlap this is safe, also see
        // `slice::split_at_mut`.
        let unused_bytes = unsafe {
            // NOTE: keep this in check with `Bytes::as_bytes`.
            let data_ptr = self.data.as_mut_ptr().add(self.data.len()).cast();
            slice::from_raw_parts_mut(data_ptr, self.capacity_left())
        };
        let used_bytes = &self.data[self.processed..];
        assert!(
            used_bytes.as_ptr() as usize + used_bytes.len()
                < unused_bytes.as_ptr() as usize + unused_bytes.len()
        );
        (used_bytes, unused_bytes)
    }

    /// Returns the next byte.
    pub fn next_byte(&self) -> Option<u8> {
        self.data[self.processed..].first().copied()
    }

    /// Create a new `BufView` from the `Buffer`.
    pub fn view(self, length: usize) -> BufView {
        debug_assert!(self.len() >= length);
        BufView { buf: self, length }
    }

    /// Split the buffer in the currently read bytes and a temporary write
    /// buffer.
    ///
    /// Operations on the returned `WriteBuffer` will be removed from the
    /// original `Buffer` (`self`) once the working buffer is dropped. This
    /// allows `Buffer` to be reused for writing while normally using it for
    /// reading.
    ///
    /// This ensures that at least `reserve` bytes are available for the
    /// `WriteBuffer`.
    pub fn split_write<'b>(&'b mut self, reserve: usize) -> (&'b [u8], WriteBuffer<'b>) {
        self.reserve_atleast(reserve);
        let (used_bytes, unused_bytes) = self.split();
        let wbuf = WriteBuffer {
            inner: TempBuffer {
                buf: unused_bytes,
                length: 0,
                processed: 0,
            },
        };
        (used_bytes, wbuf)
    }

    /// Mark `n` bytes as processed.
    pub fn processed(&mut self, n: usize) {
        assert!(
            self.processed + n <= self.data.len(),
            "marking bytes as processed beyond read range"
        );
        self.processed += n;
    }

    /// Reset the buffer so it can be used reading from another source.
    pub fn reset(&mut self) {
        self.data.clear();
        self.processed = 0;
    }

    /// Number of bytes to which can be written.
    fn capacity_left(&self) -> usize {
        self.data.capacity() - self.data.len()
    }

    /// Move the read bytes to the start of the buffer, if needed.
    ///
    /// If `force` is true it always move the buffer to the start, ensuring that
    /// `processed` is always 0.
    fn move_to_start(&mut self, force: bool) {
        if self.processed == 0 {
            // No need to do anything.
        } else if self.data.len() == self.processed {
            // All data is processed.
            self.data.clear();
            self.processed = 0;
        } else if force || self.processed >= MIN_SIZE_MOVE {
            // Move unread bytes to the start of the buffer.
            drop(self.data.drain(..self.processed));
            self.processed = 0;
        }
    }
}

impl Bytes for Buffer {
    fn as_bytes(&mut self) -> &mut [MaybeUninit<u8>] {
        // Safety: `Vec` ensures the pointer is correct for us. The pointer is
        // at least valid for start + `Vec::capacity` bytes, a range we stay
        // within.
        unsafe {
            // NOTE: keep this in check with `unused_bytes` in `split`.
            let data_ptr = self.data.as_mut_ptr().add(self.data.len()).cast();
            slice::from_raw_parts_mut(data_ptr, self.capacity_left())
        }
    }

    unsafe fn update_length(&mut self, n: usize) {
        // Safety: caller must ensure the bytes are initialised.
        debug_assert!(
            self.capacity_left() >= n,
            "marking bytes as read beyond read range"
        );
        self.data.set_len(self.data.len() + n)
    }
}

impl fmt::Debug for Buffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = self.as_slice();
        if f.alternate() {
            if let Ok(string) = from_utf8(bytes) {
                return f.write_str(string);
            }
        }
        bytes.fmt(f)
    }
}

/// An immutable view into a [`Buffer`].
pub struct BufView {
    buf: Buffer,
    length: usize,
}

impl BufView {
    /// Returns the number of bytes.
    pub fn len(&self) -> usize {
        self.length
    }

    /// Returns the bytes.
    pub fn as_slice(&self) -> &[u8] {
        &self.buf.as_slice()[..self.length]
    }

    /// Marks the bytes in this view as processed, returning the `Buffer`.
    pub fn processed(mut self) -> Buffer {
        self.buf.processed(self.length);
        self.buf
    }
}

impl fmt::Debug for BufView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = self.as_slice();
        if f.alternate() {
            if let Ok(string) = from_utf8(bytes) {
                return f.write_str(string);
            }
        }
        bytes.fmt(f)
    }
}

/// Split of from `Buffer` to allow the buffer to be used temporarily.
///
/// This allows `Buffer` to be used as both a read and write buffer.
pub struct WriteBuffer<'b> {
    inner: TempBuffer<'b>,
}

impl<'b> WriteBuffer<'b> {
    /// Returns `true` if the buffer has no unprocessed, written bytes.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns the unprocessed, written bytes.
    pub fn as_slice(&self) -> &[u8] {
        self.inner.as_slice()
    }

    /// Returns the unprocessed, written bytes.
    pub fn as_mut_bytes(&mut self) -> &mut [u8] {
        self.inner.as_mut_bytes()
    }
}

impl<'b> Write for WriteBuffer<'b> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = buf.len();
        assert!(
            self.inner.capacity_left() >= len,
            "trying to write a too large buffer"
        );
        // Safety: checked above if we have enough capacity left.
        unsafe {
            ptr::copy_nonoverlapping(
                buf.as_ptr(),
                MaybeUninit::slice_as_mut_ptr(&mut self.inner.buf[self.inner.length..]),
                buf.len(),
            );
        }
        self.inner.length += len;
        Ok(len)
    }

    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        bufs.iter().map(|buf| self.write(buf)).sum()
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.write(buf).map(|n| {
            debug_assert_eq!(buf.len(), n);
            ()
        })
    }

    fn write_all_vectored(&mut self, bufs: &mut [IoSlice<'_>]) -> io::Result<()> {
        self.write_vectored(bufs).map(|n| {
            debug_assert_eq!(bufs.iter().map(|b| b.len()).sum::<usize>(), n);
            ()
        })
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn write_fmt(&mut self, args: fmt::Arguments<'_>) -> io::Result<()> {
        // NOTE: writing never returns an error (it can only panic), so the
        // error return is irrelevant.
        fmt::Write::write_fmt(self, args).map_err(|_: fmt::Error| io::ErrorKind::Other.into())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'b> fmt::Write for WriteBuffer<'b> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.write_all(s.as_bytes()).map_err(|_| fmt::Error)
    }

    fn write_fmt(&mut self, args: fmt::Arguments<'_>) -> fmt::Result {
        if let Some(s) = args.as_str() {
            self.write_str(s)
        } else {
            fmt::write(self, args)
        }
    }
}

impl<'b> fmt::Debug for WriteBuffer<'b> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

/// Buffer backing [`ReadBuffer`] and [`WriteBuffer`].
struct TempBuffer<'b> {
    /// Buffer capacity from `Buffer`, `buf[..self.length]` bytes are valid
    /// (i.e. initialised) and `buf[self.processed..self.length]` are yet to be
    /// written.
    buf: &'b mut [MaybeUninit<u8>],
    /// Number of bytes written into `buf`.
    /// Must always be larger then `self.processed`.
    length: usize,
    /// The number of bytes *we* already processed in `buf`.
    /// Must always be less then `self.length`.
    processed: usize,
}

impl<'b> TempBuffer<'b> {
    fn len(&self) -> usize {
        self.length - self.processed
    }

    fn is_empty(&self) -> bool {
        self.processed == self.length
    }

    fn as_slice(&self) -> &[u8] {
        // Safety: `self.buf[..self.length]` bytes are initialised as per
        // the comment on the field.
        unsafe { MaybeUninit::slice_assume_init_ref(&self.buf[self.processed..self.length]) }
    }

    fn as_mut_bytes(&mut self) -> &mut [u8] {
        // Safety: See `TempBuffer::as_slice`.
        unsafe { MaybeUninit::slice_assume_init_mut(&mut self.buf[self.processed..self.length]) }
    }

    fn capacity_left(&self) -> usize {
        self.buf.len() - self.len()
    }
}

impl<'b> fmt::Debug for TempBuffer<'b> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = self.as_slice();
        if f.alternate() {
            if let Ok(string) = from_utf8(bytes) {
                return f.write_str(string);
            }
        }
        bytes.fmt(f)
    }
}
