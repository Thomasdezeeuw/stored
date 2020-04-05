//! Module with the `Buffer` type.

use std::future::Future;
use std::io::{self, IoSlice, Write};
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::task::{self, Poll};
use std::{ptr, slice};

use futures_io::AsyncRead;

#[cfg(test)]
mod tests;

/// Size used as initial buffer size.
const INITIAL_BUF_SIZE: usize = 8 * 1024;

/// Minimum number of processed bytes before the data is moved to the start of
/// the buffer.
const MIN_SIZE_MOVE: usize = 512;

/// Buffer, *you know what a buffer is*.
///
/// This buffer buffers reads (that is what a buffer does), but also keeps track
/// of the bytes processed which is useful in combination with the parsing
/// functions found in the [`parse`] module.
///
/// [`parse`]: crate::parse
#[derive(Debug)]
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
    pub fn split<'b>(&'b mut self, reserve: usize) -> (&'b [u8], WriteBuffer<'b>) {
        self.reserve_atleast(reserve);

        // Split our buffer into unused bytes (for the `WriteBuffer`) and used
        // bytes which we'll returned as first argument.
        // Safety: since the two slices don't overlap this is safe, also see
        // `slice::split_at_mut`.
        let unused_bytes = unsafe {
            let data_ptr = self.data.as_mut_ptr().add(self.data.len()) as *mut _;
            slice::from_raw_parts_mut(data_ptr, self.capacity_left())
        };
        let used_bytes = &self.data[self.processed..];

        let wbuf = WriteBuffer {
            buf: unused_bytes,
            length: 0,
            processed: 0,
        };
        (used_bytes, wbuf)
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

    /// Read from `reader` into this buffer.
    pub fn read_from<R>(&mut self, reader: R) -> Read<R>
    where
        R: AsyncRead,
    {
        self.move_to_start(false);
        Read {
            buffer: self,
            reader,
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
    pub fn as_bytes(&self) -> &[u8] {
        &self.data[self.processed..]
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
            return;
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

    /// Bytes available to read into.
    fn available_bytes(&mut self) -> &mut [MaybeUninit<u8>] {
        // Safety: `Vec` ensure the bytes are available, but there not
        // intialised so returning `MaybeUninit<u8>` is valid.
        unsafe {
            let data_ptr = self.data.as_mut_ptr().add(self.data.len()) as *mut _;
            slice::from_raw_parts_mut(data_ptr, self.capacity_left())
        }
    }

    /// Mark `n` bytes as newly read.
    ///
    /// # Unsafety
    ///
    /// Caller must ensure that the bytes are valid.
    unsafe fn read_bytes(&mut self, n: usize) {
        self.data.set_len(self.data.len() + n)
    }
}

/// [`Future`] that reads from reader `R` into a [`Buffer`].
///
/// # Notes
///
/// This future doesn't implement any waking mechanism, it up to the reader `R`
/// to handle wakeups.
#[derive(Debug)]
pub struct Read<'b, R> {
    buffer: &'b mut Buffer,
    reader: R,
}

impl<'b, R> Future for Read<'b, R>
where
    R: AsyncRead + Unpin,
{
    type Output = io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut task::Context) -> Poll<Self::Output> {
        let Read {
            buffer,
            ref mut reader,
        } = &mut *self;
        Pin::new(reader)
            .poll_read(ctx, unsafe {
                // TODO: should use a `read_unitialised` kind of method here.
                MaybeUninit::slice_get_mut(buffer.available_bytes())
            })
            .map_ok(|bytes_read| {
                // Safety: we just read into the buffer.
                unsafe {
                    buffer.read_bytes(bytes_read);
                }
                bytes_read
            })
    }
}

/// Split of from `Buffer` to allow the buffer to be used temporarily.
///
/// This allows `Buffer` to be used as both a read and write buffer.
#[derive(Debug)]
pub struct WriteBuffer<'a> {
    /// Buffer capacity from `Buffer`, `buf[..self.length]` bytes are valid
    /// (i.e. not undefined) and `buf[self.processed..self.length]` are yet to
    /// be written.
    buf: &'a mut [MaybeUninit<u8>],
    /// Number of bytes written into `buf`.
    length: usize,
    /// The number of bytes *we* already processed in `buf`.
    /// Must always be less then `self.length`.
    processed: usize,
}

impl<'a> WriteBuffer<'a> {
    /// Returns the unprocessed, written bytes.
    pub fn as_bytes(&self) -> &[u8] {
        // Safety: `self.buf[..self.length]` bytes are are valid (initialised)
        // as per the comment on the field.
        unsafe { MaybeUninit::slice_get_ref(&self.buf[self.processed..self.length]) }
    }

    /// Returns the number of unprocessed, written bytes.
    pub fn len(&self) -> usize {
        self.length - self.processed
    }

    /// Mark `n` bytes as processed.
    pub fn processed(&mut self, n: usize) {
        assert!(
            self.processed + n <= self.length,
            "marking bytes as processed beyond read range"
        );
        self.processed += n;
    }

    /// Number of bytes to which can be written.
    fn capacity_left(&self) -> usize {
        self.buf.len() - self.len()
    }
}

impl<'a> Write for WriteBuffer<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = buf.len();
        assert!(
            self.capacity_left() >= len,
            "trying to write a too large buffer"
        );
        // Safety: checked above if we have enough capacity left.
        unsafe {
            ptr::copy_nonoverlapping(
                buf.as_ptr(),
                MaybeUninit::first_ptr_mut(&mut self.buf[self.length..]),
                buf.len(),
            );
        }
        self.length += len;
        Ok(len)
    }

    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        bufs.iter().map(|buf| self.write(buf)).sum()
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.write(buf).map(|_| ())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
