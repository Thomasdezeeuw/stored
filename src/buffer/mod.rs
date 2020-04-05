//! Module with the `Buffer` type.

use std::future::Future;
use std::io::{self, IoSlice, Write};
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::slice;
use std::task::{self, Poll};

use futures_io::AsyncRead;

#[cfg(test)]
mod tests;

/// Size used as initial buffer size.
const INITIAL_BUF_SIZE: usize = 8 * 1024;

/// Minimum size of a buffer passed to calls to read.
const MIN_BUF_SIZE: usize = 2 * 1024;

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

    /// Get a temporary write buffer.
    ///
    /// Operations on the returned `WriteBuffer` will be removed from the
    /// original `Buffer` (`self`) once the working buffer is dropped. This
    /// allows `Buffer` to be reused for writing while normally using it for
    /// reading.
    pub fn write_buf<'b>(&'b mut self) -> WriteBuffer<'b> {
        let original_length = self.data.len();
        WriteBuffer {
            buf: self,
            original_length,
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

    /// Read from `reader` into this buffer.
    pub fn read_from<R>(&mut self, reader: R) -> Read<R>
    where
        R: AsyncRead,
    {
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
        self.processed = 0;
        unsafe { self.data.set_len(0) }
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
    ///
    /// This ensures that the slice has a length of at least `MIN_BUF_SIZE`.
    fn available_bytes(&mut self) -> &mut [MaybeUninit<u8>] {
        // Ensure we have some space to read into.
        if self.capacity_left() < MIN_BUF_SIZE {
            self.move_to_start(false);

            // If our buffer is filled with unhandled data we need to allocate
            // some more.
            if self.capacity_left() < MIN_BUF_SIZE {
                // FIXME: don't want to allocate infinite buffer space here, we
                // need to limit it somehow.
                // TODO: be smarter about moving bytes in the buffer and
                // reallocating together.
                self.data.reserve(MIN_BUF_SIZE);
            }
        }

        #[allow(unused_unsafe)]
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
                // Safe because we just read into the buffer.
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
    buf: &'a mut Buffer,
    /// Original length of `buf`, to which we need to reset when dropped.
    original_length: usize,
    /// The number of bytes *we* already processed in `buf.data`. Also note
    /// `buf.processed`!
    processed: usize,
}

impl<'a> WriteBuffer<'a> {
    /// Reserves an allocation that can read a buffer of at least `length`
    /// bytes, including the unprocessed bytes already in the buffer.
    pub fn reserve_atleast(&mut self, length: usize) {
        if length > self.buf.capacity_left() {
            // Need more capacity, try removing the already processed bytes
            // first.
            self.original_length -= self.buf.processed;
            self.buf.move_to_start(true);
            if length > self.buf.capacity_left() {
                // Still need more.
                self.buf.data.reserve(length);
            }
        }
    }

    /// Returns the number of unprocessed, read bytes.
    pub fn len(&self) -> usize {
        self.buf.data.len() - self.original_length - self.processed
    }

    /// Returns the unprocessed, read bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf.data[self.original_length + self.processed..]
    }

    /// Mark `n` bytes as processed.
    pub fn processed(&mut self, n: usize) {
        assert!(
            self.original_length + self.processed + n <= self.buf.data.len(),
            "marking bytes as processed beyond read range"
        );
        self.processed += n;
    }
}

impl<'a> Write for WriteBuffer<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buf.data.write(buf)
    }

    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        self.buf.data.write_vectored(bufs)
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.buf.data.write_all(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.buf.data.flush()
    }
}

impl<'a> Drop for WriteBuffer<'a> {
    fn drop(&mut self) {
        // Drop all the `WriteBuffer`'s bytes.
        self.buf.data.truncate(self.original_length);
    }
}
