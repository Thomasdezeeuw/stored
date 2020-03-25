//! Module with the `Buffer` type.

use std::future::Future;
use std::pin::Pin;
use std::task::{self, Poll};
use std::{io, slice};

use futures_io::AsyncRead;

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

    /// Reserves an allocation that can read a buffer of at least `length`
    /// bytes, including the unprocessed bytes already in the buffer.
    pub fn reserve_atleast(&mut self, length: usize) {
        self.move_to_start(true);
        if length > self.data.capacity() {
            self.data.reserve(length - self.data.capacity());
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
    ///
    /// # Unsafety
    ///
    /// The contents of the returned bytes is undefined, as such it's only valid
    /// to write into, **not** read from.
    unsafe fn available_bytes(&mut self) -> &mut [u8] {
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
            let data_ptr = self.data.as_mut_ptr().add(self.data.len());
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
            .poll_read(ctx, unsafe { buffer.available_bytes() })
            .map_ok(|bytes_read| {
                // Safe because we just read into the buffer.
                unsafe {
                    buffer.read_bytes(bytes_read);
                }
                bytes_read
            })
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{self, Poll};

    use futures_util::io::Cursor;
    use futures_util::task::noop_waker;

    use super::{Buffer, INITIAL_BUF_SIZE, MIN_BUF_SIZE, MIN_SIZE_MOVE};

    #[test]
    fn buffer_simple_read() {
        let mut buf = Buffer::new();

        assert_eq!(buf.len(), 0);
        assert_eq!(buf.as_bytes(), &[]);
        assert_eq!(buf.capacity_left(), INITIAL_BUF_SIZE);

        let mut reader = Cursor::new([1, 2, 3]);
        let bytes_read = poll_wait(Pin::new(&mut buf.read_from(&mut reader))).unwrap();
        assert_eq!(bytes_read, 3);
        assert_eq!(buf.len(), 3);
        assert_eq!(buf.as_bytes(), &[1, 2, 3]);
        assert_eq!(buf.capacity_left(), INITIAL_BUF_SIZE - bytes_read);
    }

    #[test]
    fn buffer_reserve_atleast() {
        let mut buf = Buffer::new();

        let mut reader = Cursor::new([1, 2, 3]);
        let bytes_read = poll_wait(Pin::new(&mut buf.read_from(&mut reader))).unwrap();
        assert_eq!(bytes_read, 3);
        assert_eq!(buf.len(), 3);
        assert_eq!(buf.as_bytes(), &[1, 2, 3]);
        assert_eq!(buf.capacity_left(), INITIAL_BUF_SIZE - bytes_read);

        // Shouldn't expand the buffer as it already has enough capacity.
        buf.reserve_atleast(2);
        assert_eq!(buf.len(), 3);
        assert_eq!(buf.as_bytes(), &[1, 2, 3]);
        assert_eq!(buf.capacity_left(), INITIAL_BUF_SIZE - bytes_read);

        // This should grow the buffer.
        buf.reserve_atleast(2 * INITIAL_BUF_SIZE);
        assert_eq!(buf.len(), 3);
        assert_eq!(buf.as_bytes(), &[1, 2, 3]);
        assert_eq!(buf.capacity_left(), 2 * INITIAL_BUF_SIZE - bytes_read);
    }

    #[test]
    fn buffer_processed() {
        let mut buf = Buffer::new();

        let mut reader = Cursor::new([1, 2, 3]);
        let bytes_read = poll_wait(Pin::new(&mut buf.read_from(&mut reader))).unwrap();
        assert_eq!(bytes_read, 3);
        assert_eq!(buf.len(), 3);
        assert_eq!(buf.as_bytes(), &[1, 2, 3]);
        assert_eq!(buf.capacity_left(), INITIAL_BUF_SIZE - bytes_read);

        buf.processed(1);
        assert_eq!(buf.len(), 2);
        assert_eq!(buf.as_bytes(), &[2, 3]);
        assert_eq!(buf.capacity_left(), INITIAL_BUF_SIZE - bytes_read);

        buf.processed(2);
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.as_bytes(), &[]);
        assert_eq!(buf.capacity_left(), INITIAL_BUF_SIZE - bytes_read);
    }

    #[test]
    fn buffer_reset() {
        let mut buf = Buffer::new();

        let mut reader = Cursor::new([1, 2, 3]);
        let bytes_read = poll_wait(Pin::new(&mut buf.read_from(&mut reader))).unwrap();
        assert_eq!(bytes_read, 3);
        assert_eq!(buf.as_bytes(), &[1, 2, 3]);

        buf.reset();
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.as_bytes(), &[]);
    }

    #[test]
    #[should_panic(expected = "marking bytes as processed beyond read range")]
    fn marking_processed_beyond_read_range() {
        let mut buf = Buffer::new();
        buf.processed(1);
    }

    #[test]
    #[should_panic(expected = "marking bytes as processed beyond read range")]
    fn marking_processed_beyond_read_range_after_reset() {
        let mut buf = Buffer::new();

        let mut reader = Cursor::new([1, 2, 3]);
        let bytes_read = poll_wait(Pin::new(&mut buf.read_from(&mut reader))).unwrap();
        assert_eq!(bytes_read, 3);

        buf.reset();
        buf.processed(1);
    }

    #[test]
    fn buffer_move_to_start() {
        let mut buf = Buffer::new();

        let data = [1; INITIAL_BUF_SIZE - 1];
        let mut reader = Cursor::new(data.as_ref());
        let bytes_read = poll_wait(Pin::new(&mut buf.read_from(&mut reader))).unwrap();
        assert_eq!(bytes_read, data.len());
        assert_eq!(buf.len(), data.len());
        assert_eq!(buf.as_bytes(), data.as_ref());
        assert_eq!(buf.capacity_left(), 1);

        // Should do nothing.
        buf.move_to_start(false);
        assert_eq!(buf.len(), data.len());
        assert_eq!(buf.as_bytes(), data.as_ref());
        assert_eq!(buf.capacity_left(), 1);

        buf.processed(MIN_SIZE_MOVE - 1);
        // Should again do nothing as a move would not be worth it.
        buf.move_to_start(false);
        assert_eq!(buf.as_bytes(), &data[MIN_SIZE_MOVE - 1..]);
        assert_eq!(buf.capacity_left(), 1);

        buf.processed(1);
        // Finally the data should be moved to the start of the buffer.
        buf.move_to_start(false);
        assert_eq!(buf.as_bytes(), &data[MIN_SIZE_MOVE..]);
        assert_eq!(buf.capacity_left(), MIN_SIZE_MOVE + 1);

        buf.processed(buf.as_bytes().len());
        buf.move_to_start(false);
        assert_eq!(buf.as_bytes(), &[]);
        assert_eq!(buf.capacity_left(), INITIAL_BUF_SIZE);
    }

    #[test]
    fn buffer_available_bytes() {
        let mut buf = Buffer::new();

        assert_eq!(unsafe { buf.available_bytes().len() }, INITIAL_BUF_SIZE);
        let zero = [0; INITIAL_BUF_SIZE];
        unsafe { buf.available_bytes() }.copy_from_slice(&zero);

        let data1 = [1; INITIAL_BUF_SIZE - MIN_BUF_SIZE];
        let mut reader = Cursor::new(data1.as_ref());
        let bytes_read = poll_wait(Pin::new(&mut buf.read_from(&mut reader))).unwrap();
        assert_eq!(bytes_read, data1.len());
        assert_eq!(buf.as_bytes(), data1.as_ref());
        assert_eq!(buf.capacity_left(), MIN_BUF_SIZE);

        // No need to move the buffer yet.
        assert_eq!(unsafe { buf.available_bytes().len() }, MIN_BUF_SIZE);
        assert_eq!(buf.capacity_left(), MIN_BUF_SIZE);

        // Marking some data as processed so the data can be moved by
        // `available_bytes`.
        buf.processed(MIN_BUF_SIZE);

        let data2 = [2; 2 * MIN_BUF_SIZE];
        let mut reader = Cursor::new(data2.as_ref());
        let bytes_read = poll_wait(Pin::new(&mut buf.read_from(&mut reader))).unwrap();
        assert_eq!(bytes_read, MIN_BUF_SIZE); // Partial write.
                                              // Buffer should hold the old and new data.
        let old_data_length = INITIAL_BUF_SIZE - 2 * MIN_BUF_SIZE;
        assert_eq!(buf.as_bytes()[..old_data_length], data1[MIN_BUF_SIZE..]);
        assert_eq!(buf.as_bytes()[old_data_length..], data2[..MIN_BUF_SIZE]);
        assert_eq!(buf.capacity_left(), 0);

        // Now the data should be moved to the start of the buffer.
        let data3 = [3; 2 * MIN_BUF_SIZE];
        let mut reader = Cursor::new(data3.as_ref());
        let bytes_read = poll_wait(Pin::new(&mut buf.read_from(&mut reader))).unwrap();
        assert_eq!(bytes_read, MIN_BUF_SIZE); // Partial write.
                                              // Buffer should hold the old and new data.
        let old_data_length = INITIAL_BUF_SIZE - 2 * MIN_BUF_SIZE;
        assert_eq!(buf.as_bytes()[..old_data_length], data1[MIN_BUF_SIZE..]);
        assert_eq!(
            buf.as_bytes()[old_data_length..old_data_length + MIN_BUF_SIZE],
            data2[..MIN_BUF_SIZE]
        );
        assert_eq!(
            buf.as_bytes()[old_data_length + MIN_BUF_SIZE..],
            data3[..MIN_BUF_SIZE]
        );
        assert_eq!(buf.capacity_left(), 0);

        // Ensure no additional allocation.
        assert_eq!(buf.data.capacity(), INITIAL_BUF_SIZE);
        assert_eq!(buf.processed, 0);

        // Now we have no capacity left and all bytes are unprocessed, so we
        // need to reallocate.
        let data4 = [4; MIN_BUF_SIZE];
        let mut reader = Cursor::new(data4.as_ref());
        let bytes_read = poll_wait(Pin::new(&mut buf.read_from(&mut reader))).unwrap();
        assert_eq!(bytes_read, data4.len());
        // Buffer should hold the old and new data.
        let old_data_length = INITIAL_BUF_SIZE - 2 * MIN_BUF_SIZE;
        assert_eq!(buf.as_bytes()[..old_data_length], data1[MIN_BUF_SIZE..]);
        assert_eq!(
            buf.as_bytes()[old_data_length..old_data_length + MIN_BUF_SIZE],
            data2[..MIN_BUF_SIZE]
        );
        assert_eq!(
            buf.as_bytes()[old_data_length + MIN_BUF_SIZE..old_data_length + 2 * MIN_BUF_SIZE],
            data3[..MIN_BUF_SIZE]
        );
        assert_eq!(
            buf.as_bytes()[old_data_length + 2 * MIN_BUF_SIZE..],
            data4[..MIN_BUF_SIZE]
        );
    }

    fn poll_wait<Fut>(mut future: Pin<&mut Fut>) -> Fut::Output
    where
        Fut: Future,
    {
        // This is not great.
        let waker = noop_waker();
        let mut ctx = task::Context::from_waker(&waker);
        loop {
            match future.as_mut().poll(&mut ctx) {
                Poll::Ready(result) => return result,
                Poll::Pending => continue,
            }
        }
    }
}
