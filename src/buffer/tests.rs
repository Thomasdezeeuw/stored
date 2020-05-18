use std::future::Future;
use std::io::Write;
use std::pin::Pin;
use std::task::{self, Poll};

use futures_util::io::Cursor;
use futures_util::task::noop_waker;

use super::{Buffer, INITIAL_BUF_SIZE, MIN_SIZE_MOVE};

/// Minimum size of a buffer passed to calls to read.
/// Note: no longer used in the implementation, but still in the tests below.
const MIN_BUF_SIZE: usize = 2 * 1024;

// TODO: more tests:
// Buffer -> ReadBuffer
// Buffer -> ReadBuffer -> WriteBuffer
// Buffer -> ReadBuffer -> ReadBuffer -> WriteBuffer.

const EMPTY: &[u8] = &[];

#[test]
fn buffer_simple_read() {
    let mut buf = Buffer::new();

    assert_eq!(buf.len(), 0);
    assert_eq!(buf.as_bytes(), EMPTY);
    assert_eq!(buf.capacity_left(), INITIAL_BUF_SIZE);

    let mut reader = Cursor::new([1, 2, 3]);
    let bytes_read = poll_wait(Pin::new(&mut buf.read_from(&mut reader))).unwrap();
    assert_eq!(bytes_read, 3);
    assert_eq!(buf.len(), 3);
    assert_eq!(buf.as_bytes(), &[1, 2, 3]);
    assert_eq!(buf.capacity_left(), INITIAL_BUF_SIZE - bytes_read);
}

#[test]
fn empty_buffer_reserve_atleast() {
    let mut buf = Buffer::new();

    // Shouldn't expand the buffer as it already has enough capacity.
    buf.reserve_atleast(2);
    assert_eq!(buf.len(), 0);
    assert_eq!(buf.as_bytes(), EMPTY);
    assert_eq!(buf.capacity_left(), INITIAL_BUF_SIZE);

    // This should grow the buffer.
    buf.reserve_atleast(2 * INITIAL_BUF_SIZE);
    assert_eq!(buf.len(), 0);
    assert_eq!(buf.as_bytes(), EMPTY);
    assert_eq!(buf.capacity_left(), 2 * INITIAL_BUF_SIZE);
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
    assert_eq!(buf.capacity_left(), 2 * INITIAL_BUF_SIZE);
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
    assert_eq!(buf.as_bytes(), EMPTY);
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
    assert_eq!(buf.as_bytes(), EMPTY);
}

#[test]
#[should_panic(expected = "marking bytes as processed beyond read range")]
#[ignore = "problem in libbacktrace, see https://github.com/rust-lang/rust/issues/71397"]
fn marking_processed_beyond_read_range() {
    let mut buf = Buffer::new();
    buf.processed(1);
}

#[test]
#[should_panic(expected = "marking bytes as processed beyond read range")]
#[ignore = "problem in libbacktrace, see https://github.com/rust-lang/rust/issues/71397"]
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
    assert_eq!(buf.as_bytes(), EMPTY);
    assert_eq!(buf.capacity_left(), INITIAL_BUF_SIZE);
}

#[test]
fn buffer_available_bytes() {
    let mut buf = Buffer::new();

    assert_eq!(buf.available_bytes().len(), INITIAL_BUF_SIZE);
    unsafe {
        buf.available_bytes()
            .as_mut_ptr()
            .write_bytes(0u8, INITIAL_BUF_SIZE)
    }

    let data1 = [1; INITIAL_BUF_SIZE - MIN_BUF_SIZE];
    let mut reader = Cursor::new(data1.as_ref());
    let bytes_read = poll_wait(Pin::new(&mut buf.read_from(&mut reader))).unwrap();
    assert_eq!(bytes_read, data1.len());
    assert_eq!(buf.as_bytes(), data1.as_ref());
    assert_eq!(buf.available_bytes().len(), MIN_BUF_SIZE);
    assert_eq!(buf.capacity_left(), MIN_BUF_SIZE);

    buf.processed(1);
    assert_eq!(buf.available_bytes().len(), MIN_BUF_SIZE);
    assert_eq!(buf.capacity_left(), MIN_BUF_SIZE);
}

#[test]
fn write_buffer_drops_writen_bytes() {
    let mut buf = Buffer::new();
    let bytes = &[1, 2, 3];
    add_bytes(&mut buf, bytes);
    assert_eq!(buf.len(), 3);
    assert_eq!(buf.as_bytes(), bytes);

    let (buf_bytes, mut wbuf) = buf.split_write(10);
    assert_eq!(buf_bytes, bytes);
    assert_eq!(wbuf.len(), 0);
    assert_eq!(wbuf.as_bytes(), EMPTY);

    let wbytes = &[4, 5, 6];
    wbuf.write_all(wbytes).unwrap();
    assert_eq!(wbuf.len(), 3);
    assert_eq!(wbuf.as_bytes(), wbytes);

    drop(wbuf);
    assert_eq!(buf.len(), 3);
    assert_eq!(buf.as_bytes(), bytes);
}

#[test]
fn write_buffer_drops_writen_bytes_original_empty() {
    let mut buf = Buffer::new();

    let (bytes, mut wbuf) = buf.split_write(10);
    assert_eq!(bytes, EMPTY);
    assert_eq!(wbuf.len(), 0);
    assert_eq!(wbuf.as_bytes(), EMPTY);

    let wbytes = &[4, 5, 6];
    wbuf.write_all(wbytes).unwrap();
    assert_eq!(wbuf.len(), 3);
    assert_eq!(wbuf.as_bytes(), wbytes);

    drop(wbuf);
    assert_eq!(buf.len(), 0);
    assert_eq!(buf.as_bytes(), EMPTY);
}

#[test]
fn write_buffer_length() {
    let mut buf = Buffer::new();
    let bytes: &[u8] = &[1; 200];
    let wbytes: &[u8] = &[2; 100];
    add_bytes(&mut buf, bytes);
    assert_eq!(buf.len(), bytes.len());
    assert_eq!(buf.as_bytes(), bytes);

    let (original_bytes, mut wbuf) = buf.split_write(wbytes.len());
    assert_eq!(original_bytes, bytes);
    assert_eq!(wbuf.len(), 0);
    assert_eq!(wbuf.as_bytes(), EMPTY);
    wbuf.write_all(wbytes).unwrap();
    assert_eq!(wbuf.len(), wbytes.len());
    assert_eq!(wbuf.as_bytes(), wbytes);
    drop(wbuf);

    buf.processed(100);
    let (original_bytes, mut wbuf) = buf.split_write(wbytes.len());
    assert_eq!(original_bytes, &bytes[100..]);
    assert_eq!(wbuf.len(), 0);
    assert_eq!(wbuf.as_bytes(), EMPTY);
    wbuf.write_all(wbytes).unwrap();
    assert_eq!(wbuf.len(), wbytes.len());
    assert_eq!(wbuf.as_bytes(), wbytes);
    drop(wbuf);

    buf.processed(100);
    let (original_bytes, mut wbuf) = buf.split_write(wbytes.len());
    assert_eq!(original_bytes, EMPTY);
    assert_eq!(wbuf.len(), 0);
    assert_eq!(wbuf.as_bytes(), EMPTY);
    wbuf.write_all(wbytes).unwrap();
    assert_eq!(wbuf.len(), wbytes.len());
    assert_eq!(wbuf.as_bytes(), wbytes);
    drop(wbuf);

    assert_eq!(buf.len(), 0);
}

#[test]
fn write_buffer_processed() {
    let mut buf = Buffer::new();
    let bytes = &[1, 2, 3];
    add_bytes(&mut buf, bytes);
    assert_eq!(buf.len(), 3);
    assert_eq!(buf.as_bytes(), bytes);

    let (original_bytes, mut wbuf) = buf.split_write(3);
    assert_eq!(original_bytes, bytes);
    assert_eq!(wbuf.len(), 0);
    assert_eq!(wbuf.as_bytes(), EMPTY);

    let wbytes = &[4, 5, 6];
    wbuf.write_all(wbytes).unwrap();
    assert_eq!(wbuf.len(), 3);
    assert_eq!(wbuf.as_bytes(), wbytes);

    wbuf.processed(1);
    assert_eq!(wbuf.len(), 2);
    assert_eq!(wbuf.as_bytes(), &wbytes[1..]);

    wbuf.processed(1);
    assert_eq!(wbuf.len(), 1);
    assert_eq!(wbuf.as_bytes(), &wbytes[2..]);

    assert_eq!(original_bytes, bytes);
    drop(wbuf);
    assert_eq!(buf.len(), 3);
    assert_eq!(buf.as_bytes(), bytes);
}

#[test]
fn write_buffer_processed_original_empty() {
    let mut buf = Buffer::new();

    let (original_bytes, mut wbuf) = buf.split_write(3);
    assert_eq!(original_bytes, EMPTY);
    assert_eq!(wbuf.len(), 0);
    assert_eq!(wbuf.as_bytes(), EMPTY);

    let wbytes = &[4, 5, 6];
    wbuf.write_all(wbytes).unwrap();
    assert_eq!(wbuf.len(), 3);
    assert_eq!(wbuf.as_bytes(), wbytes);

    wbuf.processed(1);
    assert_eq!(wbuf.len(), 2);
    assert_eq!(wbuf.as_bytes(), &wbytes[1..]);

    wbuf.processed(1);
    assert_eq!(wbuf.len(), 1);
    assert_eq!(wbuf.as_bytes(), &wbytes[2..]);

    assert_eq!(original_bytes, EMPTY);
    drop(wbuf);
    assert_eq!(buf.len(), 0);
    assert_eq!(buf.as_bytes(), EMPTY);
}

#[test]
#[should_panic(expected = "marking bytes as processed beyond read range")]
#[ignore = "problem in libbacktrace, see https://github.com/rust-lang/rust/issues/71397"]
fn marking_processed_write_buffer_beyond_read_range() {
    let mut buf = Buffer::new();
    let (original_bytes, mut wbuf) = buf.split_write(20);
    assert_eq!(original_bytes, EMPTY);
    wbuf.processed(1);
}

#[test]
#[should_panic(expected = "marking bytes as processed beyond read range")]
#[ignore = "problem in libbacktrace, see https://github.com/rust-lang/rust/issues/71397"]
fn marking_processed_write_buffer_beyond_read_range_after_reset() {
    let mut buf = Buffer::new();
    let bytes = &[1, 2, 3];
    add_bytes(&mut buf, bytes);

    let (original_bytes, mut wbuf) = buf.split_write(10);
    assert_eq!(original_bytes, bytes);
    wbuf.write_all(&[4, 5, 6]).unwrap();
    wbuf.processed(2); // Ok.

    wbuf.processed(2); // Should panic.
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

/// Add `bytes` to `buf`fer.
fn add_bytes(buf: &mut Buffer, bytes: &[u8]) {
    let mut reader = Cursor::new(bytes);
    let bytes_read = poll_wait(Pin::new(&mut buf.read_from(&mut reader))).unwrap();
    assert_eq!(bytes_read, bytes.len());
    assert!(buf.len() >= bytes.len());
    assert_eq!(&buf.as_bytes()[buf.len() - bytes.len()..], bytes);
}
