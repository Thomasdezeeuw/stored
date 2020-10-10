use std::cmp::min;
use std::io::Write;
use std::mem::MaybeUninit;
use std::ptr;

use heph::net::Bytes;

use super::{Buffer, INITIAL_BUF_SIZE, MIN_SIZE_MOVE};

/// Minimum size of a buffer passed to calls to read.
/// Note: no longer used in the implementation, but still in the tests below.
const MIN_BUF_SIZE: usize = 2 * 1024;

const EMPTY: &[u8] = &[];

#[test]
fn buffer_bytes() {
    let mut buf = Buffer::new();

    assert_eq!(buf.len(), 0);
    assert_eq!(buf.as_slice(), EMPTY);
    assert_eq!(buf.next_byte(), None);
    assert_eq!(buf.capacity_left(), INITIAL_BUF_SIZE);

    let n = add_bytes(&mut buf, &[1, 2, 3]);
    assert_eq!(n, 3);
    assert_eq!(buf.len(), 3);
    assert_eq!(buf.as_slice(), &[1, 2, 3]);
    assert_eq!(buf.next_byte(), Some(1));
    assert_eq!(buf.capacity_left(), INITIAL_BUF_SIZE - n);
}

#[test]
fn buffer_copy_to() {
    let mut buf = Buffer::new();
    let bytes = &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    buf.data.extend(bytes);
    assert_eq!(buf.as_slice(), bytes);

    // dst < buf.
    let mut dst = [MaybeUninit::uninit(); 2];
    assert_eq!(buf.copy_to(&mut dst), 2);
    assert_eq!(
        unsafe { MaybeUninit::slice_assume_init_ref(&dst) },
        &bytes[..2]
    );
    // Empty dst.
    let mut dst = [0; 0];
    assert_eq!(buf.copy_to(&mut dst), 0);

    // dst > buf.
    let mut dst = [MaybeUninit::uninit(); 10];
    assert_eq!(buf.copy_to(&mut dst), 8);
    assert_eq!(
        unsafe { MaybeUninit::slice_assume_init_ref(&dst[..8]) },
        &bytes[2..]
    );
}

#[test]
fn buf_view() {
    let buf = Buffer::new();
    let view = buf.view(0);

    assert_eq!(view.len(), 0);
    assert_eq!(view.as_slice(), EMPTY);

    let mut buf = view.processed();
    let n = add_bytes(&mut buf, &[1, 2, 3, 4, 5]);
    assert_eq!(n, 5);
    buf.processed(1);

    let view = buf.view(2);
    assert_eq!(view.len(), 2);
    assert_eq!(view.as_slice(), &[2, 3]);

    let buf = view.processed();
    assert_eq!(buf.len(), 2);
    assert!(!buf.is_empty());
    assert_eq!(buf.as_slice(), &[4, 5]);

    let view = buf.view(2);
    let buf = view.processed();
    assert_eq!(buf.len(), 0);
    assert!(buf.is_empty());
    assert_eq!(buf.as_slice(), EMPTY);
}

#[test]
fn empty_buffer_reserve_atleast() {
    let mut buf = Buffer::new();

    // Shouldn't expand the buffer as it already has enough capacity.
    buf.reserve_atleast(2);
    assert_eq!(buf.len(), 0);
    assert_eq!(buf.as_slice(), EMPTY);
    assert_eq!(buf.capacity_left(), INITIAL_BUF_SIZE);

    // This should grow the buffer.
    buf.reserve_atleast(2 * INITIAL_BUF_SIZE);
    assert_eq!(buf.len(), 0);
    assert_eq!(buf.as_slice(), EMPTY);
    assert_eq!(buf.capacity_left(), 2 * INITIAL_BUF_SIZE);
}

#[test]
fn buffer_reserve_atleast() {
    let mut buf = Buffer::new();

    let n = add_bytes(&mut buf, &[1, 2, 3]);
    assert_eq!(n, 3);
    assert_eq!(buf.len(), 3);
    assert_eq!(buf.as_slice(), &[1, 2, 3]);
    assert_eq!(buf.capacity_left(), INITIAL_BUF_SIZE - n);

    // Shouldn't expand the buffer as it already has enough capacity.
    buf.reserve_atleast(2);
    assert_eq!(buf.len(), 3);
    assert_eq!(buf.as_slice(), &[1, 2, 3]);
    assert_eq!(buf.capacity_left(), INITIAL_BUF_SIZE - n);

    // This should grow the buffer.
    buf.reserve_atleast(2 * INITIAL_BUF_SIZE);
    assert_eq!(buf.len(), 3);
    assert_eq!(buf.as_slice(), &[1, 2, 3]);
    assert_eq!(buf.capacity_left(), 2 * INITIAL_BUF_SIZE);
}

#[test]
fn buffer_processed() {
    let mut buf = Buffer::new();

    let n = add_bytes(&mut buf, &[1, 2, 3]);
    assert_eq!(n, 3);
    assert_eq!(buf.len(), 3);
    assert_eq!(buf.as_slice(), &[1, 2, 3]);
    assert_eq!(buf.capacity_left(), INITIAL_BUF_SIZE - n);

    buf.processed(1);
    assert_eq!(buf.len(), 2);
    assert_eq!(buf.as_slice(), &[2, 3]);
    assert_eq!(buf.capacity_left(), INITIAL_BUF_SIZE - n);

    buf.processed(2);
    assert_eq!(buf.len(), 0);
    assert_eq!(buf.as_slice(), EMPTY);
    assert_eq!(buf.capacity_left(), INITIAL_BUF_SIZE - n);
}

#[test]
fn buffer_reset() {
    let mut buf = Buffer::new();

    let n = add_bytes(&mut buf, &[1, 2, 3]);
    assert_eq!(n, 3);
    assert_eq!(buf.as_slice(), &[1, 2, 3]);

    buf.reset();
    assert_eq!(buf.len(), 0);
    assert_eq!(buf.as_slice(), EMPTY);
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

    let n = add_bytes(&mut buf, &[1, 2, 3]);
    assert_eq!(n, 3);

    buf.reset();
    buf.processed(1);
}

#[test]
fn buffer_move_to_start() {
    let mut buf = Buffer::new();

    let data = [1; INITIAL_BUF_SIZE - 1];
    let n = add_bytes(&mut buf, &data);
    assert_eq!(n, data.len());
    assert_eq!(buf.len(), data.len());
    assert_eq!(buf.as_slice(), data.as_ref());
    assert_eq!(buf.capacity_left(), 1);

    // Should do nothing.
    buf.move_to_start(false);
    assert_eq!(buf.len(), data.len());
    assert_eq!(buf.as_slice(), data.as_ref());
    assert_eq!(buf.capacity_left(), 1);

    buf.processed(MIN_SIZE_MOVE - 1);
    // Should again do nothing as a move would not be worth it.
    buf.move_to_start(false);
    assert_eq!(buf.as_slice(), &data[MIN_SIZE_MOVE - 1..]);
    assert_eq!(buf.capacity_left(), 1);

    buf.processed(1);
    // Finally the data should be moved to the start of the buffer.
    buf.move_to_start(false);
    assert_eq!(buf.as_slice(), &data[MIN_SIZE_MOVE..]);
    assert_eq!(buf.capacity_left(), MIN_SIZE_MOVE + 1);

    buf.processed(buf.as_slice().len());
    buf.move_to_start(false);
    assert_eq!(buf.as_slice(), EMPTY);
    assert_eq!(buf.capacity_left(), INITIAL_BUF_SIZE);
}

#[test]
fn buffer_available_bytes() {
    let mut buf = Buffer::new();

    assert_eq!(buf.as_bytes().len(), INITIAL_BUF_SIZE);
    unsafe {
        buf.as_bytes()
            .as_mut_ptr()
            .write_bytes(0u8, INITIAL_BUF_SIZE)
    }

    let data1 = [1; INITIAL_BUF_SIZE - MIN_BUF_SIZE];
    let n = add_bytes(&mut buf, &data1);
    assert_eq!(n, data1.len());
    assert_eq!(buf.as_slice(), data1.as_ref());
    assert_eq!(buf.as_bytes().len(), MIN_BUF_SIZE);
    assert_eq!(buf.capacity_left(), MIN_BUF_SIZE);

    buf.processed(1);
    assert_eq!(buf.as_bytes().len(), MIN_BUF_SIZE);
    assert_eq!(buf.capacity_left(), MIN_BUF_SIZE);
}

#[test]
fn write_buffer_drops_writen_bytes() {
    let mut buf = Buffer::new();
    let bytes = &[1, 2, 3];
    add_bytes(&mut buf, bytes);
    assert_eq!(buf.as_slice(), bytes);

    let (buf_bytes, mut wbuf) = buf.split_write(10);
    assert_eq!(buf_bytes, bytes);
    assert_eq!(wbuf.as_slice(), EMPTY);

    let wbytes = &[4, 5, 6];
    wbuf.write_all(wbytes).unwrap();
    assert_eq!(wbuf.as_slice(), wbytes);

    drop(wbuf);
    assert_eq!(buf.len(), 3);
    assert_eq!(buf.as_slice(), bytes);
}

#[test]
fn write_buffer_drops_writen_bytes_original_empty() {
    let mut buf = Buffer::new();

    let (bytes, mut wbuf) = buf.split_write(10);
    assert_eq!(bytes, EMPTY);
    assert_eq!(wbuf.as_slice(), EMPTY);

    let wbytes = &[4, 5, 6];
    wbuf.write_all(wbytes).unwrap();
    assert_eq!(wbuf.as_slice(), wbytes);

    drop(wbuf);
    assert_eq!(buf.len(), 0);
    assert_eq!(buf.as_slice(), EMPTY);
}

#[test]
fn write_buffer_length() {
    let mut buf = Buffer::new();
    let bytes: &[u8] = &[1; 200];
    let wbytes: &[u8] = &[2; 100];
    add_bytes(&mut buf, bytes);
    assert_eq!(buf.len(), bytes.len());
    assert_eq!(buf.as_slice(), bytes);

    let (original_bytes, mut wbuf) = buf.split_write(wbytes.len());
    assert_eq!(original_bytes, bytes);
    assert_eq!(wbuf.as_slice(), EMPTY);
    assert_eq!(wbuf.as_mut_bytes(), EMPTY);
    wbuf.write_all(wbytes).unwrap();
    assert_eq!(wbuf.as_slice(), wbytes);
    assert_eq!(wbuf.as_mut_bytes(), wbytes);
    drop(wbuf);

    buf.processed(100);
    let (original_bytes, mut wbuf) = buf.split_write(wbytes.len());
    assert_eq!(original_bytes, &bytes[100..]);
    assert_eq!(wbuf.as_slice(), EMPTY);
    assert_eq!(wbuf.as_mut_bytes(), EMPTY);
    wbuf.write_all(wbytes).unwrap();
    assert_eq!(wbuf.as_slice(), wbytes);
    assert_eq!(wbuf.as_mut_bytes(), wbytes);
    drop(wbuf);

    buf.processed(100);
    let (original_bytes, mut wbuf) = buf.split_write(wbytes.len());
    assert_eq!(original_bytes, EMPTY);
    assert_eq!(wbuf.as_slice(), EMPTY);
    assert_eq!(wbuf.as_mut_bytes(), EMPTY);
    wbuf.write_all(wbytes).unwrap();
    assert_eq!(wbuf.as_slice(), wbytes);
    assert_eq!(wbuf.as_mut_bytes(), wbytes);
    drop(wbuf);

    assert_eq!(buf.len(), 0);
}

/// Add `bytes` to `buf`fer, return the number of bytes copied.
fn add_bytes(buf: &mut Buffer, bytes: &[u8]) -> usize {
    let dst = buf.as_bytes();
    let len = min(bytes.len(), dst.len());
    // Safety: both the src and dst pointers are good. And we've ensured
    // that the length is correct, not overwriting data we don't own or
    // reading data we don't own.
    unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), dst.as_mut_ptr().cast(), len) }
    // Safety: just copied the bytes above.
    unsafe { buf.update_length(len) }
    len
}
