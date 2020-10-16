//! Tests for the [`peer::server::actor`].

// TODO: test the following requests.
// * Request keys.
// * Request keys since.

use std::convert::TryInto;
use std::io::{IoSlice, Read, Write};
use std::net::{Ipv4Addr, Shutdown, SocketAddr, TcpStream};
use std::time::{Duration, SystemTime};

use log::LevelFilter;
use stored::peer::server::{
    BLOB_LENGTH_LEN, DATE_TIME_LEN, METADATA_LEN, NO_BLOB, REQUEST_BLOB, STORE_BLOB,
};
use stored::peer::switcher::MAGIC_ERROR_MSG;
use stored::peer::{COORDINATOR_MAGIC, MAGIC_LENGTH};
use stored::storage::DateTime;
use stored::Key;

const PEER_PORT: u16 = 14100;
const DB_PATH: &str = "/tmp/stored/peer_server.db";
const CONF_PATH: &str = "tests/config/peer_server.toml";
const FILTER: LevelFilter = LevelFilter::Error;

start_stored_fn!(&[CONF_PATH], &[DB_PATH], FILTER);

#[test]
fn invalid_magic() {
    let _p = start_stored();

    let mut stream = TcpStream::connect(peer_address()).unwrap();

    let buf = &[123; MAGIC_LENGTH];
    stream.write_all(buf).unwrap();

    let mut buf = [0; MAGIC_ERROR_MSG.len() + 1];
    let n = stream.read(&mut buf).unwrap();
    assert_eq!(n, MAGIC_ERROR_MSG.len());
    assert_eq!(&buf[..n], MAGIC_ERROR_MSG);
}

#[test]
fn retrieve_not_stored_hello_world() {
    let _p = start_stored();

    let blob = b"Hello world";
    let key = Key::for_blob(blob);

    let mut stream = TcpStream::connect(peer_address()).unwrap();
    stream.write_all(COORDINATOR_MAGIC).unwrap();

    request_blob(&mut stream, &key);
    expect_blob(&mut stream, &[], DateTime::INVALID);

    expect_shutdown(stream);
}

#[test]
fn store_and_retrieve_hello_mars() {
    let _p = start_stored();

    const BLOB: &[u8] = b"Hello mars";
    let key = Key::for_blob(BLOB);
    let timestamp = DateTime::from(SystemTime::now());

    let mut stream = TcpStream::connect(peer_address()).unwrap();
    stream.write_all(COORDINATOR_MAGIC).unwrap();

    store_blob(&mut stream, BLOB, timestamp);
    expect_ok(&mut stream);

    request_blob(&mut stream, &key);
    expect_blob(&mut stream, BLOB, timestamp);

    expect_shutdown(stream);
}

#[test]
fn store_and_retrieve_large_blob() {
    let _p = start_stored();

    let blob = vec![255; 10 * 1024 * 1024];
    let key = Key::for_blob(&blob);
    let timestamp = DateTime::from(SystemTime::now());

    let mut stream = TcpStream::connect(peer_address()).unwrap();

    stream.write_all(COORDINATOR_MAGIC).unwrap();

    store_blob(&mut stream, &blob, timestamp);
    expect_ok(&mut stream);

    request_blob(&mut stream, &key);
    expect_blob(&mut stream, &blob, timestamp);

    expect_shutdown(stream);
}

#[test]
fn store_and_retrieve_removed_hello_pluto() {
    let _p = start_stored();

    const BLOB: &[u8] = b"Hello pluto";
    let key = Key::for_blob(BLOB);
    let timestamp = DateTime::from(SystemTime::now()).mark_removed();

    let mut stream = TcpStream::connect(peer_address()).unwrap();
    stream.write_all(COORDINATOR_MAGIC).unwrap();

    store_blob(&mut stream, BLOB, timestamp);
    expect_ok(&mut stream);

    request_blob(&mut stream, &key);
    expect_blob(&mut stream, &[], timestamp);

    expect_shutdown(stream);
}

#[test]
fn store_and_retrieve_removed_blob_no_length() {
    let _p = start_stored();

    const BLOB: &[u8] = b"Hello pluto no length";
    let key = Key::for_blob(BLOB);
    let timestamp = DateTime::from(SystemTime::now()).mark_removed();

    let mut stream = TcpStream::connect(peer_address()).unwrap();

    stream.write_all(COORDINATOR_MAGIC).unwrap();

    let bufs = &mut [
        IoSlice::new(&[STORE_BLOB]),
        IoSlice::new(key.as_bytes()),
        IoSlice::new(timestamp.as_bytes()),
        IoSlice::new(&NO_BLOB),
    ];
    stream.write_all_vectored(bufs).unwrap();
    expect_ok(&mut stream);

    request_blob(&mut stream, &key);
    expect_blob(&mut stream, &[], timestamp);

    expect_shutdown(stream);
}

#[test]
#[ignore]
fn retrieve_uncommitted_blob() {
    todo!();
}

#[test]
fn store_with_invalid_timestamp() {
    let _p = start_stored();

    const BLOB: &[u8] = b"Hello sun";
    let timestamp = DateTime::INVALID;

    let mut stream = TcpStream::connect(peer_address()).unwrap();
    stream.write_all(COORDINATOR_MAGIC).unwrap();

    store_blob(&mut stream, BLOB, timestamp);
    expect_err(&mut stream, b"invalid timestamp");

    expect_shutdown(stream);
}

#[test]
fn store_with_invalid_timestamp_no_length() {
    let _p = start_stored();

    const BLOB: &[u8] = b"Hello sun";
    let key = Key::for_blob(BLOB);
    let timestamp = DateTime::INVALID;

    let mut stream = TcpStream::connect(peer_address()).unwrap();

    stream.write_all(COORDINATOR_MAGIC).unwrap();

    let bufs = &mut [
        IoSlice::new(&[STORE_BLOB]),
        IoSlice::new(key.as_bytes()),
        IoSlice::new(timestamp.as_bytes()),
        IoSlice::new(&NO_BLOB),
    ];
    stream.write_all_vectored(bufs).unwrap();
    expect_err(&mut stream, b"invalid timestamp");

    expect_shutdown(stream);
}

#[test]
fn store_already_stored_timestamp_before() {
    let _p = start_stored();

    const BLOB: &[u8] = b"already stored before";
    let key = Key::for_blob(BLOB);
    let timestamp = DateTime::from(SystemTime::now());

    let mut stream = TcpStream::connect(peer_address()).unwrap();

    stream.write_all(COORDINATOR_MAGIC).unwrap();

    store_blob(&mut stream, BLOB, timestamp);
    expect_ok(&mut stream);

    let new_timestamp = DateTime::from(SystemTime::now() - Duration::from_secs(500));
    store_blob(&mut stream, BLOB, new_timestamp);
    expect_ok(&mut stream);

    // Timestamp should not be updated.
    request_blob(&mut stream, &key);
    expect_blob(&mut stream, BLOB, timestamp);

    expect_shutdown(stream);
}

#[test]
fn store_already_stored_timestamp_after() {
    let _p = start_stored();

    const BLOB: &[u8] = b"already stored after";
    let key = Key::for_blob(BLOB);
    let timestamp = DateTime::from(SystemTime::now());

    let mut stream = TcpStream::connect(peer_address()).unwrap();

    stream.write_all(COORDINATOR_MAGIC).unwrap();

    store_blob(&mut stream, BLOB, timestamp);
    expect_ok(&mut stream);

    let new_timestamp = DateTime::from(SystemTime::now() + Duration::from_secs(10));
    store_blob(&mut stream, BLOB, new_timestamp);
    expect_ok(&mut stream);

    // Timestamp should not be updated.
    request_blob(&mut stream, &key);
    expect_blob(&mut stream, BLOB, timestamp);

    expect_shutdown(stream);
}

#[test]
fn store_already_stored_removed_timestamp_before() {
    let _p = start_stored();

    const BLOB: &[u8] = b"already removed blob before";
    let key = Key::for_blob(BLOB);
    let timestamp = DateTime::from(SystemTime::now()).mark_removed();

    let mut stream = TcpStream::connect(peer_address()).unwrap();
    stream.write_all(COORDINATOR_MAGIC).unwrap();

    store_blob(&mut stream, BLOB, timestamp);
    expect_ok(&mut stream);

    let new_timestamp = DateTime::from(SystemTime::now() - Duration::from_secs(100));
    store_blob(&mut stream, BLOB, new_timestamp);
    expect_ok(&mut stream);

    // Timestamp should be updated.
    request_blob(&mut stream, &key);
    expect_blob(&mut stream, BLOB, new_timestamp);

    expect_shutdown(stream);
}

#[test]
fn store_already_stored_removed_timestamp_after() {
    let _p = start_stored();

    const BLOB: &[u8] = b"already removed blob after";
    let key = Key::for_blob(BLOB);
    let timestamp = DateTime::from(SystemTime::now()).mark_removed();

    let mut stream = TcpStream::connect(peer_address()).unwrap();
    stream.write_all(COORDINATOR_MAGIC).unwrap();

    store_blob(&mut stream, BLOB, timestamp);
    expect_ok(&mut stream);

    let new_timestamp = DateTime::from(SystemTime::now() - Duration::from_secs(500));
    store_blob(&mut stream, BLOB, new_timestamp);
    expect_ok(&mut stream);

    // Timestamp should be updated.
    request_blob(&mut stream, &key);
    expect_blob(&mut stream, BLOB, new_timestamp);

    expect_shutdown(stream);
}

#[test]
fn store_removed_already_stored_after() {
    let _p = start_stored();

    const BLOB: &[u8] = b"remove already stored blob after";
    let key = Key::for_blob(BLOB);
    let timestamp = DateTime::from(SystemTime::now());

    let mut stream = TcpStream::connect(peer_address()).unwrap();
    stream.write_all(COORDINATOR_MAGIC).unwrap();

    store_blob(&mut stream, BLOB, timestamp);
    expect_ok(&mut stream);

    let new_timestamp = DateTime::from(SystemTime::now() + Duration::from_secs(10)).mark_removed();
    store_blob(&mut stream, BLOB, new_timestamp);
    expect_ok(&mut stream);

    // Timestamp should be updated.
    request_blob(&mut stream, &key);
    expect_blob(&mut stream, &[], new_timestamp);

    expect_shutdown(stream);
}

#[test]
fn store_removed_already_stored_before() {
    let _p = start_stored();

    const BLOB: &[u8] = b"remove already stored blob before";
    let key = Key::for_blob(BLOB);
    let timestamp = DateTime::from(SystemTime::now());

    let mut stream = TcpStream::connect(peer_address()).unwrap();
    stream.write_all(COORDINATOR_MAGIC).unwrap();

    store_blob(&mut stream, BLOB, timestamp);
    expect_ok(&mut stream);

    let new_timestamp = DateTime::from(SystemTime::now() - Duration::from_secs(10)).mark_removed();
    store_blob(&mut stream, BLOB, new_timestamp);
    expect_ok(&mut stream);

    // Timestamp should be updated.
    request_blob(&mut stream, &key);
    expect_blob(&mut stream, &[], new_timestamp);

    expect_shutdown(stream);
}

#[test]
fn store_removed_already_stored_removed_after() {
    let _p = start_stored();

    const BLOB: &[u8] = b"remove already removed blob after";
    let key = Key::for_blob(BLOB);
    let timestamp = DateTime::from(SystemTime::now()).mark_removed();

    let mut stream = TcpStream::connect(peer_address()).unwrap();
    stream.write_all(COORDINATOR_MAGIC).unwrap();

    store_blob(&mut stream, BLOB, timestamp);
    expect_ok(&mut stream);

    let new_timestamp = DateTime::from(SystemTime::now() + Duration::from_secs(10)).mark_removed();
    store_blob(&mut stream, BLOB, new_timestamp);
    expect_ok(&mut stream);

    // Timestamp should not be updated.
    request_blob(&mut stream, &key);
    expect_blob(&mut stream, &[], timestamp);

    expect_shutdown(stream);
}

#[test]
fn store_removed_already_stored_removed_before() {
    let _p = start_stored();

    const BLOB: &[u8] = b"remove already removed blob before";
    let key = Key::for_blob(BLOB);
    let timestamp = DateTime::from(SystemTime::now()).mark_removed();

    let mut stream = TcpStream::connect(peer_address()).unwrap();
    stream.write_all(COORDINATOR_MAGIC).unwrap();

    store_blob(&mut stream, BLOB, timestamp);
    expect_ok(&mut stream);

    let new_timestamp = DateTime::from(SystemTime::now() - Duration::from_secs(10)).mark_removed();
    store_blob(&mut stream, BLOB, new_timestamp);
    expect_ok(&mut stream);

    // Timestamp should not be updated.
    request_blob(&mut stream, &key);
    expect_blob(&mut stream, &[], timestamp);

    expect_shutdown(stream);
}

#[test]
fn invalid_request() {
    let _p = start_stored();

    let mut stream = TcpStream::connect(peer_address()).unwrap();
    stream.write_all(COORDINATOR_MAGIC).unwrap();

    let bufs = &mut [IoSlice::new(&[6])];
    stream.write_all_vectored(bufs).unwrap();

    expect_err(&mut stream, b"invalid request type");

    let mut buf = [1; 10];
    let n = stream.read(&mut buf).unwrap();
    assert_eq!(n, 0);
}

fn peer_address() -> SocketAddr {
    SocketAddr::new(Ipv4Addr::LOCALHOST.into(), PEER_PORT)
}

#[track_caller]
fn store_blob(stream: &mut TcpStream, blob: &[u8], timestamp: DateTime) {
    let key = Key::for_blob(blob);
    let length = u64::to_be_bytes(blob.len() as u64);

    let bufs = &mut [
        IoSlice::new(&[STORE_BLOB]),
        IoSlice::new(key.as_bytes()),
        IoSlice::new(timestamp.as_bytes()),
        IoSlice::new(&length),
        IoSlice::new(blob),
    ];
    stream.write_all_vectored(bufs).unwrap();
}

#[track_caller]
fn request_blob(stream: &mut TcpStream, key: &Key) {
    let bufs = &mut [IoSlice::new(&[REQUEST_BLOB]), IoSlice::new(key.as_bytes())];
    stream.write_all_vectored(bufs).unwrap();
}

#[track_caller]
fn expect_blob(stream: &mut TcpStream, expected_blob: &[u8], expected_timestamp: DateTime) {
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();

    let mut buf = vec![0; METADATA_LEN + expected_blob.len() + 1];
    let mut n = 0;
    while n < buf.len() - 1 {
        let read = stream.read(&mut buf[n..]).unwrap();
        n += read;
    }
    assert_eq!(n, METADATA_LEN + expected_blob.len());

    let timestamp_bytes = &buf[..DATE_TIME_LEN];
    let timestamp = DateTime::from_bytes(timestamp_bytes).unwrap();
    assert_eq!(timestamp, expected_timestamp);

    let length_bytes = &buf[DATE_TIME_LEN..DATE_TIME_LEN + BLOB_LENGTH_LEN];
    assert_eq!(
        u64::from_be_bytes(length_bytes.try_into().unwrap()),
        expected_blob.len() as u64
    );

    assert_eq!(&buf[METADATA_LEN..n], expected_blob);
}

/// Expects an generic OK response.
#[track_caller]
fn expect_ok(stream: &mut TcpStream) {
    const EXPECTED: &[u8] = b"OK\0\0";
    let mut buf = [1; EXPECTED.len() + 1];
    let n = stream.read(&mut buf).unwrap();
    assert_eq!(n, EXPECTED.len());
    assert_eq!(&buf[..n], EXPECTED);
}

/// Expects an generic error response with a message.
#[track_caller]
fn expect_err(stream: &mut TcpStream, expected_msg: &[u8]) {
    const EXPECTED: &[u8] = b"ERR\0";
    let msg_len = u32::to_be_bytes(expected_msg.len() as u32);
    let mut buf = vec![1; EXPECTED.len() + 4 + expected_msg.len() + 1];
    let n = stream.read(&mut buf).unwrap();
    assert_eq!(n, buf.len() - 1);
    assert_eq!(&buf[..4], EXPECTED);
    assert_eq!(&buf[4..8], msg_len);
    assert_eq!(&buf[8..n], expected_msg);
}

#[track_caller]
fn expect_shutdown(mut stream: TcpStream) {
    stream.shutdown(Shutdown::Both).unwrap();

    let mut buf = [0; 1];
    let n = stream.read(&mut buf).unwrap();
    assert_eq!(n, 0);
}
