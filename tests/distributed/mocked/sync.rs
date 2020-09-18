use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread::{self, sleep};
use std::time::Duration;
use std::time::SystemTime;

use http::header::{CONNECTION, CONTENT_LENGTH, LAST_MODIFIED};
use http::status::StatusCode;
use lazy_static::lazy_static;
use log::LevelFilter;
use stored::storage::DateTime;
use stored::Key;

use super::{store_blob, Dispatcher, TestPeer, TestStream, BLOBS, IGN_FAILURE};
use crate::util::http::{body, date_header, header};
use crate::util::{copy_database, Proc};

#[test]
fn full_sync_same_blobs() {
    // Simplest case: the peers are in sync (i.e. both have the same blobs
    // stored).

    const DB_PATH: &str = "/tmp/stored/mocked_full_sync_same_blobs.db";
    const CONF_PATH: &str = "tests/config/mocked_full_sync_same_blobs.toml";
    const FILTER: LevelFilter = LevelFilter::Warn;

    start_stored_fn!(&[CONF_PATH], &[DB_PATH], FILTER);

    let mut peer = TestPeer::bind("127.0.0.1:13131".parse().unwrap()).unwrap();

    let process_guard = start_stored();

    let barrier = Arc::new(Barrier::new(2));

    let barrier2 = barrier.clone();
    let handle = thread::spawn(move || {
        // Accept a peer connection and set it up.
        let addr = "127.0.0.1:13130".parse().unwrap();
        let peer_stream = peer.expect_participant_conn(addr, &[]);

        // Run the full synchronisation protocol.
        peer.expect_full_sync(&[], &[]);

        barrier2.wait();

        // Wait until the process is actually stopped before dropping
        // the connection otherwise it would log warnings.
        thread::sleep(Duration::from_millis(500));
        drop(peer_stream);
    });

    // Wait until the test is complete (which is done in thread spawned above).
    barrier.wait();
    drop(process_guard);

    handle.join().expect("failed to synchronise");
}

#[test]
fn full_sync_less_blobs() {
    // Case two: the peer has a blob we don't have.

    const DB_PATH: &str = "/tmp/stored/mocked_full_sync_less_blobs.db";
    const CONF_PATH: &str = "tests/config/mocked_full_sync_less_blobs.toml";
    const FILTER: LevelFilter = LevelFilter::Warn;

    start_stored_fn!(&[CONF_PATH], FILTER);

    // Copy a database with "Hello world" and "Hello mars" already stored.
    copy_database("tests/data/001.db", DB_PATH);

    let mut peer = TestPeer::bind("127.0.0.1:13141".parse().unwrap()).unwrap();

    let process_guard = start_stored();

    let barrier = Arc::new(Barrier::new(2));

    let barrier2 = barrier.clone();
    let handle = thread::spawn(move || {
        // Accept a peer connection and set it up.
        let addr = "127.0.0.1:13140".parse().unwrap();
        let peer_stream = peer.expect_participant_conn(addr, &[]);

        // Run the full synchronisation protocol.
        let (mut sync_stream, _) = peer.accept().expect("failed to accept peer connection");
        sync_stream.expect_coordinator_magic();
        sync_stream.expect_request_keys(&[]);
        // Expect the two blobs we're missing to be send.
        sync_stream.expect_request_store_blob(b"Hello world");
        sync_stream.expect_request_store_blob(b"Hello mars");
        sync_stream.expect_end();

        barrier2.wait();

        // Wait until the process is actually stopped before dropping
        // the connection otherwise it would log warnings.
        thread::sleep(Duration::from_millis(500));
        drop(peer_stream);
    });

    // Wait until the test is complete (which is done in thread spawned above).
    barrier.wait();
    drop(process_guard);

    handle.join().expect("failed to synchronise");
}

#[test]
fn full_sync_more_blobs() {
    // Case three: we have a blob the peer doesn't have.

    const DB_PORT: u16 = 13050;
    const DB_PATH: &str = "/tmp/stored/mocked_full_sync_more_blobs.db";
    const CONF_PATH: &str = "tests/config/mocked_full_sync_more_blobs.toml";
    const FILTER: LevelFilter = LevelFilter::Warn;

    start_stored_fn!(&[CONF_PATH], &[DB_PATH], FILTER);

    let addresses = &[
        "127.0.0.1:13151".parse().unwrap(),
        "127.0.0.1:13152".parse().unwrap(),
    ];
    let peers = addresses
        .into_iter()
        .map(|addr| TestPeer::bind(*addr).unwrap())
        .collect::<Vec<_>>();

    let process_guard = start_stored();

    let barrier = Arc::new(Barrier::new(3));

    lazy_static::lazy_static! {
        static ref BLOBS_METADATA: Vec<(Key, &'static [u8], DateTime)> = {
            let mut blobs = Vec::with_capacity(3);
            blobs.push((
                Key::for_blob(BLOBS[0]),
                BLOBS[0],
                DateTime::from(SystemTime::now()),
            ));
            blobs.push((
                Key::for_blob(BLOBS[1]),
                BLOBS[1],
                DateTime::from(SystemTime::now()).mark_removed(),
            ));
            blobs.push((
                Key::for_blob(BLOBS[2]),
                BLOBS[2],
                DateTime::from(SystemTime::now()),
            ));
            blobs
        };
    }

    fn get_blob(key: &Key) -> (&[u8], DateTime) {
        // Flag used to determine if this peer has the third key.
        static HAS_KEY_3: AtomicBool = AtomicBool::new(false);

        BLOBS_METADATA
            .iter()
            .find_map(|(k, blob, timestamp)| {
                if k != key {
                    return None;
                }

                let ret = if *blob == BLOBS[2] && !HAS_KEY_3.swap(true, Ordering::AcqRel) {
                    (&[][..], DateTime::INVALID)
                } else {
                    (*blob, *timestamp)
                };
                Some(ret)
            })
            .expect("unexpected request for blob")
    }

    let handles = peers
        .into_iter()
        .map(|mut peer| {
            let barrier2 = barrier.clone();
            thread::spawn(move || {
                // Accept a peer connection and set it up.
                let addr = "127.0.0.1:13150".parse().unwrap();
                let peer_stream = peer.expect_participant_conn(addr, &[]);

                // Run the full synchronisation protocol.
                let (mut sync_stream, _) = peer.accept().expect("failed to accept peer connection");
                sync_stream.expect_coordinator_magic();

                // NOTE: we pretend like both peers have all three keys, however
                // once there are requested we'll only act like on of the peers
                // has the blob. Technically this is invalid, but it's they only
                // way (currently) to force the path we want to test.
                let keys = &[
                    Key::for_blob(BLOBS[0]),
                    Key::for_blob(BLOBS[1]),
                    Key::for_blob(BLOBS[2]),
                ];

                sync_stream.expect_request_keys(&*keys);
                // Expect some requests for the blobs.
                loop {
                    if !sync_stream.try_expect_request_blob(get_blob) {
                        break;
                    }
                }
                sync_stream.expect_end();

                // Completed the sync, unblock the main thread.
                barrier2.wait();

                // Wait until the process is stopped.
                barrier2.wait();

                // Wait until the process is actually stopped before dropping
                // the connection otherwise it would log warnings.
                thread::sleep(Duration::from_millis(500));
                drop(peer_stream);
            })
        })
        .collect::<Vec<_>>();

    // Wait until the sync is complete.
    barrier.wait();

    // Check that all blobs are stored properly.
    let last_modified = date_header();
    for (key, blob, timestamp) in BLOBS_METADATA.iter() {
        let url = format!("/blob/{}", key);
        let (status, body, body_len) = if timestamp.is_removed() {
            (StatusCode::GONE, &[][..], "0".to_string())
        } else {
            (StatusCode::OK, *blob, blob.len().to_string())
        };
        request!(
            GET DB_PORT, url, body::EMPTY,
            expected: status, body,
            CONTENT_LENGTH => &*body_len,
            LAST_MODIFIED => &last_modified,
            CONNECTION => header::KEEP_ALIVE,
        );
    }

    drop(process_guard);
    barrier.wait();

    for handle in handles {
        handle.join().expect("failed to synchronise");
    }
}

const PEER_SYNC_DB_PORT: u16 = 13000;

lazy_static! {
    /// Shared process for the `peer_sync` tests.
    static ref PEER_SYNC_PROC: Mutex<(TestPeer, Option<TestStream<Dispatcher>>, Proc<'static>, Vec<Key>)> = {
        const DB_PATH: &str = "/tmp/stored/mocked_peer_sync.db";
        const CONF_PATH: &str = "tests/config/mocked_peer_sync.toml";
        // NOTE: because these test purposely disconnect peer connections we turn off
        // logging to not log a bunch of warnings that we purposely created.
        const FILTER: LevelFilter = LevelFilter::Off;

        start_stored_fn!(&[CONF_PATH], &[DB_PATH], FILTER);

        let addr = "127.0.0.1:13101".parse().unwrap();
        let mut peer = TestPeer::bind(addr).unwrap();

        let process = start_stored();

        // Accept a peer connection and set it up.
        let peer_stream = peer.expect_participant_conn("127.0.0.1:13100".parse().unwrap(), &[]);
        // Run the full synchronisation protocol.
        peer.expect_full_sync(&[], &[]);

        // Give the HTTP some time to startup.
        sleep(Duration::from_millis(500));

        Mutex::new((peer, Some(peer_stream), process, Vec::new()))
    };
}

#[test]
fn peer_sync_same_blobs() {
    // Simplest case: the peers are still in sync (i.e. both have the same blobs
    // stored).

    let mut guard = PEER_SYNC_PROC.lock().expect(IGN_FAILURE);
    let (peer, p_stream, _p, keys) = &mut *guard;
    let peer_stream = p_stream.take().expect(IGN_FAILURE);

    // Disconnect the peer to force the peer to run a peer synchronisation.
    drop(peer_stream);
    *p_stream = Some(peer.expect_participant_conn("127.0.0.1:13100".parse().unwrap(), &[]));
    peer.expect_peer_sync(&*keys, &[]);
}

#[test]
fn peer_sync_less_blobs() {
    // Case two: the peer has a blob we don't have.

    let mut guard = PEER_SYNC_PROC.lock().expect(IGN_FAILURE);
    let (peer, p_stream, _p, keys) = &mut *guard;
    let mut peer_stream = p_stream.take().expect(IGN_FAILURE);

    // Store a blob, while concurrently doing the peer interaction.
    const BLOB: &[u8] = BLOBS[0];
    let handle = thread::spawn(|| store_blob(PEER_SYNC_DB_PORT, BLOB));
    peer_stream.expect_store_blob_full(BLOB);
    handle.join().expect("failed to store blob");

    // Disconnect the peer to force the peer to run a peer synchronisation.
    drop(peer_stream);
    *p_stream = Some(peer.expect_participant_conn("127.0.0.1:13100".parse().unwrap(), &[]));

    let (mut sync_stream, _) = peer.accept().expect("failed to accept peer connection");
    sync_stream.expect_coordinator_magic();
    // NOTE: `keys` is missing `BLOB`.
    sync_stream.expect_request_keys_since(&*keys);
    // The peer should then ask us to store the blob.
    sync_stream.expect_request_store_blob(BLOB);
    sync_stream.expect_end();
    keys.push(Key::for_blob(BLOB));
}

#[test]
fn peer_sync_more_blobs() {
    // Case three: we have a blob the peer doesn't have.

    let mut guard = PEER_SYNC_PROC.lock().expect(IGN_FAILURE);
    let (peer, p_stream, _p, keys) = &mut *guard;
    let peer_stream = p_stream.take().expect(IGN_FAILURE);

    // Disconnect the peer to force the peer to run a peer synchronisation.
    drop(peer_stream);
    *p_stream = Some(peer.expect_participant_conn("127.0.0.1:13100".parse().unwrap(), &[]));

    const BLOB: &[u8] = BLOBS[1];
    let key = Key::for_blob(BLOB);
    let url = format!("/blob/{}", key);
    keys.push(key);
    peer.expect_peer_sync(&*keys, &[BLOB]);

    let last_modified = date_header();
    request!(
        GET PEER_SYNC_DB_PORT, url, body::EMPTY,
        expected: StatusCode::OK, BLOB,
        CONTENT_LENGTH => &*BLOB.len().to_string(),
        LAST_MODIFIED => &last_modified,
        CONNECTION => header::KEEP_ALIVE,
    );
}
