use std::sync::Mutex;
use std::thread::{self, sleep};
use std::time::Duration;

use lazy_static::lazy_static;
use log::LevelFilter;
use stored::Key;

use super::{store_blob, Dispatcher, TestPeer, TestStream, BLOBS, IGN_FAILURE};
use crate::util::Proc;

// TODO: add a test where a peer disconnects, a blob is added/removed, and then
// the peer is reconnected. A partial sync needs to run adding the blob
// added/removed.

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
    // Case two: the peer has a key we don't have.

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
    // Case three: we have a key the peer doesn't have.

    let mut guard = PEER_SYNC_PROC.lock().expect(IGN_FAILURE);
    let (peer, p_stream, _p, keys) = &mut *guard;
    let peer_stream = p_stream.take().expect(IGN_FAILURE);

    // Disconnect the peer to force the peer to run a peer synchronisation.
    drop(peer_stream);
    *p_stream = Some(peer.expect_participant_conn("127.0.0.1:13100".parse().unwrap(), &[]));

    const BLOB: &[u8] = BLOBS[1];
    keys.push(Key::for_blob(BLOB));
    peer.expect_peer_sync(&*keys, &[BLOB]);
}
