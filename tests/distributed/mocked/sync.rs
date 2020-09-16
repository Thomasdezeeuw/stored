use std::thread::{self, sleep};
use std::time::{Duration, SystemTime};

use log::LevelFilter;
use stored::peer::ConsensusVote;
use stored::Key;

use super::{store_hello_world, TestPeer};

// NOTE: because these test purposely disconnect peer connections we turn off
// logging to not log a bunch of warnings that we purposely created.
const FILTER: LevelFilter = LevelFilter::Error;

// TODO: add a test where a peer disconnects, a blob is added/removed, and then
// the peer is reconnected. A partial sync needs to run adding the blob
// added/removed.

#[test]
fn peer_sync_after_disconnect() {
    const DB_PORT: u16 = 13000;
    const DB_PATH: &str = "/tmp/stored/mocked_sync_after_disconnect.db";
    const CONF_PATH: &str = "tests/config/mocked_sync_after_disconnect.toml";

    start_stored_fn!(&[CONF_PATH], &[DB_PATH], FILTER);

    let addr = "127.0.0.1:13101".parse().unwrap();
    let mut peer = TestPeer::bind(addr).unwrap();

    let process_guard = start_stored();

    // Accept a peer connection and set it up.
    let (mut peer_stream, _) = peer.accept().expect("failed to accept peer connection");
    peer_stream
        .expect_participant_magic()
        .expect("failed to read magic");
    let server_addr = peer_stream
        .read_server_addr()
        .expect("failed to read peer server address");
    assert_eq!(server_addr, "127.0.0.1:13100".parse().unwrap());
    peer_stream
        .write_peers(&[])
        .expect("failed to write known peers");

    // Run the full synchronisation protocol.
    let (mut sync_stream, _) = peer.accept().expect("failed to accept peer connection");
    sync_stream
        .expect_coordinator_magic()
        .expect("failed to read magic");
    sync_stream
        .expect_request_keys(&[])
        .expect("failed to respond to key request");
    sync_stream
        .expect_end()
        .expect("failed to close connection");

    // Give the HTTP some time to startup.
    sleep(Duration::from_secs(1));

    // Store a blob, while concurrently doing the peer interaction.
    let handle = thread::spawn(|| store_hello_world(DB_PORT));

    // Fake the peer interaction for storing the blob.
    let key = Key::for_blob(b"Hello world");
    let consensus_id = peer_stream
        .expect_add_blob_request(&key, ConsensusVote::Commit(SystemTime::now()))
        .expect("failed to handle add blob request");
    peer_stream
        .expect_commit_blob_request(&key, consensus_id, ConsensusVote::Commit(SystemTime::now()))
        .expect("failed to handle add blob request");
    peer_stream
        .expect_blob_committed_request(&key, consensus_id)
        .expect("failed to handle add blob request");

    handle.join().expect("failed to store blob");

    // Oh no! The peer disconnected.
    drop(peer_stream);
    // Now the tests can actually begin.

    // Simplest case: the peer are still in sync (i.e. both have the same blobs
    // stored).
    // After a disconnect we should start the partial peer sync.
    let (mut peer_stream, _) = peer.accept().expect("failed to accept peer connection");
    peer_stream
        .expect_participant_magic()
        .expect("failed to read magic");
    let server_addr = peer_stream
        .read_server_addr()
        .expect("failed to read peer server address");
    assert_eq!(server_addr, "127.0.0.1:13100".parse().unwrap());
    peer_stream
        .write_peers(&[])
        .expect("failed to write known peers");
    // Since the connection was disconnected and reconnected we expect the peer
    // to run a partial_sync.
    let (mut sync_stream, _) = peer.accept().expect("failed to accept peer connection");
    sync_stream
        .expect_coordinator_magic()
        .expect("failed to read magic");
    sync_stream
        .expect_request_keys_since(std::slice::from_ref(&key))
        .expect("failed to handle request keys since request");
    sync_stream
        .expect_end()
        .expect("expected to stream to be closed after the syncing process is done");

    drop(peer_stream);

    // Case two: the peer has a key we don't have.
    let (mut peer_stream, _) = peer.accept().expect("failed to accept peer connection");
    peer_stream
        .expect_participant_magic()
        .expect("failed to read magic");
    let server_addr = peer_stream
        .read_server_addr()
        .expect("failed to read peer server address");
    assert_eq!(server_addr, "127.0.0.1:13100".parse().unwrap());
    peer_stream
        .write_peers(&[])
        .expect("failed to write known peers");
    // Since the connection was disconnected and reconnected we expect the peer
    // to run a partial_sync. We'll pretend we missed the last key.
    let (mut sync_stream, _) = peer.accept().expect("failed to accept peer connection");
    sync_stream
        .expect_coordinator_magic()
        .expect("failed to read magic");
    sync_stream
        .expect_request_keys_since(&[])
        .expect("failed to handle request keys since request");
    // The peer should then ask us to store the blob.
    sync_stream
        .expect_request_store_blob(b"Hello world")
        .expect("failed to handle request store request");
    match sync_stream.expect_end() {
        Ok(()) => {}
        // FIXME: something on macOS this returns a connection reset error. Find
        // out why and fix it and then remove this.
        Err(ref err) if err.kind() == std::io::ErrorKind::ConnectionReset => {}
        Err(err) => panic!(
            "unexpected error: expected to stream to be closed after the syncing process is done: {}",
            err
        ),
    }

    drop(peer_stream);

    // Case three: we have a key the peer doesn't have.
    let (mut peer_stream, _) = peer.accept().expect("failed to accept peer connection");
    peer_stream
        .expect_participant_magic()
        .expect("failed to read magic");
    let server_addr = peer_stream
        .read_server_addr()
        .expect("failed to read peer server address");
    assert_eq!(server_addr, "127.0.0.1:13100".parse().unwrap());
    peer_stream
        .write_peers(&[])
        .expect("failed to write known peers");
    // Since the connection was disconnected and reconnected we expect the peer
    // to run a partial_sync. Since then we've stored another blob (`key2`);
    let new_blob = b"Hello mars";
    let (mut sync_stream, _) = peer.accept().expect("failed to accept peer connection");
    sync_stream
        .expect_coordinator_magic()
        .expect("failed to read magic");
    sync_stream
        .expect_request_keys_since(&[key, Key::for_blob(new_blob)])
        .expect("failed to handle request keys since request");
    // The peer should then ask us to for the new blob.
    sync_stream
        .expect_request_blob(new_blob)
        .expect("failed to handle request store request");
    sync_stream
        .expect_end()
        .expect("expected to stream to be closed after the syncing process is done");

    // And we're done.
    drop(process_guard);
    peer_stream
        .expect_end()
        .expect("expected to stream to be closed after the syncing process is done");
}
