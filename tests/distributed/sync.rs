use std::thread::{self, sleep};
use std::time::{Duration, SystemTime};

use http::header::{CONNECTION, CONTENT_LENGTH, LAST_MODIFIED, LOCATION};
use http::status::StatusCode;
use log::LevelFilter;

use crate::util::http::{body, date_header, header};
use crate::TestPeer;

use stored::peer::ConsensusVote;
use stored::Key;

// NOTE: because these test purposely disconnect peer connections we turn off
// logging to not log a bunch of warnings that we purposely created.
const FILTER: LevelFilter = LevelFilter::Off;

#[test]
fn peer_sync_after_disconnect() {
    const DB_PORT: u16 = 13000;
    const DB_PATH: &str = "/tmp/stored/sync_after_disconnect.db";
    const CONF_PATH: &str = "tests/config/sync_after_disconnect.toml";

    start_stored_fn!(&[CONF_PATH], &[DB_PATH], FILTER);

    let addr = "127.0.0.1:13101".parse().unwrap();
    let mut peer = TestPeer::bind(addr).unwrap();

    let _p = start_stored();

    // Accept a peer connection and set it up.
    let (mut peer_stream, _) = peer.accept().expect("failed to accept peer connection");
    assert!(peer_stream
        .read_participant_magic()
        .expect("failed to read magic"));
    let server_addr = peer_stream
        .read_server_addr()
        .expect("failed to read peer server address");
    assert_eq!(server_addr, "127.0.0.1:13100".parse().unwrap());
    peer_stream
        .write_peers(&[])
        .expect("failed to write known peers");

    // Run the full synchronisation protocol.
    let (mut sync_stream, _) = peer.accept().expect("failed to accept peer connection");
    assert!(sync_stream
        .read_coordinator_magic()
        .expect("failed to read magic"));
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
    assert!(peer_stream
        .read_participant_magic()
        .expect("failed to read magic"));
    let server_addr = peer_stream
        .read_server_addr()
        .expect("failed to read peer server address");
    assert_eq!(server_addr, "127.0.0.1:13100".parse().unwrap());
    peer_stream
        .write_peers(&[])
        .expect("failed to write known peers");
    // Since the connection was disconnected and reconnected we expect the peer
    // to run a partial_sync.
    peer_stream
        .expect_request_keys_since(std::slice::from_ref(&key))
        .expect("failed to handle request keys since request");

    drop(peer_stream);
    // Case two: the peer has a key we don't have.
    let (mut peer_stream, _) = peer.accept().expect("failed to accept peer connection");
    assert!(peer_stream
        .read_participant_magic()
        .expect("failed to read magic"));
    let server_addr = peer_stream
        .read_server_addr()
        .expect("failed to read peer server address");
    assert_eq!(server_addr, "127.0.0.1:13100".parse().unwrap());
    peer_stream
        .write_peers(&[])
        .expect("failed to write known peers");
    // Since the connection was disconnected and reconnected we expect the peer
    // to run a partial_sync. We'll pretend we missed the last key.
    peer_stream
        .expect_request_keys_since(&[])
        .expect("failed to handle request keys since request");
    // The peer should then ask us to store the blob.
    peer_stream
        .expect_request_store_blob(b"Hello world")
        .expect("failed to handle request store request");

    drop(peer_stream);
    // Case three: we have a key the peer doesn't have.
    let (mut peer_stream, _) = peer.accept().expect("failed to accept peer connection");
    assert!(peer_stream
        .read_participant_magic()
        .expect("failed to read magic"));
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
    peer_stream
        .expect_request_keys_since(&[key, Key::for_blob(new_blob)])
        .expect("failed to handle request keys since request");
    // The peer should then ask us to for the new blob.
    peer_stream
        .expect_request_blob(new_blob)
        .expect("failed to handle request store request");

    // And we're done.
}

fn store_hello_world(port: u16) {
    let url = "/blob/b7f783baed8297f0db917462184ff4f08e69c2d5e5f79a942600f9725f58ce1f29c18139bf80b06c0fff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47";
    request!(
        POST port, "/blob", b"Hello world",
        CONTENT_LENGTH => "11";
        expected: StatusCode::CREATED, body::EMPTY,
        CONTENT_LENGTH => body::EMPTY_LEN,
        LOCATION => url,
        CONNECTION => header::KEEP_ALIVE,
    );
    let last_modified = date_header();
    request!(
        GET port, url, body::EMPTY,
        expected: StatusCode::OK, b"Hello world",
        CONTENT_LENGTH => "11",
        LAST_MODIFIED => &last_modified,
        CONNECTION => header::KEEP_ALIVE,
    );
}
