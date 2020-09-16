#![allow(unused_imports)]

use std::thread::{self, sleep};
use std::time::{Duration, SystemTime};

use log::LevelFilter;
use stored::peer::ConsensusVote;
use stored::Key;

use super::{store_hello_world, TestPeer};

// TODO: expand testing.

#[test]
fn hello_world() {
    const DB_PORT: u16 = 13010;
    const DB_PATH: &str = "/tmp/stored/mocked_store_blob.db";
    const CONF_PATH: &str = "tests/config/mocked_store_blob.toml";
    const FILTER: LevelFilter = LevelFilter::Warn;

    start_stored_fn!(&[CONF_PATH], &[DB_PATH], FILTER);

    let addr = "127.0.0.1:13111".parse().unwrap();
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
    assert_eq!(server_addr, "127.0.0.1:13110".parse().unwrap());
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
    drop(process_guard);
}
