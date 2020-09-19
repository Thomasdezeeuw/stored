use std::sync::Mutex;
use std::thread::{self, sleep};
use std::time::{Duration, SystemTime};

use http::header::{CONNECTION, CONTENT_LENGTH, CONTENT_TYPE};
use http::status::StatusCode;
use lazy_static::lazy_static;
use log::LevelFilter;
use stored::peer::{ConsensusVote, Operation};
use stored::Key;

use super::{store_blob, Dispatcher, TestPeer, TestStream, BLOBS, IGN_FAILURE};
use crate::util::http::{body, header};
use crate::util::Proc;

const DB_PORT: u16 = 13010;
const DB_PATH: &str = "/tmp/stored/mocked_store_blob.db";
const CONF_PATH: &str = "tests/config/mocked_store_blob.toml";
// The `fail_*` test generate a lot of warnings (on purpose), we don't want that
// in our output.
const FILTER: LevelFilter = LevelFilter::Off;

start_stored_fn!(&[CONF_PATH], &[DB_PATH], FILTER);

lazy_static! {
    /// Shared process for the tests.
    static ref PROC: Mutex<(TestPeer, Option<TestStream<Dispatcher>>, Proc<'static>, Vec<Key>)> = {
        let addr = "127.0.0.1:13111".parse().unwrap();
        let mut peer = TestPeer::bind(addr).unwrap();

        let process = start_stored();

        // Accept a peer connection and set it up.
        let peer_stream = peer.expect_participant_conn("127.0.0.1:13110".parse().unwrap(), &[]);
        // Run the full synchronisation protocol.
        peer.expect_full_sync(&[], &[]);

        // Give the HTTP some time to startup.
        sleep(Duration::from_millis(500));

        Mutex::new((peer, Some(peer_stream), process, Vec::new()))
    };
}

#[test]
fn successfull_store() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (_, peer_stream, _p, keys) = &mut *guard;
    let peer_stream = peer_stream.as_mut().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOBS[0];
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle = thread::spawn(|| store_blob(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.
    let consensus_id =
        peer_stream.expect_add_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    peer_stream.expect_commit_store_blob_request(
        &key,
        consensus_id,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_blob_store_committed_request(&key, consensus_id);

    handle.join().expect("failed to store blob");
    keys.push(key);

    // Storing the same blob shouldn't involve the peer.
    // NOTE: if it would involve the peer the operation would time out as we
    // don't respond to any requests.
    store_blob(DB_PORT, BLOB);
}

#[test]
fn fail_2pc_phase_one_vote_fail() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (_, peer_stream, _p, keys) = &mut *guard;
    let peer_stream = peer_stream.as_mut().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOBS[1];
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle = thread::spawn(|| store_blob(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.
    let consensus_id_failed = peer_stream.expect_add_blob_request(&key, ConsensusVote::Fail);
    // Expect another 2PC query.
    let consensus_id =
        peer_stream.expect_add_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    assert!(consensus_id_failed < consensus_id);
    // Abort the old 2PC query.
    peer_stream.expect_abort_store_blob_request(
        &key,
        consensus_id_failed,
        ConsensusVote::Commit(SystemTime::now()),
    );
    // Commit to the new 2PC query.
    peer_stream.expect_commit_store_blob_request(
        &key,
        consensus_id,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_blob_store_committed_request(&key, consensus_id);

    handle.join().expect("failed to store blob");
    keys.push(key);
}

#[test]
fn fail_2pc_phase_one_vote_fail_no_response_timeout() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (_, peer_stream, _p, keys) = &mut *guard;
    let peer_stream = peer_stream.as_mut().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOBS[2];
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle = thread::spawn(|| store_blob(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.
    let request = peer_stream
        .read_request()
        .expect("failed to handle add blob request");
    assert_eq!(request.key, key);
    assert!(matches!(request.op, Operation::AddBlob));
    let consensus_id_failed = request.consensus_id;

    // NOTE: we don't respond here. The consensus algorithm should hit a
    // timeout.

    // Expect another 2PC query.
    let consensus_id =
        peer_stream.expect_add_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    assert!(consensus_id_failed < consensus_id);
    // Abort the old 2PC query.
    peer_stream.expect_abort_store_blob_request(
        &key,
        consensus_id_failed,
        ConsensusVote::Commit(SystemTime::now()),
    );
    // Commit to the new 2PC query.
    peer_stream.expect_commit_store_blob_request(
        &key,
        consensus_id,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_blob_store_committed_request(&key, consensus_id);

    handle.join().expect("failed to store blob");
    keys.push(key);
}

#[test]
fn fail_2pc_phase_one_vote_fail_disconnect() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (peer, p_stream, _p, keys) = &mut *guard;
    let mut peer_stream = p_stream.take().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOBS[3];
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle = thread::spawn(|| store_blob(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.
    let request = peer_stream
        .read_request()
        .expect("failed to handle add blob request");
    assert_eq!(request.key, key);
    assert!(matches!(request.op, Operation::AddBlob));
    let consensus_id_failed = request.consensus_id;

    // NOTE: we disconnect here, not sending a response. The consensus algorithm
    // should hit a timeout.
    drop(peer_stream);

    // The peer should try to reconnect.
    let mut peer_stream = peer.expect_participant_conn("127.0.0.1:13110".parse().unwrap(), &[]);
    // Running a peer sync as well.
    peer.expect_peer_sync(keys, &[]);

    // Expect another 2PC query.
    let consensus_id =
        peer_stream.expect_add_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    assert!(consensus_id_failed < consensus_id);
    // Abort the old 2PC query.
    peer_stream.expect_abort_store_blob_request(
        &key,
        consensus_id_failed,
        ConsensusVote::Commit(SystemTime::now()),
    );
    // Commit to the new 2PC query.
    peer_stream.expect_commit_store_blob_request(
        &key,
        consensus_id,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_blob_store_committed_request(&key, consensus_id);

    handle.join().expect("failed to store blob");
    keys.push(key);

    *p_stream = Some(peer_stream);
}

#[test]
fn fail_2pc_phase_one_vote_abort() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (_, peer_stream, _p, keys) = &mut *guard;
    let peer_stream = peer_stream.as_mut().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOBS[4];
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle = thread::spawn(|| store_blob(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.
    let consensus_id_failed = peer_stream.expect_add_blob_request(&key, ConsensusVote::Abort);
    // Expect another 2PC query.
    let consensus_id =
        peer_stream.expect_add_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    assert!(consensus_id_failed < consensus_id);
    // Abort the old 2PC query.
    peer_stream.expect_abort_store_blob_request(
        &key,
        consensus_id_failed,
        ConsensusVote::Commit(SystemTime::now()),
    );
    // Commit to the new 2PC query.
    peer_stream.expect_commit_store_blob_request(
        &key,
        consensus_id,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_blob_store_committed_request(&key, consensus_id);

    handle.join().expect("failed to store blob");
    keys.push(key);
}

#[test]
fn fail_2pc_phase_one_vote_abort_already_stored() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (_, peer_stream, _p, keys) = &mut *guard;
    let peer_stream = peer_stream.as_mut().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOBS[5];
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle1 = thread::spawn(|| store_blob(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.

    // Expect the AddBlob request, but don't yet respond to it.
    let request = peer_stream
        .read_request()
        .expect("failed to read AddBlob request");
    assert_eq!(request.key, key);
    assert!(matches!(request.op, Operation::AddBlob));
    let consensus_id_failed = request.consensus_id;

    // Start another concurrent request to store the same blob.
    let handle2 = thread::spawn(|| store_blob(DB_PORT, BLOB));

    // Expect another 2PC query.
    let consensus_id =
        peer_stream.expect_add_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    assert!(consensus_id_failed < consensus_id);
    // Commit to the new 2PC query.
    peer_stream.expect_commit_store_blob_request(
        &key,
        consensus_id,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_blob_store_committed_request(&key, consensus_id);

    // Respond to the initial AddBlob request.
    peer_stream
        .write_response(request.id, ConsensusVote::Abort)
        .expect("failed to write response to AddBlob request");
    // Abort the old 2PC query.
    peer_stream.expect_abort_store_blob_request(
        &key,
        consensus_id_failed,
        ConsensusVote::Commit(SystemTime::now()),
    );

    handle1.join().expect("failed to store blob");
    handle2.join().expect("failed to store blob");
    keys.push(key);
}

#[test]
fn fail_2pc_phase_two_vote_fail() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (_, peer_stream, _p, keys) = &mut *guard;
    let peer_stream = peer_stream.as_mut().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOBS[6];
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle = thread::spawn(|| store_blob(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.
    let consensus_id_failed =
        peer_stream.expect_add_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    // Vote to fail the 2PC query.
    peer_stream.expect_commit_store_blob_request(&key, consensus_id_failed, ConsensusVote::Fail);

    // Expect another 2PC query.
    let consensus_id =
        peer_stream.expect_add_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    assert!(consensus_id_failed < consensus_id);
    // Abort the old 2PC query.
    peer_stream.expect_abort_store_blob_request(
        &key,
        consensus_id_failed,
        ConsensusVote::Commit(SystemTime::now()),
    );
    // Commit to the new 2PC query.
    peer_stream.expect_commit_store_blob_request(
        &key,
        consensus_id,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_blob_store_committed_request(&key, consensus_id);

    handle.join().expect("failed to store blob");
    keys.push(key);
}

#[test]
fn fail_2pc_phase_two_vote_fail_no_response_timeout() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (_, peer_stream, _p, keys) = &mut *guard;
    let peer_stream = peer_stream.as_mut().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOBS[7];
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle = thread::spawn(|| store_blob(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.
    let consensus_id_failed =
        peer_stream.expect_add_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    let request = peer_stream
        .read_request()
        .expect("failed to handle commit store blob request");
    assert_eq!(request.key, key);
    assert!(matches!(request.op, Operation::CommitStoreBlob(..)));
    assert_eq!(request.consensus_id, consensus_id_failed);

    // NOTE: we don't respond here. The consensus algorithm should hit a
    // timeout.

    // Expect another 2PC query.
    let consensus_id =
        peer_stream.expect_add_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    assert!(consensus_id_failed < consensus_id);
    // Abort the old 2PC query.
    peer_stream.expect_abort_store_blob_request(
        &key,
        consensus_id_failed,
        ConsensusVote::Commit(SystemTime::now()),
    );
    // Commit to the new 2PC query.
    peer_stream.expect_commit_store_blob_request(
        &key,
        consensus_id,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_blob_store_committed_request(&key, consensus_id);

    handle.join().expect("failed to store blob");
    keys.push(key);
}

#[test]
fn fail_2pc_phase_two_vote_fail_disconnect() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (peer, p_stream, _p, keys) = &mut *guard;
    let mut peer_stream = p_stream.take().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOBS[8];
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle = thread::spawn(|| store_blob(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.
    let consensus_id_failed =
        peer_stream.expect_add_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    let request = peer_stream
        .read_request()
        .expect("failed to handle commit store blob request");
    assert_eq!(request.key, key);
    assert!(matches!(request.op, Operation::CommitStoreBlob(..)));
    assert_eq!(request.consensus_id, consensus_id_failed);

    // NOTE: we disconnect here, not sending a response. The consensus algorithm
    // should hit a timeout.
    drop(peer_stream);

    // The peer should try to reconnect.
    let mut peer_stream = peer.expect_participant_conn("127.0.0.1:13110".parse().unwrap(), &[]);
    // Running a peer sync as well.
    peer.expect_peer_sync(keys, &[]);

    // Expect another 2PC query.
    let consensus_id =
        peer_stream.expect_add_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    assert!(consensus_id_failed < consensus_id);
    // Abort the old 2PC query.
    peer_stream.expect_abort_store_blob_request(
        &key,
        consensus_id_failed,
        ConsensusVote::Commit(SystemTime::now()),
    );
    // Commit to the new 2PC query.
    peer_stream.expect_commit_store_blob_request(
        &key,
        consensus_id,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_blob_store_committed_request(&key, consensus_id);

    handle.join().expect("failed to store blob");
    keys.push(key);

    *p_stream = Some(peer_stream);
}

#[test]
fn fail_2pc_phase_two_vote_abort() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (_, peer_stream, _p, keys) = &mut *guard;
    let peer_stream = peer_stream.as_mut().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOBS[9];
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle = thread::spawn(|| store_blob(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.
    let consensus_id_failed =
        peer_stream.expect_add_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    // Vote to fail the 2PC query.
    peer_stream.expect_commit_store_blob_request(&key, consensus_id_failed, ConsensusVote::Abort);

    // Expect another 2PC query.
    let consensus_id =
        peer_stream.expect_add_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    assert!(consensus_id_failed < consensus_id);
    // Abort the old 2PC query.
    peer_stream.expect_abort_store_blob_request(
        &key,
        consensus_id_failed,
        ConsensusVote::Commit(SystemTime::now()),
    );
    // Commit to the new 2PC query.
    peer_stream.expect_commit_store_blob_request(
        &key,
        consensus_id,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_blob_store_committed_request(&key, consensus_id);

    handle.join().expect("failed to store blob");
    keys.push(key);
}

#[test]
fn fail_2pc_phase_two_vote_abort_already_stored() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (_, peer_stream, _p, keys) = &mut *guard;
    let peer_stream = peer_stream.as_mut().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOBS[10];
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle1 = thread::spawn(|| store_blob(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.
    let consensus_id_failed =
        peer_stream.expect_add_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    // Expect the CommitStoreBlob request, but don't yet respond to it.
    let request = peer_stream
        .read_request()
        .expect("failed to handle commit store blob request");
    assert_eq!(request.key, key);
    assert!(matches!(request.op, Operation::CommitStoreBlob(..)));
    assert_eq!(request.consensus_id, consensus_id_failed);

    // Start another concurrent request to store the same blob.
    let handle2 = thread::spawn(|| store_blob(DB_PORT, BLOB));

    // Expect another 2PC query.
    let consensus_id =
        peer_stream.expect_add_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    assert!(consensus_id_failed < consensus_id);
    // Commit to the new 2PC query.
    peer_stream.expect_commit_store_blob_request(
        &key,
        consensus_id,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_blob_store_committed_request(&key, consensus_id);

    // Respond to the initial AddBlob request.
    peer_stream
        .write_response(request.id, ConsensusVote::Abort)
        .expect("failed to write response to CommitStoreBlob request");
    // Abort the old 2PC query.
    peer_stream.expect_abort_store_blob_request(
        &key,
        consensus_id_failed,
        ConsensusVote::Commit(SystemTime::now()),
    );

    handle1.join().expect("failed to store blob");
    handle2.join().expect("failed to store blob");
    keys.push(key);
}

#[test]
fn fail_2pc_completely_phase_one_vote_fail() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (_, peer_stream, _p, _) = &mut *guard;
    let peer_stream = peer_stream.as_mut().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOBS[11];
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle = thread::spawn(|| expect_store_blob_failure(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.
    let consensus_id1 = peer_stream.expect_add_blob_request(&key, ConsensusVote::Fail);

    // Expect another attempt, aborting the old one.
    let consensus_id2 = peer_stream.expect_add_blob_request(&key, ConsensusVote::Fail);
    assert!(consensus_id1 < consensus_id2);
    peer_stream.expect_abort_store_blob_request(
        &key,
        consensus_id1,
        ConsensusVote::Commit(SystemTime::now()),
    );

    // Expect a third (and final) attempt, aborting both.
    let consensus_id3 = peer_stream.expect_add_blob_request(&key, ConsensusVote::Fail);
    assert!(consensus_id2 < consensus_id3);
    peer_stream.expect_abort_store_blob_request(
        &key,
        consensus_id2,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_abort_store_blob_request(
        &key,
        consensus_id3,
        ConsensusVote::Commit(SystemTime::now()),
    );

    handle.join().expect("failed to store blob");
}

#[test]
fn fail_2pc_completely_phase_one_vote_abort() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (_, peer_stream, _p, _) = &mut *guard;
    let peer_stream = peer_stream.as_mut().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOBS[11];
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle = thread::spawn(|| expect_store_blob_failure(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.
    let consensus_id1 = peer_stream.expect_add_blob_request(&key, ConsensusVote::Abort);

    // Expect another attempt, aborting the old one.
    let consensus_id2 = peer_stream.expect_add_blob_request(&key, ConsensusVote::Abort);
    assert!(consensus_id1 < consensus_id2);
    peer_stream.expect_abort_store_blob_request(
        &key,
        consensus_id1,
        ConsensusVote::Commit(SystemTime::now()),
    );

    // Expect a third (and final) attempt, aborting both.
    let consensus_id3 = peer_stream.expect_add_blob_request(&key, ConsensusVote::Abort);
    assert!(consensus_id2 < consensus_id3);
    peer_stream.expect_abort_store_blob_request(
        &key,
        consensus_id2,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_abort_store_blob_request(
        &key,
        consensus_id3,
        ConsensusVote::Commit(SystemTime::now()),
    );

    handle.join().expect("failed to store blob");
}

#[test]
fn fail_2pc_completely_phase_two_vote_fail() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (_, peer_stream, _p, _) = &mut *guard;
    let peer_stream = peer_stream.as_mut().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOBS[11];
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle = thread::spawn(|| expect_store_blob_failure(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.
    let consensus_id1 =
        peer_stream.expect_add_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    // Vote to fail the 2PC query.
    peer_stream.expect_commit_store_blob_request(&key, consensus_id1, ConsensusVote::Fail);

    // Expect another attempt, aborting the old one.
    let consensus_id2 =
        peer_stream.expect_add_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    assert!(consensus_id1 < consensus_id2);
    peer_stream.expect_abort_store_blob_request(
        &key,
        consensus_id1,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_commit_store_blob_request(&key, consensus_id2, ConsensusVote::Fail);

    // Expect a third (and final) attempt, aborting both.
    let consensus_id3 =
        peer_stream.expect_add_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    assert!(consensus_id2 < consensus_id3);
    peer_stream.expect_abort_store_blob_request(
        &key,
        consensus_id2,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_commit_store_blob_request(&key, consensus_id3, ConsensusVote::Fail);
    peer_stream.expect_abort_store_blob_request(
        &key,
        consensus_id3,
        ConsensusVote::Commit(SystemTime::now()),
    );

    handle.join().expect("failed to store blob");
}

#[test]
fn fail_2pc_completely_phase_two_vote_abort() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (_, peer_stream, _p, _) = &mut *guard;
    let peer_stream = peer_stream.as_mut().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOBS[11];
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle = thread::spawn(|| expect_store_blob_failure(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.
    let consensus_id1 =
        peer_stream.expect_add_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    // Vote to fail the 2PC query.
    peer_stream.expect_commit_store_blob_request(&key, consensus_id1, ConsensusVote::Abort);

    // Expect another attempt, aborting the old one.
    let consensus_id2 =
        peer_stream.expect_add_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    assert!(consensus_id1 < consensus_id2);
    peer_stream.expect_abort_store_blob_request(
        &key,
        consensus_id1,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_commit_store_blob_request(&key, consensus_id2, ConsensusVote::Abort);

    // Expect a third (and final) attempt, aborting both.
    let consensus_id3 =
        peer_stream.expect_add_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    assert!(consensus_id2 < consensus_id3);
    peer_stream.expect_abort_store_blob_request(
        &key,
        consensus_id2,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_commit_store_blob_request(&key, consensus_id3, ConsensusVote::Abort);
    peer_stream.expect_abort_store_blob_request(
        &key,
        consensus_id3,
        ConsensusVote::Commit(SystemTime::now()),
    );

    handle.join().expect("failed to store blob");
}

/// Store `blob` on a server running on localhost `port`, expecting to fail.
#[track_caller]
fn expect_store_blob_failure(port: u16, blob: &[u8]) {
    let length = blob.len().to_string();
    request!(
        POST port, "/blob", blob,
        CONTENT_LENGTH => &*length;
        expected: StatusCode::INTERNAL_SERVER_ERROR, body::SERVER_ERROR,
        CONTENT_LENGTH => body::SERVER_ERROR_LEN,
        CONTENT_TYPE => header::PLAIN_TEXT,
        CONNECTION => header::CLOSE,
    );
}

mod coordinator {
    //! Tests mocking the coordinator side of the peer interaction.

    // TODO: test peer consensus after we've committed to storing a blob, but
    // the coordinator didn't send the last `StoreCommitted` message.

    // TODO: try to use a new listener per test, creating `unique_addr`, would
    // allow us to remove the lock around accessing the peer and speed up the
    // tests.
    // ```
    // TestStream::connect(*PEER_ADDR, unique_addr, &[]);
    // ```

    use std::io::{Read, Write};
    use std::mem::size_of;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::thread::{self, sleep};
    use std::time::{Duration, SystemTime};

    use lazy_static::lazy_static;
    use log::LevelFilter;
    use stored::peer::server::REQUEST_BLOB;
    use stored::peer::{ConsensusId, RequestId};
    use stored::storage::DateTime;
    use stored::Key;

    use crate::util::http::date_header;
    use crate::util::Proc;

    use super::super::{
        check_blob_not_stored, retrieve_blob, Dispatcher, TestPeer, TestStream, BLOBS, IGN_FAILURE,
    };

    const DB_PORT: u16 = 13060;
    const DB_PATH: &str = "/tmp/stored/mocked_coordinator_store_blob.db";
    const CONF_PATH: &str = "tests/config/mocked_coordinator_store_blob.toml";
    // TODO: determine the log level.
    const FILTER: LevelFilter = LevelFilter::Warn;

    start_stored_fn!(&[CONF_PATH], &[DB_PATH], FILTER);

    lazy_static! {
        /// Address of the peer.
        static ref PEER_ADDR: SocketAddr = "127.0.0.1:13160".parse().unwrap();
        /// Address of the server we're faking.
        static ref SERVER_ADDR: SocketAddr = "127.0.0.1:13161".parse().unwrap();

        /// Shared process for the tests.
        static ref PROC: Mutex<(Option<TestPeer>, TestStream<Dispatcher>, Proc<'static>, Vec<Key>)> = {
            let mut peer = TestPeer::bind(*SERVER_ADDR).unwrap();

            let process = start_stored();

            // Accept a peer connection and set it up.
            let peer_stream = peer.expect_participant_conn(*PEER_ADDR, &[]);
            // Run the full synchronisation protocol.
            peer.expect_full_sync(&[], &[]);

            // Give the HTTP some time to startup.
            sleep(Duration::from_millis(500));

            Mutex::new((Some(peer), peer_stream, process, Vec::new()))
        };
    }

    fn next_request_id() -> RequestId {
        static ID: AtomicUsize = AtomicUsize::new(0);
        let id = ID.fetch_add(1, Ordering::Relaxed);
        RequestId(id)
    }

    fn next_consensus_id() -> ConsensusId {
        static ID: AtomicUsize = AtomicUsize::new(0);
        let id = ID.fetch_add(1, Ordering::Relaxed);
        ConsensusId(id)
    }

    #[test]
    fn successfull_store() {
        let mut guard = PROC.lock().expect(IGN_FAILURE);
        let (p, _, _p, _) = &mut *guard;
        let mut peer = p.take().expect(IGN_FAILURE);

        const BLOB: &[u8] = BLOBS[0];
        let key = Key::for_blob(BLOB);
        let consensus_id = next_consensus_id();

        // Concurrent handling of the server.
        let handle = thread::spawn(move || {
            let mut peer_stream = peer.expect_server_conn();
            peer_stream.expect_request_blob(BLOB);
            peer
        });

        let mut stream = TestStream::connect(*PEER_ADDR, *SERVER_ADDR, &[]);

        // 2PC phase one: adding the blob.
        let request_id = next_request_id();
        stream.write_add_blob_request(request_id, consensus_id, key.clone());
        stream.expect_commit_response(request_id);

        // 2PC phase two: commit to storing the blob.
        let request_id = next_request_id();
        stream.write_commit_store_blob_request(request_id, consensus_id, key.clone());
        stream.expect_commit_response(request_id);

        stream.write_store_committed_request(request_id, consensus_id, key);
        // NOTE: we don't expect a response.

        stream.close();

        let peer = handle.join().expect("peer server interaction failed");
        *p = Some(peer);
        drop(guard);

        retrieve_blob(DB_PORT, BLOB, &date_header());
    }

    /// A blob that is never stored, but used to fail in various phases.
    const BLOB_NEVER_STORED: &[u8] = BLOBS[11];

    #[test]
    fn abort_after_2pc_phase_one() {
        let mut guard = PROC.lock().expect(IGN_FAILURE);
        let (p, _, _p, _) = &mut *guard;
        let mut peer = p.take().expect(IGN_FAILURE);

        const BLOB: &[u8] = BLOB_NEVER_STORED;
        let key = Key::for_blob(BLOB);
        let consensus_id = next_consensus_id();

        // Concurrent handling of the server.
        let handle = thread::spawn(move || {
            let mut peer_stream = peer.expect_server_conn();
            peer_stream.expect_request_blob(BLOB);
            peer_stream.expect_end();
            peer
        });

        let mut stream = TestStream::connect(*PEER_ADDR, *SERVER_ADDR, &[]);

        // 2PC phase one: adding the blob.
        let request_id = next_request_id();
        stream.write_add_blob_request(request_id, consensus_id, key.clone());
        stream.expect_commit_response(request_id);

        // Instead of 2PC phase two we abort the storing.
        let request_id = next_request_id();
        stream.write_abort_store_blob_request(request_id, consensus_id, key.clone());
        stream.expect_commit_response(request_id);

        stream.close();

        let peer = handle.join().expect("peer server interaction failed");
        *p = Some(peer);
        drop(guard);

        check_blob_not_stored(DB_PORT, BLOB);
    }

    #[test]
    fn blob_already_stored() {
        let mut guard = PROC.lock().expect(IGN_FAILURE);
        let (p, _, _p, _) = &mut *guard;
        let mut peer = p.take().expect(IGN_FAILURE);

        const BLOB: &[u8] = BLOBS[1];
        let key = Key::for_blob(BLOB);

        // Concurrent handling of the server.
        let handle = thread::spawn(move || {
            let mut peer_stream = peer.expect_server_conn();
            peer_stream.expect_request_blob(BLOB);
            peer_stream.expect_end();
            peer
        });

        let mut stream = TestStream::connect(*PEER_ADDR, *SERVER_ADDR, &[]);

        // Properly store the blob.
        let last_modified = {
            let consensus_id = next_consensus_id();

            // 2PC phase one: adding the blob.
            let request_id = next_request_id();
            stream.write_add_blob_request(request_id, consensus_id, key.clone());
            stream.expect_commit_response(request_id);

            // 2PC phase two: commit to storing the blob.
            let request_id = next_request_id();
            stream.write_commit_store_blob_request(request_id, consensus_id, key.clone());
            stream.expect_commit_response(request_id);

            stream.write_store_committed_request(request_id, consensus_id, key.clone());
            // NOTE: we don't expect a response.

            let last_modified = date_header();
            retrieve_blob(DB_PORT, BLOB, &last_modified);
            last_modified
        };

        // Now the test can be begin.
        let consensus_id = next_consensus_id();

        // 2PC phase one: adding the blob.
        let request_id = next_request_id();
        stream.write_add_blob_request(request_id, consensus_id, key.clone());
        // Since the blob is already stored the peer should vote to abort.
        stream.expect_abort_response(request_id);

        // Next we cleanly abort the 2PC query.
        let request_id = next_request_id();
        stream.write_abort_store_blob_request(request_id, consensus_id, key.clone());
        stream.expect_commit_response(request_id);

        stream.close();

        let peer = handle.join().expect("peer server interaction failed");
        *p = Some(peer);
        drop(guard);

        // We expect the blob to be stored with an unmodified timestamp.
        retrieve_blob(DB_PORT, BLOB, &last_modified);
    }

    #[test]
    fn blob_already_stored_after_initial_check() {
        let mut guard = PROC.lock().expect(IGN_FAILURE);
        let (p, _, _p, _) = &mut *guard;
        let mut peer = p.take().expect(IGN_FAILURE);

        const BLOB: &[u8] = BLOBS[2];
        let key = Key::for_blob(BLOB);

        let mut stream = TestStream::connect(*PEER_ADDR, *SERVER_ADDR, &[]);

        let consensus_id_a = next_consensus_id();
        let consensus_id_b = next_consensus_id();

        // Concurrently start two new 2PC queries (A & B), starting with phase
        // one: adding the blob.
        let request_id_a = next_request_id();
        stream.write_add_blob_request(request_id_a, consensus_id_a, key.clone());
        let request_id_b = next_request_id();
        stream.write_add_blob_request(request_id_b, consensus_id_b, key.clone());

        let handle1 = thread::spawn(move || {
            let mut peer_stream = peer.expect_server_conn();
            peer_stream.expect_request_blob(BLOB);
            peer_stream.expect_end();
            peer
        });

        // Finish 2PC query A.
        let last_modified = {
            stream.expect_commit_response(request_id_a);

            // 2PC phase two: commit to storing the blob.
            let request_id = next_request_id();
            stream.write_commit_store_blob_request(request_id, consensus_id_a, key.clone());
            stream.expect_commit_response(request_id);

            stream.write_store_committed_request(request_id, consensus_id_a, key.clone());
            // NOTE: we don't expect a response.

            let last_modified = date_header();
            retrieve_blob(DB_PORT, BLOB, &last_modified);
            last_modified
        };

        // Unblock 2PC query B.
        let mut peer = handle1.join().expect("peer server interaction failed");
        let handle2 = thread::spawn(move || {
            let mut peer_stream = peer.expect_server_conn();
            peer_stream.expect_request_blob(BLOB);
            peer_stream.expect_end();
            peer
        });

        // Now that 2PC query A has completed the peer should vote to abort.
        stream.expect_abort_response(request_id_b);

        // Next we cleanly abort the 2PC query.
        let request_id = next_request_id();
        stream.write_abort_store_blob_request(request_id, consensus_id_b, key.clone());
        stream.expect_commit_response(request_id);

        stream.close();

        let peer = handle2.join().expect("peer server interaction failed");
        *p = Some(peer);
        drop(guard);

        // We expect the blob to be stored with an unmodified timestamp.
        retrieve_blob(DB_PORT, BLOB, &last_modified);
    }

    #[test]
    fn blob_already_stored_in_phase_two() {
        let mut guard = PROC.lock().expect(IGN_FAILURE);
        let (p, _, _p, _) = &mut *guard;
        let mut peer = p.take().expect(IGN_FAILURE);

        const BLOB: &[u8] = BLOBS[3];
        let key = Key::for_blob(BLOB);

        // Concurrent handling of the server. This is for the request that will
        // fail.
        let handle1 = thread::spawn(move || {
            let mut peer_stream = peer.expect_server_conn();
            peer_stream.expect_request_blob(BLOB);
            peer_stream.expect_end();
            peer
        });

        let mut stream = TestStream::connect(*PEER_ADDR, *SERVER_ADDR, &[]);

        let consensus_id = next_consensus_id();

        // 2PC phase one: adding the blob.
        let request_id = next_request_id();
        stream.write_add_blob_request(request_id, consensus_id, key.clone());
        stream.expect_commit_response(request_id);

        // Start a concurrent server to handle the request that will succeed
        // below.
        let mut peer = handle1.join().expect("peer server interaction failed");
        let handle2 = thread::spawn(move || {
            let mut peer_stream = peer.expect_server_conn();
            peer_stream.expect_request_blob(BLOB);
            peer_stream.expect_end();
            peer
        });

        // Run the full procedure to store the blob concurrently with the
        // already running 2PC query (which will fail below).
        let last_modified = {
            let consensus_id = next_consensus_id();

            // 2PC phase one: adding the blob.
            let request_id = next_request_id();
            stream.write_add_blob_request(request_id, consensus_id, key.clone());
            stream.expect_commit_response(request_id);

            // 2PC phase two: commit to storing the blob.
            let request_id = next_request_id();
            stream.write_commit_store_blob_request(request_id, consensus_id, key.clone());
            stream.expect_commit_response(request_id);

            stream.write_store_committed_request(request_id, consensus_id, key.clone());
            // NOTE: we don't expect a response.

            let last_modified = date_header();
            retrieve_blob(DB_PORT, BLOB, &last_modified);
            last_modified
        };

        // 2PC phase two: commit to storing the blob.
        let request_id = next_request_id();
        stream.write_commit_store_blob_request(request_id, consensus_id, key.clone());
        // Since we just store the blob this will have to be aborted.
        stream.expect_abort_response(request_id);

        // Next we cleanly abort the 2PC query.
        let request_id = next_request_id();
        stream.write_abort_store_blob_request(request_id, consensus_id, key.clone());
        stream.expect_commit_response(request_id);

        stream.close();

        let peer = handle2.join().expect("peer server interaction failed");
        *p = Some(peer);
        drop(guard);

        // We expect the blob to be stored with an unmodified timestamp.
        retrieve_blob(DB_PORT, BLOB, &last_modified);
    }

    #[test]
    fn respond_with_invalid_blob() {
        let mut guard = PROC.lock().expect(IGN_FAILURE);
        let (p, _, _p, _) = &mut *guard;
        let mut peer = p.take().expect(IGN_FAILURE);

        const BLOB: &[u8] = BLOB_NEVER_STORED;
        let key = Key::for_blob(BLOB);
        let consensus_id = next_consensus_id();

        // Concurrent handling of the server.
        let handle = thread::spawn(move || {
            let mut peer_stream = peer.expect_server_conn();
            peer_stream.try_expect_request_blob(|key| {
                let expected_key = Key::for_blob(BLOB);
                assert_eq!(*key, expected_key);
                // Respond with an invalid blob.
                (&[], DateTime::INVALID)
            });
            peer_stream.expect_end();
            peer
        });

        let mut stream = TestStream::connect(*PEER_ADDR, *SERVER_ADDR, &[]);

        // 2PC phase one: adding the blob.
        let request_id = next_request_id();
        stream.write_add_blob_request(request_id, consensus_id, key.clone());
        // Since we respond with a removed blob above we expect the peer to
        // fail.
        stream.expect_fail_response(request_id);

        // Next we cleanly abort the 2PC query.
        let request_id = next_request_id();
        stream.write_abort_store_blob_request(request_id, consensus_id, key.clone());
        stream.expect_commit_response(request_id);

        stream.close();

        let peer = handle.join().expect("peer server interaction failed");
        *p = Some(peer);
        drop(guard);

        check_blob_not_stored(DB_PORT, BLOB);
    }

    #[test]
    fn respond_with_removed_blob() {
        let mut guard = PROC.lock().expect(IGN_FAILURE);
        let (p, _, _p, _) = &mut *guard;
        let mut peer = p.take().expect(IGN_FAILURE);

        const BLOB: &[u8] = BLOB_NEVER_STORED;
        let key = Key::for_blob(BLOB);
        let consensus_id = next_consensus_id();

        // Concurrent handling of the server.
        let handle = thread::spawn(move || {
            let mut peer_stream = peer.expect_server_conn();
            peer_stream.try_expect_request_blob(|key| {
                let expected_key = Key::for_blob(BLOB);
                assert_eq!(*key, expected_key);
                // Respond with an invalid blob.
                let timestamp = DateTime::from(SystemTime::now()).mark_removed();
                (BLOB, timestamp)
            });
            peer_stream.expect_end();
            peer
        });

        let mut stream = TestStream::connect(*PEER_ADDR, *SERVER_ADDR, &[]);

        // 2PC phase one: adding the blob.
        let request_id = next_request_id();
        stream.write_add_blob_request(request_id, consensus_id, key.clone());
        // Since we respond with a removed blob above we expect the peer to
        // fail.
        stream.expect_fail_response(request_id);

        // Next we cleanly abort the 2PC query.
        let request_id = next_request_id();
        stream.write_abort_store_blob_request(request_id, consensus_id, key.clone());
        stream.expect_commit_response(request_id);

        stream.close();

        let peer = handle.join().expect("peer server interaction failed");
        *p = Some(peer);
        drop(guard);

        check_blob_not_stored(DB_PORT, BLOB);
    }

    #[test]
    fn respond_with_incorrect_blob() {
        let mut guard = PROC.lock().expect(IGN_FAILURE);
        let (p, _, _p, _) = &mut *guard;
        let mut peer = p.take().expect(IGN_FAILURE);

        const BLOB: &[u8] = BLOB_NEVER_STORED;
        let key = Key::for_blob(BLOB);
        let consensus_id = next_consensus_id();

        // Concurrent handling of the server.
        let handle = thread::spawn(move || {
            let mut peer_stream = peer.expect_server_conn();
            peer_stream.try_expect_request_blob(|key| {
                let expected_key = Key::for_blob(BLOB);
                assert_eq!(*key, expected_key);
                // Respond with an invalid blob.
                let timestamp = DateTime::from(SystemTime::now()).mark_removed();
                (b"PLEASE DON'T USE THIS BLOB ANYWHERE ELSE", timestamp)
            });
            peer_stream.expect_end();
            peer
        });

        let mut stream = TestStream::connect(*PEER_ADDR, *SERVER_ADDR, &[]);

        // 2PC phase one: adding the blob.
        let request_id = next_request_id();
        stream.write_add_blob_request(request_id, consensus_id, key.clone());
        // Since we respond with a removed blob above we expect the peer to
        // fail.
        stream.expect_fail_response(request_id);

        // Next we cleanly abort the 2PC query.
        let request_id = next_request_id();
        stream.write_abort_store_blob_request(request_id, consensus_id, key.clone());
        stream.expect_commit_response(request_id);

        stream.close();

        let peer = handle.join().expect("peer server interaction failed");
        *p = Some(peer);
        drop(guard);

        check_blob_not_stored(DB_PORT, BLOB);
    }

    #[test]
    fn no_response_to_retrieve_blob_request() {
        let mut guard = PROC.lock().expect(IGN_FAILURE);
        let (p, _, _p, _) = &mut *guard;
        let mut peer = p.take().expect(IGN_FAILURE);

        const BLOB: &[u8] = BLOB_NEVER_STORED;
        let key = Key::for_blob(BLOB);
        let consensus_id = next_consensus_id();

        // Expect a `REQUEST_BLOB` request, but don't respond to it.
        let handle = thread::spawn(move || {
            let mut peer_stream = peer.expect_server_conn();

            // Read the `REQUEST_BLOB` request, but don't respond.
            let mut buf = [0; 1];
            let n = peer_stream
                .socket
                .read(&mut buf)
                .expect("failed to read REQUEST_BLOB request");
            assert_eq!(n, 1, "unexpected read length");
            assert_eq!(
                buf[0], REQUEST_BLOB,
                "unexpected request, expected REQUEST_KEYS"
            );

            let mut buf = [0; size_of::<Key>()];
            let n = peer_stream
                .socket
                .read(&mut buf)
                .expect("failed to read Key in REQUEST_BLOB request");
            assert_eq!(n, buf.len(), "unexpected read length");
            let key = Key::new(buf);
            let expected_key = Key::for_blob(BLOB);
            assert_eq!(key, expected_key);

            // Sleep to cause a timeout in peer participant. Keep this
            // `IO_TIMEOUT` + 1 second.
            sleep(Duration::from_secs(6));

            peer
        });

        let mut stream = TestStream::connect(*PEER_ADDR, *SERVER_ADDR, &[]);

        // 2PC phase one: adding the blob.
        let request_id = next_request_id();
        stream.write_add_blob_request(request_id, consensus_id, key.clone());
        // Since we respond with a removed blob above we expect the peer to vote
        // to abort.
        stream.expect_fail_response(request_id);

        // Next we cleanly abort the 2PC query.
        let request_id = next_request_id();
        stream.write_abort_store_blob_request(request_id, consensus_id, key.clone());
        stream.expect_commit_response(request_id);

        stream.close();

        let peer = handle.join().expect("peer server interaction failed");
        *p = Some(peer);
        drop(guard);

        check_blob_not_stored(DB_PORT, BLOB);
    }

    #[test]
    fn no_response_to_retrieve_blob_request_partial_metadata() {
        let mut guard = PROC.lock().expect(IGN_FAILURE);
        let (p, _, _p, _) = &mut *guard;
        let mut peer = p.take().expect(IGN_FAILURE);

        const BLOB: &[u8] = BLOB_NEVER_STORED;
        let key = Key::for_blob(BLOB);
        let consensus_id = next_consensus_id();

        // Expect a `REQUEST_BLOB` request, but don't respond to it.
        let handle = thread::spawn(move || {
            let mut peer_stream = peer.expect_server_conn();

            // Read the `REQUEST_BLOB` request, but don't respond.
            let mut buf = [0; 1];
            let n = peer_stream
                .socket
                .read(&mut buf)
                .expect("failed to read REQUEST_BLOB request");
            assert_eq!(n, 1, "unexpected read length");
            assert_eq!(
                buf[0], REQUEST_BLOB,
                "unexpected request, expected REQUEST_KEYS"
            );

            let mut buf = [0; size_of::<Key>()];
            let n = peer_stream
                .socket
                .read(&mut buf)
                .expect("failed to read Key in REQUEST_BLOB request");
            assert_eq!(n, buf.len(), "unexpected read length");
            let key = Key::new(buf);
            let expected_key = Key::for_blob(BLOB);
            assert_eq!(key, expected_key);

            let timestamp = DateTime::from(SystemTime::now());
            peer_stream
                .socket
                .write_all(timestamp.as_bytes())
                .expect("failed to write timestamp in response to REQUEST_BLOB request");
            let length_bytes = u64::to_be_bytes(BLOB.len() as u64);
            peer_stream
                .socket
                .write_all(&length_bytes[..2])
                .expect("failed to write blob length in response to REQUEST_BLOB request");

            // Sleep to cause a timeout in peer participant. Keep this
            // `IO_TIMEOUT` + 1 second.
            sleep(Duration::from_secs(6));

            peer
        });

        let mut stream = TestStream::connect(*PEER_ADDR, *SERVER_ADDR, &[]);

        // 2PC phase one: adding the blob.
        let request_id = next_request_id();
        stream.write_add_blob_request(request_id, consensus_id, key.clone());
        // Since we respond with a removed blob above we expect the peer to vote
        // to abort.
        stream.expect_fail_response(request_id);

        // Next we cleanly abort the 2PC query.
        let request_id = next_request_id();
        stream.write_abort_store_blob_request(request_id, consensus_id, key.clone());
        stream.expect_commit_response(request_id);

        stream.close();

        let peer = handle.join().expect("peer server interaction failed");
        *p = Some(peer);
        drop(guard);

        check_blob_not_stored(DB_PORT, BLOB);
    }

    #[test]
    fn no_response_to_retrieve_blob_request_partial_blob() {
        let mut guard = PROC.lock().expect(IGN_FAILURE);
        let (p, _, _p, _) = &mut *guard;
        let mut peer = p.take().expect(IGN_FAILURE);

        const BLOB: &[u8] = BLOB_NEVER_STORED;
        let key = Key::for_blob(BLOB);
        let consensus_id = next_consensus_id();

        // Expect a `REQUEST_BLOB` request, but don't respond to it.
        let handle = thread::spawn(move || {
            let mut peer_stream = peer.expect_server_conn();

            // Read the `REQUEST_BLOB` request, but don't respond.
            let mut buf = [0; 1];
            let n = peer_stream
                .socket
                .read(&mut buf)
                .expect("failed to read REQUEST_BLOB request");
            assert_eq!(n, 1, "unexpected read length");
            assert_eq!(
                buf[0], REQUEST_BLOB,
                "unexpected request, expected REQUEST_KEYS"
            );

            let mut buf = [0; size_of::<Key>()];
            let n = peer_stream
                .socket
                .read(&mut buf)
                .expect("failed to read Key in REQUEST_BLOB request");
            assert_eq!(n, buf.len(), "unexpected read length");
            let key = Key::new(buf);
            let expected_key = Key::for_blob(BLOB);
            assert_eq!(key, expected_key);

            let timestamp = DateTime::from(SystemTime::now());
            peer_stream
                .socket
                .write_all(timestamp.as_bytes())
                .expect("failed to write timestamp in response to REQUEST_BLOB request");
            peer_stream
                .socket
                .write_all(&u64::to_be_bytes(BLOB.len() as u64))
                .expect("failed to write blob length in response to REQUEST_BLOB request");
            peer_stream
                .socket
                .write_all(&BLOB[..5])
                .expect("failed to write blob in response to REQUEST_BLOB request");

            // Sleep to cause a timeout in peer participant. Keep this
            // `IO_TIMEOUT` + 1 second.
            sleep(Duration::from_secs(6));

            peer
        });

        let mut stream = TestStream::connect(*PEER_ADDR, *SERVER_ADDR, &[]);

        // 2PC phase one: adding the blob.
        let request_id = next_request_id();
        stream.write_add_blob_request(request_id, consensus_id, key.clone());
        // Since we respond with a removed blob above we expect the peer to vote
        // to abort.
        stream.expect_fail_response(request_id);

        // Next we cleanly abort the 2PC query.
        let request_id = next_request_id();
        stream.write_abort_store_blob_request(request_id, consensus_id, key.clone());
        stream.expect_commit_response(request_id);

        stream.close();

        let peer = handle.join().expect("peer server interaction failed");
        *p = Some(peer);
        drop(guard);

        check_blob_not_stored(DB_PORT, BLOB);
    }

    #[test]
    fn disconnect_after_retrieve_blob_request() {
        let mut guard = PROC.lock().expect(IGN_FAILURE);
        let (p, _, _p, _) = &mut *guard;
        let mut peer = p.take().expect(IGN_FAILURE);

        const BLOB: &[u8] = BLOB_NEVER_STORED;
        let key = Key::for_blob(BLOB);
        let consensus_id = next_consensus_id();

        // Expect a `REQUEST_BLOB` request, but don't respond to it.
        let handle = thread::spawn(move || {
            let mut peer_stream = peer.expect_server_conn();

            // Read the `REQUEST_BLOB` request, but don't respond.
            let mut buf = [0; 1];
            let n = peer_stream
                .socket
                .read(&mut buf)
                .expect("failed to read REQUEST_BLOB request");
            assert_eq!(n, 1, "unexpected read length");
            assert_eq!(
                buf[0], REQUEST_BLOB,
                "unexpected request, expected REQUEST_KEYS"
            );

            let mut buf = [0; size_of::<Key>()];
            let n = peer_stream
                .socket
                .read(&mut buf)
                .expect("failed to read Key in REQUEST_BLOB request");
            assert_eq!(n, buf.len(), "unexpected read length");
            let key = Key::new(buf);
            let expected_key = Key::for_blob(BLOB);
            assert_eq!(key, expected_key);

            // Disconnect the stream.
            drop(peer_stream);

            peer
        });

        let mut stream = TestStream::connect(*PEER_ADDR, *SERVER_ADDR, &[]);

        // 2PC phase one: adding the blob.
        let request_id = next_request_id();
        stream.write_add_blob_request(request_id, consensus_id, key.clone());
        // Since we respond with a removed blob above we expect the peer to vote
        // to abort.
        stream.expect_fail_response(request_id);

        // Next we cleanly abort the 2PC query.
        let request_id = next_request_id();
        stream.write_abort_store_blob_request(request_id, consensus_id, key.clone());
        stream.expect_commit_response(request_id);

        stream.close();

        let peer = handle.join().expect("peer server interaction failed");
        *p = Some(peer);
        drop(guard);

        check_blob_not_stored(DB_PORT, BLOB);
    }

    #[test]
    fn disconnect_after_retrieve_blob_request_partial_metadata() {
        let mut guard = PROC.lock().expect(IGN_FAILURE);
        let (p, _, _p, _) = &mut *guard;
        let mut peer = p.take().expect(IGN_FAILURE);

        const BLOB: &[u8] = BLOB_NEVER_STORED;
        let key = Key::for_blob(BLOB);
        let consensus_id = next_consensus_id();

        // Expect a `REQUEST_BLOB` request, but don't respond to it.
        let handle = thread::spawn(move || {
            let mut peer_stream = peer.expect_server_conn();

            // Read the `REQUEST_BLOB` request, but don't respond.
            let mut buf = [0; 1];
            let n = peer_stream
                .socket
                .read(&mut buf)
                .expect("failed to read REQUEST_BLOB request");
            assert_eq!(n, 1, "unexpected read length");
            assert_eq!(
                buf[0], REQUEST_BLOB,
                "unexpected request, expected REQUEST_KEYS"
            );

            let mut buf = [0; size_of::<Key>()];
            let n = peer_stream
                .socket
                .read(&mut buf)
                .expect("failed to read Key in REQUEST_BLOB request");
            assert_eq!(n, buf.len(), "unexpected read length");
            let key = Key::new(buf);
            let expected_key = Key::for_blob(BLOB);
            assert_eq!(key, expected_key);

            let timestamp = DateTime::from(SystemTime::now());
            peer_stream
                .socket
                .write_all(timestamp.as_bytes())
                .expect("failed to write timestamp in response to REQUEST_BLOB request");
            let length_bytes = u64::to_be_bytes(BLOB.len() as u64);
            peer_stream
                .socket
                .write_all(&length_bytes[..2])
                .expect("failed to write blob length in response to REQUEST_BLOB request");

            // Disconnect the stream.
            drop(peer_stream);

            peer
        });

        let mut stream = TestStream::connect(*PEER_ADDR, *SERVER_ADDR, &[]);

        // 2PC phase one: adding the blob.
        let request_id = next_request_id();
        stream.write_add_blob_request(request_id, consensus_id, key.clone());
        // Since we respond with a removed blob above we expect the peer to vote
        // to abort.
        stream.expect_fail_response(request_id);

        // Next we cleanly abort the 2PC query.
        let request_id = next_request_id();
        stream.write_abort_store_blob_request(request_id, consensus_id, key.clone());
        stream.expect_commit_response(request_id);

        stream.close();

        let peer = handle.join().expect("peer server interaction failed");
        *p = Some(peer);
        drop(guard);

        check_blob_not_stored(DB_PORT, BLOB);
    }

    #[test]
    fn disconnect_after_retrieve_blob_request_partial_blob() {
        let mut guard = PROC.lock().expect(IGN_FAILURE);
        let (p, _, _p, _) = &mut *guard;
        let mut peer = p.take().expect(IGN_FAILURE);

        const BLOB: &[u8] = BLOB_NEVER_STORED;
        let key = Key::for_blob(BLOB);
        let consensus_id = next_consensus_id();

        // Expect a `REQUEST_BLOB` request, but don't respond to it.
        let handle = thread::spawn(move || {
            let mut peer_stream = peer.expect_server_conn();

            // Read the `REQUEST_BLOB` request, but don't respond.
            let mut buf = [0; 1];
            let n = peer_stream
                .socket
                .read(&mut buf)
                .expect("failed to read REQUEST_BLOB request");
            assert_eq!(n, 1, "unexpected read length");
            assert_eq!(
                buf[0], REQUEST_BLOB,
                "unexpected request, expected REQUEST_KEYS"
            );

            let mut buf = [0; size_of::<Key>()];
            let n = peer_stream
                .socket
                .read(&mut buf)
                .expect("failed to read Key in REQUEST_BLOB request");
            assert_eq!(n, buf.len(), "unexpected read length");
            let key = Key::new(buf);
            let expected_key = Key::for_blob(BLOB);
            assert_eq!(key, expected_key);

            let timestamp = DateTime::from(SystemTime::now());
            peer_stream
                .socket
                .write_all(timestamp.as_bytes())
                .expect("failed to write timestamp in response to REQUEST_BLOB request");
            peer_stream
                .socket
                .write_all(&u64::to_be_bytes(BLOB.len() as u64))
                .expect("failed to write blob length in response to REQUEST_BLOB request");
            peer_stream
                .socket
                .write_all(&BLOB[..5])
                .expect("failed to write blob in response to REQUEST_BLOB request");

            // Disconnect the stream.
            drop(peer_stream);

            peer
        });

        let mut stream = TestStream::connect(*PEER_ADDR, *SERVER_ADDR, &[]);

        // 2PC phase one: adding the blob.
        let request_id = next_request_id();
        stream.write_add_blob_request(request_id, consensus_id, key.clone());
        // Since we respond with a removed blob above we expect the peer to vote
        // to abort.
        stream.expect_fail_response(request_id);

        // Next we cleanly abort the 2PC query.
        let request_id = next_request_id();
        stream.write_abort_store_blob_request(request_id, consensus_id, key.clone());
        stream.expect_commit_response(request_id);

        stream.close();

        let peer = handle.join().expect("peer server interaction failed");
        *p = Some(peer);
        drop(guard);

        check_blob_not_stored(DB_PORT, BLOB);
    }

    #[test]
    #[ignore = "this causes other tests to fail"]
    fn dont_accept_connection_for_retrieve_blob_request() {
        let mut guard = PROC.lock().expect(IGN_FAILURE);
        let (_, _, _p, _) = &mut *guard;

        const BLOB: &[u8] = BLOB_NEVER_STORED;
        let key = Key::for_blob(BLOB);
        let consensus_id = next_consensus_id();

        // NOTE: we're not starting a server to respond to retrieve blob
        // requests from the peer.

        let mut stream = TestStream::connect(*PEER_ADDR, *SERVER_ADDR, &[]);

        // 2PC phase one: adding the blob.
        let request_id = next_request_id();
        stream.write_add_blob_request(request_id, consensus_id, key.clone());
        // Since we respond with a removed blob above we expect the peer to vote
        // to abort.
        stream.expect_fail_response(request_id);

        // Next we cleanly abort the 2PC query.
        let request_id = next_request_id();
        stream.write_abort_store_blob_request(request_id, consensus_id, key.clone());
        stream.expect_commit_response(request_id);

        stream.close();

        drop(guard);

        check_blob_not_stored(DB_PORT, BLOB);
    }

    #[test]
    fn send_phase_two_commit_message() {
        let mut guard = PROC.lock().expect(IGN_FAILURE);
        let (_, _, _p, _) = &mut *guard;

        const BLOB: &[u8] = BLOB_NEVER_STORED;
        let key = Key::for_blob(BLOB);
        let consensus_id = next_consensus_id();

        let mut stream = TestStream::connect(*PEER_ADDR, *SERVER_ADDR, &[]);

        // We'll start with phase two for a change.
        // 2PC phase two: commit to storing the blob.
        let request_id = next_request_id();
        stream.write_commit_store_blob_request(request_id, consensus_id, key);
        stream.expect_fail_response(request_id);

        stream.close();

        drop(guard);
    }

    #[test]
    fn send_phase_two_abort_message() {
        let mut guard = PROC.lock().expect(IGN_FAILURE);
        let (_, _, _p, _) = &mut *guard;

        const BLOB: &[u8] = BLOB_NEVER_STORED;
        let key = Key::for_blob(BLOB);
        let consensus_id = next_consensus_id();

        let mut stream = TestStream::connect(*PEER_ADDR, *SERVER_ADDR, &[]);

        // Send a phase two abort message out of the blue.
        let request_id = next_request_id();
        stream.write_abort_store_blob_request(request_id, consensus_id, key);
        stream.expect_fail_response(request_id);

        stream.close();

        drop(guard);
    }

    #[test]
    fn send_committed_store_message() {
        let mut guard = PROC.lock().expect(IGN_FAILURE);
        let (_, _, _p, _) = &mut *guard;

        const BLOB: &[u8] = BLOB_NEVER_STORED;
        let key = Key::for_blob(BLOB);
        let consensus_id = next_consensus_id();

        let mut stream = TestStream::connect(*PEER_ADDR, *SERVER_ADDR, &[]);

        // Send a phase two abort message out of the blue.
        let request_id = next_request_id();
        stream.write_store_committed_request(request_id, consensus_id, key);

        // NOTE: we don't expect a response.

        stream.close();

        drop(guard);
    }
}
