use std::sync::Mutex;
use std::thread::{self, sleep};
use std::time::{Duration, SystemTime};

use http::header::{CONNECTION, CONTENT_LENGTH, CONTENT_TYPE};
use http::status::StatusCode;
use lazy_static::lazy_static;
use log::LevelFilter;
use stored::peer::{ConsensusVote, Operation};
use stored::Key;

use super::{remove_blob, Dispatcher, TestPeer, TestStream, BLOBS, BLOB_NEVER_STORED, IGN_FAILURE};
use crate::util::http::{body, header};
use crate::util::Proc;

const DB_PORT: u16 = 13020;
const DB_PATH: &str = "/tmp/stored/mocked_remove_blob.db";
const CONF_PATH: &str = "tests/config/mocked_remove_blob.toml";
// The `fail_*` test generate a lot of warnings (on purpose), we don't want that
// in our output.
const FILTER: LevelFilter = LevelFilter::Off;

start_stored_fn!(&[CONF_PATH], &[DB_PATH], FILTER);

lazy_static! {
    /// Shared process for the tests.
    static ref PROC: Mutex<(TestPeer, Option<TestStream<Dispatcher>>, Proc<'static>, Vec<Key>)> = {
        let addr = "127.0.0.1:13121".parse().unwrap();
        let mut peer = TestPeer::bind(addr).unwrap();

        let process = start_stored();

        // Accept a peer connection and set it up.
        let peer_stream = peer.expect_participant_conn("127.0.0.1:13120".parse().unwrap(), &[]);
        // Run the full synchronisation protocol.
        let keys = BLOBS.iter().copied().map(Key::for_blob).collect::<Vec<_>>();
        peer.expect_full_sync(&*keys, BLOBS);

        // Give the HTTP some time to startup.
        sleep(Duration::from_millis(500));

        Mutex::new((peer, Some(peer_stream), process, keys))
    };
}

#[test]
fn successfull_remove() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (_, peer_stream, _p, _) = &mut *guard;
    let peer_stream = peer_stream.as_mut().expect(IGN_FAILURE);

    const BLOB: &[u8] = &BLOBS[0];
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle = thread::spawn(|| remove_blob(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.
    let consensus_id =
        peer_stream.expect_remove_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    peer_stream.expect_commit_remove_blob_request(
        &key,
        consensus_id,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_blob_remove_committed_request(&key, consensus_id);

    handle.join().expect("failed to remove blob");

    // Storing the same blob shouldn't involve the peer.
    // NOTE: if it would involve the peer the operation would time out as we
    // don't respond to any requests.
    remove_blob(DB_PORT, BLOB);
}

#[test]
fn fail_2pc_phase_one_vote_fail() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (_, peer_stream, _p, _) = &mut *guard;
    let peer_stream = peer_stream.as_mut().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOBS[1];
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle = thread::spawn(|| remove_blob(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.
    let consensus_id_failed = peer_stream.expect_remove_blob_request(&key, ConsensusVote::Fail);
    // Expect another 2PC query.
    let consensus_id =
        peer_stream.expect_remove_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    assert!(consensus_id_failed < consensus_id);
    // Abort the old 2PC query.
    peer_stream.expect_abort_remove_blob_request(
        &key,
        consensus_id_failed,
        ConsensusVote::Commit(SystemTime::now()),
    );
    // Commit to the new 2PC query.
    peer_stream.expect_commit_remove_blob_request(
        &key,
        consensus_id,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_blob_remove_committed_request(&key, consensus_id);

    handle.join().expect("failed to remove blob");
}

#[test]
#[ignore]
fn fail_2pc_phase_one_vote_fail_no_response_timeout() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (_, peer_stream, _p, _) = &mut *guard;
    let peer_stream = peer_stream.as_mut().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOBS[2];
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle = thread::spawn(|| remove_blob(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.
    let request = peer_stream
        .read_request()
        .expect("failed to handle add blob request");
    assert_eq!(request.key, key);
    assert!(matches!(request.op, Operation::RemoveBlob));
    let consensus_id_failed = request.consensus_id;

    // NOTE: we don't respond here. The consensus algorithm should hit a
    // timeout.

    // Expect another 2PC query.
    let consensus_id =
        peer_stream.expect_remove_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    assert!(consensus_id_failed < consensus_id);
    // Abort the old 2PC query.
    peer_stream.expect_abort_remove_blob_request(
        &key,
        consensus_id_failed,
        ConsensusVote::Commit(SystemTime::now()),
    );
    // Commit to the new 2PC query.
    peer_stream.expect_commit_remove_blob_request(
        &key,
        consensus_id,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_blob_remove_committed_request(&key, consensus_id);

    handle.join().expect("failed to remove blob");
}

#[test]
fn fail_2pc_phase_one_vote_fail_disconnect() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (peer, p_stream, _p, keys) = &mut *guard;
    let mut peer_stream = p_stream.take().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOBS[3];
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle = thread::spawn(|| remove_blob(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.
    let request = peer_stream
        .read_request()
        .expect("failed to handle remove blob request");
    assert_eq!(request.key, key);
    assert!(matches!(request.op, Operation::RemoveBlob));
    let consensus_id_failed = request.consensus_id;

    // NOTE: we disconnect here, not sending a response. The consensus algorithm
    // should hit a timeout.
    drop(peer_stream);

    // The peer should try to reconnect.
    let mut peer_stream = peer.expect_participant_conn("127.0.0.1:13120".parse().unwrap(), &[]);
    // Running a peer sync as well.
    peer.expect_peer_sync(keys, &[]);

    // Expect another 2PC query.
    let consensus_id =
        peer_stream.expect_remove_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    assert!(consensus_id_failed < consensus_id);
    // Abort the old 2PC query.
    peer_stream.expect_abort_remove_blob_request(
        &key,
        consensus_id_failed,
        ConsensusVote::Commit(SystemTime::now()),
    );
    // Commit to the new 2PC query.
    peer_stream.expect_commit_remove_blob_request(
        &key,
        consensus_id,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_blob_remove_committed_request(&key, consensus_id);

    handle.join().expect("failed to remove blob");

    *p_stream = Some(peer_stream);
}

#[test]
fn fail_2pc_phase_one_vote_abort() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (_, peer_stream, _p, _) = &mut *guard;
    let peer_stream = peer_stream.as_mut().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOBS[4];
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle = thread::spawn(|| remove_blob(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.
    let consensus_id_failed = peer_stream.expect_remove_blob_request(&key, ConsensusVote::Abort);
    // Expect another 2PC query.
    let consensus_id =
        peer_stream.expect_remove_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    assert!(consensus_id_failed < consensus_id);
    // Abort the old 2PC query.
    peer_stream.expect_abort_remove_blob_request(
        &key,
        consensus_id_failed,
        ConsensusVote::Commit(SystemTime::now()),
    );
    // Commit to the new 2PC query.
    peer_stream.expect_commit_remove_blob_request(
        &key,
        consensus_id,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_blob_remove_committed_request(&key, consensus_id);

    handle.join().expect("failed to remove blob");
}

#[test]
fn fail_2pc_phase_one_vote_abort_already_stored() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (_, peer_stream, _p, _) = &mut *guard;
    let peer_stream = peer_stream.as_mut().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOBS[5];
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle1 = thread::spawn(|| remove_blob(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.

    // Expect the AddBlob request, but don't yet respond to it.
    let request = peer_stream
        .read_request()
        .expect("failed to read RemoveBlob request");
    assert_eq!(request.key, key);
    assert!(matches!(request.op, Operation::RemoveBlob));
    let consensus_id_failed = request.consensus_id;

    // Start another concurrent request to remove the same blob.
    let handle2 = thread::spawn(|| remove_blob(DB_PORT, BLOB));

    // Expect another 2PC query.
    let consensus_id =
        peer_stream.expect_remove_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    assert!(consensus_id_failed < consensus_id);
    // Commit to the new 2PC query.
    peer_stream.expect_commit_remove_blob_request(
        &key,
        consensus_id,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_blob_remove_committed_request(&key, consensus_id);

    // Respond to the initial RemoveBlob request.
    peer_stream
        .write_response(request.id, ConsensusVote::Abort)
        .expect("failed to write response to RemoveBlob request");
    // Abort the old 2PC query.
    peer_stream.expect_abort_remove_blob_request(
        &key,
        consensus_id_failed,
        ConsensusVote::Commit(SystemTime::now()),
    );

    handle1.join().expect("failed to remove blob");
    handle2.join().expect("failed to remove blob");
}

#[test]
fn fail_2pc_phase_two_vote_fail() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (_, peer_stream, _p, _) = &mut *guard;
    let peer_stream = peer_stream.as_mut().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOBS[6];
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle = thread::spawn(|| remove_blob(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.
    let consensus_id_failed =
        peer_stream.expect_remove_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    // Vote to fail the 2PC query.
    peer_stream.expect_commit_remove_blob_request(&key, consensus_id_failed, ConsensusVote::Fail);

    // Expect another 2PC query.
    let consensus_id =
        peer_stream.expect_remove_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    assert!(consensus_id_failed < consensus_id);
    // Abort the old 2PC query.
    peer_stream.expect_abort_remove_blob_request(
        &key,
        consensus_id_failed,
        ConsensusVote::Commit(SystemTime::now()),
    );
    // Commit to the new 2PC query.
    peer_stream.expect_commit_remove_blob_request(
        &key,
        consensus_id,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_blob_remove_committed_request(&key, consensus_id);

    handle.join().expect("failed to remove blob");
}

#[test]
fn fail_2pc_phase_two_vote_fail_no_response_timeout() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (_, peer_stream, _p, _) = &mut *guard;
    let peer_stream = peer_stream.as_mut().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOBS[7];
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle = thread::spawn(|| remove_blob(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.
    let consensus_id_failed =
        peer_stream.expect_remove_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    let request = peer_stream
        .read_request()
        .expect("failed to handle commit remove blob request");
    assert_eq!(request.key, key);
    assert!(matches!(request.op, Operation::CommitRemoveBlob(..)));
    assert_eq!(request.consensus_id, consensus_id_failed);

    // NOTE: we don't respond here. The consensus algorithm should hit a
    // timeout.

    // Expect another 2PC query.
    let consensus_id =
        peer_stream.expect_remove_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    assert!(consensus_id_failed < consensus_id);
    // Abort the old 2PC query.
    peer_stream.expect_abort_remove_blob_request(
        &key,
        consensus_id_failed,
        ConsensusVote::Commit(SystemTime::now()),
    );
    // Commit to the new 2PC query.
    peer_stream.expect_commit_remove_blob_request(
        &key,
        consensus_id,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_blob_remove_committed_request(&key, consensus_id);

    handle.join().expect("failed to remove blob");
}

#[test]
fn fail_2pc_phase_two_vote_fail_disconnect() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (peer, p_stream, _p, keys) = &mut *guard;
    let mut peer_stream = p_stream.take().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOBS[8];
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle = thread::spawn(|| remove_blob(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.
    let consensus_id_failed =
        peer_stream.expect_remove_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    let request = peer_stream
        .read_request()
        .expect("failed to handle commit remove blob request");
    assert_eq!(request.key, key);
    assert!(matches!(request.op, Operation::CommitRemoveBlob(..)));
    assert_eq!(request.consensus_id, consensus_id_failed);

    // NOTE: we disconnect here, not sending a response. The consensus algorithm
    // should hit a timeout.
    drop(peer_stream);

    // The peer should try to reconnect.
    let mut peer_stream = peer.expect_participant_conn("127.0.0.1:13120".parse().unwrap(), &[]);
    // Running a peer sync as well.
    peer.expect_peer_sync(keys, &[]);

    // Expect another 2PC query.
    let consensus_id =
        peer_stream.expect_remove_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    assert!(consensus_id_failed < consensus_id);
    // Abort the old 2PC query.
    peer_stream.expect_abort_remove_blob_request(
        &key,
        consensus_id_failed,
        ConsensusVote::Commit(SystemTime::now()),
    );
    // Commit to the new 2PC query.
    peer_stream.expect_commit_remove_blob_request(
        &key,
        consensus_id,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_blob_remove_committed_request(&key, consensus_id);

    handle.join().expect("failed to remove blob");

    *p_stream = Some(peer_stream);
}

#[test]
fn fail_2pc_phase_two_vote_abort() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (_, peer_stream, _p, _) = &mut *guard;
    let peer_stream = peer_stream.as_mut().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOBS[9];
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle = thread::spawn(|| remove_blob(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.
    let consensus_id_failed =
        peer_stream.expect_remove_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    // Vote to fail the 2PC query.
    peer_stream.expect_commit_remove_blob_request(&key, consensus_id_failed, ConsensusVote::Abort);

    // Expect another 2PC query.
    let consensus_id =
        peer_stream.expect_remove_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    assert!(consensus_id_failed < consensus_id);
    // Abort the old 2PC query.
    peer_stream.expect_abort_remove_blob_request(
        &key,
        consensus_id_failed,
        ConsensusVote::Commit(SystemTime::now()),
    );
    // Commit to the new 2PC query.
    peer_stream.expect_commit_remove_blob_request(
        &key,
        consensus_id,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_blob_remove_committed_request(&key, consensus_id);

    handle.join().expect("failed to remove blob");
}

#[test]
fn fail_2pc_phase_two_vote_abort_already_stored() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (_, peer_stream, _p, _) = &mut *guard;
    let peer_stream = peer_stream.as_mut().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOBS[10];
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle1 = thread::spawn(|| remove_blob(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.
    let consensus_id_failed =
        peer_stream.expect_remove_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    // Expect the CommitRemoveBlob request, but don't yet respond to it.
    let request = peer_stream
        .read_request()
        .expect("failed to handle commit remove blob request");
    assert_eq!(request.key, key);
    assert!(matches!(request.op, Operation::CommitRemoveBlob(..)));
    assert_eq!(request.consensus_id, consensus_id_failed);

    // Start another concurrent request to remove the same blob.
    let handle2 = thread::spawn(|| remove_blob(DB_PORT, BLOB));

    // Expect another 2PC query.
    let consensus_id =
        peer_stream.expect_remove_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    assert!(consensus_id_failed < consensus_id);
    // Commit to the new 2PC query.
    peer_stream.expect_commit_remove_blob_request(
        &key,
        consensus_id,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_blob_remove_committed_request(&key, consensus_id);

    // Respond to the initial AddBlob request.
    peer_stream
        .write_response(request.id, ConsensusVote::Abort)
        .expect("failed to write response to CommitStoreBlob request");
    // Abort the old 2PC query.
    peer_stream.expect_abort_remove_blob_request(
        &key,
        consensus_id_failed,
        ConsensusVote::Commit(SystemTime::now()),
    );

    handle1.join().expect("failed to remove blob");
    handle2.join().expect("failed to remove blob");
}

#[test]
fn fail_2pc_completely_phase_one_vote_fail() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (_, peer_stream, _p, _) = &mut *guard;
    let peer_stream = peer_stream.as_mut().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOB_NEVER_STORED;
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle = thread::spawn(|| expect_remove_blob_failure(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.
    let consensus_id1 = peer_stream.expect_remove_blob_request(&key, ConsensusVote::Fail);

    // Expect another attempt, aborting the old one.
    let consensus_id2 = peer_stream.expect_remove_blob_request(&key, ConsensusVote::Fail);
    assert!(consensus_id1 < consensus_id2);
    peer_stream.expect_abort_remove_blob_request(
        &key,
        consensus_id1,
        ConsensusVote::Commit(SystemTime::now()),
    );

    // Expect a third (and final) attempt, aborting both.
    let consensus_id3 = peer_stream.expect_remove_blob_request(&key, ConsensusVote::Fail);
    assert!(consensus_id2 < consensus_id3);
    peer_stream.expect_abort_remove_blob_request(
        &key,
        consensus_id2,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_abort_remove_blob_request(
        &key,
        consensus_id3,
        ConsensusVote::Commit(SystemTime::now()),
    );

    handle.join().expect("failed to remove blob");
}

#[test]
fn fail_2pc_completely_phase_one_vote_abort() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (_, peer_stream, _p, _) = &mut *guard;
    let peer_stream = peer_stream.as_mut().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOB_NEVER_STORED;
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle = thread::spawn(|| expect_remove_blob_failure(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.
    let consensus_id1 = peer_stream.expect_remove_blob_request(&key, ConsensusVote::Abort);

    // Expect another attempt, aborting the old one.
    let consensus_id2 = peer_stream.expect_remove_blob_request(&key, ConsensusVote::Abort);
    assert!(consensus_id1 < consensus_id2);
    peer_stream.expect_abort_remove_blob_request(
        &key,
        consensus_id1,
        ConsensusVote::Commit(SystemTime::now()),
    );

    // Expect a third (and final) attempt, aborting both.
    let consensus_id3 = peer_stream.expect_remove_blob_request(&key, ConsensusVote::Abort);
    assert!(consensus_id2 < consensus_id3);
    peer_stream.expect_abort_remove_blob_request(
        &key,
        consensus_id2,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_abort_remove_blob_request(
        &key,
        consensus_id3,
        ConsensusVote::Commit(SystemTime::now()),
    );

    handle.join().expect("failed to remove blob");
}

#[test]
fn fail_2pc_completely_phase_two_vote_fail() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (_, peer_stream, _p, _) = &mut *guard;
    let peer_stream = peer_stream.as_mut().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOB_NEVER_STORED;
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle = thread::spawn(|| expect_remove_blob_failure(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.
    let consensus_id1 =
        peer_stream.expect_remove_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    // Vote to fail the 2PC query.
    peer_stream.expect_commit_remove_blob_request(&key, consensus_id1, ConsensusVote::Fail);

    // Expect another attempt, aborting the old one.
    let consensus_id2 =
        peer_stream.expect_remove_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    assert!(consensus_id1 < consensus_id2);
    peer_stream.expect_abort_remove_blob_request(
        &key,
        consensus_id1,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_commit_remove_blob_request(&key, consensus_id2, ConsensusVote::Fail);

    // Expect a third (and final) attempt, aborting both.
    let consensus_id3 =
        peer_stream.expect_remove_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    assert!(consensus_id2 < consensus_id3);
    peer_stream.expect_abort_remove_blob_request(
        &key,
        consensus_id2,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_commit_remove_blob_request(&key, consensus_id3, ConsensusVote::Fail);
    peer_stream.expect_abort_remove_blob_request(
        &key,
        consensus_id3,
        ConsensusVote::Commit(SystemTime::now()),
    );

    handle.join().expect("failed to remove blob");
}

#[test]
#[ignore]
fn fail_2pc_completely_phase_two_vote_abort() {
    let mut guard = PROC.lock().expect(IGN_FAILURE);
    let (_, peer_stream, _p, _) = &mut *guard;
    let peer_stream = peer_stream.as_mut().expect(IGN_FAILURE);

    const BLOB: &[u8] = BLOB_NEVER_STORED;
    let key = Key::for_blob(BLOB);

    // Store a blob, while concurrently doing the peer interaction.
    let handle = thread::spawn(|| expect_remove_blob_failure(DB_PORT, BLOB));

    // Fake the peer interaction for storing the blob.
    let consensus_id1 =
        peer_stream.expect_remove_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    // Vote to fail the 2PC query.
    peer_stream.expect_commit_remove_blob_request(&key, consensus_id1, ConsensusVote::Abort);

    // Expect another attempt, aborting the old one.
    let consensus_id2 =
        peer_stream.expect_remove_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    assert!(consensus_id1 < consensus_id2);
    peer_stream.expect_abort_remove_blob_request(
        &key,
        consensus_id1,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_commit_remove_blob_request(&key, consensus_id2, ConsensusVote::Abort);

    // Expect a third (and final) attempt, aborting both.
    let consensus_id3 =
        peer_stream.expect_remove_blob_request(&key, ConsensusVote::Commit(SystemTime::now()));
    assert!(consensus_id2 < consensus_id3);
    peer_stream.expect_abort_remove_blob_request(
        &key,
        consensus_id2,
        ConsensusVote::Commit(SystemTime::now()),
    );
    peer_stream.expect_commit_remove_blob_request(&key, consensus_id3, ConsensusVote::Abort);
    peer_stream.expect_abort_remove_blob_request(
        &key,
        consensus_id3,
        ConsensusVote::Commit(SystemTime::now()),
    );

    handle.join().expect("failed to remove blob");
}

/// Remove `blob` on a server running on localhost `port`, expecting to fail.
#[track_caller]
fn expect_remove_blob_failure(port: u16, blob: &[u8]) {
    let key = Key::for_blob(blob);
    let url = format!("/blob/{}", key);
    request!(
        DELETE port, url, body::EMPTY,
        expected: StatusCode::INTERNAL_SERVER_ERROR, body::SERVER_ERROR,
        CONTENT_TYPE => header::PLAIN_TEXT,
        CONTENT_LENGTH => body::SERVER_ERROR_LEN,
        CONNECTION => header::KEEP_ALIVE,
    );
}
