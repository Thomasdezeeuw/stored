use std::io::{self, Read, Write};
use std::marker::PhantomData;
use std::mem::size_of;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::str::from_utf8;
use std::time::{Duration, SystemTime};

use http::header::{CONNECTION, CONTENT_LENGTH, LAST_MODIFIED, LOCATION};
use http::status::StatusCode;
use serde_json;
use stored::peer::server::{REQUEST_BLOB, REQUEST_KEYS, REQUEST_KEYS_SINCE, STORE_BLOB};
use stored::peer::{
    ConsensusId, ConsensusVote, Operation, Request, Response, COORDINATOR_MAGIC, PARTICIPANT_MAGIC,
};
use stored::storage::DateTime;
use stored::Key;

use crate::util::http::{body, date_header, header};

mod store_blob;
mod sync;

struct TestPeer {
    socket: TcpListener,
}

impl TestPeer {
    fn bind(addr: SocketAddr) -> io::Result<TestPeer> {
        TcpListener::bind(addr).map(|socket| TestPeer { socket })
    }

    fn accept<A>(&mut self) -> io::Result<(TestStream<A>, SocketAddr)> {
        // TODO: add timeout.
        self.socket
            .accept()
            .and_then(|(socket, addr)| TestStream::new(socket).map(|stream| (stream, addr)))
    }

    /// Accept a participant connection.
    #[track_caller]
    fn expect_participant_conn(
        &mut self,
        expect_server_address: SocketAddr,
        peers: &[SocketAddr],
    ) -> TestStream<Dispatcher> {
        let (mut peer_stream, _) = self.accept().expect("failed to accept peer connection");
        peer_stream.expect_participant_magic();
        let server_addr = peer_stream
            .read_server_addr()
            .expect("failed to read peer server address");
        assert_eq!(
            server_addr, expect_server_address,
            "unexpected server address"
        );
        peer_stream
            .write_peers(peers)
            .expect("failed to write known peers");
        peer_stream
    }

    /// Expect a stream running a full synchronisation.
    #[track_caller]
    fn expect_full_sync(&mut self, keys: &[Key]) {
        let (mut sync_stream, _) = self.accept().expect("failed to accept peer connection");
        sync_stream.expect_coordinator_magic();
        sync_stream.expect_request_keys(keys);
        sync_stream.expect_end();
    }

    /// Expect a stream that wants to run peer synchronisation. Returns a
    /// response with `keys`, expecting both peers to be fully synced.
    #[track_caller]
    fn expect_peer_sync(&mut self, keys: &[Key]) {
        let (mut sync_stream, _) = self.accept().expect("failed to accept peer connection");
        sync_stream.expect_coordinator_magic();
        sync_stream.expect_request_keys_since(keys);
        sync_stream.expect_end();
    }
}

/// Stream accepted from [`TestPeer`]. The stream can acts as a coordinator
/// [`Server`] or participant [`Dispatcher`].
#[derive(Debug)]
struct TestStream<A> {
    socket: TcpStream,
    actor: PhantomData<A>,
}

/// The following functions can be used to setup the connection correctly.
impl<A> TestStream<A> {
    fn new(socket: TcpStream) -> io::Result<TestStream<A>> {
        let timeout = Some(Duration::from_secs(10));
        socket.set_read_timeout(timeout)?;
        socket.set_write_timeout(timeout)?;
        Ok(TestStream {
            socket,
            actor: PhantomData,
        })
    }

    /// Expect the stream to be closed.
    #[track_caller]
    fn expect_end(mut self) {
        let mut buf = [0; 10];
        match self.socket.read(&mut buf) {
            Ok(n) => assert_eq!(n, 0, "unexpected read: {:?}", &buf[..n]),
            // FIXME: something on macOS this returns a connection reset error. Find
            // out why and fix it and then remove this.
            Err(ref err) if err.kind() == std::io::ErrorKind::ConnectionReset => {}
            Err(err) => panic!(
                "unexpected error: expected to stream to be closed after the syncing process is done: {}",
                err
            ),
        }
    }
}

/// Acting as `peer::server::actor`.
enum Server {}

/// Connection acting as `peer::server::actor`.
impl TestStream<Server> {
    /// Returns `true` if it could read `COORDINATOR_MAGIC` from the connection.
    #[track_caller]
    fn expect_coordinator_magic(&mut self) {
        let mut buf = [0; COORDINATOR_MAGIC.len()];
        self.socket
            .read_exact(&mut buf)
            .expect("failed to read COORDINATOR_MAGIC");
        if buf == PARTICIPANT_MAGIC {
            panic!("unexpected PARTICIPANT_MAGIC, expected COORDINATOR_MAGIC");
        } else {
            assert_eq!(buf, COORDINATOR_MAGIC, "unexpected bytes");
        }
    }

    /// Expect a `REQUEST_BLOB` request and write `keys` as response.
    #[track_caller]
    fn expect_request_blob(&mut self, expected_blob: &[u8]) {
        let mut buf = [0; 1];
        let n = self
            .socket
            .read(&mut buf)
            .expect("failed to read REQUEST_BLOB request");
        assert_eq!(n, 1, "unexpected read length");
        assert_eq!(
            buf[0], REQUEST_BLOB,
            "unexpected request, expected REQUEST_KEYS"
        );

        let mut buf = [0; size_of::<Key>()];
        let n = self
            .socket
            .read(&mut buf)
            .expect("failed to read Key in REQUEST_BLOB request");
        assert_eq!(n, buf.len(), "unexpected read length");
        let key = Key::new(buf);
        let expected_key = Key::for_blob(expected_blob);
        assert_eq!(key, expected_key, "read unexpected key");

        let timestamp = DateTime::from(SystemTime::now());
        self.socket
            .write_all(timestamp.as_bytes())
            .expect("failed to write timestamp in response to REQUEST_BLOB request");
        self.socket
            .write_all(&u64::to_be_bytes(expected_blob.len() as u64))
            .expect("failed to write blob length in response to REQUEST_BLOB request");
        self.socket
            .write_all(expected_blob)
            .expect("failed to write blob in response to REQUEST_BLOB request");
    }

    /// Expect a `REQUEST_KEYS` request and write `keys` as response.
    #[track_caller]
    fn expect_request_keys(&mut self, keys: &[Key]) {
        let mut buf = [0; 1];
        let n = self
            .socket
            .read(&mut buf)
            .expect("failed to read REQUEST_BLOB request");
        assert_eq!(n, 1, "unexpected read length");
        assert_eq!(
            buf[0], REQUEST_KEYS,
            "unexpected request, expected REQUEST_KEYS"
        );

        let length = keys.len();
        self.socket
            .write_all(&u64::to_be_bytes(length as u64))
            .expect("failed to write key set length to REQUEST_KEYS request");

        for key in keys {
            self.socket
                .write_all(key.as_bytes())
                .expect("failed to write key to REQUEST_KEYS request");
        }
    }

    /// Expect a `REQUEST_KEYS_SINCE` request and write `keys` as response.
    #[track_caller]
    fn expect_request_keys_since(&mut self, keys: &[Key]) {
        let mut buf = [0; 1];
        let n = self
            .socket
            .read(&mut buf)
            .expect("failed to read REQUEST_KEYS_SINCE request");
        assert_eq!(n, 1, "unexpected read length");
        assert_eq!(
            buf[0], REQUEST_KEYS_SINCE,
            "unexpected request, expected REQUEST_KEYS_SINCE"
        );

        let mut buf = [0; size_of::<DateTime>()];
        let n = self
            .socket
            .read(&mut buf)
            .expect("failed to read timestamp in REQUEST_KEYS_SINCE request");
        assert_eq!(n, buf.len(), "unexpected read length");
        let since = DateTime::from_bytes(&buf[..n]).unwrap_or(DateTime::INVALID);
        assert!(!since.is_invalid(), "timestamp is invalid");

        let length = keys.len();
        self.socket
            .write_all(&u64::to_be_bytes(length as u64))
            .expect("failed to write key set length to REQUEST_KEYS_SINCE request");
        for key in keys {
            self.socket
                .write_all(key.as_bytes())
                .expect("failed to write key to REQUEST_KEYS_SINCE request");
        }
        self.socket
            .write_all(&u64::to_be_bytes(0))
            .expect("failed to write key set end to REQUEST_KEYS_SINCE request");
    }

    /// Expect a `STORE_BLOB` request and write `keys` as response.
    #[track_caller]
    fn expect_request_store_blob(&mut self, expected_blob: &[u8]) {
        let mut buf = [0; 1];
        let n = self
            .socket
            .read(&mut buf)
            .expect("failed to read STORE_BLOB request");
        assert_eq!(n, 1, "unexpected read length");
        assert_eq!(
            buf[0], STORE_BLOB,
            "unexpected request, expected STORE_BLOB"
        );

        let mut buf = [0; size_of::<Key>()];
        let n = self
            .socket
            .read(&mut buf)
            .expect("failed to read key in STORE_BLOB request");
        assert_eq!(n, buf.len(), "unexpected read length");
        let key = Key::new(buf);
        let expected_key = Key::for_blob(expected_blob);
        assert_eq!(key, expected_key, "unexpected key");

        let mut buf = [0; size_of::<DateTime>()];
        let n = self
            .socket
            .read(&mut buf)
            .expect("failed to read timestamp in STORE_BLOB request");
        assert_eq!(n, buf.len(), "unexpected read length");
        let since = DateTime::from_bytes(&buf[..n]).unwrap_or(DateTime::INVALID);
        assert!(!since.is_invalid(), "invalid timestamp");

        let mut buf = [0; size_of::<u64>()];
        let n = self
            .socket
            .read(&mut buf)
            .expect("failed to read blob length in STORE_BLOB request");
        assert_eq!(n, buf.len(), "unexpected read length");
        let length = u64::from_be_bytes(buf);
        assert_eq!(length, expected_blob.len() as u64, "unexpected blob length");

        let mut buf = vec![0; expected_blob.len()];
        let n = self
            .socket
            .read(&mut buf)
            .expect("failed to read blob in STORE_BLOB request");
        assert_eq!(n, buf.len(), "unexpected read length");
        assert_eq!(&buf[..n], expected_blob, "unexpected blob");
    }
}

/// Acting as `participant::dispatcher::actor`.
enum Dispatcher {}

/// Connection acting as `participant::dispatcher::actor`.
impl TestStream<Dispatcher> {
    /// Returns `true` if it could read `PARTICIPANT_MAGIC` from the connection.
    #[track_caller]
    fn expect_participant_magic(&mut self) {
        let mut buf = [0; PARTICIPANT_MAGIC.len()];
        self.socket
            .read_exact(&mut buf)
            .expect("failed to read PARTICIPANT_MAGIC");
        if buf == COORDINATOR_MAGIC {
            panic!("unexpected COORDINATOR_MAGIC, expected PARTICIPANT_MAGIC");
        } else {
            assert_eq!(buf, PARTICIPANT_MAGIC, "unexpected bytes");
        }
    }

    /// Reads the server address.
    #[track_caller]
    fn read_server_addr(&mut self) -> io::Result<SocketAddr> {
        let mut buf = [0; 64];
        let n = self.socket.read(&mut buf)?;

        let addr_input = from_utf8(&buf[1..n - 1])
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err.to_string()))?;
        let addr = addr_input
            .parse::<SocketAddr>()
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err.to_string()))?;

        Ok(addr)
    }

    /// Write `peers` to the connection.
    #[track_caller]
    fn write_peers(&mut self, peers: &[SocketAddr]) -> io::Result<()> {
        self.socket.write_all(b"[")?;
        let mut first = true;
        for peer in peers {
            if first {
                first = false;
            } else {
                self.socket.write_all(b", ")?;
            }
            write!(self.socket, "\"{}\"", peer)?;
        }
        self.socket.write_all(b"]")
    }

    /// Expects an `AddBlob` request, responding with `response_vote`.
    #[track_caller]
    fn expect_add_blob_request(&mut self, key: &Key, response_vote: ConsensusVote) -> ConsensusId {
        let request = self.read_request().expect("failed to read AddBlob request");
        assert_eq!(request.key, *key, "unexpected key");
        assert!(
            matches!(request.op, Operation::AddBlob),
            "unexpected operation"
        );
        self.write_response(request.id, response_vote)
            .expect("failed to write response to AddBlob request");
        request.consensus_id
    }

    /// Expects a `CommitStoreBlob` request, responding with `response_vote`.
    #[track_caller]
    fn expect_commit_blob_request(
        &mut self,
        key: &Key,
        consensus_id: ConsensusId,
        response_vote: ConsensusVote,
    ) {
        let request = self
            .read_request()
            .expect("failed to read CommitStoreBlob request");
        assert_eq!(request.key, *key, "unexpected key");
        assert_eq!(
            request.consensus_id, consensus_id,
            "unexpected consensus id"
        );
        assert!(
            matches!(request.op, Operation::CommitStoreBlob(..)),
            "unexpected operation"
        );
        self.write_response(request.id, response_vote)
            .expect("failed to write response to CommitStoreBlob request");
    }

    /// Expects a `AbortStoreBlob` request, responding with `response_vote`.
    #[track_caller]
    fn expect_abort_store_blob_request(
        &mut self,
        key: &Key,
        consensus_id: ConsensusId,
        response_vote: ConsensusVote,
    ) {
        let request = self
            .read_request()
            .expect("failed to read AbortStoreBlob request");
        assert_eq!(request.key, *key, "unexpected key");
        assert_eq!(
            request.consensus_id, consensus_id,
            "unexpected consensus id"
        );
        assert!(
            matches!(request.op, Operation::AbortStoreBlob),
            "unexpected operation"
        );
        self.write_response(request.id, response_vote)
            .expect("failed to write response to AbortStoreBlob request");
    }

    /// Expects a `StoreCommitted` request, sending back no response.
    #[track_caller]
    fn expect_blob_committed_request(&mut self, key: &Key, consensus_id: ConsensusId) {
        let request = self
            .read_request()
            .expect("failed to read StoreCommitted request");
        assert_eq!(request.key, *key, "unexpected key");
        assert_eq!(
            request.consensus_id, consensus_id,
            "unexpected consensus id"
        );
        assert!(
            matches!(request.op, Operation::StoreCommitted(..)),
            "unexpected operation"
        );
    }

    /* TODO: add remove related request functions.
    RemoveBlob,
    CommitRemoveBlob(SystemTime),
    AbortRemoveBlob,
    RemoveCommitted(SystemTime),
    */

    fn read_request(&mut self) -> io::Result<Request> {
        let mut buf = [0; 1024];
        let n = self.socket.read(&mut buf)?;
        serde_json::from_slice(&buf[..n]).map_err(io::Error::from)
    }

    fn write_response(&mut self, request_id: usize, vote: ConsensusVote) -> io::Result<()> {
        let response = Response { request_id, vote };
        serde_json::to_writer(&mut self.socket, &response).map_err(io::Error::from)
    }
}

/// Store `blob` on a server running on localhost `port`.
#[track_caller]
fn store_blob(port: u16, blob: &[u8]) {
    let key = Key::for_blob(blob);
    let url = format!("/blob/{}", key);
    let length = blob.len().to_string();
    request!(
        POST port, "/blob", blob,
        CONTENT_LENGTH => &*length;
        expected: StatusCode::CREATED, body::EMPTY,
        CONTENT_LENGTH => body::EMPTY_LEN,
        LOCATION => &*url,
        CONNECTION => header::KEEP_ALIVE,
    );
    let last_modified = date_header();
    request!(
        GET port, url, body::EMPTY,
        expected: StatusCode::OK, blob,
        CONTENT_LENGTH => &*length,
        LAST_MODIFIED => &last_modified,
        CONNECTION => header::KEEP_ALIVE,
    );
}
