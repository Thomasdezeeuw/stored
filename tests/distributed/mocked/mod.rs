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
    fn expect_end(mut self) -> io::Result<()> {
        let mut buf = [0; 10];
        let n = self.socket.read(&mut buf)?;
        assert_eq!(n, 0);
        Ok(())
    }
}

/// Acting as `peer::server::actor`.
enum Server {}

/// Connection acting as `peer::server::actor`.
impl TestStream<Server> {
    /// Returns `true` if it could read `COORDINATOR_MAGIC` from the connection.
    #[track_caller]
    fn expect_coordinator_magic(&mut self) -> io::Result<()> {
        let mut buf = [0; COORDINATOR_MAGIC.len()];
        self.socket.read_exact(&mut buf)?;
        assert_eq!(buf, COORDINATOR_MAGIC);
        Ok(())
    }

    /// Expect a `REQUEST_BLOB` request and write `keys` as response.
    #[track_caller]
    fn expect_request_blob(&mut self, expected_blob: &[u8]) -> io::Result<()> {
        let mut buf = [0; 1];
        let n = self.socket.read(&mut buf)?;
        assert_eq!(n, 1);
        assert_eq!(buf[0], REQUEST_BLOB);

        let mut buf = [0; size_of::<Key>()];
        let n = self.socket.read(&mut buf)?;
        assert_eq!(n, buf.len());
        let key = Key::new(buf);
        let expected_key = Key::for_blob(expected_blob);
        assert_eq!(key, expected_key);

        let timestamp = DateTime::from(SystemTime::now());
        self.socket.write_all(timestamp.as_bytes())?;
        self.socket
            .write_all(&u64::to_be_bytes(expected_blob.len() as u64))?;
        self.socket.write_all(expected_blob)
    }

    /// Expect a `REQUEST_KEYS` request and write `keys` as response.
    #[track_caller]
    fn expect_request_keys(&mut self, keys: &[Key]) -> io::Result<()> {
        let mut buf = [0; 1];
        let n = self.socket.read(&mut buf)?;
        assert_eq!(n, 1);
        assert_eq!(buf[0], REQUEST_KEYS);

        let length = keys.len();
        self.socket.write_all(&u64::to_be_bytes(length as u64))?;

        for key in keys {
            self.socket.write_all(key.as_bytes())?;
        }

        Ok(())
    }

    /// Expect a `REQUEST_KEYS_SINCE` request and write `keys` as response.
    #[track_caller]
    fn expect_request_keys_since(&mut self, keys: &[Key]) -> io::Result<()> {
        let mut buf = [0; 1];
        let n = self.socket.read(&mut buf)?;
        assert_eq!(n, 1);
        assert_eq!(buf[0], REQUEST_KEYS_SINCE);

        let mut buf = [0; size_of::<DateTime>()];
        let n = self.socket.read(&mut buf)?;
        assert_eq!(n, buf.len());
        let since = DateTime::from_bytes(&buf[..n]).unwrap_or(DateTime::INVALID);
        assert!(!since.is_invalid());

        let length = keys.len();
        self.socket.write_all(&u64::to_be_bytes(length as u64))?;
        for key in keys {
            self.socket.write_all(key.as_bytes())?;
        }
        self.socket.write_all(&u64::to_be_bytes(0))?;

        Ok(())
    }

    /// Expect a `STORE_BLOB` request and write `keys` as response.
    #[track_caller]
    fn expect_request_store_blob(&mut self, expected_blob: &[u8]) -> io::Result<()> {
        let mut buf = [0; 1];
        let n = self.socket.read(&mut buf)?;
        assert_eq!(n, 1);
        assert_eq!(buf[0], STORE_BLOB);

        let mut buf = [0; size_of::<Key>()];
        let n = self.socket.read(&mut buf)?;
        assert_eq!(n, buf.len());
        let key = Key::new(buf);
        let expected_key = Key::for_blob(expected_blob);
        assert_eq!(key, expected_key);

        let mut buf = [0; size_of::<DateTime>()];
        let n = self.socket.read(&mut buf)?;
        assert_eq!(n, buf.len());
        let since = DateTime::from_bytes(&buf[..n]).unwrap_or(DateTime::INVALID);
        assert!(!since.is_invalid());

        let mut buf = [0; size_of::<u64>()];
        let n = self.socket.read(&mut buf)?;
        assert_eq!(n, buf.len());
        let length = u64::from_be_bytes(buf);
        assert_eq!(length, expected_blob.len() as u64);

        let mut buf = vec![0; expected_blob.len()];
        let n = self.socket.read(&mut buf)?;
        assert_eq!(n, buf.len());
        assert_eq!(&buf[..n], expected_blob);

        Ok(())
    }
}

/// Acting as `participant::dispatcher::actor`.
enum Dispatcher {}

/// Connection acting as `participant::dispatcher::actor`.
impl TestStream<Dispatcher> {
    /// Returns `true` if it could read `PARTICIPANT_MAGIC` from the connection.
    #[track_caller]
    fn expect_participant_magic(&mut self) -> io::Result<()> {
        let mut buf = [0; PARTICIPANT_MAGIC.len()];
        self.socket.read_exact(&mut buf)?;
        assert_eq!(buf, PARTICIPANT_MAGIC);
        Ok(())
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
    fn expect_add_blob_request(
        &mut self,
        key: &Key,
        response_vote: ConsensusVote,
    ) -> io::Result<ConsensusId> {
        let request = self.read_request()?;
        assert_eq!(request.key, *key);
        assert!(matches!(request.op, Operation::AddBlob));
        let response = Response {
            request_id: request.id,
            vote: response_vote,
        };
        match serde_json::to_writer(&mut self.socket, &response) {
            Ok(()) => Ok(request.consensus_id),
            Err(err) => Err(io::Error::from(err)),
        }
    }

    /// Expects a `CommitBlob` request, responding with `response_vote`.
    #[track_caller]
    fn expect_commit_blob_request(
        &mut self,
        key: &Key,
        consensus_id: ConsensusId,
        response_vote: ConsensusVote,
    ) -> io::Result<()> {
        let request = self.read_request()?;
        assert_eq!(request.key, *key);
        assert_eq!(request.consensus_id, consensus_id);
        assert!(matches!(request.op, Operation::CommitStoreBlob(..)));
        let response = Response {
            request_id: request.id,
            vote: response_vote,
        };
        serde_json::to_writer(&mut self.socket, &response).map_err(io::Error::from)
    }

    /// Expects a `StoreCommitted` request, responding with `response_vote`.
    #[track_caller]
    fn expect_blob_committed_request(
        &mut self,
        key: &Key,
        consensus_id: ConsensusId,
    ) -> io::Result<()> {
        let request = self.read_request()?;
        assert_eq!(request.key, *key);
        assert_eq!(request.consensus_id, consensus_id);
        assert!(matches!(request.op, Operation::StoreCommitted(..)));
        Ok(())
    }

    fn read_request(&mut self) -> io::Result<Request> {
        let mut buf = [0; 1024];
        let n = self.socket.read(&mut buf)?;
        serde_json::from_slice(&buf[..n]).map_err(io::Error::from)
    }
}

/// Store the blob "Hello world" on a server running on localhost `port`.
#[track_caller]
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
