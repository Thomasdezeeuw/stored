//! Store a client for Store*d*.

use std::mem::take;
use std::net::SocketAddr;
use std::{fmt, io};

use heph_rt::net::tcp::stream::TcpStream;
use heph_rt::Access;

pub mod key;
pub use key::Key;

mod resp;

const NIL: &str = "$-1\r\n";
const CRLF: &str = "\r\n";

/// Client for Store*d*.
pub struct Client {
    conn: TcpStream,
    buf: Vec<u8>,
}

impl Client {
    /// Create a new `Client` connecting to `address`.
    pub async fn connect<RT>(rt: &RT, address: SocketAddr) -> io::Result<Client>
    where
        RT: Access,
    {
        let conn = TcpStream::connect(rt, address).await?;
        Ok(Client {
            conn,
            buf: Vec::with_capacity(512),
        })
    }

    /// Add a `blob` to the store.
    pub async fn add(&mut self, blob: Blob) -> io::Result<Key> {
        let mut buf = take(&mut self.buf);

        resp::encode::array(&mut buf, 2); // "SET" + blob.
        resp::encode::string(&mut buf, "SET");
        resp::encode::string_start(&mut buf, blob.len());

        let bufs = (buf, blob, CRLF);
        let bufs = self.conn.send_vectored_all(bufs).await?;
        self.buf = bufs.0;
        self.buf.clear();

        self.read_key().await
    }

    /// Remove a blob with `key` from the store.
    ///
    /// Returns true if the blob was removed, false if the blob was never
    /// stored.
    pub async fn remove(&mut self, key: &Key) -> io::Result<bool> {
        let mut buf = take(&mut self.buf);

        resp::encode::array(&mut buf, 2); // "DEL" + key.
        resp::encode::string(&mut buf, "DEL");
        resp::encode::key(&mut buf, key);

        self.buf = self.conn.send_all(buf).await?;
        self.buf.clear();

        self.read_bool().await
    }

    /// Get blob with `key`.
    pub async fn get(&mut self, key: &Key) -> io::Result<Option<Blob>> {
        let mut buf = take(&mut self.buf);
        buf.clear();

        resp::encode::array(&mut buf, 2); // "GET" + key.
        resp::encode::string(&mut buf, "GET");
        resp::encode::key(&mut buf, key);

        self.buf = self.conn.send_all(buf).await?;
        self.buf.clear();

        self.read_opt_string(|blob| Ok(blob.map(Into::into))).await
    }

    /// Check if a blob with `key` is stored.
    pub async fn contains(&mut self, key: &Key) -> io::Result<bool> {
        let mut buf = take(&mut self.buf);

        resp::encode::array(&mut buf, 2); // "EXISTS" + key.
        resp::encode::string(&mut buf, "EXISTS");
        resp::encode::key(&mut buf, key);

        self.buf = self.conn.send_all(buf).await?;
        self.buf.clear();

        self.read_bool().await
    }

    /// Check the number of blobs stored.
    pub async fn blobs_stored(&mut self) -> io::Result<usize> {
        let mut buf = take(&mut self.buf);

        resp::encode::array(&mut buf, 1); // "DBSIZE".
        resp::encode::string(&mut buf, "DBSIZE");

        self.buf = self.conn.send_all(buf).await?;
        self.buf.clear();

        self.read_integer().await
    }

    async fn read_key(&mut self) -> io::Result<Key> {
        self.read_opt_string(|key| match key {
            Some(key) => Key::try_parse_bytes(key).map_err(|err| {
                new_error(format_args!("failed to parse expected key response: {err}"))
            }),
            None => Err(invalid_response()),
        })
        .await
    }

    async fn read_bool(&mut self) -> io::Result<bool> {
        let n = self.read_integer().await?;
        Ok(if n == 0 { false } else { true })
    }

    async fn read_integer(&mut self) -> io::Result<usize> {
        loop {
            self.read().await?;
            let Some((n, bytes_read)) = resp::decode::integer(&self.buf)? else {
                continue;
            };
            self.processed(bytes_read);
            return Ok(n);
        }
    }

    async fn read_opt_string<F, T>(&mut self, map: F) -> io::Result<T>
    where
        F: FnOnce(Option<&[u8]>) -> io::Result<T>,
    {
        loop {
            self.read().await?;
            let Some((string, bytes_read)) = resp::decode::string(&self.buf)? else {
                continue;
            };
            let res = map(string);
            self.processed(bytes_read);
            return res;
        }
    }

    /// Read more bytes into the buffer.
    async fn read(&mut self) -> io::Result<()> {
        let mut buf = take(&mut self.buf);
        buf.reserve(512);
        let n = buf.len();
        self.buf = self.conn.recv(buf).await?;
        if self.buf.len() == n {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        Ok(())
    }

    fn processed(&mut self, processed: usize) {
        // TODO: improve this. Currently we don't process any additional bytes
        // send, so we remove them all from the buffer.
        debug_assert!(self.buf.len() == processed);
        self.buf.clear();
    }
}

fn new_error<E: fmt::Display>(err: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, err.to_string())
}

fn invalid_response() -> io::Error {
    new_error("invalid response")
}

/// BLOB (Binary Large OBject).
// TODO: better type.
pub type Blob = Box<[u8]>;
