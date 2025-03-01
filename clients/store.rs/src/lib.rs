//! Store a client for Store*d*.

#![feature(impl_trait_in_assoc_type)]

use std::mem::take;
use std::net::SocketAddr;
use std::{fmt, io};

use heph_rt::net::tcp::stream::TcpStream;
use heph_rt::Access;

pub mod key;
pub use key::Key;

pub mod ops;
mod resp;

/// Client for Store*d*.
pub struct Client {
    conn: TcpStream,
    buf: Vec<u8>,
}

impl Client {
    /// Create a new `Client` connecting to `address`.
    pub const fn connect<'rt, RT>(rt: &'rt RT, address: SocketAddr) -> ops::Connect<'rt, RT>
    where
        RT: Access,
    {
        ops::Connect::new(rt, address)
    }

    /// Add a `blob` to the store.
    pub const fn add<'c>(&'c mut self, blob: Blob) -> ops::Add<'c> {
        ops::Add::new(self, blob)
    }

    /// Remove a blob with `key` from the store.
    ///
    /// Returns true if the blob was removed, false if the blob was never
    /// stored.
    pub const fn remove<'c, 'k>(&'c mut self, key: &'k Key) -> ops::Remove<'c, 'k> {
        ops::Remove::new(self, key)
    }

    /// Get blob with `key`.
    pub const fn get<'c, 'k>(&'c mut self, key: &'k Key) -> ops::Get<'c, 'k> {
        ops::Get::new(self, key)
    }

    /// Check if a blob with `key` is stored.
    pub const fn contains<'c, 'k>(&'c mut self, key: &'k Key) -> ops::Contains<'c, 'k> {
        ops::Contains::new(self, key)
    }

    /// Check the number of blobs stored.
    pub const fn blobs_stored<'c>(&'c mut self) -> ops::BlobsStored<'c> {
        ops::BlobsStored::new(self)
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
