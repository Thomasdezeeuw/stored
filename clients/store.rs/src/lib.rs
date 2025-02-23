//! Store a client for Store*d*.

use std::mem::take;
use std::net::SocketAddr;
use std::{fmt, io};

use heph_rt::net::tcp::stream::TcpStream;
use heph_rt::Access;

pub mod key;
pub use key::Key;

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

        encode::array(&mut buf, 2); // "SET" + blob.
        encode::string(&mut buf, "SET");
        encode::string_start(&mut buf, blob.len());

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

        encode::array(&mut buf, 2); // "DEL" + key.
        encode::string(&mut buf, "DEL");
        encode::key(&mut buf, key);

        self.buf = self.conn.send_all(buf).await?;
        self.buf.clear();

        self.read_bool().await
    }

    /// Get blob with `key`.
    pub async fn get(&mut self, key: &Key) -> io::Result<Option<Blob>> {
        let mut buf = take(&mut self.buf);
        buf.clear();

        encode::array(&mut buf, 2); // "GET" + key.
        encode::string(&mut buf, "GET");
        encode::key(&mut buf, key);

        self.buf = self.conn.send_all(buf).await?;
        self.buf.clear();

        self.read_opt_string(|blob| Ok(blob.map(Into::into))).await
    }

    /// Check if a blob with `key` is stored.
    pub async fn contains(&mut self, key: &Key) -> io::Result<bool> {
        let mut buf = take(&mut self.buf);

        encode::array(&mut buf, 2); // "EXISTS" + key.
        encode::string(&mut buf, "EXISTS");
        encode::key(&mut buf, key);

        self.buf = self.conn.send_all(buf).await?;
        self.buf.clear();

        self.read_bool().await
    }

    /// Check the number of blobs stored.
    pub async fn blobs_stored(&mut self) -> io::Result<usize> {
        let mut buf = take(&mut self.buf);

        encode::array(&mut buf, 1); // "DBSIZE".
        encode::string(&mut buf, "DBSIZE");

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
            let Some((n, bytes_read)) = decode::integer(&self.buf)? else {
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
            let Some((string, bytes_read)) = decode::string(&self.buf)? else {
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

mod encode {
    //! Module that encodes following the Redis Protocol (RESP2).
    //!
    //! <https://redis.io/topics/protocol>.

    use crate::Key;

    /// Encode the start of an array of `length` elements onto `buf` (without
    /// changing it's current contents).
    pub(super) fn array(buf: &mut Vec<u8>, length: usize) {
        int(buf, b'*', length);
    }

    /// Encode a string onto `buf` (without changing it's current contents).
    pub(super) fn string(buf: &mut Vec<u8>, value: &str) {
        string_start(buf, value.len());
        buf.extend_from_slice(value.as_bytes());
        buf.push(b'\r');
        buf.push(b'\n');
    }

    /// Encode the start of a string onto `buf` (without changing it's current
    /// contents).
    pub(super) fn string_start(buf: &mut Vec<u8>, length: usize) {
        int(buf, b'$', length);
    }

    /// Encode a key onto `buf` (without changing it's current contents).
    pub(super) fn key(buf: &mut Vec<u8>, key: &Key) {
        int(buf, b'$', Key::STR_LENGTH);
        key.append_to(buf);
        buf.push(b'\r');
        buf.push(b'\n');
    }

    /// Encode an integer with `prefix`.
    fn int(buf: &mut Vec<u8>, prefix: u8, value: usize) {
        let mut buffer = itoa::Buffer::new();
        let int_bytes = buffer.format(value).as_bytes();
        buf.reserve(int_bytes.len() + 3); // 3 = prefix + CRLF.
        buf.push(prefix);
        buf.extend_from_slice(int_bytes);
        buf.push(b'\r');
        buf.push(b'\n');
    }
}

mod decode {
    //! Module that can decode the Redis Protocol (RESP2).
    //!
    //! <https://redis.io/topics/protocol>.

    use std::io;

    use super::{invalid_response, new_error, NIL};

    /// Result of a parsing function.
    ///
    /// Returns `Ok(Some((value, bytes_read)))` on success, `Ok(None)` is returned
    /// if `buf` doesn't contain a complete result and an error otherwise.
    pub(super) type ParseResult<T> = Result<Option<(T, usize)>, io::Error>;

    /// Parse a string from `buf` including starting `$` or `+` and `\r\n` end.
    pub(super) fn string(buf: &[u8]) -> ParseResult<Option<&[u8]>> {
        match buf.first() {
            Some(b'$') => {} // Bulk string, continue below.
            // Simple string.
            Some(b'+') => match until_crlf(buf) {
                Some((string, bytes_read)) => return Ok(Some((Some(string), bytes_read))),
                None => return Ok(None),
            },
            Some(b'-') => match error(buf) {
                Some(err) => return Err(err),
                None => return Ok(None),
            },
            Some(_) => return Err(invalid_response()),
            None => return Ok(None),
        }

        if buf.starts_with(NIL.as_bytes()) {
            return Ok(Some((None, NIL.len())));
        }

        let (length, processed) = match int(buf) {
            Ok(Some((len, processed))) => (len, processed), // $ is included in processed.
            Ok(None) => return Ok(None),
            Err(err) => return Err(err),
        };

        let buf = &buf[processed..];
        if buf.len() < length + 2 {
            return Ok(None);
        }

        if !matches!(buf.get(length), Some(b'\r')) || !matches!(buf.get(length + 1), Some(b'\n')) {
            Err(invalid_response())
        } else {
            Ok(Some((Some(&buf[..length]), processed + length + 2))) // + 2 = CRLF
        }
    }

    /// Parse an integer from `buf` including starting `:` and `\r\n` end.
    ///
    /// Returns the integer value.
    pub(super) fn integer(buf: &[u8]) -> ParseResult<usize> {
        match buf.first() {
            Some(b':') => int(buf),
            Some(b'-') => match error(buf) {
                Some(err) => Err(err),
                None => Ok(None),
            },
            Some(_) => Err(invalid_response()),
            None => Ok(None),
        }
    }

    fn int(buf: &[u8]) -> ParseResult<usize> {
        let mut value: usize = 0;
        let mut bytes = buf[1..].iter();
        while let Some(b) = bytes.next() {
            match b {
                b'0'..=b'9' => match value
                    .checked_mul(10)
                    .and_then(|v| v.checked_add((b - b'0') as usize))
                {
                    Some(v) => value = v,
                    None => return Err(new_error("response integer too large")),
                },
                b'\r' => match bytes.next() {
                    Some(b'\n') => {
                        let left = buf.len() - bytes.as_slice().len();
                        return Ok(Some((value, left)));
                    }
                    _ => return Err(invalid_response()),
                },
                _ => return Err(invalid_response()),
            }
        }
        Ok(None)
    }

    /// Parse an error from `buf` including starting `-` and `\r\n` end.
    fn error(buf: &[u8]) -> Option<io::Error> {
        debug_assert_eq!(buf.first(), Some(&b'-'));
        let res = until_crlf(buf)?;
        Some(new_error(format_args!(
            "error from server: {}",
            String::from_utf8_lossy(&res.0)
        )))
    }

    /// Returns the range until it hits CRLF.
    ///
    /// Ignores the first bytes (type indicator).
    fn until_crlf(buf: &[u8]) -> Option<(&[u8], usize)> {
        let mut bytes = buf.iter();
        _ = bytes.next();
        let mut end: usize = 1; // Skipping first byte per the docs.
        while let Some(b) = bytes.next() {
            if *b == b'\r' {
                if let Some(b'\n') = bytes.next() {
                    return Some((&buf[1..end], end + 2)); // +2 = CRLF.
                }
                end += 1;
            }
            end += 1;
        }
        None
    }
}
