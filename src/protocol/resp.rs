//! Redis Protocol specification (RESP) version 2 like protocol
//! (<https://redis.io/topics/protocol>).
//!
//! The implementation starts with [`Resp`].

use std::fmt;
use std::future::Future;
use std::io::IoSlice;
use std::mem::replace;
use std::ops::Range;
use std::time::Duration;

use crate::key::{InvalidKeyStr, Key};
use crate::protocol::{Connection, IsFatal, Protocol, Request, Response};
use crate::storage::Blob;

const NIL: &str = "$-1\r\n";
const CRLF: &str = "\r\n";

/// Redis Protocol specification (RESP) like implementation of [`Protocol`].
pub struct Resp<C> {
    /// Underlying connection.
    conn: C,
    /// I/O buffer.
    buf: Vec<u8>,
    /// Amount of bytes processed from `buf`.
    processed: usize,
}

impl<C> Resp<C>
where
    C: Connection,
{
    /// Create a new RESP [`Protocol`].
    pub fn new(conn: C) -> Resp<C> {
        Resp {
            conn,
            buf: Vec::with_capacity(4096),
            processed: 0,
        }
    }

    /// Read a single argument from the connection.
    ///
    /// Returns `None` if no more arguments can be read from the connection.
    async fn read_argument(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<Value>, RequestError<C::Error>> {
        loop {
            match parse::argument(self.buf()) {
                // Sucessfully parsed an argument.
                Ok(Some((mut arg, processed))) => {
                    if let Value::String(Some(idx)) | Value::Error(idx) = &mut arg {
                        // Indices are based on the unprocessed bytes.
                        idx.start += self.processed;
                        idx.end += self.processed;
                    }
                    self.processed += processed;
                    return Ok(Some(arg));
                }
                // We don't have a complete argument in the buffer, read some
                // more bytes.
                Ok(None) => match self.read(timeout).await {
                    // Read some bytes, let's try parsing again.
                    Ok(true) => continue,
                    // Read everything, but still got some bytes left, so we got
                    // an incomplete last command. This is an fatal error,
                    // though the sender already closed their write half any
                    // way.
                    Ok(false) if !self.buf.is_empty() => {
                        return Err(RequestError::User(Error::INCOMPLETE, true))
                    }
                    // Read and processed everything, so we're done.
                    Ok(false) => return Ok(None),
                    Err(err) => return Err(RequestError::Conn(err)),
                },
                // Parsing error are always fatal as it's quite difficult to
                // recover from them.
                Err(err) => return Err(RequestError::User(err, true)),
            }
        }
    }

    /// Read an argument from the connection, expecting it to be a `Key`. If the
    /// argument is not a valid key it will return an error.
    ///
    /// Returns an [`Error::INCOMPLETE`] error if no more arguments can be read
    /// from the connection.
    async fn read_key(&mut self, timeout: Duration) -> Result<Key, RequestError<C::Error>> {
        match self.read_string_idx(timeout).await {
            Ok(idx) => match Key::from_byte_str(&self.buf[idx]) {
                Ok(key) => Ok(key),
                Err(InvalidKeyStr) => Err(RequestError::User(Error::INVALID_KEY, false)),
            },
            Err(err) => Err(err),
        }
    }

    /// Read an argument from the connection, expecting it to be a (not nil)
    /// string. If the argument is not a string it will return an error.
    ///
    /// Returns an [`Error::INCOMPLETE`] error if no more arguments can be read
    /// from the connection.
    async fn read_string(&mut self, timeout: Duration) -> Result<&[u8], RequestError<C::Error>> {
        match self.read_string_idx(timeout).await {
            Ok(idx) => Ok(&self.buf[idx]),
            Err(err) => Err(err),
        }
    }

    /// Same as [`Resp::parse_string`] but returns the index range instead of
    /// the actual string.
    async fn read_string_idx(
        &mut self,
        timeout: Duration,
    ) -> Result<Range<usize>, RequestError<C::Error>> {
        match self.read_argument(timeout).await {
            Ok(Some(Value::String(Some(idx)))) => Ok(idx),
            Ok(Some(_)) => Err(RequestError::User(Error::INVALID_ARG_TYPE_EXP_STR, false)),
            Ok(None) => return Err(RequestError::User(Error::INCOMPLETE, true)),
            Err(err) => Err(err),
        }
    }

    /// Ensure we `expected` number of arguments in the request array, that is
    /// `length == expected + 1`. If not this will attempt to recover from the
    /// error and return [`Error::INVALID_ARGUMENTS`].
    async fn ensure_arguments(
        &mut self,
        length: usize,
        expected: usize,
        timeout: Duration,
    ) -> Result<(), RequestError<C::Error>> {
        if length == expected + 1 {
            Ok(())
        } else {
            let fatal = self.recover(length, timeout).await.is_err();
            Err(RequestError::User(Error::INVALID_ARGUMENTS, fatal))
        }
    }

    /// Attempt to recover from a protocol error, removes `arguments` arguments
    /// from the connection.
    ///
    /// If this returns `Ok(())` it means all argument where succesfully removed
    /// from the connection. If this returns `Err(())` we failed to remove the
    /// arguments from the connection and it should be considered broken.
    async fn recover(&mut self, arguments: usize, timeout: Duration) -> Result<(), ()> {
        let mut iter = 0..arguments;
        while iter.next().is_some() {
            match self.read_argument(timeout).await {
                Ok(Some(Value::Array(Some(n)))) => iter.end += n, // Great, more stuff to ignore.
                Ok(Some(_)) => continue,
                // Couldn't delete all arguments from the connction, we'll
                // consider it fatal.
                Ok(None) => return Err(()),
                // Unexpected user error.
                Err(_) => return Err(()),
            }
        }
        Ok(())
    }

    /// Read some bytes into the buffer.
    ///
    /// Returns `Ok(true)` if at least 1 byte was read, `Ok(false)` if we read 0
    /// bytes (thus read all bytes in the the connection) and an error
    /// otherwise.
    async fn read(&mut self, timeout: Duration) -> Result<bool, C::Error> {
        self.prepare_buf();
        let buf = replace(&mut self.buf, Vec::new());
        let before_length = buf.len();
        self.buf = self.conn.read_into(buf, timeout).await?;
        Ok(before_length != self.buf.len())
    }

    /// Prepare the buffer, removing all processed bytes.
    fn prepare_buf(&mut self) {
        match replace(&mut self.processed, 0) {
            // Entire buffer is processed, we can clear it.
            n if self.buf.len() == n => self.buf.clear(),
            // Still have some unprocessed bytes.
            _ => drop(self.buf.drain(0..self.processed)),
        }
    }

    /// Returns the unprocessed bytes in the buffer.
    fn buf(&self) -> &[u8] {
        &self.buf[self.processed..]
    }
}

impl<C> Protocol for Resp<C>
where
    C: Connection + 'static,
{
    type NextRequest<'a> = impl Future<Output = Result<Option<Request<'a>>, Self::RequestError>> + 'a
    where
        Self: 'a;
    type RequestError = RequestError<C::Error>;
    type Reply<'a, B: Blob> = impl Future<Output = Result<(), Self::ResponseError>> + 'a
    where
        Self: 'a,
        B: Blob + 'a;
    type ReplyWithError<'a> = impl Future<Output = Result<(), Self::ResponseError>> + 'a
    where
        Self: 'a;
    type ResponseError = C::Error;

    fn next_request<'a>(&'a mut self, timeout: Duration) -> Self::NextRequest<'a> {
        async move {
            match self.read_argument(timeout).await {
                Ok(Some(Value::Array(Some(length)))) => {
                    if length == 0 {
                        let fatal = self.recover(length, timeout).await.is_err();
                        return Err(RequestError::User(Error::MISSING_COMMAND, fatal));
                    }

                    let cmd = match self.read_argument(timeout).await {
                        Ok(Some(Value::String(Some(idx)))) => &self.buf[idx],
                        // Unexpected argument type.
                        Ok(Some(_)) => {
                            let fatal = self.recover(length - 1, timeout).await.is_err();
                            return Err(RequestError::User(Error::INVALID_COMMAND_TYPE, fatal));
                        }
                        // Missing command.
                        Ok(None) => return Err(RequestError::User(Error::MISSING_COMMAND, true)),
                        // Fatal error reading the argument.
                        Err(err) => return Err(err),
                    };

                    match cmd {
                        b"GET" => {
                            self.ensure_arguments(length, 1, timeout).await?;
                            match self.read_key(timeout).await {
                                Ok(key) => Ok(Some(Request::GetBlob(key))),
                                Err(err) => Err(err),
                            }
                        }
                        // NOTE: `ADD` is not a Redis command.
                        b"SET" | b"ADD" => {
                            self.ensure_arguments(length, 1, timeout).await?;
                            match self.read_string(timeout).await {
                                Ok(blob) => Ok(Some(Request::AddBlob(blob))),
                                Err(err) => Err(err),
                            }
                        }
                        b"EXISTS" => {
                            self.ensure_arguments(length, 1, timeout).await?;
                            match self.read_key(timeout).await {
                                Ok(key) => Ok(Some(Request::CointainsBlob(key))),
                                Err(err) => Err(err),
                            }
                        }
                        b"DEL" => {
                            self.ensure_arguments(length, 1, timeout).await?;
                            match self.read_key(timeout).await {
                                Ok(key) => Ok(Some(Request::RemoveBlob(key))),
                                Err(err) => Err(err),
                            }
                        }
                        _ => {
                            let fatal = self.recover(length, timeout).await.is_err();
                            Err(RequestError::User(Error::UNKNOWN_COMMAND, fatal))
                        }
                    }
                }
                // Unexpected argument.
                Ok(Some(_)) => {
                    // We'll consider this a fatal error because we can't
                    // (reliably) determine where the next request starts.
                    // TODO: attempt to the above any way.
                    Err(RequestError::User(Error::INVALID_FORMAT, true))
                }
                // No more requests and no more bytes to read. Job well done.
                Ok(None) => Ok(None),
                // Can't recover from this protocol error.
                Err(err) => match err {
                    _ => todo!("User error fatal?"),
                },
            }
        }
    }

    fn reply<'a, B>(&'a mut self, response: Response<B>, timeout: Duration) -> Self::Reply<'a, B>
    where
        B: Blob + 'a,
    {
        // TODO: only prepare buf if we need the additional space.
        self.prepare_buf();

        async move {
            match response {
                // Responses to GET.
                // Returns the `key` as bulk string.
                Response::Added(key) | Response::AlreadyStored(key) => {
                    let start_idx = self.buf.len();
                    {
                        use std::io::Write; // Don't want to use this anywhere else.
                        write!(&mut self.buf, "{}", key).unwrap();
                    }
                    encode::length(&mut self.buf, Key::STR_LENGTH);
                    let key = &self.buf[start_idx..start_idx + Key::STR_LENGTH];
                    let header = &self.buf[start_idx + Key::STR_LENGTH..];
                    let mut bufs = [
                        IoSlice::new(header),
                        IoSlice::new(key),
                        IoSlice::new(CRLF.as_bytes()),
                    ];
                    let res = self.conn.write_vectored(&mut bufs, timeout).await;
                    self.buf.truncate(start_idx);
                    res
                }

                // Responses to DEL.
                // Returns 1 as integer.
                Response::BlobRemoved => {
                    let (value, start_idx) = encode::integer(&mut self.buf, 1);
                    let res = self.conn.write(value, timeout).await;
                    self.buf.truncate(start_idx);
                    res
                }
                // Returns 0 as integer.
                Response::BlobNotRemoved => {
                    let (value, start_idx) = encode::integer(&mut self.buf, 0);
                    let res = self.conn.write(value, timeout).await;
                    self.buf.truncate(start_idx);
                    res
                }

                // Responses to GET.
                // Returns the `blob` as bulk string.
                Response::Blob(blob) => {
                    let (header, start_idx) = encode::length(&mut self.buf, blob.len());
                    let res = blob
                        .write(header, CRLF.as_bytes(), &mut self.conn, timeout)
                        .await;
                    self.buf.truncate(start_idx);
                    res
                }
                // Returns the `NIL` string as simple string.
                Response::BlobNotFound => self.conn.write(NIL.as_bytes(), timeout).await,

                // Responses to EXISTS.
                // Returns 1 as integer.
                Response::ContainsBlob => {
                    let (value, start_idx) = encode::integer(&mut self.buf, 1);
                    let res = self.conn.write(value, timeout).await;
                    self.buf.truncate(start_idx);
                    res
                }
                // Returns 0 as integer.
                Response::NotContainBlob => {
                    let (value, start_idx) = encode::integer(&mut self.buf, 0);
                    let res = self.conn.write(value, timeout).await;
                    self.buf.truncate(start_idx);
                    res
                }

                // Generic server error.
                // Returns a server error as simple string.
                Response::Error => {
                    self.conn
                        .write(Error::SERVER_ERROR.as_bytes(), timeout)
                        .await
                }
            }
        }
    }

    fn reply_to_error<'a>(
        &'a mut self,
        err: Self::RequestError,
        timeout: Duration,
    ) -> Self::ReplyWithError<'a> {
        async move {
            match err {
                RequestError::User(err, ..) => self.conn.write(err.as_bytes(), timeout).await,
                RequestError::Conn(..) => Ok(()),
            }
        }
    }
}

/// Value send by the client.
#[derive(Debug)]
enum Value {
    /// Contains the range of bytes that make up the string in the buffer.
    /// `None` means a null, or nil, string.
    String(Option<Range<usize>>),
    /// Contains the range of bytes that make up the error in the buffer.
    Error(Range<usize>),
    /// The integer value.
    Integer(isize),
    /// Returns the amount of values in the array.
    /// `None` means a null array.
    Array(Option<usize>),
}

/// Error reading request.
#[derive(Debug)]
pub enum RequestError<E> {
    /// User error, e.g. protocol violation or incorrect argument(s).
    User(Error, bool),
    /// Connection error.
    Conn(E),
}

impl<E> IsFatal for RequestError<E> {
    fn is_fatal(&self) -> bool {
        match self {
            RequestError::User(_, fatal) => *fatal,
            RequestError::Conn(..) => true,
        }
    }
}

impl<E> fmt::Display for RequestError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestError::User(err, ..) => err.message().fmt(f),
            RequestError::Conn(err) => err.fmt(f),
        }
    }
}

/// Error string that can be returned, following RESP (i.e. starts with `-` and
/// ends with `\r\n`).
///
/// Error must be recoverable.
#[derive(Copy, Clone, Debug)]
pub struct Error(&'static str);

/// Creates a new [`Error`].
macro_rules! user_error {
    ($( $e: expr ),* $(,)?) => { Error(concat!("-", $( $e ),*, "\r\n")) };
}

impl Error {
    // TODO: add Error Prefix to errors, see
    // <https://redis.io/topics/protocol#resp-errors>.

    const INVALID_FORMAT: Error = user_error!("request has an invalid format");
    const INCOMPLETE: Error = user_error!("request is incomplete");
    const MISSING_COMMAND: Error = user_error!("request is missing a command");
    const INVALID_COMMAND_TYPE: Error =
        user_error!("request command is of an invalid type (expected a string)");
    const INVALID_ARGUMENTS: Error = user_error!("invalid number of arguments");
    const INVALID_ARG_TYPE_EXP_STR: Error =
        user_error!("invalid argument type (expecting a string)");
    // Array errors.
    const PARSE_ARRAY_NEGATIVE_LENGTH: Error =
        user_error!("unable to parse array: negative length");
    // String errors.
    const PARSE_STR_NEGATIVE_LENGTH: Error = user_error!("unable to parse string: negative length");
    const PARSE_STR_END: Error = user_error!("unable to parse string: end of line (CRLF) invalid");
    // Int errors, shared by array, bulk string and integer.
    const PARSE_INT_OVERFLOW: Error =
        user_error!("unable to parse number: value too large (overflow)");
    const PARSE_INT_END: Error = user_error!("unable to parse number: end of line (CRLF) invalid");
    const PARSE_INT_INVALID_BYTE: Error = user_error!("unable to parse number: invalid byte");
    const UNKNOWN_COMMAND: Error = user_error!("unknown command");
    const INVALID_KEY: Error = user_error!("invalid key");
    // Server errors.
    const UNIMPLEMENTED_INLINE_COMMANDS: Error =
        user_error!("inline commands (simple protocol) not implemented");
    const SERVER_ERROR: Error = user_error!("internal server error");

    /// Returns the message of this error.
    fn message(&self) -> &'static str {
        debug_assert_eq!(self.0.as_bytes()[0], b'-');
        debug_assert_eq!(&self.0[self.0.len() - 2..], "\r\n");
        &self.0[1..self.0.len() - 2]
    }

    /// Returns the error as error response.
    fn as_bytes(&self) -> &'static [u8] {
        debug_assert_eq!(self.0.as_bytes()[0], b'-');
        debug_assert_eq!(&self.0[self.0.len() - 2..], "\r\n");
        self.0.as_bytes()
    }
}

mod parse {
    //! Module that can parse the Redis Protocol (RESP2).
    //!
    //! <https://redis.io/topics/protocol>.

    use std::ops::Range;

    use super::{Error, Value};

    /// Result of a parsing function.
    ///
    /// Returns `Ok(Some((value, bytes_read)))` on success, `Ok(None)` is returned
    /// if `buf` doesn't contain a complete result and an error otherwise.
    pub(super) type ParseResult<T> = Result<Option<(T, usize)>, Error>;

    pub(super) fn argument(buf: &[u8]) -> ParseResult<Value> {
        match buf.first() {
            Some(b'*') => array(buf).map(|v| v.map(|(v, p)| (Value::Array(v), p))),
            Some(b'$') => bulk_string(buf).map(|v| v.map(|(idx, p)| (Value::String(idx), p))),
            Some(b'+') => {
                simple_string(buf).map(|v| v.map(|(idx, p)| (Value::String(Some(idx)), p)))
            }
            Some(b'-') => error(buf).map(|v| v.map(|(idx, p)| (Value::Error(idx), p))),
            Some(b':') => integer(buf).map(|v| v.map(|(v, p)| (Value::Integer(v), p))),
            Some(_) => Err(Error::UNIMPLEMENTED_INLINE_COMMANDS),
            None => Ok(None),
        }
    }

    /// Parse an array from `buf` including starting `*` and `\r\n` end.
    ///
    /// Returns the length of the array.
    fn array<'a>(buf: &'a [u8]) -> ParseResult<Option<usize>> {
        debug_assert_eq!(buf.first(), Some(&b'*'));
        match int(&buf[1..]) {
            // Null array. Format `*-1\r\n`.
            Ok(Some((-1, processed))) => Ok(Some((None, processed))),
            Ok(Some((len, _))) if len.is_negative() => Err(Error::PARSE_ARRAY_NEGATIVE_LENGTH),
            Ok(Some((len, processed))) => Ok(Some((Some(len as usize), processed))),
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Parse a bulk string from `buf` including starting `$` and `\r\n` end.
    ///
    /// Returns the range bytes that make up the string in `buf`.
    fn bulk_string<'a>(buf: &'a [u8]) -> ParseResult<Option<Range<usize>>> {
        debug_assert_eq!(buf.first(), Some(&b'$'));
        let (length, processed) = match int(&buf[1..]) {
            // Null, or nill, string. Format `$-1\r\n`.
            Ok(Some((-1, processed))) => return Ok(Some((None, processed))),
            Ok(Some((len, _))) if len.is_negative() => {
                return Err(Error::PARSE_STR_NEGATIVE_LENGTH)
            }
            Ok(Some((len, processed))) => (len as usize, processed), // $ is included in processed.
            Ok(None) => return Ok(None),
            Err(err) => return Err(err),
        };

        let buf = &buf[processed..];
        if buf.len() < length + 2 {
            return Ok(None);
        }

        if !matches!(buf.get(length), Some(b'\r')) || !matches!(buf.get(length + 1), Some(b'\n')) {
            Err(Error::PARSE_STR_END)
        } else {
            let end = processed + length;
            Ok(Some((Some(processed..end), end + 2))) // + 2 = CRLF
        }
    }

    /// Parse a simple string from `buf` including starting `+` and `\r\n` end.
    ///
    /// Returns the range bytes that make up the string in `buf`.
    fn simple_string<'a>(buf: &'a [u8]) -> ParseResult<Range<usize>> {
        debug_assert_eq!(buf.first(), Some(&b'+'));
        until_crlf(&buf[1..])
    }

    /// Parse an error from `buf` including starting `-` and `\r\n` end.
    ///
    /// Returns the range bytes that make up the string in `buf`.
    fn error<'a>(buf: &'a [u8]) -> ParseResult<Range<usize>> {
        debug_assert_eq!(buf.first(), Some(&b'-'));
        until_crlf(&buf[1..])
    }

    /// Parse an integer from `buf` including starting `:` and `\r\n` end.
    ///
    /// Returns the integer value.
    fn integer<'a>(buf: &'a [u8]) -> ParseResult<isize> {
        debug_assert_eq!(buf.first(), Some(&b':'));
        int(&buf[1..])
    }

    /// Returns the range until it hits CRLF.
    ///
    /// # Notes
    ///
    /// The first byte (`+` or `-`) should **not** be included. The processed
    /// bytes will always be +1 (so that the first byte can be safely ignored).
    fn until_crlf<'a>(buf: &'a [u8]) -> ParseResult<Range<usize>> {
        let mut end: usize = 1; // Skipping first byte per the docs.
        let mut bytes = buf.iter();
        while let Some(b) = bytes.next() {
            if *b == b'\r' {
                if let Some(b'\n') = bytes.next() {
                    return Ok(Some((1..end, end + 2))); // +2 = CRLF.
                } else {
                    end += 1;
                }
            }
            end += 1;
        }
        Ok(None)
    }

    /// Parse an integer from `buf` including `\r\n`.
    ///
    /// # Notes
    ///
    /// The first byte (`:` or `$`) should **not** be included. The processed
    /// bytes will always be +1 (so that the first byte can be safely ignored).
    fn int(buf: &[u8]) -> ParseResult<isize> {
        let mut value: isize = 0;
        let mut is_positive = true;
        let mut end: usize = 3; // Skipping first byte per the docs and CRLF.
        let mut bytes = buf.iter();
        if buf.first().copied() == Some(b'-') {
            is_positive = false;
            bytes.next();
        }
        while let Some(b) = bytes.next() {
            match b {
                b'0'..=b'9' => match value
                    .checked_mul(10)
                    .and_then(|v| v.checked_add((b - b'0') as isize))
                {
                    Some(v) => value = v,
                    None => return Err(Error::PARSE_INT_OVERFLOW),
                },
                b'\r' => match bytes.next() {
                    Some(b'\n') => {
                        if !is_positive {
                            value = -value
                        }
                        return Ok(Some((value, end)));
                    }
                    _ => return Err(Error::PARSE_INT_END),
                },
                _ => return Err(Error::PARSE_INT_INVALID_BYTE),
            }
            end += 1;
        }
        Ok(None)
    }
}

mod encode {
    //! Module that encodes following the Redis Protocol (RESP2).
    //!
    //! <https://redis.io/topics/protocol>.

    /// Encode `length` onto `buf` (without changing it's current contents),
    /// returns the bytes with the length line and the index to which to
    /// truncate the buffer to restore it.
    pub(super) fn length<'a>(buf: &'a mut Vec<u8>, length: usize) -> (&'a [u8], usize) {
        let start_idx = length_idx(buf, length);
        (&buf[start_idx..], start_idx)
    }

    /// Encodes `length` onto `buf` (without changing it's current contents),
    /// returns the index at which the encoded length starts (end at the end of
    /// the buffer).
    pub(super) fn length_idx<'a>(buf: &'a mut Vec<u8>, length: usize) -> usize {
        int(buf, b'$', length)
    }

    /// Encode `value` onto `buf` (without changing it's current contents),
    /// returns the bytes with the length line and the index to which to
    /// truncate the buffer to restore it.
    pub(super) fn integer<'a>(buf: &'a mut Vec<u8>, value: usize) -> (&'a [u8], usize) {
        let start_idx = int(buf, b':', value);
        (&buf[start_idx..], start_idx)
    }

    /// Encode an integer with `prefix`.
    fn int<'a>(buf: &'a mut Vec<u8>, prefix: u8, value: usize) -> usize {
        let start_idx = buf.len();
        let mut buffer = itoa::Buffer::new();
        let int_bytes = buffer.format(value).as_bytes();
        buf.reserve(int_bytes.len() + 3); // 3 = prefix + CRLF.
        buf.push(prefix);
        buf.extend_from_slice(int_bytes);
        buf.push(b'\r');
        buf.push(b'\n');
        start_idx
    }
}
