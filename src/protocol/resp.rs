//! Redis Protocol specification (RESP) version 2 like protocol
//! (<https://redis.io/topics/protocol>).
//!
//! The implementation starts with [`Resp`].

use std::future::Future;
use std::mem::replace;
use std::ops::Range;
use std::pin::Pin;
use std::task::{self, ready, Poll};
use std::time::Duration;
use std::{fmt, io};

use crate::key::{InvalidKeyStr, Key};
use crate::protocol::{Connection, IsFatal, Protocol, Request, Response};
use crate::storage::Blob;

/// Redis Protocol specification (RESP) like implementation of [`Protocol`].
pub struct Resp<IO> {
    io: IO,
    /// Read everything from `io`, i.e. it returned 0 at some point.
    read_all: bool,
    /// I/O buffer.
    buf: Vec<u8>,
    /// Amount of bytes processed from `buf`.
    processed: usize,
}

impl<IO> Protocol for Resp<IO>
where
    IO: Connection + 'static,
{
    type NextRequest<'a> = NextRequest<'a, IO>;
    type RequestError = RequestError;
    type Reply<'a, B: Blob> = Reply<'a, B, IO>
    where
        Self: 'a,
        B: Blob + 'a;
    type ReplyWithError<'a> = ReplyWithError<'a, IO>
    where
        Self: 'a;
    type ResponseError = io::Error;

    fn next_request<'a>(&'a mut self, timeout: Duration) -> Self::NextRequest<'a> {
        NextRequest {
            resp: self,
            timeout,
            state: RequestState::Reading,
        }
    }

    fn reply<'a, B>(&'a mut self, response: Response<B>, timeout: Duration) -> Self::Reply<'a, B>
    where
        B: Blob + 'a,
    {
        Reply {
            resp: self,
            response,
            timeout,
        }
    }

    fn reply_to_error<'a>(
        &'a mut self,
        error: Self::RequestError,
        timeout: Duration,
    ) -> Self::ReplyWithError<'a> {
        ReplyWithError {
            resp: self,
            error,
            timeout,
        }
    }
}

pub struct NextRequest<'a, IO> {
    resp: &'a mut Resp<IO>,
    timeout: Duration,
    state: RequestState,
}

#[derive(Copy, Clone)]
enum RequestState {
    /// Reading the next request.
    Reading,
    /// Need to read more bytes to read the next request.
    NeedRead,
    /// Trying to recover from a protocol error.
    Recovery { error: Error, arguments_left: usize },
}

impl<'a, IO> Future for NextRequest<'a, IO>
where
    IO: Connection,
{
    type Output = Result<Option<Request<'a>>, RequestError>;

    fn poll(mut self: Pin<&mut Self>, _: &mut task::Context<'_>) -> Poll<Self::Output> {
        loop {
            match self.state {
                RequestState::Reading => {
                    match self.resp.read_argument()? {
                        Some(Value::Array(Some(length))) => {
                            if length != 2 {
                                // All commands (currently) expect a single
                                // argument (+1 for the command itself).
                                self.state = RequestState::Recovery {
                                    error: if length == 0 {
                                        Error::MISSING_COMMAND
                                    } else {
                                        Error::INVALID_ARGUMENTS
                                    },
                                    arguments_left: length,
                                };
                                // Continue with recovery in the next iteration.
                                continue;
                            }

                            let cmd = match self.resp.read_argument() {
                                Ok(Some(Value::String(Some(idx)))) => &self.resp.buf[idx],
                                Ok(Some(_)) => {
                                    self.state = RequestState::Recovery {
                                        error: Error::INVALID_COMMAND_TYPE,
                                        // Already read command.
                                        arguments_left: length - 1,
                                    };
                                    continue;
                                }
                                Ok(None) => {
                                    todo!("FIXME: need to read more, but also restore the initial array.");
                                }
                                Err(error) => {
                                    self.state = RequestState::Recovery {
                                        error,
                                        arguments_left: length,
                                    };
                                    continue;
                                }
                            };

                            // Already read command.
                            let arguments_left = length - 1;
                            // TODO: DRY this.
                            match cmd {
                                b"GET" => match self.resp.read_key() {
                                    Ok(Some(key)) => {
                                        return Poll::Ready(Ok(Some(Request::GetBlob(key))))
                                    }
                                    Ok(None) => {
                                        todo!("FIXME: need to read more, but also restore the initial array.");
                                    }
                                    Err(error) => {
                                        self.state = RequestState::Recovery {
                                            error,
                                            arguments_left,
                                        };
                                        continue;
                                    }
                                },
                                // NOTE: `ADD` is not a Redis command.
                                b"SET" | b"ADD" => match self.resp.read_string() {
                                    Ok(Some(blob)) => {
                                        // FIXME: remove this lifetime workaround.
                                        let blob = unsafe { std::mem::transmute(blob) };
                                        return Poll::Ready(Ok(Some(Request::AddBlob(blob))));
                                    }
                                    Ok(None) => {
                                        todo!("FIXME: need to read more, but also restore the initial array.");
                                    }
                                    Err(error) => {
                                        self.state = RequestState::Recovery {
                                            error,
                                            arguments_left,
                                        };
                                        continue;
                                    }
                                },
                                b"EXISTS" => match self.resp.read_key() {
                                    Ok(Some(key)) => {
                                        return Poll::Ready(Ok(Some(Request::CointainsBlob(key))));
                                    }
                                    Ok(None) => {
                                        todo!("FIXME: need to read more, but also restore the initial array.");
                                    }
                                    Err(error) => {
                                        self.state = RequestState::Recovery {
                                            error,
                                            arguments_left,
                                        };
                                        continue;
                                    }
                                },
                                b"DEL" => match self.resp.read_key() {
                                    Ok(Some(key)) => {
                                        return Poll::Ready(Ok(Some(Request::RemoveBlob(key))));
                                    }
                                    Ok(None) => {
                                        todo!("FIXME: need to read more, but also restore the initial array.");
                                    }
                                    Err(error) => {
                                        self.state = RequestState::Recovery {
                                            error,
                                            arguments_left,
                                        };
                                        continue;
                                    }
                                },
                                _ => {
                                    self.state = RequestState::Recovery {
                                        error: Error::UNKNOWN_COMMAND,
                                        arguments_left,
                                    };
                                    continue;
                                }
                            }
                        }
                        // Unexpected argument.
                        Some(_) => {
                            // We'll consider this a fatal error because we
                            // can't (reliably) determine where the next request
                            // starts.
                            // TODO: attempt to the above any way.
                            return Poll::Ready(Err(RequestError::User(
                                Error::INVALID_FORMAT,
                                true,
                            )));
                        }
                        // No more requests and no more bytes to read. Job well
                        // done.
                        None if self.resp.read_all => return Poll::Ready(Ok(None)),
                        // Can't read another argument, need to read some more
                        // bytes.
                        None => self.state = RequestState::NeedRead,
                    }
                }
                // We're in need of bytes! Try reading some more.
                RequestState::NeedRead => {
                    let timeout = self.timeout;
                    ready!(self.resp.read(timeout))?;
                    // Try reading commands again.
                    self.state = RequestState::Reading;
                }
                // We're trying to recover from user error.
                RequestState::Recovery {
                    error,
                    arguments_left,
                } => match self.resp.recover(arguments_left) {
                    // Success! Let's try reading again.
                    Ok(0) => self.state = RequestState::Reading,
                    // Need to ignore more arguments, but we read all bytes from
                    // the connection. We'll return the original error and make
                    // it fatal.
                    Ok(_) if self.resp.read_all => {
                        return Poll::Ready(Err(RequestError::User(error, true)));
                    }
                    // Didn't recover fully (yet), try reading.
                    Ok(arguments_left) => {
                        self.state = RequestState::Recovery {
                            error,
                            arguments_left,
                        };
                        let timeout = self.timeout;
                        ready!(self.resp.read(timeout))?;
                    }
                    // Failed to recover, return the original error as fatal.
                    Err(()) => return Poll::Ready(Err(RequestError::User(error, true))),
                },
            }
        }
    }
}

impl<IO> Resp<IO>
where
    IO: Connection,
{
    /// Read a single argument from the connection.
    fn read_argument(&mut self) -> Result<Option<Value>, Error> {
        match parse::argument(self.buf())? {
            Some((mut arg, processed)) => {
                if let Value::String(Some(idx)) | Value::Error(idx) = &mut arg {
                    // Indices are based on the unprocessed bytes.
                    idx.start += self.processed;
                    idx.end += self.processed;
                }
                self.processed += processed;
                Ok(Some(arg))
            }
            None => Ok(None),
        }
    }

    /// Read an argument from the connection, expecting it to be a `Key`. If the
    /// argument is not a valid key it will return an [`Error`].
    fn read_key(&mut self) -> Result<Option<Key>, Error> {
        match self.read_string_idx() {
            Ok(Some(idx)) => match Key::from_byte_str(&self.buf[idx]) {
                Ok(key) => Ok(Some(key)),
                Err(InvalidKeyStr) => Err(Error::INVALID_KEY),
            },
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Read an argument from the connection, expecting it to be a (not nil)
    /// string. If the argument is not a string it will return an [`Error`].
    fn read_string(&mut self) -> Result<Option<&[u8]>, Error> {
        match self.read_string_idx() {
            Ok(Some(idx)) => Ok(Some(&self.buf[idx])),
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Same as [`Connection::read_string`] but returns the index range instead
    /// of the actual string.
    fn read_string_idx(&mut self) -> Result<Option<Range<usize>>, Error> {
        match self.read_argument()? {
            Some(Value::String(Some(idx))) => Ok(Some(idx)),
            Some(_) => Err(Error::INVALID_ARG_TYPE_EXP_STR),
            None => Ok(None),
        }
    }

    /// Returns the unprocessed bytes in the buffer.
    fn buf(&self) -> &[u8] {
        &self.buf[self.processed..]
    }

    /// Read some bytes from the connection into `self.buf`.
    fn read(&mut self, timeout: Duration) -> Poll<Result<(), io::Error>> {
        debug_assert!(!self.read_all);
        match replace(&mut self.processed, 0) {
            n if self.buf.len() == n => self.buf.clear(),
            _ => drop(self.buf.drain(0..self.processed)),
        }
        let buf = replace(&mut self.buf, Vec::new());
        let before = buf.len();
        match self.io.read_into(buf, timeout) {
            Ok(buf) => {
                if buf.len() == before {
                    self.read_all = true;
                }
                self.buf = buf;
                Poll::Ready(Ok(()))
            }
            Err(err) => {
                self.read_all = true;
                Poll::Ready(Err(err))
            }
        }
    }

    /// Attampt to recover from a protocol error, removes `arguments`
    /// arguments from the incoming message queue.
    ///
    /// Returns a boolean indicating whether or not recovery was successful,
    /// i.e. whether or not the arguments were removed.
    fn recover(&mut self, arguments: usize) -> Result<usize, ()> {
        let mut iter = 0..arguments;
        while iter.next().is_some() {
            match self.read_argument() {
                Ok(Some(Value::Array(Some(n)))) => iter.end += n, // Great, more stuff to ignore.
                Ok(Some(_)) => continue,
                // Need to read more.
                Ok(None) => return Ok(iter.end - iter.start),
                // Unexpected user error
                Err(_) => return Err(()),
            }
        }
        Ok(0)
    }
}

pub struct Reply<'a, B, IO> {
    resp: &'a mut Resp<IO>,
    response: Response<B>,
    timeout: Duration,
}

impl<'a, B, IO> Future for Reply<'a, B, IO>
where
    B: Blob,
{
    type Output = Result<(), io::Error>;

    fn poll(self: Pin<&mut Self>, ctx: &mut task::Context<'_>) -> Poll<Self::Output> {
        todo!()
    }
}

pub struct ReplyWithError<'a, IO> {
    resp: &'a mut Resp<IO>,
    error: RequestError,
    timeout: Duration,
}

impl<'a, IO> Future for ReplyWithError<'a, IO> {
    type Output = Result<(), io::Error>;

    fn poll(self: Pin<&mut Self>, ctx: &mut task::Context<'_>) -> Poll<Self::Output> {
        todo!()
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
pub enum RequestError {
    /// User error, e.g. protocol violation or incorrect argument(s).
    User(Error, bool),
    /// I/O error.
    IO(io::Error),
}

impl From<Error> for RequestError {
    fn from(err: Error) -> RequestError {
        RequestError::User(err, false)
    }
}

impl From<io::Error> for RequestError {
    fn from(err: io::Error) -> RequestError {
        RequestError::IO(err)
    }
}

impl IsFatal for RequestError {
    fn is_fatal(&self) -> bool {
        match self {
            RequestError::User(_, fatal) => *fatal,
            RequestError::IO(..) => true,
        }
    }
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestError::User(err, ..) => err.message().fmt(f),
            RequestError::IO(err) => err.fmt(f),
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
    // Not really user errors.
    const UNIMPLEMENTED_INLINE_COMMANDS: Error =
        user_error!("inline commands (simple protocol) not implemented");

    /// Returns the message of this error.
    fn message(&self) -> &'static str {
        debug_assert_eq!(self.0.as_bytes()[0], b'-');
        debug_assert_eq!(&self.0[self.0.len() - 2..], "\r\n");
        &self.0[1..self.0.len() - 2]
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
