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
pub struct Resp<C> {
    /// Underlying connection.
    conn: C,
    /// Read everything from `io`, i.e. it returned 0 at some point.
    read_all: bool,
    /// I/O buffer.
    buf: Vec<u8>,
    /// Amount of bytes processed from `buf`.
    processed: usize,
}

impl<C> Resp<C> {
    /// Preparse the buffer, removing all processed bytes.
    fn prepare_buf(&mut self) {
        match replace(&mut self.processed, 0) {
            // Entire buffer is processed, we can clear it.
            n if self.buf.len() == n => self.buf.clear(),
            // Still have some unprocessed bytes.
            _ => drop(self.buf.drain(0..self.processed)),
        }
    }
}

impl<C> Protocol for Resp<C>
where
    C: Connection + 'static,
{
    type NextRequest<'a> = NextRequest<'a, C>;
    type RequestError = RequestError<C::Error>;
    type Reply<'a, B: Blob> = Reply<'a, B, C>
    where
        Self: 'a,
        B: Blob + 'a;
    type ReplyWithError<'a> = ReplyWithError<'a, C>
    where
        Self: 'a;
    type ResponseError = io::Error;

    fn next_request<'a>(&'a mut self, timeout: Duration) -> Self::NextRequest<'a> {
        NextRequest {
            resp: self,
            timeout,
            state: RequestState::Parsing,
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

/// [`Future`] for reading the next request from [`Resp`].
pub struct NextRequest<'a, C>
where
    C: Connection,
{
    resp: &'a mut Resp<C>,
    timeout: Duration,
    state: RequestState<C::Read<'a>>,
}

/// State of [`NextRequest`]
enum RequestState<Fut> {
    /// Parsing the next argument.
    Parsing,
    /// Need to read more bytes to read the next request.
    NeedRead {
        future: Fut,
        before_length: usize,
        /// State to restore after reading.
        next_state: NextRequestState,
    },
    /// Trying to recover from a protocol error.
    Recovery { error: Error, arguments_left: usize },
}

/// State to transfer to after reading more bytes.
enum NextRequestState {
    /// Continue parsing arguments.
    Parsing,
    /// Trying to recover from a protocol error.
    Recovery { error: Error, arguments_left: usize },
}

impl<'a, C> Future for NextRequest<'a, C>
where
    C: Connection,
{
    type Output = Result<Option<Request<'a>>, RequestError<C::Error>>;

    fn poll(self: Pin<&mut Self>, ctx: &mut task::Context<'_>) -> Poll<Self::Output> {
        // SAFETY: not moving `self` or `this`.
        let this = unsafe { Pin::into_inner_unchecked(self) };
        loop {
            // Bytes already fully processed, used to recover from partial
            // reads.
            let init_processed = this.resp.processed;
            match &mut this.state {
                RequestState::Parsing => match this.resp.parse_argument() {
                    Ok(Some(Value::Array(Some(length)))) => {
                        if length != 2 {
                            // All commands (currently) expect a single argument
                            // (+1 for the command itself).
                            this.state = RequestState::Recovery {
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

                        let cmd = match this.resp.parse_argument() {
                            Ok(Some(Value::String(Some(idx)))) => &this.resp.buf[idx],
                            Ok(Some(_)) => {
                                this.state = RequestState::Recovery {
                                    error: Error::INVALID_COMMAND_TYPE,
                                    // Already read command.
                                    arguments_left: length - 1,
                                };
                                continue;
                            }
                            Ok(None) => {
                                // Restore the argument(s) marked as processed.
                                this.resp.processed = init_processed;
                                this.start_read(NextRequestState::Parsing);
                                continue;
                            }
                            // Fatal error reading the argument.
                            Err(err) => return Poll::Ready(Err(RequestError::User(err, true))),
                        };

                        // Already read command.
                        let arguments_left = length - 1;
                        // TODO: DRY this.
                        match cmd {
                            b"GET" => match this.resp.parse_key() {
                                Ok(Some(key)) => {
                                    return Poll::Ready(Ok(Some(Request::GetBlob(key))))
                                }
                                Ok(None) => {
                                    // Restore the argument(s) marked as processed.
                                    this.resp.processed = init_processed;
                                    this.start_read(NextRequestState::Parsing);
                                }
                                Err(error) => {
                                    this.state = RequestState::Recovery {
                                        error,
                                        arguments_left,
                                    };
                                }
                            },
                            // NOTE: `ADD` is not a Redis command.
                            b"SET" | b"ADD" => match this.resp.parse_string() {
                                Ok(Some(blob)) => {
                                    // FIXME: remove this lifetime workaround.
                                    let blob = unsafe { std::mem::transmute(blob) };
                                    return Poll::Ready(Ok(Some(Request::AddBlob(blob))));
                                }
                                Ok(None) => {
                                    // Restore the argument(s) marked as processed.
                                    this.resp.processed = init_processed;
                                    this.start_read(NextRequestState::Parsing);
                                }
                                Err(error) => {
                                    this.state = RequestState::Recovery {
                                        error,
                                        arguments_left,
                                    };
                                }
                            },
                            b"EXISTS" => match this.resp.parse_key() {
                                Ok(Some(key)) => {
                                    return Poll::Ready(Ok(Some(Request::CointainsBlob(key))));
                                }
                                Ok(None) => {
                                    // Restore the argument(s) marked as processed.
                                    this.resp.processed = init_processed;
                                    this.start_read(NextRequestState::Parsing);
                                }
                                Err(error) => {
                                    this.state = RequestState::Recovery {
                                        error,
                                        arguments_left,
                                    };
                                }
                            },
                            b"DEL" => match this.resp.parse_key() {
                                Ok(Some(key)) => {
                                    return Poll::Ready(Ok(Some(Request::RemoveBlob(key))))
                                }
                                Ok(None) => {
                                    // Restore the argument(s) marked as processed.
                                    this.resp.processed = init_processed;
                                    this.start_read(NextRequestState::Parsing);
                                }
                                Err(error) => {
                                    this.state = RequestState::Recovery {
                                        error,
                                        arguments_left,
                                    };
                                }
                            },
                            _ => {
                                this.state = RequestState::Recovery {
                                    error: Error::UNKNOWN_COMMAND,
                                    arguments_left,
                                };
                            }
                        }
                    }
                    // Unexpected argument.
                    Ok(Some(_)) => {
                        // We'll consider this a fatal error because we can't
                        // (reliably) determine where the next request starts.
                        // TODO: attempt to the above any way.
                        return Poll::Ready(Err(RequestError::User(Error::INVALID_FORMAT, true)));
                    }
                    // Can't read another argument, need to read some more
                    // bytes.
                    Ok(None) => this.start_read(NextRequestState::Parsing),
                    // Can't recover from this protocol error.
                    Err(err) => return Poll::Ready(Err(RequestError::User(err, true))),
                },
                // We're in need of bytes! Try reading some more.
                RequestState::NeedRead {
                    future,
                    before_length,
                    next_state,
                } => match unsafe { Pin::new_unchecked(&mut *future).poll(ctx) } {
                    Poll::Ready(Ok(buf)) => {
                        if buf.len() == *before_length {
                            // Read 0 bytes.
                            this.resp.read_all = true;
                            return match next_state {
                                // No more requests and no more bytes to read.
                                // Job well done.
                                NextRequestState::Parsing => Poll::Ready(Ok(None)),
                                // Couldn't recover from the error, so we'll
                                // return it and mark it as fatal.
                                NextRequestState::Recovery { error, .. } => {
                                    Poll::Ready(Err(RequestError::User(*error, true)))
                                }
                            };
                        }
                        this.resp.buf = buf;
                        match next_state {
                            NextRequestState::Parsing => this.state = RequestState::Parsing,
                            NextRequestState::Recovery {
                                error,
                                arguments_left,
                            } => {
                                this.state = RequestState::Recovery {
                                    error: *error,
                                    arguments_left: *arguments_left,
                                }
                            }
                        }
                    }
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(RequestError::Conn(err))),
                    Poll::Pending => return Poll::Pending,
                },
                // We're trying to recover from user error.
                RequestState::Recovery {
                    error,
                    arguments_left,
                } => match this.resp.recover(*arguments_left) {
                    // Success! We'll return an error to the user, but won't
                    // mark it as fatal.
                    Ok(0) => return Poll::Ready(Err(RequestError::User(*error, false))),
                    // Need to ignore more arguments, but we read all bytes from
                    // the connection. We'll return the original error and make
                    // it fatal.
                    Ok(_) if this.resp.read_all => {
                        return Poll::Ready(Err(RequestError::User(*error, true)));
                    }
                    // Didn't recover fully (yet), try reading.
                    Ok(arguments_left) => {
                        let error = *error;
                        this.start_read(NextRequestState::Recovery {
                            error,
                            arguments_left,
                        });
                    }
                    // Failed to recover, return the original error as fatal.
                    Err(()) => return Poll::Ready(Err(RequestError::User(*error, true))),
                },
            }
        }
    }
}

/// Reading methods.
impl<C> Resp<C>
where
    C: Connection,
{
    /// Parse a single argument from the connection.
    ///
    /// Returns `None` if no complete argument is in the current buffer.
    fn parse_argument(&mut self) -> Result<Option<Value>, Error> {
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

    /// Parse an argument from the connection, expecting it to be a `Key`. If
    /// the argument is not a valid key it will return an [`Error`].
    ///
    /// Returns `None` if no complete argument is in the current buffer.
    fn parse_key(&mut self) -> Result<Option<Key>, Error> {
        match self.parse_string_idx() {
            Ok(Some(idx)) => match Key::from_byte_str(&self.buf[idx]) {
                Ok(key) => Ok(Some(key)),
                Err(InvalidKeyStr) => Err(Error::INVALID_KEY),
            },
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Parse an argument from the connection, expecting it to be a (not nil)
    /// string. If the argument is not a string it will return an [`Error`].
    ///
    /// Returns `None` if the no complete argument is in the current buffer.
    fn parse_string(&mut self) -> Result<Option<&[u8]>, Error> {
        match self.parse_string_idx() {
            Ok(Some(idx)) => Ok(Some(&self.buf[idx])),
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Same as [`Resp::parse_string`] but returns the index range instead of
    /// the actual string.
    fn parse_string_idx(&mut self) -> Result<Option<Range<usize>>, Error> {
        match self.parse_argument()? {
            Some(Value::String(Some(idx))) => Ok(Some(idx)),
            Some(_) => Err(Error::INVALID_ARG_TYPE_EXP_STR),
            None => Ok(None),
        }
    }

    /// Returns the unprocessed bytes in the buffer.
    fn buf(&self) -> &[u8] {
        &self.buf[self.processed..]
    }

    /// Attempt to recover from a protocol error, removes `arguments` arguments
    /// from the incoming message queue.
    ///
    /// If this returns `Ok(0)` it means all argument where succesfully removed
    /// from the incoming queue. If this returns `Ok(n)`, where n >= 1, it means
    /// that `n` arguments still have to be removed from the queue. Finally, if
    /// this returns `Err(())` we failed to remove the arguments from the queue
    /// and it should be considered broken.
    fn recover(&mut self, arguments: usize) -> Result<usize, ()> {
        let mut iter = 0..arguments;
        while iter.next().is_some() {
            match self.parse_argument() {
                Ok(Some(Value::Array(Some(n)))) => iter.end += n, // Great, more stuff to ignore.
                Ok(Some(_)) => continue,
                // Need to read more.
                Ok(None) => return Ok(iter.end - iter.start),
                // Unexpected user error.
                Err(_) => return Err(()),
            }
        }
        Ok(0)
    }
}

impl<'a, C> NextRequest<'a, C>
where
    C: Connection,
{
    /// Set the state to reading.
    fn start_read(&mut self, next_state: NextRequestState) {
        debug_assert!(!self.resp.read_all);
        self.resp.prepare_buf();
        let buf = replace(&mut self.resp.buf, Vec::new());
        let before_length = buf.len();
        let future = self.resp.conn.read_into(buf, self.timeout);
        let future = unsafe { std::mem::transmute(future) }; // FIXME: work around for lifetime.
        self.state = RequestState::NeedRead {
            future,
            before_length,
            next_state,
        };
    }
}

/// [`Future`] for replying to a request for [`Resp`].
pub struct Reply<'a, B, C> {
    resp: &'a mut Resp<C>,
    response: Response<B>,
    timeout: Duration,
}

impl<'a, B, C> Future for Reply<'a, B, C>
where
    B: Blob,
{
    type Output = Result<(), io::Error>;

    fn poll(self: Pin<&mut Self>, ctx: &mut task::Context<'_>) -> Poll<Self::Output> {
        todo!("TODO: RESP::reply")
    }
}

/// [`Future`] for replying to a request for [`Resp`] with an error.
pub struct ReplyWithError<'a, C>
where
    C: Connection,
{
    resp: &'a mut Resp<C>,
    error: RequestError<C::Error>,
    timeout: Duration,
}

impl<'a, C> Future for ReplyWithError<'a, C>
where
    C: Connection,
{
    type Output = Result<(), io::Error>;

    fn poll(self: Pin<&mut Self>, ctx: &mut task::Context<'_>) -> Poll<Self::Output> {
        todo!("TODO: RESP::reply_to_error")
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
