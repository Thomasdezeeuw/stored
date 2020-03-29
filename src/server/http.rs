//! Module that parses HTTP/1.1 requests.

// TODO: add tests.

use std::convert::TryFrom;
use std::error::Error;
use std::future::Future;
use std::io::{self, IoSlice, Write};
use std::marker::Unpin;
use std::mem::replace;
use std::net::Shutdown;
use std::pin::Pin;
use std::task::{self, Poll};
use std::{fmt, str};

use chrono::{DateTime, Datelike, Timelike, Utc};
use futures_io::{AsyncRead, AsyncWrite};
use heph::net::TcpStream;
use httparse::EMPTY_HEADER;

use crate::key::InvalidKeyStr;
use crate::server::storage::Blob;
use crate::{Buffer, Key, WriteBuffer};

/// Maximum number of headers read from an incoming request.
pub const MAX_HEADERS: usize = 16;

/// `Connection` types that wraps a stream based I/O, e.g. a TCP stream.
///
/// # Notes
///
/// The `IO` is not buffered.
pub struct Connection<IO> {
    buf: Buffer,
    io: IO,
}

impl<IO> Connection<IO> {
    /// Create a new `Connection`.
    pub fn new(io: IO) -> Connection<IO> {
        Connection {
            buf: Buffer::new(),
            io,
        }
    }
}

impl Connection<TcpStream> {
    /// Close the connection.
    pub fn close(mut self) -> io::Result<()> {
        self.io.shutdown(Shutdown::Both)
    }
}

impl<IO> Connection<IO>
where
    IO: AsyncRead,
{
    /// Returns a [`Future`] that parses an HTTP/1.1 request and returns a
    /// [`Request`].
    pub fn read_header(&mut self) -> ReadRequest<IO> {
        ReadRequest {
            buf: &mut self.buf,
            io: &mut self.io,
            too_short: 0,
        }
    }

    /// Read a body of `size`.
    pub fn read_body(&mut self, size: usize) -> ReadBody<IO> {
        self.buf.reserve_atleast(size);
        ReadBody {
            buf: Some(&mut self.buf),
            io: &mut self.io,
            want_length: size,
        }
    }
}

/// [`Future`] to read a [`Request`] from a [`Connection`].
pub struct ReadRequest<'c, IO> {
    buf: &'c mut Buffer,
    io: &'c mut IO,
    /// Number of bytes that are too short to read an entire request.
    too_short: usize,
}

impl<'c, IO> Future for ReadRequest<'c, IO>
where
    IO: AsyncRead + Unpin,
{
    type Output = Result<Request, RequestError>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut task::Context) -> Poll<Self::Output> {
        loop {
            // self.buf.len() <= self.too_short
            // At the start we don't have enough bytes to read the entire
            // request, so we need to read some more.
            let ReadRequest { buf, io, .. } = &mut *self;
            let mut read_future = buf.read_from(io);
            match Pin::new(&mut read_future).poll(ctx)? {
                // Read all bytes, but don't have a complete HTTP request yet.
                Poll::Ready(0) => {
                    let err = RequestError {
                        is_head: false, // Don't know...
                        kind: RequestErrorKind::Incomplete,
                    };
                    return Poll::Ready(Err(err));
                }
                Poll::Ready(_) => (),
                // Didn't read any more bytes, try again later.
                Poll::Pending => return Poll::Pending,
            }

            let mut headers = [EMPTY_HEADER; MAX_HEADERS];
            let mut req = httparse::Request::new(&mut headers);
            match req.parse(self.buf.as_bytes()) {
                Ok(httparse::Status::Complete(bytes_read)) => {
                    // NOTE: don't early return here, we **always** need to mark
                    // the bytes as processed so we don't process them again.
                    let request = Request::try_from(req);
                    self.buf.processed(bytes_read);
                    return Poll::Ready(request);
                }
                Ok(httparse::Status::Partial) => {
                    // Need to read some more bytes.
                    self.too_short = self.buf.len();
                    continue;
                }
                Err(err) => return Poll::Ready(Err(err.into())),
            }
        }
    }
}

/// Parsed request.
#[derive(Debug, Eq, PartialEq)]
pub enum Request {
    /// POST request to store a blob, returns the length of the blob.
    ///
    /// Body is the blob to store, with length the length provided. Note: the
    /// length might be invalid as its user-supplied (Content-Length header).
    Post(usize),
    /// GET request for the blob with `key`.
    ///
    /// No body.
    Get(Key),
    /// HEAD request for the blob with `key`.
    ///
    /// No body.
    Head(Key),
    /// DELETE request for the blob with `key`.
    ///
    /// No body.
    Delete(Key),
}

impl Request {
    /// Returns the HTTP method for the request.
    pub fn method(&self) -> &'static str {
        use Request::*;
        match self {
            Post(_) => "POST",
            Get(_) => "GET",
            Head(_) => "HEAD",
            Delete(_) => "DELETE",
        }
    }

    /// Returns `true` if this is a HEAD request.
    pub fn is_head(&self) -> bool {
        use Request::*;
        match self {
            Head(_) => true,
            _ => false,
        }
    }
}

impl<'headers, 'buf> TryFrom<httparse::Request<'headers, 'buf>> for Request {
    type Error = RequestError;

    /// Returns an `(err, bool)` in case of an error, where the bool is `true`
    /// if it was a HEAD request.
    fn try_from(req: httparse::Request<'_, '_>) -> Result<Self, Self::Error> {
        // Path prefix to post/get/head/delete a blob.
        const BLOB_PATH_PREFIX: &str = "/blob/";
        // Required content length header for post requests.
        const CONTENT_LENGTH: &str = "content-length";

        let res: Result<Self, RequestErrorKind> = match (req.method, req.path) {
            (Some("POST"), Some("/blob")) | (Some("POST"), Some("/blob/")) => {
                let text_length = req.headers.iter().find_map(|header| {
                    (header.name.to_lowercase() == CONTENT_LENGTH).then_some(header.value)
                });

                if let Some(text_length) = text_length {
                    str::from_utf8(text_length)
                        .map_err(|_err| RequestErrorKind::InvalidContentLength)
                        .and_then(|str_length| {
                            str_length
                                .parse()
                                .map_err(|_err| RequestErrorKind::InvalidContentLength)
                        })
                        .map(|length| Request::Post(length))
                } else {
                    Err(RequestErrorKind::MissingContentLength)
                }
            }
            // TODO: DRY the next three routes.
            (Some("GET"), Some(path)) if path.starts_with(BLOB_PATH_PREFIX) => {
                // Safety: just check the prefix.
                path[BLOB_PATH_PREFIX.len()..]
                    .parse()
                    .map(|key| Request::Get(key))
                    .map_err(Into::into)
            }
            (Some("HEAD"), Some(path)) if path.starts_with(BLOB_PATH_PREFIX) => {
                // Safety: just check the prefix.
                path[BLOB_PATH_PREFIX.len()..]
                    .parse()
                    .map(|key| Request::Head(key))
                    .map_err(Into::into)
            }
            (Some("DELETE"), Some(path)) if path.starts_with(BLOB_PATH_PREFIX) => {
                // Safety: just check the prefix.
                path[BLOB_PATH_PREFIX.len()..]
                    .parse()
                    .map(|key| Request::Delete(key))
                    .map_err(Into::into)
            }
            // After parsing a request `method` and `path` should always be
            // some, so we're left with an invalid route, aka 404.
            _ => Err(RequestErrorKind::InvalidRoute),
        };

        res.map_err(|kind| RequestError {
            is_head: req.method == Some("HEAD"),
            kind,
        })
    }
}

/// Error returned by [`ReadRequest`].
#[derive(Debug)]
pub struct RequestError {
    is_head: bool,
    kind: RequestErrorKind,
}

impl RequestError {
    /// Returns `true` if the request was a HEAD request (and thus shouldn't
    /// return a body).
    ///
    /// # Notes
    ///
    /// In case of an I/O error ([`RequestErrorKind::Io`]) or parsing error
    /// ([`RequestErrorKind::Parse`]) this will likely always return `false`,
    /// even if the request was a HEAD request.
    pub const fn is_head(&self) -> bool {
        self.is_head
    }

    /// Returns the kind of error.
    pub fn kind(self) -> RequestErrorKind {
        self.kind
    }
}

impl From<io::Error> for RequestError {
    fn from(err: io::Error) -> RequestError {
        RequestError {
            is_head: false,
            kind: RequestErrorKind::Io(err),
        }
    }
}

impl From<httparse::Error> for RequestError {
    fn from(err: httparse::Error) -> RequestError {
        RequestError {
            is_head: false,
            kind: RequestErrorKind::Parse(err),
        }
    }
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.kind.fmt(f)
    }
}

impl Error for RequestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        use RequestErrorKind::*;
        match &self.kind {
            Io(err) => Some(err),
            Parse(err) => Some(err),
            InvalidKey(err) => Some(err),
            Incomplete | InvalidRoute | MissingContentLength | InvalidContentLength => None,
        }
    }
}

/// Kind of request error.
#[derive(Debug)]
pub enum RequestErrorKind {
    /// I/O error.
    Io(io::Error),
    /// Error parsing the HTTP response.
    Parse(httparse::Error),
    /// Retrieve an incomplete HTTP request.
    Incomplete,
    /// Invalid method/path combination.
    InvalidRoute,
    /// Post request is missing the content length header.
    MissingContentLength,
    /// Post request has an invalid content length header.
    InvalidContentLength,
    /// Invalid key in path.
    InvalidKey(InvalidKeyStr),
}

impl From<InvalidKeyStr> for RequestErrorKind {
    fn from(err: InvalidKeyStr) -> RequestErrorKind {
        RequestErrorKind::InvalidKey(err)
    }
}

impl fmt::Display for RequestErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use RequestErrorKind::*;
        match self {
            Io(err) => write!(f, "I/O error: {}", err),
            Parse(err) => write!(f, "HTTP parsing error: {}", err),
            Incomplete => write!(f, "incomplete HTTP request"),
            InvalidRoute => write!(f, "invalid route"),
            MissingContentLength => write!(f, "request missing Content-Length header"),
            InvalidContentLength => write!(f, "request's Content-Length header is invalid"),
            InvalidKey(err) => write!(f, "key in route is invalid: {}", err),
        }
    }
}

/// [`Future`] to read the request's body from a [`Connection`].
pub struct ReadBody<'c, IO> {
    buf: Option<&'c mut Buffer>,
    io: &'c mut IO,
    want_length: usize,
}

/// The body of a HTTP request.
pub struct Body<'c> {
    buf: &'c mut Buffer,
}

impl<'c> Body<'c> {
    /// Replace the current buffer with an empty `Buffer`. Returns the current
    /// buffer.
    pub fn take(&mut self) -> Buffer {
        self.replace(Buffer::empty())
    }

    /// Replace the current buffer with `buf`. Returns the current buffer.
    pub fn replace(&mut self, buf: Buffer) -> Buffer {
        replace(self.buf, buf)
    }
}

impl<'c, IO> Future for ReadBody<'c, IO>
where
    IO: AsyncRead + Unpin,
{
    type Output = io::Result<Body<'c>>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut task::Context) -> Poll<Self::Output> {
        // TODO: remove the `unwrap`s here.
        while self.buf.as_ref().unwrap().len() < self.want_length {
            // Need more bytes.
            let ReadBody { buf, io, .. } = &mut *self;
            let mut read_future = buf.as_mut().unwrap().read_from(io);
            match Pin::new(&mut read_future).poll(ctx)? {
                // Read all bytes.
                Poll::Ready(0) => break,
                Poll::Ready(_) => continue,
                // Didn't read any more bytes, try again later.
                Poll::Pending => return Poll::Pending,
            }
        }

        return Poll::Ready(Ok(Body {
            buf: self.buf.take().unwrap(),
        }));
    }
}

impl<IO> Connection<IO>
where
    IO: AsyncWrite,
{
    /// Returns a [`Future`] that writes an HTTP/1.1 [`Response`].
    pub fn write_response<'a>(&'a mut self, response: &'a Response) -> WriteResponse<'a, IO> {
        let mut write_buf = self.buf.write_buf();
        write_buf.reserve_atleast(Response::MAX_HEADERS_SIZE);
        response.write_headers(&mut write_buf);
        // TODO: replace with `write_all_vectored`:
        // https://github.com/rust-lang/futures-rs/pull/1741.
        WriteResponse {
            state: WrittenState::Both(write_buf, response.body()),
            io: &mut self.io,
        }
    }
}

/// HTTP response.
pub struct Response {
    /// Request was a HEAD request.
    is_head: bool,
    kind: ResponseKind,
}

/// HTTP response kind.
pub enum ResponseKind {
    /// Blob was stored.
    /// Response to POST.
    ///
    /// 201 Created. No body, Location header set to "/blob/$key".
    Stored(Key),

    /// Blob found.
    /// Response to GET response.
    ///
    /// 200 Ok. Body is the blob (the passed bytes).
    Ok(Blob),

    /// Blob deleted.
    /// Response to DELETE.
    ///
    /// 204 No Content. No body.
    Deleted,

    // Errors:
    /// Blob not found.
    /// Possibly returned by GET, HEAD or DELETE, and a response for
    /// `RequestError::InvalidRoute`.
    ///
    /// 404 Not found. No body.
    NotFound,
    /// Too many headers.
    /// Response `RequestError::Parse(httparse::Error::TooManyHeaders)`.
    ///
    /// 431 Request Header Fields Too Large. No body.
    TooManyHeaders,
    /// Request didn't have a content length.
    /// Response to `RequestError::MissingContentLength` and
    /// `RequestError::InvalidContentLength`.
    ///
    /// 411 Length Required. No body.
    NoContentLength,
    /// Payload too large.
    /// Response when the body is too large.
    ///
    /// 413 Payload Too Large. No body.
    // TODO: maybe the body should be the maximum body length?
    TooLargePayload,
    /// Invalid key provided.
    /// Response for `RequestError::InvalidKey`.
    ///
    /// 400 Bad Request. Body is an error message.
    InvalidKey,
    /// Malformed request.
    /// Response to all `httparse::Error` errors, except for `TooManyHeaders`.
    ///
    /// 400 Bad Request. Body is the provided error message.
    BadRequest(&'static str),

    /// Server error.
    /// Response if something unexpected doesn't work.
    ///
    /// 500 Internal Server Error. No body.
    ServerError,
}

/// Append a header with a date format to `buf`.
fn append_date_header(timestamp: &DateTime<Utc>, header_name: &str, buf: &mut WriteBuffer) {
    static MONTHS: [&'static str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    write!(
        buf,
        // <header_name>: <day-name>, <day> <month> <year> <hour>:<minute>:<second> GMT
        "{}: {}, {:02} {} {:004} {:02}:{:02}:{:02} GMT\r\n",
        header_name,
        timestamp.weekday(),
        timestamp.day(),
        MONTHS[timestamp.month0() as usize],
        timestamp.year(),
        timestamp.hour(),
        timestamp.minute(),
        timestamp.second(),
    )
    .unwrap()
}

impl Response {
    /// Returns a new `Response`.
    pub const fn new(is_head: bool, kind: ResponseKind) -> Response {
        Response { is_head, kind }
    }

    /// Returns the correct `Response` for a `RequestError`, returns an
    /// [`io::Error`] if the request error kind is I/O
    /// ([`RequestErrorKind::Io`])
    pub fn for_error(req_err: RequestError) -> io::Result<Response> {
        let is_head = req_err.is_head;
        use RequestErrorKind::*;
        let kind = match req_err.kind {
            Io(err) => return Err(err),
            // HTTP parsing errors.
            Parse(httparse::Error::HeaderName) => ResponseKind::BadRequest("invalid header name"),
            Parse(httparse::Error::HeaderValue) => ResponseKind::BadRequest("invalid header value"),
            Parse(httparse::Error::NewLine)
            | Parse(httparse::Error::Status)
            | Parse(httparse::Error::Token)
            | Parse(httparse::Error::Version) => {
                ResponseKind::BadRequest("invalid HTTP request format")
            }
            Incomplete => ResponseKind::BadRequest("incomplete HTTP request"),
            Parse(httparse::Error::TooManyHeaders) => ResponseKind::TooManyHeaders,
            // 404 Not found.
            InvalidRoute => ResponseKind::NotFound,
            // Always need a Content-Length header for `Post` requests.
            MissingContentLength | InvalidContentLength => ResponseKind::NoContentLength,
            // Invalid key format in "/blob/$key" path.
            InvalidKey(_err) => ResponseKind::InvalidKey,
        };
        Ok(Response { is_head, kind })
    }

    /// Maximum size of `write_headers`.
    const MAX_HEADERS_SIZE: usize =
        // Status line, +31 for the longest reason (`TooManyHeaders`).
        15 + 31 +
        // Server and Content-Length (+39 max. length as text), Date headers.
        16 + 18 + 39 + 37 +
        // Extra headers: Location header (the longest).
        149;

    /// Write all headers to `buf`, including the last empty line.
    fn write_headers(&self, buf: &mut WriteBuffer) {
        let (status_code, status_msg) = self.status_code();
        let content_length = self.len();

        write!(
            buf,
            "HTTP/1.1 {} {}\r\nServer: stored\r\nContent-Length: {}\r\n",
            status_code, status_msg, content_length,
        )
        .unwrap();
        append_date_header(&Utc::now(), "Date", buf);

        use ResponseKind::*;
        match &self.kind {
            Stored(key) => {
                // Set the Location header to point to the blob.
                write!(buf, "Location: /blob/{}\r\n", key).unwrap()
            }
            Ok(blob) => {
                let timestamp: DateTime<Utc> = blob.created_at().into();
                append_date_header(&timestamp, "Last-Modified", buf);
            }
            NotFound | TooManyHeaders | NoContentLength | TooLargePayload | InvalidKey
            | BadRequest(_) | ServerError => {
                // The body will is an (error) message in plain text, UTF-8. For
                // errors we want to close the connection.
                write!(
                    buf,
                    "Content-Type: text/plain; charset=utf-8\r\nConnection: close\r\n"
                )
                .unwrap()
            }
            Deleted => {}
        }

        write!(buf, "\r\n").unwrap();
    }

    /// Returns the status code and reason.
    pub fn status_code(&self) -> (u16, &'static str) {
        use ResponseKind::*;
        match &self.kind {
            Stored(_) => (201, "Created"),
            Ok(_) => (200, "OK"),
            Deleted => (204, "No Content"),
            NotFound => (404, "Not Found"),
            TooManyHeaders => (431, "Request Header Fields Too Large"),
            NoContentLength => (411, "Length Required"),
            TooLargePayload => (413, "Payload Too Large"),
            InvalidKey | BadRequest(_) => (400, "Bad Request"),
            ServerError => (500, "Internal Server Error"),
        }
    }

    /// Returns the body for the response.
    ///
    /// For HEAD requests this will always be empty.
    fn body(&self) -> &[u8] {
        if self.is_head {
            // Responses to HEAD request MUST NOT have a body.
            b""
        } else {
            self.kind.body()
        }
    }

    /// Returns the Content-Length.
    ///
    /// # Notes
    ///
    /// This is not the same as `body().len()`, as that will be 0 for bodies
    /// responding to HEAD requests, this will return the correct length.
    fn len(&self) -> usize {
        self.kind.body().len()
    }
}

impl ResponseKind {
    /// Returns the body for the response.
    fn body(&self) -> &[u8] {
        use ResponseKind::*;
        match &self {
            Stored(_) | Deleted => b"",
            Ok(blob) => blob.bytes(),
            NotFound => b"Not found",
            TooManyHeaders => b"Too many headers",
            NoContentLength => b"Missing required content length header",
            TooLargePayload => b"Blob too large",
            InvalidKey => b"Invalid key",
            BadRequest(msg) => msg.as_bytes(),
            ServerError => b"Internal server error",
        }
    }
}

/// [`Future`] to write a [`Response`] to a [`Connection`].
pub struct WriteResponse<'c, IO> {
    state: WrittenState<'c>,
    io: &'c mut IO,
}

/// Status of [`WriteResponse`].
enum WrittenState<'c> {
    /// Write the headers and body.
    Both(WriteBuffer<'c>, &'c [u8]),
    /// Write the body.
    Body(&'c [u8]),
}

impl<'c, IO> Future for WriteResponse<'c, IO>
where
    IO: AsyncWrite + Unpin,
{
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, ctx: &mut task::Context) -> Poll<Self::Output> {
        let mut this = Pin::into_inner(self);
        loop {
            let WriteResponse { state, io } = &mut this;
            use WrittenState::*;
            match state {
                Both(headers, body) => {
                    let bufs = &[IoSlice::new(headers.as_bytes()), IoSlice::new(body)];
                    match Pin::new(io).poll_write_vectored(ctx, bufs) {
                        Poll::Ready(Ok(written)) => {
                            let headers_len = headers.len();
                            if written >= headers_len + body.len() {
                                // Written all headers and the body.
                                return Poll::Ready(Ok(()));
                            } else if written >= headers_len {
                                // Written all headers, just (part of) the body
                                // left.
                                *state = WrittenState::Body(&body[written - headers_len..]);
                            } else {
                                // Only written part of the headers.
                                headers.processed(written);
                            }
                        }
                        Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                        Poll::Pending => return Poll::Pending,
                    }
                }
                Body(body) => match Pin::new(io).poll_write(ctx, body) {
                    Poll::Ready(Ok(written)) if written >= body.len() => {
                        // Written the entire body.
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Ok(written)) => {
                        // Only written part of the body.
                        *state = WrittenState::Body(&body[written..]);
                    }
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Pending => return Poll::Pending,
                },
            }
        }
    }
}
