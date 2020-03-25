//! Module that parses HTTP/1.1 requests.

// TODO: add tests.

use std::convert::TryFrom;
use std::error::Error;
use std::future::Future;
use std::io::{self, Write};
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
use crate::{Buffer, Key};

/// Maximum number of headers read from an incoming request.
pub const MAX_HEADERS: usize = 16;

/// `Connection` types that wraps a stream based I/O, e.g. a TCP stream.
///
/// # Notes
///
/// The `IO` is not buffered.
pub struct Connection<IO> {
    buf: Buffer,
    write_buf: Vec<u8>,
    io: IO,
}

impl<IO> Connection<IO> {
    /// Create a new `Connection`.
    pub fn new(io: IO) -> Connection<IO> {
        Connection {
            buf: Buffer::new(),
            write_buf: Vec::new(),
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
            if self.buf.len() <= self.too_short {
                // Don't have enough bytes to read the entire request, so we
                // need to read some more.
                let ReadRequest { buf, io, .. } = &mut *self;
                let mut read_future = buf.read_from(io);
                if Pin::new(&mut read_future).poll(ctx)?.is_pending() {
                    // Didn't read any more bytes, try again later.
                    return Poll::Pending;
                }
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
}

impl<'headers, 'buf> TryFrom<httparse::Request<'headers, 'buf>> for Request {
    type Error = RequestError;

    fn try_from(req: httparse::Request<'_, '_>) -> Result<Self, Self::Error> {
        // Path prefix to post/get/head/delete a blob.
        const BLOB_PATH_PREFIX: &str = "/blob/";
        // Required content length header for post requests.
        const CONTENT_LENGTH: &str = "Content-Length";

        match (req.method, req.path) {
            (Some("POST"), Some("/blob")) | (Some("POST"), Some("/blob/")) => {
                let text_length = req
                    .headers
                    .iter()
                    .find_map(|header| (header.name == CONTENT_LENGTH).then_some(header.value));

                if let Some(text_length) = text_length {
                    str::from_utf8(text_length)
                        .map_err(|_err| RequestError::InvalidContentLength)
                        .and_then(|str_length| {
                            str_length
                                .parse()
                                .map_err(|_err| RequestError::InvalidContentLength)
                        })
                        .map(|length| Request::Post(length))
                } else {
                    Err(RequestError::MissingContentLength)
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
            (Some("HEAD"), _) => Err(RequestError::InvalidRouteNoBody),
            _ => Err(RequestError::InvalidRoute),
        }
    }
}

/// Error returned by [`ReadRequest`].
#[derive(Debug)]
pub enum RequestError {
    /// I/O error.
    Io(io::Error),
    /// Error parsing the HTTP response.
    Parse(httparse::Error),
    /// Invalid method/path combination.
    InvalidRoute,
    /// Same as `InvalidRoute` but for a HEAD request.
    InvalidRouteNoBody,
    /// Post request is missing the content length header.
    MissingContentLength,
    /// Post request has an invalid content length header.
    InvalidContentLength,
    /// Invalid key in path.
    InvalidKey(InvalidKeyStr),
}

impl From<io::Error> for RequestError {
    fn from(err: io::Error) -> RequestError {
        RequestError::Io(err)
    }
}

impl From<httparse::Error> for RequestError {
    fn from(err: httparse::Error) -> RequestError {
        RequestError::Parse(err)
    }
}

impl From<InvalidKeyStr> for RequestError {
    fn from(err: InvalidKeyStr) -> RequestError {
        RequestError::InvalidKey(err)
    }
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use RequestError::*;
        match self {
            Io(err) => write!(f, "I/O error: {}", err),
            Parse(err) => write!(f, "HTTP parsing error: {}", err),
            InvalidRoute | InvalidRouteNoBody => write!(f, "invalid route"),
            MissingContentLength => write!(f, "request missing Content-Length header"),
            InvalidContentLength => write!(f, "request's Content-Length header is invalid"),
            InvalidKey(err) => write!(f, "key in route is invalid: {}", err),
        }
    }
}

impl Error for RequestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        use RequestError::*;
        match self {
            Io(err) => Some(err),
            Parse(err) => Some(err),
            InvalidKey(err) => Some(err),
            InvalidRoute | InvalidRouteNoBody | MissingContentLength | InvalidContentLength => None,
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
            if Pin::new(&mut read_future).poll(ctx)?.is_pending() {
                // Didn't read any more bytes, try again later.
                return Poll::Pending;
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
        self.write_buf.clear();
        response.write_headers(&mut self.write_buf);
        // TODO: replace with `write_all_vectored`:
        // https://github.com/rust-lang/futures-rs/pull/1741.
        WriteResponse {
            state: WrittenState::Both(&self.write_buf, response.body()),
            io: &mut self.io,
        }
    }
}

/// HTTP response.
pub enum Response {
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

    /// Blob found.
    /// Response to HEAD response.
    ///
    /// 204 No Content. No body.
    OkNobody(Blob),

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
    /// Same as `NotFound`, but to a HEAD request.
    NotFoundNoBody,
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
    /// Same as `ServerError`, but to a HEAD request.
    ServerErrorNoBody,
}

fn append_date_header(timestamp: &DateTime<Utc>, header_name: &str, buf: &mut Vec<u8>) {
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
    /// Write all headers to `buf`, including the last empty line.
    fn write_headers(&self, buf: &mut Vec<u8>) {
        let (status_code, status_msg) = self.status_code();
        let content_length = self.len();

        write!(
            buf,
            "HTTP/1.1 {} {}\r\nServer: stored\r\nContent-Length: {}\r\n",
            status_code, status_msg, content_length,
        )
        .unwrap();
        append_date_header(&Utc::now(), "Date", buf);

        use Response::*;
        match self {
            Stored(key) => {
                // Binary content and set the location of the blob.
                write!(
                    buf,
                    "Content-Type: application/octet-stream\r\nLocation: /blob/{}\r\n",
                    key
                )
                .unwrap()
            }
            Ok(blob) | OkNobody(blob) => {
                let timestamp: DateTime<Utc> = blob.created_at().into();
                append_date_header(&timestamp, "Last-Modified", buf);
            }
            Deleted | NotFound | NotFoundNoBody | TooManyHeaders | NoContentLength
            | TooLargePayload | InvalidKey | BadRequest(_) => {
                // The body will is an (error) message in plain text, UTF-8.
                write!(buf, "Content-Type: text/plain; charset=utf-8\r\n").unwrap()
            }
            ServerError | ServerErrorNoBody => {}
        }

        write!(buf, "\r\n").unwrap();
    }

    pub fn status_code(&self) -> (u16, &'static str) {
        use Response::*;
        match self {
            Stored(_) => (201, "Created"),
            Ok(_) | OkNobody(_) => (200, "OK"),
            Deleted => (204, "No Content"),
            NotFound | NotFoundNoBody => (404, "Not Found"),
            TooManyHeaders => (431, "Request Header Fields Too Large"),
            NoContentLength => (411, "Length Required"),
            TooLargePayload => (413, "Payload Too Large "),
            InvalidKey | BadRequest(_) => (400, "Bad Request"),
            ServerError | ServerErrorNoBody => (500, "Internal Server Error"),
        }
    }

    fn body(&self) -> &[u8] {
        use Response::*;
        match self {
            Stored(_) | Deleted => b"Ok",
            Ok(blob) => blob.bytes(),
            OkNobody(_) | NotFoundNoBody | ServerErrorNoBody => b"", // Note: must be empty!
            NotFound => b"Not found",
            TooManyHeaders => b"Too many headers",
            NoContentLength => b"Missing required content length header",
            TooLargePayload => b"Blob too large",
            InvalidKey => b"Invalid key",
            BadRequest(msg) => msg.as_bytes(),
            ServerError => b"Internal server error",
        }
    }

    /// Returns the Content-Length.
    ///
    /// # Notes
    ///
    /// This is not the same as `body().len()`, as that will be 0 for bodies
    /// responding to HEAD requests, this will return the correct length.
    fn len(&self) -> usize {
        use Response::*;
        match self {
            OkNobody(blob) => blob.bytes().len(),
            NotFoundNoBody => NotFound.len(),
            ServerErrorNoBody => ServerError.len(),
            other => other.body().len(),
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
    Both(&'c [u8], &'c [u8]),
    /// Write the body.
    Body(&'c [u8]),
}

impl<'c, IO> Future for WriteResponse<'c, IO>
where
    IO: AsyncWrite + Unpin,
{
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut task::Context) -> Poll<Self::Output> {
        loop {
            use WrittenState::*;
            match self.state {
                Both(headers, body) => match Pin::new(&mut self.io).poll_write(ctx, headers) {
                    Poll::Ready(Ok(written)) if written >= headers.len() => {
                        // Written all headers, just the body left.
                        self.state = WrittenState::Body(body);
                    }
                    Poll::Ready(Ok(written)) => {
                        // Only written part of the headers.
                        self.state = WrittenState::Both(&headers[written..], body);
                    }
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Pending => return Poll::Pending,
                },
                Body(body) => match Pin::new(&mut self.io).poll_write(ctx, body) {
                    Poll::Ready(Ok(written)) if written >= body.len() => {
                        // Written the entire body.
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Ok(written)) => {
                        // Only written part of the body.
                        self.state = WrittenState::Body(&body[written..]);
                    }
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Pending => return Poll::Pending,
                },
            }
        }
    }
}
