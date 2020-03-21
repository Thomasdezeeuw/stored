//! Module that parses HTTP/1.1 requests.

// TODO: handle body:
// https://github.com/hyperium/hyper/blob/0eaf304644a396895a4ce1f0146e596640bb666a/src/proto/h1/role.rs#L72-L243

// Example from FireFox:
//
// Host: github.com
// User-Agent: Mozilla/5.0 (Windows NT 10.0; rv:68.0) Gecko/20100101 Firefox/68.0
// Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8
// Accept-Language: en-US,en;q=0.7,nl;q=0.3
// Accept-Encoding: gzip, deflate, br
// DNT: 1
// Connection: keep-alive
// Upgrade-Insecure-Requests: 1
// Pragma: no-cache
// Cache-Control: no-cache

// TODO: add tests.

use std::convert::TryFrom;
use std::future::Future;
use std::io::{self, Write};
use std::marker::Unpin;
use std::pin::Pin;
use std::str;
use std::task::{self, Poll};

use futures_io::{AsyncRead, AsyncWrite};
use httparse::EMPTY_HEADER;

use crate::key::InvalidKeyStr;
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
    /// Note: the length might be invalid as its user-supplied.
    Post(usize),
    /// GET request for the blob with `key`.
    Get(Key),
    /// HEAD request for the blob with `key`.
    Head(Key),
    /// DELETE request for the blob with `key`.
    Delete(Key),
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
            _ => Err(RequestError::InvalidRoute),
        }
    }
}

/// Error returned by [`ReadRequest`].
pub enum RequestError {
    /// I/O error.
    Io(io::Error),
    /// Error parsing the HTTP response.
    Parse(httparse::Error),
    /// Invalid method/path combination.
    InvalidRoute,
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

impl<IO> Connection<IO>
where
    IO: AsyncWrite,
{
    /// Returns a [`Future`] that writes an HTTP/1.1 [`Response`].
    pub fn write_response(&mut self, response: Response) -> WriteResponse<IO> {
        self.write_buf.clear();
        response.write_headers(&mut self.write_buf);
        WriteResponse { io: &mut self.io }
    }
}

/// HTTP response.
pub enum Response<'a> {
    /// Blob was stored.
    /// Response to POST.
    ///
    /// 201 Created. No body, Location header set to "/blob/$key".
    Stored(&'a Key),

    /// Blob found.
    /// Response to GET response.
    ///
    /// 200 Ok. Body is the blob (the passed bytes).
    Ok(&'a [u8]),

    /// Blob found.
    /// Response to HEAD response.
    ///
    /// 204 No Content. No body.
    OkNobody,

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
    BadRequest(&'a str),
}

impl<'a> Response<'a> {
    /// Write all headers to `buf`, including the last empty line.
    fn write_headers(&self, buf: &mut Vec<u8>) {
        let (status_code, status_msg) = self.status_code();
        // TODO: use proper date!
        let date = "Thu, 19 Mar 2020 13:37:07 GMT";
        let content_length = self.body().len();

        write!(
            buf,
            "HTTP/1.1 {} {}\r\nData: {}\r\nServer: stored\r\nContent-Length: {}\r\n",
            status_code, status_msg, date, content_length,
        )
        .unwrap();

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
            Ok(_) | OkNobody => {
                // TODO: set `Last-Modified` header.
                todo!("Last-Modified: Tue, 15 Nov 1994 12:45:26 GMT");
            }
            InvalidKey | BadRequest(_) => {
                // The body will be the error message in plain text, UTF-8.
                write!(buf, "Content-Type: text/plain; charset=utf-8\r\n").unwrap()
            }
            _ => (),
        }

        write!(buf, "\r\n").unwrap();
    }

    fn status_code(&self) -> (u16, &'static str) {
        use Response::*;
        match self {
            Stored(_) => (201, "Created"),
            Ok(_) | OkNobody => (200, "OK"),
            Deleted => (204, "No Content"),
            NotFound => (404, "Not Found"),
            TooManyHeaders => (431, "Request Header Fields Too Large"),
            NoContentLength => (411, "Length Required"),
            TooLargePayload => (413, "Payload Too Large "),
            InvalidKey | BadRequest(_) => (400, "Bad Request"),
        }
    }

    fn body(&self) -> &[u8] {
        use Response::*;
        match self {
            Ok(blob) => blob,
            Stored(_) | OkNobody | Deleted | NotFound | TooManyHeaders | NoContentLength
            | TooLargePayload => b"",
            InvalidKey => InvalidKeyStr::DESC.as_bytes(),
            BadRequest(msg) => msg.as_bytes(),
        }
    }
}

pub struct WriteResponse<'c, IO> {
    io: &'c mut IO,
}

impl<'c, IO> Future for WriteResponse<'c, IO>
where
    IO: AsyncWrite + Unpin,
{
    type Output = Result<Request, RequestError>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut task::Context) -> Poll<Self::Output> {
        todo!()
        /*
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
        */
    }
}
