//! Module with the server's HTTP/1.1 implementation.
//!
//! The [`http::actor`] is the main type, its started by [`tcp::Server`] and is
//! supervised by [`http::Supervisor`].
//!
//! [`http::actor`]: crate::http::actor()
//! [`tcp::Server`]: heph::net::tcp::Server
//! [`http::Supervisor`]: crate::http::Supervisor

// TODO: add tests.

// TODO: timeouts:
// - For reading, respond with 408: https://tools.ietf.org/html/rfc7231#section-6.5.7.
// - For writing.
// - For rpc with storage actor.

use std::convert::TryFrom;
use std::error::Error;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::time::{Instant, SystemTime};
use std::{fmt, mem, str};

use chrono::{DateTime, Datelike, Timelike, Utc};
use futures_util::io::AsyncWriteExt;
use heph::log::request;
use heph::net::{tcp, TcpStream};
use heph::{actor, ActorRef};
use heph::{NewActor, Supervisor, SupervisorStrategy};
use httparse::EMPTY_HEADER;
use log::{debug, error};

use crate::buffer::{Buffer, WriteBuffer};
use crate::db::{self, AddBlobResponse, HealthCheck, RemoveBlobResponse};
use crate::storage::{Blob, BlobEntry, PAGE_SIZE};
use crate::Key;

/// Supervisor for the [`http::actor`]'s listener the [`tcp::Server`].
///
/// [`http::actor`]: crate::http::actor()
/// [`tcp::Server`]: heph::net::tcp::Server
///
/// Attempts to restart the listener once, stops it the second time.
pub struct ServerSupervisor;

impl<S, NA> Supervisor<tcp::ServerSetup<S, NA>> for ServerSupervisor
where
    S: Supervisor<NA> + Clone + 'static,
    NA: NewActor<Argument = (TcpStream, SocketAddr), Error = !> + Clone + 'static,
{
    fn decide(&mut self, err: tcp::ServerError<!>) -> SupervisorStrategy<()> {
        use tcp::ServerError::*;
        match err {
            Accept(err) => {
                error!("error accepting new connection: {}", err);
                SupervisorStrategy::Restart(())
            }
            NewActor::<!>(_) => unreachable!(),
        }
    }

    fn decide_on_restart_error(&mut self, err: io::Error) -> SupervisorStrategy<()> {
        error!("error restarting the HTTP server: {}", err);
        SupervisorStrategy::Stop
    }

    fn second_restart_error(&mut self, err: io::Error) {
        error!("error restarting the HTTP server a second time: {}", err);
    }
}

/// Supervisor for the [`http::actor`].
///
/// [`http::actor`]: crate::http::actor()
///
/// Logs the error and stops the actor.
pub fn supervisor(err: io::Error) -> SupervisorStrategy<(TcpStream, SocketAddr)> {
    error!("error handling HTTP connection: {}", err);
    SupervisorStrategy::Stop
}

/// Actor that handles a single TCP `stream`, expecting HTTP requests.
///
/// Returns any and all I/O errors.
pub async fn actor(
    mut ctx: actor::Context<!>,
    stream: TcpStream,
    address: SocketAddr,
    mut db_ref: ActorRef<db::Message>,
) -> io::Result<()> {
    debug!("accepted connection: address={}", address);
    let mut conn = Connection {
        stream,
        buf: Buffer::new(),
    };
    let mut request = Request::empty();

    loop {
        let start = Instant::now();

        let response = match read_request(&mut conn, &mut request).await {
            // Parsed a request, now route and process it.
            Ok(true) => route_request(&mut ctx, &mut db_ref, &mut conn, &request).await?,
            // Read all requests on the stream, so this actor's work is done.
            Ok(false) => break,
            // Try operator return the I/O errors.
            Err(err) => Response::for_error(err)?,
        };

        write_response(&mut conn, &response).await?;

        request!(
            "request: remote_address=\"{}\", method=\"{}\", path=\"{}\", user_agent=\"{}\", \
                request_length={}, response_time=\"{:?}\", response_status={}, \
                response_length={}",
            address,
            request.method,
            request.path,
            request.user_agent,
            request.length.unwrap_or(0),
            start.elapsed(),
            response.status_code().0,
            response.len(),
        );

        // In cases were we don't/can't read the (entire) body we need to close
        // the connection.
        if response.should_close() {
            break;
        }
    }

    debug!("closing connection: address={}", address);
    Ok(())
}

/// `Connection` types that wraps a TCP stream and buffers it using `Buffer`.
#[derive(Debug)]
struct Connection {
    stream: TcpStream,
    buf: Buffer,
}

/// Maximum number of headers read from an incoming request.
const MAX_HEADERS: usize = 16;

/// Maximum size of the headers of a request in bytes.
const MAX_HEADERS_SIZE: usize = 2 * 1024;

/// Headers we're interested in.
const USER_AGENT: &str = "user-agent";
const CONTENT_LENGTH: &str = "content-length";

/// Reads a [`Request`] from `stream`, using `buf` as buffer. Parts of the
/// `Request` will be stored in `wbuf`.
///
/// Returns `true` if a request was read into `request`, `false` if there are no
/// more requests on `stream` or an error otherwise.
async fn read_request(conn: &mut Connection, request: &mut Request) -> Result<bool, RequestError> {
    let mut too_short = 0;
    loop {
        // At the start we likely don't have enough bytes to read the entire
        // request, however it could be that we read (part of) a request in
        // reading a previous request, so we need to check.
        if conn.buf.len() > too_short {
            match request.parse(conn.buf.as_bytes()) {
                Ok(httparse::Status::Complete(bytes_read)) => {
                    conn.buf.processed(bytes_read);
                    return Ok(true);
                }
                // Need to read some more bytes.
                Ok(httparse::Status::Partial) => too_short = conn.buf.len(),
                Err(err) => return Err(err),
            }

            if conn.buf.len() >= MAX_HEADERS_SIZE {
                return Err(RequestError::TooLarge);
            }
        }

        match conn.buf.read_from(&mut conn.stream).await {
            // No more bytes in the connection or buffer, processed all
            // requests.
            Ok(0) if conn.buf.is_empty() => return Ok(false),
            // Read all bytes, but don't have a complete HTTP request yet.
            Ok(0) => return Err(RequestError::Incomplete),
            // Read some bytes, now try parsing the request.
            Ok(_) => continue,
            Err(err) => return Err(RequestError::Io(err)),
        }
    }
}

/// Returns `true` if `value` is equal to `want` in lowercase.
fn lower_case_cmp(value: &str, want: &str) -> bool {
    if value.len() != want.len() {
        return false;
    }

    value
        .as_bytes()
        .iter()
        .copied()
        .zip(want.as_bytes().iter().copied())
        .all(|(x, y)| x.to_ascii_lowercase() == y)
}

/// Parsed HTTP request.
#[derive(Debug)]
struct Request {
    method: Method,
    path: String,
    /// Empty string means not present.
    user_agent: String,
    length: Option<usize>,
}

impl Request {
    /// Create an empty `Request`.
    const fn empty() -> Request {
        Request {
            method: Method::Options,
            path: String::new(),
            user_agent: String::new(),
            length: None,
        }
    }

    /// Returns `true` if the "Content-Length" header is > 0.
    fn has_body(&self) -> bool {
        if let Some(body_length) = self.length {
            body_length > 0
        } else {
            false
        }
    }

    /// Parse a request.
    fn parse(&mut self, bytes: &[u8]) -> Result<httparse::Status<usize>, RequestError> {
        let mut headers = [EMPTY_HEADER; MAX_HEADERS];
        let mut req = httparse::Request::new(&mut headers);
        match req.parse(bytes) {
            Ok(httparse::Status::Complete(bytes_read)) => {
                // TODO: check req.version?

                let method =
                    Method::try_from(req.method).map_err(|()| RequestError::InvalidMethod)?;

                let mut user_agent = None;
                let mut length = None;
                for header in req.headers.iter() {
                    if lower_case_cmp(header.name, USER_AGENT) {
                        user_agent = str::from_utf8(header.value).ok();
                    } else if lower_case_cmp(header.name, CONTENT_LENGTH) {
                        length = str::from_utf8(header.value)
                            .map_err(|_| RequestError::InvalidContentLength)
                            .and_then(|str_length| {
                                str_length
                                    .parse()
                                    .map(Some)
                                    .map_err(|_| RequestError::InvalidContentLength)
                            })?;
                    }
                }

                self.method = method;
                self.path.clear();
                self.path.push_str(req.path.unwrap_or(""));
                self.user_agent.clear();
                if let Some(user_agent) = user_agent {
                    self.user_agent.push_str(user_agent);
                }
                self.length = length;
                Ok(httparse::Status::Complete(bytes_read))
            }
            Ok(httparse::Status::Partial) => Ok(httparse::Status::Partial),
            Err(err) => Err(RequestError::Parse(err)),
        }
    }
}

/// HTTP request method.
#[derive(Copy, Clone, Debug)]
enum Method {
    Options,
    Get,
    Post,
    Put,
    Delete,
    Head,
    Trace,
    Connect,
    Patch,
}

impl Method {
    /// Returns `true` if `self` is a HEAD method.
    fn is_head(self) -> bool {
        match self {
            Method::Head => true,
            _ => false,
        }
    }
}

impl TryFrom<Option<&str>> for Method {
    type Error = ();

    fn try_from(method: Option<&str>) -> Result<Self, Self::Error> {
        match method {
            Some("OPTIONS") => Ok(Method::Options),
            Some("GET") => Ok(Method::Get),
            Some("POST") => Ok(Method::Post),
            Some("PUT") => Ok(Method::Put),
            Some("DELETE") => Ok(Method::Delete),
            Some("HEAD") => Ok(Method::Head),
            Some("TRACE") => Ok(Method::Trace),
            Some("CONNECT") => Ok(Method::Connect),
            Some("PATCH") => Ok(Method::Patch),
            _ => Err(()),
        }
    }
}

impl fmt::Display for Method {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Method::*;
        f.write_str(match self {
            Options => "OPTIONS",
            Get => "GET",
            Post => "POST",
            Put => "PUT",
            Delete => "DELETE",
            Head => "HEAD",
            Trace => "TRACE",
            Connect => "CONNECT",
            Patch => "PATCH",
        })
    }
}

/// Request error in which we couldn't parse a proper HTTP request.
#[derive(Debug)]
enum RequestError {
    /// I/O error.
    Io(io::Error),
    /// Error parsing the HTTP response.
    Parse(httparse::Error),
    /// Retrieved an incomplete HTTP request.
    Incomplete,
    /// Request has an invalid "Content-Length" header, i.e its not a UTF-8
    /// formatted number.
    InvalidContentLength,
    /// Method is invalid, i.e. not in [`Method`].
    InvalidMethod,
    /// Request is too large.
    TooLarge,
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use RequestError::*;
        match self {
            Io(err) => write!(f, "I/O error: {}", err),
            Parse(err) => write!(f, "HTTP parsing error: {}", err),
            Incomplete => write!(f, "incomplete HTTP request"),
            InvalidContentLength => write!(f, "request's Content-Length header is invalid"),
            InvalidMethod => write!(f, "request's method is invalid"),
            TooLarge => write!(f, "request's header is too large"),
        }
    }
}

impl Error for RequestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        use RequestError::*;
        match self {
            Io(err) => Some(err),
            Parse(err) => Some(err),
            Incomplete | InvalidContentLength | InvalidMethod | TooLarge => None,
        }
    }
}

/// Path prefix to GET/HEAD/DELETE a blob.
const BLOB_PATH_PREFIX: &str = "/blob/";

/// Routes the request to the correct function.
async fn route_request(
    ctx: &mut actor::Context<!>,
    db_ref: &mut ActorRef<db::Message>,
    conn: &mut Connection,
    request: &Request,
) -> io::Result<Response> {
    use Method::*;
    match (request.method, &*request.path) {
        (Post, "/blob") | (Post, "/blob/") => match request.length {
            Some(length) => {
                let (kind, should_close) = store_blob(ctx, db_ref, conn, length).await?;
                Ok(Response {
                    is_head: false,
                    should_close,
                    kind,
                })
            }
            None => Ok(Response {
                is_head: false,
                should_close: true,
                kind: ResponseKind::NoContentLength,
            }),
        },
        (Get, "/health") | (Get, "/health/") | (Head, "/health") | (Head, "/health/") => {
            match check_no_body(request) {
                Ok(()) => Ok(Response {
                    is_head: request.method.is_head(),
                    should_close: false,
                    kind: health_check(ctx, db_ref).await,
                }),
                Err(err) => Ok(err),
            }
        }
        // TODO: DRY the next three routes.
        (Get, path) if path.starts_with(BLOB_PATH_PREFIX) => match check_no_body(request) {
            Ok(()) => match parse_blob_path(path) {
                Ok(key) => Ok(Response {
                    is_head: false,
                    should_close: false,
                    kind: retrieve_blob(ctx, db_ref, key).await,
                }),
                Err(err) => Ok(Response {
                    is_head: false,
                    should_close: false,
                    kind: err,
                }),
            },
            Err(err) => Ok(err),
        },
        (Head, path) if path.starts_with(BLOB_PATH_PREFIX) => match check_no_body(request) {
            Ok(()) => match parse_blob_path(path) {
                Ok(key) => Ok(Response {
                    is_head: true,
                    should_close: false,
                    kind: retrieve_blob(ctx, db_ref, key).await,
                }),
                Err(err) => Ok(Response {
                    is_head: true,
                    should_close: false,
                    kind: err,
                }),
            },
            Err(err) => Ok(err),
        },
        (Delete, path) if path.starts_with(BLOB_PATH_PREFIX) => match check_no_body(request) {
            Ok(()) => match parse_blob_path(path) {
                Ok(key) => Ok(Response {
                    is_head: false,
                    should_close: false,
                    kind: remove_blob(ctx, db_ref, key).await,
                }),
                Err(err) => Ok(Response {
                    is_head: false,
                    should_close: false,
                    kind: err,
                }),
            },
            Err(err) => Ok(err),
        },
        _ => Ok(Response {
            is_head: request.method.is_head(),
            should_close: request.has_body(),
            kind: ResponseKind::NotFound,
        }),
    }
}

/// Returns an error if `request` has a body.
fn check_no_body(request: &Request) -> Result<(), Response> {
    match request.length {
        Some(0) | None => Ok(()),
        _ => Err(Response {
            is_head: request.method.is_head(),
            should_close: true,
            kind: ResponseKind::BadRequest("Unexpected request body"),
        }),
    }
}

/// Parse a blob path path.
/// Note: path must start with `BLOB_PATH_PREFIX`.
fn parse_blob_path(path: &str) -> Result<Key, ResponseKind> {
    debug_assert!(path.starts_with(BLOB_PATH_PREFIX));
    path[BLOB_PATH_PREFIX.len()..]
        .parse()
        .map_err(|_| ResponseKind::BadRequest("Invalid key in URI"))
}

/// Stores a blob in the database, reading `body_length` bytes from the
/// `conn`ection as blob. Returns a `ResponseKind` and whether or not the
/// connection should be closed (for use in `Response`).
async fn store_blob(
    ctx: &mut actor::Context<!>,
    db_ref: &mut ActorRef<db::Message>,
    conn: &mut Connection,
    body_length: usize,
) -> io::Result<(ResponseKind, bool)> {
    // TODO: get this from a configuration.
    const MAX_SIZE: usize = 1024 * 1024; // 1MB.

    if body_length > MAX_SIZE {
        return Ok((ResponseKind::TooLargePayload, true));
    }

    conn.buf.reserve_atleast(body_length);

    while conn.buf.len() < body_length {
        match conn.buf.read_from(&mut conn.stream).await {
            // No more bytes left, but didn't yet read the entire request body.
            Ok(0) => return Ok((ResponseKind::BadRequest("Incomplete blob"), true)),
            // Read some bytes.
            Ok(_) => continue,
            Err(err) => return Err(err),
        }
    }

    // Take the body from the connection and use it as a blob to send to the
    // storage actor.
    let blob = mem::replace(&mut conn.buf, Buffer::empty());

    if blob.is_empty() {
        Ok((ResponseKind::BadRequest("Can't store empty blob"), false))
    } else {
        match db_ref.rpc(ctx, (blob, body_length)) {
            Ok(rpc) => match rpc.await {
                Ok((result, mut buffer)) => {
                    // Mark the body as processed and put back the buffer.
                    buffer.processed(body_length);
                    mem::replace(&mut conn.buf, buffer);
                    match result {
                        AddBlobResponse::Query(query) => match db_ref
                            .rpc(ctx, (query, SystemTime::now()))
                        {
                            Ok(rpc) => match rpc.await {
                                Ok(key) => Ok((ResponseKind::Stored(key), false)),
                                Err(err) => {
                                    error!("error waiting for RPC response from database: {}", err);
                                    Ok((ResponseKind::ServerError, true))
                                }
                            },
                            Err(err) => {
                                error!("error making RPC call to database: {}", err);
                                Ok((ResponseKind::ServerError, true))
                            }
                        },
                        AddBlobResponse::AlreadyStored(key) => {
                            Ok((ResponseKind::Stored(key), false))
                        }
                    }
                }
                Err(err) => {
                    error!("error waiting for RPC response from database: {}", err);
                    Ok((ResponseKind::ServerError, true))
                }
            },
            Err(err) => {
                error!("error making RPC call to database: {}", err);
                Ok((ResponseKind::ServerError, true))
            }
        }
    }
}

/// Retrieve the blob associated with `key` from the actor behind the `db_ref`.
async fn retrieve_blob(
    ctx: &mut actor::Context<!>,
    db_ref: &mut ActorRef<db::Message>,
    key: Key,
) -> ResponseKind {
    match db_ref.rpc(ctx, key) {
        Ok(rpc) => match rpc.await {
            Ok(Some(BlobEntry::Stored(blob))) => {
                if blob.len() > PAGE_SIZE {
                    // If the blob is large(-ish) we'll prefetch it from disk to
                    // improve performance.
                    // TODO: benchmark this with large(-ish) blobs.
                    let _ = blob.prefetch();
                }
                ResponseKind::Ok(blob)
            }
            Ok(Some(BlobEntry::Removed(removed_at))) => ResponseKind::Removed(removed_at),
            Ok(None) => ResponseKind::NotFound,
            Err(err) => {
                error!("error waiting for RPC response from database: {}", err);
                ResponseKind::ServerError
            }
        },
        Err(err) => {
            error!("error making RPC call to database: {}", err);
            ResponseKind::ServerError
        }
    }
}

/// Retrieve the blob associated with `key` from the actor behind the `db_ref`.
async fn remove_blob(
    ctx: &mut actor::Context<!>,
    db_ref: &mut ActorRef<db::Message>,
    key: Key,
) -> ResponseKind {
    match db_ref.rpc(ctx, key) {
        Ok(rpc) => match rpc.await {
            Ok(result) => match result {
                RemoveBlobResponse::Query(query) => {
                    match db_ref.rpc(ctx, (query, SystemTime::now())) {
                        Ok(rpc) => match rpc.await {
                            Ok(removed_at) => ResponseKind::Removed(removed_at),
                            Err(err) => {
                                error!("error waiting for RPC response from database: {}", err);
                                ResponseKind::ServerError
                            }
                        },
                        Err(err) => {
                            error!("error making RPC call to database: {}", err);
                            ResponseKind::ServerError
                        }
                    }
                }
                RemoveBlobResponse::NotStored(Some(removed_at)) => {
                    ResponseKind::Removed(removed_at)
                }
                // Blob was never stored.
                RemoveBlobResponse::NotStored(None) => ResponseKind::NotFound,
            },
            Err(err) => {
                error!("error waiting for RPC response from database: {}", err);
                ResponseKind::ServerError
            }
        },
        Err(err) => {
            error!("error making RPC call to database: {}", err);
            ResponseKind::ServerError
        }
    }
}

/// Runs a health check on the actor behind the `db_ref`.
async fn health_check(
    ctx: &mut actor::Context<!>,
    db_ref: &mut ActorRef<db::Message>,
) -> ResponseKind {
    match db_ref.rpc(ctx, HealthCheck) {
        Ok(_health_ok) => ResponseKind::HealthOk,
        Err(err) => {
            error!("error making RPC call to database: {}", err);
            ResponseKind::ServerError
        }
    }
}

/// Write `response` to `stream`, using buffer `buf`.
async fn write_response(conn: &mut Connection, response: &Response) -> io::Result<()> {
    // Create a write buffer a write the request headers to it.
    let (_, mut write_buf) = conn.buf.split_write(Response::MAX_HEADERS_SIZE);
    response.write_headers(&mut write_buf);

    // TODO: replace with `write_all_vectored`:
    // https://github.com/rust-lang/futures-rs/pull/1741.

    conn.stream.write_all(write_buf.as_bytes()).await?;
    conn.stream.write_all(response.body()).await
}

/// HTTP response.
#[derive(Debug)]
struct Response {
    /// Request was a HEAD request.
    is_head: bool,
    should_close: bool,
    kind: ResponseKind,
}

/// HTTP response kind.
#[derive(Debug)]
enum ResponseKind {
    /// Blob was stored.
    /// Response to POST new blob.
    ///
    /// 201 Created. No body, Location header set to "/blob/$key".
    Stored(Key),
    /// Blob found.
    /// Response to GET blob.
    ///
    /// 200 OK. Body is the blob (the passed bytes).
    Ok(Blob),
    /// Blob removed.
    /// Response to DELETE blob.
    ///
    /// 410 Gone. No body.
    Removed(SystemTime),
    /// Health check is OK.
    ///
    /// 200 OK. No body.
    HealthOk,

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
    static MONTHS: [&str; 12] = [
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
    /// Returns the correct `Response` for a `RequestError`, returns an
    /// [`io::Error`] if the request error kind is I/O
    /// ([`RequestError::Io`])
    fn for_error(err: RequestError) -> io::Result<Response> {
        use RequestError::*;
        let kind = match err {
            Io(err) => return Err(err),
            // HTTP parsing errors.
            Parse(httparse::Error::HeaderName) => ResponseKind::BadRequest("Invalid header name"),
            Parse(httparse::Error::HeaderValue) => ResponseKind::BadRequest("Invalid header value"),
            Parse(httparse::Error::NewLine)
            | Parse(httparse::Error::Status)
            | Parse(httparse::Error::Token)
            | Parse(httparse::Error::Version) => {
                ResponseKind::BadRequest("Invalid HTTP request format")
            }
            Incomplete => ResponseKind::BadRequest("Incomplete HTTP request"),
            Parse(httparse::Error::TooManyHeaders) => ResponseKind::TooManyHeaders,
            // Always need a Content-Length header for `Post` requests.
            InvalidContentLength => ResponseKind::NoContentLength,
            // Unexpected method.
            InvalidMethod => ResponseKind::BadRequest("Invalid request HTTP method"),
            // Request too large.
            TooLarge => ResponseKind::BadRequest("Request too large"),
        };
        Ok(Response {
            // TODO: we can know this for some of the errors.
            is_head: false, // Don't know.
            should_close: true,
            kind,
        })
    }

    /// Hint of the maximum size of `write_headers`, use in reserving buffer
    /// capacity, never as actual maximum (it gets outdated easily).
    const MAX_HEADERS_SIZE: usize =
        // Status line, +31 for the longest reason (`TooManyHeaders`).
        15 + 31 +
        // Server and Content-Length (+39 max. length as text), Date headers.
        16 + 18 + 39 + 37 +
        // Connection header (keep-alive).
        24 +
        // Extra headers: Location header (the longest) and ending "\r\n".
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

        // For some errors, where we can't process the body properly, we want to
        // close the connection as we don't know where the next request begins.
        if self.should_close() {
            write!(buf, "Connection: close\r\n").unwrap();
        } else {
            write!(buf, "Connection: keep-alive\r\n").unwrap();
        }

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
            Removed(removed_at) => {
                let timestamp: DateTime<Utc> = (*removed_at).into();
                append_date_header(&timestamp, "Last-Modified", buf);
            }
            HealthOk | NotFound | TooManyHeaders | NoContentLength | TooLargePayload
            | BadRequest(_) | ServerError => {
                // The body is an (error) message in plain text, UTF-8.
                write!(buf, "Content-Type: text/plain; charset=utf-8\r\n").unwrap()
            }
        }

        write!(buf, "\r\n").unwrap();
    }

    /// Returns the status code and reason.
    fn status_code(&self) -> (u16, &'static str) {
        use ResponseKind::*;
        match &self.kind {
            Stored(_) => (201, "Created"),
            Ok(_) | HealthOk => (200, "OK"),
            Removed(_) => (410, "Gone"),
            NotFound => (404, "Not Found"),
            TooManyHeaders => (431, "Request Header Fields Too Large"),
            NoContentLength => (411, "Length Required"),
            TooLargePayload => (413, "Payload Too Large"),
            BadRequest(_) => (400, "Bad Request"),
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

    /// Returns `true` if the connection should be closed.
    fn should_close(&self) -> bool {
        self.should_close
    }
}

impl ResponseKind {
    /// Returns the body for the response.
    fn body(&self) -> &[u8] {
        use ResponseKind::*;
        match &self {
            Stored(_) | Removed(_) => b"",
            Ok(blob) => blob.bytes(),
            HealthOk => b"OK",
            NotFound => b"Not found",
            TooManyHeaders => b"Too many headers",
            NoContentLength => b"Missing required content length header",
            TooLargePayload => b"Blob too large",
            BadRequest(msg) => msg.as_bytes(),
            ServerError => b"Internal server error",
        }
    }
}
