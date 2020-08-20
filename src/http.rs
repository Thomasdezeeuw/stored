//! HTTP/1.1 server implementation.
//!
//! The [`http::actor`] is the main type, its started by [`tcp::Server`] and is
//! supervised by [`http::supervisor`]. Adding the HTTP actor to the runtime is
//! a two step process, first by setting up the listener by calling
//! [`http::setup`] and then calling the returned function in the runtime setup
//! function.
//!
//! [`http::actor`]: crate::http::actor()
//! [`tcp::Server`]: heph::net::tcp::Server
//! [`http::supervisor`]: crate::http::supervisor
//! [`http::setup`]: crate::http::setup
//! [`http::start`]: crate::http::start

// TODO: add tests.

// TODO: timeouts:
// - For reading, respond with 408: https://tools.ietf.org/html/rfc7231#section-6.5.7.
// - For writing.
// - For rpc with storage actor.

use std::convert::TryFrom;
use std::error::Error;
use std::io::{self, IoSlice, Write};
use std::net::SocketAddr;
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use std::{fmt, str};

use chrono::{DateTime, Datelike, Timelike, Utc};
use futures_util::io::AsyncWriteExt;
use heph::actor::context::ThreadLocal;
use heph::actor::messages::Start;
use heph::actor_ref::ActorGroup;
use heph::log::request;
use heph::net::tcp::{self, ServerMessage, TcpStream};
use heph::rt::options::{ActorOptions, Priority};
use heph::supervisor::NoSupervisor;
use heph::timer::{Deadline, DeadlinePassed};
use heph::{actor, Actor, ActorRef, NewActor, RuntimeRef, Supervisor, SupervisorStrategy};
use httparse::EMPTY_HEADER;
use log::{debug, error, info, trace, warn};
use parking_lot::{Once, RwLock};

use crate::buffer::{Buffer, WriteBuffer};
use crate::error::Describe;
use crate::op::{self, Outcome};
use crate::passport::{Event, Passport, Uuid};
use crate::peer::Peers;
use crate::storage::{Blob, BlobEntry};
use crate::{db, Key};

/// Setup the HTTP listener.
///
/// This returns a function which can be used to start the HTTP listener.
pub fn setup(
    address: SocketAddr,
    db_ref: ActorRef<db::Message>,
    start_group: Arc<RwLock<ActorGroup<Start>>>,
    peers: Peers,
    delay_start: bool,
) -> io::Result<impl FnOnce(&mut RuntimeRef) -> io::Result<()> + Send + Clone + 'static> {
    let http_actor = (actor as fn(_, _, _, _, _) -> _)
        .map_arg(move |(stream, arg)| (stream, arg, db_ref.clone(), peers.clone()));
    let http_listener =
        tcp::Server::setup(address, supervisor, http_actor, ActorOptions::default())?;
    // The returned closure is copied for each worker thread started, but we
    // only want to log that we start the HTTP listener once.
    let log_once = Arc::new(Once::new());
    Ok(move |runtime: &mut RuntimeRef| {
        if delay_start {
            // Delay the start of the HTTP listener, running the peer
            // synchronisation first, once  complete we'll start the HTTP
            // listener (see `crate::peer::sync`).
            let actor = delayed_start as fn(_, _) -> _;
            let start_listener = move |runtime: &mut RuntimeRef| {
                spawn_listener(runtime, &log_once, http_listener, address)
            };
            let start_http_ref =
                runtime.spawn_local(NoSupervisor, actor, start_listener, ActorOptions::default());

            start_group.write().deref_mut().add(start_http_ref);
            Ok(())
        } else {
            spawn_listener(runtime, &log_once, http_listener, address)
        }
    })
}

fn spawn_listener<S, NA>(
    runtime: &mut RuntimeRef,
    log_once: &Once,
    http_listener: tcp::ServerSetup<S, NA>,
    address: SocketAddr,
) -> io::Result<()>
where
    S: Supervisor<NA> + Clone + 'static,
    NA: NewActor<Argument = (TcpStream, SocketAddr), Context = ThreadLocal, Error = !>
        + Clone
        + 'static,
{
    log_once.call_once(|| {
        info!("listening on http://{}", address);
    });

    let options = ActorOptions::default().with_priority(Priority::LOW);
    runtime
        .try_spawn_local(ServerSupervisor, http_listener, (), options)
        .map(|server_ref| runtime.receive_signals(server_ref.try_map()))
}

/// Actor that starts the HTTP listener once we're fully synced by calling the
/// `start_http` function.
pub async fn delayed_start<F>(mut ctx: actor::Context<Start>, start_http: F) -> Result<(), !>
where
    F: FnOnce(&mut RuntimeRef) -> io::Result<()>,
{
    // Wait until we get the start signal.
    let _start = ctx.receive_next().await;

    // Start the HTTP listener.
    if let Err(err) = start_http(ctx.runtime()) {
        // TODO: stop the application?
        error!("error binding HTTP server: {}", err);
    }
    Ok(())
}

/// Supervisor for the [`http::actor`]'s listener the [`tcp::Server`].
///
/// [`http::actor`]: crate::http::actor()
/// [`tcp::Server`]: heph::net::tcp::Server
///
/// Attempts to restart the listener once, stops it the second time.
pub struct ServerSupervisor;

impl<L, A> Supervisor<L> for ServerSupervisor
where
    L: NewActor<Message = ServerMessage, Argument = (), Actor = A, Error = io::Error>,
    A: Actor<Error = tcp::ServerError<!>>,
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
pub fn supervisor(err: crate::Error) -> SupervisorStrategy<(TcpStream, SocketAddr)> {
    error!("error handling HTTP connection: {}", err);
    SupervisorStrategy::Stop
}

/// Default timeout in I/O operations.
const TIMEOUT: Duration = Duration::from_secs(10);
/// Timeout used when reading a second request.
const ALIVE_TIMEOUT: Duration = Duration::from_secs(120);

/// Actor that handles a single TCP `stream`, expecting HTTP requests.
///
/// Returns any and all I/O errors.
pub async fn actor(
    mut ctx: actor::Context<!>,
    stream: TcpStream,
    address: SocketAddr,
    mut db_ref: ActorRef<db::Message>,
    peers: Peers,
) -> crate::Result<()> {
    debug!("accepted connection: remote_address={}", address);
    let mut conn = Connection::new(stream);
    let mut request = Request::empty();

    let mut timeout = TIMEOUT;
    loop {
        let result = Deadline::timeout(&mut ctx, timeout, conn.read_request(&mut request)).await;
        let start = Instant::now();
        let response = match result {
            // Parsed a request, now route and process it.
            Ok(true) => {
                route_request(&mut ctx, &mut db_ref, &peers, &mut conn, &mut request).await?
            }
            // Read all requests on the stream, so this actor's work is done.
            Ok(false) => break,
            // Try operator returns the I/O errors.
            Err(err) => Response::for_error(err).map_err(|err| err.describe("reading request"))?,
        };

        let result = Deadline::timeout(
            &mut ctx,
            TIMEOUT,
            conn.write_response(&response, &mut request.passport),
        )
        .await;
        match result {
            Ok(()) => {}
            Err(err) => return Err(err.describe("writing HTTP response")),
        }

        // TODO: don't log invalid/partial requests.
        request!(
            "request: request_id=\"{}\", remote_address=\"{}\", method=\"{}\", \
                path=\"{}\", user_agent=\"{}\", request_length={}, \
                response_time=\"{:?}\", response_status={}, response_length={}",
            request.id(),
            address,
            request.method,
            request.path,
            request.user_agent,
            request.length.unwrap_or(0),
            start.elapsed(),
            response.status_code().0,
            response.len(),
        );
        // Log the request passport to standard error.
        info!("request passport: {}", request.passport);

        // In cases were we don't/can't read the (entire) body we need to close
        // the connection.
        if response.should_close() {
            break;
        }

        // Keep the connection alive for longer.
        timeout = ALIVE_TIMEOUT;
    }

    debug!("closing connection: remote_address={}", address);
    Ok(())
}

/// `Connection` types that wraps a TCP stream and buffers it using `Buffer`.
#[derive(Debug)]
pub struct Connection {
    stream: TcpStream,
    buf: Buffer,
}

/// Maximum number of headers read from an incoming request.
const MAX_HEADERS: usize = 32;

/// Maximum size of the headers of a request in bytes.
const MAX_HEADERS_SIZE: usize = 2 * 1024;

/// Headers we're interested in.
const USER_AGENT: &str = "user-agent";
const CONTENT_LENGTH: &str = "content-length";
const REQUEST_ID: &str = "x-request-id";

impl Connection {
    /// Create a new `Connection`.
    pub fn new(stream: TcpStream) -> Connection {
        Connection {
            stream,
            buf: Buffer::new(),
        }
    }

    /// Reads a [`Request`] from this connection.
    ///
    /// Returns `true` if a request was read into `request`, `false` if there
    /// are no more requests on `stream` or an error otherwise.
    pub async fn read_request(&mut self, request: &mut Request) -> Result<bool, RequestError> {
        request.reset();

        let mut too_short = 0;
        loop {
            // At the start we likely don't have enough bytes to read the entire
            // request, however it could be that we read (part of) a request in
            // reading a previous request, so we need to check.
            if self.buf.len() > too_short {
                match request.parse(self.buf.as_bytes()) {
                    Ok(httparse::Status::Complete(bytes_read)) => {
                        self.buf.processed(bytes_read);
                        request.passport.mark(Event::ParsedHttpRequest);
                        trace!("read HTTP request: {:?}", request);
                        return Ok(true);
                    }
                    // Need to read some more bytes.
                    Ok(httparse::Status::Partial) => too_short = self.buf.len(),
                    Err(err) => return Err(err),
                }

                if self.buf.len() >= MAX_HEADERS_SIZE {
                    return Err(RequestError::TooLarge);
                }
            }

            match self.buf.read_from(&mut self.stream).await {
                // No more bytes in the connection or buffer, processed all
                // requests.
                Ok(0) if self.buf.is_empty() => return Ok(false),
                // Read all bytes, but don't have a complete HTTP request yet.
                Ok(0) => return Err(RequestError::Incomplete),
                // Read some bytes, now try parsing the request.
                Ok(_) => continue,
                Err(err) => return Err(RequestError::Io(err)),
            }
        }
    }

    /// Write `response` to the connection.
    pub async fn write_response(
        &mut self,
        response: &Response,
        passport: &mut Passport,
    ) -> io::Result<()> {
        trace!("writing HTTP response: {:?}", response);
        // Create a write buffer a write the request headers to it.
        let (_, mut write_buf) = self.buf.split_write(Response::MAX_HEADERS_SIZE);
        response.write_headers(&mut write_buf, passport.id());

        let bufs = &mut [
            IoSlice::new(write_buf.as_bytes()),
            IoSlice::new(response.body()),
        ];
        // TODO: add a timeout to this.
        self.stream
            .write_all_vectored(bufs)
            .await
            .map(|()| passport.mark(Event::WrittenHttpResponse))
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
pub struct Request {
    pub passport: Passport,
    pub method: Method,
    pub path: String,
    /// Empty string means not present.
    pub user_agent: String,
    pub length: Option<usize>,
}

impl Request {
    /// Create an empty `Request`.
    pub fn empty() -> Request {
        Request {
            passport: Passport::empty(),
            method: Method::Get,
            path: String::new(),
            user_agent: String::new(),
            length: None,
        }
    }

    /// Returns the request id.
    ///
    /// This is either uniquely generated or provided by the request via the
    /// `X-Request-ID` header.
    pub fn id(&self) -> &Uuid {
        self.passport.id()
    }

    /// Returns `true` if the "Content-Length" header is > 0.
    pub fn has_body(&self) -> bool {
        matches!(self.length, Some(n) if n > 0)
    }

    fn reset(&mut self) {
        self.passport.reset();
        self.method = Method::Get;
        self.path.clear();
        self.user_agent.clear();
        self.length = None;
    }

    /// Parse a request.
    ///
    /// # Notes
    ///
    /// Does not [`reset`] the request, use that before calling this method.
    fn parse(&mut self, bytes: &[u8]) -> Result<httparse::Status<usize>, RequestError> {
        let mut headers = [EMPTY_HEADER; MAX_HEADERS];
        let mut req = httparse::Request::new(&mut headers);
        match req.parse(bytes) {
            Ok(httparse::Status::Complete(bytes_read)) => {
                // TODO: check req.version?

                self.path.push_str(req.path.unwrap_or(""));
                self.method =
                    Method::try_from(req.method).map_err(|()| RequestError::InvalidMethod)?;

                let mut set_request_id = false;
                for header in req.headers.iter() {
                    if lower_case_cmp(header.name, USER_AGENT) {
                        if let Ok(user_agent) = str::from_utf8(header.value) {
                            self.user_agent.push_str(user_agent);
                        }
                    } else if lower_case_cmp(header.name, CONTENT_LENGTH) {
                        self.length = str::from_utf8(header.value)
                            .map_err(|_| RequestError::InvalidContentLength)
                            .and_then(|str_length| {
                                str_length
                                    .parse()
                                    .map(Some)
                                    .map_err(|_| RequestError::InvalidContentLength)
                            })?;
                    } else if lower_case_cmp(header.name, REQUEST_ID) {
                        if let Ok(request_id) = Uuid::parse_bytes(header.value) {
                            self.passport.set_id(request_id);
                            set_request_id = true;
                        }
                        // TODO: somehow log to the user that the header was
                        // invalid?
                    }
                }

                if !set_request_id {
                    self.passport.set_id(Uuid::new());
                }

                Ok(httparse::Status::Complete(bytes_read))
            }
            Ok(httparse::Status::Partial) => Ok(httparse::Status::Partial),
            Err(err) => Err(RequestError::Parse(err)),
        }
    }
}

/// HTTP request method.
#[derive(Copy, Clone, Debug)]
pub enum Method {
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
        matches!(self, Method::Head)
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
pub enum RequestError {
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

impl From<DeadlinePassed> for RequestError {
    fn from(err: DeadlinePassed) -> Self {
        RequestError::Io(err.into())
    }
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
    peers: &Peers,
    conn: &mut Connection,
    request: &mut Request,
) -> crate::Result<Response> {
    trace!("routing request: {:?}", request);
    use Method::*;
    match (request.method, &*request.path) {
        (Post, "/blob") | (Post, "/blob/") => match request.length {
            Some(length) => {
                let (kind, should_close) =
                    store_blob(ctx, db_ref, conn, &mut request.passport, peers, length).await?;
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
                    kind: health_check(ctx, db_ref, &mut request.passport).await,
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
                    kind: retrieve_blob(ctx, db_ref, &mut request.passport, key, false).await,
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
                    kind: retrieve_blob(ctx, db_ref, &mut request.passport, key, true).await,
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
                    kind: remove_blob(ctx, db_ref, &mut request.passport, peers, key).await,
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
    passport: &mut Passport,
    peers: &Peers,
    body_length: usize,
) -> crate::Result<(ResponseKind, bool)> {
    match read_blob(ctx, conn, passport, body_length).await {
        Ok(Outcome::Continue(())) => {}
        Ok(Outcome::Done((response, should_close))) => return Ok((response, should_close)),
        Err(err) => return Err(err),
    }

    // NOTE: `store_blob` will advance the buffer for use.
    match op::store_blob(ctx, db_ref, passport, peers, &mut conn.buf, body_length).await {
        Ok(key) => Ok((ResponseKind::Stored(key), false)),
        Err(()) => Ok((ResponseKind::ServerError, true)),
    }
}

/// Read a blob of length `body_length` from the `conn`ection.
async fn read_blob(
    ctx: &mut actor::Context<!>,
    conn: &mut Connection,
    passport: &mut Passport,
    body_length: usize,
) -> crate::Result<Outcome<(), (ResponseKind, bool)>> {
    trace!(
        "reading blob from HTTP request body: request_id={}",
        passport.id()
    );
    // TODO: get this from a configuration.
    const MAX_SIZE: usize = 1024 * 1024; // 1MB.

    if body_length == 0 {
        return Ok(Outcome::Done((
            ResponseKind::BadRequest("Can't store empty blob"),
            false,
        )));
    } else if body_length > MAX_SIZE {
        return Ok(Outcome::Done((ResponseKind::TooLargePayload, true)));
    }

    if conn.buf.len() < body_length {
        // Haven't read entire body yet.
        let want_n = body_length - conn.buf.len();
        let read_n = conn.buf.read_n_from(&mut conn.stream, want_n);
        match Deadline::timeout(ctx, TIMEOUT, read_n).await {
            Ok(()) => {}
            Err(ref err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(Outcome::Done((
                    ResponseKind::BadRequest("Incomplete blob"),
                    true,
                )))
            }
            Err(err) => return Err(err.describe("reading blob from HTTP body")),
        }
    }

    trace!(
        "read blob from HTTP request body: length={}",
        conn.buf.len()
    );
    passport.mark(Event::ReadHttpBody);
    Ok(Outcome::Continue(()))
}

/// Retrieve the blob associated with `key` from the actor behind the `db_ref`.
async fn retrieve_blob(
    ctx: &mut actor::Context<!>,
    db_ref: &mut ActorRef<db::Message>,
    passport: &mut Passport,
    key: Key,
    is_head: bool,
) -> ResponseKind {
    match op::retrieve_blob(ctx, db_ref, passport, key).await {
        Ok(Some(BlobEntry::Stored(blob))) => {
            if !is_head {
                if let Err(err) = blob.prefetch() {
                    warn!(
                        "error prefetching blob, continuing: {}: request_id=\"{}\"",
                        err,
                        passport.id()
                    );
                }
            }
            ResponseKind::Ok(blob)
        }
        Ok(Some(BlobEntry::Removed(removed_at))) => ResponseKind::Removed(removed_at),
        Ok(None) => ResponseKind::NotFound,
        Err(()) => ResponseKind::ServerError,
    }
}

/// Remove the blob associated with `key` from the actor behind the `db_ref`.
async fn remove_blob(
    ctx: &mut actor::Context<!>,
    db_ref: &mut ActorRef<db::Message>,
    passport: &mut Passport,
    peers: &Peers,
    key: Key,
) -> ResponseKind {
    match op::remove_blob(ctx, db_ref, passport, peers, key).await {
        Ok(Some(removed_at)) => ResponseKind::Removed(removed_at),
        Ok(None) => ResponseKind::NotFound,
        Err(()) => ResponseKind::ServerError,
    }
}

/// Runs a health check on the actor behind the `db_ref`.
async fn health_check(
    ctx: &mut actor::Context<!>,
    db_ref: &mut ActorRef<db::Message>,
    passport: &mut Passport,
) -> ResponseKind {
    match op::check_health(ctx, db_ref, passport).await {
        Ok(..) => ResponseKind::HealthOk,
        Err(()) => ResponseKind::ServerError,
    }
}

/// HTTP response.
#[derive(Debug)]
pub struct Response {
    /// Request was a HEAD request.
    pub is_head: bool,
    pub should_close: bool,
    pub kind: ResponseKind,
}

/// HTTP response kind.
#[derive(Debug)]
pub enum ResponseKind {
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
    pub fn for_error(err: RequestError) -> io::Result<Response> {
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
        16 + 48 + 18 + 39 + 38 +
        // Connection header (keep-alive).
        24 +
        // Extra headers: Location header (the longest) and ending "\r\n".
        149;

    /// Write all headers to `buf`, including the last empty line.
    fn write_headers(&self, buf: &mut WriteBuffer, request_id: &Uuid) {
        let (status_code, status_msg) = self.status_code();
        let content_length = self.len();

        write!(
            buf,
            "HTTP/1.1 {} {}\r\nServer: stored\r\nX-Request-ID: {}\r\nContent-Length: {}\r\n",
            status_code, status_msg, request_id, content_length,
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
