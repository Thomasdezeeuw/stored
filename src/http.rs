//! Module with the server's HTTP/1.1 implementation.
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
use std::io::{self, Write};
use std::net::SocketAddr;
use std::time::{Duration, Instant, SystemTime};
use std::{fmt, mem, str};

use chrono::{DateTime, Datelike, Timelike, Utc};
use futures_util::io::AsyncWriteExt;
use heph::log::request;
use heph::net::tcp::{self, ServerMessage, TcpStream};
use heph::rt::options::{ActorOptions, Priority};
use heph::timer::{Deadline, DeadlinePassed};
use heph::{actor, Actor, ActorRef, NewActor, RuntimeRef, Supervisor, SupervisorStrategy};
use httparse::EMPTY_HEADER;
use log::{debug, error, trace, warn};

use crate::buffer::{Buffer, WriteBuffer};
use crate::db::{self, AddBlobResponse, RemoveBlobResponse};
use crate::error::Describe;
use crate::peer::coordinator::RelayError;
use crate::peer::Peers;
use crate::storage::{AddBlob, Blob, BlobEntry, PAGE_SIZE};
use crate::{op, Key};

/// Setup the HTTP listener.
///
/// This returns a function which can be used to start the HTTP listener.
pub fn setup(
    address: SocketAddr,
    db_ref: ActorRef<db::Message>,
    peers: Peers,
) -> io::Result<impl FnOnce(&mut RuntimeRef) -> io::Result<()> + Send + Clone + 'static> {
    let http_actor = (actor as fn(_, _, _, _, _) -> _)
        .map_arg(move |(stream, arg)| (stream, arg, db_ref.clone(), peers.clone()));
    let http_listener =
        tcp::Server::setup(address, supervisor, http_actor, ActorOptions::default())?;
    Ok(move |runtime: &mut RuntimeRef| {
        let options = ActorOptions::default().with_priority(Priority::LOW);
        runtime
            .try_spawn_local(ServerSupervisor, http_listener, (), options)
            .map(|server_ref| runtime.receive_signals(server_ref.try_map()))
    })
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
            Ok(Ok(true)) => {
                route_request(&mut ctx, &mut db_ref, &peers, &mut conn, &request).await?
            }
            // Read all requests on the stream, so this actor's work is done.
            Ok(Ok(false)) => break,
            // Try operator returns the I/O errors.
            Ok(Err(err)) => {
                Response::for_error(err).map_err(|err| err.describe("reading request"))?
            }
            Err(err) => return Err(io::Error::from(err).describe("timeout reading request")),
        };

        let result = Deadline::timeout(&mut ctx, TIMEOUT, conn.write_response(&response)).await;
        match result {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(err.describe("writing HTTP response")),
            Err(err) => return Err(io::Error::from(err).describe("timeout writing HTTP response")),
        }

        // TODO: don't log invalid/partial requests.
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
const MAX_HEADERS: usize = 16;

/// Maximum size of the headers of a request in bytes.
const MAX_HEADERS_SIZE: usize = 2 * 1024;

/// Headers we're interested in.
const USER_AGENT: &str = "user-agent";
const CONTENT_LENGTH: &str = "content-length";

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
        let mut too_short = 0;
        loop {
            // At the start we likely don't have enough bytes to read the entire
            // request, however it could be that we read (part of) a request in
            // reading a previous request, so we need to check.
            if self.buf.len() > too_short {
                match request.parse(self.buf.as_bytes()) {
                    Ok(httparse::Status::Complete(bytes_read)) => {
                        self.buf.processed(bytes_read);
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
    pub async fn write_response(&mut self, response: &Response) -> io::Result<()> {
        trace!("writing HTTP response: {:?}", response);
        // Create a write buffer a write the request headers to it.
        let (_, mut write_buf) = self.buf.split_write(Response::MAX_HEADERS_SIZE);
        response.write_headers(&mut write_buf);

        // TODO: replace with `write_all_vectored`:
        // https://github.com/rust-lang/futures-rs/pull/1741.
        self.stream.write_all(write_buf.as_bytes()).await?;
        self.stream.write_all(response.body()).await
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
    pub method: Method,
    pub path: String,
    /// Empty string means not present.
    pub user_agent: String,
    pub length: Option<usize>,
}

impl Request {
    /// Create an empty `Request`.
    pub const fn empty() -> Request {
        Request {
            method: Method::Get,
            path: String::new(),
            user_agent: String::new(),
            length: None,
        }
    }

    /// Returns `true` if the "Content-Length" header is > 0.
    pub fn has_body(&self) -> bool {
        match self.length {
            Some(n) if n > 0 => true,
            _ => false,
        }
    }

    fn reset(&mut self) {
        self.method = Method::Get;
        self.path.clear();
        self.user_agent.clear();
        self.length = None;
    }

    /// Parse a request.
    fn parse(&mut self, bytes: &[u8]) -> Result<httparse::Status<usize>, RequestError> {
        let mut headers = [EMPTY_HEADER; MAX_HEADERS];
        let mut req = httparse::Request::new(&mut headers);
        match req.parse(bytes) {
            Ok(httparse::Status::Complete(bytes_read)) => {
                self.reset();

                // TODO: check req.version?

                self.path.push_str(req.path.unwrap_or(""));
                self.method =
                    Method::try_from(req.method).map_err(|()| RequestError::InvalidMethod)?;

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
                    }
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
    request: &Request,
) -> crate::Result<Response> {
    trace!("routing request: {:?}", request);
    use Method::*;
    match (request.method, &*request.path) {
        (Post, "/blob") | (Post, "/blob/") => match request.length {
            Some(length) => {
                let (kind, should_close) = store_blob(ctx, db_ref, conn, peers, length).await?;
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
                    kind: retrieve_blob(ctx, db_ref, key, false).await,
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
                    kind: retrieve_blob(ctx, db_ref, key, true).await,
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
    peers: &Peers,
    body_length: usize,
) -> crate::Result<(ResponseKind, bool)> {
    debug!("handling store blob request");
    match read_blob(ctx, conn, body_length).await {
        Ok(Status::Continue(())) => {}
        Ok(Status::Return(response, should_close)) => return Ok((response, should_close)),
        Err(err) => return Err(err),
    }

    let query = match add_blob_to_db(ctx, conn, db_ref, body_length).await {
        Status::Continue(query) => query,
        Status::Return(response, should_close) => return Ok((response, should_close)),
    };

    let key = query.key().clone();
    if !peers.is_empty() {
        debug!("running consensus algorithm to store blob: key={}", key);
        match add_blob_consensus(ctx, db_ref, peers, query).await {
            Status::Continue(()) => Ok((ResponseKind::Stored(key), false)),
            Status::Return(response, should_close) => Ok((response, should_close)),
        }
    } else {
        debug!(
            "running in single mode, not running consensus algorithm: key={}",
            key
        );
        match commit_blob_to_db(ctx, db_ref, query, SystemTime::now()).await {
            Status::Continue(()) => Ok((ResponseKind::Stored(key), false)),
            Status::Return(response, should_close) => Ok((response, should_close)),
        }
    }
}

/// Status of processing the request.
enum Status<T> {
    /// Continue processing the request.
    Continue(T),
    /// Early return in case a condition is not method, e.g. a blob is too large
    /// to store.
    Return(ResponseKind, bool),
}

/// Read a blob of length `body_length` from the `conn`ection.
async fn read_blob(
    ctx: &mut actor::Context<!>,
    conn: &mut Connection,
    body_length: usize,
) -> crate::Result<Status<()>> {
    trace!("reading blob from HTTP request body");
    // TODO: get this from a configuration.
    const MAX_SIZE: usize = 1024 * 1024; // 1MB.

    if body_length == 0 {
        return Ok(Status::Return(
            ResponseKind::BadRequest("Can't store empty blob"),
            false,
        ));
    } else if body_length > MAX_SIZE {
        return Ok(Status::Return(ResponseKind::TooLargePayload, true));
    }

    conn.buf.reserve_atleast(body_length);

    while conn.buf.len() < body_length {
        let future = Deadline::timeout(ctx, TIMEOUT, conn.buf.read_from(&mut conn.stream));
        match future.await {
            // No more bytes left, but didn't yet read the entire request body.
            Ok(Ok(0)) => {
                return Ok(Status::Return(
                    ResponseKind::BadRequest("Incomplete blob"),
                    true,
                ))
            }
            // Read some bytes.
            Ok(Ok(_)) => continue,
            Ok(Err(err)) => return Err(err.describe("reading blob from HTTP body")),
            Err(err) => {
                return Err(io::Error::from(err).describe("timeout reading blob from HTTP body"))
            }
        }
    }

    trace!(
        "read blob from HTTP request body: length={}",
        conn.buf.len()
    );
    Ok(Status::Continue(()))
}

/// Add the blob in `conn.buf` (with length `blob_length`) to the database.
/// Returns the query to commit the blob to the database.
async fn add_blob_to_db(
    ctx: &mut actor::Context<!>,
    conn: &mut Connection,
    db_ref: &mut ActorRef<db::Message>,
    blob_length: usize,
) -> Status<AddBlob> {
    // Take the body from the connection and use it as a blob to send to the
    // storage actor.
    let blob = mem::replace(&mut conn.buf, Buffer::empty());

    trace!("adding blob to database: blob_length={}", blob_length);
    match db_ref.rpc(ctx, (blob, blob_length)) {
        Ok(rpc) => match rpc.await {
            Ok((result, mut buffer)) => {
                // Mark the body as processed and put back the buffer.
                buffer.processed(blob_length);
                conn.buf = buffer;
                match result {
                    AddBlobResponse::Query(query) => Status::Continue(query),
                    AddBlobResponse::AlreadyStored(key) => {
                        Status::Return(ResponseKind::Stored(key), false)
                    }
                }
            }
            Err(err) => {
                error!("error waiting for RPC response from database: {}", err);
                Status::Return(ResponseKind::ServerError, true)
            }
        },
        Err(err) => {
            error!("error making RPC call to database: {}", err);
            Status::Return(ResponseKind::ServerError, true)
        }
    }
}

/// Runs the consensus algorithm for adding the blob (with `key`).
async fn add_blob_consensus(
    ctx: &mut actor::Context<!>,
    db_ref: &mut ActorRef<db::Message>,
    peers: &Peers,
    query: AddBlob,
) -> Status<()> {
    // Phase one: start the consensus algorithm, letting the participants
    // (peers) know we want to store a blob.
    let key = query.key().clone();
    // TODO: add a timeout.
    let (consensus_id, rpc) = peers.add_blob(ctx, key.clone());
    debug!(
        "requesting peers to store blob: consensus_id={}, query={:?}",
        consensus_id, query
    );
    let results = rpc.await;
    let (commit, abort, failed) = count_consensus_votes(&results);

    if abort > 0 || failed > 0 {
        // FIXME: support partial success.
        error!(
            "consensus algorithm failed: consensus_id={}, key={}, votes_commit={}, votes_abort={}, failed_votes={}",
            consensus_id, key, commit, abort, failed
        );
        // TODO: let the peers know to abort the query.
        // TODO: abort `AddBlob` query.
        return Status::Return(ResponseKind::ServerError, true);
    }
    debug!(
        "consensus algorithm succeeded: consensus_id={}, key={}, votes_commit={}, votes_abort={}, failed_votes={}",
        consensus_id, key, commit, abort, failed
    );

    let timestamp = select_timestamp(&results);

    // Phase two: let the participants know to commit to adding the blob.
    debug!(
        "requesting peers to commit to storing blob: consensus_id={}, query={:?}, timestamp={:?}",
        consensus_id, query, timestamp
    );
    let rpc = peers.commit_to_add_blob(ctx, consensus_id, key.clone(), timestamp);

    match commit_blob_to_db(ctx, db_ref, query, SystemTime::now()).await {
        Status::Continue(()) => {}
        Status::Return(response, should_close) => {
            return Status::Return(response, should_close);
        }
    }

    // FIXME: do something with the results.
    let results = rpc.await;
    let (commit, abort, failed) = count_consensus_votes(&results);
    debug!(
        "consensus algorithm commitment: consensus_id={}, key={}, votes_commit={}, votes_abort={}, failed_votes={}",
        consensus_id, key, commit, abort, failed
    );

    Status::Continue(())
}

/// Returns the (commit, abort, failed) votes.
fn count_consensus_votes<T>(results: &[Result<T, RelayError>]) -> (usize, usize, usize) {
    let mut commit = 1; // This peer votes to commit.
    let mut abort = 0;
    let mut failed = 0;

    for result in results {
        match result {
            Ok(..) => commit += 1,
            Err(RelayError::Abort) => abort += 1,
            Err(RelayError::Failed) => failed += 1,
        }
    }

    (commit, abort, failed)
}

/// Select the timestamp to use from consensus results.
fn select_timestamp(_results: &[Result<SystemTime, RelayError>]) -> SystemTime {
    let timestamp = SystemTime::now();
    // TODO: sync the time somehow? To ensure that if this peer has an incorrect
    // time we don't use that? We could use the largest timestamp (the most in
    // the future), but we don't want a time in the year 2100 because a peer's
    // time is incorrect.
    timestamp
}

/// Commit the blob in the `query` to the database.
async fn commit_blob_to_db(
    ctx: &mut actor::Context<!>,
    db_ref: &mut ActorRef<db::Message>,
    query: AddBlob,
    timestamp: SystemTime,
) -> Status<()> {
    trace!("committing to adding blob: query={:?}", query);
    match db_ref.rpc(ctx, (query, timestamp)) {
        Ok(rpc) => match rpc.await {
            Ok(()) => Status::Continue(()),
            Err(err) => {
                error!("error waiting for RPC response from database: {}", err);
                Status::Return(ResponseKind::ServerError, true)
            }
        },
        Err(err) => {
            error!("error making RPC call to database: {}", err);
            Status::Return(ResponseKind::ServerError, true)
        }
    }
}

/// Retrieve the blob associated with `key` from the actor behind the `db_ref`.
async fn retrieve_blob(
    ctx: &mut actor::Context<!>,
    db_ref: &mut ActorRef<db::Message>,
    key: Key,
    is_head: bool,
) -> ResponseKind {
    match op::retrieve_blob(ctx, db_ref, key).await {
        Ok(Some(BlobEntry::Stored(blob))) => {
            if blob.len() > PAGE_SIZE && !is_head {
                // If the blob is large(-ish) we'll prefetch it from disk to
                // improve performance.
                // TODO: benchmark this with large(-ish) blobs.
                if let Err(err) = blob.prefetch() {
                    warn!("error prefetching blob, continuing: {}", err);
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
    key: Key,
) -> ResponseKind {
    debug!("handling remove blob request");
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
    match op::check_health(ctx, db_ref).await {
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
