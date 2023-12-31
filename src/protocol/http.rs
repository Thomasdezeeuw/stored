//! Hypertext Transfer Protocol (HTTP) version 1.1 protocol
//! (<https://www.rfc-editor.org/rfc/rfc9112>).
//!
//! The implementation starts with [`Http`].

#![allow(warnings)] // FIXME: remove.

use std::mem::replace;
use std::ops::Range;
use std::{fmt, io};

use heph_http::body::{BodyLength, EmptyBody, OneshotBody, StreamingBody};
use heph_http::head::{Header, HeaderName, Headers, Method, StatusCode};
use heph_http::server::Connection;
use heph_rt::timer::DeadlinePassed;

use crate::key::{InvalidKeyStr, Key};
use crate::protocol::{IsFatal, Protocol, Request, Response};
use crate::storage::Blob;

/// HTTP implementation of [`Protocol`].
pub struct Http {
    conn: Connection,
    /// Reusable headers.
    headers: Headers,
    /// Reusable buffer.
    buf: Vec<u8>,
}

impl Http {
    fn new(mut conn: Connection) -> io::Result<Http> {
        conn.set_nodelay(true)?;
        Ok(Http {
            conn,
            headers: Headers::EMPTY,
            buf: Vec::new(),
        })
    }

    /// Respond with an empty body with `status_code` and a `Location` header
    /// pointing to the blob with `key`.
    async fn redirect_response(&mut self, status_code: StatusCode, key: Key) -> io::Result<()> {
        self.buf.clear();
        {
            use std::io::Write; // Limited scope.
            write!(&mut self.buf, "/blob/{key}").unwrap();
        }
        let location = Header::new(HeaderName::LOCATION, &self.buf);
        self.headers.append(location);
        self.empty_response(status_code).await
    }

    /// Respond without an empty.
    async fn empty_response(&mut self, status_code: StatusCode) -> io::Result<()> {
        self.conn
            .respond(status_code, &self.headers, EmptyBody)
            .await
    }

    /// Respond with a string response.
    async fn string_response(
        &mut self,
        status_code: StatusCode,
        body: &'static str,
    ) -> io::Result<()> {
        self.conn
            .respond(status_code, &self.headers, OneshotBody::new(body))
            .await
    }

    async fn integer_response(&mut self, status_code: StatusCode, value: usize) -> io::Result<()> {
        // TODO: avoid allocation.
        let body = OneshotBody::new(value.to_string());
        self.conn.respond(status_code, &self.headers, body).await
    }

    async fn blob_response<B: Blob>(&mut self, blob: B) -> io::Result<()> {
        let body = StreamingBody::new(blob.len(), blob.into_async_iter());
        self.conn.respond(StatusCode::OK, &self.headers, body).await
    }
}

impl Protocol for Http {
    async fn source(&mut self) -> Result<Self::Source, Self::ResponseError> {
        self.conn.peer_addr()
    }

    type Source = std::net::SocketAddr;

    async fn next_request<'a>(&'a mut self) -> Result<Option<Request<'a>>, Self::RequestError> {
        match self.conn.next_request().await {
            Ok(Some(request)) => {
                let (head, mut body) = request.split();
                match head.method() {
                    Method::Post if head.path() == "/blob" => {
                        // Read the entire blob into memory.
                        let mut body_buf = replace(&mut self.buf, Vec::new());
                        body_buf.clear();
                        // FIXME: make `Content-Length` required (expected for
                        // chunked encoding when we support streaming adding of
                        // blobs).
                        if let BodyLength::Known(len) = body.len() {
                            // FIXME: put size restrictions on this.
                            body_buf.reserve(len);
                        }
                        let before_length = body_buf.len();
                        while before_length != body_buf.len() {
                            // FIXME: put size restrictions on this.
                            body_buf.reserve(4096);
                            body_buf = body.recv(body_buf).await
                                .map_err(|err| RequestError::Conn(heph_http::server::RequestError::Io(err)))?;
                        }
                        self.buf = body_buf;
                        Ok(Some(Request::AddBlob(&self.buf)))
                    }
                    Method::Get if head.path() == "/blobs-stored" => {
                        if body.is_empty() {
                            Ok(Some(Request::BlobStored))
                        } else {
                            Err(RequestError::BodyNotEmpty)
                        }
                    }
                    Method::Get if let Some(key) = key_from_path(head.path()) => {
                        if body.is_empty() {
                            Ok(Some(Request::GetBlob(key)))
                        } else {
                            Err(RequestError::BodyNotEmpty)
                        }
                    }
                    Method::Delete if let Some(key) = key_from_path(head.path()) => {
                        if body.is_empty() {
                            Ok(Some(Request::RemoveBlob(key)))
                        } else {
                            Err(RequestError::BodyNotEmpty)
                        }
                    }
                    Method::Head if let Some(key) = key_from_path(head.path()) => {
                        if body.is_empty() {
                            Ok(Some(Request::ContainsBlob(key)))
                        } else {
                            Err(RequestError::BodyNotEmpty)
                        }
                    }
                    method => Err(RequestError::NotFound),
                }
            }
            Ok(None) => Ok(None),
            Err(err) => Err(RequestError::Conn(err)),
        }
    }

    type RequestError = RequestError;

    async fn reply<B>(&mut self, response: Response<B>) -> Result<(), Self::ResponseError>
    where
        B: Blob,
    {
        self.headers.clear();
        match response {
            // Responses to SET.
            Response::Added(key) => self.redirect_response(StatusCode::CREATED, key).await,
            Response::AlreadyStored(key) => self.redirect_response(StatusCode::CONFLICT, key).await,

            // Responses to DEL.
            Response::BlobRemoved => self.empty_response(StatusCode::NO_CONTENT).await,
            Response::BlobNotRemoved => {
                self.string_response(StatusCode::NOT_FOUND, "blob not found")
                    .await
            }

            // Responses to GET.
            Response::Blob(blob) => self.blob_response(blob).await,
            Response::BlobNotFound => {
                self.string_response(StatusCode::NOT_FOUND, "blob not found")
                    .await
            }

            // Responses to EXISTS.
            // NOTE: this is a response to a `HEAD` request, so don't return a
            // body.
            Response::ContainsBlob => self.empty_response(StatusCode::OK).await,
            Response::NotContainBlob => self.empty_response(StatusCode::NOT_FOUND).await,

            // Response to DBSIZE.
            Response::ContainsBlobs(amount) => self.integer_response(StatusCode::OK, amount).await,

            // Generic server error.
            Response::Error => {
                self.string_response(StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
                    .await
            }
        }
    }

    async fn reply_to_error<'a>(
        &'a mut self,
        err: Self::RequestError,
    ) -> Result<(), Self::ResponseError> {
        match err {
            RequestError::NotFound => {
                self.string_response(StatusCode::NOT_FOUND, "path not found")
                    .await
            }
            RequestError::BodyNotEmpty => {
                self.string_response(StatusCode::NOT_FOUND, "unexpected non-empty body")
                    .await
            }
            RequestError::Conn(heph_http::server::RequestError::Io(_)) => Ok(()),
            RequestError::Conn(err) => {
                self.string_response(err.proper_status_code(), err.as_str())
                    .await
            }
        }
    }

    type ResponseError = io::Error;
}

/// Extracts `/blob/{key}` from `path`, or `None` if it's invalid.
fn key_from_path(path: &str) -> Option<Key> {
    path.strip_prefix("/blob/")
        .and_then(|key_str| key_str.parse().ok())
}

/// Error reading request.
#[derive(Debug)]
pub enum RequestError {
    /// Invalid path.
    NotFound,
    /// Expected an empty body, but got a non-empty body.
    BodyNotEmpty,
    /// Connection error.
    Conn(heph_http::server::RequestError),
}

impl From<DeadlinePassed> for RequestError {
    fn from(err: DeadlinePassed) -> RequestError {
        RequestError::Conn(heph_http::server::RequestError::Io(err.into()))
    }
}

impl IsFatal for RequestError {
    fn is_fatal(&self) -> bool {
        match self {
            RequestError::NotFound | RequestError::BodyNotEmpty => false,
            RequestError::Conn(err) => err.should_close(),
        }
    }
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestError::NotFound => "not found".fmt(f),
            RequestError::BodyNotEmpty => "unexpected body".fmt(f),
            RequestError::Conn(err) => err.fmt(f),
        }
    }
}
