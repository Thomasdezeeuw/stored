//! Tests for GET and HEAD requests.

use std::str;

use http::header::{CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, LAST_MODIFIED};
use http::status::StatusCode;
use lazy_static::lazy_static;
use log::LevelFilter;

mod util;

use util::http::{assert_response, body, header, request};
use util::{Proc, ProcLock};

const DB_PORT: u16 = 9001;
const CONF_PATH: &'static str = "tests/config/get_head.toml";
const FILTER: LevelFilter = LevelFilter::Error;

/// Start the stored server.
fn start_stored() -> Proc {
    lazy_static! {
        static ref PROC: ProcLock = ProcLock::new(None);
    }
    util::start_stored(CONF_PATH, &PROC, FILTER)
}

/// Make a GET and HEAD request and check the response.
macro_rules! request {
    (
        // Request path and body.
        $path: expr, $body: expr,
        // The wanted status, body and headers in the response.
        expected: $want_status: expr, $want_body: expr,
        $($header_name: ident => $header_value: expr),*,
    ) => {{
        let _p = start_stored();

        let response = request("GET", $path, DB_PORT, &[], $body).unwrap();
        assert_response(response, $want_status, &[ $( ($header_name, $header_value),)* ], $want_body);

        // HEAD must always be the same as the response to a GET request, but
        // must return an empty body.
        let response = request("HEAD", $path, DB_PORT, &[], $body).unwrap();
        assert_response(response, $want_status, &[ $( ($header_name, $header_value),)* ], body::EMPTY);
    }};
}

#[test]
fn index() {
    request!(
        "/", body::EMPTY,
        expected: StatusCode::NOT_FOUND, body::NOT_FOUND,
        CONTENT_LENGTH => body::NOT_FOUND_LEN,
        CONTENT_TYPE => header::PLAIN_TEXT,
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
fn not_found() {
    request!(
        "/404", body::EMPTY,
        expected: StatusCode::NOT_FOUND, body::NOT_FOUND,
        CONTENT_LENGTH => body::NOT_FOUND_LEN,
        CONTENT_TYPE => header::PLAIN_TEXT,
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
fn hello_world_blob() {
    let url = "/blob/b7f783baed8297f0db917462184ff4f08e69c2d5e5f79a942600f9725f58ce1f29c18139bf80b06c0fff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47";
    request!(
        url, body::EMPTY,
        expected: StatusCode::OK, b"Hello world",
        CONTENT_LENGTH => "11",
        LAST_MODIFIED => "Thu, 01 Jan 1970 00:00:05 GMT",
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
fn zero_content_length() {
    let url = "/blob/b7f783baed8297f0db917462184ff4f08e69c2d5e5f79a942600f9725f58ce1f29c18139bf80b06c0fff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47";

    let _p = start_stored();

    for method in &["GET", "HEAD"] {
        let response =
            request(method, url, DB_PORT, &[(CONTENT_LENGTH, "0")], body::EMPTY).unwrap();
        assert_response(
            response,
            StatusCode::OK,
            &[
                (CONTENT_LENGTH, "11"),
                (LAST_MODIFIED, "Thu, 01 Jan 1970 00:00:05 GMT"),
                (CONNECTION, header::KEEP_ALIVE),
            ],
            if *method == "GET" {
                b"Hello world"
            } else {
                body::EMPTY
            },
        );
    }
}

#[test]
fn hello_mars_blob() {
    let url = "/blob/b09bcc84b88e440dad90bb19baf0c0216d8929baebc785fa0e387a17c46fe131f45109b5f06a632781c5ecf1bf1257c205bbea6d3651a9364a7fc6048cdc155c";
    request!(
        url, body::EMPTY,
        expected: StatusCode::OK, b"Hello mars",
        CONTENT_LENGTH => "10",
        LAST_MODIFIED => "Thu, 01 Jan 1970 00:00:50 GMT",
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
fn not_present_blob() {
    let url = "/blob/a09bcc84b88e440dad90bb19baf0c0216d8929baebc785fa0e387a17c46fe131f45109b5f06a632781c5ecf1bf1257c205bbea6d3651a9364a7fc6048cdc155c";
    request!(
        url, body::EMPTY,
        expected: StatusCode::NOT_FOUND, body::NOT_FOUND,
        CONTENT_LENGTH => body::NOT_FOUND_LEN,
        CONTENT_TYPE => header::PLAIN_TEXT,
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
fn blob_index() {
    request!(
        "/blob", body::EMPTY,
        expected: StatusCode::NOT_FOUND, body::NOT_FOUND,
        CONTENT_LENGTH => body::NOT_FOUND_LEN,
        CONTENT_TYPE => header::PLAIN_TEXT,
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
fn empty_key_blob() {
    request!(
        "/blob/", body::EMPTY,
        expected: StatusCode::BAD_REQUEST, body::INVALID_KEY,
        CONTENT_LENGTH => body::INVALID_KEY_LEN,
        CONTENT_TYPE => header::PLAIN_TEXT,
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
fn invalid_key_blob_too_short() {
    request!(
        "/blob/abc", body::EMPTY,
        expected: StatusCode::BAD_REQUEST, body::INVALID_KEY,
        CONTENT_LENGTH => body::INVALID_KEY_LEN,
        CONTENT_TYPE => header::PLAIN_TEXT,
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
fn invalid_key_blob_too_long() {
    let url = "/blob/a09bcc84b88e440dad90bb19baf0c0216d8929baebc785fa0e387a17c46fe131f45109b5f06a632781c5ecf1bf1257c205bbea6d3651a9364a7fc6048cdc155c123";
    request!(
        url, body::EMPTY,
        expected: StatusCode::BAD_REQUEST, body::INVALID_KEY,
        CONTENT_LENGTH => body::INVALID_KEY_LEN,
        CONTENT_TYPE => header::PLAIN_TEXT,
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
fn invalid_key_blob_too_not_hex() {
    let url = "/blob/zzzbcc84b88e440dad90bb19baf0c0216d8929baebc785fa0e387a17c46fe131f45109b5f06a632781c5ecf1bf1257c205bbea6d3651a9364a7fc6048cdc155c";
    request!(
        url, body::EMPTY,
        expected: StatusCode::BAD_REQUEST, body::INVALID_KEY,
        CONTENT_LENGTH => body::INVALID_KEY_LEN,
        CONTENT_TYPE => header::PLAIN_TEXT,
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
fn with_body() {
    let url = "/blob/b7f783baed8297f0db917462184ff4f08e69c2d5e5f79a942600f9725f58ce1f29c18139bf80b06c0fff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47";

    let _p = start_stored();

    for method in &["GET", "HEAD"] {
        let response =
            request(method, url, DB_PORT, &[(CONTENT_LENGTH, "9")], b"some body").unwrap();
        assert_response(
            response,
            StatusCode::BAD_REQUEST,
            &[
                (CONTENT_LENGTH, body::UNEXPECTED_BODY_LEN),
                (CONTENT_TYPE, header::PLAIN_TEXT),
                (CONNECTION, header::CLOSE),
            ],
            if *method == "GET" {
                body::UNEXPECTED_BODY
            } else {
                body::EMPTY
            },
        );
    }
}

#[test]
fn health_check() {
    request!(
        "/health", body::EMPTY,
        expected: StatusCode::OK, body::OK,
        CONTENT_LENGTH => body::OK_LEN,
        CONTENT_TYPE => header::PLAIN_TEXT,
        CONNECTION => header::KEEP_ALIVE,
    );
}
