//! Tests for GET and HEAD requests.

use std::sync::Once;

use http::header::{CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, LAST_MODIFIED};
use http::status::StatusCode;
use log::LevelFilter;

use crate::util::copy_database;
use crate::util::http::{assert_response, body, header, request};

const DB_PORT: u16 = 9001;
const DB_PATH: &str = "/tmp/stored/get_head_tests.db";
const CONF_PATH: &str = "tests/config/get_head.toml";
const FILTER: LevelFilter = LevelFilter::Warn;

static CP_DB: Once = Once::new();

start_stored_fn!(&[CONF_PATH], &[], FILTER);

/// Make a GET and HEAD request and check the response.
macro_rules! test {
    (
        // Request path and body.
        $path: expr, $body: expr,
        // The wanted status, body and headers in the response.
        expected: $want_status: expr, $want_body: expr,
        $($header_name: ident => $header_value: expr),*,
    ) => {{
        CP_DB.call_once(|| {
            copy_database("tests/data/001.db", DB_PATH);
        });

        let _p = start_stored();

        request!(
            GET DB_PORT, $path, $body,
            expected: $want_status, $want_body,
            $($header_name => $header_value),*,
        );

        request!(
            HEAD DB_PORT, $path, $body,
            expected: $want_status,
            $($header_name => $header_value),*,
        );
    }};
}

#[test]
fn index() {
    test!(
        "/", body::EMPTY,
        expected: StatusCode::NOT_FOUND, body::NOT_FOUND,
        CONTENT_LENGTH => body::NOT_FOUND_LEN,
        CONTENT_TYPE => header::PLAIN_TEXT,
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
fn not_found() {
    test!(
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
    test!(
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
        let response = request(method, url, DB_PORT, &[(CONTENT_LENGTH, "0")], body::EMPTY);
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
    test!(
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
    test!(
        url, body::EMPTY,
        expected: StatusCode::NOT_FOUND, body::NOT_FOUND,
        CONTENT_LENGTH => body::NOT_FOUND_LEN,
        CONTENT_TYPE => header::PLAIN_TEXT,
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
fn blob_index() {
    test!(
        "/blob", body::EMPTY,
        expected: StatusCode::NOT_FOUND, body::NOT_FOUND,
        CONTENT_LENGTH => body::NOT_FOUND_LEN,
        CONTENT_TYPE => header::PLAIN_TEXT,
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
fn empty_key_blob() {
    test!(
        "/blob/", body::EMPTY,
        expected: StatusCode::BAD_REQUEST, body::INVALID_KEY,
        CONTENT_LENGTH => body::INVALID_KEY_LEN,
        CONTENT_TYPE => header::PLAIN_TEXT,
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
fn invalid_key_blob_too_short() {
    test!(
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
    test!(
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
    test!(
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
        let response = request(method, url, DB_PORT, &[(CONTENT_LENGTH, "9")], b"some body");
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
    test!(
        "/health", body::EMPTY,
        expected: StatusCode::OK, body::OK,
        CONTENT_LENGTH => body::OK_LEN,
        CONTENT_TYPE => header::PLAIN_TEXT,
        CONNECTION => header::KEEP_ALIVE,
    );
}
