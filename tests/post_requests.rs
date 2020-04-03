//! Tests for POST requests.

use std::sync::Once;
use std::{fs, str};

use http::header::{CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, LAST_MODIFIED, LOCATION};
use http::status::StatusCode;
use lazy_static::lazy_static;
use log::LevelFilter;

mod util;

use util::http::{assert_response, body, date_header, header, request};
use util::{Proc, ProcLock};

const DB_PORT: u16 = 9002;
const DB_PATH: &'static str = "/tmp/stored_post_tests.db";
const CONF_PATH: &'static str = "tests/config/post.toml";
const FILTER: LevelFilter = LevelFilter::Error;

/// Start the stored server.
fn start_stored() -> Proc {
    lazy_static! {
        static ref PROC: ProcLock = ProcLock::new(None);
    }

    static REMOVE: Once = Once::new();
    REMOVE.call_once(|| {
        // Remove the old database from previous tests.
        let _ = fs::remove_dir_all(DB_PATH);
    });

    util::start_stored(CONF_PATH, &PROC, FILTER)
}

/// Make a POST request and check the response.
macro_rules! request {
    (
        // POST request path, body and headers.
        $path: expr, $body: expr,
        $($r_header_name: ident => $r_header_value: expr),*;
        // The wanted status, body and headers in the response.
        expected: $want_status: expr, $want_body: expr,
        $($header_name: ident => $header_value: expr),*,
    ) => {{
        let response = request("POST", $path, DB_PORT,
            &[ $( ($r_header_name, $r_header_value),)* ],
            $body).unwrap();
        assert_response(response, $want_status, &[ $( ($header_name, $header_value),)* ], $want_body);
    }};
    (
        // GET request path and body.
        GET $path: expr, $body: expr,
        // The wanted status, body and headers in the response.
        expected: $want_status: expr, $want_body: expr,
        $($header_name: ident => $header_value: expr),*,
    ) => {{
        let response = request("GET", $path, DB_PORT, &[], $body).unwrap();
        assert_response(response, $want_status, &[ $( ($header_name, $header_value),)* ], $want_body);
    }};
}

#[test]
fn store_hello_world() {
    let _p = start_stored();

    let url = "/blob/b7f783baed8297f0db917462184ff4f08e69c2d5e5f79a942600f9725f58ce1f29c18139bf80b06c0fff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47";
    request!(
        "/blob", b"Hello world",
        CONTENT_LENGTH => "11";
        expected: StatusCode::CREATED, body::EMPTY,
        CONTENT_LENGTH => body::EMPTY_LEN,
        LOCATION => url,
        CONNECTION => header::KEEP_ALIVE,
    );

    let last_modified = date_header();
    request!(
        GET url, body::EMPTY,
        expected: StatusCode::OK, b"Hello world",
        CONTENT_LENGTH => "11",
        LAST_MODIFIED => &last_modified,
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
fn store_hello_mars_twice() {
    let _p = start_stored();

    let url = "/blob/b09bcc84b88e440dad90bb19baf0c0216d8929baebc785fa0e387a17c46fe131f45109b5f06a632781c5ecf1bf1257c205bbea6d3651a9364a7fc6048cdc155c";
    let blob = b"Hello mars";
    let blob_len = "10";
    request!(
        "/blob", blob,
        CONTENT_LENGTH => blob_len;
        expected: StatusCode::CREATED, body::EMPTY,
        CONTENT_LENGTH => body::EMPTY_LEN,
        LOCATION => url,
        CONNECTION => header::KEEP_ALIVE,
    );

    let last_modified = date_header();
    request!(
        GET url, body::EMPTY,
        expected: StatusCode::OK, blob,
        CONTENT_LENGTH => blob_len,
        LAST_MODIFIED => &last_modified,
        CONNECTION => header::KEEP_ALIVE,
    );

    // Storing the blob a second time shouldn't change anything.
    request!(
        "/blob", blob,
        CONTENT_LENGTH => blob_len;
        expected: StatusCode::CREATED, body::EMPTY,
        CONTENT_LENGTH => body::EMPTY_LEN,
        LOCATION => url,
        CONNECTION => header::KEEP_ALIVE,
    );

    request!(
        GET url, body::EMPTY,
        expected: StatusCode::OK, blob,
        CONTENT_LENGTH => blob_len,
        LAST_MODIFIED => &last_modified,
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
fn index() {
    let _p = start_stored();
    request!(
        "/", body::EMPTY,
        /* No headers. */;
        expected: StatusCode::NOT_FOUND, body::NOT_FOUND,
        CONTENT_LENGTH => body::NOT_FOUND_LEN,
        CONTENT_TYPE => header::PLAIN_TEXT,
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
fn not_found() {
    let _p = start_stored();
    request!(
        "/404", body::EMPTY,
        /* No headers. */;
        expected: StatusCode::NOT_FOUND, body::NOT_FOUND,
        CONTENT_LENGTH => body::NOT_FOUND_LEN,
        CONTENT_TYPE => header::PLAIN_TEXT,
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
fn no_content_length() {
    let _p = start_stored();
    request!(
        "/blob", body::EMPTY,
        /* No headers. */;
        expected: StatusCode::LENGTH_REQUIRED, body::LENGTH_REQUIRED,
        CONTENT_LENGTH => body::LENGTH_REQUIRED_LEN,
        CONTENT_TYPE => header::PLAIN_TEXT,
        CONNECTION => header::CLOSE,
    );
}

#[test]
fn invalid_content_length_text() {
    let _p = start_stored();
    request!(
        "/blob", body::EMPTY,
        CONTENT_LENGTH => "abc";
        expected: StatusCode::LENGTH_REQUIRED, body::LENGTH_REQUIRED,
        CONTENT_LENGTH => body::LENGTH_REQUIRED_LEN,
        CONTENT_TYPE => header::PLAIN_TEXT,
        CONNECTION => header::CLOSE,
    );
}

// TODO: test Content-Length header that is not UTF-8.

#[test]
fn content_length_too_large() {
    let _p = start_stored();
    request!(
        "/blob", body::EMPTY,
        CONTENT_LENGTH => "1048577"; // 1 above max.
        expected: StatusCode::PAYLOAD_TOO_LARGE, body::PAYLOAD_TOO_LARGE,
        CONTENT_LENGTH => body::PAYLOAD_TOO_LARGE_LEN,
        CONTENT_TYPE => header::PLAIN_TEXT,
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
#[ignore = "returns two responses now that pipelining is supported"]
fn body_larger_than_content_length() {
    let _p = start_stored();
    let body = [1; 200];
    let url = "/blob/ceacfdb0944ac37da84556adaac97bbc9a0190ae8ca091576b91ca70e134d1067da2dd5cc311ef147b51adcfbfc2d4086560e7af1f580db8bdc961d5d7a1f127";
    request!(
        "/blob", &body,
        CONTENT_LENGTH => "100";
        expected: StatusCode::CREATED, body::EMPTY,
        CONTENT_LENGTH => body::EMPTY_LEN,
        LOCATION => url,
        CONNECTION => header::KEEP_ALIVE,
    );

    let last_modified = date_header();
    request!(
        GET url, body::EMPTY,
        expected: StatusCode::OK, &body[..100],
        CONTENT_LENGTH => "100",
        LAST_MODIFIED => &last_modified,
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
#[ignore = "writing side is shutdown in request function"]
fn body_smaller_than_content_length_no_shutdown() {
    let _p = start_stored();
    let body = [2; 100];
    request!(
        "/blob", &body,
        CONTENT_LENGTH => "200";
        expected: StatusCode::REQUEST_TIMEOUT, b"TODO.",
        CONTENT_LENGTH => "TODO.",
        CONTENT_TYPE => header::PLAIN_TEXT,
    );
}

#[test]
#[ignore = "TODO: need to shutdown the writing side of the request stream."]
fn body_smaller_than_content_length_shutdown() {
    let _p = start_stored();
    let body = [2; 100];
    request!(
        "/blob", &body,
        CONTENT_LENGTH => "200";
        expected: StatusCode::BAD_REQUEST, body::INCOMPLETE,
        CONTENT_LENGTH => body::INCOMPLETE_LEN,
        CONTENT_TYPE => header::PLAIN_TEXT,
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
fn empty_blob() {
    let _p = start_stored();
    request!(
        "/blob", body::EMPTY,
        CONTENT_LENGTH => "0";
        expected: StatusCode::BAD_REQUEST, b"Can't store empty blob",
        CONTENT_LENGTH => "22",
        CONTENT_TYPE => header::PLAIN_TEXT,
        CONNECTION => header::KEEP_ALIVE,
    );
}
