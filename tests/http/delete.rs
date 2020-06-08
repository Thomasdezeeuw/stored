//! Tests for DELETE requests.

use std::{fs, str};

use http::header::{CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, LAST_MODIFIED, LOCATION};
use http::status::StatusCode;
use log::LevelFilter;

use crate::util::http::{assert_response, body, date_header, header, request};

const DB_PORT: u16 = 9005;
const DB_PATH: &'static str = "/tmp/stored_delete_tests.db";
const CONF_PATH: &'static str = "tests/config/delete.toml";
const FILTER: LevelFilter = LevelFilter::Error;

start_stored_fn!(&[CONF_PATH], &[DB_PATH], FILTER);

#[test]
fn remove_hello_world() {
    let _p = start_stored();

    let url = "/blob/b7f783baed8297f0db917462184ff4f08e69c2d5e5f79a942600f9725f58ce1f29c18139bf80b06c0fff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47";
    request!(
        POST DB_PORT, "/blob", b"Hello world",
        CONTENT_LENGTH => "11";
        expected: StatusCode::CREATED, body::EMPTY,
        CONTENT_LENGTH => body::EMPTY_LEN,
        LOCATION => url,
        CONNECTION => header::KEEP_ALIVE,
    );

    let last_modified = date_header();
    request!(
        GET DB_PORT, url, body::EMPTY,
        expected: StatusCode::OK, b"Hello world",
        CONTENT_LENGTH => "11",
        LAST_MODIFIED => &last_modified,
        CONNECTION => header::KEEP_ALIVE,
    );

    request!(
        DELETE DB_PORT, url, body::EMPTY,
        expected: StatusCode::GONE, body::EMPTY,
        CONTENT_LENGTH => body::EMPTY_LEN,
        LAST_MODIFIED => &last_modified,
        CONNECTION => header::KEEP_ALIVE,
    );

    request!(
        GET DB_PORT, url, body::EMPTY,
        expected: StatusCode::GONE, body::EMPTY,
        CONTENT_LENGTH => body::EMPTY_LEN,
        LAST_MODIFIED => &last_modified,
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
fn remove_hello_mars_twice() {
    let _p = start_stored();

    let url = "/blob/b09bcc84b88e440dad90bb19baf0c0216d8929baebc785fa0e387a17c46fe131f45109b5f06a632781c5ecf1bf1257c205bbea6d3651a9364a7fc6048cdc155c";
    let blob = b"Hello mars";
    let blob_len = "10";
    request!(
        POST DB_PORT, "/blob", blob,
        CONTENT_LENGTH => blob_len;
        expected: StatusCode::CREATED, body::EMPTY,
        CONTENT_LENGTH => body::EMPTY_LEN,
        LOCATION => url,
        CONNECTION => header::KEEP_ALIVE,
    );

    let last_modified = date_header();
    request!(
        GET DB_PORT, url, body::EMPTY,
        expected: StatusCode::OK, blob,
        CONTENT_LENGTH => blob_len,
        LAST_MODIFIED => &last_modified,
        CONNECTION => header::KEEP_ALIVE,
    );

    let last_modified = date_header();
    request!(
        DELETE DB_PORT, url, body::EMPTY,
        expected: StatusCode::GONE, body::EMPTY,
        CONTENT_LENGTH => body::EMPTY_LEN,
        LAST_MODIFIED => &last_modified,
        CONNECTION => header::KEEP_ALIVE,
    );

    request!(
        DELETE DB_PORT, url, body::EMPTY,
        expected: StatusCode::GONE, body::EMPTY,
        CONTENT_LENGTH => body::EMPTY_LEN,
        LAST_MODIFIED => &last_modified, // Mustn't overwrite the time.
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
fn remove_blob_never_stored() {
    let _p = start_stored();

    let url = "/blob/aaaaaa84b88e440dad90bb19baf0c0216d8929baebc785fa0e387a17c46fe131f45109b5f06a632781c5ecf1bf1257c205bbea6d3651a9364a7fc6048cdc155c";
    request!(
        DELETE DB_PORT, url, body::EMPTY,
        expected: StatusCode::NOT_FOUND, body::NOT_FOUND,
        CONTENT_LENGTH => body::NOT_FOUND_LEN,
        CONTENT_TYPE => header::PLAIN_TEXT,
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
fn index() {
    let _p = start_stored();
    request!(
        DELETE DB_PORT, "/", body::EMPTY,
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
        DELETE DB_PORT, "/404", body::EMPTY,
        expected: StatusCode::NOT_FOUND, body::NOT_FOUND,
        CONTENT_LENGTH => body::NOT_FOUND_LEN,
        CONTENT_TYPE => header::PLAIN_TEXT,
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
fn invalid_content_length_text() {
    let _p = start_stored();

    let _p = start_stored();
    let url = "/blob/b7f783baed8297f0db917462184ff4f08e69c2d5e5f79a942600f9725f58ce1f29c18139bf80b06c0fff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47";
    let response = request(
        "DELETE",
        url,
        DB_PORT,
        &[(CONTENT_LENGTH, "abc")],
        b"some body",
    )
    .unwrap();
    assert_response(
        response,
        StatusCode::LENGTH_REQUIRED,
        &[
            (CONTENT_LENGTH, body::LENGTH_REQUIRED_LEN),
            (CONTENT_TYPE, header::PLAIN_TEXT),
            (CONNECTION, header::CLOSE),
        ],
        body::LENGTH_REQUIRED,
    );
}

// TODO: test Content-Length header that is not UTF-8.

#[test]
fn with_body() {
    let _p = start_stored();
    let url = "/blob/b7f783baed8297f0db917462184ff4f08e69c2d5e5f79a942600f9725f58ce1f29c18139bf80b06c0fff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47";
    let response = request(
        "DELETE",
        url,
        DB_PORT,
        &[(CONTENT_LENGTH, "9")],
        b"some body",
    )
    .unwrap();
    assert_response(
        response,
        StatusCode::BAD_REQUEST,
        &[
            (CONTENT_LENGTH, body::UNEXPECTED_BODY_LEN),
            (CONTENT_TYPE, header::PLAIN_TEXT),
            (CONNECTION, header::CLOSE),
        ],
        body::UNEXPECTED_BODY,
    );
}
