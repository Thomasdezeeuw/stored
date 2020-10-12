//! Tests for POST requests.

use http::header::{CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, LAST_MODIFIED, LOCATION};
use http::status::StatusCode;
use log::LevelFilter;

use stored::Key;

use crate::util::http::{body, date_header, header};

const DB_PORT: u16 = 9002;
const DB_PATH: &str = "/tmp/stored/post_tests.db";
const CONF_PATH: &str = "tests/config/post.toml";
const FILTER: LevelFilter = LevelFilter::Warn;

start_stored_fn!(&[CONF_PATH], &[DB_PATH], FILTER);

#[test]
fn store_hello_world() {
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
}

#[test]
fn store_hello_mars_twice() {
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

    // Storing the blob a second time shouldn't change anything.
    request!(
        POST DB_PORT, "/blob", blob,
        CONTENT_LENGTH => blob_len;
        expected: StatusCode::CREATED, body::EMPTY,
        CONTENT_LENGTH => body::EMPTY_LEN,
        LOCATION => url,
        CONNECTION => header::KEEP_ALIVE,
    );

    request!(
        GET DB_PORT, url, body::EMPTY,
        expected: StatusCode::OK, blob,
        CONTENT_LENGTH => blob_len,
        LAST_MODIFIED => &last_modified,
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
fn stream_blob() {
    let _p = start_stored();

    let url = "/blob/102a731f35127adb6dc73d39ff4c2613b732d288bd26fc920474934f0dfa53f5e36f625a7238e6a341b4e0b3bc8e09af80b0d51ec018fda4baa3ae07615d5fbf";

    const N: usize = 10;
    let mut blob = Vec::with_capacity(N * 1024);
    let blob_len = "10240";
    for i in 1u8..=N as u8 {
        blob.resize(i as usize * 1024, i);
    }

    request!(
        POST DB_PORT, "/blob", &blob,
        CONTENT_LENGTH => blob_len;
        expected: StatusCode::CREATED, body::EMPTY,
        CONTENT_LENGTH => body::EMPTY_LEN,
        LOCATION => url,
        CONNECTION => header::KEEP_ALIVE,
    );

    let last_modified = date_header();
    request!(
        GET DB_PORT, url, body::EMPTY,
        expected: StatusCode::OK, &blob,
        CONTENT_LENGTH => blob_len,
        LAST_MODIFIED => &last_modified,
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
fn stream_same_blob_twice() {
    let _p = start_stored();

    let url = "/blob/f2f26d8590761a0ed61817059cca12c8615c93c4d9bbfb7d4d105487934df53da449a6b70923c377acbd0daf3672937d7ac9ac5542374ee2bdfae16300071cdb";

    const N: usize = 32;
    let mut blob = Vec::with_capacity(N * 1024);
    let blob_len = "32768";
    for i in 0u8..N as u8 {
        blob.resize((i + 1) as usize * 1024, i);
    }

    request!(
        POST DB_PORT, "/blob", &blob,
        CONTENT_LENGTH => blob_len;
        expected: StatusCode::CREATED, body::EMPTY,
        CONTENT_LENGTH => body::EMPTY_LEN,
        LOCATION => url,
        CONNECTION => header::KEEP_ALIVE,
    );

    let last_modified = date_header();
    request!(
        GET DB_PORT, url, body::EMPTY,
        expected: StatusCode::OK, &blob,
        CONTENT_LENGTH => blob_len,
        LAST_MODIFIED => &last_modified,
        CONNECTION => header::KEEP_ALIVE,
    );

    // Storing the blob a second time shouldn't change anything.
    request!(
        POST DB_PORT, "/blob", &blob,
        CONTENT_LENGTH => blob_len;
        expected: StatusCode::CREATED, body::EMPTY,
        CONTENT_LENGTH => body::EMPTY_LEN,
        LOCATION => url,
        CONNECTION => header::KEEP_ALIVE,
    );

    request!(
        GET DB_PORT, url, body::EMPTY,
        expected: StatusCode::OK, &blob,
        CONTENT_LENGTH => blob_len,
        LAST_MODIFIED => &last_modified,
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
fn stream_part_of_a_blob() {
    let _p = start_stored();

    const N: usize = 5;
    let mut blob = Vec::with_capacity(N * 1024);
    let blob_len = "6000"; // Should be 5125.
    for i in 1u8..=N as u8 {
        blob.resize(i as usize * 1024, i);
    }

    request!(
        POST DB_PORT, "/blob", &blob,
        CONTENT_LENGTH => blob_len;
        expected: StatusCode::BAD_REQUEST, body::INCOMPLETE,
        CONTENT_LENGTH => body::INCOMPLETE_LEN,
        CONTENT_TYPE => header::PLAIN_TEXT,
        CONNECTION => header::CLOSE,
    );
}

#[test]
fn index() {
    let _p = start_stored();
    request!(
        POST DB_PORT, "/", body::EMPTY,
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
        POST DB_PORT, "/404", body::EMPTY,
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
        POST DB_PORT, "/blob", body::EMPTY,
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
        POST DB_PORT, "/blob", body::EMPTY,
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
        POST DB_PORT, "/blob", body::EMPTY,
        CONTENT_LENGTH => "1099511627776";
        expected: StatusCode::PAYLOAD_TOO_LARGE, body::PAYLOAD_TOO_LARGE,
        CONTENT_LENGTH => body::PAYLOAD_TOO_LARGE_LEN,
        CONTENT_TYPE => header::PLAIN_TEXT,
        CONNECTION => header::CLOSE,
    );
}

#[test]
#[ignore = "returns two responses now that pipelining is supported"]
fn body_larger_than_content_length() {
    let _p = start_stored();
    let body = [1; 200];
    let url = "/blob/ceacfdb0944ac37da84556adaac97bbc9a0190ae8ca091576b91ca70e134d1067da2dd5cc311ef147b51adcfbfc2d4086560e7af1f580db8bdc961d5d7a1f127";
    request!(
        POST DB_PORT, "/blob", &body,
        CONTENT_LENGTH => "100";
        expected: StatusCode::CREATED, body::EMPTY,
        CONTENT_LENGTH => body::EMPTY_LEN,
        LOCATION => url,
        CONNECTION => header::KEEP_ALIVE,
    );

    let last_modified = date_header();
    request!(
        GET DB_PORT, url, body::EMPTY,
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
        POST DB_PORT, "/blob", &body,
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
        POST DB_PORT, "/blob", &body,
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
        POST DB_PORT, "/blob", body::EMPTY,
        CONTENT_LENGTH => "0";
        expected: StatusCode::BAD_REQUEST, b"Can't store empty blob",
        CONTENT_LENGTH => "22",
        CONTENT_TYPE => header::PLAIN_TEXT,
        CONNECTION => header::KEEP_ALIVE,
    );
}

#[test]
fn max_non_stream_size() {
    let _p = start_stored();

    let blob = &[4; 4 * 1024]; // `http::MIN_STREAM_BLOB_SIZE`.
    let key = Key::for_blob(blob);
    let url = format!("/blob/{}", key);
    let blob_len = "4096";

    request!(
        POST DB_PORT, "/blob", blob,
        CONTENT_LENGTH => blob_len;
        expected: StatusCode::CREATED, body::EMPTY,
        CONTENT_LENGTH => body::EMPTY_LEN,
        LOCATION => &*url,
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
}

#[test]
fn max_non_stream_size_minus_1() {
    let _p = start_stored();

    let blob = &[5; (4 * 1024) - 1]; // `http::MIN_STREAM_BLOB_SIZE - 1`.
    let key = Key::for_blob(blob);
    let url = format!("/blob/{}", key);
    let blob_len = "4095";

    request!(
        POST DB_PORT, "/blob", blob,
        CONTENT_LENGTH => blob_len;
        expected: StatusCode::CREATED, body::EMPTY,
        CONTENT_LENGTH => body::EMPTY_LEN,
        LOCATION => &*url,
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
}

#[test]
fn min_stream_size() {
    let _p = start_stored();

    let blob = &[6; (4 * 1024) + 1]; // `http::MIN_STREAM_BLOB_SIZE + 1`.
    let key = Key::for_blob(blob);
    let url = format!("/blob/{}", key);
    let blob_len = "4097";

    request!(
        POST DB_PORT, "/blob", blob,
        CONTENT_LENGTH => blob_len;
        expected: StatusCode::CREATED, body::EMPTY,
        CONTENT_LENGTH => body::EMPTY_LEN,
        LOCATION => &*url,
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
}
