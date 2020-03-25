//! Tests for GET and HEAD requests.

use std::str;

use http::header::{CONTENT_LENGTH, CONTENT_TYPE};
use http::status::StatusCode;
use lazy_static::lazy_static;
use log::LevelFilter;

mod util;

use util::http::{assert_response, body, header, request};
use util::{Proc, ProcLock};

const DB_PORT: u16 = 9001;
const DB_PATH: &'static str = "tests/data/001.db";
const FILTER: LevelFilter = LevelFilter::Off;

/// Start the stored server.
fn start_stored() -> Proc {
    lazy_static! {
        static ref PROC: ProcLock = ProcLock::new(None);
    }
    util::start_stored(DB_PORT, DB_PATH, &PROC, FILTER)
}

/// Make a GET request and check the response.
macro_rules! request {
    (
        // Request path and body.
        $path: expr, $req_body: expr,
        // The wanted status, body and headers in the response.
        expected: $status: expr, $body: expr,
        $($name: ident => $want: expr),*,
    ) => {{
        let response = request("GET", $path, DB_PORT, $req_body).unwrap();
        assert_response(response, $status, &[ $( ($name, $want),)* ], $body);

        // HEAD must always be the same as the response to a GET request, but
        // must return an empty body.
        let response = request("HEAD", $path, DB_PORT, $req_body).unwrap();
        assert_response(response, $status, &[ $( ($name, $want),)* ], body::EMPTY);
    }};
}

#[test]
fn index() {
    let _p = start_stored();

    request!(
        "/", body::EMPTY,
        expected: StatusCode::NOT_FOUND, body::NOT_FOUND,
        CONTENT_LENGTH => body::NOT_FOUND_LEN,
        CONTENT_TYPE => header::PLAIN_TEXT,
    );
}
