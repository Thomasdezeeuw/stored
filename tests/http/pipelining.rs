//! Tests for pipelining.

// TODO: expand testing.

use std::net::{IpAddr, Ipv4Addr, Shutdown, SocketAddr, TcpStream};
use std::sync::Once;
use std::{fs, str};

use http::header::{HeaderName, CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, LAST_MODIFIED, LOCATION};
use http::method::Method;
use http::status::StatusCode;
use lazy_static::lazy_static;
use log::LevelFilter;

use crate::util::http::{
    assert_response, body, date_header, header, read_responses, write_request,
};
use crate::util::{self, Proc, ProcLock};

const DB_PORT: u16 = 9003;
const DB_PATH: &'static str = "/tmp/stored_pipelining_tests.db";
const CONF_PATH: &'static str = "tests/config/pipelining.toml";
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

/// Make multiple request pipelining them on the same connection.
macro_rules! pipeline {
    (
        $( {
        // Request parameters: method, path, body and headers.
        $method: ident $path: expr, $body: expr,
        $($r_header_name: ident => $r_header_value: expr),*;
        // Expected response: status, body and headers in the response.
        expected: $want_status: expr, $want_body: expr,
        $($header_name: ident => $header_value: expr),*,
        } ),+ $(,)*
    ) => {{
        let _p = start_stored();

        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let address = SocketAddr::new(ip, DB_PORT);
        let mut stream = TcpStream::connect(address).unwrap();

        $(
            write_request(&mut stream, Method::$method .as_str(), $path, &[
                $( ($r_header_name, $r_header_value),)*
            ], $body).unwrap();
        )+

        // By shutting down the writing since the server will know not to expect
        // any more requests. Otherwise `read_responses` will block for ever (as
        // the server is still waiting for us to send additional requests and
        // won't close the connection).
        stream.shutdown(Shutdown::Write).unwrap();

        // FIXME: second argument -> request.method == "HEAD".
        let responses = read_responses(&mut stream, false).unwrap();
        let want: &[(StatusCode, &[(HeaderName, &str)], &[u8])] = &[
        $(
            ($want_status, &[ $( ($header_name, $header_value),)* ], $want_body),
        )+
        ];

        assert_eq!(responses.len(), want.len(), "unexpected amount of responses");
        for (response, want) in responses.into_iter().zip(want) {
            assert_response(response, want.0, want.1, want.2);
        }
    }};
}

#[test]
fn store_and_retrieve_hello_world_blob() {
    let url = "/blob/b7f783baed8297f0db917462184ff4f08e69c2d5e5f79a942600f9725f58ce1f29c18139bf80b06c0fff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47";
    pipeline!(
        {
            POST "/blob", b"Hello world",
            CONTENT_LENGTH => "11";
            expected: StatusCode::CREATED, body::EMPTY,
            CONTENT_LENGTH => body::EMPTY_LEN,
            LOCATION => url,
            CONNECTION => header::KEEP_ALIVE,
        },
        {
            GET url, body::EMPTY,
            /* No headers. */;
            expected: StatusCode::OK, b"Hello world",
            CONTENT_LENGTH => "11",
            LAST_MODIFIED => &date_header(),
            CONNECTION => header::KEEP_ALIVE,
        }
    );
}

#[test]
fn not_found_twice() {
    let url1 = "/404";
    let url2 = "/404_also";
    pipeline!(
        {
            GET url1, body::EMPTY,
            /* No headers. */;
            expected: StatusCode::NOT_FOUND, body::NOT_FOUND,
            CONTENT_LENGTH => body::NOT_FOUND_LEN,
            CONTENT_TYPE => header::PLAIN_TEXT,
            CONNECTION => header::KEEP_ALIVE,
        },
        {
            GET url2, body::EMPTY,
            /* No headers. */;
            expected: StatusCode::NOT_FOUND, body::NOT_FOUND,
            CONTENT_LENGTH => body::NOT_FOUND_LEN,
            CONTENT_TYPE => header::PLAIN_TEXT,
            CONNECTION => header::KEEP_ALIVE,
        }
    );
}
