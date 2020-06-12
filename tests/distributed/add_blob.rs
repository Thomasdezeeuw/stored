use std::{fs, str};

use http::header::{CONNECTION, CONTENT_LENGTH, LAST_MODIFIED, LOCATION};
use http::status::StatusCode;
use log::LevelFilter;

use crate::util::http::{body, date_header, header};

const FILTER: LevelFilter = LevelFilter::Warn;

// TODO: add tests:
// * replicas = 'all'

// FIXME: there is a race between the second peer starting and connecting to the
// first peer, letting it know its a peer, and the starting of the test.
//
//
// FIXME: sometime the peer relation seems to go one way: we only get:
// relay looping: remote=127.0.0.1:10101, server=127.0.0.1:10102

/// Macro to create tests with different numbers of peers.
macro_rules! tests {
    (
        $(
        $( #[$meta: meta] )*
        fn $name: ident ()
            $body: block
        )+
    ) => {
        mod with_2_peers {
            use super::*;

            const DB_PORTS: &[u16] = &[10001, 10002];
            const DB_PATHS: &[&str] = tests!(_db_paths: 1, 2);
            const CONF_PATHS: &[&str] = tests!(_conf_paths: 1, 2);

            start_stored_fn!(
                &[CONF_PATHS[0], CONF_PATHS[1]],
                &[DB_PATHS[0], DB_PATHS[1]],
                FILTER
            );

            $(
            $( #[$meta] )*
            fn $name() {
                $body
            }
            )+
        }

        // TODO: add modules for 3 and more peers.
    };

    (_db_paths: $( $n: tt ),*) => {
        &[ $( concat!("/tmp/stored_add_blob_tests", $n, ".db") ),* ];
    };
    (_conf_paths: $( $n: tt ),*) => {
        &[ $( concat!("tests/config/add_blob_peer", $n, ".toml") ),* ];
    };
}

tests! {
    #[test]
    fn store_hello_world() {
        let _p = start_stored();

        let url = "/blob/b7f783baed8297f0db917462184ff4f08e69c2d5e5f79a942600f9725f58ce1f29c18139bf80b06c0fff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47";
        request!(
            POST DB_PORTS[0], "/blob", b"Hello world",
            CONTENT_LENGTH => "11";
            expected: StatusCode::CREATED, body::EMPTY,
            CONTENT_LENGTH => body::EMPTY_LEN,
            LOCATION => url,
            CONNECTION => header::KEEP_ALIVE,
        );
        let last_modified = date_header();

        // Now it should be available on all peers.
        for port in DB_PORTS.iter().copied() {
            request!(
                GET port, url, body::EMPTY,
                expected: StatusCode::OK, b"Hello world",
                CONTENT_LENGTH => "11",
                LAST_MODIFIED => &last_modified,
                CONNECTION => header::KEEP_ALIVE,
            );
        }
    }
}
