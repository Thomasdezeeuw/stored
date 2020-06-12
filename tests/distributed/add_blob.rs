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
            fn $name: ident () $body: block
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
                fn $name() $body
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

    #[test]
    fn store_hello_mars_twice_same_node() {
        let _p = start_stored();

        let url = "/blob/b09bcc84b88e440dad90bb19baf0c0216d8929baebc785fa0e387a17c46fe131f45109b5f06a632781c5ecf1bf1257c205bbea6d3651a9364a7fc6048cdc155c";
        let blob = b"Hello mars";
        let blob_len = "10";
        let post_port = DB_PORTS[1];
        request!(
            POST post_port, "/blob", blob,
            CONTENT_LENGTH => blob_len;
            expected: StatusCode::CREATED, body::EMPTY,
            CONTENT_LENGTH => body::EMPTY_LEN,
            LOCATION => url,
            CONNECTION => header::KEEP_ALIVE,
        );
        let last_modified = date_header();

        for port in DB_PORTS.iter().copied() {
            request!(
                GET port, url, body::EMPTY,
                expected: StatusCode::OK, b"Hello world",
                CONTENT_LENGTH => "11",
                LAST_MODIFIED => &last_modified,
                CONNECTION => header::KEEP_ALIVE,
            );
        }

        // Storing the blob a second time shouldn't change anything.
        request!(
            POST post_port, "/blob", blob,
            CONTENT_LENGTH => blob_len;
            expected: StatusCode::CREATED, body::EMPTY,
            CONTENT_LENGTH => body::EMPTY_LEN,
            LOCATION => url,
            CONNECTION => header::KEEP_ALIVE,
        );

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

    #[test]
    fn store_hello_moon_multiple_times_different_nodes() {
        let _p = start_stored();

        let url = "/blob/03ae65afb6e91c5668faf2c829de80160b79c812555f52c90693df7d9489114adfb85191084e3a816ec4e36870ddf4fbe84c03cd4006bcd705a983f755f4a6f7";
        let blob = b"Hello moon";
        let blob_len = "10";
        request!(
            POST DB_PORTS[0], "/blob", blob,
            CONTENT_LENGTH => blob_len;
            expected: StatusCode::CREATED, body::EMPTY,
            CONTENT_LENGTH => body::EMPTY_LEN,
            LOCATION => url,
            CONNECTION => header::KEEP_ALIVE,
        );
        let last_modified = date_header();

        for port in DB_PORTS.iter().copied() {
            request!(
                GET port, url, body::EMPTY,
                expected: StatusCode::OK, b"Hello world",
                CONTENT_LENGTH => "11",
                LAST_MODIFIED => &last_modified,
                CONNECTION => header::KEEP_ALIVE,
            );
        }

        // Storing the blob a second time shouldn't change anything.
        for post_port in DB_PORTS.iter().copied().skip(1) {
            request!(
                POST post_port, "/blob", blob,
                CONTENT_LENGTH => blob_len;
                expected: StatusCode::CREATED, body::EMPTY,
                CONTENT_LENGTH => body::EMPTY_LEN,
                LOCATION => url,
                CONNECTION => header::KEEP_ALIVE,
            );

            for get_port in DB_PORTS.iter().copied() {
                request!(
                    GET get_port, url, body::EMPTY,
                    expected: StatusCode::OK, b"Hello world",
                    CONTENT_LENGTH => "11",
                    LAST_MODIFIED => &last_modified,
                    CONNECTION => header::KEEP_ALIVE,
                );
            }
        }
    }
}
