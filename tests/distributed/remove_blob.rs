use http::header::{CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, LAST_MODIFIED, LOCATION};
use http::status::StatusCode;
use log::LevelFilter;

use crate::util::http::{body, date_header, header};

const FILTER: LevelFilter = LevelFilter::Warn;

// TODO: expand testing.

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

            const DB_PORTS: &[u16] = &[11021, 11022];
            const DB_PATHS: &[&str] = tests!(_db_paths: "2_1", "2_2");
            const CONF_PATHS: &[&str] = tests!(_conf_paths: "2_1", "2_2");

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

        mod with_3_peers {
            use super::*;

            const DB_PORTS: &[u16] = &[11031, 11032, 11033];
            const DB_PATHS: &[&str] = tests!(_db_paths: "3_1", "3_2", "3_3");
            const CONF_PATHS: &[&str] = tests!(_conf_paths: "3_1", "3_2", "3_3");

            start_stored_fn!(
                &[CONF_PATHS[0], CONF_PATHS[1], CONF_PATHS[2]],
                &[DB_PATHS[0], DB_PATHS[1], DB_PATHS[2]],
                FILTER
            );

            $(
                $( #[$meta] )*
                fn $name() $body
            )+
        }
    };

    (_db_paths: $( $n: tt ),*) => {
        &[ $( concat!("/tmp/stored_remove_blob_tests", $n, ".db") ),* ];
    };
    (_conf_paths: $( $n: tt ),*) => {
        &[ $( concat!("tests/config/remove_blob_peer", $n, ".toml") ),* ];
    };
}

tests! {
    #[test]
    fn remove_hello_world() {
        let _p = start_stored();

        let url = "/blob/b7f783baed8297f0db917462184ff4f08e69c2d5e5f79a942600f9725f58ce1f29c18139bf80b06c0fff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47";
        let blob = b"Hello world";
        let blob_len = "11";

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
                expected: StatusCode::OK, blob,
                CONTENT_LENGTH => blob_len,
                LAST_MODIFIED => &last_modified,
                CONNECTION => header::KEEP_ALIVE,
            );
        }

        request!(
            DELETE DB_PORTS[1], url, body::EMPTY,
            expected: StatusCode::GONE, body::EMPTY,
            CONTENT_LENGTH => body::EMPTY_LEN,
            LAST_MODIFIED => &date_header(),
            CONNECTION => header::KEEP_ALIVE,
        );

        let last_modified = date_header();
        for port in DB_PORTS.iter().copied() {
            request!(
                GET port, url, body::EMPTY,
                expected: StatusCode::GONE, body::EMPTY,
                CONTENT_LENGTH => body::EMPTY_LEN,
                LAST_MODIFIED => &last_modified,
                CONNECTION => header::KEEP_ALIVE,
            );
        }
    }

    #[test]
    fn remove_hello_mars_twice_same_node() {
        let _p = start_stored();

        let url = "/blob/b09bcc84b88e440dad90bb19baf0c0216d8929baebc785fa0e387a17c46fe131f45109b5f06a632781c5ecf1bf1257c205bbea6d3651a9364a7fc6048cdc155c";
        let blob = b"Hello mars";
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
                expected: StatusCode::OK, blob,
                CONTENT_LENGTH => blob_len,
                LAST_MODIFIED => &last_modified,
                CONNECTION => header::KEEP_ALIVE,
            );
        }

        let last_modified = date_header();
        for _ in 0..2 {
            request!(
                DELETE DB_PORTS[0], url, body::EMPTY,
                expected: StatusCode::GONE, body::EMPTY,
                CONTENT_LENGTH => body::EMPTY_LEN,
                LAST_MODIFIED => &last_modified, // Second call mustn't overwrite the time.
                CONNECTION => header::KEEP_ALIVE,
            );
        }

        for port in DB_PORTS.iter().copied() {
            request!(
                GET port, url, body::EMPTY,
                expected: StatusCode::GONE, body::EMPTY,
                CONTENT_LENGTH => body::EMPTY_LEN,
                LAST_MODIFIED => &last_modified,
                CONNECTION => header::KEEP_ALIVE,
            );
        }
    }

    #[test]
    fn remove_hello_moon_twice_different_nodes() {
        let _p = start_stored();

        let url = "/blob/f9024e47777547c1e80da15d5b8edcf3cbc592ae889bb4d86b059dd5947977eb94bac26024d9d9dcd5a57a758efd30ed0011290b15ea09bfe07ff53bfdbaeac3";
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
                expected: StatusCode::OK, blob,
                CONTENT_LENGTH => blob_len,
                LAST_MODIFIED => &last_modified,
                CONNECTION => header::KEEP_ALIVE,
            );
        }

        let last_modified = date_header();
        for _ in 0..2 {
            request!(
                DELETE DB_PORTS[1], url, body::EMPTY,
                expected: StatusCode::GONE, body::EMPTY,
                CONTENT_LENGTH => body::EMPTY_LEN,
                LAST_MODIFIED => &last_modified, // Second call mustn't overwrite the time.
                CONNECTION => header::KEEP_ALIVE,
            );
        }

        for port in DB_PORTS.iter().copied() {
            request!(
                GET port, url, body::EMPTY,
                expected: StatusCode::GONE, body::EMPTY,
                CONTENT_LENGTH => body::EMPTY_LEN,
                LAST_MODIFIED => &last_modified,
                CONNECTION => header::KEEP_ALIVE,
            );
        }
    }

    #[test]
    fn remove_blob_never_stored() {
        let _p = start_stored();

        let url = "/blob/aaaaaa84b88e440dad90bb19baf0c0216d8929baebc785fa0e387a17c46fe131f45109b5f06a632781c5ecf1bf1257c205bbea6d3651a9364a7fc6048cdc155c";
        for port in DB_PORTS.iter().copied() {
            request!(
                DELETE port, url, body::EMPTY,
                expected: StatusCode::NOT_FOUND, body::NOT_FOUND,
                CONTENT_LENGTH => body::NOT_FOUND_LEN,
                CONTENT_TYPE => header::PLAIN_TEXT,
                CONNECTION => header::KEEP_ALIVE,
            );
        }
    }
}
