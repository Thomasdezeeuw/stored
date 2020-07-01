use http::header::{CONNECTION, CONTENT_LENGTH, LAST_MODIFIED, LOCATION};
use http::status::StatusCode;
use log::LevelFilter;

use crate::util::http::{body, date_header, header};

const FILTER: LevelFilter = LevelFilter::Warn;

// TODO: add tests:
// * replicas = 'all'

// FIXME: something the participant dispatcher reads 0 bytes from the
// connection, thinking that the coordinator relay closed the connection, if
// though it didn't, logs:
//
// ```
// 2020-07-01T08:51:56.326494Z [WARN] stored::peer::switcher: participant
//     dispatcher failed: coordinator relay closed connection unexpectedly:
//     unexpected end of file: remote=127.0.0.1:63303, server=127.0.0.1:11133
// 2020-07-01T08:51:56.326494Z [WARN] stored::peer::coordinator::relay:
//     coordinator relay failed, restarting it (4/5 restarts left): participant
//     dispatcher closed connection unexpectedly: unexpected end of file:
//     remote=127.0.0.1:11131, server=127.0.0.1:11133
// ```

// FIXME: sometimes reading a blob from the coordinator times out, log:
//
// ```log
// 2020-07-01T08:59:03.556219Z [WARN] stored::peer::participant::dispatcher:
//     store blob consensus actor failed: reading blob length: timed out
// ```

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

            const DB_PORTS: &[u16] = &[10021, 10022];
            const DB_PATHS: &[&str] = tests!(_db_paths: "2_1", "2_2");
            const CONF_PATHS: &[&str] = tests!(_conf_paths: "2_1", "2_2");

            start_stored_fn!(
                &[CONF_PATHS[0], CONF_PATHS[1]],
                &[DB_PATHS[0], DB_PATHS[1]],
                FILTER
            );

            $(
                $( #[$meta] )*
                fn $name() {
                    let _p = start_stored();
                    $body
                }
            )+
        }

        mod with_3_peers {
            use super::*;

            const DB_PORTS: &[u16] = &[10031, 10032, 10033];
            const DB_PATHS: &[&str] = tests!(_db_paths: "3_1", "3_2", "3_3");
            const CONF_PATHS: &[&str] = tests!(_conf_paths: "3_1", "3_2", "3_3");

            start_stored_fn!(
                &[CONF_PATHS[0], CONF_PATHS[1], CONF_PATHS[2]],
                &[DB_PATHS[0], DB_PATHS[1], DB_PATHS[2]],
                FILTER
            );

            $(
                $( #[$meta] )*
                fn $name() {
                    let _p = start_stored();
                    $body
                }
            )+
        }
    };

    (_db_paths: $( $n: tt ),*) => {
        &[ $( concat!("/tmp/stored_store_blob_tests", $n, ".db") ),* ];
    };
    (_conf_paths: $( $n: tt ),*) => {
        &[ $( concat!("tests/config/store_blob_peer", $n, ".toml") ),* ];
    };
}

tests! {
    #[test]
    fn store_hello_world() {
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

        // Now it should be available on all peers.
        for port in DB_PORTS.iter().copied() {
            request!(
                GET port, url, body::EMPTY,
                expected: StatusCode::OK, blob,
                CONTENT_LENGTH => blob_len,
                LAST_MODIFIED => &last_modified,
                CONNECTION => header::KEEP_ALIVE,
            );
        }
    }

    #[test]
    fn store_hello_mars_twice_same_node() {
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
                expected: StatusCode::OK, blob,
                CONTENT_LENGTH => blob_len,
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
                expected: StatusCode::OK, blob,
                CONTENT_LENGTH => blob_len,
                LAST_MODIFIED => &last_modified,
                CONNECTION => header::KEEP_ALIVE,
            );
        }
    }

    #[test]
    fn store_hello_moon_multiple_times_different_nodes() {
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
                    expected: StatusCode::OK, blob,
                    CONTENT_LENGTH => blob_len,
                    LAST_MODIFIED => &last_modified,
                    CONNECTION => header::KEEP_ALIVE,
                );
            }
        }
    }
}
