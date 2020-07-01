use std::thread::sleep;
use std::time::Duration;

use http::header::{CONNECTION, CONTENT_LENGTH, LAST_MODIFIED, LOCATION};
use http::status::StatusCode;
use log::LevelFilter;

use crate::util::http::{body, date_header, header};

const FILTER: LevelFilter = LevelFilter::Warn;

#[test]
fn syncing_peers() {
    // Node connections in configuration:
    // Node 1 --> Node 2 --> Node 3 (not connected to any peers).
    // After the startup the should all be connected to each other.

    const DB_PORTS: &[u16] = &[10001, 10002, 10003];
    const DB_PATHS: &[&str] = &[
        "/tmp/sync_peers_tests_1.db",
        "/tmp/sync_peers_tests_2.db",
        "/tmp/sync_peers_tests_3.db",
    ];
    const CONF_PATHS: &[&str] = &[
        "tests/config/sync_peers_1.toml",
        "tests/config/sync_peers_2.toml",
        "tests/config/sync_peers_3.toml",
    ];

    start_stored_fn!(
        &[CONF_PATHS[0], CONF_PATHS[1], CONF_PATHS[2]],
        &[DB_PATHS[0], DB_PATHS[1], DB_PATHS[2]],
        FILTER
    );

    let _p = start_stored();

    // Because we send our post request to the node that doesn't have any peers
    // in the configuration we give the other nodes some time to start up and
    // sync.
    sleep(Duration::from_secs(1));

    let url = "/blob/b7f783baed8297f0db917462184ff4f08e69c2d5e5f79a942600f9725f58ce1f29c18139bf80b06c0fff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47";
    let blob = b"Hello world";
    let blob_len = "11";
    request!(
        POST DB_PORTS[2], "/blob", blob,
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
