use std::thread::sleep;
use std::time::Duration;

use http::header::{CONNECTION, CONTENT_LENGTH, LAST_MODIFIED, LOCATION};
use http::status::StatusCode;
use log::LevelFilter;

use crate::util::copy_database;
use crate::util::http::{body, date_header, header};

const FILTER: LevelFilter = LevelFilter::Warn;

#[test]
fn syncing_peers() {
    // Node connections in configuration:
    // Node 1 --> Node 2 --> Node 3 (not connected to any peers).
    // After the startup the should all be connected to each other.

    const DB_PORTS: &[u16] = &[10001, 10002, 10003];
    const DB_PATHS: &[&str] = &[
        "/tmp/stored/sync_peers_tests_1.db",
        "/tmp/stored/sync_peers_tests_2.db",
        "/tmp/stored/sync_peers_tests_3.db",
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

#[test]
fn syncing_blobs() {
    // All three nodes are fully connection, but only node 1 has the blobs
    // stored.
    // After the startup all three nodes should have all blobs.

    const DB_PORTS: &[u16] = &[10011, 10012, 10013];
    const DB_PATHS: &[&str] = &[
        "/tmp/stored/sync_blob_tests_1.db",
        "/tmp/stored/sync_blob_tests_2.db",
        "/tmp/stored/sync_blob_tests_3.db",
    ];
    const CONF_PATHS: &[&str] = &[
        "tests/config/sync_blobs_1.toml",
        "tests/config/sync_blobs_2.toml",
        "tests/config/sync_blobs_3.toml",
    ];

    start_stored_fn!(
        &[CONF_PATHS[0], CONF_PATHS[1], CONF_PATHS[2]],
        // Note: database 0 must not be removed.
        &[DB_PATHS[1], DB_PATHS[2]],
        FILTER
    );

    copy_database("tests/data/stored_sync_blobs.db", DB_PATHS[0]);

    let _p = start_stored();

    // Give the nodes some time to sync up.
    sleep(Duration::from_secs(5));

    // The blobs stored in the database of node 1.
    let tests: &[(&str, &[u8], &str, &str)] = &[
        (
            "/blob/b7f783baed8297f0db917462184ff4f08e69c2d5e5f79a942600f9725f58ce1f29c18139bf80b06c0fff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47",
            b"Hello world",
            "11",
            "Wed, 01 Jul 2020 12:28:59 GMT",
        ),
        (
            "/blob/b09bcc84b88e440dad90bb19baf0c0216d8929baebc785fa0e387a17c46fe131f45109b5f06a632781c5ecf1bf1257c205bbea6d3651a9364a7fc6048cdc155c",
            b"Hello mars",
            "10",
            "Wed, 01 Jul 2020 12:29:01 GMT",
        ),
        (
            "/blob/f9024e47777547c1e80da15d5b8edcf3cbc592ae889bb4d86b059dd5947977eb94bac26024d9d9dcd5a57a758efd30ed0011290b15ea09bfe07ff53bfdbaeac3",
            b"Hello moon",
            "10",
            "Wed, 01 Jul 2020 12:29:02 GMT",
        ),
    ];

    // Now all the blobs should be available on all nodes.
    for (url, blob, blob_len, last_modified) in tests {
        for port in DB_PORTS.iter().copied() {
            request!(
                GET port, url, body::EMPTY,
                expected: StatusCode::OK, blob,
                CONTENT_LENGTH => blob_len,
                LAST_MODIFIED => last_modified,
                CONNECTION => header::KEEP_ALIVE,
            );
        }
    }
}
