//! End to end tests for distributed implementation.
//!
//! Ports in the following ranges:
//! * 10000 - 10999: startup
//! * 11000 - 11999: store blob.
//! * 12000 - 12999: remove blob.
//! * 12000 - 12999: remove blob.
//! * 13000 - 13999: mocked tests.
//! * 14000 - 14999: peer server tests.
//! Where 1x1xx (e.g. 10101, or 12103) are ports used for peers.

#![feature(write_all_vectored)]

#[macro_use]
mod util;

// The actual tests.
mod distributed {
    mod remove_blob;
    mod startup;
    mod store_blob;

    // The tests above are proper end to end tests. However we can't test
    // everything that way (e.g. disconnects), so we need to mock some stuff.
    mod mocked;

    mod peer_server;
}
