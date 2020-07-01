//! End to end tests for distributed implementation.
//!
//! Ports in the following ranges:
//! * 10000 - 10999: startup
//! * 11000 - 11999: store blob.
//! * 12000 - 12999: remove blob.
//! Where 1x1xx (e.g. 10101, or 12103) are ports used for peers.

#[macro_use]
mod util;

#[allow(dead_code)]
struct TestPeer {
    // TODO: implementation of Stored used for testing.
}

mod distributed {
    mod remove_blob;
    mod startup;
    mod store_blob;
}
