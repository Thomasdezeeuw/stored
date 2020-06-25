//! End to end tests for distributed implementation.

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
