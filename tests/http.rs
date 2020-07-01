//! End to end tests for the HTTP interface.
//!
//! Ports:
//! * 9001: get_head.
//! * 9002: post.
//! * 9003: delete.
//! * 9004: pipelining.

#[macro_use]
mod util;

mod http {
    mod delete;
    mod get_head;
    mod pipelining;
    mod post;
}
