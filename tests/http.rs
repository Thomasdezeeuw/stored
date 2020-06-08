//! End to end tests for the HTTP interface.

#[macro_use]
mod util;

mod http {
    mod delete;
    mod get_head;
    mod pipelining;
    mod post;
}
