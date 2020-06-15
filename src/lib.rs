#![feature(
    bool_to_option,
    box_into_raw_non_null,
    maybe_uninit_slice,
    maybe_uninit_slice_assume_init,
    never_type,
    result_flattening
)]

pub mod buffer;
pub mod cli;
pub mod config;
pub mod db;
pub mod error;
pub mod http;
pub mod key;
pub mod op;
pub mod peer;
pub mod storage;

pub use buffer::Buffer;
pub use error::{Describe, Error, Result};
pub use key::Key;
