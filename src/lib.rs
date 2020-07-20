#![feature(
    bool_to_option,
    box_into_raw_non_null,
    drain_filter,
    exact_size_is_empty,
    hash_set_entry,
    maybe_uninit_slice,
    maybe_uninit_slice_assume_init,
    maybe_uninit_uninit_array,
    never_type,
    result_flattening,
    type_alias_impl_trait
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
pub mod util;

pub use buffer::Buffer;
pub use error::{Describe, Error, Result};
pub use key::Key;
