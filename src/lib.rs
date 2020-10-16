#![feature(
    array_chunks,
    array_value_iter,
    bool_to_option,
    can_vector,
    const_panic,
    exact_size_is_empty,
    fmt_as_str,
    hash_set_entry,
    maybe_uninit_slice,
    move_ref_pattern,
    never_type,
    type_alias_impl_trait,
    write_all_vectored
)]

pub mod buffer;
pub mod cli;
pub mod config;
pub mod db;
pub mod error;
pub mod http;
pub mod key;
pub mod op;
pub mod passport;
pub mod peer;
pub mod storage;
pub mod timeout;
pub mod util;

mod net;

pub use buffer::Buffer;
pub use error::{Describe, Error, Result};
pub use key::Key;
