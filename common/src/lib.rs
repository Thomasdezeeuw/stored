//! Coeus common code, shared between the client and server.

pub mod parse;

mod key;

pub use key::{InvalidKeyStr, Key, KeyCalculator};

/// Type to represent a request.
#[derive(Debug, Eq, PartialEq)]
pub enum Request<'a> {
    /// Request to store value.
    Store(&'a [u8]),
    /// A store request large enough to stream.
    ///
    /// This will be returned if the `size` of the value is larger then
    /// [`STREAMING_SIZE_MIN`].
    ///
    /// Also see [`Request::Store`].
    StreamStore {
        /// Size of the value.
        value_size: usize,
    },
    /// Retrieve a value with the given key.
    Retrieve(&'a Key),
    /// Remove a value with the given key.
    Remove(&'a Key),
}

/// Type to represent a response.
#[derive(Debug, Eq, PartialEq)]
pub enum Response<'a> {
    /// Generic OK response.
    Ok,
    /// Value is successfully stored.
    Store(&'a Key),
    /// A retrieved value.
    Value(&'a [u8]),
    /// A value large enough to stream.
    ///
    /// This will be returned if the `size` of the value is larger then
    /// [`STREAMING_SIZE_MIN`].
    ///
    /// Also see [`Response::Value`].
    StreamValue {
        /// Size of the value.
        value_size: usize,
    },
    /// Value is not found.
    ValueNotFound,
}
