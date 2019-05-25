//! Module with parsing types.

use std::mem::size_of;

use byteorder::{ByteOrder, NetworkEndian};

use crate::Hash;

/// Minimum size for a value to using streaming.
pub const STREAMING_SIZE_MIN: usize = 1024;

/// The result of a parsing function.
///
/// In case of sucessfull parsing it will return `T` (either a request or
/// response) and the number of bytes used for the request.
pub type Result<T> = std::result::Result<(T, usize), Error>;

/// Error returned by parsing.
#[derive(Debug, Eq, PartialEq)]
pub enum Error {
    /// The bytes provided did not include a complete request.
    ///
    /// This not really an error.
    Incomplete,
    /// Request or response type is invalid.
    InvalidType,
}

/// A parsed request.
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
    /// Retrieve a value with the given hash.
    Retrieve(&'a Hash),
    /// Remove a value with the given hash.
    Remove(&'a Hash),
}

/// Parse a request.
///
/// It returns a parsed [`Request`], borrowing data from the input, and the
/// number of bytes that make up the request. Or it returns an [`Error`].
pub fn request<'a>(bytes: &'a [u8]) -> Result<Request<'a>> {
    match bytes.first() {
        Some(byte) => match byte {
            1 => parse_value(bytes).map(|(value, n)| match value {
                    Value::Stream(value_size) => (Request::StreamStore { value_size }, n),
                    Value::Full(value) => (Request::Store(value), n),
                }),
            2 => parse_hash(bytes)
                .map(|(hash, n)| (Request::Retrieve(hash), n)),
            3 => parse_hash(bytes)
                .map(|(hash, n)| (Request::Remove(hash), n)),
            _ => Err(Error::InvalidType),
        },
        None => Err(Error::Incomplete),
    }
}

/// A parsed response.
#[derive(Debug, Eq, PartialEq)]
pub enum Response<'a> {
    /// Generic OK response.
    Ok,
    /// Value is successfully stored.
    Store(&'a Hash),
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

/// Parse a response.
///
/// It returns a parsed [`Response`], borrowing data from the input, and the
/// number of bytes that make up the response. Or it returns an [`Error`].
pub fn response<'a>(bytes: &'a [u8]) -> Result<Response<'a>> {
    match bytes.first() {
        Some(byte) => match byte {
            1 => Ok((Response::Ok, 1)),
            2 => parse_hash(bytes)
                .map(|(hash, n)| (Response::Store(hash), n)),
            3 => parse_value(bytes).map(|(value, n)| match value {
                    Value::Stream(value_size) => (Response::StreamValue { value_size }, n),
                    Value::Full(value) => (Response::Value(value), n),
                }),
            4 => Ok((Response::ValueNotFound, 1)),
            _ => Err(Error::InvalidType),
        },
        None => Err(Error::Incomplete),
    }
}

/// A parsed value that either can be streamed or is fully parsed.
enum Value<'a> {
    Stream(usize),
    Full(&'a [u8]),
}

/// Parse a value.
///
/// Expects the first byte to be the request type, which is ignored.
fn parse_value<'a>(bytes: &'a [u8]) -> Result<Value<'a>> {
    if bytes.len() >= 1 + size_of::<u32>() {
        const HEADER_SIZE: usize = 1 + size_of::<u32>();
        let size = NetworkEndian::read_u32(&bytes) as usize;
        if size >= STREAMING_SIZE_MIN {
            // Large values we'll stream.
            Ok((Value::Stream(size), HEADER_SIZE))
        } else if bytes.len() >= (HEADER_SIZE + size) {
            // Small values we parse in one go if possible.
            let value = &bytes[HEADER_SIZE..HEADER_SIZE + size];
            Ok((Value::Full(value), HEADER_SIZE + size))
        } else {
            Err(Error::Incomplete)
        }
    } else {
        Err(Error::Incomplete)
    }
}

/// Parse a `Hash`.
///
/// Expects the first byte to be the request type, which is ignored.
fn parse_hash<'a>(bytes: &'a [u8]) -> Result<&'a Hash> {
    if bytes.len() >= 1 + Hash::LENGTH {
        let hash = Hash::from_bytes(&bytes[1..Hash::LENGTH+1]);
        Ok((hash, 1 + Hash::LENGTH))
    } else {
        Err(Error::Incomplete)
    }
}
