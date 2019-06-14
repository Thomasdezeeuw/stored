//! Module with parsing types.

// TODO: limit the maximum value size.
// TODO: support for streaming values.

use std::mem::size_of;

use byteorder::{ByteOrder, NetworkEndian};

use crate::Key;

/// The result of a parsing function.
///
/// In case of successful parsing it will return `T` (either a request or
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

/// Parsed request.
#[derive(Debug, Eq, PartialEq)]
pub enum Request<'a> {
    /// Request to store value.
    Store(&'a [u8]),
    /// Retrieve a value with the given key.
    Retrieve(&'a Key),
    /// Remove a value with the given key.
    Remove(&'a Key),
}

/// Parse a request.
///
/// It returns a parsed [`Request`], borrowing data from the input, and the
/// number of bytes that make up the request. Or it returns an [`Error`].
pub fn request<'a>(bytes: &'a [u8]) -> Result<Request<'a>> {
    match bytes.first() {
        Some(byte) => match byte {
            1 => parse_value(bytes).map(|(value, n)| match value {
                Value::Stream(_value_size) => unimplemented!("parsing streaming value request"),
                Value::Full(value) => (Request::Store(value), n),
            }),
            2 => parse_key(bytes).map(|(key, n)| (Request::Retrieve(key), n)),
            3 => parse_key(bytes).map(|(key, n)| (Request::Remove(key), n)),
            _ => Err(Error::InvalidType),
        },
        None => Err(Error::Incomplete),
    }
}

/// Parsed reponse.
#[derive(Debug, Eq, PartialEq)]
pub enum Response<'a> {
    /// Generic OK response.
    Ok,
    /// Value is successfully stored.
    Store(&'a Key),
    /// A retrieved value.
    Value(&'a [u8]),
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
            2 => parse_key(bytes).map(|(key, n)| (Response::Store(key), n)),
            3 => parse_value(bytes).map(|(value, n)| match value {
                Value::Stream(_value_size) => unimplemented!("parsing streaming value response"),
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
    #[allow(dead_code)]
    Stream(usize),
    Full(&'a [u8]),
}

/// Parse a value.
///
/// Expects the first byte to be the request type, which is ignored.
fn parse_value<'a>(bytes: &'a [u8]) -> Result<Value<'a>> {
    // 1 bytes for the request type and the bytes for the size of the value.
    const HEADER_SIZE: usize = 1 + size_of::<u32>();
    if bytes.len() >= HEADER_SIZE {
        // Skip the request type byte.
        let size = NetworkEndian::read_u32(&bytes[1..]) as usize;
        if bytes.len() >= (HEADER_SIZE + size) {
            let value = &bytes[HEADER_SIZE..HEADER_SIZE + size];
            Ok((Value::Full(value), HEADER_SIZE + size))
        } else {
            Err(Error::Incomplete)
        }
    } else {
        Err(Error::Incomplete)
    }
}

/// Parse a `Key`.
///
/// Expects the first byte to be the request type, which is ignored.
fn parse_key<'a>(bytes: &'a [u8]) -> Result<&'a Key> {
    // 1 bytes for the request type and the bytes for the key.
    const SIZE: usize = 1 + Key::LENGTH;
    if bytes.len() >= SIZE {
        let key = Key::from_bytes(&bytes[1..SIZE]);
        Ok((key, SIZE))
    } else {
        Err(Error::Incomplete)
    }
}
