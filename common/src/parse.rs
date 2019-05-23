use std::mem::size_of;

use byteorder::{ByteOrder, NetworkEndian};

use crate::{request, response, Hash};

/// Minimum size for a value to using streaming.
pub const STREAMING_SIZE_MIN: usize = 1024;

/// The result of a parsing function.
///
/// In case of sucessfull parsing it will return `T` (either a request or
/// response) and the number of bytes used for the request.
type Result<T> = std::result::Result<(T, usize), Error>;

/// Error returned by parsing.
pub enum Error {
    /// The bytes provided did not include a complete request.
    ///
    /// This not really an error.
    Incomplete,
    /// Request or response type is invalid.
    InvalidType,
}

/// A parsed response.
pub enum Response<'a> {
    Ok(response::Ok),
    Store(response::Store<'a>),
    Value(response::Value<'a>),
    /// A partially parsed value response. This will be returned if the size of
    /// the value is larger then [`STREAMING_SIZE_MIN`].
    StreamValue {
        /// Size of the value.
        value_size: usize,
    },
    ValueNotFound(response::ValueNotFound),
}

/// Parse a response.
pub fn parse_response<'a>(bytes: &'a [u8]) -> Result<Response<'a>> {
    match bytes.first() {
        Some(byte) => match byte {
            1 => Ok((Response::Ok(response::Ok), 1)),
            2 if bytes.len() >= Hash::LENGTH + 1 => {
                let hash = Hash::from_bytes(&bytes[1..Hash::LENGTH+1]);
                Ok((Response::Store(response::Store::new(hash)), Hash::LENGTH + 1))
            },
            2 => Err(Error::Incomplete),
            3 if bytes.len() >= size_of::<u32>() + 1 => {
                const HEADER_SIZE: usize = size_of::<u32>() + 1;
                let size = NetworkEndian::read_u32(&bytes) as usize;
                if size >= STREAMING_SIZE_MIN {
                    // Large values we'll stream.
                    Ok((Response::StreamValue { value_size: size }, HEADER_SIZE))
                } else if bytes.len() >= (HEADER_SIZE + size) {
                    // Small values we parse in one go.
                    let value = &bytes[HEADER_SIZE..HEADER_SIZE + size];
                    Ok((Response::Value(response::Value::new(value)), HEADER_SIZE + size))
                } else {
                    Err(Error::Incomplete)
                }
            },
            3 => Err(Error::Incomplete),
            4 => Ok((Response::ValueNotFound(response::ValueNotFound), 1)),
            _ => Err(Error::InvalidType),
        },
        None => Err(Error::Incomplete),
    }
}
