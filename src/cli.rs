//! Collection of functions and types used by the CLI programs.

use std::{env, fmt, io};

/// Error returned from `main`.
pub enum Error {
    Io(io::Error),
    Parse(httparse::Error),
    StatusCode(u16, String),
    LocationHeader,
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<httparse::Error> for Error {
    fn from(err: httparse::Error) -> Error {
        Error::Parse(err)
    }
}

/// Implemented as `fmt::Display` as this is returned by `main`.
impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Error::*;
        match self {
            Io(err) => write!(f, "I/O error: {}", err),
            Parse(err) => write!(f, "HTTP parsing error: {}", err),
            StatusCode(code, msg) => write!(f, "Unexpected HTTP status code: {}: {}", code, msg),
            LocationHeader => write!(f, "Missing 'Location' header"),
        }
    }
}

/// Returns the first argument, if any and  if not "-".
pub fn first_arg() -> Option<String> {
    env::args()
        .nth(1)
        .and_then(|arg| (arg != "-").then_some(arg))
}

/// The number of response headers to expect.
pub const N_RESPONSE_HEADERS: usize = 6;
