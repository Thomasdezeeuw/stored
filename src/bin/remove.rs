//! Retrieve a blob.
//!
//! Usage:
//!
//!
//! ```bash
//! $ retrieve "$key"
//!
//! $ echo "$key" | retrieve
//! ```

#![feature(bool_to_option)]

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::{env, fmt};

fn main() -> Result<(), Error> {
    // TODO: make the address configurable.
    let address: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let mut stream = TcpStream::connect(address)?;

    // Read the blob we need to store. Uses the first argument passed (if not
    // "-") or reads from standard in.
    let key = env::args()
        .skip(1)
        .next()
        .and_then(|arg| (arg != "-").then_some(arg));
    let key = if let Some(key) = key {
        key
    } else {
        let mut buf = String::new();
        io::stdin().read_to_string(&mut buf)?;
        buf
    };

    let mut request = Vec::new();
    write!(&mut request, "DELETE /blob/{} HTTP/1.1\r\n\r\n", key)?;
    stream.write_all(&request)?;
    stream.flush()?;

    // Reuse the buffer.
    let mut buf = request;
    buf.clear();

    // Read the response.
    stream.read_to_end(&mut buf)?;

    let mut headers = [httparse::EMPTY_HEADER; 5];
    let mut resp = httparse::Response::new(&mut headers);
    let headers_length = resp.parse(&buf)?.unwrap();

    if resp.code != Some(204) {
        let msg = String::from_utf8_lossy(&buf[headers_length..]).into_owned();
        return Err(Error::StatusCode(resp.code.unwrap(), msg));
    } else {
        Ok(())
    }
}

enum Error {
    Io(io::Error),
    Parse(httparse::Error),
    StatusCode(u16, String),
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

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Error::*;
        match self {
            Io(err) => write!(f, "I/O error: {}", err),
            Parse(err) => write!(f, "HTTP parsing error: {}", err),
            StatusCode(code, msg) => write!(f, "Unexpected HTTP status code: {}: {}", code, msg),
        }
    }
}
