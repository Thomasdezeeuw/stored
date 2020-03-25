//! Store a blob.
//!
//! Usage:
//!
//! ```bash
//! $ store "Hello world"
//!
//! $ echo "Hello world" | store
//! ```

#![feature(bool_to_option)]

use std::io::{self, IoSlice, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::os::unix::ffi::OsStringExt;
use std::{env, fmt};

fn main() -> Result<(), Error> {
    // TODO: make the address configurable.
    let address: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let mut stream = TcpStream::connect(address)?;

    // Read the blob we need to store. Uses the first argument passed (if not
    // "-") or reads from standard in.
    let blob = env::args_os()
        .skip(1)
        .next()
        .and_then(|arg| (arg != "-").then_some(arg));
    let blob = if let Some(blob) = blob {
        blob.into_vec()
    } else {
        let mut buf = Vec::new();
        io::stdin().read_to_end(&mut buf)?;
        buf
    };

    let mut headers = Vec::new();
    write!(
        &mut headers,
        "POST /blob HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
        blob.len()
    )?;

    let bufs = &[IoSlice::new(&headers), IoSlice::new(&blob)];
    stream.write_vectored(bufs)?;
    stream.flush()?;

    // Reuse the largest buffer.
    let mut buf = if headers.len() > blob.len() {
        headers
    } else {
        blob
    };
    buf.clear();

    // Read the response.
    stream.read_to_end(&mut buf)?;

    let mut headers = [httparse::EMPTY_HEADER; 5];
    let mut resp = httparse::Response::new(&mut headers);
    let headers_length = resp.parse(&buf)?.unwrap();

    if resp.code != Some(201) {
        let msg = String::from_utf8_lossy(&buf[headers_length..]).into_owned();
        return Err(Error::StatusCode(resp.code.unwrap(), msg));
    }

    let location = resp.headers.iter().find(|header| header.name == "Location");
    if let Some(location) = location {
        // Remove "/blob/".
        io::stdout()
            .write_all(&location.value[6..])
            .map_err(Into::into)
    } else {
        return Err(Error::LocationHeader);
    }
}

enum Error {
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
