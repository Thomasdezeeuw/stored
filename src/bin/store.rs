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

use std::env;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};

use stored::cli::{first_arg, Error};

fn main() -> Result<(), Error> {
    // TODO: make the address configurable.
    let address: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let mut stream = TcpStream::connect(address)?;

    let blob = if let Some(blob) = first_arg() {
        blob.into_bytes()
    } else {
        let mut buf = Vec::new();
        io::stdin().read_to_end(&mut buf)?;
        buf
    };

    let mut headers = Vec::new();
    write!(
        &mut headers,
        "POST /blob HTTP/1.1\r\nUser-Agent: Stored-store/{}\r\nContent-Length: {}\r\n\r\n",
        env!("CARGO_PKG_VERSION"),
        blob.len()
    )?;

    /* TODO: use vectored I/O:
    let bufs = &[IoSlice::new(&headers), IoSlice::new(&blob)];
    stream.write_all_vectored(bufs)?;
    */
    stream.write_all(&headers)?;
    stream.write_all(&blob)?;
    stream.shutdown(Shutdown::Write)?;

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
        Err(Error::LocationHeader)
    }
}
