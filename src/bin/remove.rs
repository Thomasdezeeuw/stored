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

use std::env;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};

use stored::cli::{first_arg, Error};

fn main() -> Result<(), Error> {
    // TODO: make the address configurable.
    let address: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let mut stream = TcpStream::connect(address)?;

    let key = if let Some(key) = first_arg() {
        key
    } else {
        let mut buf = String::new();
        io::stdin().read_to_string(&mut buf)?;
        buf
    };

    let mut request = Vec::new();
    write!(
        &mut request,
        "DELETE /blob/{} HTTP/1.1\r\nUser-Agent: Stored-store/{}\r\n\r\n",
        key,
        env!("CARGO_PKG_VERSION"),
    )?;
    stream.write_all(&request)?;
    stream.shutdown(Shutdown::Write)?;

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
        Err(Error::StatusCode(resp.code.unwrap(), msg))
    } else {
        Ok(())
    }
}
