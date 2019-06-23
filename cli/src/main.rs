#![allow(dead_code, unused_variables)]

use std::fmt;
use std::future::Future;
use std::io::{self, Write};
use std::pin::Pin;
use std::task::{self, Poll};

use futures_util::task::noop_waker;
use log::trace;

use coeus::Client;

mod connection;
mod parse;
mod print;

use connection::Connection;

const fn wrap_error(err: io::Error, msg: &'static str) -> Error {
    Error { msg, err }
}

struct Error {
    msg: &'static str,
    err: io::Error,
}

/// Used to print the actual error.
impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.msg, self.err)
    }
}

fn main() -> Result<(), Error> {
    std_logger::init();

    let address = "127.0.0.1:8080".parse().unwrap();
    let conn = Connection::connect(address).map_err(|err| wrap_error(err, "error connecting to server"))?;
    let mut client = Client::new(conn);

    let mut line = String::new();
    loop {
        line.clear();
        io::stdout()
            .write_all(b"> ")
            .and_then(|()| io::stdout().flush())
            .map_err(|err| wrap_error(err, "error writing prompt"))?;

        io::stdin()
            .read_line(&mut line)
            .map_err(|err| wrap_error(err, "error reading input line"))?;
        let request = match parse::request(&line) {
            Ok(request) => request,
            Err(err) => {
                eprintln!("failed to parse request: {}", err);
                continue;
            },
        };
        trace!("parsed request: {:?}", request);
        match request {
            parse::Request::Store(value) => {
                let mut future = client.store(value);
                let future = Pin::new(&mut future);
                let response = poll_wait(future).map_err(|err| wrap_error(err, "error storing value"))?;
                trace!("got response: {:?}", response);
                print::store(response).map_err(|err| wrap_error(err, "error printing response"))?;
            },
            parse::Request::Retrieve(key) => {
                let mut future = client.retrieve(&key);
                let future = Pin::new(&mut future);
                let response = poll_wait(future).map_err(|err| wrap_error(err, "error retrieving value"))?;
                trace!("got response: {:?}", response);
                print::retrieve(response).map_err(|err| wrap_error(err, "error printing response"))?;
            },
            parse::Request::Remove(key) => {
                let mut future = client.remove(&key);
                let future = Pin::new(&mut future);
                let response = poll_wait(future).map_err(|err| wrap_error(err, "error removing value"))?;
                trace!("got response: {:?}", response);
                print::remove(response).map_err(|err| wrap_error(err, "error printing response"))?;
            },
            parse::Request::Quit => return Ok(()),
        }
    }
}

fn poll_wait<Fut>(mut future: Pin<&mut Fut>) -> Fut::Output
where
    Fut: Future,
{
    // This is not great.
    let waker = noop_waker();
    let mut ctx = task::Context::from_waker(&waker);
    loop {
        match future.as_mut().poll(&mut ctx) {
            Poll::Ready(result) => return result,
            Poll::Pending => continue,
        }
    }
}
