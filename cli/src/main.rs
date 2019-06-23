#![allow(dead_code, unused_variables)]

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

fn main() -> io::Result<()> {
    std_logger::init();

    let address = "127.0.0.1:8080".parse().unwrap();
    let conn = Connection::connect(address)?;
    let mut client = Client::new(conn);

    let mut line = String::new();
    loop {
        line.clear();
        io::stdout().write_all(b"> ")?;
        io::stdout().flush()?;

        io::stdin().read_line(&mut line)?;
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
                let response = poll_wait(future)?;
                trace!("got response: {:?}", response);
                print::store(response)?;
            },
            parse::Request::Retrieve(key) => {
                let mut future = client.retrieve(&key);
                let future = Pin::new(&mut future);
                let response = poll_wait(future)?;
                trace!("got response: {:?}", response);
                print::retrieve(response)?;
            },
            parse::Request::Remove(key) => {
                let mut future = client.remove(&key);
                let future = Pin::new(&mut future);
                let response = poll_wait(future)?;
                trace!("got response: {:?}", response);
                print::remove(response)?;
            },
            parse::Request::Quit => return Ok(()),
        }
    }
}

fn poll_wait<Fut>(mut future: Pin<&mut Fut>) -> Fut::Output
    where Fut: Future,
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
