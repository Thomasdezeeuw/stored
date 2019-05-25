// TODO: start a coeus server.
// TODO: use an AsyncRead/AsyncWrite client.

use std::io::{self, Read, Write};
use std::future::Future;
use std::pin::Pin;
use std::marker::Unpin;
use std::net::{SocketAddr, TcpStream};
use std::thread::sleep;
use std::time::Duration;
use std::task::{self, Poll};

use futures_test::task::noop_context;
use futures_io::{AsyncRead, AsyncWrite};

use coeus::response_to;

#[test]
#[ignore = "This doesn't work yet."]
fn simple() {
    let value = b"Hello world";
    let key = "b7f783baed8297f0db917462184ff4f08e69c2d5e\
        5f79a942600f9725f58ce1f29c18139bf80b06c0f\
        ff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47".parse().unwrap();

    let address = "127.0.0.1:8080".parse().unwrap();
    let connection = Connection::connect(address)
        .expect("unable to connect server");
    let mut client = coeus::Client::new(connection);

    // Storing a value.
    let response = wait_loop(client.store(value.as_ref()))
        .expect("unexpected error with store request");
    assert_eq!(response, response_to::Store::Success(&key));

    // Retrieving the store value.
    let response = wait_loop(client.retrieve(&key))
        .expect("unexpected error with retrieve request");
    assert_eq!(response, response_to::Retrieve::Value(value.as_ref()));

    // And removing the value.
    let response = wait_loop(client.remove(&key))
        .expect("unexpected error with remove request");
    assert_eq!(response, response_to::Remove::Ok);
}

/// A simple wait loop that completes the future.
fn wait_loop<Fut>(mut future: Fut) -> Fut::Output
    where Fut: Future + Unpin,
{
    let mut future = Pin::new(&mut future);
    let mut ctx = noop_context();

    loop {
        if let Poll::Ready(val) = future.as_mut().poll(&mut ctx) {
            return val;
        }
        sleep(Duration::from_millis(10));
    }
}

/// A blocking connection implementation that implements `AsyncRead` and
/// `AsyncWrite` (incorrectly).
// TODO: replace with `Heph` connection.
#[derive(Debug)]
struct Connection {
    stream: TcpStream,
}

impl Connection {
    fn connect(address: SocketAddr) -> io::Result<Connection> {
        Ok(Connection {
            stream: TcpStream::connect(address)?,
        })
    }
}

impl AsyncRead for Connection {
    fn poll_read(mut self: Pin<&mut Self>, _ctx: &mut task::Context, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        Poll::Ready(self.stream.read(buf))
    }
}

impl AsyncWrite for Connection {
    fn poll_write(mut self: Pin<&mut Self>, _ctx: &mut task::Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        Poll::Ready(self.stream.write(buf))
    }

    fn poll_flush(mut self: Pin<&mut Self>, _ctx: &mut task::Context) -> Poll<io::Result<()>> {
        Poll::Ready(self.stream.flush())
    }

    fn poll_close(self: Pin<&mut Self>, ctx: &mut task::Context) -> Poll<io::Result<()>> {
        self.poll_flush(ctx)
    }
}
