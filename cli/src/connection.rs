use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::pin::Pin;
use std::task::{self, Poll};

use futures_io::{AsyncRead, AsyncWrite};

pub struct Connection {
    socket: TcpStream,
}

impl Connection {
    pub fn connect(address: SocketAddr) -> io::Result<Connection> {
        TcpStream::connect(address)
            .map(|socket| Connection { socket })
    }
}

// This is not pretty, but it works.

impl AsyncRead for Connection {
    fn poll_read(mut self: Pin<&mut Self>, _ctx: &mut task::Context, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        Poll::Ready(self.socket.read(buf))
    }
}

impl AsyncWrite for Connection {
    fn poll_write(mut self: Pin<&mut Self>, _ctx: &mut task::Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        Poll::Ready(self.socket.write(buf))
    }

    fn poll_flush(mut self: Pin<&mut Self>, _ctx: &mut task::Context) -> Poll<io::Result<()>> {
        Poll::Ready(self.socket.flush())
    }

    fn poll_close(self: Pin<&mut Self>, ctx: &mut task::Context) -> Poll<io::Result<()>> {
        self.poll_flush(ctx)
    }
}
