//! Testing utlities.

use std::io;
use std::sync::mpsc::{channel, Receiver, Sender};

use heph_rt::io::{Buf, BufMut, BufMutSlice, BufSlice, Read, Write};
use stored::io::Connection;

pub type Bytes = Box<[u8]>;

/// Testing connection.
pub struct TestConn {
    pub input: Receiver<Bytes>,
    pub buf: Vec<u8>,
    pub output: Sender<Bytes>,
}

impl TestConn {
    pub fn new() -> (TestConn, TestConnReceiver) {
        let (input_sender, input_receiver) = channel();
        let (output_sender, output_receiver) = channel();
        let conn = TestConn {
            input: input_receiver,
            buf: Vec::new(),
            output: output_sender,
        };
        let recv = TestConnReceiver {
            input: input_sender,
            output: output_receiver,
        };
        (conn, recv)
    }

    pub fn with_input(input: &[u8]) -> TestConn {
        let (conn, recv) = TestConn::new();
        recv.send_bytes(input);
        conn
    }
}

impl Connection for TestConn {
    async fn source(&mut self) -> io::Result<Self::Source> {
        Ok("test connection")
    }

    type Source = &'static str;
}

impl Read for TestConn {
    async fn read<B: BufMut>(&mut self, buf: B) -> io::Result<B> {
        if !self.buf.is_empty() {
            return self.buf.read(buf).await;
        }
        match self.input.recv() {
            Ok(b) => {
                self.buf = b.into();
                return self.buf.read(buf).await;
            }
            Err(_) => return Ok(buf),
        }
    }

    async fn read_n<B: BufMut>(&mut self, _: B, _: usize) -> io::Result<B> {
        todo!("TestConn::read_n")
    }

    fn is_read_vectored(&self) -> bool {
        false
    }

    async fn read_vectored<B: BufMutSlice<N>, const N: usize>(&mut self, _: B) -> io::Result<B> {
        todo!("TestConn::read_vectored")
    }

    async fn read_n_vectored<B: BufMutSlice<N>, const N: usize>(
        &mut self,
        _: B,
        _: usize,
    ) -> io::Result<B> {
        todo!("TestConn::read_n_vectored")
    }
}

impl Write for TestConn {
    async fn write<B: Buf>(&mut self, buf: B) -> io::Result<(B, usize)> {
        let written = match self.output.send(buf.as_slice().into()) {
            Ok(()) => buf.len(),
            Err(_) => 0,
        };
        Ok((buf, written))
    }

    async fn write_all<B: Buf>(&mut self, _: B) -> io::Result<B> {
        todo!("TestConn::write_all");
    }

    fn is_write_vectored(&self) -> bool {
        false
    }

    async fn write_vectored<B: BufSlice<N>, const N: usize>(
        &mut self,
        _: B,
    ) -> io::Result<(B, usize)> {
        todo!("TestConn::write_vectored");
    }

    async fn write_vectored_all<B: BufSlice<N>, const N: usize>(
        &mut self,
        bufs: B,
    ) -> io::Result<B> {
        todo!("TestConn::write_vectored_all");
    }
}

/// Receiving side of the [`TestConn`].
pub struct TestConnReceiver {
    pub input: Sender<Bytes>,
    pub output: Receiver<Bytes>,
}

impl TestConnReceiver {
    /// Make more bytes available for reading.
    pub fn send_bytes<B: Into<Bytes>>(&self, bytes: B) {
        self.input.send(bytes.into()).expect("failed to send data");
    }

    /// Bytes read.
    pub fn received(&self) -> Bytes {
        self.output.try_recv().expect("no more data send")
    }
}
