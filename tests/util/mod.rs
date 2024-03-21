//! Testing utlities.

use std::io;

use heph_rt::io::{Buf, BufMut, BufMutSlice, BufSlice, Read, Write};
use stored::io::Connection;

/// Testing connection.
pub struct TestConn<'a> {
    pub input: &'a [u8],
    pub output: Vec<u8>,
}

impl<'a> TestConn<'a> {
    pub const fn new(input: &'a [u8]) -> TestConn<'a> {
        TestConn {
            input,
            output: Vec::new(),
        }
    }
}

impl<'a> Connection for TestConn<'a> {
    async fn source(&mut self) -> io::Result<Self::Source> {
        Ok("test connection")
    }

    type Source = &'static str;
}

impl<'a> Read for TestConn<'a> {
    async fn read<B: BufMut>(&mut self, buf: B) -> io::Result<B> {
        self.input.read(buf).await
    }

    async fn read_n<B: BufMut>(&mut self, buf: B, n: usize) -> io::Result<B> {
        self.input.read_n(buf, n).await
    }

    fn is_read_vectored(&self) -> bool {
        self.input.is_read_vectored()
    }

    async fn read_vectored<B: BufMutSlice<N>, const N: usize>(&mut self, bufs: B) -> io::Result<B> {
        self.input.read_vectored(bufs).await
    }

    async fn read_n_vectored<B: BufMutSlice<N>, const N: usize>(
        &mut self,
        bufs: B,
        n: usize,
    ) -> io::Result<B> {
        self.input.read_n_vectored(bufs, n).await
    }
}

impl<'a> Write for TestConn<'a> {
    async fn write<B: Buf>(&mut self, buf: B) -> io::Result<(B, usize)> {
        self.output.write(buf).await
    }

    async fn write_all<B: Buf>(&mut self, buf: B) -> io::Result<B> {
        self.output.write_all(buf).await
    }

    fn is_write_vectored(&self) -> bool {
        self.output.is_write_vectored()
    }

    async fn write_vectored<B: BufSlice<N>, const N: usize>(
        &mut self,
        bufs: B,
    ) -> io::Result<(B, usize)> {
        self.output.write_vectored(bufs).await
    }

    async fn write_vectored_all<B: BufSlice<N>, const N: usize>(
        &mut self,
        bufs: B,
    ) -> io::Result<B> {
        self.output.write_vectored_all(bufs).await
    }
}
