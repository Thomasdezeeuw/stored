//! I/O helper types.

use std::future::Future;
use std::{fmt, io};

use heph_rt::extract::Extract;
use heph_rt::fd::AsyncFd;
use heph_rt::io::{Buf, BufMut, BufSlice};

/// Helper macro to execute a system call that returns an `io::Result`.
macro_rules! syscall {
    ($fn: ident ( $($arg: expr),* $(,)? ) ) => {{
        let res = unsafe { libc::$fn($( $arg, )*) };
        if res == -1 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(res)
        }
    }};
}

pub(crate) use syscall;

/// Connection abstraction.
pub trait Connection {
    /// Return the source of the client.
    ///
    /// # Errors
    ///
    /// The error is considered fatal.
    fn source(&mut self) -> impl Future<Output = io::Result<Self::Source>>;

    /// Source of the client.
    ///
    /// For example for TCP connections this will be the IP address.
    type Source: fmt::Display;

    /// Receive bytes, writing them into `buf`.
    ///
    /// # Notes
    ///
    /// The caller must always check if at least one byte was received as
    /// reading zero bytes is an indication that no more bytes will come (we've
    /// hit the end of the file, `EOF`). Failing to do so can result in an
    /// infinite loop.
    fn recv<B: BufMut>(&mut self, buf: B) -> impl Future<Output = io::Result<B>>;

    /// Send all bytes in `buf`.
    ///
    /// If this fails to write all bytes (this happens if a send returns
    /// `Ok(0)`) this will return [`io::ErrorKind::WriteZero`].
    ///
    /// [`io::ErrorKind::WriteZero`]: std::io::ErrorKind::WriteZero
    fn send_all<B: Buf>(&mut self, buf: B) -> impl Future<Output = io::Result<B>>;

    /// Send all bytes in `bufs`.
    ///
    /// If this fails to send all bytes (this happens if a send returns `Ok(0)`)
    /// this will return [`io::ErrorKind::WriteZero`].
    ///
    /// [`io::ErrorKind::WriteZero`]: std::io::ErrorKind::WriteZero
    fn send_all_vectored<B: BufSlice<N>, const N: usize>(
        &mut self,
        bufs: B,
    ) -> impl Future<Output = io::Result<B>>;
}

impl Connection for AsyncFd {
    async fn source(&mut self) -> io::Result<Self::Source> {
        self.peer_addr().await
    }

    type Source = std::net::SocketAddr;

    fn recv<B: BufMut>(&mut self, buf: B) -> impl Future<Output = io::Result<B>> {
        AsyncFd::recv(&*self, buf)
    }

    fn send_all<B: Buf>(&mut self, buf: B) -> impl Future<Output = io::Result<B>> {
        AsyncFd::send_all(&*self, buf).extract()
    }

    fn send_all_vectored<B: BufSlice<N>, const N: usize>(
        &mut self,
        bufs: B,
    ) -> impl Future<Output = io::Result<B>> {
        AsyncFd::send_all_vectored(&*self, bufs).extract()
    }
}

/// Helper type to reuse read buffer.
pub(crate) struct WriteBuf {
    buf: Vec<u8>,
    start: usize,
}

impl WriteBuf {
    /// Create a new `WriteBuf`.
    pub(crate) fn new(buf: Vec<u8>, start: usize) -> WriteBuf {
        debug_assert!(buf.len() >= start);
        WriteBuf { buf, start }
    }

    /// Reset the buffer to remove all written bytes, i.e. restoring the read
    /// buffer.
    pub(crate) fn reset(mut self) -> Vec<u8> {
        self.buf.truncate(self.start);
        self.buf
    }
}

// SAFETY: `Vec<u8>` manages the allocation of the bytes, so as long as it's
// alive, so is the slice of bytes.
unsafe impl Buf for WriteBuf {
    unsafe fn parts(&self) -> (*const u8, u32) {
        let (ptr, len) = unsafe { self.buf.parts() };
        (unsafe { ptr.add(self.start) }, len - self.start as u32)
    }
}
