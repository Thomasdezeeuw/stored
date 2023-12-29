//! I/O helper types.

use std::future::Future;
use std::{fmt, io};

use heph_rt::io::{Buf, Read, Write};
use heph_rt::net::TcpStream;

/// Connection abstraction.
pub trait Connection: Read + Write {
}

impl Connection for TcpStream {
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
    unsafe fn parts(&self) -> (*const u8, usize) {
        let (ptr, len) = self.buf.parts();
        (ptr.add(self.start), len - self.start)
    }
}
