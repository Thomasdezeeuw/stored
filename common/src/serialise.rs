//! Module with the `write_to` types.

use std::future::Future;
use std::io;
use std::marker::Unpin;
use std::pin::Pin;
use std::task::{self, Poll};

use futures_io::{AsyncWrite, IoSlice};

use crate::Key;

/// A response to serialise.
#[derive(Debug)]
pub enum Response<'a> {
    /// Generic OK response.
    Ok,
    /// Value is successfully stored.
    Store(&'a Key),
    /// A retrieved value.
    Value(&'a [u8]),
    /// Value is not found.
    ValueNotFound,
}

impl<'a> Response<'a> {
    /// Write this response to an I/O object.
    ///
    /// # Notes
    ///
    /// The future doesn't flush the underlying I/O object.
    pub fn write_to<IO>(self, to: IO) -> WriteResponse<'a, IO> {
        WriteResponse::new(self, to)
    }
}

pub struct WriteResponse<'a, IO> {
    response: Response<'a>,
    io: IO,
    written: usize,
}

impl<'a, IO> WriteResponse<'a, IO> {
    fn new(response: Response<'a>, io: IO) -> WriteResponse<'a, IO> {
        WriteResponse {
            response,
            io,
            written: 0,
        }
    }
}

impl<'a, IO> Future for WriteResponse<'a, IO>
where
    IO: AsyncWrite + Unpin, // TODO: remove the need for `Unpin`.
{
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut task::Context) -> Poll<Self::Output> {
        let WriteResponse {
            ref response,
            ref mut io,
            ref mut written,
        } = &mut *self;
        let io = Pin::new(io);

        match response {
            Response::Ok => io.poll_write(ctx, &[1]),
            Response::Store(key) => {
                let key_bytes = key.as_bytes();
                let mut bufs = [IoSlice::new(&[2]), IoSlice::new(key_bytes)];
                let bufs = if *written == 0 {
                    &bufs[..]
                } else {
                    bufs[1] = IoSlice::new(&key_bytes[*written - 1..]);
                    &bufs[1..]
                };
                io.poll_write_vectored(ctx, bufs)
            },
            Response::Value(value) => {
                let mut bufs = [IoSlice::new(&[3]), IoSlice::new(value)];
                let bufs = if *written == 0 {
                    &bufs[..]
                } else {
                    bufs[1] = IoSlice::new(&value[*written - 1..]);
                    &bufs[1..]
                };
                io.poll_write_vectored(ctx, bufs)
            },
            Response::ValueNotFound => io.poll_write(ctx, &[4]),
        }
        .map_ok(|bytes_written| *written += bytes_written)
    }
}
