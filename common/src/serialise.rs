//! Module with the `write_to` types.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::marker::Unpin;
use std::task::{self, Poll};

use futures_io::AsyncWrite;

use crate::Response;

pub struct WriteResponse<'a, IO> {
    response: Response<'a>,
    io: IO,
}

impl<'a, IO> WriteResponse<'a, IO> {
    pub(super) fn new(response: Response<'a>, io: IO) -> WriteResponse<'a, IO> {
        WriteResponse {
            response,
            io,
        }
    }
}

impl<'a, IO> Future for WriteResponse<'a, IO>
    where IO: AsyncWrite + Unpin, // TODO: remove the need for `Unpin`.
{
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut task::Context) -> Poll<Self::Output> {
        // TODO: handle partial writes, etc.
        let WriteResponse { ref response, ref mut io } = &mut *self;
        let mut io = Pin::new(io);
        match response {
            Response::Ok => poll_write_all(io, ctx, &[1]),
            Response::Store(key) => {
                poll_write_all(io.as_mut(), ctx, &[2])?;
                poll_write_all(io, ctx, key.as_bytes())
            },
            Response::Value(value) => {
                poll_write_all(io.as_mut(), ctx, &[3])?;
                poll_write_all(io, ctx, value)
            },
            // FIXME: don't have the actual value here...
            Response::StreamValue { .. } => unimplemented!("TODO: streaming value"),
            Response::ValueNotFound => poll_write_all(io, ctx, &[4]),
        }
    }
}

fn poll_write_all<IO>(io: Pin<&mut IO>, ctx: &mut task::Context, buf: &[u8]) -> Poll<io::Result<()>>
    where IO: AsyncWrite,
{
    io.poll_write(ctx, buf).map(|result| match result {
        Ok(n) if n != 1 => Err(io::ErrorKind::WriteZero.into()),
        Ok(n) => Ok(()),
        Err(err) => Err(err),
    })
}
