//! Module with the `write_to` types.

use std::future::Future;
use std::io;
use std::marker::Unpin;
use std::pin::Pin;
use std::task::{self, Poll};

use byteorder::{ByteOrder, NetworkEndian};
use futures_io::{AsyncWrite, IoSlice};

use crate::Key;

/// A request to serialise.
#[derive(Debug)]
pub enum Request<'a> {
    /// Request to store value.
    Store(&'a [u8]),
    /// Retrieve a value with the given key.
    Retrieve(&'a Key),
    /// Remove a value with the given key.
    Remove(&'a Key),
}

impl<'a> Request<'a> {
    /// Write this request to an I/O object.
    ///
    /// # Notes
    ///
    /// The future doesn't flush the underlying I/O object.
    pub fn write_to<IO>(self, to: IO) -> WriteRequest<'a, IO> {
        WriteRequest {
            request: self,
            io: to,
            written: 0,
        }
    }
}

pub struct WriteRequest<'a, IO> {
    request: Request<'a>,
    io: IO,
    written: usize,
}

impl<'a, IO> Future for WriteRequest<'a, IO>
where
    IO: AsyncWrite + Unpin, // TODO: remove the need for `Unpin`.
{
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut task::Context) -> Poll<Self::Output> {
        let WriteRequest {
            ref request,
            ref mut io,
            ref mut written,
        } = &mut *self;
        let io = Pin::new(io);

        match request {
            Request::Store(value) => async_write_value(io, ctx, *written, 1, value),
            Request::Retrieve(key) => async_write_key(io, ctx, *written, 2, key),
            Request::Remove(key) => async_write_key(io, ctx, *written, 3, key),
        }.map_ok(|bytes_written| *written += bytes_written)
    }
}

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
        WriteResponse {
            response: self,
            io: to,
            written: 0,
        }
    }
}

pub struct WriteResponse<'a, IO> {
    response: Response<'a>,
    io: IO,
    written: usize,
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
            Response::Store(key) => async_write_key(io, ctx, *written, 2, key),
            Response::Value(value) => async_write_value(io, ctx, *written, 3, value),
            Response::ValueNotFound => io.poll_write(ctx, &[4]),
        }
        .map_ok(|bytes_written| *written += bytes_written)
    }
}

fn async_write_value<IO>(io: Pin<&mut IO>, ctx: &mut task::Context, written: usize, request_type: u8, value: &[u8]) -> Poll<io::Result<usize>>
    where IO: AsyncWrite,
{
    let request_type_bytes = &[request_type];
    let mut value_size_buf = [0; 4];
    NetworkEndian::write_u32(&mut value_size_buf, value.len() as u32);
    let mut bufs = [IoSlice::new(request_type_bytes), IoSlice::new(&value_size_buf), IoSlice::new(value)];
    let bufs = if written == 0 {
        // Not written anything yet.
        &bufs[..]
    } else if written == 1 {
        // Already written the request type, so we can skip that.
        &bufs[1..]
    } else {
        // Already written the request type and value size, so we can skip that.
        bufs[2] = IoSlice::new(&value[written - 5..]);
        &bufs[2..]
    };
    io.poll_write_vectored(ctx, bufs)
}

fn async_write_key<IO>(io: Pin<&mut IO>, ctx: &mut task::Context, written: usize, request_type: u8, key: &Key) -> Poll<io::Result<usize>>
    where IO: AsyncWrite,
{
    let key_bytes = key.as_bytes();
    let request_type_bytes = &[request_type];
    let mut bufs = [IoSlice::new(request_type_bytes), IoSlice::new(key_bytes)];
    let bufs = if written == 0 {
        &bufs[..]
    } else {
        // Already written something so we don't write the request type any
        // maybe only a part of the key.
        bufs[1] = IoSlice::new(&key_bytes[written - 1..]);
        &bufs[1..]
    };
    io.poll_write_vectored(ctx, bufs)
}
