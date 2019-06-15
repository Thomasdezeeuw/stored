//! Request future.

use std::future::Future;
use std::pin::Pin;
use std::task::{self, Poll};
use std::{io, slice};

use byteorder::{ByteOrder, NetworkEndian};
use futures_io::{AsyncRead, AsyncWrite};
use log::trace;

use coeus_common::{self as coeus, parse};

use crate::{response_to, Client, Key};

/// Request future.
pub struct Request<'c, C, D> {
    client: &'c mut Client<C>,
    data: D,
    state: State,
}

impl<'c, C, D> Request<'c, C, D> {
    fn new(client: &'c mut Client<C>, data: D) -> Request<'c, C, D> {
        Request {
            client,
            data,
            state: State::Initial,
        }
    }
}

impl<'c, 'v, C> Request<'c, C, Store<'v>> {
    pub(crate) fn store(client: &'c mut Client<C>, value: &'v [u8]) -> Request<'c, C, Store<'v>> {
        Request::new(client, Store { value })
    }
}

impl<'c, 'h, C> Request<'c, C, Retrieve<'h>> {
    pub(crate) fn retrieve(client: &'c mut Client<C>, key: &'h Key) -> Request<'c, C, Retrieve<'h>> {
        Request::new(client, Retrieve { key })
    }
}

impl<'c, 'h, C> Request<'c, C, Remove<'h>> {
    pub(crate) fn remove(client: &'c mut Client<C>, key: &'h Key) -> Request<'c, C, Remove<'h>> {
        Request::new(client, Remove { key })
    }
}

/// Store request.
pub struct Store<'a> {
    value: &'a [u8],
}

/// Retrieve request.
pub struct Retrieve<'a> {
    key: &'a Key,
}

/// Remove request.
pub struct Remove<'a> {
    key: &'a Key,
}

enum State {
    /// Initial state of the request, nothing has been done.
    Initial,
    /// Written the request type and possibly part of the request.
    Written(usize),
    /// Written the request type and request data, flushing the request.
    FlushRequest,
    /// Request has been written and flushed, preparing the buffer.
    PrepareReceive,
    /// Buffer has been prepared, now waiting on the response.
    Receiving(usize),
    /// Response is read (into `client.buf`) and is ready to be parsed.
    ParseResponse,
}

impl<'c, 'v, C> Future for Request<'c, C, Store<'v>>
where
    C: AsyncRead + AsyncWrite + Unpin, // TODO: remove Unpin bound.
{
    type Output = io::Result<response_to::Store<'c>>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut task::Context) -> Poll<Self::Output> {
        // TODO(Thomas): DRY, optimise and cleanup this code.
        match self.state {
            State::Initial => {
                trace!("writing request type byte");
                // Write the request type to the connection.
                match Pin::new(&mut self.client.connection).poll_write(ctx, &[1]) {
                    Poll::Ready(Ok(bytes_written)) => {
                        assert_eq!(bytes_written, 1, "TODO: deal with partial writes");
                        // Move to the next state.
                        self.state = State::Written(0);
                        self.poll(ctx)
                    },
                    Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                    Poll::Pending => Poll::Pending,
                }
            },
            State::Written(mut already_written) => {
                let Request { ref mut client, ref data, .. } = &mut *self;

                if already_written < 4 {
                    trace!("writing value length");
                    let mut buf = [0; 4];
                    NetworkEndian::write_u32(&mut buf, data.value.len() as u32);
                    match Pin::new(&mut client.connection).poll_write(ctx, &buf) {
                        Poll::Ready(Ok(bytes_written)) => {
                            trace!("written value length, {} bytes", bytes_written);
                            assert_eq!(bytes_written, 4, "TODO: deal with partial writes");
                            self.state = State::Written(4);
                            return self.poll(ctx);
                        },
                        Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                        Poll::Pending => return Poll::Pending,
                    }
                }

                // Write the value to the connection.
                trace!("writing value");
                match Pin::new(&mut client.connection).poll_write(ctx, &data.value[already_written - 4..]) {
                    Poll::Ready(Ok(bytes_written)) => {
                        // TODO: special case for `bytes_written` == 0?
                        let total_written = already_written + bytes_written;
                        trace!("written {} bytes, now written {} of {} bytes",
                            bytes_written, total_written, data.value.len() + 4);
                        if total_written >= data.value.len() + 4 {
                            // Written the entire request, move to the next state.
                            self.state = State::FlushRequest;
                        } else {
                            // Short write. Update our state and try again.
                            self.state = State::Written(total_written);
                        }
                        self.poll(ctx)
                    },
                    Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                    Poll::Pending => Poll::Pending,
                }
            },
            State::FlushRequest => {
                trace!("flushing request");
                // The entire request has been written, now flush it.
                match Pin::new(&mut self.client.connection).poll_flush(ctx) {
                    Poll::Ready(Ok(())) => {
                        // Move to the next state.
                        self.state = State::PrepareReceive;
                        self.poll(ctx)
                    },
                    Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                    Poll::Pending => Poll::Pending,
                }
            },
            State::PrepareReceive => {
                trace!("preparing to receive response");
                // TODO(Thomas): this can be done more efficiently, but would
                // require unsafe.
                self.client.buf.resize(1 + Key::LENGTH, 0);
                // Move to the next state.
                self.state = State::Receiving(0);
                self.poll(ctx)
            },
            State::Receiving(already_read) => {
                trace!("receiving response");
                let Request { ref mut client, .. } = &mut *self;
                let Client { ref mut connection, ref mut buf } = client;
                match Pin::new(connection).poll_read(ctx, &mut buf[already_read..]) {
                    Poll::Ready(Ok(bytes_read)) => {
                        // TODO: special case for `bytes_read` == 0, means
                        // connection is closed.
                        let total_read = already_read + bytes_read;
                        trace!("read {} bytes, now read {} of {} bytes",
                            bytes_read, total_read, 1 + Key::LENGTH);
                        if total_read >= 1 + Key::LENGTH {
                            // Read the entire request, move to the next state.
                            self.state = State::ParseResponse;
                        } else {
                            // Short read. Update our state and try again.
                            self.state = State::Receiving(total_read);
                        }
                        self.poll(ctx)
                    },
                    Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                    Poll::Pending => Poll::Pending,
                }
            },
            State::ParseResponse => {
                trace!("parsing response");
                let buf = unsafe {
                    // FIXME(Thomas): problems with lifetimes caused this. This
                    // should be safe as the output has the same lifetime ('c)
                    // as the client, which owns the buffer.
                    slice::from_raw_parts(self.client.buf.as_ptr(), self.client.buf.len())
                };
                let (response, n_bytes) = parse::response(buf).expect("TODO: deal with parse failures");
                match response {
                    parse::Response::Store(key) => {
                        assert_eq!(n_bytes, 1 + Key::LENGTH, "TODO: deal with unexpected longer parses");
                        Poll::Ready(Ok(response_to::Store::Success(key)))
                    },
                    response => unimplemented!("TODO: deal with unexpected responses: {:?}", response),
                }
            },
        }
    }
}

impl<'c, 'h, C> Future for Request<'c, C, Retrieve<'h>>
where
    C: AsyncRead + AsyncWrite,
{
    type Output = io::Result<response_to::Retrieve<'c>>;

    fn poll(self: Pin<&mut Self>, ctx: &mut task::Context) -> Poll<Self::Output> {
        // TODO:
        // 1. Write request type.
        // 2. Write request data, fully (dealing with partial writes).
        // 3. Read request.
        // 4. Parse request.
        unimplemented!();
    }
}

impl<'c, 'h, C> Future for Request<'c, C, Remove<'h>>
where
    C: AsyncRead + AsyncWrite,
{
    type Output = io::Result<response_to::Remove>;

    fn poll(self: Pin<&mut Self>, ctx: &mut task::Context) -> Poll<Self::Output> {
        // TODO:
        // 1. Write request type.
        // 2. Write request data, fully (dealing with partial writes).
        // 3. Read request.
        // 4. Parse request.
        unimplemented!();
    }
}
