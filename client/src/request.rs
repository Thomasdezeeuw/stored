//! Request future.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{self, Poll};

use futures_io::{AsyncRead, AsyncWrite};
use futures_util::try_ready;
use log::{debug, trace};

use coeus_common::parse;
use coeus_common::serialise::{async_write_key, async_write_value};

use crate::{response_to, Client, Key};

enum State<'d, Data: ?Sized> {
    /// Initial state of the request, writing the request.
    Written(&'d Data, usize),
    /// Written the request, flushing the request.
    FlushRequest,
    /// Request has been written and flushed, now we're going to receive the
    /// response.
    Receiving,
}

/// [`Future`] behind [`Client::retrieve`].
pub struct Store<'client, 'value, Conn> {
    client: &'client mut Client<Conn>,
    state: State<'value, [u8]>,
}

impl<'client, 'key, Conn> Store<'client, 'key, Conn> {
    pub(crate) fn new(client: &'client mut Client<Conn>, value: &'key [u8]) -> Store<'client, 'key, Conn> {
        Store {
            client,
            state: State::Written(&value, 0),
        }
    }
}

impl<'client, 'key, Conn> Future for Store<'client, 'key, Conn>
where
    Conn: AsyncRead + AsyncWrite + Unpin,
{
    type Output = io::Result<response_to::Store<'client>>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut task::Context) -> Poll<Self::Output> {
        let Store { client, ref mut state } = &mut *self;
        match state {
            State::Written(ref value, already_written) => {
                trace!("polling write request future");
                let conn = Pin::new(&mut client.conn);
                let written = try_ready!(async_write_value(conn, ctx, *already_written, 1, value));
                if written == 0 {
                    Poll::Ready(Err(io::ErrorKind::WriteZero.into()))
                } else if written + *already_written >= value.len() + 4 + 1 {
                    // Wrote the entire request.
                    *state = State::FlushRequest;
                    self.poll(ctx)
                } else {
                    // Didn't write the complete request yet.
                    *already_written += written;
                    Poll::Pending
                }
            },
            State::FlushRequest => {
                trace!("flushing request");
                // The entire request has been written, now flush it.
                try_ready!(Pin::new(&mut client.conn).poll_flush(ctx));
                *state = State::Receiving;
                self.poll(ctx)
            },
            State::Receiving => {
                trace!("receiving response");
                let mut future = client.buf.read_from(&mut client.conn);
                let future = Pin::new(&mut future);
                if try_ready!(future.poll(ctx)) == 0 {
                    return Poll::Ready(Err(io::ErrorKind::UnexpectedEof.into()));
                }

                let bytes: &'client [u8] = unsafe {
                    // This is not safe, but I don't quite know how the fix the
                    // lifetime troubles.
                    &*(client.buf.as_bytes() as *const [u8])
                };
                match parse::response(bytes) {
                    Ok((response, response_length)) => {
                        debug!("successfully parsed response: {:?}", response);
                        client.buf.processed(response_length);
                        Poll::Ready(Ok(response_to::Store::from_parsed(response)))
                    },
                    Err(parse::Error::Incomplete) => {
                        debug!("incomplete request");
                        Poll::Pending
                    },
                    Err(parse::Error::InvalidType) => {
                        debug!("error parsing request: invalid request type");
                        // FIXME: this isn't really correct.
                        Poll::Ready(Ok(response_to::Store::UnexpectedResponse))
                    },
                }
            },
        }
    }
}

/// [`Future`] behind [`Client::retrieve`].
pub struct Retrieve<'client, 'key, Conn> {
    client: &'client mut Client<Conn>,
    state: State<'key, Key>,
}

impl<'client, 'key, Conn> Retrieve<'client, 'key, Conn> {
    pub(crate) fn new(client: &'client mut Client<Conn>, key: &'key Key) -> Retrieve<'client, 'key, Conn> {
        Retrieve {
            client,
            state: State::Written(&key, 0),
        }
    }
}

impl<'client, 'key, Conn> Future for Retrieve<'client, 'key, Conn>
where
    Conn: AsyncRead + AsyncWrite + Unpin,
{
    type Output = io::Result<response_to::Retrieve<'client>>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut task::Context) -> Poll<Self::Output> {
        let Retrieve { client, ref mut state } = &mut *self;
        match state {
            State::Written(ref key, already_written) => {
                trace!("polling write request future");
                let conn = Pin::new(&mut client.conn);
                let written = try_ready!(async_write_key(conn, ctx, *already_written, 2, key));
                if written == 0 {
                    Poll::Ready(Err(io::ErrorKind::WriteZero.into()))
                } else if written + *already_written >= Key::LENGTH + 1 {
                    // Wrote the entire request.
                    *state = State::FlushRequest;
                    self.poll(ctx)
                } else {
                    // Didn't write the complete request yet.
                    *already_written += written;
                    Poll::Pending
                }
            },
            State::FlushRequest => {
                trace!("flushing request");
                // The entire request has been written, now flush it.
                try_ready!(Pin::new(&mut client.conn).poll_flush(ctx));
                *state = State::Receiving;
                self.poll(ctx)
            },
            State::Receiving => {
                trace!("receiving response");
                let mut future = client.buf.read_from(&mut client.conn);
                let future = Pin::new(&mut future);
                if try_ready!(future.poll(ctx)) == 0 {
                    return Poll::Ready(Err(io::ErrorKind::UnexpectedEof.into()));
                }

                let bytes: &'client [u8] = unsafe {
                    // This is not safe, but I don't quite know how the fix the
                    // lifetime troubles.
                    &*(client.buf.as_bytes() as *const [u8])
                };
                match parse::response(bytes) {
                    Ok((response, response_length)) => {
                        debug!("successfully parsed response: {:?}", response);
                        client.buf.processed(response_length);
                        Poll::Ready(Ok(response_to::Retrieve::from_parsed(response)))
                    },
                    Err(parse::Error::Incomplete) => {
                        debug!("incomplete request");
                        Poll::Pending
                    },
                    Err(parse::Error::InvalidType) => {
                        debug!("error parsing request: invalid request type");
                        // FIXME: this isn't really correct.
                        Poll::Ready(Ok(response_to::Retrieve::UnexpectedResponse))
                    },
                }
            },
        }
    }
}

/// [`Future`] behind [`Client::remove`].
pub struct Remove<'client, 'key, Conn> {
    client: &'client mut Client<Conn>,
    state: State<'key, Key>,
}

impl<'client, 'key, Conn> Remove<'client, 'key, Conn> {
    pub(crate) fn new(client: &'client mut Client<Conn>, key: &'key Key) -> Remove<'client, 'key, Conn> {
        Remove {
            client,
            state: State::Written(&key, 0),
        }
    }
}

impl<'client, 'key, Conn> Future for Remove<'client, 'key, Conn>
where
    Conn: AsyncRead + AsyncWrite + Unpin,
{
    type Output = io::Result<response_to::Remove>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut task::Context) -> Poll<Self::Output> {
        let Remove { client, ref mut state } = &mut *self;
        match state {
            State::Written(ref key, already_written) => {
                trace!("polling write request future");
                let conn = Pin::new(&mut client.conn);
                let written = try_ready!(async_write_key(conn, ctx, *already_written, 3, key));
                if written == 0 {
                    Poll::Ready(Err(io::ErrorKind::WriteZero.into()))
                } else if written + *already_written >= Key::LENGTH + 1 {
                    // Wrote the entire request.
                    *state = State::FlushRequest;
                    self.poll(ctx)
                } else {
                    // Didn't write the complete request yet.
                    *already_written += written;
                    Poll::Pending
                }
            },
            State::FlushRequest => {
                trace!("flushing request");
                // The entire request has been written, now flush it.
                try_ready!(Pin::new(&mut client.conn).poll_flush(ctx));
                *state = State::Receiving;
                self.poll(ctx)
            },
            State::Receiving => {
                trace!("receiving response");
                let mut future = client.buf.read_from(&mut client.conn);
                let future = Pin::new(&mut future);
                if try_ready!(future.poll(ctx)) == 0 {
                    return Poll::Ready(Err(io::ErrorKind::UnexpectedEof.into()));
                }

                match parse::response(client.buf.as_bytes()) {
                    Ok((response, response_length)) => {
                        debug!("successfully parsed response: {:?}", response);
                        Poll::Ready(Ok(response_to::Remove::from_parsed(response))).map(|res| {
                            client.buf.processed(response_length);
                            res
                        })
                    },
                    Err(parse::Error::Incomplete) => {
                        debug!("incomplete request");
                        Poll::Pending
                    },
                    Err(parse::Error::InvalidType) => {
                        debug!("error parsing request: invalid request type");
                        // FIXME: this isn't really correct.
                        Poll::Ready(Ok(response_to::Remove::UnexpectedResponse))
                    },
                }
            },
        }
    }
}
