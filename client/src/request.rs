//! Request future.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{self, Poll};

use futures_io::{AsyncRead, AsyncWrite};

use crate::{response_to, Client, Hash};

// TODO: cleanup all the public access, it's only needed because we create the
// future in `src/lib.rs`.

/// Request future.
pub struct Request<'c, C, T> {
    pub(crate) client: &'c mut Client<C>,
    pub(crate) state: State,
    pub(crate) request_type: T,
}

/// Store request.
pub struct Store<'a> {
    pub(crate) value: &'a [u8],
}

/// Retrieve request.
pub struct Retrieve<'a> {
    pub(crate) hash: &'a Hash,
}

/// Remove request.
pub struct Remove<'a> {
    pub(crate) hash: &'a Hash,
}

pub(crate) enum State {
    /// Initial state of the request, nothing has been done.
    Initial,
    /// Written part of the request.
    Written(usize),
}

impl<'c, 'v, C> Future for Request<'c, C, Store<'v>>
    where C: AsyncRead + AsyncWrite,
{
    type Output = io::Result<response_to::Store<'c>>;

    fn poll(self: Pin<&mut Self>, ctx: &mut task::Context) -> Poll<Self::Output> {
        // TODO:
        // 1. Write request type.
        // 2. Write request data, fully (dealing with partial writes).
        // 3. Read request.
        // 4. Parse request.
        unimplemented!();
    }
}
