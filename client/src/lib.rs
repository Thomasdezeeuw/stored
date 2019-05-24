//! Coeus.

use futures_io::{AsyncRead, AsyncWrite};

pub mod response_to;
pub mod request;

#[doc(inline)]
pub use coeus_common::Hash;

pub use request::Request;

// TODO: doc requirements of `C`.
// TODO: doc no buffering is done, or should we add buffering?
pub struct Client<C> {
    /// Underlying connection.
    connection: C,
}

impl<C> Client<C>
    where C: AsyncRead + AsyncWrite,
{
    /// Create a new client from a connection.
    pub fn new(connection: C) -> Client<C> {
        Client {
            connection,
        }
    }

    /// Store a `value`.
    ///
    /// Returns [`response_to::Store`].
    pub fn store<'c, 'v>(&'c mut self, value: &'v [u8]) -> Request<'c, C, request::Store<'v>> {
        Request {
            client: self,
            state: request::State::Initial,
            request_type: request::Store {
                value,
            },
        }
    }

    /// Retrieve a value based on its `hash`.
    ///
    /// Returns [`response_to::Retrieve`].
    pub fn retrieve<'c, 'h>(&'c mut self, hash: &'h Hash) -> Request<'c, C, request::Retrieve<'h>> {
        Request {
            client: self,
            state: request::State::Initial,
            request_type: request::Retrieve {
                hash,
            },
        }
    }

    /// Remove a value based on its `hash`.
    ///
    /// Returns [`response_to::Remove`].
    pub fn remove<'c, 'h>(&'c mut self, hash: &'h Hash) -> Request<'c, C, request::Remove<'h>> {
        Request {
            client: self,
            state: request::State::Initial,
            request_type: request::Remove {
                hash,
            },
        }
    }
}
