//! Coeus.

use futures_io::{AsyncRead, AsyncWrite};

use coeus_common::buffer::Buffer;

pub mod request;
pub mod response_to;

#[doc(inline)]
pub use coeus_common::Key;

use request::{Store, Retrieve, Remove};

// TODO: doc requirements of `C`.
// TODO: doc no buffering is done, or should we add buffering?
pub struct Client<Conn> {
    /// Underlying connection.
    conn: Conn,
    buf: Buffer,
}

impl<Conn> Client<Conn>
where
    Conn: AsyncRead + AsyncWrite + Unpin,
{
    /// Create a new client from a connection.
    pub fn new(connection: Conn) -> Client<Conn> {
        Client {
            conn: connection,
            buf: Buffer::new(),
        }
    }

    /// Store a `value`.
    ///
    /// Returns [`response_to::Store`].
    pub fn store<'client, 'value>(&'client mut self, value: &'value [u8]) -> Store<'client, 'value, Conn> {
        Store::new(self, value)
    }

    /// Retrieve a value based on its `key`.
    ///
    /// Returns [`response_to::Retrieve`].
    pub fn retrieve<'client, 'key>(&'client mut self, key: &'key Key) -> Retrieve<'client, 'key, Conn> {
        Retrieve::new(self, key)
    }

    /// Remove a value based on its `key`.
    ///
    /// Returns [`response_to::Remove`].
    pub fn remove<'client, 'key>(&'client mut self, key: &'key Key) -> Remove<'client, 'key, Conn> {
        Remove::new(self, key)
    }
}
