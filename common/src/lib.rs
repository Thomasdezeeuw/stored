//! Coeus common code, shared between the client and server.

pub mod key;
pub mod parse;
pub mod serialise;

use serialise::WriteResponse;

impl<'a> Response<'a> {
    /// Write this response to an I/O object.
    pub fn write_to<IO>(self, to: IO) -> WriteResponse<'a, IO> {
        WriteResponse::new(self, to)
    }
}

pub use key::Key;
