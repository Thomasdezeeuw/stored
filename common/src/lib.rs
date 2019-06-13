//! Coeus common code, shared between the client and server.

pub mod parse;
pub mod serialise;

mod key;

pub use key::{InvalidKeyStr, Key, KeyCalculator};

use serialise::WriteResponse;

impl<'a> Response<'a> {
    /// Write this response to an I/O object.
    pub fn write_to<IO>(self, to: IO) -> WriteResponse<'a, IO> {
        WriteResponse::new(self, to)
    }
}
