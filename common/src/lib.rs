//! Coeus common code, shared between the client and server.

pub mod parse;

mod hash;

pub use hash::{Hash, InvalidHashStr};
