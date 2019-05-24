//! Coeus common code, shared between the client and server.

pub mod parse;
pub mod request;
pub mod response;

mod hash;

pub use hash::Hash;
