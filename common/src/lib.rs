//! Coeus common code, shared between the client and server.

pub mod parse;

mod key;

pub use key::{InvalidKeyStr, Key, KeyCalculator};
