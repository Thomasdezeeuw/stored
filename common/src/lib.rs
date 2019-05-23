//! Coeus common code, shared between the client and server.

use ring::digest::SHA512_OUTPUT_LEN;

pub mod request;
pub mod response;

/// Length of the hash in bytes.
pub const HASH_LENGTH: usize = SHA512_OUTPUT_LEN;

/// Type that represents a hashed value, used as key.
pub type Hash = [u8; HASH_LENGTH];
