//! Coeus common code, shared between the client and server.

use ring::digest::SHA512_OUTPUT_LEN;

pub mod parse;
pub mod request;
pub mod response;

/// Length of the hash in bytes.
pub const HASH_LENGTH: usize = SHA512_OUTPUT_LEN;

/// Type that represents a hashed value, used as key.
pub type Hash = [u8; HASH_LENGTH];

/// Converts a slice of bytes of length `HASH_LENGTH` into a `Hash`.
///
/// # Panics
///
/// This will panic if `hash` is not of length `HASH_LENGTH`.
fn hash_from_bytes<'a>(hash: &'a [u8]) -> &'a Hash {
    assert_eq!(hash.len(), HASH_LENGTH);
    unsafe {
        // This is safe because we ensured above that `hash` is of
        // length `HASH_LENGTH`.
        &*(hash.as_ptr() as *const [_; HASH_LENGTH])
    }
}
