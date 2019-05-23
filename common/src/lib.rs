//! Coeus common code, shared between the client and server.

use std::fmt;

use ring::digest::SHA512_OUTPUT_LEN;

pub mod parse;
pub mod request;
pub mod response;

/// Type that represents a hashed value, used as key.
#[repr(transparent)]
pub struct Hash {
    bytes: [u8; Hash::LENGTH],
}

impl Hash {
    /// Length of the hash in bytes.
    pub const LENGTH: usize = SHA512_OUTPUT_LEN;

    /// Create a new `Hash`.
    pub fn new(hash: [u8; Hash::LENGTH]) -> Hash {
        Hash {
            bytes: hash,
        }
    }

    /// Converts a slice of bytes of length `Hash::LENGTH` into `&Hash`.
    ///
    /// # Panics
    ///
    /// This will panic if `hash` is not of length `Hash::LENGTH`.
    pub fn from_bytes<'a>(hash: &'a [u8]) -> &'a Hash {
        assert_eq!(hash.len(), Hash::LENGTH);
        unsafe {
            // This is safe because we ensured above that `hash` is of length
            // `Hash::LENGTH` and because `Hash` has the same layout as `[u8;
            // Hash::LENGTH]` because we use the `repr(transparent)` attribute.
            &*(hash.as_ptr() as *const Hash)
        }
    }
}

impl Eq for Hash { }

impl PartialEq for Hash {
    fn eq(&self, other: &Hash) -> bool {
        self.bytes[..] == other.bytes[..]
    }

    fn ne(&self, other: &Hash) -> bool {
        self.bytes[..] != other.bytes[..]
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let h = self.bytes;
        write!(f,
            "{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7], h[8], h[9], h[10], h[11], h[12], h[13], h[14], h[15], h[16], h[17], h[18], h[19], h[20], h[21], h[22], h[23], h[24], h[25], h[26], h[27], h[28], h[29], h[30], h[31], h[32], h[33], h[34], h[35], h[36], h[37], h[38], h[39], h[40], h[41], h[42], h[43], h[44], h[45], h[46], h[47], h[48], h[49], h[50], h[51], h[52], h[53], h[54], h[55], h[56], h[57], h[58], h[59], h[60], h[61], h[62], h[63])
    }
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
