//! Module with the `Key` type.

use std::error::Error;
use std::fmt;
use std::str::FromStr;

use ring::digest::{digest, SHA512, SHA512_OUTPUT_LEN};

/// The key of a value.
///
/// This is always the SHA-512 checksum of the value, which can be calculated
/// using the [`Key::for_value`] method.
#[repr(transparent)]
pub struct Key {
    bytes: [u8; Key::LENGTH],
}

impl Key {
    /// Length of the key in bytes.
    pub const LENGTH: usize = SHA512_OUTPUT_LEN;

    /// Create a new `Key` from the provided `bytes`..
    pub const fn new(bytes: [u8; Key::LENGTH]) -> Key {
        Key { bytes }
    }

    /// Convert a slice of bytes of length `Key::LENGTH` into `&Key`.
    ///
    /// # Panics
    ///
    /// This will panic if `bytes` is not of length `Key::LENGTH`.
    pub fn from_bytes<'a>(bytes: &'a [u8]) -> &'a Key {
        assert_eq!(bytes.len(), Key::LENGTH);
        unsafe {
            // This is safe because we ensured above that `bytes` is of length
            // `Key::LENGTH` and because `Key` has the same layout as `[u8;
            // Key::LENGTH]` because we use the `repr(transparent)` attribute.
            &*(bytes.as_ptr() as *const Key)
        }
    }

    /// Calculate the `Key` for the provided `value`.
    pub fn for_value<'a>(value: &'a [u8]) -> Key {
        let result = digest(&SHA512, value);
        Key::from_bytes(result.as_ref()).to_owned()
    }
}

/// Error returned by [`Key`]'s [`FromStr`] implementation.
#[derive(Debug, Eq, PartialEq)]
pub struct InvalidKeyStr;

impl fmt::Display for InvalidKeyStr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.pad(self.description())
    }
}

impl Error for InvalidKeyStr {
    fn description(&self) -> &str {
        "invalid SHA-512 checksum string"
    }
}

impl FromStr for Key {
    type Err = InvalidKeyStr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != Key::LENGTH * 2 {
            return Err(InvalidKeyStr);
        }

        let mut bytes = [0; Key::LENGTH];
        for (i, digits) in s.as_bytes().chunks_exact(2).enumerate() {
            let high = from_hex_digit(digits[0])?;
            let low = from_hex_digit(digits[1])?;
            bytes[i] = (high * 16) | low;
        }
        Ok(Key::new(bytes))
    }
}

fn from_hex_digit(digit: u8) -> Result<u8, InvalidKeyStr> {
    if (b'0'..=b'9').contains(&digit) {
        Ok(digit - b'0')
    } else if (b'a'..=b'f').contains(&digit) {
        Ok(digit - b'a' + 10)
    } else if (b'A'..=b'F').contains(&digit) {
        Ok(digit - b'A' + 10)
    } else {
        Err(InvalidKeyStr)
    }
}

impl<'a> ToOwned for Key {
    type Owned = Key;

    fn to_owned(&self) -> Self::Owned {
        // FIXME(Thomas): I think this can be done more efficiently.
        let mut bytes = [0; Key::LENGTH];
        bytes.copy_from_slice(&self.bytes);
        Key::new(bytes)
    }
}

impl Eq for Key {}

impl PartialEq for Key {
    fn eq(&self, other: &Key) -> bool {
        self.bytes[..] == other.bytes[..]
    }

    fn ne(&self, other: &Key) -> bool {
        self.bytes[..] != other.bytes[..]
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let b = self.bytes;
        write!(f,
            "{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15], b[16], b[17], b[18], b[19], b[20], b[21], b[22], b[23], b[24], b[25], b[26], b[27], b[28], b[29], b[30], b[31], b[32], b[33], b[34], b[35], b[36], b[37], b[38], b[39], b[40], b[41], b[42], b[43], b[44], b[45], b[46], b[47], b[48], b[49], b[50], b[51], b[52], b[53], b[54], b[55], b[56], b[57], b[58], b[59], b[60], b[61], b[62], b[63])
    }
}

impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[cfg(test)]
mod test {
    use crate::{InvalidKeyStr, Key};

    #[test]
    fn to_owned() {
        let bytes: Vec<u8> = (0..64).collect();
        let key1 = Key::from_bytes(&bytes);
        let key2 = key1.to_owned();
        assert_eq!(key1, &key2);
    }

    #[test]
    fn formatting() {
        let key = Key::for_value(b"Hello world");
        let expected = "b7f783baed8297f0db917462184ff4f08e69c2d5e\
                        5f79a942600f9725f58ce1f29c18139bf80b06c0f\
                        ff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47";
        assert_eq!(format!("{}", key), expected); // `fmt::Display` trait.
        assert_eq!(format!("{:?}", key), expected); // `fmt::Debug` trait.
        assert_eq!(key.to_string(), expected); // ToString trait.
    }

    #[test]
    fn parsing() {
        let expected = Key::for_value(b"Hello world");
        let input = "b7f783baed8297f0db917462184ff4f08e69c2d5e\
                     5f79a942600f9725f58ce1f29c18139bf80b06c0f\
                     ff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47";
        let key: Key = input.parse().expect("unexpected error parsing key");
        assert_eq!(key, expected);
    }

    #[test]
    fn parsing_errors() {
        // Invalid input length.
        let input = "";
        assert_eq!(input.parse::<Key>(), Err(InvalidKeyStr));

        // Invalid hex digits.
        let input = "G7f783baed8297f0db917462184ff4f08e69c2d5e\
                     5f79a942600f9725f58ce1f29c18139bf80b06c0f\
                     ff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47";
        assert_eq!(input.parse::<Key>(), Err(InvalidKeyStr));
    }
}
