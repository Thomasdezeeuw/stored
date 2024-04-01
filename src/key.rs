//! Key of a blob.
//!
//! See [`Key`].

use std::error::Error;
use std::fmt;
use std::str::FromStr;

use ring::digest::{self, SHA512_OUTPUT_LEN};

/// The key of a blob.
///
/// This is always the SHA-512 checksum of the blob, which can be calculated
/// using the [`Key::for_blob`] method.
#[derive(Eq, PartialEq, Clone)]
#[repr(transparent)]
pub struct Key {
    bytes: [u8; Key::LENGTH],
}

impl Key {
    /// Length of the key in bytes.
    pub const LENGTH: usize = SHA512_OUTPUT_LEN;

    /// Length of the key formatted as string (using hex).
    pub const STR_LENGTH: usize = Self::LENGTH * 2;

    /// Create a new `Key` from the provided `bytes`.
    pub const fn new(bytes: [u8; Key::LENGTH]) -> Key {
        Key { bytes }
    }

    /// Convert a slice of bytes of length `Key::LENGTH` into `&Key`.
    ///
    /// # Panics
    ///
    /// This will panic if `bytes` is not of length `Key::LENGTH`.
    pub fn from_bytes(bytes: &[u8]) -> &Key {
        assert!(bytes.len() >= Key::LENGTH, "invalid Key length");
        // Safety: we ensured above that `bytes` is of length `Key::LENGTH` and
        // `Key` has the same layout as `[u8; Key::LENGTH]` because we use the
        // `repr(transparent)` attribute, so this cast is same.
        unsafe { &*(bytes.as_ptr().cast()) }
    }

    /// Parse a key from a string.
    ///
    /// This is the same as the [`FromStr::from_str`] implementation, but is
    /// a constant function.
    pub const fn try_parse(key: &str) -> Result<Key, InvalidKeyStr> {
        Key::try_parse_bytes(key.as_bytes())
    }

    /// Same as the [`Key::try_parse`], but uses `&[u8]` instead of a `str`ing.
    pub const fn try_parse_bytes(input: &[u8]) -> Result<Self, InvalidKeyStr> {
        if input.len() != Key::LENGTH * 2 {
            return Err(InvalidKeyStr);
        }

        let mut bytes = [0; Key::LENGTH];
        let mut i = 0;
        while i < Key::LENGTH {
            let high = from_hex_digit(input[i * 2]);
            let low = from_hex_digit(input[(i * 2) + 1]);
            if high == INVALID_HEX_DIGIT || low == INVALID_HEX_DIGIT {
                return Err(InvalidKeyStr);
            }
            bytes[i] = (high << 4) | low;
            i += 1;
        }
        Ok(Key::new(bytes))
    }

    /// Calculate the `Key` for the provided `blob`.
    pub fn for_blob(blob: &[u8]) -> Key {
        let mut calc = KeyCalculator::new();
        calc.update(blob);
        calc.finish()
    }

    /// Get the key as bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Error returned by [`Key`]'s [`FromStr`] implementation.
#[derive(Debug, Eq, PartialEq)]
pub struct InvalidKeyStr;

impl InvalidKeyStr {
    #[doc(hidden)] // For the `key!` macro.
    pub const fn description() -> &'static str {
        "invalid stored Key: invalid SHA-512 checksum"
    }
}

impl fmt::Display for InvalidKeyStr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(InvalidKeyStr::description())
    }
}

impl Error for InvalidKeyStr {
    fn description(&self) -> &str {
        InvalidKeyStr::description()
    }
}

impl FromStr for Key {
    type Err = InvalidKeyStr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Key::try_parse(s)
    }
}

const INVALID_HEX_DIGIT: u8 = u8::MAX;

/// Returns `INVALID_HEX_DIGIT` in case of an error.
const fn from_hex_digit(digit: u8) -> u8 {
    match digit {
        b'0'..=b'9' => digit - b'0',
        b'a'..=b'f' => digit - b'a' + 10,
        b'A'..=b'F' => digit - b'A' + 10,
        _ => INVALID_HEX_DIGIT,
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

/// The key calculator, see [`Key::calculator`].
pub struct KeyCalculator {
    digest: digest::Context,
}

impl KeyCalculator {
    /// Create a `KeyCalculator`.
    ///
    /// # Examples
    ///
    /// ```
    /// use stored::key::{Key, KeyCalculator};
    ///
    /// let blob = b"Hello world";
    ///
    /// let mut calculator = KeyCalculator::new();
    /// calculator.update(&blob[..6]);
    /// calculator.update(&blob[6..]);
    /// let key = calculator.finish();
    /// assert_eq!(key, Key::for_blob(blob));
    /// ```
    pub fn new() -> KeyCalculator {
        KeyCalculator {
            digest: digest::Context::new(&digest::SHA512),
        }
    }

    /// Update the calculation with `bytes`.
    pub fn update(&mut self, bytes: &[u8]) {
        self.digest.update(bytes);
    }

    /// Finish the calculation returning the [`Key`] for all read/written bytes.
    pub fn finish(self) -> Key {
        let result = self.digest.finish();
        Key::from_bytes(result.as_ref()).to_owned()
    }
}

impl fmt::Debug for KeyCalculator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("KeyCalculator").finish_non_exhaustive()
    }
}

/// Macro to create a constant [`Key`].
#[macro_export]
macro_rules! key {
    ($key: literal) => {{
        const OUTPUT: $crate::key::Key = match $crate::key::Key::try_parse($key) {
            ::std::result::Result::Ok(key) => key,
            ::std::result::Result::Err($crate::key::InvalidKeyStr) => {
                panic!("{}", $crate::key::InvalidKeyStr::description())
            }
        };
        OUTPUT
    }};
}

pub use key;
