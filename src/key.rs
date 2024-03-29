//! Blob's key.

use std::error::Error;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io::{self, IoSlice, IoSliceMut, Read, Write};
use std::ops::Deref;
use std::pin::Pin;
use std::str::FromStr;
use std::task::{self, Poll};

use futures_io::{AsyncRead, AsyncWrite};
use ring::digest::{self, digest, SHA512, SHA512_OUTPUT_LEN};
use serde::ser::SerializeTuple;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The key of a blob.
///
/// This is always the SHA-512 checksum of the blob, which can be calculated
/// using the [`Key::for_blob`] method.
#[derive(Clone)]
#[repr(transparent)]
pub struct Key {
    bytes: [u8; Key::LENGTH],
}

impl Key {
    /// Length of the key in bytes.
    pub const LENGTH: usize = SHA512_OUTPUT_LEN;

    /// Create a new `Key` from the provided `bytes`.
    pub const fn new(bytes: [u8; Key::LENGTH]) -> Key {
        Key { bytes }
    }

    /// Convert a slice of bytes of length `Key::LENGTH` into `&Key`.
    ///
    /// # Panics
    ///
    /// This will panic if `bytes` is not of length `Key::LENGTH`.
    pub fn from_bytes<'a>(bytes: &'a [u8]) -> &'a Key {
        assert_eq!(bytes.len(), Key::LENGTH, "invalid Key length");
        // Safety: we ensured above that `bytes` is of length `Key::LENGTH` and
        // `Key` has the same layout as `[u8; Key::LENGTH]` because we use the
        // `repr(transparent)` attribute, so this cast is same.
        unsafe { &*(bytes.as_ptr().cast()) }
    }

    /// Calculate the `Key` for the provided `blob`.
    pub fn for_blob<'a>(blob: &'a [u8]) -> Key {
        let result = digest(&SHA512, blob);
        Key::from_bytes(result.as_ref()).to_owned()
    }

    /// Get the key as bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Create a `KeyCalculator`.
    ///
    /// `KeyCalculator` is a wrapper around I/O to calculate the [`Key`] for a
    /// blob, while streaming its contents.
    ///
    /// It can be used while [`Reading`] or [`Writing`], and even asynchronously
    /// with [`AsyncRead`] or [`AsyncWrite`].
    ///
    /// [`Reading`]: std::io::Read
    /// [`Writing`]: std::io::Write
    ///
    /// # Notes
    ///
    /// When using `KeyCalculator`'s asynchronous reading and writing traits it
    /// doesn't implement any waking mechanism, it up to the `IO` type to handle
    /// wakeups.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::io;
    /// use std::io::{Write, IoSlice};
    ///
    /// # use stored::Key;
    /// #
    /// # fn main() -> io::Result<()> {
    /// // Our `Write` implementation.
    /// let mut streamed_blob = Vec::new();
    /// let mut calculator = Key::calculator(&mut streamed_blob);
    ///
    /// // We can now stream the blob.
    /// calculator.write(b"Hello")?;
    /// calculator.write_vectored(&mut [IoSlice::new(b" "), IoSlice::new(b"world")])?;
    ///
    /// let key = calculator.finish();
    /// assert_eq!(key, Key::for_blob(b"Hello world"));
    ///
    /// // Now the writer can be used again.
    /// streamed_blob.write(b"!")?;
    /// assert_eq!(streamed_blob, b"Hello world!");
    /// # Ok(())
    /// # }
    /// ```
    pub fn calculator<IO>(io: IO) -> KeyCalculator<IO> {
        Key::calculator_skip(io, 0)
    }

    /// Same as [`Key::calculator`] but skips `skip` bytes before using them in
    /// the `Key` calculation.
    pub fn calculator_skip<IO>(io: IO, skip: usize) -> KeyCalculator<IO> {
        KeyCalculator {
            digest: digest::Context::new(&digest::SHA512),
            io,
            skip_left: skip,
        }
    }
}

/// Error returned by [`Key`]'s [`FromStr`] implementation.
#[derive(Debug, Eq, PartialEq)]
pub struct InvalidKeyStr;

impl InvalidKeyStr {
    pub(crate) const DESC: &'static str = "invalid SHA-512 checksum string";
}

impl fmt::Display for InvalidKeyStr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(Self::DESC)
    }
}

impl Error for InvalidKeyStr {
    fn description(&self) -> &str {
        Self::DESC
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

impl<'de> Deserialize<'de> for Key {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{Error, SeqAccess, Visitor};

        struct KeyVisitor;

        impl<'de> Visitor<'de> for KeyVisitor {
            type Value = Key;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a key")
            }

            fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                s.parse().map_err(Error::custom)
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut key_bytes = [0; Key::LENGTH];
                for byte in key_bytes.iter_mut() {
                    match seq.next_element()? {
                        Some(b) => *byte = b,
                        None => return Err(Error::invalid_length(Key::LENGTH, &self)),
                    }
                }
                Ok(Key::new(key_bytes))
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_str(KeyVisitor)
        } else {
            deserializer.deserialize_tuple(Key::LENGTH, KeyVisitor)
        }
    }

    // TODO: implement deserialize_in_place.
}

impl Serialize for Key {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            let mut buf = [0; Key::LENGTH * 2];
            write!(&mut buf[..], "{}", self).unwrap();
            let output = std::str::from_utf8(&buf).unwrap();
            serializer.serialize_str(output)
        } else {
            let mut seq = serializer.serialize_tuple(Key::LENGTH)?;
            for byte in self.bytes.iter() {
                seq.serialize_element(byte)?;
            }
            seq.end()
        }
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

impl Hash for Key {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        Hash::hash(&self.bytes[..], state)
    }
}

/// The key calculator, see [`Key::calculator`].
pub struct KeyCalculator<IO> {
    /// NOTE: don't use this directly, use `update_digest` and `update_digestv`,
    /// which take `skip_left` into account.
    digest: digest::Context,
    /// Number of bytes left to ignore in the [`Key`] calculation.
    skip_left: usize,
    io: IO,
}

impl KeyCalculator<()> {
    /// Create a new [`KeyCalculator`] which is not backed by I/O.
    ///
    /// # Examples
    ///
    /// ```
    /// use stored::key::{Key, KeyCalculator};
    ///
    /// let blob = b"Hello world";
    ///
    /// let mut calculator = KeyCalculator::new();
    /// calculator.add_bytes(&blob[..6]);
    /// calculator.add_bytes(&blob[6..]);
    /// let key = calculator.finish();
    /// assert_eq!(key, Key::for_blob(blob));
    /// ```
    pub fn new() -> KeyCalculator<()> {
        Key::calculator(())
    }

    /// Add blob bytes to the calculation.
    pub fn add_bytes(&mut self, bytes: &[u8]) {
        self.digest.update(&bytes);
    }
}

impl<IO> KeyCalculator<IO> {
    /// Finish the calculation returning the [`Key`] for all read/written bytes.
    pub fn finish(self) -> Key {
        let result = self.digest.finish();
        Key::from_bytes(result.as_ref()).to_owned()
    }

    fn update_digest(&mut self, bytes: &[u8]) {
        if self.skip_left == 0 {
            // No more bytes to skip.
            self.digest.update(&bytes);
        } else if bytes.len() <= self.skip_left {
            // Need to skip all bytes.
            self.skip_left -= bytes.len();
        } else {
            // Need to skip some of the bytes.
            self.digest.update(&bytes[self.skip_left..]);
            if bytes.len() > self.skip_left {
                self.skip_left = 0;
            } else {
                self.skip_left -= bytes.len();
            }
        }
    }

    fn update_digestv<B>(&mut self, bufs: &[B], processed: usize)
    where
        B: Deref<Target = [u8]>,
    {
        let mut left = processed;
        for buf in bufs {
            let length = buf.len();
            if length >= left {
                self.update_digest(&buf[..left]);
                return;
            } else {
                // Entire buffer was filled.
                self.update_digest(&buf);
                left -= length;
            }
        }
    }
}

impl<IO: fmt::Debug> fmt::Debug for KeyCalculator<IO> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("KeyCalculator")
            .field("skip_left", &self.skip_left)
            .field("io", &self.io)
            .finish()
    }
}

impl<R> Read for KeyCalculator<R>
where
    R: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.io.read(buf).map(|n| {
            self.update_digest(&buf[..n]);
            n
        })
    }

    fn read_vectored(&mut self, bufs: &mut [IoSliceMut]) -> io::Result<usize> {
        self.io.read_vectored(bufs).map(|n| {
            self.update_digestv(bufs, n);
            n
        })
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        self.io.read_to_end(buf).map(|n| {
            self.update_digest(&buf[..n]);
            n
        })
    }

    fn read_to_string(&mut self, buf: &mut String) -> io::Result<usize> {
        self.io.read_to_string(buf).map(|n| {
            self.update_digest(&buf.as_bytes());
            n
        })
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.io.read_exact(buf).map(|()| self.update_digest(buf))
    }
}

impl<W> Write for KeyCalculator<W>
where
    W: Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.io.write(buf).map(|n| {
            self.update_digest(&buf[..n]);
            n
        })
    }

    fn flush(&mut self) -> io::Result<()> {
        self.io.flush()
    }

    fn write_vectored(&mut self, bufs: &[IoSlice]) -> io::Result<usize> {
        self.io.write_vectored(bufs).map(|n| {
            self.update_digestv(bufs, n);
            n
        })
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.io.write_all(buf).map(|()| self.update_digest(buf))
    }
}

// TODO: lose the `Unpin` requirement.

impl<R> AsyncRead for KeyCalculator<R>
where
    R: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        ctx: &mut task::Context,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_read(ctx, buf).map_ok(|n| {
            self.update_digest(&buf[..n]);
            n
        })
    }

    fn poll_read_vectored(
        mut self: Pin<&mut Self>,
        ctx: &mut task::Context,
        bufs: &mut [IoSliceMut],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io)
            .poll_read_vectored(ctx, bufs)
            .map_ok(|n| {
                self.update_digestv(bufs, n);
                n
            })
    }
}

impl<W> AsyncWrite for KeyCalculator<W>
where
    W: AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        ctx: &mut task::Context,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(ctx, buf).map_ok(|n| {
            self.update_digest(&buf[..n]);
            n
        })
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        ctx: &mut task::Context,
        bufs: &[IoSlice],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io)
            .poll_write_vectored(ctx, bufs)
            .map_ok(|n| {
                self.update_digestv(bufs, n);
                n
            })
    }

    fn poll_flush(mut self: Pin<&mut Self>, ctx: &mut task::Context) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(ctx)
    }

    fn poll_close(mut self: Pin<&mut Self>, ctx: &mut task::Context) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_close(ctx)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Read};

    use serde_test::{assert_tokens, Configure, Token};

    use crate::key::{InvalidKeyStr, Key};

    #[test]
    fn to_owned() {
        let bytes: Vec<u8> = (0..64).collect();
        let key1 = Key::from_bytes(&bytes);
        assert_eq!(key1.as_bytes(), &*bytes);
        let key2 = key1.to_owned();
        assert_eq!(key1, &key2);
    }

    #[test]
    fn formatting() {
        let key = Key::for_blob(b"Hello world");
        let expected = "b7f783baed8297f0db917462184ff4f08e69c2d5e\
                        5f79a942600f9725f58ce1f29c18139bf80b06c0f\
                        ff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47";
        assert_eq!(format!("{}", key), expected); // `fmt::Display` trait.
        assert_eq!(format!("{:?}", key), expected); // `fmt::Debug` trait.
        assert_eq!(key.to_string(), expected); // ToString trait.
    }

    #[test]
    fn parsing() {
        let expected = Key::for_blob(b"Hello world");
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

    #[test]
    fn serialisation() {
        let key = Key::for_blob(b"Hello world");
        let expected = "\"b7f783baed8297f0db917462184ff4f08e69c2d5e\
                        5f79a942600f9725f58ce1f29c18139bf80b06c0f\
                        ff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47\"";
        let got = toml::ser::to_string(&key).unwrap();
        assert_eq!(got, expected);
    }

    #[test]
    fn deserialisation() {
        use serde::Deserialize;

        #[derive(Debug, Deserialize, Eq, PartialEq)]
        struct T {
            key: Key,
        }
        let expected = T {
            key: Key::for_blob(b"Hello world"),
        };
        let input = "key = \"b7f783baed8297f0db917462184ff4f08e69c2d5e\
                        5f79a942600f9725f58ce1f29c18139bf80b06c0f\
                        ff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47\"";
        let got: T = toml::de::from_str(&input).unwrap();
        assert_eq!(got, expected);
    }

    #[test]
    fn serialisation_compact() {
        let key = Key::for_blob(b"Hello world");
        let mut expected: Vec<Token> = Vec::with_capacity(Key::LENGTH + 2);
        expected.push(Token::Tuple { len: Key::LENGTH });
        for byte in key.as_bytes().iter().copied() {
            expected.push(Token::U8(byte));
        }
        expected.push(Token::TupleEnd);

        assert_tokens(&key.compact(), &expected);
    }

    #[test]
    fn serialisation_readable() {
        let key = Key::for_blob(b"Hello world");
        let expected = "b7f783baed8297f0db917462184ff4f08e69c2d5e\
                        5f79a942600f9725f58ce1f29c18139bf80b06c0f\
                        ff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47";
        assert_tokens(&key.readable(), &[Token::Str(expected)]);
    }

    #[test]
    fn key_calculator() {
        let want = Key::for_blob(b"Hello world");
        let reader = io::Cursor::new(b"Hello world");
        let mut calc = Key::calculator(reader);
        let mut buf = [0; 6];
        assert_eq!(calc.read(&mut buf).unwrap(), 6);
        assert_eq!(&buf, b"Hello ");
        assert_eq!(calc.read(&mut buf).unwrap(), 5);
        assert_eq!(&buf, b"world "); // Last space from previous read.
        assert_eq!(calc.finish(), want);
    }

    #[test]
    fn key_calculator_skip_n() {
        let want = Key::for_blob(b"Hello world");
        let reader = io::Cursor::new(b"123Hello world");
        let mut calc = Key::calculator_skip(reader, 3);
        let mut buf = [0; 7];
        assert_eq!(calc.read(&mut buf).unwrap(), 7);
        assert_eq!(&buf, b"123Hell");
        assert_eq!(calc.read(&mut buf).unwrap(), 7);
        assert_eq!(&buf, b"o world");
        assert_eq!(calc.finish(), want);
    }
}
