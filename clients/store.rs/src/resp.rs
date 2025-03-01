//! Redis Serialization Protocol (RESP) version 2 protocol
//! (<https://redis.io/topics/protocol>).

pub(crate) const NIL: &str = "$-1\r\n";
pub(crate) const CRLF: &str = "\r\n";

pub(crate) mod encode {
    //! Module that encodes following the Redis Protocol (RESP2).
    //!
    //! <https://redis.io/topics/protocol>.

    use crate::Key;

    /// Encode the start of an array of `length` elements onto `buf` (without
    /// changing it's current contents).
    pub(crate) fn array(buf: &mut Vec<u8>, length: usize) {
        int(buf, b'*', length);
    }

    /// Encode a string onto `buf` (without changing it's current contents).
    pub(crate) fn string(buf: &mut Vec<u8>, value: &str) {
        string_start(buf, value.len());
        buf.extend_from_slice(value.as_bytes());
        buf.push(b'\r');
        buf.push(b'\n');
    }

    /// Encode the start of a string onto `buf` (without changing it's current
    /// contents).
    pub(crate) fn string_start(buf: &mut Vec<u8>, length: usize) {
        int(buf, b'$', length);
    }

    /// Encode a key onto `buf` (without changing it's current contents).
    pub(crate) fn key(buf: &mut Vec<u8>, key: &Key) {
        int(buf, b'$', Key::STR_LENGTH);
        key.append_to(buf);
        buf.push(b'\r');
        buf.push(b'\n');
    }

    /// Encode an integer with `prefix`.
    fn int(buf: &mut Vec<u8>, prefix: u8, value: usize) {
        let mut buffer = itoa::Buffer::new();
        let int_bytes = buffer.format(value).as_bytes();
        buf.reserve(int_bytes.len() + 3); // 3 = prefix + CRLF.
        buf.push(prefix);
        buf.extend_from_slice(int_bytes);
        buf.push(b'\r');
        buf.push(b'\n');
    }
}

pub(crate) mod decode {
    //! Module that can decode the Redis Protocol (RESP2).
    //!
    //! <https://redis.io/topics/protocol>.

    use std::io;

    use crate::resp::NIL;
    use crate::{invalid_response, new_error};

    /// Result of a parsing function.
    ///
    /// Returns `Ok(Some((value, bytes_read)))` on success, `Ok(None)` is returned
    /// if `buf` doesn't contain a complete result and an error otherwise.
    pub(crate) type ParseResult<T> = Result<Option<(T, usize)>, io::Error>;

    /// Parse a string from `buf` including starting `$` or `+` and `\r\n` end.
    pub(crate) fn string(buf: &[u8]) -> ParseResult<Option<&[u8]>> {
        match buf.first() {
            Some(b'$') => {} // Bulk string, continue below.
            // Simple string.
            Some(b'+') => match until_crlf(buf) {
                Some((string, bytes_read)) => return Ok(Some((Some(string), bytes_read))),
                None => return Ok(None),
            },
            Some(b'-') => match error(buf) {
                Some(err) => return Err(err),
                None => return Ok(None),
            },
            Some(_) => return Err(invalid_response()),
            None => return Ok(None),
        }

        if buf.starts_with(NIL.as_bytes()) {
            return Ok(Some((None, NIL.len())));
        }

        let (length, processed) = match int(buf) {
            Ok(Some((len, processed))) => (len, processed), // $ is included in processed.
            Ok(None) => return Ok(None),
            Err(err) => return Err(err),
        };

        let buf = &buf[processed..];
        if buf.len() < length + 2 {
            return Ok(None);
        }

        if !matches!(buf.get(length), Some(b'\r')) || !matches!(buf.get(length + 1), Some(b'\n')) {
            Err(invalid_response())
        } else {
            Ok(Some((Some(&buf[..length]), processed + length + 2))) // + 2 = CRLF
        }
    }

    /// Parse an integer from `buf` including starting `:` and `\r\n` end.
    ///
    /// Returns the integer value.
    pub(crate) fn integer(buf: &[u8]) -> ParseResult<usize> {
        match buf.first() {
            Some(b':') => int(buf),
            Some(b'-') => match error(buf) {
                Some(err) => Err(err),
                None => Ok(None),
            },
            Some(_) => Err(invalid_response()),
            None => Ok(None),
        }
    }

    fn int(buf: &[u8]) -> ParseResult<usize> {
        let mut value: usize = 0;
        let mut bytes = buf[1..].iter();
        while let Some(b) = bytes.next() {
            match b {
                b'0'..=b'9' => match value
                    .checked_mul(10)
                    .and_then(|v| v.checked_add((b - b'0') as usize))
                {
                    Some(v) => value = v,
                    None => return Err(new_error("response integer too large")),
                },
                b'\r' => match bytes.next() {
                    Some(b'\n') => {
                        let left = buf.len() - bytes.as_slice().len();
                        return Ok(Some((value, left)));
                    }
                    _ => return Err(invalid_response()),
                },
                _ => return Err(invalid_response()),
            }
        }
        Ok(None)
    }

    /// Parse an error from `buf` including starting `-` and `\r\n` end.
    fn error(buf: &[u8]) -> Option<io::Error> {
        debug_assert_eq!(buf.first(), Some(&b'-'));
        let res = until_crlf(buf)?;
        Some(new_error(format_args!(
            "error from server: {}",
            String::from_utf8_lossy(&res.0)
        )))
    }

    /// Returns the range until it hits CRLF.
    ///
    /// Ignores the first bytes (type indicator).
    fn until_crlf(buf: &[u8]) -> Option<(&[u8], usize)> {
        let mut bytes = buf.iter();
        _ = bytes.next();
        let mut end: usize = 1; // Skipping first byte per the docs.
        while let Some(b) = bytes.next() {
            if *b == b'\r' {
                if let Some(b'\n') = bytes.next() {
                    return Some((&buf[1..end], end + 2)); // +2 = CRLF.
                }
                end += 1;
            }
            end += 1;
        }
        None
    }
}
