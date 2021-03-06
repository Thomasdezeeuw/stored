//! Stored's error and result types.

use std::{error, fmt, io};

/// Trait to describe an error, providing additional context over the plain
/// error.
///
/// # Examples
///
/// ```rust
/// use std::io::{self, Write};
///
/// use stored::error::Describe;
///
/// fn my_fn() -> stored::Result<()> {
///     io::stdout().write_all(b"Hello world!")
///         .map_err(|err| err.describe("writing to standard out"))
/// }
/// ```
pub trait Describe {
    /// Describe an error.
    fn describe(self, description: &'static str) -> Error<Self>
    where
        Self: Sized,
    {
        Error::new(self, description)
    }

    /// Describe an error with additional context.
    fn describe_with<T>(self, description: &'static str, ctx: T) -> Error<(Self, T)>
    where
        Self: Sized,
    {
        Error::new((self, ctx), description)
    }
}

impl<E> Describe for E {}

/// Convenience type to use with [`Error`].
pub type Result<T, E = io::Error> = std::result::Result<T, Error<E>>;

/// An error with an description of the failed operation.
///
/// This can be easily created using the [`Describe`] trait.
#[derive(Debug)]
pub struct Error<E = io::Error> {
    /// The original error.
    err: E,
    /// Description of the failed operation.
    description: &'static str,
}

impl<E> Error<E> {
    /// Create a new error with a description.
    pub const fn new(err: E, description: &'static str) -> Error<E> {
        Error { err, description }
    }

    /// Add more context to the error.
    pub fn with<T>(self, ctx: T) -> Error<(E, T)> {
        Error {
            err: (self.err, ctx),
            description: self.description,
        }
    }

    /// Returns the wrapped error `E`.
    pub fn into_inner(self) -> E {
        self.err
    }

    /// Returns the description of the error.
    pub const fn description(&self) -> &'static str {
        self.description
    }
}

impl<E, T> Error<(E, T)> {
    /// Returns a reference to the wrapper error `E`.
    pub fn error(&self) -> &E {
        &self.err.0
    }

    /// Returns a reference to the additional context.
    pub fn context(&self) -> &T {
        &self.err.1
    }
}

impl<E: fmt::Display> fmt::Display for Error<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.description, self.err)
    }
}

impl<E: error::Error + 'static> error::Error for Error<E> {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        Some(&self.err)
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::mem::size_of;

    use super::{Describe, Error};

    #[test]
    fn size() {
        assert_eq!(size_of::<Error<io::Error>>(), 32);
        assert_eq!(size_of::<Error<()>>(), 16);
    }

    #[test]
    fn display() {
        let err = Error::<io::Error>::new(io::ErrorKind::WouldBlock.into(), "whoopsie");
        assert_eq!(err.to_string(), "whoopsie: operation would block");
    }

    #[test]
    fn describe_trait() {
        let err = io::Error::from(io::ErrorKind::WouldBlock);
        let err = err.describe("made a boo boo");
        assert_eq!(err.to_string(), "made a boo boo: operation would block");
    }
}
