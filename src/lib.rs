#![feature(never_type, type_alias_impl_trait)]

use std::fmt;

pub mod controller;
pub mod key;
pub mod protocol;
pub mod storage;

/// Describe a result.
pub trait Describe<T, E> {
    /// Describe an error or result.
    fn describe(self, description: &'static str) -> Result<T, Error<E>>;
}

impl<T, E> Describe<T, E> for Result<T, E> {
    fn describe(self, description: &'static str) -> Result<T, Error<E>> {
        match self {
            Ok(ok) => Ok(ok),
            Err(err) => Err(Error {
                description,
                source: err,
            }),
        }
    }
}

/// A [`Describe`]d error.
///
/// This wraps an error type`E` to provide a better description. For example
/// instead of returning an [`std::io::Error`] without context we return it with
/// the string "can't open storage", which provides much more information than
/// just the raw error message.
#[derive(Debug)]
pub struct Error<E> {
    description: &'static str,
    source: E,
}

impl<E> std::error::Error for Error<E>
where
    E: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

impl<E> fmt::Display for Error<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.description, self.source)
    }
}

/// Whether or not an error is fatal.
pub trait IsFatal {
    /// If this returns true the component is considered broken and will no
    /// longer be used.
    fn is_fatal(&self) -> bool;
}
