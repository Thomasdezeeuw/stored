use std::{error, fmt, io};

/// Convenience type to use with [`ErrorDetail`].
pub type DetailResult<T, E = std::io::Error> = std::result::Result<T, ErrorDetail<E>>;

/// A context describing error.
#[derive(Debug)]
pub struct ErrorDetail<E = io::Error> {
    /// The original error.
    err: E,
    /// Additional details for the context which the error occurred.
    detail: &'static str,
}

impl<E> ErrorDetail<E> {
    /// Create a new error with a context description.
    pub const fn new(err: E, detail: &'static str) -> ErrorDetail<E> {
        ErrorDetail { detail, err }
    }
}

impl<E: fmt::Display> fmt::Display for ErrorDetail<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.detail, self.err)
    }
}

impl<E: error::Error + 'static> error::Error for ErrorDetail<E> {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        Some(&self.err)
    }
}
