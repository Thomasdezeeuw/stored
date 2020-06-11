use std::{error, fmt, io};

/// Trait to add details to an error.
pub trait Detail {
    /// Add a detail to an error.
    fn detail(self, detail: &'static str) -> ErrorDetail<Self>
    where
        Self: Sized;
}

impl<E> Detail for E {
    fn detail(self, detail: &'static str) -> ErrorDetail<E>
    where
        Self: Sized,
    {
        ErrorDetail::new(self, detail)
    }
}

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

#[cfg(test)]
mod tests {
    use std::io;
    use std::mem::size_of;

    use super::ErrorDetail;

    #[test]
    fn size() {
        assert_eq!(size_of::<ErrorDetail<io::Error>>(), 32);
        assert_eq!(size_of::<ErrorDetail<()>>(), 16);
    }

    #[test]
    fn display() {
        let err = ErrorDetail::<io::Error>::new(io::ErrorKind::WouldBlock.into(), "whoopsie");
        assert_eq!(err.to_string(), "whoopsie: operation would block");
    }

    #[test]
    fn detail_trait() {
        let err = io::Error::from(io::ErrorKind::WouldBlock.into());
        err.detail("made a boo boo");
        assert_eq!(err.to_string(), "made a boo boo: operation would block");
    }
}
