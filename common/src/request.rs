//! Request types.

use crate::Hash;

/// Request to store value.
pub struct Store<'a> {
    /// Value to store.
    value: &'a [u8],
}

impl<'a> Store<'a> {
    /// Create a new store request.
    pub const fn new(value: &'a [u8]) -> Store<'a> {
        Store {
            value,
        }
    }
}

/// Request to retrieve a value with a given hash.
pub struct Retrieve<'a> {
    /// Hash of the value to retrieve.
    hash: &'a Hash,
}

impl<'a> Retrieve<'a> {
    /// Create a new retrieve request.
    pub const fn new(hash: &'a Hash) -> Retrieve<'a> {
        Retrieve {
            hash,
        }
    }
}

/// Request to remove a value with a given hash.
pub struct Remove<'a> {
    /// Hash of the value to remove.
    hash: &'a Hash,
}

impl<'a> Remove<'a> {
    /// Create a new remove request.
    pub const fn new(hash: &'a Hash) -> Remove<'a> {
        Remove {
            hash,
        }
    }
}
