//! Response types.

use crate::Hash;

// Serialize, Deserialize

/// Generic OK response.
///
/// Returned by remove.
pub struct Ok;

/// Value is successfully stored.
///
/// Return by store.
pub struct Store<'a> {
    /// Hash of the value stored.
    hash: &'a Hash,
}

impl<'a> Store<'a> {
    /// Create a new `Store` response.
    pub(crate) const fn new(hash: &'a Hash) -> Store<'a> {
        Store {
            hash,
        }
    }

    /// The hash of the stored value.
    pub const fn hash(&'a self) -> &'a Hash {
        self.hash
    }
}

/// A retrieved value.
///
/// Returned by retrieve.
pub struct Value<'a> {
    /// Retrieved value.
    value: &'a [u8],
}

impl<'a> Value<'a> {
    /// Create a new `Value` response.
    pub(crate) const fn new(value: &'a [u8]) -> Value<'a> {
        Value {
            value,
        }
    }

    /// The retrieved value.
    pub const fn value(&'a self) -> &'a [u8] {
        self.value
    }
}

/// Value in the request is not found.
///
/// Returned by retrieve and remove.
pub struct ValueNotFound;
