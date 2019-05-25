//! Response types.
//!
//! All variants of each type in the module implements [`ToOwned`] which can be
//! used to get an owned version of the response when dealing with lifetimes
//! becomes to complicated.

use crate::Key;

/// Response to a store request.
#[derive(Debug, Eq, PartialEq)]
pub enum Store<'a> {
    /// The value was successfully stored.
    Success(&'a Key),
}

/// Response to a retrieve request.
#[derive(Debug, Eq, PartialEq)]
pub enum Retrieve<'a> {
    /// Value was found.
    Value(&'a [u8]),
    /// Value was not found.
    ValueNotFound,
}

/// Response to a remove request.
#[derive(Debug, Eq, PartialEq)]
pub enum Remove {
    /// Value was successfully removed.
    Ok,
    /// Value was not found.
    ValueNotFound,
}
