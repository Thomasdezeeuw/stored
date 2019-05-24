//! Response types.
//!
//! All variants of each type in the module implements [`ToOwned`] which can be
//! used to get an owned version of the response when dealing with lifetimes
//! becomes to complicated.

use crate::Hash;

/// Response to a store request.
pub enum Store<'a>{
    /// The value was successfully stored.
    Success(&'a Hash),
}

/// Response to a retrieve request.
pub enum Retrieve<'a> {
    /// Value was found.
    Value(&'a [u8]),
    /// Value was not found.
    ValueNotFound,
}

/// Response to a remove request.
pub enum Remove {
    /// Value was successfully removed.
    Ok,
    /// Value was not found.
    ValueNotFound,
}
