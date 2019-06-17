//! Response types.
//!
//! All variants of each type in the module implements [`ToOwned`] which can be
//! used to get an owned version of the response when dealing with lifetimes
//! becomes to complicated.

use crate::Key;

use coeus_common::parse;

/// Response to a store request.
#[derive(Debug, Eq, PartialEq)]
pub enum Store<'a> {
    /// The value was successfully stored.
    Success(&'a Key),
    /// Got a response we didn't expect, this should be seen as an error.
    UnexpectedResponse,
}

impl<'a> Store<'a> {
    pub(crate) fn from_parsed(response: parse::Response<'a>) -> Store<'a> {
        match response {
            parse::Response::Store(key) => Store::Success(key),
            _ => Store::UnexpectedResponse,
        }
    }
}

/// Response to a retrieve request.
#[derive(Debug, Eq, PartialEq)]
pub enum Retrieve<'a> {
    /// Value was found.
    Value(&'a [u8]),
    /// Value was not found.
    ValueNotFound,
    /// Got a response we didn't expect, this should be seen as an error.
    UnexpectedResponse,
}

impl<'a> Retrieve<'a> {
    pub(crate) fn from_parsed(response: parse::Response<'a>) -> Retrieve<'a> {
        match response {
            parse::Response::Value(value) => Retrieve::Value(value),
            parse::Response::ValueNotFound => Retrieve::ValueNotFound,
            _ => Retrieve::UnexpectedResponse,
        }
    }
}

/// Response to a remove request.
#[derive(Debug, Eq, PartialEq)]
pub enum Remove {
    /// Value was successfully removed.
    Ok,
    /// Value was not found.
    ValueNotFound,
    /// Got a response we didn't expect, this should be seen as an error.
    UnexpectedResponse,
}

impl Remove {
    pub(crate) fn from_parsed<'a>(response: parse::Response<'a>) -> Remove {
        match response {
            parse::Response::Ok => Remove::Ok,
            parse::Response::ValueNotFound => Remove::ValueNotFound,
            _ => Remove::UnexpectedResponse,
        }
    }
}
