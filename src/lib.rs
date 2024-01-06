//! Store*d* (pronounced store-daemon, or just stored) is a distributed
//! immutable blob store. Store*d* is not a key-value store, as the key isn't the
//! decided by the user but by the SHA-512 checksum of the stored blob.
//!
//! It supports three operations: storing, retrieving and removing blobs. As the
//! key of a blob is its checksum it is not possible to modify blobs. If a blob
//! needs to be modified a new blob simply needs to be stored and the new key
//! used. The client can validate the correct delivery of the blob by using the
//! returned key (checksum). The blob themselves are unchanged by Store*d*.
//!
//! # Code Navigation
//!
//! The core of the logic is defined in the [`controller`] module. The
//! [`controller::actor`] defines the logic of handling requests. For this it
//! uses a [`Protocol`], which defines how requests are read and responses are
//! returned, and a reference to the [`Storage`], which defines the logic of
//! actually storing the blobs.
//!
//! [`Protocol`]: protocol::Protocol
//! [`Storage`]: storage::Storage

#![feature(
    async_iter_from_iter,
    async_iterator,
    if_let_guard,
    impl_trait_in_assoc_type,
    never_type
)]

pub mod config;
pub mod controller;
mod io;
pub mod key;
pub mod protocol;
pub mod storage;
