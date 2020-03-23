//! Module with the database actor.

use std::io;

use heph::actor::sync::SyncContext;
use heph::actor_ref::RpcMessage;
use log::debug;

use crate::server::storage::{AddBlob, AddResult, Blob, Storage};
use crate::Key;

/// Message type send to the storage [`actor`].
pub enum Message {
    /// Add a blob to the database.
    ///
    /// Request are the bytes that make up the blob.
    ///
    /// Responds with  a query to commit to adding the blob, or the key of the
    /// blob if the blob is already in the database.
    AddBlob(RpcMessage<Box<[u8]>, AddBlobResponse>),

    /// Commit to a blob being added.
    ///
    /// Request is the query to add the blob, returned by [`Message::AddBlob`].
    ///
    /// Responds the `Key` of the added blob.
    CommitBlob(RpcMessage<AddBlob, Key>),

    /// Get a blob from storage.
    ///
    /// Request is the key to look up.
    ///
    /// Responds with the `Blob`, if its in the database.
    GetBlob(RpcMessage<Key, Option<Blob>>),
}

impl From<RpcMessage<Box<[u8]>, AddBlobResponse>> for Message {
    fn from(msg: RpcMessage<Box<[u8]>, AddBlobResponse>) -> Message {
        Message::AddBlob(msg)
    }
}

impl From<RpcMessage<AddBlob, Key>> for Message {
    fn from(msg: RpcMessage<AddBlob, Key>) -> Message {
        Message::CommitBlob(msg)
    }
}

impl From<RpcMessage<Key, Option<Blob>>> for Message {
    fn from(msg: RpcMessage<Key, Option<Blob>>) -> Message {
        Message::GetBlob(msg)
    }
}

/// Response to [`Message::AddBlob`].
pub enum AddBlobResponse {
    /// Query to commit to adding a blob.
    Query(AddBlob),
    /// The blob is already stored.
    AlreadyPresent(Key),
}

/// Actor that handles storage [`Message`]s and applies them to [`Storage`].
pub fn actor(mut ctx: SyncContext<Message>, mut storage: Storage) -> io::Result<()> {
    debug!(
        "storage actor started: data_size={}, index_size={}, total_size={}",
        storage.data_size(),
        storage.index_size(),
        storage.total_size()
    );

    while let Ok(msg) = ctx.receive_next() {
        match msg {
            Message::AddBlob(RpcMessage { request, response }) => {
                let blob = request;
                debug!("adding new blob: size={}", blob.len());
                use AddResult::*;
                let result = match storage.add_blob(&blob) {
                    Ok(query) => AddBlobResponse::Query(query),
                    AlreadyPresent(key) => AddBlobResponse::AlreadyPresent(key),
                    Err(err) => return Result::Err(err),
                };
                // If the actor is disconnected this is not really a problem.
                let _ = response.respond(result);
            }
            Message::CommitBlob(RpcMessage { request, response }) => {
                let key = storage.commit(request)?;
                // If the actor is disconnected this is not really a problem.
                let _ = response.respond(key);
            }
            Message::GetBlob(RpcMessage { request, response }) => {
                let key = request;
                debug!("retrieve blob: key={}", key);
                let result = storage.lookup(&key);
                // If the actor is disconnected this is not really a problem.
                let _ = response.respond(result);
            }
        }
    }

    debug!("storage actor stopping");
    Ok(())
}
