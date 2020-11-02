//! Database actor.
//!
//! The [`db::actor`] is the main type, which accepts [`db::Message`]s and is
//! supervised by [`db::Supervisor`]. The actor can be started by using the
//! [`db::start`] function.
//!
//! [`db::actor`]: crate::db::actor
//! [`db::Message`]: crate::db::Message
//! [`db::Supervisor`]: crate::db::Supervisor
//! [`db::start`]: crate::db::start

use std::io;
use std::path::Path;
use std::time::SystemTime;

use heph::actor::sync::SyncContext;
use heph::actor_ref::{ActorRef, RpcMessage};
use heph::rt::SyncActorOptions;
use heph::supervisor::SupervisorStrategy;
use heph::{from_message, rt, Runtime};
use log::{debug, error, trace, warn};

use crate::buffer::BufView;
use crate::error::Describe;
use crate::storage::{
    AddResult, BlobAlreadyStored, BlobEntry, Entries, Keys, RemoveBlob, RemoveResult, Storage,
    StoreBlob, StreamBlob, UncommittedBlob,
};
use crate::Key;

/// Start the database actor.
pub fn start(
    runtime: &mut Runtime,
    path: &Path,
) -> crate::Result<ActorRef<Message>, rt::Error<io::Error>> {
    let storage =
        Storage::open(path).map_err(|err| rt::Error::from(err).describe("opening database"))?;
    let supervisor = |err| {
        error!("error operating on database: {}", err);
        SupervisorStrategy::Stop
    };
    let options = SyncActorOptions::default().with_name("Storage".to_owned());
    runtime
        .spawn_sync_actor(supervisor, actor as fn(_, _) -> _, storage, options)
        .map_err(|err| err.map_type().describe("spawning database actor"))
}

/// Actor that handles storage [`Message`]s and applies them to [`Storage`].
pub fn actor(mut ctx: SyncContext<Message>, mut storage: Storage) -> crate::Result<()> {
    debug!(
        "database actor started: data_size={}, index_size={}, total_size={}",
        storage.data_size(),
        storage.index_size(),
        storage.total_size()
    );

    while let Ok(msg) = ctx.receive_next() {
        trace!("database actor received message: {:?}", msg);
        match msg {
            Message::AddBlob(RpcMessage { request, response }) => {
                let view = request;
                use AddResult::*;
                let result = match storage.add_blob(view.as_slice()) {
                    Ok(query) => (AddBlobResponse::Query(query), view),
                    AlreadyStored(key) => (AddBlobResponse::AlreadyStored(key), view),
                    Err(err) => return Result::Err(err.describe("adding a blob")),
                };
                if let Result::Err(err) = response.respond(result) {
                    warn!("db actor failed to send response to actor: {}", err);
                }
            }
            Message::StreamBlob(RpcMessage { request, response }) => {
                let blob_length = request;
                let stream_blob = storage
                    .stream_blob(blob_length)
                    .map(Box::new)
                    .map_err(|err| err.describe("streaming a blob"))?;
                if let Result::Err(err) = response.respond(stream_blob) {
                    warn!("db actor failed to send response to actor: {}", err);
                }
            }
            Message::AddStreamedBlob(RpcMessage { request, response }) => {
                let stream_blob = request;
                use AddResult::*;
                let result = match stream_blob.finish(&mut storage) {
                    Ok(query) => AddBlobResponse::Query(query),
                    AlreadyStored(key) => AddBlobResponse::AlreadyStored(key),
                    Err(err) => return Result::Err(err.describe("adding streamed blob")),
                };
                if let Result::Err(err) = response.respond(result) {
                    warn!("db actor failed to send response to actor: {}", err);
                }
            }
            Message::CommitStoreBlob(RpcMessage { request, response }) => {
                let (query, created_at) = request;
                let timestamp = storage
                    .commit(query, created_at)
                    .map_err(|err| err.describe("committing to adding blob"))?;
                if let Err(err) = response.respond(timestamp) {
                    warn!("db actor failed to send response to actor: {}", err);
                }
            }
            Message::AbortStoreBlob(RpcMessage { request, response }) => {
                storage
                    .abort(request)
                    .map_err(|err| err.describe("aborting adding blob"))?;
                if let Err(err) = response.respond(()) {
                    warn!("db actor failed to send response to actor: {}", err);
                }
            }
            Message::GetBlob(RpcMessage { request, response }) => {
                let key = request;
                debug!("retrieving blob: key=\"{}\"", key);
                let result = storage.lookup(&key);
                if let Err(err) = response.respond(result) {
                    warn!("db actor failed to send response to actor: {}", err);
                }
            }
            Message::ContainsBlob(RpcMessage { request, response }) => {
                let key = request;
                debug!("checking if blob is stored: key=\"{}\"", key);
                let result = storage.contains(&key);
                if let Err(err) = response.respond(result) {
                    warn!("db actor failed to send response to actor: {}", err);
                }
            }
            Message::GetUncommittedBlob(RpcMessage { request, response }) => {
                let key = request;
                debug!("retrieving uncommitted blob: key=\"{}\"", key);
                let result = storage
                    .lookup_uncommitted(&key)
                    .ok_or_else(|| storage.lookup(&key));
                if let Err(err) = response.respond(result) {
                    warn!("db actor failed to send response to actor: {}", err);
                }
            }
            Message::GetStoreBlobQuery(RpcMessage { request, response }) => {
                let key = request;
                debug!("retrieving uncommitted store blob query: key=\"{}\"", key);
                let result = storage.get_store_blob_query(key);
                if let Err(err) = response.respond(result) {
                    warn!("db actor failed to send response to actor: {}", err);
                }
            }
            Message::GetKeys(RpcMessage { response, .. }) => {
                debug!("retrieving storage keys");
                let result = storage
                    .keys()
                    .map_err(|err| err.describe("retrieving stored keys"))?;
                if let Err(err) = response.respond(result) {
                    warn!("db actor failed to send response to actor: {}", err);
                }
            }
            Message::GetEntries(RpcMessage { response, .. }) => {
                debug!("retrieving index entries");
                let result = storage
                    .entries()
                    .map_err(|err| err.describe("retrieving index entries"))?;
                if let Err(err) = response.respond(result) {
                    warn!("db actor failed to send response to actor: {}", err);
                }
            }
            Message::SyncStoredBlob(RpcMessage { request, response }) => {
                let (view, created_at) = request;
                storage
                    .store_blob(view.as_slice(), created_at)
                    .map_err(|err| err.describe("syncing stored blob"))?;
                if let Err(err) = response.respond(view) {
                    warn!("db actor failed to send response to actor: {}", err);
                }
            }
            Message::SyncRemovedBlob(RpcMessage { request, response }) => {
                let (key, removed_at) = request;
                storage
                    .store_removed_blob(key, removed_at)
                    .map_err(|err| err.describe("syncing removed blob"))?;
                if let Err(err) = response.respond(()) {
                    warn!("db actor failed to send response to actor: {}", err);
                }
            }
            Message::RemoveBlob(RpcMessage { request, response }) => {
                let key = request;
                use RemoveResult::*;
                let result = match storage.remove_blob(key) {
                    Ok(query) => RemoveBlobResponse::Query(query),
                    NotStored(t) => RemoveBlobResponse::NotStored(t),
                };
                if let Err(err) = response.respond(result) {
                    warn!("db actor failed to send response to actor: {}", err);
                }
            }
            Message::CommitRemoveBlob(RpcMessage { request, response }) => {
                let (query, removed_at) = request;
                let removed_at = storage
                    .commit(query, removed_at)
                    .map_err(|err| err.describe("committing to removing blob"))?;
                if let Err(err) = response.respond(removed_at) {
                    warn!("db actor failed to send response to actor: {}", err);
                }
            }
            Message::AbortRemoveBlob(RpcMessage { request, response }) => {
                storage
                    .abort(request)
                    .map_err(|err| err.describe("aborting a remove blob operation"))?;
                // If the actor is disconnected this is not really a problem.
                if let Err(err) = response.respond(()) {
                    warn!("db actor failed to send response to actor: {}", err);
                }
            }
            Message::HealthCheck(RpcMessage { response, .. }) => {
                debug!("database health check");
                if let Err(err) = response.respond(HealthOk(())) {
                    warn!("db actor failed to send response to actor: {}", err);
                }
            }
        }
    }

    debug!("storage actor stopping");
    Ok(())
}

/// Message type send to the storage [`actor`].
#[derive(Debug)]
pub enum Message {
    /// Add a blob to the database, phase one of storing the blob.
    ///
    /// Request is the `BufView` that makes up the blob.
    ///
    /// Responds with a query to commit to storing the blob, or the key of the
    /// blob if the blob is already in the database. Also returns the original,
    /// unchanged `BufView` in `Message::AddBlob`.
    AddBlob(RpcMessage<BufView, (AddBlobResponse, BufView)>),
    /// Add a blob by streaming it to the database.
    ///
    /// Request is the length of the buffer to add.
    ///
    /// Responds with a [`StreamBlob`] which allows the blob to be stream to the
    /// data file directly.
    // NOTE: `StreamBlob` is rather large (272 bytes at the time of writing)
    // compare the other variants. Using a box here to reduce the in-balance a
    // bit.
    StreamBlob(RpcMessage<usize, Box<StreamBlob>>),
    /// Add a streamed blob to the database.
    ///
    /// Request is a filled [`StreamBlob`].
    ///
    /// Returns the same thing as [`Message::AddBlob`].
    AddStreamedBlob(RpcMessage<Box<StreamBlob>, AddBlobResponse>),
    /// Commit to a blob being stored, phase two of storing the blob.
    ///
    /// Request is the query to store the blob, returned by [`Message::AddBlob`].
    CommitStoreBlob(RpcMessage<(StoreBlob, SystemTime), SystemTime>),
    /// Abort storing a blob.
    ///
    /// Request is the query to abort, returned by [`Message::AddBlob`].
    AbortStoreBlob(RpcMessage<StoreBlob, ()>),

    /// Get a blob from storage.
    ///
    /// Request is the blob's key to look up.
    ///
    /// Responds with the `Blob`, if its in the database.
    GetBlob(RpcMessage<Key, Option<BlobEntry>>),
    /// Check is a blob is stored.
    ///
    /// Request is the blob's key to look up.
    ///
    /// Responds with `true`, if the blob is in the database.
    ContainsBlob(RpcMessage<Key, bool>),

    /// Get an uncommitted blob from storage.
    ///
    /// Request is the key to look up.
    ///
    /// Responds with the `UncommittedBlob`, if its in the database, or tries the
    /// committed blobs, returning the same thing as [`GetBlob`].
    ///
    /// [`GetBlob`]: Message::GetBlob
    GetUncommittedBlob(RpcMessage<Key, Result<UncommittedBlob, Option<BlobEntry>>>),
    /// Get an uncommitted [`StoreBlob`] query from storage.
    ///
    /// Request is the key of the blob for the query,
    ///
    /// Responds with `Err(())` if the blob is already stored (*not removed*),
    /// `Ok(Some(query))` if a uncommitted query is found and `Ok(None)` if no
    /// query is found.
    GetStoreBlobQuery(RpcMessage<Key, Result<Option<StoreBlob>, BlobAlreadyStored>>),

    /// Get the keys currently stored in the database. Used by the
    /// synchronisation process.
    ///
    /// Responds with [`Keys`], an iterator over all [`Key`]s stored **at the
    /// moment the request is made**.
    ///
    /// # Notes
    ///
    /// The returned keys can easily become outdated with concurrent
    /// store/remove operations.
    GetKeys(RpcMessage<(), Keys>),
    /// Get all entries currently in the index. Used by the synchronisation
    /// process.
    ///
    /// Responds with [`Entries`], an iterator over all index [`Entry`]s stored
    /// **at the moment the request is made**.
    ///
    /// # Notes
    ///
    /// The returned entries can easily become outdated with concurrent
    /// store/remove operations.
    GetEntries(RpcMessage<(), Entries>),
    /// Store and commit a blob to storage, used by the synchronisation process.
    ///
    /// Request is the [`Key`], the blob (must match the key) and the timestamp
    /// at which it was committed.
    ///
    /// Returns the [`BufView`] unchanged.
    SyncStoredBlob(RpcMessage<(BufView, SystemTime), BufView>),
    /// Store and commit to a removed blob, used by the synchronisation process.
    ///
    /// Request is the [`Key`]  and the timestamp at which the blob was removed.
    SyncRemovedBlob(RpcMessage<(Key, SystemTime), ()>),

    /// Remove a blob from the database.
    ///
    /// Request is the key for the blob to remove.
    ///
    /// Responds with a query to commit to removing the blob, or
    RemoveBlob(RpcMessage<Key, RemoveBlobResponse>),
    /// Commit to a blob being removed.
    ///
    /// Request is the query to remove the blob, returned by
    /// [`Message::RemoveBlob`].
    CommitRemoveBlob(RpcMessage<(RemoveBlob, SystemTime), SystemTime>),
    /// Abort removing of a blob.
    ///
    /// Request is the query to abort, returned by [`Message::RemoveBlob`].
    AbortRemoveBlob(RpcMessage<RemoveBlob, ()>),

    /// Check if the database actor is running.
    HealthCheck(RpcMessage<HealthCheck, HealthOk>),
}

/// Message to check if the database actor is running.
#[derive(Debug)]
pub struct HealthCheck;

/// Message returned to [`HealthCheck`].
#[derive(Debug)]
pub struct HealthOk(());

from_message!(Message::AddBlob(BufView) -> (AddBlobResponse, BufView));
from_message!(Message::CommitStoreBlob(StoreBlob, SystemTime) -> SystemTime);
from_message!(Message::AbortStoreBlob(StoreBlob) -> ());
from_message!(Message::StreamBlob(usize) -> Box<StreamBlob>);
from_message!(Message::AddStreamedBlob(Box<StreamBlob>) -> AddBlobResponse);
from_message!(Message::GetBlob(Key) -> Option<BlobEntry>);
from_message!(Message::ContainsBlob(Key) -> bool);
from_message!(Message::GetUncommittedBlob(Key) -> Result<UncommittedBlob, Option<BlobEntry>>);
from_message!(Message::GetStoreBlobQuery(Key) -> Result<Option<StoreBlob>, BlobAlreadyStored>);
from_message!(Message::GetKeys(()) -> Keys);
from_message!(Message::GetEntries(()) -> Entries);
from_message!(Message::SyncStoredBlob(BufView, SystemTime) -> BufView);
from_message!(Message::SyncRemovedBlob(Key, SystemTime) -> ());
from_message!(Message::RemoveBlob(Key) -> RemoveBlobResponse);
from_message!(Message::CommitRemoveBlob(RemoveBlob, SystemTime) -> SystemTime);
from_message!(Message::AbortRemoveBlob(RemoveBlob) -> ());
from_message!(Message::HealthCheck(HealthCheck) -> HealthOk);

/// Response to [`Message::AddBlob`].
#[derive(Debug)]
pub enum AddBlobResponse {
    /// Query to commit to adding a blob.
    Query(StoreBlob),
    /// The blob is already stored.
    AlreadyStored(Key),
}

/// Response to [`Message::RemoveBlob`].
#[derive(Debug)]
pub enum RemoveBlobResponse {
    /// Query to commit to removing a blob.
    Query(RemoveBlob),
    /// The blob is not stored, either it was never stored or it was already
    /// removed.
    /// This is `None` if the blob was never present, or `Some` if it was
    /// removed already.
    NotStored(Option<SystemTime>),
}

/// Return an error to use when the database has failed.
pub fn db_error() -> crate::Error {
    io::Error::from(io::ErrorKind::NotConnected).describe("database actor failed")
}
