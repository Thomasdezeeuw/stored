//! Module with the database actor.
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

use heph::actor::sync::{SyncActor, SyncContext};
use heph::actor_ref::{ActorRef, RpcMessage};
use heph::supervisor::{SupervisorStrategy, SyncSupervisor};
use heph::{rt, Runtime};
use log::{debug, error, info, trace, warn};

use crate::error::Describe;
use crate::storage::{
    AddResult, BlobEntry, RemoveBlob, RemoveResult, Storage, StoreBlob, UncommittedBlob,
};
use crate::{Buffer, Key};

/// Start the database actor.
pub fn start(
    runtime: &mut Runtime,
    path: Box<Path>,
) -> crate::Result<ActorRef<Message>, rt::Error<io::Error>> {
    let storage =
        Storage::open(&*path).map_err(|err| rt::Error::from(err).describe("opening database"))?;
    let supervisor = Supervisor::new(path);
    runtime
        .spawn_sync_actor(supervisor, actor as fn(_, _) -> _, storage)
        .map_err(|err| err.map_type().describe("spawning database actor"))
}

/// Supervisor for the [`db::actor`].
///
/// [`db::actor`]: crate::db::actor
///
/// It logs the error and tries to reopen the database, restarting the actor if
/// successful.
pub struct Supervisor(Box<Path>);

impl Supervisor {
    /// Create a new `DbSupervisor`.
    pub const fn new(path: Box<Path>) -> Supervisor {
        Supervisor(path)
    }
}

impl<A> SyncSupervisor<A> for Supervisor
where
    A: SyncActor<Argument = Storage, Error = crate::Error>,
{
    fn decide(&mut self, err: crate::Error) -> SupervisorStrategy<Storage> {
        error!("error operating on database: {}", err);
        info!("attempting to reopen database: path={}", self.0.display());
        match Storage::open(&self.0) {
            Ok(storage) => {
                info!(
                    "successfully reopened database, restarting database actor: path={}",
                    self.0.display()
                );
                SupervisorStrategy::Restart(storage)
            }
            Err(err) => {
                // FIXME: shutdown the entire server somehow? Maybe by sending
                // the TCP server a shutdown message?
                error!(
                    "failed to reopen database, not restarting database actor: {}: path={}",
                    err,
                    self.0.display(),
                );
                SupervisorStrategy::Stop
            }
        }
    }
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
                let (blob, length) = request;
                use AddResult::*;
                let result = match storage.add_blob(&blob.as_bytes()[..length]) {
                    Ok(query) => (AddBlobResponse::Query(query), blob),
                    AlreadyStored(key) => (AddBlobResponse::AlreadyStored(key), blob),
                    Err(err) => return Result::Err(err.describe("adding a blob")),
                };
                if let Result::Err(err) = response.respond(result) {
                    warn!("db actor failed to send response to actor: {}", err);
                }
            }
            Message::CommitStoreBlob(RpcMessage { request, response }) => {
                let (query, created_at) = request;
                storage
                    .commit(query, created_at)
                    .map_err(|err| err.describe("committing to adding blob"))?;
                if let Err(err) = response.respond(()) {
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
                debug!("retrieving blob: key={}", key);
                let result = storage.lookup(&key);
                if let Err(err) = response.respond(result) {
                    warn!("db actor failed to send response to actor: {}", err);
                }
            }
            Message::GetUncommittedBlob(RpcMessage { request, response }) => {
                let key = request;
                debug!("retrieving uncommitted blob: key={}", key);
                let result = storage
                    .lookup_uncommitted(&key)
                    .ok_or_else(|| storage.lookup(&key));
                if let Err(err) = response.respond(result) {
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
    /// Request is the `Buffer`, of which `length` (usize) bytes are used, that
    /// makes up the blob.
    ///
    /// Responds with a query to commit to storing the blob, or the key of the
    /// blob if the blob is already in the database. Also returns the original,
    /// unchanged `Buffer` in `Message::AddBlob`.
    ///
    /// # Panics
    ///
    /// This will panic if the `length` is larger then the bytes in the
    /// `Buffer`.
    AddBlob(RpcMessage<(Buffer, usize), (AddBlobResponse, Buffer)>),
    /// Commit to a blob being stored, phase two of storing the blob.
    ///
    /// Request is the query to store the blob, returned by [`Message::AddBlob`].
    CommitStoreBlob(RpcMessage<(StoreBlob, SystemTime), ()>),
    /// Abort storing a blob.
    ///
    /// Request is the query to abort, returned by [`Message::AddBlob`].
    AbortStoreBlob(RpcMessage<StoreBlob, ()>),

    /// Get a blob from storage.
    ///
    /// Request is the key to look up.
    ///
    /// Responds with the `Blob`, if its in the database.
    GetBlob(RpcMessage<Key, Option<BlobEntry>>),

    /// Get an uncommitted blob from storage.
    ///
    /// Request is the key to look up.
    ///
    /// Responds with the `UncommittedBlob`, if its in the database, or tries the
    /// committed blobs, returning the same thing as [`GetBlob`].
    ///
    /// [`GetBlob`]: Message::GetBlob
    GetUncommittedBlob(RpcMessage<Key, Result<UncommittedBlob, Option<BlobEntry>>>),

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

/// Macro to implement [`From`]`<`[`RpcMessage`]`>` for an enum message type.
// TODO: maybe add something like this to Heph?
macro_rules! from_rpc_message {
    // Single field.
    ($name: ident :: $variant: ident ( $ty: ty ) -> $return_ty: ty) => {
        impl From<RpcMessage<$ty, $return_ty>> for $name {
            fn from(msg: RpcMessage<$ty, $return_ty>) -> $name {
                $name::$variant(msg)
            }
        }
    };
    // For multiple fields use the tuple format.
    ($name: ident :: $variant: ident ( $( $ty: ty ),+ ) -> $return_ty: ty) => {
        impl From<RpcMessage<( $( $ty ),+ ), $return_ty>> for $name {
            fn from(msg: RpcMessage<( $( $ty ),+ ), $return_ty>) -> $name {
                $name::$variant(msg)
            }
        }
    };
}

from_rpc_message!(Message::AddBlob(Buffer, usize) -> (AddBlobResponse, Buffer));
from_rpc_message!(Message::CommitStoreBlob(StoreBlob, SystemTime) -> ());
from_rpc_message!(Message::AbortStoreBlob(StoreBlob) -> ());
from_rpc_message!(Message::GetBlob(Key) -> Option<BlobEntry>);
from_rpc_message!(Message::GetUncommittedBlob(Key) -> Result<UncommittedBlob, Option<BlobEntry>>);
from_rpc_message!(Message::RemoveBlob(Key) -> RemoveBlobResponse);
from_rpc_message!(Message::CommitRemoveBlob(RemoveBlob, SystemTime) -> SystemTime);
from_rpc_message!(Message::AbortRemoveBlob(RemoveBlob) -> ());
from_rpc_message!(Message::HealthCheck(HealthCheck) -> HealthOk);

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
