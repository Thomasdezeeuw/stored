//! Module with the database actor.
//!
//! The [`db::actor`] is the main type, which accepts [`db::Message`]s and is
//! supervised by [`db::Supervisor`].
//!
//! [`db::actor`]: actor
//! [`db::Message`]: Message
//! [`db::Supervisor`]: Supervisor

use std::io;
use std::path::Path;
use std::time::SystemTime;

use heph::actor::sync::{SyncActor, SyncContext};
use heph::actor_ref::RpcMessage;
use heph::supervisor::{SupervisorStrategy, SyncSupervisor};
use log::{debug, error, info};

use crate::storage::{AddBlob, AddResult, Blob, Storage};
use crate::{Buffer, Key};

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
    A: SyncActor<Argument = Storage, Error = io::Error>,
{
    fn decide(&mut self, err: io::Error) -> SupervisorStrategy<Storage> {
        error!("error operating on database: {}", err);
        info!("attempting to reopen database");
        match Storage::open(&self.0) {
            Ok(storage) => {
                info!("successfully reopened database, restarting database actor");
                SupervisorStrategy::Restart(storage)
            }
            Err(err) => {
                // FIXME: shutdown the entire server somehow? Maybe by sending
                // the TCP server a shutdown message?
                error!(
                    "failed to reopen database, not restarting database actor: {}",
                    err
                );
                SupervisorStrategy::Stop
            }
        }
    }
}

/// Message type send to the storage [`actor`].
pub enum Message {
    /// Add a blob to the database.
    ///
    /// Request is the `Buffer`, of which `length` (usize) bytes are used, that
    /// makes up the blob.
    ///
    /// Responds with  a query to commit to adding the blob, or the key of the
    /// blob if the blob is already in the database.
    ///
    /// # Panics
    ///
    /// This will panic if the `length` is larger then the bytes in the
    /// `Buffer`.
    AddBlob(RpcMessage<(Buffer, usize), AddBlobResponse>),

    /// Commit to a blob being added.
    ///
    /// Request is the query to add the blob, returned by [`Message::AddBlob`].
    ///
    /// Responds with the `Key` of the added blob.
    CommitBlob(RpcMessage<AddBlob, Key>),

    /// Get a blob from storage.
    ///
    /// Request is the key to look up.
    ///
    /// Responds with the `Blob`, if its in the database.
    GetBlob(RpcMessage<Key, Option<Blob>>),

    /// Check if the database actor is running.
    HealthCheck(RpcMessage<HealthCheck, HealthOk>),
}

/// Message to check if the database actor is running.
pub struct HealthCheck;

/// Message returned to [`HealthCheck`].
pub struct HealthOk(());

impl From<RpcMessage<(Buffer, usize), AddBlobResponse>> for Message {
    fn from(msg: RpcMessage<(Buffer, usize), AddBlobResponse>) -> Message {
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

impl From<RpcMessage<HealthCheck, HealthOk>> for Message {
    fn from(msg: RpcMessage<HealthCheck, HealthOk>) -> Message {
        Message::HealthCheck(msg)
    }
}

/// Response to [`Message::AddBlob`].
pub enum AddBlobResponse {
    /// Query to commit to adding a blob. And the original, unchanged `Buffer`
    /// in `Message::AddBlob`.
    Query(AddBlob, Buffer),
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
                let (blob, length) = request;
                debug!("adding new blob: size={}", length);
                use AddResult::*;
                let result = match storage.add_blob(&blob.as_bytes()[..length]) {
                    Ok(query) => AddBlobResponse::Query(query, blob),
                    AlreadyPresent(key) => AddBlobResponse::AlreadyPresent(key),
                    Err(err) => return Result::Err(err),
                };
                // If the actor is disconnected this is not really a problem.
                let _ = response.respond(result);
            }
            Message::CommitBlob(RpcMessage { request, response }) => {
                let created_at = SystemTime::now();
                let key = storage.commit(request, created_at)?;
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
            Message::HealthCheck(RpcMessage { response, .. }) => {
                debug!("database health check");
                // If the actor is disconnected this is not really a problem.
                let _ = response.respond(HealthOk(()));
            }
        }
    }

    debug!("storage actor stopping");
    Ok(())
}
