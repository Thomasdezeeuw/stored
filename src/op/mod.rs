//! Supported Stored operations.

use std::future::Future;
use std::pin::Pin;
use std::task::{self, Poll};
use std::time::SystemTime;

use heph::actor;
use heph::actor_ref::{ActorRef, Rpc, RpcMessage};
use heph::rt::RuntimeAccess;
use heph::timer::Timer;
use log::{debug, error};

use crate::buffer::BufView;
use crate::db::{self, HealthCheck, HealthOk};
use crate::passport::{Event, Passport, Uuid};
use crate::storage::{BlobAlreadyStored, BlobEntry, Entries, Keys, StoreBlob, UncommittedBlob};
use crate::{timeout, Key};

mod consensus;
mod remove;
mod store;
mod sync;

#[doc(inline)]
pub(crate) use remove::prep_remove_blob;
#[doc(inline)]
pub use remove::remove_blob;
#[doc(inline)]
pub(crate) use store::add_blob;
#[doc(inline)]
pub use store::{store_blob, store_streaming_blob, StreamResult};
#[doc(inline)]
pub use sync::{full_sync, peer_sync};

pub(crate) use consensus::{abort_query, commit_query, consensus, Query};
pub(crate) use store::stream_add_blob;

/// Outcome of a successful operation. E.g. when trying to add a blob, but the
/// blob is already stored.
pub(crate) enum Outcome<T, U> {
    /// Continue like normal.
    Continue(T),
    /// We're done early.
    Done(U),
}

/// Retrieve the blob with `key`.
///
/// Returns an error if the database actor can't be accessed.
pub async fn retrieve_blob<M, K>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    passport: &mut Passport,
    key: Key,
) -> Result<Option<BlobEntry>, ()>
where
    actor::Context<M, K>: RuntimeAccess,
{
    debug!(
        "running retrieve operation: request_id=\"{}\", key=\"{}\"",
        passport.id(),
        key
    );
    match db_rpc(ctx, db_ref, *passport.id(), key) {
        Ok(rpc) => match rpc.await {
            Ok(blob) => {
                passport.mark(Event::RetrievedBlob);
                Ok(blob)
            }
            Err(()) => {
                passport.mark(Event::FailedToRetrieveBlob);
                Err(())
            }
        },
        Err(()) => {
            passport.mark(Event::FailedToRetrieveBlob);
            Err(())
        }
    }
}

/// Check if the blob with `key` is stored.
///
/// Returns an error if the database actor can't be accessed.
pub async fn contains_blob<M, K>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    passport: &mut Passport,
    key: Key,
) -> Result<bool, ()>
where
    actor::Context<M, K>: RuntimeAccess,
{
    debug!(
        "running contains key operation: request_id=\"{}\", key=\"{}\"",
        passport.id(),
        key
    );
    match db_rpc(ctx, db_ref, *passport.id(), key) {
        Ok(rpc) => match rpc.await {
            Ok(contains) => {
                passport.mark(Event::ContainsBlob);
                Ok(contains)
            }
            Err(()) => {
                passport.mark(Event::FailedToCheckIfContainsBlob);
                Err(())
            }
        },
        Err(()) => {
            passport.mark(Event::FailedToCheckIfContainsBlob);
            Err(())
        }
    }
}

/// Retrieve a possibly uncommitted blob with `key`.
///
/// Returns an error if the database actor can't be accessed.
pub(crate) async fn retrieve_uncommitted_blob<M, K>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    passport: &mut Passport,
    key: Key,
) -> Result<Result<UncommittedBlob, Option<BlobEntry>>, ()>
where
    actor::Context<M, K>: RuntimeAccess,
{
    debug!(
        "running uncommitted retrieve operation: request_id=\"{}\", key=\"{}\"",
        passport.id(),
        key
    );
    match db_rpc(ctx, db_ref, *passport.id(), key) {
        Ok(rpc) => match rpc.await {
            Ok(blob) => {
                passport.mark(Event::RetrievedUncommittedBlob);
                Ok(blob)
            }
            Err(()) => {
                passport.mark(Event::FailedToRetrieveUncommittedBlob);
                Err(())
            }
        },
        Err(()) => {
            passport.mark(Event::FailedToRetrieveUncommittedBlob);
            Err(())
        }
    }
}

/// Retrieve a uncommitted store blob query.
///
/// Returns an error if the database actor can't be accessed.
pub(crate) async fn retrieve_store_blob_query<M, K>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    passport: &mut Passport,
    key: Key,
) -> Result<Result<Option<StoreBlob>, BlobAlreadyStored>, ()>
where
    actor::Context<M, K>: RuntimeAccess,
{
    debug!(
        "running retrieve uncommitted store query operation: request_id=\"{}\", key=\"{}\"",
        passport.id(),
        key
    );
    match db_rpc(ctx, db_ref, *passport.id(), key) {
        Ok(rpc) => match rpc.await {
            Ok(result) => {
                passport.mark(Event::RetrievedStoreBlobQuery);
                Ok(result)
            }
            Err(()) => {
                passport.mark(Event::FailedToRetrieveStoreBlobQuery);
                Err(())
            }
        },
        Err(()) => {
            passport.mark(Event::FailedToRetrieveStoreBlobQuery);
            Err(())
        }
    }
}

/// Retrieves all keys currently stored.
///
/// Returns an error if the database actor can't be accessed.
pub(crate) async fn retrieve_keys<M, K>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    passport: &mut Passport,
) -> Result<Keys, ()>
where
    actor::Context<M, K>: RuntimeAccess,
{
    debug!(
        "running retrieve keys operation: request_id=\"{}\"",
        passport.id()
    );
    match db_rpc(ctx, db_ref, *passport.id(), ()) {
        Ok(rpc) => match rpc.await {
            Ok(keys) => {
                passport.mark(Event::RetrievedKeys);
                Ok(keys)
            }
            Err(()) => {
                passport.mark(Event::FailedToRetrieveKeys);
                Err(())
            }
        },
        Err(()) => {
            passport.mark(Event::FailedToRetrieveKeys);
            Err(())
        }
    }
}

/// Retrieves all index entries currently stored.
///
/// Returns an error if the database actor can't be accessed.
pub(crate) async fn retrieve_entries<M, K>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    passport: &mut Passport,
) -> Result<Entries, ()>
where
    actor::Context<M, K>: RuntimeAccess,
{
    debug!(
        "running retrieve index entries operation: request_id=\"{}\"",
        passport.id()
    );
    match db_rpc(ctx, db_ref, *passport.id(), ()) {
        Ok(rpc) => match rpc.await {
            Ok(keys) => {
                passport.mark(Event::RetrievedEntries);
                Ok(keys)
            }
            Err(()) => {
                passport.mark(Event::FailedToRetrieveEntries);
                Err(())
            }
        },
        Err(()) => {
            passport.mark(Event::FailedToRetrieveEntries);
            Err(())
        }
    }
}

/// Runs a health check on the database.
///
/// Returns an error if the database actor can't be accessed.
pub async fn check_health<M, K>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    passport: &mut Passport,
) -> Result<HealthOk, ()>
where
    actor::Context<M, K>: RuntimeAccess,
{
    debug!("running health check: request_id=\"{}\"", passport.id());
    match db_rpc(ctx, db_ref, *passport.id(), HealthCheck) {
        Ok(rpc) => match rpc.await {
            Ok(result) => {
                passport.mark(Event::HealthCheckComplete);
                Ok(result)
            }
            Err(()) => {
                passport.mark(Event::HealthCheckFailed);
                Err(())
            }
        },
        Err(()) => {
            passport.mark(Event::HealthCheckFailed);
            Err(())
        }
    }
}

/// Store the blob in `view` at `timestamp`.
pub(crate) async fn sync_stored_blob<M, K>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    passport: &mut Passport,
    view: BufView,
    timestamp: SystemTime,
) -> Result<BufView, ()>
where
    actor::Context<M, K>: RuntimeAccess,
{
    debug!(
        "syncing stored blob: request_id=\"{}\", blob_length={}",
        passport.id(),
        view.len()
    );
    match db_rpc(ctx, db_ref, *passport.id(), (view, timestamp)) {
        Ok(rpc) => match rpc.await {
            Ok(view) => {
                passport.mark(Event::SyncedStoredBlob);
                Ok(view)
            }
            Err(()) => {
                passport.mark(Event::FailedToSyncStoredBlob);
                Err(())
            }
        },
        Err(()) => {
            passport.mark(Event::FailedToSyncStoredBlob);
            Err(())
        }
    }
}

/// Store the blob with `key` and removed `timestamp`.
pub(crate) async fn sync_removed_blob<M, K>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    passport: &mut Passport,
    key: Key,
    timestamp: SystemTime,
) -> Result<(), ()>
where
    actor::Context<M, K>: RuntimeAccess,
{
    debug!(
        "syncing removed blob: request_id=\"{}\", key=\"{}\"",
        passport.id(),
        key
    );
    match db_rpc(ctx, db_ref, *passport.id(), (key, timestamp)) {
        Ok(rpc) => match rpc.await {
            Ok(()) => {
                passport.mark(Event::SyncedRemovedBlob);
                Ok(())
            }
            Err(()) => {
                passport.mark(Event::FailedToSyncRemovedBlob);
                Err(())
            }
        },
        Err(()) => {
            passport.mark(Event::FailedToSyncRemovedBlob);
            Err(())
        }
    }
}

/// Make a RPC to the database (`db_ref`) applying the `timeout::DB`.
fn db_rpc<M, K, Req, Res>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    request_id: Uuid,
    request: Req,
) -> Result<DbRpc<Res>, ()>
where
    db::Message: From<RpcMessage<Req, Res>>,
    actor::Context<M, K>: RuntimeAccess,
{
    match db_ref.rpc(ctx, request) {
        Ok(rpc) => Ok(DbRpc {
            rpc,
            timer: Timer::timeout(ctx, timeout::DB),
            request_id,
        }),
        Err(err) => {
            error!(
                "error making RPC call to database: {}: request_id=\"{}\"",
                err, request_id
            );
            Err(())
        }
    }
}

/// Wrapper around [`Rpc`] that adds a [`timeout::DB`], logging if it hits any
/// errors.
///
/// See [`db_rpc`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
#[derive(Debug)]
struct DbRpc<Res> {
    rpc: Rpc<Res>,
    timer: Timer,
    request_id: Uuid,
}

/* TODO: change Result::Err for `DbRpc` to:
 * enum DbError {
 *  TimedOut,
 *  Unavailable,
 * }
 */

impl<Res> Future for DbRpc<Res> {
    /// Returns an error if the database doesn't respond.
    type Output = Result<Res, ()>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut task::Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.rpc).poll(ctx) {
            Poll::Ready(Ok(res)) => Poll::Ready(Ok(res)),
            Poll::Ready(Err(err)) => {
                error!(
                    "error waiting for RPC response from database: {}: request_id=\"{}\"",
                    err, self.request_id
                );
                Poll::Ready(Err(()))
            }
            Poll::Pending => Pin::new(&mut self.timer).poll(ctx).map(|_| {
                error!(
                    "timeout waiting for RPC response from database: request_id=\"{}\"",
                    self.request_id
                );
                Err(())
            }),
        }
    }
}
