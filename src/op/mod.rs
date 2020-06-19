//! Module with functions for the supported operations.

use std::future::Future;
use std::pin::Pin;
use std::task::{self, Poll};
use std::time::Duration;

use heph::actor_ref::rpc::{Rpc, RpcMessage};
use heph::rt::RuntimeAccess;
use heph::timer::Timer;
use heph::{actor, ActorRef};
use log::{debug, error};

use crate::db::{self, HealthCheck, HealthOk};
use crate::storage::{BlobEntry, UncommittedBlob};
use crate::Key;

pub mod store;

#[doc(inline)]
pub use store::store_blob;

/// Retrieve the blob with `key`.
///
/// Returns an error if the database actor can't be accessed.
pub async fn retrieve_blob<M>(
    ctx: &mut actor::Context<M>,
    db_ref: &mut ActorRef<db::Message>,
    key: Key,
) -> Result<Option<BlobEntry>, ()> {
    debug!("running retrieve operation");
    match db_rpc(ctx, db_ref, key) {
        Ok(rpc) => rpc.await,
        Err(err) => Err(err),
    }
}

/// Retrieve a possibly uncommitted blob with `key`.
///
/// Returns an error if the database actor can't be accessed.
pub async fn retrieve_uncommitted_blob<M, K>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    key: Key,
) -> Result<Result<UncommittedBlob, Option<BlobEntry>>, ()>
where
    K: RuntimeAccess,
{
    debug!("running uncommitted retrieve operation");
    match db_rpc(ctx, db_ref, key) {
        Ok(rpc) => rpc.await,
        Err(err) => Err(err),
    }
}

/// Runs a health check on the database.
///
/// Returns an error if the database actor can't be accessed.
pub async fn check_health<M>(
    ctx: &mut actor::Context<M>,
    db_ref: &mut ActorRef<db::Message>,
) -> Result<HealthOk, ()> {
    debug!("running health check");
    match db_rpc(ctx, db_ref, HealthCheck) {
        Ok(rpc) => rpc.await,
        Err(err) => Err(err),
    }
}

/// Timeout for connecting to the database.
// TODO: base this on something.
const DB_TIMEOUT: Duration = Duration::from_secs(1);

/// Make a RPC to the database (`db_ref`) applying the `DB_TIMEOUT`.
fn db_rpc<M, K, Req, Res>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    request: Req,
) -> Result<DbRpc<Res>, ()>
where
    db::Message: From<RpcMessage<Req, Res>>,
    K: RuntimeAccess,
{
    match db_ref.rpc(ctx, request) {
        Ok(rpc) => Ok(DbRpc {
            rpc,
            timer: Timer::timeout(ctx, DB_TIMEOUT),
        }),
        Err(err) => {
            error!("error making RPC call to database: {}", err);
            Err(())
        }
    }
}

/// Wrapper around [`Rpc`] that adds a [`DB_TIMEOUT`], logging if it hits any
/// errors.
///
/// See [`db_rpc`].
struct DbRpc<Res> {
    rpc: Rpc<Res>,
    timer: Timer,
}

impl<Res> Future for DbRpc<Res> {
    /// Returns an error if the database doesn't respond.
    type Output = Result<Res, ()>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut task::Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.rpc).poll(ctx) {
            Poll::Ready(Ok(res)) => Poll::Ready(Ok(res)),
            Poll::Ready(Err(err)) => {
                error!("error waiting for RPC response from database: {}", err);
                Poll::Ready(Err(()))
            }
            Poll::Pending => Pin::new(&mut self.timer).poll(ctx).map(|_| {
                error!("timeout waiting for RPC response from database");
                Err(())
            }),
        }
    }
}
