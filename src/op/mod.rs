//! Module with functions for the supported operations.

use std::future::Future;
use std::pin::Pin;
use std::task::{self, Poll};
use std::time::{Duration, SystemTime};

use heph::actor_ref::rpc::{Rpc, RpcMessage};
use heph::rt::RuntimeAccess;
use heph::timer::Timer;
use heph::{actor, ActorRef};
use log::{debug, error};

use crate::db::{self, HealthCheck, HealthOk};
use crate::peer::coordinator::relay;
use crate::peer::{ConsensusId, PeerRpc};
use crate::storage::{BlobEntry, UncommittedBlob};
use crate::Key;

pub mod remove;
pub mod store;

#[doc(inline)]
pub use remove::remove_blob;
#[doc(inline)]
pub use store::store_blob;

/// Maximum number of tries we will attempt to run the consensus algorithm.
const MAX_CONSENSUS_TRIES: usize = 3;

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

/// Checks if a blob with `key` is stored.
///
/// Returns an error if the database actor can't be accessed.
pub async fn contains_blob<M>(
    ctx: &mut actor::Context<M>,
    db_ref: &mut ActorRef<db::Message>,
    key: Key,
) -> Result<bool, ()> {
    debug!("checking if blob is stored: key={}", key);
    match db_rpc(ctx, db_ref, key) {
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

/// Await the results in `abort_rpc`, logging the results.
async fn abort_consensus(abort_rpc: PeerRpc<()>, consensus_id: ConsensusId, key: &Key) {
    let results = abort_rpc.await;
    let (committed, aborted, failed) = count_consensus_votes(&results);
    error!(
        "aborted consensus algorithm: consensus_id={}, key={}, success={}, failed={}",
        consensus_id,
        key,
        committed,
        aborted + failed,
    );
}

/// Returns the `(committed, aborted, failed)` votes.
fn count_consensus_votes<T>(results: &[Result<T, relay::Error>]) -> (usize, usize, usize) {
    let mut commit = 1; // Coordinator always votes to commit.
    let mut abort = 0;
    let mut failed = 0;

    for result in results {
        match result {
            Ok(..) => commit += 1,
            Err(relay::Error::Abort) => abort += 1,
            Err(relay::Error::Failed) => failed += 1,
        }
    }

    (commit, abort, failed)
}

/// Select the timestamp to use from consensus results.
fn select_timestamp(results: &[Result<SystemTime, relay::Error>]) -> SystemTime {
    let mut timestamp = SystemTime::now();

    for result in results {
        if let Ok(peer_timestamp) = result {
            // FIXME: sync the time somehow? To ensure that if this peer has an
            // incorrect time we don't use that? We using the largest timestamp
            // (the most in the future), but we don't want a time in the year
            // 2100 because a peer's time is incorrect.

            if *peer_timestamp > timestamp {
                timestamp = *peer_timestamp;
            }
        }
    }

    timestamp
}
