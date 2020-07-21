//! Module with functions for the supported operations.

use std::future::Future;
use std::pin::Pin;
use std::task::{self, Poll};
use std::time::{Duration, SystemTime};

use heph::actor_ref::rpc::{Rpc, RpcMessage};
use heph::rt::RuntimeAccess;
use heph::timer::Timer;
use heph::{actor, ActorRef};
use log::{debug, error, warn};

use crate::buffer::BufView;
use crate::db::{self, HealthCheck, HealthOk};
use crate::peer::coordinator::relay;
use crate::peer::{ConsensusId, PeerRpc, Peers};
use crate::storage::{self, BlobEntry, Keys, UncommittedBlob};
use crate::Key;

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
pub use store::store_blob;
#[doc(inline)]
pub use sync::full_sync;

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
pub async fn retrieve_blob<M, K>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    key: Key,
) -> Result<Option<BlobEntry>, ()>
where
    K: RuntimeAccess,
{
    debug!("running retrieve operation");
    match db_rpc(ctx, db_ref, key) {
        Ok(rpc) => rpc.await,
        Err(()) => Err(()),
    }
}

/// Retrieve a possibly uncommitted blob with `key`.
///
/// Returns an error if the database actor can't be accessed.
pub(crate) async fn retrieve_uncommitted_blob<M, K>(
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
        Err(()) => Err(()),
    }
}

/// Retrieves all keys currently stored.
///
/// Returns an error if the database actor can't be accessed.
pub(crate) async fn retrieve_keys<M, K>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
) -> Result<Keys, ()>
where
    K: RuntimeAccess,
{
    debug!("running retrieve keys operation");
    match db_rpc(ctx, db_ref, ()) {
        Ok(rpc) => rpc.await,
        Err(()) => Err(()),
    }
}

/// Runs a health check on the database.
///
/// Returns an error if the database actor can't be accessed.
pub async fn check_health<M, K>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
) -> Result<HealthOk, ()>
where
    K: RuntimeAccess,
{
    debug!("running health check");
    match db_rpc(ctx, db_ref, HealthCheck) {
        Ok(rpc) => rpc.await,
        Err(()) => Err(()),
    }
}

/// Store the blob in `view` at `timestamp`.
pub(crate) async fn sync_stored_blob<M, K>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    view: BufView,
    timestamp: SystemTime,
) -> Result<BufView, ()>
where
    K: RuntimeAccess,
{
    debug!("syncing stored blob");
    match db_rpc(ctx, db_ref, (view, timestamp)) {
        Ok(rpc) => rpc.await,
        Err(()) => Err(()),
    }
}

/// Store the blob with `key` and removed `timestamp`.
pub(crate) async fn sync_removed_blob<M, K>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    key: Key,
    timestamp: SystemTime,
) -> Result<(), ()>
where
    K: RuntimeAccess,
{
    debug!("syncing removed blob");
    match db_rpc(ctx, db_ref, (key, timestamp)) {
        Ok(rpc) => rpc.await,
        Err(()) => Err(()),
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
#[must_use = "futures do nothing unless you `.await` or poll them"]
#[derive(Debug)]
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

pub(crate) trait Query: storage::Query {
    type AlreadyDone: Future<Output = Result<Option<SystemTime>, ()>>;

    /// Check in between consensus attempt to ensure the operation isn't already
    /// complete by another client. For example when running the consensus
    /// algorithm to store a blob this method should check if the blob is not
    /// already stored.
    ///
    /// If this returns `Ok(None)` the consensus algorithm will proceed.
    ///
    /// If this return `Ok(Some(..))` it means the operation was completed by
    /// another actor, the [`consensus`] function will return timestamp and
    /// consider it a success, e.g. when storing a blob and the blob is already
    /// stored `consensus` will return the result returned by this method.
    ///
    /// If an error is returned it will also be returned by
    /// the `consensus` actor, this method must do the logging.
    // TODO: change this to an async function once possible.
    fn already_done<M>(
        &self,
        ctx: &mut actor::Context<M>,
        db_ref: &mut ActorRef<db::Message>,
    ) -> Self::AlreadyDone;

    /// Start phase one of the 2PC protocol, asking the `peers` to prepare the
    /// query.
    fn peers_prepare<M>(
        &self,
        ctx: &mut actor::Context<M>,
        peers: &Peers,
    ) -> (ConsensusId, PeerRpc<SystemTime>);

    /// Second phase of the 2PC protocol, asking the `peers` to commit to the
    /// query.
    fn peers_commit<M>(
        &self,
        ctx: &mut actor::Context<M>,
        peers: &Peers,
        id: ConsensusId,
        timestamp: SystemTime,
    ) -> PeerRpc<()>;

    /// Ask the `peers` to abort the query.
    fn peers_abort<M>(
        &self,
        ctx: &mut actor::Context<M>,
        peers: &Peers,
        id: ConsensusId,
    ) -> PeerRpc<()>;
}

/// Runs a consensus algorithm for `query`.
async fn consensus<M, Q>(
    ctx: &mut actor::Context<M>,
    db_ref: &mut ActorRef<db::Message>,
    peers: &Peers,
    query: Q,
) -> Result<SystemTime, ()>
where
    Q: Query,
    db::Message: From<RpcMessage<(Q, SystemTime), SystemTime>> + From<RpcMessage<Q, ()>>,
{
    // The consensus id of a previous run, only used after we failed a consensus
    // run previously.
    let mut prev_consensus_id = None;

    // TODO: optimisation on a retry only let aborted/failed peers retry.

    for _ in 0..MAX_CONSENSUS_TRIES {
        if let Some(consensus_id) = prev_consensus_id {
            // It could be that one of the peers aborted because the operation
            // is already complete (i.e. the blob already stored/removed). Check
            // for that before proceeding.
            match query.already_done(ctx, db_ref).await {
                Ok(Some(timestamp)) => {
                    // Operation already completed by another actor. Abort the
                    // old 2PC query (from the previous iteration).
                    return abort(ctx, db_ref, peers, consensus_id, query)
                        .await
                        .map(|()| timestamp);
                }
                // Operation not yet complete, we can continue.
                Ok(None) => {}
                // Operation failed. This only happens if the database actor is
                // unavailable, so we can't do much now.
                Err(()) => return Err(()),
            }
        }

        // Phase one of 2PC: ask the participants (`peers`) to prepare the
        // storage layer.
        let (consensus_id, rpc) = query.peers_prepare(ctx, peers);
        debug!(
            "requesting peers to prepare query: consensus_id={}, key={}",
            consensus_id,
            query.key()
        );
        // Wait for the results.
        let results = rpc.await;

        // If we failed a previous run we want to start aborting it now.
        // NOTE: we wait for the participants to prepare it first to ensure that
        // the storage layer query can be reused.
        let abort_rpc =
            prev_consensus_id.map(|consensus_id| query.peers_abort(ctx, peers, consensus_id));

        let (committed, aborted, failed) = count_consensus_votes(&results);
        if aborted > 0 || failed > 0 {
            // TODO: allow some failure here.
            warn!(
                "consensus algorithm failed: consensus_id={}, key={}, votes_commit={}, votes_abort={}, failed_votes={}",
                consensus_id, query.key(), committed, aborted, failed
            );

            // Await aborting the previous run, if any.
            if let Some(abort_rpc) = abort_rpc {
                abort_consensus(abort_rpc, prev_consensus_id.take().unwrap(), query.key()).await;
            }

            // Try again, ensuring that this run is aborted in the next
            // iteration.
            prev_consensus_id = Some(consensus_id);
            continue;
        }

        debug!(
            "consensus algorithm succeeded: consensus_id={}, key={}, votes_commit={}, votes_abort={}, failed_votes={}",
            consensus_id, query.key(), committed, aborted, failed
        );

        // Phase two of 2PC: ask the participants to commit.
        debug!(
            "requesting peers to commit: consensus_id={}, key={}",
            consensus_id,
            query.key()
        );
        // Select a single timestamp to for the operation, to ensure its
        // consistent on all nodes.
        let timestamp = select_timestamp(&results);
        let rpc = query.peers_commit(ctx, peers, consensus_id, timestamp);

        // Await aborting the previous run, if any.
        if let Some(abort_rpc) = abort_rpc {
            abort_consensus(abort_rpc, prev_consensus_id.take().unwrap(), query.key()).await;
        }

        // Await the commit results.
        let results = rpc.await;
        let (committed, aborted, failed) = count_consensus_votes(&results);
        if aborted > 0 || failed > 0 {
            // TODO: allow some failure here.
            warn!(
                "consensus algorithm commitment failed: consensus_id={}, key={}, votes_commit={}, votes_abort={}, failed_votes={}",
                consensus_id, query.key(), committed, aborted, failed
            );

            // Try again, ensuring that this run is aborted in the next
            // iteration.
            prev_consensus_id = Some(consensus_id);
            continue;
        }

        debug!(
            "consensus algorithm commitment success: consensus_id={}, key={}, votes_commit={}, votes_abort={}, failed_votes={}",
            consensus_id, query.key(), committed, aborted, failed
        );

        // NOTE: Its crucial here that at least a single peer received the
        // message to commit before the coordinator (this node) commits to
        // storing the blob. If the coordinator would commit before sending a
        // message to at least a single peer the coordinator could crash before
        // doing so and after a restart be the only peer that has the blob
        // stored, i.e. be in an inconsistent state.
        //
        // TODO: optimisation: check `rpc` to see if we got a single `Ok`
        // response and then start the commit process ourselves, then we can
        // wait for the peers and the storing concurrently.
        return commit_query(ctx, db_ref, query, timestamp).await;
    }

    // Failed too many times.
    error!(
        "failed {} consensus algorithm runs: key={}",
        MAX_CONSENSUS_TRIES,
        query.key()
    );
    // Abort the last consensus run.
    let consensus_id = prev_consensus_id.unwrap();
    abort(ctx, db_ref, peers, consensus_id, query)
        .await
        .and(Err(()))
}

/// Commit to the `query`.
pub(crate) async fn commit_query<M, K, Q>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    query: Q,
    timestamp: SystemTime,
) -> Result<SystemTime, ()>
where
    K: RuntimeAccess,
    Q: storage::Query,
    db::Message: From<RpcMessage<(Q, SystemTime), SystemTime>>,
{
    debug!("committing to query: key={}", query.key());
    match db_rpc(ctx, db_ref, (query, timestamp)) {
        Ok(rpc) => rpc.await,
        Err(()) => Err(()),
    }
}

/// Await the `abort_rpc` and abort the `query`.
async fn abort<M, Q>(
    ctx: &mut actor::Context<M>,
    db_ref: &mut ActorRef<db::Message>,
    peers: &Peers,
    consensus_id: ConsensusId,
    query: Q,
) -> Result<(), ()>
where
    Q: Query,
    db::Message: From<RpcMessage<Q, ()>>,
{
    let abort_rpc = query.peers_abort(ctx, peers, consensus_id);
    abort_consensus(abort_rpc, consensus_id, query.key()).await;
    abort_query(ctx, db_ref, query).await
}

/// Await the results in `abort_rpc`, logging the results.
async fn abort_consensus(abort_rpc: PeerRpc<()>, consensus_id: ConsensusId, key: &Key) {
    let results = abort_rpc.await;
    let (committed, aborted, failed) = count_consensus_votes(&results);
    warn!(
        "aborted consensus algorithm: consensus_id={}, key={}, success={}, failed={}",
        consensus_id,
        key,
        committed,
        aborted + failed,
    );
}

/// Abort the `query`.
pub(crate) async fn abort_query<M, K, Q>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    query: Q,
) -> Result<(), ()>
where
    K: RuntimeAccess,
    Q: storage::Query,
    db::Message: From<RpcMessage<Q, ()>>,
{
    debug!("aborting query: key={}", query.key());
    match db_rpc(ctx, db_ref, query) {
        Ok(rpc) => rpc.await,
        Err(()) => Err(()),
    }
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
