//! Module with the remove blob operation.

use std::time::SystemTime;

use heph::rt::RuntimeAccess;
use heph::{actor, ActorRef};
use log::{debug, error};

use crate::db::{self, RemoveBlobResponse};
use crate::op::{
    abort_consensus, count_consensus_votes, db_rpc, retrieve_blob, select_timestamp, Outcome,
    MAX_CONSENSUS_TRIES,
};
use crate::peer::Peers;
use crate::storage::{BlobEntry, Query, RemoveBlob};
use crate::Key;

/// Removes a blob from the database.
///
/// Returns an error if the removing process fails.
pub async fn remove_blob<M>(
    ctx: &mut actor::Context<M>,
    db_ref: &mut ActorRef<db::Message>,
    peers: &Peers,
    key: Key,
) -> Result<Option<SystemTime>, ()> {
    debug!("running remove operation");

    let query = match prep_remove_blob(ctx, db_ref, key.clone()).await {
        Ok(Outcome::Continue(query)) => query,
        // Already removed or never stored.
        Ok(Outcome::Done(timestamp)) => return Ok(timestamp),
        Err(err) => return Err(err),
    };

    if peers.is_empty() {
        // Easy mode!
        debug!(
            "running in single mode, not running consensus algorithm to remove blob: key={}",
            query.key()
        );
        // We can directly commit to removing the blob, we're always in
        // agreement with ourselves.
        commit(ctx, db_ref, query, SystemTime::now())
            .await
            .map(Some)
    } else {
        // Hard mode.
        debug!(
            "running consensus algorithm to remove blob: key={}",
            query.key()
        );
        consensus(ctx, db_ref, peers, query).await.map(Some)
    }
}

/// Phase one of removing a blob: preparing the storage layer.
pub(crate) async fn prep_remove_blob<M, K>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    key: Key,
) -> Result<Outcome<RemoveBlob, Option<SystemTime>>, ()>
where
    K: RuntimeAccess,
{
    match db_rpc(ctx, db_ref, key) {
        Ok(rpc) => match rpc.await {
            Ok(result) => match result {
                RemoveBlobResponse::Query(query) => Ok(Outcome::Continue(query)),
                RemoveBlobResponse::NotStored(timestamp) => Ok(Outcome::Done(timestamp)),
            },
            Err(err) => Err(err),
        },
        Err(err) => Err(err),
    }
}

/// Runs the consensus algorithm for storing a blob.
// TODO: DRY with `op::store::consensus`.
async fn consensus<M>(
    ctx: &mut actor::Context<M>,
    db_ref: &mut ActorRef<db::Message>,
    peers: &Peers,
    query: RemoveBlob,
) -> Result<SystemTime, ()> {
    let key = query.key().clone();

    // The consensus id of a previous run, only used after we failed a consensus
    // run previously.
    let mut prev_consensus_id = None;

    // TODO: on a retry only let aborted/failed peers retry.

    for _ in 0..MAX_CONSENSUS_TRIES {
        if let Some(consensus_id) = prev_consensus_id {
            // It could be that one of the peers aborted because the blob is
            // already removed, which means its also removed on this node. Check
            // for that before proceeding.
            match retrieve_blob(ctx, db_ref, key.clone()).await {
                // Blob is still stored, we can continue.
                Ok(Some(BlobEntry::Stored(..))) => {}
                Ok(Some(BlobEntry::Removed(timestamp))) => {
                    // Blob already removed. Abort the old 2PC query (from the
                    // previous iteration).
                    let abort_rpc = peers.abort_remove_blob(ctx, consensus_id, key.clone());
                    abort_consensus(abort_rpc, consensus_id, &key).await;
                    // Blob is removed so we can return Ok.
                    return Ok(timestamp);
                }
                // This is not possible as we have a `RemoveBlob` query, which
                // ensures that at some point the blob with `key` was stored.
                Ok(None) => unreachable!("consensus for removing blob that was never stored"),
                // Break out of the consensus loop and abort the consensus run.
                Err(()) => break,
            }
        }

        // Phase one of 2PC: start the algorithm, asking the participants
        // (`peers`) to prepare the storage layer for removal.
        let (consensus_id, rpc) = peers.remove_blob(ctx, key.clone());
        debug!(
            "requesting peers to remove blob: consensus_id={}, key={}",
            consensus_id,
            query.key()
        );
        // Wait for the results.
        let results = rpc.await;

        // If we failed a previous run we want to start aborting it now.
        let abort_rpc = prev_consensus_id
            .map(|consensus_id| peers.abort_remove_blob(ctx, consensus_id, key.clone()));

        let (committed, aborted, failed) = count_consensus_votes(&results);
        if aborted > 0 || failed > 0 {
            // TODO: allow some failure here.
            error!(
                "consensus algorithm failed: consensus_id={}, key={}, votes_commit={}, votes_abort={}, failed_votes={}",
                consensus_id, key, committed, aborted, failed
            );

            // Await aborting the previous run, if any.
            if let Some(abort_rpc) = abort_rpc {
                abort_consensus(abort_rpc, prev_consensus_id.take().unwrap(), &key).await;
            }

            // Try again, ensuring that this run is aborted in the next
            // iteration.
            prev_consensus_id = Some(consensus_id);
            continue;
        }

        debug!(
            "consensus algorithm succeeded: consensus_id={}, key={}, votes_commit={}, votes_abort={}, failed_votes={}",
            consensus_id, key, committed, aborted, failed
        );

        // Phase two of 2PC: ask the participants to commit to removing the blob.
        debug!(
            "requesting peers to commit to removing blob: consensus_id={}, key={}",
            consensus_id,
            query.key()
        );
        // Select a single timestamp to use for all nodes, when the blob is
        // officially stored.
        let timestamp = select_timestamp(&results);
        let rpc = peers.commit_to_remove_blob(ctx, consensus_id, key.clone(), timestamp);

        // Await aborting the previous run, if any.
        if let Some(abort_rpc) = abort_rpc {
            abort_consensus(abort_rpc, prev_consensus_id.take().unwrap(), &key).await;
        }

        // Await the commit results.
        let results = rpc.await;
        let (committed, aborted, failed) = count_consensus_votes(&results);
        if aborted > 0 || failed > 0 {
            // TODO: allow some failure here.
            error!(
                "consensus algorithm commitment failed: consensus_id={}, key={}, votes_commit={}, votes_abort={}, failed_votes={}",
                consensus_id, key, committed, aborted, failed
            );

            // Try again, ensuring that this run is aborted in the next
            // iteration.
            prev_consensus_id = Some(consensus_id);
            continue;
        }

        debug!(
            "consensus algorithm commitment success: consensus_id={}, key={}, votes_commit={}, votes_abort={}, failed_votes={}",
            consensus_id, key, committed, aborted, failed
        );

        // NOTE: Its crucial here that at least a single peer received the
        // message to commit before the coordinator (this node) commits to
        // storing the blob. If we would commit before sending a message to at
        // least a single peer we could crash before doing so and after a
        // restart be the only peer that has the blob stored, i.e. be in an
        // inconsistent state.
        //
        // TODO: optimisation: check `rpc` to see if we got a single `Ok`
        // response and then start the commit process ourselves, then we can
        // wait for the peers and the storing concurrently.
        return commit(ctx, db_ref, query, timestamp).await;
    }

    // Failed too many times.
    // Abort the last consensus run.
    let consensus_id = prev_consensus_id.unwrap();
    let abort_rpc = peers.abort_remove_blob(ctx, consensus_id, key.clone());
    abort_consensus(abort_rpc, consensus_id, &key).await;

    error!(
        "failed {} removing consensus algorithm runs: key={}",
        MAX_CONSENSUS_TRIES, key
    );
    // Always return an error if we failed to store the blob.
    return abort(ctx, db_ref, query).await.and(Err(()));
}

/// Commit to removing the blob in the `query` to the database.
pub(crate) async fn commit<M, K>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    query: RemoveBlob,
    timestamp: SystemTime,
) -> Result<SystemTime, ()>
where
    K: RuntimeAccess,
{
    debug!("committing to removing blob: key={}", query.key());
    match db_rpc(ctx, db_ref, (query, timestamp)) {
        Ok(rpc) => rpc.await,
        Err(err) => Err(err),
    }
}

/// Abort removing the blob in the `query` to the database.
pub(crate) async fn abort<M, K>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    query: RemoveBlob,
) -> Result<(), ()>
where
    K: RuntimeAccess,
{
    debug!("aborting removing blob: key={}", query.key());
    match db_rpc(ctx, db_ref, query) {
        Ok(rpc) => rpc.await,
        Err(err) => Err(err),
    }
}
