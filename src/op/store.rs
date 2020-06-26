//! Module with the store blob operation.

use std::mem::replace;
use std::time::SystemTime;

use heph::rt::RuntimeAccess;
use heph::{actor, ActorRef};
use log::{debug, error};

use crate::db::{self, AddBlobResponse};
use crate::op::{
    abort_consensus, contains_blob, count_consensus_votes, db_rpc, select_timestamp, Outcome,
    MAX_CONSENSUS_TRIES,
};
use crate::peer::Peers;
use crate::storage::{Query, StoreBlob};
use crate::{Buffer, Key};

/// Stores a blob in the database.
///
/// Returns an error if the storing process fails.
pub async fn store_blob<M>(
    ctx: &mut actor::Context<M>,
    db_ref: &mut ActorRef<db::Message>,
    peers: &Peers,
    blob: &mut Buffer,
    blob_length: usize,
) -> Result<Key, ()> {
    debug!("running store operation");

    let query = match add_blob(ctx, db_ref, blob, blob_length).await {
        Ok(Outcome::Continue(query)) => query,
        Ok(Outcome::Done(key)) => return Ok(key),
        Err(err) => return Err(err),
    };

    let key = query.key().clone();
    if peers.is_empty() {
        // Easy mode!
        debug!(
            "running in single mode, not running consensus algorithm to store blob: key={}",
            key
        );
        // We can directly commit to storing the blob, we're always in agreement
        // with ourselves.
        commit(ctx, db_ref, query, SystemTime::now())
            .await
            .map(|()| key)
    } else {
        // Hard mode.
        debug!("running consensus algorithm to store blob: key={}", key);
        consensus(ctx, db_ref, peers, query).await.map(|()| key)
    }
}

/// Phase one of storing a blob: adding the bytes to the data file.
pub(crate) async fn add_blob<M, K>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    buf: &mut Buffer,
    blob_length: usize,
) -> Result<Outcome<StoreBlob, Key>, ()>
where
    K: RuntimeAccess,
{
    // We need ownership of the `Buffer`, so temporarily replace it with an
    // empty one.
    let view = replace(buf, Buffer::empty()).view(blob_length);

    match db_rpc(ctx, db_ref, view) {
        Ok(rpc) => match rpc.await {
            Ok((result, view)) => {
                // Mark the blob's bytes as processed and put back the buffer.
                let mut buffer = view.into_inner();
                buffer.processed(blob_length);
                *buf = buffer;

                match result {
                    AddBlobResponse::Query(query) => Ok(Outcome::Continue(query)),
                    AddBlobResponse::AlreadyStored(key) => Ok(Outcome::Done(key)),
                }
            }
            Err(err) => Err(err),
        },
        Err(err) => Err(err),
    }
}

/// Runs the consensus algorithm for storing a blob.
// TODO: DRY with `op::remove::consensus`.
async fn consensus<M>(
    ctx: &mut actor::Context<M>,
    db_ref: &mut ActorRef<db::Message>,
    peers: &Peers,
    query: StoreBlob,
) -> Result<(), ()> {
    let key = query.key().clone();

    // The consensus id of a previous run, only used after we failed a consensus
    // run previously.
    let mut prev_consensus_id = None;

    // TODO: on a retry only let aborted/failed peers retry.

    for _ in 0..MAX_CONSENSUS_TRIES {
        if let Some(consensus_id) = prev_consensus_id {
            // It could be that one of the peers aborted because the blob is
            // already stored, which means its also stored on this node. Check
            // for that before proceeding.
            match contains_blob(ctx, db_ref, key.clone()).await {
                Ok(true) => {
                    // Blob already stored. Abort the old 2PC query (from the
                    // previous iteration).
                    let abort_rpc = peers.abort_store_blob(ctx, consensus_id, key.clone());
                    abort_consensus(abort_rpc, consensus_id, &key).await;
                    // Blob is stored so we can return Ok.
                    return Ok(());
                }
                // Blob not stored, we can continue.
                Ok(false) => {}
                // Break out of the consensus loop and abort the consensus run.
                Err(()) => break,
            }
        }

        // Phase one of 2PC: start the algorithm, asking the participants
        // (`peers`) to add the blob to there storage.
        let (consensus_id, rpc) = peers.add_blob(ctx, key.clone());
        debug!(
            "requesting peers to add blob: consensus_id={}, key={}",
            consensus_id,
            query.key()
        );
        // Wait for the results.
        let results = rpc.await;

        // If we failed a previous run we want to start aborting it now.
        // NOTE: this  must happen after the adding of the blob above to ensure
        // the add blob store query can be reused.
        let abort_rpc = prev_consensus_id
            .map(|consensus_id| peers.abort_store_blob(ctx, consensus_id, key.clone()));

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

        // Phase two of 2PC: ask the participants to commit to storing the blob.
        debug!(
            "requesting peers to commit to storing blob: consensus_id={}, key={}",
            consensus_id,
            query.key()
        );
        // Select a single timestamp to use for all nodes, when the blob is
        // officially stored.
        let timestamp = select_timestamp(&results);
        let rpc = peers.commit_to_store_blob(ctx, consensus_id, key.clone(), timestamp);

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
    let abort_rpc = peers.abort_store_blob(ctx, consensus_id, key.clone());
    abort_consensus(abort_rpc, consensus_id, &key).await;

    error!(
        "failed {} storing consensus algorithm runs: key={}",
        MAX_CONSENSUS_TRIES, key
    );
    // Always return an error if we failed to store the blob.
    return abort(ctx, db_ref, query).await.and(Err(()));
}

/// Commit the blob in the `query` to the database.
pub(crate) async fn commit<M, K>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    query: StoreBlob,
    timestamp: SystemTime,
) -> Result<(), ()>
where
    K: RuntimeAccess,
{
    debug!("committing to storing blob: key={}", query.key());
    match db_rpc(ctx, db_ref, (query, timestamp)) {
        Ok(rpc) => rpc.await,
        Err(err) => Err(err),
    }
}

/// Abort storing the blob in the `query` to the database.
pub(crate) async fn abort<M, K>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    query: StoreBlob,
) -> Result<(), ()>
where
    K: RuntimeAccess,
{
    debug!("aborting storing blob: key={}", query.key());
    match db_rpc(ctx, db_ref, query) {
        Ok(rpc) => rpc.await,
        Err(err) => Err(err),
    }
}
