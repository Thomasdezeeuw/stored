//! Module with the store blob operation.

use std::mem::replace;
use std::time::SystemTime;

use heph::{actor, ActorRef};
use log::{debug, error, trace};

use crate::db::AddBlobResponse;
use crate::op::db_rpc;
use crate::peer::coordinator::RelayError;
use crate::peer::Peers;
use crate::storage::StoreBlob;
use crate::{db, Buffer, Key};

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
        Ok(Success::Continue(query)) => query,
        Ok(Success::Done(key)) => return Ok(key),
        Err(err) => return Err(err),
    };

    let key = query.key().clone();
    if peers.is_empty() {
        // Easy mode!
        debug!(
            "running in single mode, not running consensus algorithm: key={}",
            key
        );
        // We can directly commit to storing the blob, we're always in agreement
        // with ourselves.
        commit_blob(ctx, db_ref, query, SystemTime::now())
            .await
            .map(|()| key)
    } else {
        // Hard mode.
        debug!("running consensus algorithm to store blob: key={}", key);
        consensus(ctx, db_ref, peers, query).await.map(|()| key)
    }
}

/// Operation succeeded, but can return early. E.g. when the blob is already
/// stored.
enum Success<T, U> {
    Continue(T),
    Done(U),
}

/// Phase one of storing a blob: adding the bytes to the data file.
async fn add_blob<M>(
    ctx: &mut actor::Context<M>,
    db_ref: &mut ActorRef<db::Message>,
    buf: &mut Buffer,
    blob_length: usize,
) -> Result<Success<StoreBlob, Key>, ()> {
    // We need ownership of the `Buffer`, so temporarily replace it with an
    // empty one.
    let blob = replace(buf, Buffer::empty());

    match db_rpc(ctx, db_ref, (blob, blob_length)) {
        Ok(rpc) => match rpc.await {
            Ok((result, mut buffer)) => {
                // Mark the blob's bytes as processed and put back the buffer.
                buffer.processed(blob_length);
                *buf = buffer;

                match result {
                    AddBlobResponse::Query(query) => Ok(Success::Continue(query)),
                    AddBlobResponse::AlreadyStored(key) => Ok(Success::Done(key)),
                }
            }
            Err(err) => Err(err),
        },
        Err(err) => Err(err),
    }
}

/// Runs the consensus algorithm for storing a blob.
async fn consensus<M>(
    ctx: &mut actor::Context<M>,
    db_ref: &mut ActorRef<db::Message>,
    peers: &Peers,
    query: StoreBlob,
) -> Result<(), ()> {
    // Phase one of 2PC: start the algorithm, letting the participants (peers)
    // know we want to store a blob.
    let key = query.key().clone();

    let (consensus_id, rpc) = peers.add_blob(ctx, key.clone());
    debug!(
        "requesting peers to store blob: consensus_id={}, key={}",
        consensus_id,
        query.key()
    );
    let results = rpc.await;
    let (commit, abort, failed) = count_consensus_votes(&results);

    if abort > 0 || failed > 0 {
        // FIXME: if too many peers want to abort: try again -> max 3 times.
        // Before trying again first check if the blob is already in the
        // database, then we when don't have to run the consensus algorithm
        // again. Let the peers know to abort the query after the max failed
        // attempts.
        error!(
            "consensus algorithm failed: consensus_id={}, key={}, votes_commit={}, votes_abort={}, failed_votes={}",
            consensus_id, key, commit, abort, failed
        );
        return Err(());
    }

    debug!(
        "consensus algorithm succeeded: consensus_id={}, key={}, votes_commit={}, votes_abort={}, failed_votes={}",
        consensus_id, key, commit, abort, failed
    );

    // Phase two of 2PC: let the participants know to commit to storing the
    // blob.
    debug!(
        "requesting peers to commit to storing blob: consensus_id={}, key={}",
        consensus_id,
        query.key()
    );
    let timestamp = select_timestamp(&results);
    let rpc = peers.commit_to_add_blob(ctx, consensus_id, key.clone(), timestamp);
    let results = rpc.await;
    let (commit, abort, failed) = count_consensus_votes(&results);
    debug!(
        "consensus algorithm commitment: consensus_id={}, key={}, votes_commit={}, votes_abort={}, failed_votes={}",
        consensus_id, key, commit, abort, failed
    );

    if abort > 0 || failed > 0 {
        // FIXME: support partial success.
        error!(
            "consensus algorithm failed: consensus_id={}, key={}, votes_commit={}, votes_abort={}, failed_votes={}",
            consensus_id, key, commit, abort, failed
        );
        return Err(());
    }

    // NOTE: Its crucial here that at least a single peer received the message
    // to commit before the coordinator (this) commit to adding the blob
    // ourselves. If we would commit before sending a message to at least a
    // single peer we could crash before doing so and after a restart be the
    // only peer that has the blob stored, i.e. be in an invalid state.
    // TODO: optimisation: check `rpc` to see if we got a single `Ok` response
    // and then start the commit process ourselves, then we can wait for the
    // peers and the storing concurrently.
    commit_blob(ctx, db_ref, query, timestamp).await
}

/// Returns the `(commit, abort, failed)` votes.
fn count_consensus_votes<T>(results: &[Result<T, RelayError>]) -> (usize, usize, usize) {
    let mut commit = 1; // Coordinator always votes to commit.
    let mut abort = 0;
    let mut failed = 0;

    for result in results {
        match result {
            Ok(..) => commit += 1,
            Err(RelayError::Abort) => abort += 1,
            Err(RelayError::Failed) => failed += 1,
        }
    }

    (commit, abort, failed)
}

/// Select the timestamp to use from consensus results.
fn select_timestamp(results: &[Result<SystemTime, RelayError>]) -> SystemTime {
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

/// Commit the blob in the `query` to the database.
async fn commit_blob<M>(
    ctx: &mut actor::Context<M>,
    db_ref: &mut ActorRef<db::Message>,
    query: StoreBlob,
    timestamp: SystemTime,
) -> Result<(), ()> {
    trace!("committing to adding blob: query={:?}", query);
    db_rpc(ctx, db_ref, (query, timestamp))?.await
}
