//! Module with the consensus operation.
//!
//! Used by storing and removing blob operations.

use std::future::Future;
use std::time::SystemTime;

use heph::actor;
use heph::actor_ref::{ActorRef, RpcMessage};
use heph::rt::RuntimeAccess;
use log::{debug, error, info, warn};

use crate::op::db_rpc;
use crate::passport::{Event, Passport};
use crate::peer::coordinator::relay;
use crate::peer::{ConsensusId, PeerRpc, Peers};
use crate::{db, storage, Key};

/// Maximum number of tries we will attempt to run the consensus algorithm.
const MAX_CONSENSUS_TRIES: usize = 3;

pub(crate) trait Query: storage::Query {
    /// Name of the consensus, e.g. "store" or "remove.
    const NAME: &'static str;

    /// [`Future`] returned by [`Query::alredy_done`].
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
        passport: &mut Passport,
    ) -> Self::AlreadyDone;

    /// Start phase one of the 2PC protocol, asking the `peers` to prepare the
    /// query.
    fn peers_prepare<M>(
        &self,
        ctx: &mut actor::Context<M>,
        peers: &Peers,
    ) -> (ConsensusId, PeerRpc<SystemTime>);

    // Events in the request [`Passport`] regarding committing to a storage
    // query.
    const COMMITTED: Event;
    const FAILED_TO_COMMIT: Event;

    /// Second phase of the 2PC protocol, asking the `peers` to commit to the
    /// query.
    fn peers_commit<M>(
        &self,
        ctx: &mut actor::Context<M>,
        peers: &Peers,
        id: ConsensusId,
        timestamp: SystemTime,
    ) -> PeerRpc<()>;

    // Events in the request [`Passport`] regarding aborting a storage query.
    const ABORTED: Event;
    const FAILED_TO_ABORT: Event;

    /// Ask the `peers` to abort the query.
    fn peers_abort<M>(
        &self,
        ctx: &mut actor::Context<M>,
        peers: &Peers,
        id: ConsensusId,
    ) -> PeerRpc<()>;

    /// Coordinator committed to the query.
    fn committed(peers: &Peers, id: ConsensusId, key: Key, timestamp: SystemTime);
}

/// Runs a consensus algorithm for `query`.
pub(crate) async fn consensus<M, Q>(
    ctx: &mut actor::Context<M>,
    db_ref: &mut ActorRef<db::Message>,
    passport: &mut Passport,
    peers: &Peers,
    query: Q,
) -> Result<SystemTime, ()>
where
    Q: Query,
    db::Message: From<RpcMessage<(Q, SystemTime), SystemTime>>, // Commit.
    db::Message: From<RpcMessage<Q, ()>>,                       // Abort.
{
    // The consensus id of a previous run, only used after we failed a consensus
    // run previously.
    let mut prev_consensus_id = None;
    passport.mark(Event::StartingConsensus);

    // TODO: optimisation on a retry only let aborted/failed peers retry.

    for _ in 0..MAX_CONSENSUS_TRIES {
        if let Some(consensus_id) = prev_consensus_id {
            // It could be that one of the peers aborted because the operation
            // is already complete (i.e. the blob already stored/removed). Check
            // for that before proceeding.
            match query.already_done(ctx, db_ref, passport).await {
                Ok(Some(timestamp)) => {
                    // Operation already completed by another actor. Abort the
                    // old 2PC query (from the previous iteration).
                    return abort(ctx, db_ref, passport, peers, consensus_id, query)
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
            "requesting peers to prepare {} query: request_id=\"{}\", consensus_id={}, key=\"{}\"",
            Q::NAME,
            passport.id(),
            consensus_id,
            query.key()
        );
        // Wait for the results.
        let results = rpc.await;
        passport.mark(Event::ConsensusPhaseOneResults);

        // If we failed a previous run we want to start aborting it now.
        // NOTE: we wait for the participants to prepare it first to ensure that
        // the storage layer query can be reused.
        let abort_rpc =
            prev_consensus_id.map(|consensus_id| query.peers_abort(ctx, peers, consensus_id));

        let (committed, aborted, failed) = count_consensus_votes(&results);
        if aborted > 0 || failed > 0 {
            // TODO: allow some failure here.
            warn!(
                "{} consensus algorithm failed: request_id=\"{}\", consensus_id={}, key=\"{}\", votes_commit={}, votes_abort={}, failed_votes={}",
                Q::NAME, passport.id(), consensus_id, query.key(), committed, aborted, failed
            );

            // Await aborting the previous run, if any.
            if let Some(abort_rpc) = abort_rpc {
                abort_consensus::<Q>(
                    passport,
                    abort_rpc,
                    prev_consensus_id.take().unwrap(),
                    query.key(),
                )
                .await;
            }

            // Try again, ensuring that this run is aborted in the next
            // iteration.
            prev_consensus_id = Some(consensus_id);
            continue;
        }

        debug!(
            "{} consensus algorithm succeeded: request_id=\"{}\", consensus_id={}, key=\"{}\", votes_commit={}, votes_abort={}, failed_votes={}",
            Q::NAME, passport.id(), consensus_id, query.key(), committed, aborted, failed
        );

        // Phase two of 2PC: ask the participants to commit.
        debug!(
            "requesting peers to commit to {} query: request_id=\"{}\", consensus_id={}, key=\"{}\"",
            Q::NAME,
            passport.id(),
            consensus_id,
            query.key()
        );
        // Select a single timestamp to for the operation, to ensure its
        // consistent on all nodes.
        let timestamp = select_timestamp(&results);
        let rpc = query.peers_commit(ctx, peers, consensus_id, timestamp);

        // Await aborting the previous run, if any.
        if let Some(abort_rpc) = abort_rpc {
            abort_consensus::<Q>(
                passport,
                abort_rpc,
                prev_consensus_id.take().unwrap(),
                query.key(),
            )
            .await;
        }

        // Await the commit results.
        let results = rpc.await;
        passport.mark(Event::ConsensusPhaseTwoResults);
        let (committed, aborted, failed) = count_consensus_votes(&results);
        if aborted > 0 || failed > 0 {
            // TODO: allow some failure here.
            warn!(
                "{} consensus algorithm commitment failed: request_id=\"{}\", consensus_id={}, key=\"{}\", votes_commit={}, votes_abort={}, failed_votes={}",
                Q::NAME, passport.id(), consensus_id, query.key(), committed, aborted, failed
            );

            // Try again, ensuring that this run is aborted in the next
            // iteration.
            prev_consensus_id = Some(consensus_id);
            continue;
        }

        debug!(
            "{} consensus algorithm commitment success: request_id=\"{}\", consensus_id={}, key=\"{}\", votes_commit={}, votes_abort={}, failed_votes={}",
            Q::NAME, passport.id(), consensus_id, query.key(), committed, aborted, failed
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
        let key = query.key().to_owned();
        let timestamp = commit_query(ctx, db_ref, passport, query, timestamp).await?;

        // Let the participants know the operation is complete.
        Q::committed(peers, consensus_id, key, timestamp);

        passport.mark(Event::ConsensusCommitted);
        return Ok(timestamp);
    }

    // Failed too many times.
    error!(
        "failed {} {} consensus algorithm runs: request_id=\"{}\", key=\"{}\"",
        MAX_CONSENSUS_TRIES,
        Q::NAME,
        passport.id(),
        query.key()
    );
    // Abort the last consensus run.
    let consensus_id = prev_consensus_id.unwrap();
    abort(ctx, db_ref, passport, peers, consensus_id, query)
        .await
        .and(Err(()))
}

/// Commit to the `query`.
pub(crate) async fn commit_query<M, K, Q>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    passport: &mut Passport,
    query: Q,
    timestamp: SystemTime,
) -> Result<SystemTime, ()>
where
    actor::Context<M, K>: RuntimeAccess,
    Q: Query,
    db::Message: From<RpcMessage<(Q, SystemTime), SystemTime>>,
{
    debug!(
        "committing to {} query: request_id=\"{}\", key=\"{}\"",
        Q::NAME,
        passport.id(),
        query.key()
    );
    match db_rpc(ctx, db_ref, *passport.id(), (query, timestamp)) {
        Ok(rpc) => match rpc.await {
            Ok(time) => {
                passport.mark(Q::COMMITTED);
                Ok(time)
            }
            Err(()) => {
                passport.mark(Q::FAILED_TO_COMMIT);
                Err(())
            }
        },
        Err(()) => {
            passport.mark(Q::FAILED_TO_COMMIT);
            Err(())
        }
    }
}

/// Await the `abort_rpc` and abort the `query`.
async fn abort<M, Q>(
    ctx: &mut actor::Context<M>,
    db_ref: &mut ActorRef<db::Message>,
    passport: &mut Passport,
    peers: &Peers,
    consensus_id: ConsensusId,
    query: Q,
) -> Result<(), ()>
where
    Q: Query,
    db::Message: From<RpcMessage<Q, ()>>,
{
    let abort_rpc = query.peers_abort(ctx, peers, consensus_id);
    abort_consensus::<Q>(passport, abort_rpc, consensus_id, query.key()).await;
    abort_query(ctx, db_ref, passport, query).await
}

/// Await the results in `abort_rpc`, logging the results.
async fn abort_consensus<Q>(
    passport: &mut Passport,
    abort_rpc: PeerRpc<()>,
    consensus_id: ConsensusId,
    key: &Key,
) where
    Q: Query,
{
    let results = abort_rpc.await;
    passport.mark(Event::AbortedConsensusRun);
    let (committed, aborted, failed) = count_consensus_votes(&results);
    info!(
        "aborted {} consensus algorithm: request_id=\"{}\", consensus_id={}, key=\"{}\", success={}, failed={}",
        Q::NAME,
        passport.id(),
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
    passport: &mut Passport,
    query: Q,
) -> Result<(), ()>
where
    actor::Context<M, K>: RuntimeAccess,
    Q: Query,
    db::Message: From<RpcMessage<Q, ()>>,
{
    debug!(
        "aborting {} query: request_id=\"{}\", key=\"{}\"",
        Q::NAME,
        passport.id(),
        query.key()
    );
    match db_rpc(ctx, db_ref, *passport.id(), query) {
        Ok(rpc) => match rpc.await {
            Ok(()) => {
                passport.mark(Q::ABORTED);
                Ok(())
            }
            Err(()) => {
                passport.mark(Q::FAILED_TO_ABORT);
                Err(())
            }
        },
        Err(()) => {
            passport.mark(Q::FAILED_TO_ABORT);
            Err(())
        }
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
