//! Module with the remove blob operation.

use std::future::Future;
use std::pin::Pin;
use std::task::{self, Poll};
use std::time::SystemTime;

use heph::rt::RuntimeAccess;
use heph::{actor, ActorRef};
use log::debug;

use crate::db::{self, RemoveBlobResponse};
use crate::op::{commit_query, consensus, db_rpc, DbRpc, Outcome};
use crate::passport::{Event, Passport};
use crate::peer::{ConsensusId, PeerRpc, Peers};
use crate::storage::{BlobEntry, Query, RemoveBlob};
use crate::Key;

/// Removes a blob from the database.
///
/// Returns an error if the removing process fails.
pub async fn remove_blob<M>(
    ctx: &mut actor::Context<M>,
    db_ref: &mut ActorRef<db::Message>,
    passport: &mut Passport,
    peers: Option<&Peers>,
    key: Key,
) -> Result<Option<SystemTime>, ()> {
    debug!(
        "running remove operation: request_id=\"{}\", key=\"{}\"",
        passport.id(),
        key,
    );

    let query = match prep_remove_blob(ctx, db_ref, passport, key.clone()).await {
        Ok(Outcome::Continue(query)) => query,
        // Already removed or never stored.
        Ok(Outcome::Done(timestamp)) => return Ok(timestamp),
        Err(()) => return Err(()),
    };

    match peers {
        Some(peers) if !peers.is_empty() => {
            // Hard mode.
            debug!(
                "running consensus algorithm to remove blob: request_id=\"{}\", key=\"{}\"",
                passport.id(),
                query.key(),
            );
            consensus(ctx, db_ref, passport, peers, query)
                .await
                .map(Some)
        }
        // No peers, or no connected peers.
        _ => {
            // Easy mode!
            debug!(
                "running in stand-alone mode, not running consensus algorithm to remove blob: request_id=\"{}\", key=\"{}\"",
                passport.id(),
                query.key(),
            );
            // We can directly commit to removing the blob, we're always in
            // agreement with ourselves.
            commit_query(ctx, db_ref, passport, query, SystemTime::now())
                .await
                .map(Some)
        }
    }
}

/// Phase one of removing a blob: preparing the storage layer.
pub(crate) async fn prep_remove_blob<M, K>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    passport: &mut Passport,
    key: Key,
) -> Result<Outcome<RemoveBlob, Option<SystemTime>>, ()>
where
    actor::Context<M, K>: RuntimeAccess,
{
    debug!(
        "prepping storage to removing blob: request_id=\"{}\", key=\"{}\"",
        passport.id(),
        key,
    );
    match db_rpc(ctx, db_ref, *passport.id(), key) {
        Ok(rpc) => match rpc.await {
            Ok(result) => {
                passport.mark(Event::PreppedRemoveBlob);
                match result {
                    RemoveBlobResponse::Query(query) => Ok(Outcome::Continue(query)),
                    RemoveBlobResponse::NotStored(timestamp) => Ok(Outcome::Done(timestamp)),
                }
            }
            Err(()) => {
                passport.mark(Event::FailedToPrepRemoveBlob);
                Err(())
            }
        },
        Err(()) => {
            passport.mark(Event::FailedToPrepRemoveBlob);
            Err(())
        }
    }
}

impl super::Query for RemoveBlob {
    const NAME: &'static str = "remove";

    type AlreadyDone = AlreadyDone;

    fn already_done<M>(
        &self,
        ctx: &mut actor::Context<M>,
        db_ref: &mut ActorRef<db::Message>,
        passport: &mut Passport,
    ) -> Self::AlreadyDone {
        debug!(
            "checking if blob is already removed: request_id=\"{}\", key=\"{}\"",
            passport.id(),
            self.key(),
        );
        AlreadyDone {
            db_rpc: db_rpc(ctx, db_ref, *passport.id(), self.key().clone()),
        }
    }

    fn peers_prepare<M>(
        &self,
        ctx: &mut actor::Context<M>,
        peers: &Peers,
    ) -> (ConsensusId, PeerRpc<SystemTime>) {
        peers.remove_blob(ctx, self.key().clone())
    }

    const COMMITTED: Event = Event::CommittedRemovingBlob;
    const FAILED_TO_COMMIT: Event = Event::FailedToCommitRemovingBlob;

    fn peers_commit<M>(
        &self,
        ctx: &mut actor::Context<M>,
        peers: &Peers,
        id: ConsensusId,
        timestamp: SystemTime,
    ) -> PeerRpc<()> {
        peers.commit_to_remove_blob(ctx, id, self.key().clone(), timestamp)
    }

    const ABORTED: Event = Event::AbortedRemovingBlob;
    const FAILED_TO_ABORT: Event = Event::FailedToAbortRemovingBlob;

    fn peers_abort<M>(
        &self,
        ctx: &mut actor::Context<M>,
        peers: &Peers,
        id: ConsensusId,
    ) -> PeerRpc<()> {
        peers.abort_remove_blob(ctx, id, self.key().clone())
    }

    fn committed(peers: &Peers, id: ConsensusId, key: Key, timestamp: SystemTime) {
        peers.committed_remove_blob(id, key, timestamp)
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub(crate) struct AlreadyDone {
    db_rpc: Result<DbRpc<Option<BlobEntry>>, ()>,
}

impl Future for AlreadyDone {
    type Output = Result<Option<SystemTime>, ()>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut task::Context<'_>) -> Poll<Self::Output> {
        match self.db_rpc.as_mut() {
            Ok(db_rpc) => Pin::new(db_rpc).poll(ctx).map_ok(|entry| match entry {
                // Still stored.
                Some(BlobEntry::Stored(..)) => None,
                // Blob is already removed.
                Some(BlobEntry::Removed(timestamp)) => Some(timestamp),
                // This is not possible as we have a `RemoveBlob` query, which
                // ensures that at some point the blob with `key` was stored.
                None => unreachable!("consensus for removing blob that was never stored"),
            }),
            Err(()) => Poll::Ready(Err(())),
        }
    }
}
