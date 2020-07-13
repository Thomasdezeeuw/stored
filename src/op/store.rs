//! Module with the store blob operation.

use std::future::Future;
use std::mem::replace;
use std::pin::Pin;
use std::task::{self, Poll};
use std::time::SystemTime;

use heph::rt::RuntimeAccess;
use heph::{actor, ActorRef};
use log::debug;

use crate::db::{self, AddBlobResponse};
use crate::op::{commit_query, consensus, db_rpc, DbRpc, Outcome};
use crate::peer::{ConsensusId, PeerRpc, Peers};
use crate::storage::{BlobEntry, Query, StoreBlob};
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
        Err(()) => return Err(()),
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
        commit_query(ctx, db_ref, query, SystemTime::now())
            .await
            .map(|_| key)
    } else {
        // Hard mode.
        debug!("running consensus algorithm to store blob: key={}", key);
        consensus(ctx, db_ref, peers, query).await.map(|_| key)
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
            Err(()) => Err(()),
        },
        Err(()) => Err(()),
    }
}

impl super::Query for StoreBlob {
    type AlreadyDone = AlreadyDone;

    fn already_done<M>(
        &self,
        ctx: &mut actor::Context<M>,
        db_ref: &mut ActorRef<db::Message>,
    ) -> Self::AlreadyDone {
        debug!("checking if blob is already stored: key={}", self.key());
        AlreadyDone {
            db_rpc: db_rpc(ctx, db_ref, self.key().clone()),
        }
    }

    fn peers_prepare<M>(
        &self,
        ctx: &mut actor::Context<M>,
        peers: &Peers,
    ) -> (ConsensusId, PeerRpc<SystemTime>) {
        peers.add_blob(ctx, self.key().clone())
    }

    fn peers_commit<M>(
        &self,
        ctx: &mut actor::Context<M>,
        peers: &Peers,
        id: ConsensusId,
        timestamp: SystemTime,
    ) -> PeerRpc<()> {
        peers.commit_to_store_blob(ctx, id, self.key().clone(), timestamp)
    }

    fn peers_abort<M>(
        &self,
        ctx: &mut actor::Context<M>,
        peers: &Peers,
        id: ConsensusId,
    ) -> PeerRpc<()> {
        peers.abort_store_blob(ctx, id, self.key().clone())
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
                Some(BlobEntry::Stored(blob)) => Some(blob.created_at()),
                // Blob is not stored.
                Some(BlobEntry::Removed(..)) | None => None,
            }),
            Err(()) => Poll::Ready(Err(())),
        }
    }
}
