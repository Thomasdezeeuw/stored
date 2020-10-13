//! Module with the store blob operation.

use std::future::Future;
use std::io;
use std::mem::replace;
use std::pin::Pin;
use std::task::{self, Poll};
use std::time::SystemTime;

use heph::rt::RuntimeAccess;
use heph::{actor, ActorRef};
use log::debug;

use crate::db::{self, AddBlobResponse};
use crate::op::{commit_query, consensus, db_rpc, DbRpc, Outcome};
use crate::passport::{Event, Passport};
use crate::peer::{ConsensusId, PeerRpc, Peers};
use crate::storage::{BlobEntry, Query, StoreBlob, StreamBlob};
use crate::{Buffer, Key};

/// Stores a blob in the database.
///
/// Returns an error if the storing process fails.
pub async fn store_blob<M>(
    ctx: &mut actor::Context<M>,
    db_ref: &mut ActorRef<db::Message>,
    passport: &mut Passport,
    peers: Option<&Peers>,
    blob: &mut Buffer,
    blob_length: usize,
) -> Result<Key, ()> {
    debug!(
        "running store operation: request_id=\"{}\", blob_length={}",
        passport.id(),
        blob_length,
    );

    let query = match add_blob(ctx, db_ref, passport, blob, blob_length).await {
        Ok(Outcome::Continue(query)) => query,
        Ok(Outcome::Done(key)) => return Ok(key),
        Err(()) => return Err(()),
    };

    store_phase_two(ctx, db_ref, passport, peers, query).await
}

/// Phase one of storing a blob: adding the bytes to the data file.
pub(crate) async fn add_blob<M, K>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    passport: &mut Passport,
    buf: &mut Buffer,
    blob_length: usize,
) -> Result<Outcome<StoreBlob, Key>, ()>
where
    actor::Context<M, K>: RuntimeAccess,
{
    // We need ownership of the `Buffer`, so temporarily replace it with an
    // empty one.
    let view = replace(buf, Buffer::empty()).view(blob_length);
    debug!(
        "adding blob to storage: request_id=\"{}\", blob_length={}",
        passport.id(),
        view.len()
    );

    match db_rpc(ctx, db_ref, *passport.id(), view) {
        Ok(rpc) => match rpc.await {
            Ok((result, view)) => {
                // Mark the blob's bytes as processed and put back the buffer.
                *buf = view.processed();

                passport.mark(Event::AddedBlob);
                match result {
                    AddBlobResponse::Query(query) => Ok(Outcome::Continue(query)),
                    AddBlobResponse::AlreadyStored(key) => Ok(Outcome::Done(key)),
                }
            }
            Err(()) => {
                passport.mark(Event::FailedToAddBlob);
                Err(())
            }
        },
        Err(()) => {
            passport.mark(Event::FailedToAddBlob);
            Err(())
        }
    }
}

/// Stores a blob in the database, streaming the blob to the data file.
///
/// The `write` function must return a [`Future`] that writes blob into the
/// [`StreamBlob`] type.
///
/// Also see [`store_blob`].
///
/// # Panics
///
/// This panics if the returned `Future` by `write` fails to fully write the
/// blob into `StreamBlob`.
pub async fn store_streaming_blob<M, F, W>(
    ctx: &mut actor::Context<M>,
    db_ref: &mut ActorRef<db::Message>,
    passport: &mut Passport,
    peers: Option<&Peers>,
    blob_length: usize,
    write: F,
) -> StreamResult<Key>
where
    // Note: `F` really should be `FnOnce(&mut StreamBlob) -> W`, but I couldn't
    // get the lifetime to work.
    F: FnOnce(Box<StreamBlob>) -> W,
    W: Future<Output = io::Result<Box<StreamBlob>>>,
{
    debug!(
        "running streaming store operation: request_id=\"{}\", blob_length={}",
        passport.id(),
        blob_length,
    );

    let query = match stream_add_blob(ctx, db_ref, passport, blob_length, write).await {
        StreamResult::Ok(Outcome::Continue(query)) => query,
        StreamResult::Ok(Outcome::Done(key)) => return StreamResult::Ok(key),
        StreamResult::IoErr(err) => return StreamResult::IoErr(err),
        StreamResult::DbError => return StreamResult::DbError,
    };

    match store_phase_two(ctx, db_ref, passport, peers, query).await {
        Ok(key) => StreamResult::Ok(key),
        Err(()) => StreamResult::DbError,
    }
}

/// Result returned by [`store_streaming_blob`].
pub enum StreamResult<T> {
    Ok(T),
    /// Streaming the blob to disk failed, the source of the data returned an
    /// error.
    IoErr(io::Error),
    /// RPC to db actor failed.
    DbError,
}

/// Phase one of storing a blob: streaming the bytes to the data file.
pub(crate) async fn stream_add_blob<M, K, F, W>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    passport: &mut Passport,
    blob_length: usize,
    write: F,
) -> StreamResult<Outcome<StoreBlob, Key>>
where
    actor::Context<M, K>: RuntimeAccess,
    F: FnOnce(Box<StreamBlob>) -> W,
    W: Future<Output = io::Result<Box<StreamBlob>>>,
{
    // Behold the great `match` pyramid!
    match db_rpc(ctx, db_ref, *passport.id(), blob_length) {
        Ok(rpc) => match rpc.await {
            Ok(stream_blob) => {
                // Write the blob to the data file.
                let stream_blob = match write(stream_blob).await {
                    Ok(stream_blob) => stream_blob,
                    Err(err) => {
                        passport.mark(Event::FailedToAddBlob);
                        return StreamResult::IoErr(err);
                    }
                };
                debug_assert_eq!(stream_blob.bytes_left(), 0);

                // Add the blob to the database, completing phase one.
                match db_rpc(ctx, db_ref, *passport.id(), stream_blob) {
                    Ok(rpc) => match rpc.await {
                        Ok(result) => {
                            passport.mark(Event::AddedBlob);
                            match result {
                                AddBlobResponse::Query(query) => {
                                    StreamResult::Ok(Outcome::Continue(query))
                                }
                                AddBlobResponse::AlreadyStored(key) => {
                                    StreamResult::Ok(Outcome::Done(key))
                                }
                            }
                        }
                        Err(()) => {
                            passport.mark(Event::FailedToAddBlob);
                            StreamResult::DbError
                        }
                    },
                    Err(()) => {
                        passport.mark(Event::FailedToAddBlob);
                        StreamResult::DbError
                    }
                }
            }
            Err(()) => {
                passport.mark(Event::FailedToAddBlob);
                StreamResult::DbError
            }
        },
        Err(()) => {
            passport.mark(Event::FailedToAddBlob);
            StreamResult::DbError
        }
    }
}

/// Phase one of storing a blob: running consensus with the peers and committing
/// the blob to storage.
async fn store_phase_two<M>(
    ctx: &mut actor::Context<M>,
    db_ref: &mut ActorRef<db::Message>,
    passport: &mut Passport,
    peers: Option<&Peers>,
    query: StoreBlob,
) -> Result<Key, ()> {
    let key = query.key().clone();
    match peers {
        Some(peers) if !peers.is_empty() => {
            // Hard mode.
            debug!(
                "running consensus algorithm to store blob: request_id=\"{}\", key=\"{}\"",
                passport.id(),
                key,
            );
            consensus(ctx, db_ref, passport, peers, query)
                .await
                .map(|_| key)
        }
        // No peers, or no connected peers.
        _ => {
            // Easy mode!
            debug!(
                "running in stand-alone mode, not running consensus algorithm to store blob: request_id=\"{}\", key=\"{}\"",
                passport.id(),
                key,
            );
            // We can directly commit to storing the blob, we're always in agreement
            // with ourselves.
            commit_query(ctx, db_ref, passport, query, SystemTime::now())
                .await
                .map(|_| key)
        }
    }
}

impl super::Query for StoreBlob {
    type AlreadyDone = AlreadyDone;

    fn already_done<M>(
        &self,
        ctx: &mut actor::Context<M>,
        db_ref: &mut ActorRef<db::Message>,
        passport: &mut Passport,
    ) -> Self::AlreadyDone {
        debug!(
            "checking if blob is already stored: request_id=\"{}\", key=\"{}\"",
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
        peers.add_blob(ctx, self.key().clone())
    }

    const COMMITTED: Event = Event::CommittedStoringBlob;
    const FAILED_TO_COMMIT: Event = Event::FailedToCommitStoringBlob;

    fn peers_commit<M>(
        &self,
        ctx: &mut actor::Context<M>,
        peers: &Peers,
        id: ConsensusId,
        timestamp: SystemTime,
    ) -> PeerRpc<()> {
        peers.commit_to_store_blob(ctx, id, self.key().clone(), timestamp)
    }

    const ABORTED: Event = Event::AbortedStoringBlob;
    const FAILED_TO_ABORT: Event = Event::FailedToAbortStoringBlob;

    fn peers_abort<M>(
        &self,
        ctx: &mut actor::Context<M>,
        peers: &Peers,
        id: ConsensusId,
    ) -> PeerRpc<()> {
        peers.abort_store_blob(ctx, id, self.key().clone())
    }

    fn committed(peers: &Peers, id: ConsensusId, key: Key, timestamp: SystemTime) {
        peers.committed_store_blob(id, key, timestamp)
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
