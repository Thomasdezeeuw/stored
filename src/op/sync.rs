use std::cmp::min;
use std::collections::HashSet;
use std::convert::TryInto;
use std::future::Future;
use std::io::IoSlice;
use std::mem::replace;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::Poll;
use std::time::{Duration, SystemTime};
use std::{fmt, io, slice};

use futures_util::future::poll_fn;
use futures_util::io::AsyncWriteExt;
use heph::actor::context::ThreadSafe;
use heph::actor_ref::{ActorRef, Rpc, RpcMessage};
use heph::net::TcpStream;
use heph::rt::options::{ActorOptions, Priority};
use heph::rt::RuntimeAccess;
use heph::timer::Deadline;
use heph::{actor, restart_supervisor};
use log::{debug, error, warn};

use crate::buffer::Buffer;
use crate::db::{self, db_error};
use crate::net::tcp_connect_retry;
use crate::op::{db_rpc, retrieve_blob, retrieve_entries, sync_removed_blob, sync_stored_blob};
use crate::passport::{Passport, Uuid};
use crate::peer::server::{
    BLOB_LENGTH_LEN, DATE_TIME_LEN, KEY_SET_SIZE_LEN, METADATA_LEN, REQUEST_BLOB, REQUEST_KEYS,
    REQUEST_KEYS_SINCE, STORE_BLOB,
};
use crate::peer::{Peers, COORDINATOR_MAGIC};
use crate::storage::{BlobEntry, DateTime, Keys, ModifiedTime};
use crate::{timeout, Describe, Key};

// FIXME: what happens to the consensus algorithm when we're not synced and a
// request is made to remove a blob?

// FIXME: handle the case where the peers have removed a blob and is still
// stored locally, currently we just check if key is in the database.

// FIXME: also sync metadata.

// TODO: cleanup `full_sync`, it is too long.

/// Run a full sync of the stored blobs.
///
/// # Notes
///
/// The `peers` should be connected to all known peers.
pub async fn full_sync<M>(
    ctx: &mut actor::Context<M, ThreadSafe>,
    db_ref: &mut ActorRef<db::Message>,
    peers: &Peers,
) -> Result<(), ()> {
    debug!("running full synchronisation");
    if peers.is_empty() {
        return Ok(());
    }

    debug_assert!(
        peers.all_connected(),
        "called `full_sync` with not all peers connected"
    );

    // Keys stored locally.
    // TODO: use `retrieve_keys` here once peers use passports.
    let stored_keys: Keys = db_rpc(ctx, db_ref, Uuid::empty(), ())?.await?;
    // Keys missing locally.
    let mut missing_keys = HashSet::new();

    let mut peers = peers
        .addresses()
        .into_iter()
        .filter_map(|peer_address| SyncingPeer::start(ctx, db_ref.clone(), peer_address))
        .collect::<Vec<_>>();

    // Whether or not we received all known keys from all peers.
    let mut got_all_known_keys = false;

    poll_fn(move |task_ctx| {
        if !got_all_known_keys {
            // All peers (not in the failed state) are in the waiting state.
            let mut all_waiting = true;

            for peer in peers.iter_mut() {
                match &mut peer.state {
                    State::GettingKeys(rpc) => match Pin::new(&mut *rpc).poll(task_ctx) {
                        Poll::Ready(Ok(known_keys)) => {
                            // All the keys the peer didn't store, but we stored
                            // locally.
                            let peer_missing_keys = stored_keys
                                .into_iter()
                                .filter_map(|key| {
                                    (!known_keys.iter().any(|k| k == key)).then(|| key.clone())
                                })
                                .collect::<Vec<_>>();

                            // All the keys missing locally.
                            let local_missing_keys = known_keys.iter().filter_map(|key| {
                                (!stored_keys.into_iter().any(|k| k == key)).then(|| key.clone())
                            });
                            missing_keys.extend(local_missing_keys);

                            if peer_missing_keys.is_empty() {
                                peer.state = State::Waiting;
                            } else {
                                match peer.actor_ref.rpc(ctx, peer_missing_keys.clone()) {
                                    Ok(rpc) => {
                                        peer.state = State::SharingBlobs(rpc, peer_missing_keys);
                                        all_waiting = false;
                                    }
                                    Err(err) => peer_failed(peer, err),
                                }
                            }
                        }
                        // NOTE: error is already logged by the supervisor.
                        Poll::Ready(Err(..)) => match peer.actor_ref.rpc(ctx, ()) {
                            // Try again, the supervisor keep track of restarts
                            // so we don't do this for ever..
                            Ok(r) => {
                                *rpc = r;
                                all_waiting = false;
                            }
                            Err(err) => peer_failed(peer, err),
                        },
                        Poll::Pending => all_waiting = false,
                    },
                    State::SharingBlobs(rpc, peer_missing_keys) => {
                        match Pin::new(&mut *rpc).poll(task_ctx) {
                            Poll::Ready(Ok(())) => peer.state = State::Waiting,
                            // NOTE: error is already logged by the supervisor.
                            Poll::Ready(Err(..)) => {
                                // Try again.
                                match peer.actor_ref.rpc(ctx, peer_missing_keys.clone()) {
                                    Ok(r) => {
                                        *rpc = r;
                                        all_waiting = false;
                                    }
                                    Err(err) => peer_failed(peer, err),
                                }
                            }
                            Poll::Pending => all_waiting = false,
                        }
                    }
                    State::Waiting | State::Failed => {}
                    State::RetrievingBlobs(..) => unreachable!(
                        "`full_sync` in an invalid state while getting known keys: state={:?}",
                        peer.state
                    ),
                }
            }

            if !all_waiting {
                return Poll::Pending;
            }

            got_all_known_keys = true;
            split_keys(ctx, &mut peers, &missing_keys)?;
        }

        loop {
            let mut all_complete = true;
            for peer in peers.iter_mut() {
                match &mut peer.state {
                    State::RetrievingBlobs(rpc, keys) => match Pin::new(&mut *rpc).poll(task_ctx) {
                        Poll::Ready(Ok(stored_keys)) => {
                            remove_from(stored_keys, &mut missing_keys);
                            peer.state = State::Waiting;
                        }
                        // NOTE: error is already logged by the supervisor.
                        Poll::Ready(Err(..)) => match peer.actor_ref.rpc(ctx, keys.clone()) {
                            Ok(r) => {
                                all_complete = false;
                                *rpc = r
                            }
                            Err(err) => peer_failed(peer, err),
                        },
                        Poll::Pending => all_complete = false,
                    },
                    State::Waiting | State::Failed => {}
                    State::GettingKeys(..) | State::SharingBlobs(..) => unreachable!(
                        "`full_sync` in an invalid state while retrieving blobs: state={:?}",
                        peer.state
                    ),
                }
            }

            if all_complete {
                if missing_keys.is_empty() {
                    break Poll::Ready(Ok(()));
                } else {
                    // Some peers (partially) failed, so we need to try again.
                    split_keys(ctx, &mut peers, &missing_keys)?;
                }
            } else {
                break Poll::Pending;
            }
        }
    })
    .await
}

/// Run a partial sync of the stored blobs with a single peer.
///
/// To be called if a peer disconnects. The `last_seen` time is the time the
/// peer was known to be connected (and synced), e.g. the last time it received
/// a message from it.
pub async fn peer_sync<M>(
    ctx: &mut actor::Context<M, ThreadSafe>,
    db_ref: &mut ActorRef<db::Message>,
    passport: &mut Passport,
    stream: &mut TcpStream,
    remote: SocketAddr,
    buf: &mut Buffer,
    last_seen: SystemTime,
) -> crate::Result<()> {
    // TODO: actually use the `passport`.

    debug!("running partial synchronisation: remote=\"{}\"", remote);

    // To be very cautions we check all keys stored/removed one hour since we've
    // last seen the peer. This is likely too pessimistic, but better safe then
    // sorry.
    let since = last_seen - Duration::from_secs(60 * 60); // 1 hour.

    // Entries stored locally.
    let entries = retrieve_entries(ctx, db_ref, passport)
        .await
        .map_err(|()| db_error())?;

    // The keys stored by the peer.
    let mut peer_known_keys = get_known_keys_since(ctx, stream, buf, since).await?;

    // All the keys the peer didn't store, but we stored locally.
    let peer_missing_keys = entries
        .into_iter()
        .filter_map(|entry| {
            if entry.modified_time().after(&since) {
                let key = entry.key();
                // Remove all locally known keys from `peer_known_keys`. If it
                // returns `true` it means the peer is missing the key.
                (!peer_known_keys.remove(key)).then(|| key.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    // Any keys not removed from `peer_known_keys` above are missing locally.
    let local_missing_keys = peer_known_keys;

    // Share the keys the peer is missing.
    share_blobs(ctx, db_ref, passport, stream, peer_missing_keys).await?;

    // TODO: change retrieve_blobs to support `HashSet`.
    let local_missing_keys_len = local_missing_keys.len();
    let mut local_missing_keys = local_missing_keys.into_iter().collect();
    retrieve_blobs(ctx, db_ref, passport, stream, buf, &mut local_missing_keys).await?;
    debug_assert_eq!(local_missing_keys.len(), local_missing_keys_len);
    Ok(())
}

fn peer_failed<E>(peer: &mut SyncingPeer, err: E)
where
    E: fmt::Display,
{
    warn!(
        "syncing with peer failed: {}: remote_address=\"{}\"",
        err, peer.address
    );
    peer.state = State::Failed;
}

/// Returns the number of peers that are in an ok state (i.e. have not failed).
fn ok_peers(peers: &[SyncingPeer]) -> usize {
    let mut n = 0;
    for peer in peers {
        if !peer.has_failed() {
            n += 1;
        }
    }
    n
}

/// Partitions `keys`, returning a list of `n` lists of keys.
fn partition(keys: &HashSet<Key>, n: usize) -> Vec<Vec<Key>> {
    debug_assert!(n != 0, "called `partition` with 0");
    let mut size = keys.len() / n;
    if size * n != keys.len() {
        // If we don't have an even split the first couple of partitions will do
        // some extra work.
        size += 1;
    }

    let mut iter = keys.iter().cloned();
    (0..n).map(|_| (&mut iter).take(size).collect()).collect()
}

/// All peers not in the failed state must be in the [`State::Waiting`] state.
fn split_keys<M>(
    ctx: &mut actor::Context<M, ThreadSafe>,
    peers: &mut [SyncingPeer],
    missing_keys: &HashSet<Key>,
) -> Result<(), ()> {
    let n = ok_peers(&peers);
    if n == 0 {
        error!("failed to synchronise blobs, all peers failed");
        return Err(());
    }

    let partitions = partition(&missing_keys, n);

    for (peer, keys) in peers.iter_mut().filter(|p| !p.has_failed()).zip(partitions) {
        match &mut peer.state {
            State::Waiting => match peer.actor_ref.rpc(ctx, keys.clone()) {
                Ok(rpc) => peer.state = State::RetrievingBlobs(rpc, keys),
                Err(err) => peer_failed(peer, err),
            },
            State::GettingKeys(..)
            | State::SharingBlobs(..)
            | State::RetrievingBlobs(..)
            | State::Failed => unreachable!(
                "`full_sync` in an invalid state starting to retrieve missing blobs: state={:?}",
                peer.state
            ),
        }
    }
    Ok(())
}

/// Removes `keys` from `missing_keys`.
fn remove_from(keys: Vec<Key>, missing_keys: &mut HashSet<Key>) {
    for key in keys {
        missing_keys.remove(&key);
    }
}

#[derive(Debug)]
struct SyncingPeer {
    actor_ref: ActorRef<Message>,
    address: SocketAddr,
    state: State,
}

impl SyncingPeer {
    /// Start [`peer_sync_actor`], retuning `None` we can't make a RPC to it.
    fn start<M>(
        ctx: &mut actor::Context<M, ThreadSafe>,
        db_ref: ActorRef<db::Message>,
        peer_address: SocketAddr,
    ) -> Option<SyncingPeer> {
        let args = (db_ref.clone(), peer_address);
        let supervisor = Supervisor::new((db_ref, peer_address));
        let peer_sync_actor = peer_sync_actor as fn(_, _, _) -> _;
        let options = ActorOptions::default()
            .with_priority(Priority::HIGH)
            .mark_ready();
        let actor_ref = ctx.spawn(supervisor, peer_sync_actor, args, options);
        match actor_ref.rpc(ctx, ()) {
            Ok(rpc) => Some(SyncingPeer {
                actor_ref,
                address: peer_address,
                state: State::GettingKeys(rpc),
            }),
            Err(err) => {
                warn!(
                    "failed to start syncing to actor: {}: remote_address=\"{}\"",
                    err, peer_address
                );
                None
            }
        }
    }

    /// Returns `true` if the peer is in the [`State::Failed`] state.
    fn has_failed(&self) -> bool {
        matches!(self.state, State::Failed)
    }
}

impl Drop for SyncingPeer {
    fn drop(&mut self) {
        let _ = self.actor_ref.send(Message::Stop);
    }
}

/// State of the [`SyncingPeer`].
#[derive(Debug)]
enum State {
    /// Getting the known keys from the peer.
    GettingKeys(Rpc<HashSet<Key>>),
    /// Actor is sharing blobs with the peer, the vector is the list of keys the
    /// peer is missing.
    SharingBlobs(Rpc<()>, Vec<Key>),
    /// Actor is retrieving blobs and storing them locally. Vector is the list
    /// of keys to store.
    RetrievingBlobs(Rpc<Vec<Key>>, Vec<Key>),
    /// Actor is currently inactive.
    Waiting,
    /// Actor failed.
    Failed,
}

restart_supervisor!(
    pub Supervisor,
    "peer synchronisation actor",
    (ActorRef<db::Message>, SocketAddr),
    5,
    Duration::from_secs(60),
);

/// Actor that synchronises with a single peer.
async fn peer_sync_actor<K>(
    mut ctx: actor::Context<Message, K>,
    mut db_ref: ActorRef<db::Message>,
    peer_address: SocketAddr,
) -> crate::Result<()>
where
    K: RuntimeAccess,
{
    // We use a low retry value because the peer should already be connected
    // when a peer sync is run, so we know its up and running.
    let mut stream = tcp_connect_retry(&mut ctx, peer_address, Duration::from_millis(100), 3)
        .await
        .map_err(|err| err.describe("connecting to peer"))?;

    // Set `TCP_NODELAY` as we send single byte requests and buffer larger
    // writes.
    if let Err(err) = stream.set_nodelay(true) {
        error!(
            "error setting `TCP_NODELAY`, continuing: {}: remote_address=\"{}\"",
            err, peer_address
        );
    }

    stream
        .write_all(COORDINATOR_MAGIC)
        .await
        .map_err(|err| err.describe("writing magic bytes"))?;

    // FIXME: actually use the passport.
    let mut passport = Passport::empty();
    passport.set_id(Uuid::new());
    let mut buf = Buffer::new();
    // FIXME: this doesn't return. Also fix `TestStream::expect_end`.
    // Change this to `while let Some(msg) = ctx.receive_next()`.
    loop {
        match ctx.receive_next().await {
            Message::GetKnownKeys(RpcMessage { response, .. }) => {
                let known_keys = get_known_keys(&mut ctx, &mut stream, &mut buf).await?;
                if let Result::Err(err) = response.respond(known_keys) {
                    // TODO: better name for the actor?
                    warn!("peer sync actor failed to send response to actor: {}", err);
                }
            }
            Message::ShareBlobs(RpcMessage { request, response }) => {
                share_blobs(&mut ctx, &mut db_ref, &mut passport, &mut stream, request).await?;
                if let Result::Err(err) = response.respond(()) {
                    // TODO: better name for the actor?
                    warn!("peer sync actor failed to send response to actor: {}", err);
                }
            }
            Message::RetrieveBlobs(RpcMessage { request, response }) => {
                let mut stored_keys = request;
                let res = retrieve_blobs(
                    &mut ctx,
                    &mut db_ref,
                    &mut passport,
                    &mut stream,
                    &mut buf,
                    &mut stored_keys,
                )
                .await;
                if let Result::Err(err) = response.respond(stored_keys) {
                    // TODO: better name for the actor?
                    warn!("peer sync actor failed to send response to actor: {}", err);
                }
                res?;
            }
            Message::Stop => return Ok(()),
        }
    }
}

/// Message type used by [`peer_sync_actor`].
enum Message {
    /// Get the set of known keys from the peer.
    GetKnownKeys(RpcMessage<(), HashSet<Key>>),
    /// Request the peer to store the blobs with the provided keys.
    ///
    /// # Panics
    ///
    /// All keys must be stored, or the actor will panic.
    ShareBlobs(RpcMessage<Vec<Key>, ()>),
    /// Retrieves the blobs with keys and stores them.
    ///
    /// Returns the list of blobs successfully stored.
    RetrieveBlobs(RpcMessage<Vec<Key>, Vec<Key>>),
    // FIXME: this shouldn't be a thing. Heph's `actor::Context::receive_next`
    // should return `None` once all `ActorRef`s to it are dropped, then
    // `peer_sync_actor` can stop itself.
    Stop,
}

impl From<RpcMessage<(), HashSet<Key>>> for Message {
    fn from(msg: RpcMessage<(), HashSet<Key>>) -> Message {
        Message::GetKnownKeys(msg)
    }
}

impl From<RpcMessage<Vec<Key>, ()>> for Message {
    fn from(msg: RpcMessage<Vec<Key>, ()>) -> Message {
        Message::ShareBlobs(msg)
    }
}

impl From<RpcMessage<Vec<Key>, Vec<Key>>> for Message {
    fn from(msg: RpcMessage<Vec<Key>, Vec<Key>>) -> Message {
        Message::RetrieveBlobs(msg)
    }
}

/// Request all known keys from `stream` (connected to [`peer::server`]).
async fn get_known_keys<M, K>(
    ctx: &mut actor::Context<M, K>,
    stream: &mut TcpStream,
    buf: &mut Buffer,
) -> crate::Result<HashSet<Key>>
where
    K: RuntimeAccess,
{
    stream
        .write_all(slice::from_ref(&REQUEST_KEYS))
        .await
        .map_err(|err| err.describe("writing known keys request"))?;

    if buf.len() < KEY_SET_SIZE_LEN {
        let n = KEY_SET_SIZE_LEN - buf.len();
        match Deadline::timeout(ctx, timeout::PEER_READ, buf.read_n_from(&mut *stream, n)).await {
            Ok(..) => {}
            Err(err) => return Err(err.describe("reading number of keys")),
        }
    }

    let bytes = buf.as_bytes();
    let size_bytes = bytes[0..KEY_SET_SIZE_LEN].try_into().unwrap();
    let size = u64::from_be_bytes(size_bytes) as usize;
    buf.processed(KEY_SET_SIZE_LEN);

    // FIXME: put a limit on `size`.
    // TODO: given a large enough set we might want to stream the set.
    let mut known_keys = HashSet::with_capacity(size);

    for _ in 0..size {
        if buf.len() < Key::LENGTH {
            let n = Key::LENGTH - buf.len();
            match Deadline::timeout(ctx, timeout::PEER_READ, buf.read_n_from(&mut *stream, n)).await
            {
                Ok(..) => {}
                Err(err) => return Err(err.describe("reading known keys")),
            }
        }

        // Safety: we've checked the length above, so this slicing won't panic.
        let key = Key::from_bytes(&buf.as_bytes()[..Key::LENGTH]);
        known_keys.get_or_insert_owned(key);
        buf.processed(Key::LENGTH);
    }

    Ok(known_keys)
}

/// Request all known keys from `stream` (connected to [`peer::server`]) `since`
/// a certain date.
async fn get_known_keys_since<M, K>(
    ctx: &mut actor::Context<M, K>,
    stream: &mut TcpStream,
    buf: &mut Buffer,
    since: SystemTime,
) -> crate::Result<HashSet<Key>>
where
    K: RuntimeAccess,
{
    let since = DateTime::from(since);
    let bufs = &mut [
        IoSlice::new(slice::from_ref(&REQUEST_KEYS_SINCE)),
        IoSlice::new(since.as_bytes()),
    ];
    stream
        .write_all_vectored(bufs)
        .await
        .map_err(|err| err.describe("writing known keys since request"))?;

    // TODO: given a large enough set we might want to stream the set.
    let mut known_keys = HashSet::new();

    loop {
        // Read the number of keys in this part.
        if buf.len() < KEY_SET_SIZE_LEN {
            let n = KEY_SET_SIZE_LEN - buf.len();
            match Deadline::timeout(ctx, timeout::PEER_READ, buf.read_n_from(&mut *stream, n)).await
            {
                Ok(..) => {}
                Err(err) => return Err(err.describe("reading number of keys")),
            }
        }
        let bytes = buf.as_bytes();
        // Safety: checked the length above, so this won't panic.
        let size_bytes = bytes[0..KEY_SET_SIZE_LEN].try_into().unwrap();
        let size = u64::from_be_bytes(size_bytes) as usize;
        buf.processed(KEY_SET_SIZE_LEN);

        if size == 0 {
            return Ok(known_keys);
        }

        // FIXME: put a limit on `size`.
        known_keys.reserve(size);

        for _ in 0..size {
            if buf.len() < Key::LENGTH {
                let n = Key::LENGTH - buf.len();
                match Deadline::timeout(ctx, timeout::PEER_READ, buf.read_n_from(&mut *stream, n))
                    .await
                {
                    Ok(..) => {}
                    Err(err) => return Err(err.describe("reading known keys")),
                }
            }

            // Safety: we've checked the length above, so this slicing won't panic.
            let key = Key::from_bytes(&buf.as_bytes()[..Key::LENGTH]);
            known_keys.get_or_insert_owned(key);
            buf.processed(Key::LENGTH);
        }
    }
}

/// Request all peer to store the blobs in `keys`.
///
/// # Panics
///
/// This panics if a key in `keys` in not stored by the `db_ref` actor.
async fn share_blobs<M, K>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    passport: &mut Passport,
    stream: &mut TcpStream,
    keys: Vec<Key>,
) -> crate::Result<()>
where
    K: RuntimeAccess,
{
    for key in keys {
        match retrieve_blob(ctx, db_ref, passport, key.clone()).await {
            Ok(Some(BlobEntry::Stored(blob))) => {
                write_store_blob_request(
                    ctx,
                    stream,
                    &key,
                    ModifiedTime::Created(blob.created_at()),
                    blob.bytes(),
                )
                .await?;
            }
            Ok(Some(BlobEntry::Removed(timestamp))) => {
                write_store_blob_request(ctx, stream, &key, ModifiedTime::Removed(timestamp), &[])
                    .await?
            }
            // SAFETY: this can never happen.
            Ok(None) => unreachable!(
                "failed to share blob with peer: blob not stored locally: key={}",
                key
            ),
            Err(()) => return Err(db_error()),
        }
    }

    Ok(())
}

/// Writes a blob, with `key`, `timestamp` and the `blob` bytes, to `stream`.
async fn write_store_blob_request<M, K>(
    ctx: &mut actor::Context<M, K>,
    stream: &mut TcpStream,
    key: &Key,
    timestamp: ModifiedTime,
    blob: &[u8],
) -> crate::Result<()>
where
    K: RuntimeAccess,
{
    // TODO: buffer smaller blobs, current minimum is 84 bytes (which we
    // directly send as we use `TCP_NODELAY`).
    let timestamp: DateTime = timestamp.into();
    let length: [u8; BLOB_LENGTH_LEN] = (blob.len() as u64).to_be_bytes();
    let bufs = &mut [
        IoSlice::new(slice::from_ref(&STORE_BLOB)),
        IoSlice::new(key.as_bytes()),
        IoSlice::new(timestamp.as_bytes()),
        IoSlice::new(&length),
        IoSlice::new(blob),
    ];
    let timeout = timeout::peer_write(blob.len() as u64);
    Deadline::timeout(ctx, timeout, stream.write_all_vectored(bufs))
        .await
        .map_err(|err| err.describe("writing store blob request"))
}

/// Maximum number of keys [`retrieve_blobs`] will request per iteration.
const RETRIEVE_MAX_KEYS: usize = 20;

/// Retrieve all blobs with keys in `stored_keys`.
///
/// After this function `stored_keys` will hold the keys successfully stored.
async fn retrieve_blobs<M, K>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    passport: &mut Passport,
    stream: &mut TcpStream,
    buf: &mut Buffer,
    stored_keys: &mut Vec<Key>,
) -> crate::Result<()>
where
    K: RuntimeAccess,
{
    let mut keys = replace(stored_keys, Vec::new());

    while !keys.is_empty() {
        // TODO: use `MaybeUninit` here?
        let mut bufs = [IoSlice::new(&[]); 2 * RETRIEVE_MAX_KEYS];
        for (i, key) in (0..)
            .step_by(2)
            .zip(keys.iter().rev().take(RETRIEVE_MAX_KEYS))
        {
            bufs[i] = IoSlice::new(slice::from_ref(&REQUEST_BLOB));
            bufs[i + 1] = IoSlice::new(key.as_bytes());
        }

        let length = min(RETRIEVE_MAX_KEYS, keys.len());
        let bufs = &mut bufs[0..length * 2];
        Deadline::timeout(ctx, timeout::PEER_WRITE, stream.write_all_vectored(bufs))
            .await
            .map_err(|err| err.describe("writing blob request"))?;

        let mut left = length;
        let mut want_read = METADATA_LEN;
        while left > 0 {
            if buf.len() < want_read {
                let n = want_read - buf.len();
                let timeout = timeout::peer_read(n as u64);
                match Deadline::timeout(ctx, timeout, buf.read_n_from(&mut *stream, n)).await {
                    Ok(..) => {}
                    Err(err) => return Err(err.describe("reading blob")),
                }
            }

            // Safety: checked above if we read enough bytes so indexing won't
            // panic.
            let bytes = buf.as_bytes();
            let timestamp =
                DateTime::from_bytes(&bytes[0..DATE_TIME_LEN]).unwrap_or(DateTime::INVALID);
            let blob_length_bytes = bytes[DATE_TIME_LEN..DATE_TIME_LEN + BLOB_LENGTH_LEN]
                .try_into()
                .unwrap();
            let blob_length = u64::from_be_bytes(blob_length_bytes) as usize;

            if (timestamp.is_invalid() || timestamp.is_removed()) && blob_length != 0 {
                return Err(io::Error::from(io::ErrorKind::InvalidData)
                    .describe("got send an removed/invalid blob, with a non-zero length"));
            }

            // FIXME: put a limit on `blob_length`.
            if buf.len() < blob_length + METADATA_LEN {
                // Don't have the entire blob yet.
                want_read = blob_length + METADATA_LEN;
                continue;
            }

            buf.processed(METADATA_LEN);
            left -= 1;
            want_read = METADATA_LEN;

            // Safety: we only run this loop if its not empty, thus
            // `pop().unwrap()` is safe to call.
            let key = keys.pop().unwrap();
            match timestamp.into() {
                ModifiedTime::Created(timestamp) => {
                    let view = replace(buf, Buffer::empty()).view(blob_length as usize);
                    match sync_stored_blob(ctx, db_ref, passport, view, timestamp).await {
                        Ok(view) => *buf = view.processed(),
                        Err(()) => return Err(db_error()),
                    }
                }
                ModifiedTime::Removed(timestamp) => {
                    match sync_removed_blob(ctx, db_ref, passport, key.clone(), timestamp).await {
                        Ok(()) => {}
                        Err(()) => return Err(db_error()),
                    }
                }
                ModifiedTime::Invalid => continue,
            }

            stored_keys.push(key);
        }
    }

    Ok(())
}
