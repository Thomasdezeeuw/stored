use std::collections::HashSet;
use std::convert::TryInto;
use std::fmt;
use std::future::Future;
use std::io::{self, IoSlice};
use std::mem::{replace, size_of, take, MaybeUninit};
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{self, Poll};
use std::time::Duration;

use futures_io::AsyncWrite;
use futures_util::future::poll_fn;
use heph::net::TcpStream;
use heph::rt::RuntimeAccess;
use heph::timer::Timer;
use heph::{actor, ActorRef};
use log::{debug, error, trace, warn};

use crate::buffer::{BufView, Buffer};
use crate::db::{self, db_error};
use crate::op::{db_rpc, DbRpc};
use crate::peer::server::{
    BLOB_LENGTH_LEN, METADATA_LEN, NO_MORE_KEYS, N_KEYS, REQUEST_BLOB, REQUEST_KEYS,
    REQUEST_MORE_KEYS,
};
use crate::peer::Peers;
use crate::storage::{DateTime, Keys, ModifiedTime};
use crate::util::wait_for_wakeup;
use crate::{Describe, Key};

const GET_KEYS_TIMEOUT: Duration = Duration::from_secs(30);
const RETRIEVE_BLOBS_TIMEOUT: Duration = Duration::from_secs(30);

// FIXME: what happens to the consensus algorithm when we're not synced and a
// request is made to remove a blob?

// FIXME: handle the case where the peers have removed a blob and is still
// stored locally.

/// Run a full sync of the stored blob.
///
/// # Notes
///
/// The `peers` should be connected to all known peers.
pub async fn full_sync<M, K>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    peers: &Peers,
    buf: &mut Buffer,
) -> Result<(), ()>
where
    K: RuntimeAccess,
{
    debug_assert!(
        peers.all_connected(),
        "called `full_sync` with not all peers connected"
    );

    let addresses = peers.addresses();
    let mut conns = Vec::with_capacity(addresses.len());
    for address in addresses {
        match Connection::connect(ctx, address) {
            Ok(conn) => conns.push(conn),
            // TODO: try again here?
            Err(err) => {
                error!(
                    "error connecting to peer, ignoring the peer in synchronisation: {}: \
                    remote_address={}",
                    err, address
                );
                continue;
            }
        }
    }

    let mut peer_keys = get_known_keys(ctx, &mut conns, buf).await;

    // Remove any connection that failed.
    // FIXME: reconnect the connections.
    conns.retain(Connection::is_complete);

    let stored_keys: Keys = db_rpc(ctx, db_ref, ())?.await?;

    // NOTE: "stored" is used below to mean either that a blob is stored, or
    // stored and removed, i.e any blob for which we have an index entry.

    // Blobs stored by us, but not by any peers.
    let extra_keys = stored_keys
        .into_iter()
        // `remove` returns `true` if the key is in `peer_keys`.
        .filter(|key| !peer_keys.remove(key))
        .cloned()
        .collect();

    // Any keys left we in `extra_keys` means that we store the key, but our
    // peers didn't.
    share_blobs(db_ref, &mut conns, buf, extra_keys).await;

    // Any blobs not removed from `peer_keys` are missing from our local
    // storage.
    retrieve_blobs(ctx, db_ref, conns, buf, peer_keys).await;

    Ok(())
}

#[derive(Debug)]
struct Connection<S> {
    stream: TcpStream,
    address: SocketAddr,
    state: S,
}

trait State {
    fn is_complete(&self) -> bool;
}

impl<S> Connection<S>
where
    S: State,
{
    fn connect<M, K>(
        ctx: &mut actor::Context<M, K>,
        address: SocketAddr,
    ) -> io::Result<Connection<S>>
    where
        K: RuntimeAccess,
        S: Default,
    {
        TcpStream::connect(ctx, address).and_then(|mut stream| {
            // Set `TCP_NODELAY` as we send single byte requests.
            stream.set_nodelay(true).map(|()| Connection {
                stream,
                address,
                state: S::default(),
            })
        })
    }

    /// Return `true` if the connection is in the `Complete` state.
    fn is_complete(&self) -> bool {
        self.state.is_complete()
    }
}

/// State of a [`Connection`] for [`get_known_keys`].
#[derive(Copy, Clone, Debug)]
enum GetKeysState {
    /// Initial state.
    ///
    /// Write the request to the stream.
    Init,
    /// Waiting for a response from the peer. `usize` is the number of response
    /// retrieved so far.
    ///
    /// Read the response from the stream.
    WaitingResponse(usize),
    /// Need to request more keys from the peer.
    ///
    /// Write the request for more to the stream.
    RequestingMore,
    /// Operation is complete.
    Complete,
    /// Previously returned an I/O error.
    Failed,
}

impl State for GetKeysState {
    fn is_complete(&self) -> bool {
        matches!(self, GetKeysState::Complete)
    }
}

impl Default for GetKeysState {
    fn default() -> GetKeysState {
        GetKeysState::Init
    }
}

impl Connection<GetKeysState> {
    fn get_keys<'c, 'k, 'b>(
        &'c mut self,
        keys: &'k mut HashSet<Key>,
        buf: &'b mut Buffer,
    ) -> GetKeys<'c, 'k, 'b> {
        GetKeys {
            conn: self,
            keys,
            buf,
        }
    }

    fn prepare_retrieve_blobs(self) -> Connection<RetrieveBlobsSate> {
        Connection {
            stream: self.stream,
            address: self.address,
            state: self.state.into(),
        }
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
struct GetKeys<'c, 'k, 'b> {
    conn: &'c mut Connection<GetKeysState>,
    keys: &'k mut HashSet<Key>,
    buf: &'b mut Buffer,
}

// Keep in sync with `COORDINATOR_MAGIC` in `peer` module.
#[rustfmt::skip]
const REQUEST_BYTES: &[u8] = &[
    // "Stored coordinate\0".
    83, 116, 111, 114, 101, 100, 32, 99, 111, 111, 114, 100, 105, 110, 97, 116, 101, 0,
    // Request all keys.
    REQUEST_KEYS,
];

/// Future state functions.
impl<'c, 'k, 'b> GetKeys<'c, 'k, 'b> {
    fn write_initial_request(
        mut self: Pin<&mut Self>,
        ctx: &mut task::Context,
    ) -> Poll<crate::Result<()>> {
        debug!(
            "writing keys requests to connection: remote_address={}",
            self.conn.address
        );
        debug_assert!(matches!(self.conn.state, GetKeysState::Init));

        match Pin::new(&mut self.conn.stream).poll_write(ctx, &REQUEST_BYTES) {
            Poll::Ready(Ok(n)) if n == REQUEST_BYTES.len() => {
                // We're sending a small amount of bytes, so set no delay.
                if let Err(err) = self.conn.stream.set_nodelay(true) {
                    warn!("failed to set no delay, continuing: {}", err);
                }

                // Advance to the next state.
                self.conn.state = GetKeysState::WaitingResponse(0);
                self.poll(ctx)
            }
            Poll::Ready(Ok(..)) => {
                self.fail(io::Error::from(io::ErrorKind::WriteZero).describe("writing request"))
            }
            Poll::Ready(Err(err)) => self.fail(err.describe("writing request")),
            Poll::Pending => Poll::Pending,
        }
    }

    fn read_response(mut self: Pin<&mut Self>, ctx: &mut task::Context) -> Poll<crate::Result<()>> {
        let GetKeys { conn, keys, buf } = &mut *self;

        loop {
            debug!(
                "reading response from connection: remote_address={}",
                conn.address
            );
            debug_assert!(matches!(conn.state, GetKeysState::WaitingResponse(..)));

            match Pin::new(&mut buf.read_from(&mut conn.stream)).poll(ctx) {
                Poll::Ready(Ok(0)) => {
                    return self.fail(
                        io::Error::from(io::ErrorKind::UnexpectedEof).describe("reading response"),
                    )
                }
                Poll::Ready(Ok(1)) if buf.next_byte() == Some(NO_MORE_KEYS) => {
                    debug!("retrieved all keys: remote_address={}", conn.address);
                    // Read all keys the peer stored.
                    conn.state = GetKeysState::Complete;
                    buf.processed(1);
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Ok(..)) => {
                    // Add all keys to our set.
                    let mut n_keys = 0;
                    for key_bytes in buf.as_bytes().chunks(Key::LENGTH) {
                        if key_bytes.len() != Key::LENGTH {
                            let no_more_keys = key_bytes == [NO_MORE_KEYS];
                            buf.processed(buf.len());

                            return if no_more_keys {
                                debug!("retrieved all keys: remote_address={}", conn.address);
                                // No more keys, so we're done.
                                conn.state = GetKeysState::Complete;
                                Poll::Ready(Ok(()))
                            } else {
                                self.fail(
                                    io::Error::from(io::ErrorKind::InvalidData)
                                        .describe("invalid length when reading keys"),
                                )
                            };
                        }

                        // Safety: we've ensure that the chunks are
                        // correctly sized.
                        let key = Key::from_bytes(key_bytes);
                        keys.get_or_insert_owned(key);
                        n_keys += 1;
                    }

                    buf.processed(buf.len());

                    match conn.state {
                        GetKeysState::WaitingResponse(ref mut cnt) => {
                            *cnt += n_keys;
                            if *cnt >= N_KEYS {
                                // Read all keys, request some more.
                                conn.state = GetKeysState::RequestingMore;
                                return self.poll(ctx);
                            }
                            // Haven't yet read all send keys.
                        }
                        _ => unreachable!("invalid `GetKeys` state"),
                    }
                }
                Poll::Ready(Err(err)) => return self.fail(err.describe("reading response")),
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    fn write_more_keys_request(
        mut self: Pin<&mut Self>,
        ctx: &mut task::Context,
    ) -> Poll<crate::Result<()>> {
        debug!(
            "writing more keys request to connection: remote_address={}",
            self.conn.address
        );
        debug_assert!(matches!(self.conn.state, GetKeysState::RequestingMore));

        match Pin::new(&mut self.conn.stream).poll_write(ctx, &[REQUEST_MORE_KEYS]) {
            Poll::Ready(Ok(1)) => {
                // Advance to the next state.
                self.conn.state = GetKeysState::WaitingResponse(0);
                self.poll(ctx)
            }
            Poll::Ready(Ok(..)) => self.fail(
                io::Error::from(io::ErrorKind::WriteZero).describe("writing more keys request"),
            ),
            Poll::Ready(Err(err)) => self.fail(err.describe("writing more keys request")),
            Poll::Pending => Poll::Pending,
        }
    }

    /// Fail the future, setting the state to `Failed` and return `err`.
    fn fail(&mut self, err: crate::Error) -> Poll<crate::Result<()>> {
        self.conn.state = GetKeysState::Failed;
        Poll::Ready(Err(err))
    }
}

impl<'c, 'k, 'b> Future for GetKeys<'c, 'k, 'b> {
    type Output = crate::Result<()>;

    fn poll(self: Pin<&mut Self>, ctx: &mut task::Context) -> Poll<Self::Output> {
        match self.conn.state {
            GetKeysState::Init => self.write_initial_request(ctx),
            GetKeysState::WaitingResponse(..) => self.read_response(ctx),
            GetKeysState::RequestingMore => self.write_more_keys_request(ctx),
            GetKeysState::Complete | GetKeysState::Failed => Poll::Ready(Ok(())),
        }
    }
}

/// Wrapper to log all failed connections.
struct FailedConnections<I>(I);

impl<'a, I, S: 'a> fmt::Display for FailedConnections<I>
where
    I: Iterator<Item = &'a Connection<S>> + Clone,
    S: State,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[")?;
        let mut first = true;
        for conn in self.0.clone() {
            if !conn.is_complete() {
                if !first {
                    f.write_str(", ")?;
                }
                first = false;
                conn.address.fmt(f)?;
            }
        }
        f.write_str("]")
    }
}

/// Get all keys known to our `peers`.
async fn get_known_keys<M, K>(
    ctx: &mut actor::Context<M, K>,
    conns: &mut [Connection<GetKeysState>],
    buf: &mut Buffer,
) -> HashSet<Key>
where
    K: RuntimeAccess,
{
    let timer = Timer::timeout(ctx, GET_KEYS_TIMEOUT);
    let mut keys = HashSet::new();
    loop {
        // This is more complex then it needs to be, but here we are...
        //
        // We need to call the `GetKeys` future only once, not `await`ing until
        // its complete, as that would waste time waiting for each connection
        // one by one. But to call the future once we need a `task::Context`,
        // hence we need `poll_fn`.
        let done = poll_fn(|ctx| {
            let mut done = true;
            for conn in conns.iter_mut() {
                // Try to advance the state of the connection just once, not
                // waiting for one connection to complete before checking the
                // next.
                // NOTE: we're polling not ready and already completed `Future`s
                // here, not the most efficient way (event technically
                // unspecified behaviour), but with a low number of peers its
                // fine.
                match Pin::new(&mut conn.get_keys(&mut keys, buf)).poll(ctx) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(err)) => {
                        // TODO: try again?
                        warn!(
                            "error synchronising stored blobs with peers, \
                            continuing with other peers: {}: remote_address={}",
                            err, conn.address
                        );
                    }
                    // Incomplete, try again later.
                    Poll::Pending => done = false,
                }
            }
            // Always return `Poll::Ready` since we're looping.
            Poll::Ready(done)
        })
        .await;

        if done {
            break;
        }

        if timer.has_passed() {
            warn!(
                "getting the set of stored blobs timed out, \
                continuing with current set: failed_peers={}",
                FailedConnections(conns.iter())
            );
            break;
        }

        // We get scheduled once one of the stream is ready to read or write, or
        // if the deadline passed.
        wait_for_wakeup().await;
    }

    keys
}

/// Request all `peers` to store the blobs in `keys`.
async fn share_blobs(
    _db_ref: &mut ActorRef<db::Message>,
    _conns: &mut [Connection<GetKeysState>],
    _buf: &mut Buffer,
    _keys: Vec<Key>,
) {
    // FIXME: todo.
}

/// State of a [`Connection`] for [`retrieve_blobs`].
#[derive(Debug)]
enum RetrieveBlobsSate {
    /// Request blobs.
    ///
    /// Writes the request to the stream.
    Requesting,
    /// Waiting for a response from the peer. `usize` is the number of response
    /// left to receive.
    ///
    /// Reads the blobs from the stream.
    WaitingResponse(usize),
    /// Holds the number of responses left to receive (same as
    /// `WaitingResponse`) and the database RPC we're currently handling.
    WaitingDbSyncStored(usize, DbRpc<BufView>),
    /// Same as `WaitingDbSyncStored`, but then for syncing a removed blob.
    WaitingDbSyncRemoved(usize, DbRpc<()>),
    /// Operation is complete.
    Complete,
    /// Previously returned an I/O error.
    Failed,
}

impl State for RetrieveBlobsSate {
    fn is_complete(&self) -> bool {
        matches!(self, RetrieveBlobsSate::Complete)
    }
}

impl From<GetKeysState> for RetrieveBlobsSate {
    fn from(prev_state: GetKeysState) -> RetrieveBlobsSate {
        match prev_state {
            GetKeysState::Complete => RetrieveBlobsSate::Requesting,
            _ => RetrieveBlobsSate::Failed,
        }
    }
}

impl Connection<RetrieveBlobsSate> {
    fn retrieve_blobs<'c, 'cn, 'k, 'b, 'd, 'f, M, K>(
        &'cn mut self,
        ctx: &'c mut actor::Context<M, K>,
        keys: &'k mut Vec<Key>,
        buf: &'b mut Buffer,
        db_ref: &'d mut ActorRef<db::Message>,
        failed: &'f mut Vec<Key>,
    ) -> RetrieveBlob<'c, 'cn, 'k, 'b, 'd, 'f, M, K> {
        RetrieveBlob {
            ctx,
            conn: self,
            keys,
            buf,
            db_ref,
            failed,
        }
    }

    /// Return `true` if the connection is in the `Failed` state.
    fn has_failed(&self) -> bool {
        matches!(self.state, RetrieveBlobsSate::Failed)
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
struct RetrieveBlob<'c, 'cn, 'k, 'b, 'd, 'f, M, K> {
    ctx: &'c mut actor::Context<M, K>,
    conn: &'cn mut Connection<RetrieveBlobsSate>,
    keys: &'k mut Vec<Key>,
    buf: &'b mut Buffer,
    db_ref: &'d mut ActorRef<db::Message>,
    failed: &'f mut Vec<Key>,
}

impl<'c, 'cn, 'k, 'b, 'd, 'f, M, K> RetrieveBlob<'c, 'cn, 'k, 'b, 'd, 'f, M, K>
where
    K: RuntimeAccess,
{
    fn write_blob_requests(
        mut self: Pin<&mut Self>,
        ctx: &mut task::Context,
    ) -> Poll<crate::Result<()>> {
        if self.keys.is_empty() {
            self.conn.state = RetrieveBlobsSate::Complete;
            return Poll::Ready(Ok(()));
        };

        debug!(
            "requesting keys from peer: remote_address={}",
            self.conn.address
        );

        let RetrieveBlob { conn, keys, .. } = &mut *self;

        // Request a maximum of 5 keys at a time.
        let mut bufs: [MaybeUninit<IoSlice>; 10] = MaybeUninit::uninit_array();
        let mut length = 0;
        for (key, i) in keys.iter().take(bufs.len() / 2).zip((0..).step_by(2)) {
            bufs[i] = MaybeUninit::new(IoSlice::new(&[REQUEST_BLOB]));
            bufs[i + 1] = MaybeUninit::new(IoSlice::new(key.as_bytes()));
            length += 2;
        }

        // Safety: initialised up to `length` elements in `bufs` above.
        let bufs = unsafe { MaybeUninit::slice_get_ref(&bufs[..length]) };

        let want_length = (length / 2) * (1 + Key::LENGTH);
        match Pin::new(&mut conn.stream).poll_write_vectored(ctx, bufs) {
            Poll::Ready(Ok(n)) if n == want_length => {
                // Advance to the next state.
                conn.state = RetrieveBlobsSate::WaitingResponse(length);
                self.poll(ctx)
            }
            Poll::Ready(Ok(..)) => {
                self.fail(io::Error::from(io::ErrorKind::WriteZero).describe("writing request"))
            }
            Poll::Ready(Err(err)) => self.fail(err.describe("writing request")),
            Poll::Pending => Poll::Pending,
        }
    }

    fn read_responses(
        mut self: Pin<&mut Self>,
        ctx: &mut task::Context,
    ) -> Poll<crate::Result<()>> {
        debug!(
            "reading responses from peer: remote_address={}",
            self.conn.address
        );

        // Number of bytes we want to read after we exit this loop.
        let mut want_read = METADATA_LEN;
        while self.buf.len() >= METADATA_LEN {
            // Safety: checked above if we read enough bytes so indexing won't
            // panic.
            let bytes = self.buf.as_bytes();
            let timestamp =
                DateTime::from_bytes(&bytes[0..size_of::<DateTime>()]).unwrap_or(DateTime::INVALID);
            let blob_length_bytes = bytes[..BLOB_LENGTH_LEN].try_into().unwrap();
            let blob_length = u64::from_be_bytes(blob_length_bytes);
            trace!("read blob length: length={}", blob_length);

            if (self.buf.len() as u64) < blob_length {
                // Haven't yet read the entire blob, we do that below.
                want_read = blob_length as usize;
                break;
            }

            let cnt = match self.conn.state {
                RetrieveBlobsSate::WaitingResponse(cnt, ..) => cnt,
                _ => unreachable!("invalid `RetrieveBlob` state"),
            };

            self.buf.processed(METADATA_LEN);
            let RetrieveBlob {
                ctx: c,
                db_ref,
                conn,
                buf,
                keys,
                ..
            } = &mut *self;

            match timestamp.into() {
                ModifiedTime::Created(timestamp) => {
                    let view = replace(*buf, Buffer::empty()).view(blob_length as usize);
                    match db_rpc(c, db_ref, (view, timestamp)) {
                        Ok(rpc) => {
                            conn.state = RetrieveBlobsSate::WaitingDbSyncStored(cnt, rpc);
                            return self.sync_stored(ctx);
                        }
                        Err(()) => return Poll::Ready(Err(db_error())),
                    }
                }
                ModifiedTime::Removed(timestamp) => {
                    match db_rpc(c, db_ref, (keys[0].clone(), timestamp)) {
                        Ok(rpc) => {
                            conn.state = RetrieveBlobsSate::WaitingDbSyncRemoved(cnt, rpc);
                            return self.sync_removed(ctx);
                        }
                        Err(()) => return Poll::Ready(Err(db_error())),
                    }
                }
                ModifiedTime::Invalid => {
                    // This can happen if the peer isn't fully synced, in
                    // which case we let another peer handle the request for us.
                    let failed_key = self.keys.remove(0);
                    self.failed.push(failed_key);
                }
            }
        }

        // Read at least the minimum amount of bytes we want, either the
        // `META_SIZE` or the length of the blob.
        loop {
            let RetrieveBlob { conn, buf, .. } = &mut *self;
            match Pin::new(&mut buf.read_from(&mut conn.stream)).poll(ctx) {
                Poll::Ready(Ok(0)) => {
                    break self.fail(
                        io::Error::from(io::ErrorKind::UnexpectedEof).describe("reading blob"),
                    )
                }
                // Read enough bytes to continue above (by calling ourself).
                Poll::Ready(Ok(n)) if n >= want_read => break self.read_responses(ctx),
                // Didn't read enough bytes, so read some more.
                Poll::Ready(Ok(..)) => continue,
                Poll::Ready(Err(err)) => break self.fail(err.describe("reading blob")),
                Poll::Pending => break Poll::Pending,
            }
        }
    }

    fn sync_stored(mut self: Pin<&mut Self>, ctx: &mut task::Context) -> Poll<crate::Result<()>> {
        let RetrieveBlob {
            conn, keys, buf, ..
        } = &mut *self;
        match &mut conn.state {
            RetrieveBlobsSate::WaitingDbSyncStored(cnt, rpc) => match Pin::new(rpc).poll(ctx) {
                Poll::Ready(Ok(view)) => {
                    // Mark the blob's bytes as processed and put back the buffer.
                    **buf = view.processed();

                    let blobs_left = *cnt - 1;
                    keys.remove(0);
                    if blobs_left == 0 {
                        self.conn.state = RetrieveBlobsSate::Requesting;
                        self.write_blob_requests(ctx)
                    } else {
                        self.conn.state = RetrieveBlobsSate::WaitingResponse(blobs_left);
                        self.read_responses(ctx)
                    }
                }
                Poll::Ready(Err(())) => Poll::Ready(Err(db_error())),
                Poll::Pending => Poll::Pending,
            },
            _ => unreachable!("invalid `RetrieveBlob` state"),
        }
    }

    fn sync_removed(mut self: Pin<&mut Self>, ctx: &mut task::Context) -> Poll<crate::Result<()>> {
        let RetrieveBlob { conn, keys, .. } = &mut *self;
        match &mut conn.state {
            RetrieveBlobsSate::WaitingDbSyncRemoved(cnt, rpc) => match Pin::new(rpc).poll(ctx) {
                Poll::Ready(Ok(())) => {
                    let blobs_left = *cnt - 1;
                    keys.remove(0);
                    if blobs_left == 0 {
                        self.conn.state = RetrieveBlobsSate::Requesting;
                        self.write_blob_requests(ctx)
                    } else {
                        self.conn.state = RetrieveBlobsSate::WaitingResponse(blobs_left);
                        self.read_responses(ctx)
                    }
                }
                Poll::Ready(Err(())) => Poll::Ready(Err(db_error())),
                Poll::Pending => Poll::Pending,
            },
            _ => unreachable!("invalid `RetrieveBlob` state"),
        }
    }

    /// Fail the future, setting the state to `Failed` and return `err`.
    fn fail(&mut self, err: crate::Error) -> Poll<crate::Result<()>> {
        self.conn.state = RetrieveBlobsSate::Failed;
        Poll::Ready(Err(err))
    }
}

impl<'c, 'cn, 'k, 'b, 'd, 'f, M, K> Future for RetrieveBlob<'c, 'cn, 'k, 'b, 'd, 'f, M, K>
where
    K: RuntimeAccess,
{
    type Output = crate::Result<()>;

    fn poll(self: Pin<&mut Self>, ctx: &mut task::Context) -> Poll<Self::Output> {
        match self.conn.state {
            RetrieveBlobsSate::Requesting => self.write_blob_requests(ctx),
            RetrieveBlobsSate::WaitingResponse(..) => self.read_responses(ctx),
            RetrieveBlobsSate::WaitingDbSyncStored(..) => self.sync_stored(ctx),
            RetrieveBlobsSate::WaitingDbSyncRemoved(..) => self.sync_removed(ctx),
            RetrieveBlobsSate::Complete | RetrieveBlobsSate::Failed => Poll::Ready(Ok(())),
        }
    }
}

/// Retrieve all blobs in `keys` for our `peers` and store them locally.
async fn retrieve_blobs<M, K>(
    ctx: &mut actor::Context<M, K>,
    db_ref: &mut ActorRef<db::Message>,
    conns: Vec<Connection<GetKeysState>>,
    buf: &mut Buffer,
    keys: HashSet<Key>,
) where
    K: RuntimeAccess,
{
    if keys.is_empty() {
        debug!("no blobs to retrieve from peers");
        return;
    } else if conns.is_empty() {
        error!("can not receive blobs from peers, all connections to our peers have failed");
    }
    debug!("retrieving blobs from peers");

    let partitions = partition(keys, conns.len());
    trace!(
        "retrieving blobs, paritions ({}): {:?}",
        partitions.len(),
        partitions
    );

    debug_assert_eq!(conns.len(), partitions.len());
    let mut conns: Vec<(_, Vec<Key>)> = conns
        .into_iter()
        .zip(partitions)
        .map(|(conn, keys)| (conn.prepare_retrieve_blobs(), keys))
        .collect();

    let timer = Timer::timeout(ctx, RETRIEVE_BLOBS_TIMEOUT);
    // List of keys we failed to retrieve.
    let mut failed = Vec::new();
    loop {
        let done = poll_fn(|task_ctx| {
            let mut done = true;
            for (conn, keys) in conns.iter_mut() {
                if conn.has_failed() {
                    continue;
                }

                if keys.is_empty() {
                    if let Some(key) = failed.pop() {
                        keys.push(key);
                    } else {
                        // Retrieved all keys.
                        continue;
                    }
                }

                match Pin::new(&mut conn.retrieve_blobs(ctx, keys, buf, db_ref, &mut failed))
                    .poll(task_ctx)
                {
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(err)) => {
                        warn!(
                            "error retrieving blob(s) from peer, \
                                continuing with other peers: {}: remote_address={}",
                            err, conn.address
                        );
                        failed.extend(take(keys));

                        // FIXME: this could lead to awaiting for ever if all
                        // other blobs are already retrieved.
                        done = false;
                    }
                    // Incomplete, try again later.
                    Poll::Pending => done = false,
                }
            }

            // Always return `Poll::Ready` since we're looping.
            Poll::Ready(done)
        })
        .await;

        if done {
            break;
        }

        if timer.has_passed() {
            for (_, keys) in conns.iter_mut() {
                failed.extend(take(keys));
            }
            break;
        }

        // We get scheduled once one of the stream is ready to read or write, or
        // if the deadline passed.
        wait_for_wakeup().await;
    }

    if !failed.is_empty() {
        // FIXME: request keys using different connections.
        error!(
            "retrieving blobs timed out: failed_peers={}, n_missing_keys={}, missing_keys={}",
            FailedConnections(conns.iter().map(|(conn, _)| conn)),
            failed.len(),
            Key::display_keys(failed.iter()),
        );
    }
}

/// Partitions `keys`, returning a list of `n` lists of keys.
fn partition(keys: HashSet<Key>, n: usize) -> Vec<Vec<Key>> {
    debug_assert!(n != 0, "called `partition` with 0");
    let mut size = keys.len() / n;
    if size * n != keys.len() {
        // If we don't have an even split the first couple of partitions will do
        // some extra work.
        size += 1;
    }

    let mut iter = keys.into_iter();
    (0..n).map(|_| (&mut iter).take(size).collect()).collect()
}
