//! Module with the [peer server actor].
//!
//! Peers connect to this actor to retrieve stored blobs and keys.
//!
//! [peer server actor]: actor()

use std::convert::TryInto;
use std::io::{self, IoSlice, Write};
use std::mem::{replace, size_of};
use std::net::SocketAddr;
use std::time::{Duration, SystemTime};

use futures_util::io::AsyncWriteExt;
use heph::actor::context::ThreadSafe;
use heph::net::TcpStream;
use heph::timer::Deadline;
use heph::{actor, ActorRef};
use log::{debug, warn};

use crate::buffer::Buffer;
use crate::db::{self, db_error};
use crate::error::Describe;
use crate::op::{self, sync_removed_blob, sync_stored_blob};
use crate::passport::{Event, Passport, Uuid};
use crate::storage::{self, BlobEntry, DateTime, ModifiedTime};
use crate::Key;

/// The length (in bytes) that make the metadata that prefixes the blob send.
pub const METADATA_LEN: usize = DATE_TIME_LEN + BLOB_LENGTH_LEN;

/// The length (in bytes) for the timestamp.
pub const DATE_TIME_LEN: usize = size_of::<DateTime>();

/// Type used to send the length of the blob over the write in big endian.
pub type BlobLength = u64;

/// The length (in bytes) that make up the length of the blob.
pub const BLOB_LENGTH_LEN: usize = size_of::<BlobLength>();

/// Length send if the blob is not found.
pub const NO_BLOB: [u8; BLOB_LENGTH_LEN] = 0u64.to_be_bytes();

/// Type used to send the size of the set of known keys over the write in big
/// endian.
pub type KeysSetSize = u64;

/// The length (in bytes) that make up the size of the key set
pub const KEY_SET_SIZE_LEN: usize = size_of::<KeysSetSize>();

/// Size of the set send if no blobs are stored.
pub const NO_KEYS: [u8; KEY_SET_SIZE_LEN] = 0u64.to_be_bytes();

/// Request type to retrieve a blob.
pub const REQUEST_BLOB: u8 = 1;
/// Request type to retrieve all keys.
pub const REQUEST_KEYS: u8 = 2;
/// Request type to retrieve all keys since a certain date.
pub const REQUEST_KEYS_SINCE: u8 = 3;
/// Request type to store a blob, used by the synchronisation process.
pub const STORE_BLOB: u8 = 4;

/// Timeout used for I/O.
const IO_TIMEOUT: Duration = Duration::from_secs(5);

/// Timeout used to keep the connection alive.
const ALIVE_TIMEOUT: Duration = Duration::from_secs(120);

/// Function to change the log target and module for the warning message,
/// the [`peer::switcher`] has little to do with the error.
pub(crate) async fn run_actor<M>(
    ctx: actor::Context<M, ThreadSafe>,
    stream: TcpStream,
    buf: Buffer,
    db_ref: ActorRef<db::Message>,
    server: SocketAddr,
    remote: SocketAddr,
) {
    if let Err(err) = actor(ctx, stream, buf, db_ref).await {
        warn!(
            "peer server failed: {}: remote_address=\"{}\", server_address=\"{}\"",
            err, remote, server
        );
    }
}

/// Actor that serves the synchronisation process and the
/// [`participant::consensus`] actor in retrieving keys and (uncommitted) blobs.
///
/// Expects to read a request byte, one of the `REQUEST_*` constants.
/// * For [`REQUEST_BLOB`] it expects a [`Key`] (as bytes) on the `stream`,
///   responding with metadata ([`DateTime`] and length as `u64`) followed by
///   the bytes that make up the [`Blob`]. If the blob is not found (not
///   committed or uncommitted) it writes [`DateTime::INVALID`] and a blob of
///   length 0. For uncommitted blob the timestamp will also be
///   [`DateTime::INVALID`], but the length non-zero.
/// * [`REQUEST_KEYS`] returns a stream of [`Key`]s, prefixed with the length as
///   `u64`.
/// * [`REQUEST_KEYS_SINCE`] returns a stream of [`Key`]s, prefixed with the length as
///   `u64`.
/// * [`STORE_BLOB`] expects the [`Key`], metadata ([`DateTime`] and length as
///   `u64`) and the blob to store. Supports both actually stored blobs and
///   removed blobs.
///
/// [`participant::consensus`]: crate::peer::participant::consensus
/// [`Blob`]: crate::storage::Blob
pub async fn actor<M>(
    mut ctx: actor::Context<M, ThreadSafe>,
    mut stream: TcpStream,
    mut buf: Buffer,
    mut db_ref: ActorRef<db::Message>,
) -> crate::Result<()> {
    debug!("starting peer server");

    // We buffer larger all responses, so set no delay.
    if let Err(err) = stream.set_nodelay(true) {
        warn!("failed to set no delay, continuing: {}", err);
    }

    // TODO: read request-id from peer?
    let mut passport = Passport::new();
    passport.mark(Event::ReadingPeerRequest);
    loop {
        // NOTE: we don't create `buf` ourselves so it could be that it already
        // contains a request, so check it first and only after read some more
        // bytes.
        while let Some(request_byte) = buf.next_byte() {
            passport.mark(Event::ReadPeerRequest);
            match request_byte {
                REQUEST_BLOB => {
                    buf.processed(1);
                    retrieve_blob(&mut ctx, &mut stream, &mut passport, &mut buf, &mut db_ref)
                        .await?;
                }
                REQUEST_KEYS => {
                    buf.processed(1);
                    retrieve_keys(&mut ctx, &mut stream, &mut passport, &mut buf, &mut db_ref)
                        .await?;
                }
                REQUEST_KEYS_SINCE => {
                    buf.processed(1);
                    retrieve_keys_since(
                        &mut ctx,
                        &mut stream,
                        &mut passport,
                        &mut buf,
                        &mut db_ref,
                    )
                    .await?;
                }
                STORE_BLOB => {
                    buf.processed(1);
                    store_blob(&mut ctx, &mut stream, &mut passport, &mut buf, &mut db_ref).await?;
                }
                byte => {
                    // Invalid byte. Forcefully close the connection, letting
                    // the peer known there's an error.
                    warn!(
                        "unexpected request from peer (byte: '{}'), closing connection",
                        byte
                    );
                    return Ok(());
                }
            }
            passport.reset();
            passport.set_id(Uuid::new());
            passport.mark(Event::ReadingPeerRequest);
        }

        // Read some more bytes.
        match Deadline::timeout(&mut ctx, ALIVE_TIMEOUT, buf.read_from(&mut stream)).await {
            Ok(0) => return Ok(()),
            Ok(..) => {}
            Err(err) => return Err(err.describe("reading from socket")),
        }
    }
}

#[derive(Debug)]
enum Blob {
    Committed(storage::Blob),
    Uncommitted(storage::UncommittedBlob),
    NotFound,
}

impl Blob {
    fn bytes(&self) -> &[u8] {
        use Blob::*;
        match self {
            Committed(blob) => blob.bytes(),
            Uncommitted(blob) => blob.bytes(),
            NotFound => &[],
        }
    }

    fn len(&self) -> usize {
        use Blob::*;
        match self {
            Committed(blob) => blob.len(),
            Uncommitted(blob) => blob.len(),
            NotFound => 0,
        }
    }
}

/// Expects to read a [`Key`] on the `stream` and writes the metadata and blob
/// bytes to it.
async fn retrieve_blob<M>(
    ctx: &mut actor::Context<M, ThreadSafe>,
    stream: &mut TcpStream,
    passport: &mut Passport,
    buf: &mut Buffer,
    db_ref: &mut ActorRef<db::Message>,
) -> crate::Result<()> {
    passport.mark(Event::ReadingPeerKey);
    if buf.len() < Key::LENGTH {
        let n = Key::LENGTH - buf.len();
        match Deadline::timeout(ctx, IO_TIMEOUT, buf.read_n_from(&mut *stream, n)).await {
            Ok(..) => passport.mark(Event::ReadPeerKey),
            Err(err) => return Err(err.describe("reading key of blob to retrieve")),
        }
    }

    // SAFETY: checked length above, so indexing is safe.
    let key = Key::from_bytes(&buf.as_bytes()[..Key::LENGTH]).to_owned();
    buf.processed(Key::LENGTH);
    debug!(
        "retrieving blob for peer: request_id=\"{}\", key=\"{}\"",
        passport.id(),
        key
    );

    let (blob, timestamp) = match op::retrieve_uncommitted_blob(ctx, db_ref, passport, key).await {
        Ok(Ok(blob)) => {
            if let Err(err) = blob.prefetch() {
                warn!(
                    "error prefetching uncommitted blob, continuing: {}: request_id=\"{}\"",
                    err,
                    passport.id()
                );
            }
            (Blob::Uncommitted(blob), DateTime::INVALID)
        }
        Ok(Err(Some(BlobEntry::Stored(blob)))) => {
            if let Err(err) = blob.prefetch() {
                warn!(
                    "error prefetching committed blob, continuing: {}: request_id=\"{}\"",
                    err,
                    passport.id()
                );
            }
            let created_at = blob.created_at().into();
            (Blob::Committed(blob), created_at)
        }
        Ok(Err(Some(BlobEntry::Removed(removed_at)))) => (Blob::NotFound, removed_at.into()),
        Ok(Err(None)) => (Blob::NotFound, DateTime::INVALID),
        Err(()) => return Err(db_error()),
    };

    passport.mark(Event::WritingPeerResponse);
    let length: [u8; BLOB_LENGTH_LEN] = (blob.len() as u64).to_be_bytes();
    let bufs = &mut [
        IoSlice::new(timestamp.as_bytes()),
        IoSlice::new(&length),
        IoSlice::new(blob.bytes()),
    ];
    Deadline::timeout(ctx, IO_TIMEOUT, stream.write_all_vectored(bufs))
        .await
        .map(|()| passport.mark(Event::WrittenPeerResponse))
        .map_err(|err| err.describe("writing blob"))
}

/// Writes all [`Key`]s stored at the time of calling this function, prefixed
/// with the length as `u64`.
async fn retrieve_keys<M>(
    ctx: &mut actor::Context<M, ThreadSafe>,
    stream: &mut TcpStream,
    passport: &mut Passport,
    buf: &mut Buffer,
    db_ref: &mut ActorRef<db::Message>,
) -> crate::Result<()> {
    debug!("retrieving keys for peer: request_id=\"{}\"", passport.id());

    let keys = crate::op::retrieve_keys(ctx, db_ref, passport)
        .await
        .map_err(|()| db_error())?;
    let mut iter = keys.into_iter();
    let length = iter.len();

    /// The number of keys send at a time in [`REQUEST_KEYS`] request.
    // TODO: benchmark with larger sizes.
    const N_KEYS: usize = 100;

    passport.mark(Event::WritingPeerResponse);
    let mut first = true;
    loop {
        let mut wbuf = buf.split_write(BLOB_LENGTH_LEN + (N_KEYS * Key::LENGTH)).1;

        if first {
            // NOTE: writing to buffer never fails.
            let _ = wbuf.write(&u64::to_be_bytes(length as u64)).unwrap();
            first = false;
        }

        let mut iter = (&mut iter).take(N_KEYS);
        while let Some(key) = iter.next() {
            // NOTE: writing to buffer never fails.
            let bytes_written = wbuf.write(key.as_bytes()).unwrap();
            debug_assert_eq!(bytes_written, Key::LENGTH);
        }

        // Wrote all keys.
        if wbuf.is_empty() {
            passport.mark(Event::WrittenPeerResponse);
            return Ok(());
        }

        // TODO: use vectored I/O here using `Key::as_bytes` directly.
        Deadline::timeout(ctx, IO_TIMEOUT, stream.write_all(wbuf.as_bytes()))
            .await
            .map_err(|err| err.describe("writing keys"))?;
    }
}

async fn retrieve_keys_since<M>(
    ctx: &mut actor::Context<M, ThreadSafe>,
    stream: &mut TcpStream,
    passport: &mut Passport,
    buf: &mut Buffer,
    db_ref: &mut ActorRef<db::Message>,
) -> crate::Result<()> {
    debug!(
        "retrieving keys since for peer: request_id=\"{}\"",
        passport.id()
    );

    // Read the time before which we don't need to send the keys.
    if buf.len() < DATE_TIME_LEN {
        let n = DATE_TIME_LEN - buf.len();
        match Deadline::timeout(ctx, IO_TIMEOUT, buf.read_n_from(&mut *stream, n)).await {
            Ok(..) => {}
            Err(err) => return Err(err.describe("reading date since to retrieve keys")),
        }
    }
    let since = DateTime::from_bytes(buf.as_bytes()).unwrap_or(DateTime::INVALID);
    buf.processed(DATE_TIME_LEN);
    if since.is_invalid() {
        return Err(io::Error::from(io::ErrorKind::InvalidInput)
            .describe("received invalid date to retrieve keys from"));
    }
    let since: SystemTime = since.into();

    let keys = crate::op::retrieve_entries(ctx, db_ref, passport)
        .await
        .map_err(|()| db_error())?;
    let mut iter = keys.into_iter();

    /// The number of keys send at a time in [`REQUEST_KEYS_SINCE`] request.
    // TODO: benchmark with larger sizes.
    const N_KEYS: usize = 100;

    passport.mark(Event::WritingPeerResponse);
    loop {
        let mut wbuf = buf.split_write(BLOB_LENGTH_LEN + (N_KEYS * Key::LENGTH)).1;

        // Make space for the number of keys we're going to write, the actual
        // value is set further down.
        // NOTE: writing to buffer never fails.
        let _ = wbuf.write(&u64::to_be_bytes(0)).unwrap();

        // Write all the keys, if the date is after `since` (and not invalid).
        let mut iter = (&mut iter).take(N_KEYS);
        let mut length: u64 = 0;
        while let Some(entry) = iter.next() {
            if entry.modified_time().after(&since) {
                // NOTE: writing to buffer never fails.
                let bytes_written = wbuf.write(entry.key().as_bytes()).unwrap();
                debug_assert_eq!(bytes_written, Key::LENGTH);
                length += 1;
            }
        }

        // Wrote all keys.
        if length == 0 {
            // Write the last length, which is zero, to indicate no more keys
            // are coming.
            let res =
                Deadline::timeout(ctx, IO_TIMEOUT, stream.write_all(&u64::to_be_bytes(length)))
                    .await
                    .map_err(|err| err.describe("writing keys"));
            passport.mark(Event::WrittenPeerResponse);
            return res;
        }

        // Write the actual number of keys we're going to write.
        // Safety: we've made space above for the length.
        wbuf.as_mut_bytes()[0..KEY_SET_SIZE_LEN].copy_from_slice(&u64::to_be_bytes(length));

        // TODO: use vectored I/O here using `Key::as_bytes` directly.
        Deadline::timeout(ctx, IO_TIMEOUT, stream.write_all(wbuf.as_bytes()))
            .await
            .map_err(|err| err.describe("writing keys"))?;
    }
}

/// Expects to read the [`Key`], metadata of the blob (timestamp, length),
/// followed by the bytes that make up the blob. Writes nothing to the
/// connection.
async fn store_blob<M>(
    ctx: &mut actor::Context<M, ThreadSafe>,
    stream: &mut TcpStream,
    passport: &mut Passport,
    buf: &mut Buffer,
    db_ref: &mut ActorRef<db::Message>,
) -> crate::Result<()> {
    passport.mark(Event::ReadingPeerMetadata);
    // Read at least the metadata of the blob to store.
    if buf.len() < Key::LENGTH + METADATA_LEN {
        let n = (Key::LENGTH + METADATA_LEN) - buf.len();
        match Deadline::timeout(ctx, IO_TIMEOUT, buf.read_n_from(&mut *stream, n)).await {
            Ok(..) => passport.mark(Event::ReadPeerMetadata),
            Err(err) => return Err(err.describe("reading metadata from socket")),
        }
    }

    // Read the key, timestamp and blob length from the buffer.
    // Safety: ensured above that we read enough bytes so indexing and
    // `split_at` won't panic.
    let bytes = buf.as_bytes();
    // Key.
    let (key_bytes, bytes) = bytes.split_at(Key::LENGTH);
    let key = Key::from_bytes(key_bytes).to_owned();
    // Timestamp.
    let (timestamp_bytes, bytes) = bytes.split_at(size_of::<DateTime>());
    let timestamp = DateTime::from_bytes(timestamp_bytes).unwrap_or(DateTime::INVALID);
    // Blob length.
    let blob_length_bytes = bytes[0..BLOB_LENGTH_LEN].try_into().unwrap();
    let blob_length = u64::from_be_bytes(blob_length_bytes);
    buf.processed(Key::LENGTH + METADATA_LEN);

    debug!(
        "storing blob for peer: request_id=\"{}\", key=\"{}\", blob_length={}",
        passport.id(),
        key,
        blob_length,
    );

    // Read the entire blob.
    passport.mark(Event::ReadingPeerBlob);
    if buf.len() < (blob_length as usize) {
        let n = (blob_length as usize) - buf.len();
        match Deadline::timeout(ctx, IO_TIMEOUT, buf.read_n_from(&mut *stream, n)).await {
            Ok(..) => passport.mark(Event::ReadPeerBlob),
            Err(err) => return Err(err.describe("reading blob from socket")),
        }
    }

    match timestamp.into() {
        ModifiedTime::Created(timestamp) => {
            let view = replace(buf, Buffer::empty()).view(blob_length as usize);
            match sync_stored_blob(ctx, db_ref, passport, view, timestamp).await {
                Ok(view) => {
                    *buf = view.processed();
                    Ok(())
                }
                Err(()) => Err(db_error()),
            }
        }
        ModifiedTime::Removed(timestamp) => {
            match sync_removed_blob(ctx, db_ref, passport, key, timestamp).await {
                Ok(()) => Ok(()),
                Err(()) => Err(db_error()),
            }
        }
        ModifiedTime::Invalid => {
            warn!("peer wanted to a blob with an invalid timestamp");
            // TODO: do something more?
            Ok(())
        }
    }

    // TODO: add a response to the peer.
}
