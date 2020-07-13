//! Module with the [peer server actor].
//!
//! Peers connect to this actor to retrieve stored blobs and keys.
//!
//! [peer server actor]: actor()

use std::io::Write;
use std::mem::size_of;
use std::net::SocketAddr;
use std::time::Duration;

use futures_util::io::AsyncWriteExt;
use heph::actor::context::ThreadSafe;
use heph::net::TcpStream;
use heph::timer::Deadline;
use heph::{actor, ActorRef};
use log::{debug, warn};

use crate::buffer::{Buffer, INITIAL_BUF_SIZE};
use crate::db::{self, db_error};
use crate::error::Describe;
use crate::storage::{self, BlobEntry, DateTime};
use crate::{op, Key};

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

/// Request type to retrieve a blob.
pub const REQUEST_BLOB: u8 = 1;
/// Request type to retrieve all keys.
pub const REQUEST_KEYS: u8 = 2;
/// Request type to retrieve more keys, follow-up to [`REQUEST_KEYS`].
pub const REQUEST_MORE_KEYS: u8 = 3;

/// Byte send to indicate no more keys are coming, i.e. all keys are send, in a
/// response to [`REQUEST_MORE_KEYS`].
pub const NO_MORE_KEYS: u8 = 0;

/// The number of keys send at a time in [`REQUEST_KEYS`] request.
// TODO: benchmark with larger sizes.
pub const N_KEYS: usize = INITIAL_BUF_SIZE / Key::LENGTH;

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
            "peer server failed: {}: remote_address={}, server_address={}",
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
/// * [`REQUEST_KEYS`] returns a stream of [`Key`]s, writing [`N_KEYS`] keys at
///   a time, writing more keys once [`REQUEST_MORE_KEYS`] byte is read.
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

    loop {
        // NOTE: we don't create `buf` ourselves so it could be that it already
        // contains a request, so check it first and only after read some more
        // bytes.
        while let Some(request_byte) = buf.next_byte() {
            match request_byte {
                // Need `Key::LENGTH + 1` bytes (hence `>`).
                REQUEST_BLOB if buf.len() > Key::LENGTH => {
                    buf.processed(1);
                    retrieve_blob(&mut ctx, &mut stream, &mut buf, &mut db_ref).await?;
                }
                // Not enough bytes yet, read some more.
                REQUEST_BLOB => break,
                REQUEST_KEYS => {
                    buf.processed(1);
                    retrieve_keys(&mut ctx, &mut stream, &mut buf, &mut db_ref).await?;
                }
                _ => {
                    // Forcefully close the connection, letting the peer known
                    // there's an error.
                    return Ok(());
                }
            }
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
    buf: &mut Buffer,
    db_ref: &mut ActorRef<db::Message>,
) -> crate::Result<()> {
    let key = Key::from_bytes(&buf.as_bytes()[..Key::LENGTH]).to_owned();
    buf.processed(Key::LENGTH);
    debug!("got peer request for blob: key={}", key);

    let (blob, time) = match op::retrieve_uncommitted_blob(ctx, db_ref, key).await {
        Ok(Ok(blob)) => {
            if let Err(err) = blob.prefetch() {
                warn!("error prefetching uncommitted blob, continuing: {}", err);
            }
            (Blob::Uncommitted(blob), DateTime::INVALID)
        }
        Ok(Err(Some(BlobEntry::Stored(blob)))) => {
            if let Err(err) = blob.prefetch() {
                warn!("error prefetching committed blob, continuing: {}", err);
            }
            let created_at = blob.created_at().into();
            (Blob::Committed(blob), created_at)
        }
        Ok(Err(Some(BlobEntry::Removed(removed_at)))) => (Blob::NotFound, removed_at.into()),
        Ok(Err(None)) => (Blob::NotFound, DateTime::INVALID),
        Err(()) => return Err(db_error()),
    };

    // TODO: use vectored I/O here. See
    // https://github.com/rust-lang/futures-rs/pull/2181.
    match Deadline::timeout(ctx, IO_TIMEOUT, stream.write_all(time.as_bytes())).await {
        Ok(()) => {}
        Err(err) => return Err(err.describe("writing blob length")),
    }
    let length: [u8; 8] = (blob.len() as u64).to_be_bytes();
    match Deadline::timeout(ctx, IO_TIMEOUT, stream.write_all(&length)).await {
        Ok(()) => {}
        Err(err) => return Err(err.describe("writing blob length")),
    }
    Deadline::timeout(ctx, IO_TIMEOUT, stream.write_all(blob.bytes()))
        .await
        .map_err(|err| err.describe("writing blob bytes"))
}

/// Writes up to [`N_KEYS`] [`Key`]s at a time, reading [`REQUEST_MORE_KEYS`]
/// will send more keys. Once all keys are send it will write [`NO_MORE_KEYS`].
/// bytes to it.
async fn retrieve_keys<M>(
    ctx: &mut actor::Context<M, ThreadSafe>,
    stream: &mut TcpStream,
    buf: &mut Buffer,
    db_ref: &mut ActorRef<db::Message>,
) -> crate::Result<()> {
    debug!("retrieving keys");

    let keys = crate::op::retrieve_keys(ctx, db_ref)
        .await
        .map_err(|()| db_error())?;
    let mut iter = keys.into_iter();

    loop {
        let mut wbuf = buf.split_write(N_KEYS * Key::LENGTH).1;

        let mut n = 0;
        while let Some(key) = iter.next() {
            // NOTE: writing to buffer never fails.
            let bytes_written = wbuf.write(key.as_bytes()).unwrap();
            debug_assert_eq!(bytes_written, Key::LENGTH);

            // At most write `N_KEYS` keys to not overflow the buffer.
            n += 1;
            if n == N_KEYS {
                break;
            }
        }

        // TODO: use vectored I/O here using `Key::as_bytes` directly.
        Deadline::timeout(ctx, IO_TIMEOUT, stream.write_all(wbuf.as_bytes()))
            .await
            .map_err(|err| err.describe("writing keys"))?;

        if n < N_KEYS {
            // TODO: use vectored I/O here.
            return Deadline::timeout(ctx, IO_TIMEOUT, stream.write_all(&[NO_MORE_KEYS]))
                .await
                .map_err(|err| err.describe("writing no more keys"));
        }

        // Wait before sending more keys.
        match Deadline::timeout(ctx, ALIVE_TIMEOUT, buf.read_from(&mut *stream)).await {
            Ok(0) => return Ok(()),
            Ok(..) => {}
            Err(err) => return Err(err.describe("reading from socket")),
        }

        match buf.next_byte() {
            // Want more keys.
            Some(REQUEST_MORE_KEYS) => buf.processed(1),
            // Some different request, we don't handle that.
            _ => return Ok(()),
        }
    }
}
