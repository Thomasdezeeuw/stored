//! Coordinator side of the consensus connection.

use std::collections::HashMap;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::time::{Duration, SystemTime};

use futures_util::future::{select, Either};
use futures_util::io::AsyncWriteExt;
use heph::actor;
use heph::actor::context::ThreadSafe;
use heph::actor_ref::{RpcMessage, RpcResponse};
use heph::net::TcpStream;
use log::{debug, trace, warn};
use serde::Serialize;

use crate::buffer::{Buffer, WriteBuffer};
use crate::error::Describe;
use crate::peer::participant::{Response, ResponseKind};
use crate::peer::{ConsensusId, Peers, PARTICIPANT_MAGIC};
use crate::Key;

/// Message relayed to the peer the [`relay`] is connected to.
#[derive(Debug)]
pub enum RelayMessage {
    /// Add the blob with [`Key`].
    ///
    /// Returns the peer's time at which the blob was added.
    AddBlob(RpcMessage<(ConsensusId, Key), Result<SystemTime, RelayError>>),
    /// Commit to adding blob with [`Key`] at the provided timestamp.
    CommitStoreBlob(RpcMessage<(ConsensusId, Key, SystemTime), Result<(), RelayError>>),
    /// Remove the blob with [`Key`].
    ///
    /// Returns the peer's time at which the blob was added.
    RemoveBlob(RpcMessage<(ConsensusId, Key), Result<SystemTime, RelayError>>),
    /*
    /// Commit to removing blob with [`Key`].
    CommitRemoveBlob(RpcMessage<(ConsensusId, Key, SystemTime), Result<(), RelayError>>),
    */
}

/// Error return to [`RelayMessage`].
#[derive(Debug)]
pub enum RelayError {
    /// Peer wants to abort the operation.
    Abort,
    /// Peer failed.
    Failed,
}

/// Macro to allow `concat!` to used in creating the `doc` attribute.
macro_rules! doc_comment {
    ($doc: expr, $( $tt: tt )*) => {
        #[doc = $doc]
        $($tt)*
    };
}

/// Macro to create stand-alone types for [`RelayMessage`] variants.
/// This is required because the most of them have the same request type
/// ([`Key`]) when using RPC.
// TODO: maybe add something like this to Heph?
macro_rules! msg_types {
    ($name: ident ($inner_type1: ty, $inner_type2: ty) -> $return_type: ty) => {
        msg_types!(struct $name, $inner_type1, $inner_type2, stringify!($name));
        msg_types!(impl $name, $name, $return_type, 0, 1);
    };
    ($name: ident ($inner_type: ty) -> $return_type: ty) => {
        msg_types!(struct $name, $inner_type, stringify!($name));
        msg_types!(impl $name, $name, $return_type, 0);
    };
    (struct $name: ident, $inner_type: ty, $doc: expr) => {
        doc_comment! {
            concat!("Message type to use with [`ActorRef::rpc`] for [`RelayMessage::", $doc, "`]."),
            #[derive(Debug, Clone)]
            pub(super) struct $name(pub $inner_type);
        }
    };
    (struct $name: ident, $inner_type1: ty, $inner_type2: ty, $doc: expr) => {
        doc_comment! {
            concat!("Message type to use with [`ActorRef::rpc`] for [`RelayMessage::", $doc, "`]."),
            #[derive(Debug, Clone)]
            pub(super) struct $name(pub $inner_type1, pub $inner_type2);
        }
    };
    (impl $name: ident, $ty: ty, $return_type: ty, $( $field: tt ),*) => {
        impl From<RpcMessage<(ConsensusId, $ty), Result<$return_type, RelayError>>> for RelayMessage {
            fn from(msg: RpcMessage<(ConsensusId, $ty), Result<$return_type, RelayError>>) -> RelayMessage {
                RelayMessage::$name(RpcMessage {
                    request: (msg.request.0, $( (msg.request.1).$field ),* ),
                    response: msg.response,
                })
            }
        }
    };
}

msg_types!(AddBlob(Key) -> SystemTime);
msg_types!(CommitStoreBlob(Key, SystemTime) -> ());
msg_types!(RemoveBlob(Key) -> SystemTime);

/// Enum to collect all possible [`RpcResponse`]s from [`RelayMessage`].
#[derive(Debug)]
enum RelayResponse {
    /// Response for [`RelayMessage::AddBlob`].
    AddBlob(RpcResponse<Result<SystemTime, RelayError>>),
    /// Response for [`RelayMessage::CommitStoreBlob`].
    CommitStoreBlob(RpcResponse<Result<(), RelayError>>),
    /// Response for [`RelayMessage::RemoveBlob`].
    RemoveBlob(RpcResponse<Result<SystemTime, RelayError>>),
}

/// Actor that relays messages to a [`participant::dispatcher`] actor running on
/// the `remote` node.
///
/// [`participant::dispatcher`]: super::participant::dispatcher
pub async fn relay(
    mut ctx: actor::Context<RelayMessage, ThreadSafe>,
    remote: SocketAddr,
    peers: Peers,
    server: SocketAddr,
) -> crate::Result<()> {
    debug!(
        "starting coordinator relay: remote_address={}, server={}",
        remote, server
    );

    let mut responses = HashMap::new();
    let mut req_id = 0;
    let mut buf = Buffer::new();

    let mut stream = start_participant_connection(&mut ctx, remote, &mut buf, &server).await?;

    read_known_peers(&mut ctx, &mut stream, &mut buf, &peers, server).await?;

    loop {
        match select(ctx.receive_next(), buf.read_from(&mut stream)).await {
            Either::Left((msg, _)) => {
                debug!("coordinator relay received a message: {:?}", msg);
                // Received a message to relay.
                let req_id = next_id(&mut req_id);
                let wbuf = buf.split_write(MAX_REQ_SIZE).1;
                write_message(&mut stream, wbuf, &mut responses, req_id, msg).await?;
            }
            Either::Right((Ok(0), _)) => {
                debug!("peer participant closed connection");
                while let Some(msg) = ctx.try_receive_next() {
                    // Write any response left in our mailbox.
                    // TODO: ignore errors here since the other side is already
                    // disconnected?
                    debug!("coordinator relay received a message: {:?}", msg);
                    let req_id = next_id(&mut req_id);
                    let wbuf = buf.split_write(MAX_REQ_SIZE).1;
                    write_message(&mut stream, wbuf, &mut responses, req_id, msg).await?;
                }
                return Ok(());
            }
            Either::Right((Ok(_), _)) => {
                // Read one or more requests from the stream.
                relay_responses(&mut responses, &mut buf)?;
            }
            // Read error.
            Either::Right((Err(err), _)) => {
                return Err(err.describe("reading from peer connection"))
            }
        }

        next_id(&mut req_id);
    }
}

/// Maximum number of tries `start_participant_connection` attempts to make a
/// connection to the peer before stopping.
const CONNECT_TRIES: usize = 5;

/// Time to wait between connection tries in [`start_participant_connection`],
/// get doubled after each try.
const START_WAIT: Duration = Duration::from_millis(200);

/// Start a participant connection to `remote` address.
async fn start_participant_connection(
    ctx: &mut actor::Context<RelayMessage, ThreadSafe>,
    remote: SocketAddr,
    buf: &mut Buffer,
    server: &SocketAddr,
) -> crate::Result<TcpStream> {
    trace!(
        "coordinator relay connecting to peer participant: remote_address={}",
        remote
    );
    let mut wait = START_WAIT;
    let mut i = 0;
    let mut stream = loop {
        match TcpStream::connect(ctx, remote) {
            Ok(stream) => break stream,
            Err(err) if i >= CONNECT_TRIES => return Err(err.describe("connecting to peer")),
            Err(err) => {
                warn!(
                    "failed to connect to peer, but trying again ({}/{} tries): {}",
                    err, i, CONNECT_TRIES
                );
            }
        }
        // FIXME: don't use sleep here.
        std::thread::sleep(wait);
        //Timer::timeout(ctx, wait).await;
        wait *= 2;
        i += 1;
    };

    // Need space for the magic bytes and a IPv6 address (max. 45 bytes).
    let mut wbuf = buf.split_write(PARTICIPANT_MAGIC.len() + 45).1;

    // The connection magic to request the node to act as participant.
    wbuf.write_all(PARTICIPANT_MAGIC).unwrap();

    // The address of the `coordinator::server`.
    serde_json::to_writer(&mut wbuf, server)
        .map_err(|err| io::Error::from(err).describe("serializing server address"))?;

    trace!(
        "coordinator relay writing setup to peer participant: remote_address={}, server_address={}",
        remote,
        server,
    );
    stream
        .write_all(wbuf.as_bytes())
        .await
        .map(|()| stream)
        .map_err(|err| err.describe("writing peer connection setup"))
}

/// Read the known peers from the `stream`, starting a new [`relay`] actor for
/// each and adding it to `peers`.
async fn read_known_peers(
    ctx: &mut actor::Context<RelayMessage, ThreadSafe>,
    stream: &mut TcpStream,
    buf: &mut Buffer,
    peers: &Peers,
    server: SocketAddr,
) -> crate::Result<()> {
    trace!("coordinator relay reading known peers from connection");
    loop {
        let n = buf
            .read_from(&mut *stream)
            .await
            .map_err(|err| err.describe("reading known peers"))?;
        if n == 0 {
            return Err(
                io::Error::from(io::ErrorKind::UnexpectedEof).describe("reading known peers")
            );
        }

        // TODO: put `Deserializer` outside the loop.
        let mut iter =
            serde_json::Deserializer::from_slice(buf.as_bytes()).into_iter::<Vec<SocketAddr>>();
        // This is a bit weird; using an iterator for a single item. But the
        // `StreamDeserializer` keeps track of the number of bytes processed,
        // which we need to advance the buffer.
        match iter.next() {
            Some(Ok(addresses)) => {
                peers.spawn_many(ctx, &addresses, server);
                let bytes_processed = iter.byte_offset();
                buf.processed(bytes_processed);
                return Ok(());
            }
            Some(Err(ref err)) if err.is_eof() => continue,
            Some(Err(err)) => {
                return Err(io::Error::from(err).describe("deserializing known peers"))
            }
            None => {} // Continue reading more.
        }
    }
}

fn next_id(id: &mut usize) -> usize {
    let i = *id;
    *id += 1;
    i
}

/// Writes `msg` to `stream`.
async fn write_message<'b>(
    stream: &mut TcpStream,
    mut wbuf: WriteBuffer<'b>,
    responses: &mut HashMap<usize, RelayResponse>,
    id: usize,
    msg: RelayMessage,
) -> crate::Result<()> {
    let (consensus_id, kind) = match &msg {
        RelayMessage::AddBlob(RpcMessage {
            request: (consensus_id, key),
            ..
        }) => (*consensus_id, RequestKind::AddBlob(key)),
        RelayMessage::CommitStoreBlob(RpcMessage {
            request: (consensus_id, key, timestamp),
            ..
        }) => (*consensus_id, RequestKind::CommitStoreBlob(key, timestamp)),
        RelayMessage::RemoveBlob(RpcMessage {
            request: (consensus_id, key),
            ..
        }) => (*consensus_id, RequestKind::RemoveBlob(key)),
    };

    let request = Request {
        id,
        consensus_id,
        kind,
    };
    serde_json::to_writer(&mut wbuf, &request)
        .map_err(|err| io::Error::from(err).describe("serializing request"))?;
    trace!(
        "coordinator relay writing request to peer participant: {:?}",
        request
    );
    stream
        .write_all(&wbuf.as_bytes())
        .await
        .map_err(|err| err.describe("writing request"))?;

    match msg {
        RelayMessage::AddBlob(RpcMessage { response, .. }) => {
            responses.insert(id, RelayResponse::AddBlob(response));
        }
        RelayMessage::CommitStoreBlob(RpcMessage { response, .. }) => {
            responses.insert(id, RelayResponse::CommitStoreBlob(response));
        }
        RelayMessage::RemoveBlob(RpcMessage { response, .. }) => {
            responses.insert(id, RelayResponse::RemoveBlob(response));
        }
    }
    Ok(())
}

/// Read one or more requests in the `buf`fer and relay the responses to the
/// correct actor in `responses`.
fn relay_responses(
    responses: &mut HashMap<usize, RelayResponse>,
    buf: &mut Buffer,
) -> crate::Result<()> {
    // TODO: reuse the `Deserializer`, it allocates scratch memory.
    let mut de = serde_json::Deserializer::from_slice(buf.as_bytes()).into_iter::<Response>();

    while let Some(result) = de.next() {
        match result {
            Ok(response) => {
                debug!("coordinator relay received a response: {:?}", response);

                match responses.remove(&response.request_id) {
                    Some(relay) => match relay {
                        RelayResponse::AddBlob(relay) | RelayResponse::RemoveBlob(relay) => {
                            // Convert the response into the type required for
                            // `RelayResponse`.
                            let response = match response.response {
                                // FIXME: get timestamp form peer.
                                ResponseKind::Commit => Ok(SystemTime::now()),
                                ResponseKind::Abort => Err(RelayError::Abort),
                                ResponseKind::Fail => Err(RelayError::Failed),
                            };
                            if let Err(err) = relay.respond(response) {
                                warn!("failed to relay peer response to actor: {}", err);
                            }
                        }
                        RelayResponse::CommitStoreBlob(relay) => {
                            // Convert the response into the type required for
                            // `RelayResponse`.
                            let response = match response.response {
                                ResponseKind::Commit => Ok(()),
                                ResponseKind::Abort => Err(RelayError::Abort),
                                ResponseKind::Fail => Err(RelayError::Failed),
                            };
                            if let Err(err) = relay.respond(response) {
                                warn!("failed to relay peer response to actor: {}", err);
                            }
                        }
                    },
                    None => {
                        warn!("got an unexpected response: {:?}", response);
                        continue;
                    }
                }
            }
            Err(err) if err.is_eof() => break,
            Err(err) => return Err(io::Error::from(err).describe("deserializing peer response")),
        }
    }

    let bytes_processed = de.byte_offset();
    buf.processed(bytes_processed);
    Ok(())
}

/// Maximum size of [`Request`].
// TODO: this.
const MAX_REQ_SIZE: usize = 1000;

/// Request message send from [`relay`] to [`participant::dispatcher`].
///
/// [`participant::dispatcher`]: super::participant::dispatcher
#[derive(Debug, Serialize)]
pub struct Request<'a> {
    pub id: usize,
    pub consensus_id: ConsensusId,
    pub kind: RequestKind<'a>,
}

/// Kind of request.
#[derive(Debug, Serialize)]
pub enum RequestKind<'a> {
    /// Add the blob with [`Key`].
    // TODO: provide the length so we can pre-allocate a buffer at the
    // participant?
    AddBlob(&'a Key),
    /// Commit to store the blob.
    CommitStoreBlob(&'a Key, &'a SystemTime),
    /// Remove the blob with [`Key`].
    RemoveBlob(&'a Key),
}

pub mod server {
    //! Module with the [coordinator server actor].
    //!
    //! The [participant consensus actor] connects to this actor to retrieve the
    //! blobs it needs to store.
    //!
    //! [coordinator server actor]: actor()
    //! [participant consensus actor]: crate::peer::participant::consensus

    use std::io;
    use std::mem::size_of;
    use std::time::Duration;

    use futures_util::io::AsyncWriteExt;
    use heph::actor::context::ThreadSafe;
    use heph::net::TcpStream;
    use heph::timer::Deadline;
    use heph::{actor, ActorRef};
    use log::{debug, warn};

    use crate::buffer::Buffer;
    use crate::error::Describe;
    use crate::peer::participant::Response;
    use crate::storage::{self, BlobEntry, PAGE_SIZE};
    use crate::{db, op, Key};

    /// Type used to send the length of the blob over the write in big endian.
    pub type BlobLength = u64;

    /// The length (in bytes) that make up the length of the blob.
    pub const BLOB_LENGTH_LEN: usize = size_of::<BlobLength>();

    /// Length send if the blob is not found.
    pub const NO_BLOB: [u8; BLOB_LENGTH_LEN] = 0u64.to_be_bytes();

    /// Timeout used for I/O.
    const IO_TIMEOUT: Duration = Duration::from_secs(5);

    /// Timeout used to keep the connection alive.
    const ALIVE_TIMEOUT: Duration = Duration::from_secs(120);

    /// Blob type used by [`server`].
    #[derive(Debug)]
    enum Blob {
        Committed(storage::Blob),
        Uncommitted(storage::UncommittedBlob),
    }

    impl Blob {
        fn bytes(&self) -> &[u8] {
            use Blob::*;
            match self {
                Committed(blob) => blob.bytes(),
                Uncommitted(blob) => blob.bytes(),
            }
        }

        fn len(&self) -> usize {
            use Blob::*;
            match self {
                Committed(blob) => blob.len(),
                Uncommitted(blob) => blob.len(),
            }
        }

        fn prefetch(&self) -> io::Result<()> {
            use Blob::*;
            match self {
                Committed(blob) => blob.prefetch(),
                Uncommitted(blob) => blob.prefetch(),
            }
        }
    }

    /// Actor that serves the [`participant::consensus`] actor in retrieving
    /// uncommitted blobs.
    ///
    /// Expects to read [`Key`]s (as bytes) on the `stream`, responding with
    /// [`Blob`]s (length as `u64`, following by the bytes). If the returned
    /// length is 0 the blob is not found (either never stored or removed).
    ///
    /// This can also return uncommitted blobs, the response does not indicate
    /// whether or not the blob is commit.
    ///
    /// [`participant::consensus`]: crate::peer::participant::consensus
    /// [`Blob`]: crate::storage::Blob
    pub async fn actor(
        // Having `Response` here as message type makes no sense, but its required
        // because `participant::dispatcher` has it as message type. It should be
        // `!`.
        mut ctx: actor::Context<Response, ThreadSafe>,
        mut stream: TcpStream,
        mut buf: Buffer,
        mut db_ref: ActorRef<db::Message>,
    ) -> crate::Result<()> {
        debug!("starting coordinator server");

        loop {
            // NOTE: this we don't `buf` ourselves it could be that it already
            // contains a request, so check it first and only after read some
            // more bytes.
            while buf.len() >= Key::LENGTH {
                let key = Key::from_bytes(&buf.as_bytes()[..Key::LENGTH]).to_owned();
                buf.processed(Key::LENGTH);
                debug!("got peer request for blob: key={}", key);

                let f = op::retrieve_uncommitted_blob(&mut ctx, &mut db_ref, key);
                let blob = match f.await {
                    Ok(Ok(blob)) => Blob::Uncommitted(blob),
                    Ok(Err(Some(BlobEntry::Stored(blob)))) => Blob::Committed(blob),
                    Ok(Err(Some(BlobEntry::Removed(_)))) | Ok(Err(None)) => {
                        // By writing a length of 0 we indicate the blob is not
                        // found.
                        let f = Deadline::timeout(&mut ctx, IO_TIMEOUT, stream.write_all(&NO_BLOB));
                        match f.await {
                            Ok(Ok(())) => continue,
                            Ok(Err(err)) => return Err(err.describe("writing no blob length")),
                            Err(err) => {
                                return Err(
                                    io::Error::from(err).describe("timeout writing no blob length")
                                )
                            }
                        }
                    }
                    Err(()) => {
                        // Forcefully close the connection to force the client
                        // to recognise the error.
                        return Ok(());
                    }
                };

                if blob.len() > PAGE_SIZE {
                    // If the blob is large(-ish) we'll prefetch it from
                    // disk to improve performance.
                    // TODO: benchmark this with large(-ish) blobs.
                    if let Err(err) = blob.prefetch() {
                        warn!("error prefetching blob, continuing: {}", err);
                    }
                }

                // TODO: use vectored I/O here. See
                // https://github.com/rust-lang/futures-rs/pull/2181.
                let length: [u8; 8] = (blob.len() as u64).to_be_bytes();
                match Deadline::timeout(&mut ctx, IO_TIMEOUT, stream.write_all(&length)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => return Err(err.describe("writing blob length")),
                    Err(err) => {
                        return Err(io::Error::from(err).describe("timeout writing blob length"))
                    }
                }
                let f = Deadline::timeout(&mut ctx, IO_TIMEOUT, stream.write_all(blob.bytes()));
                match f.await {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => return Err(err.describe("writing blob bytes")),
                    Err(err) => {
                        return Err(io::Error::from(err).describe("timeout writing blob bytes"))
                    }
                }
            }

            // Read some more bytes.
            match Deadline::timeout(&mut ctx, ALIVE_TIMEOUT, buf.read_from(&mut stream)).await {
                Ok(Ok(..)) => {}
                Ok(Err(err)) => return Err(err.describe("reading from socket")),
                Err(err) => {
                    return Err(io::Error::from(err).describe("timeout reading from socket"))
                }
            }
        }
    }
}
