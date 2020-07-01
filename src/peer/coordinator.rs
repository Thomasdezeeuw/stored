//! Coordinator side of the consensus connection.

pub mod relay {
    //! Module with the [coordinator relay actor].
    //!
    //! [coordinator relay actor]: actor()

    use std::collections::HashMap;
    use std::future::Future;
    use std::io;
    use std::net::SocketAddr;
    use std::pin::Pin;
    use std::task::{self, Poll};
    use std::time::{Duration, SystemTime};

    use futures_util::future::{select, Either};
    use futures_util::io::AsyncWriteExt;
    use heph::actor::context::ThreadSafe;
    use heph::actor_ref::{RpcMessage, RpcResponse, SendError};
    use heph::net::TcpStream;
    use heph::timer::{Deadline, Timer};
    use heph::{actor, Actor, NewActor, SupervisorStrategy};
    use log::{debug, trace, warn};

    use crate::buffer::{Buffer, WriteBuffer};
    use crate::error::Describe;
    use crate::peer::{
        ConsensusId, ConsensusVote, Operation, Peers, Request, Response, EXIT_COORDINATOR,
        EXIT_PARTICIPANT, PARTICIPANT_MAGIC,
    };
    use crate::Key;

    /// Maximum number of tries `start_participant_connection` attempts to make
    /// a connection to the peer before stopping.
    const CONNECT_TRIES: usize = 5;

    /// Time to wait between connection tries in [`connect_to_participant`], get
    /// doubled after each try.
    const START_WAIT: Duration = Duration::from_millis(500);

    /// Timeout used for I/O.
    const IO_TIMEOUT: Duration = Duration::from_secs(5);

    /// An estimate of the largest size of an [`Request`] in bytes.
    const MAX_REQ_SIZE: usize = 300;

    pub struct Supervisor {
        remote: SocketAddr,
        peers: Peers,
        server: SocketAddr,
        restarts_left: usize,
    }

    /// Maximum number of times the [`actor`] will be restarted.
    const MAX_RESTARTS: usize = 5;

    impl Supervisor {
        /// Create a new `Supervisor`.
        pub fn new(remote: SocketAddr, peers: Peers, server: SocketAddr) -> Supervisor {
            Supervisor {
                remote,
                peers,
                server,
                restarts_left: MAX_RESTARTS,
            }
        }
    }

    impl<NA, A> heph::Supervisor<NA> for Supervisor
    where
        NA: NewActor<Argument = (SocketAddr, Peers, SocketAddr), Error = !, Actor = A>,
        A: Actor<Error = crate::Error>,
    {
        fn decide(&mut self, err: crate::Error) -> SupervisorStrategy<NA::Argument> {
            if self.restarts_left >= 1 {
                self.restarts_left -= 1;
                warn!(
                    "coordinator relay failed, restarting it ({}/{} restarts left): {}: remote={}, server={}",
                    self.restarts_left, MAX_RESTARTS, err, self.remote, self.server
                );
                SupervisorStrategy::Restart((self.remote, self.peers.clone(), self.server))
            } else {
                warn!(
                    "peer coordinator relay failed, stopping it: {}: remote={}, server={}",
                    err, self.remote, self.server,
                );
                SupervisorStrategy::Stop
            }
        }

        fn decide_on_restart_error(&mut self, err: NA::Error) -> SupervisorStrategy<NA::Argument> {
            err
        }

        fn second_restart_error(&mut self, err: NA::Error) {
            err
        }
    }

    /// Actor that relays messages to a [`participant::dispatcher`] actor
    /// running on the `remote` node.
    ///
    /// [`participant::dispatcher`]: crate::peer::participant::dispatcher
    pub async fn actor(
        mut ctx: actor::Context<Message, ThreadSafe>,
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

        let mut stream = connect_to_participant(&mut ctx, remote, &mut buf, &server).await?;
        read_known_peers(&mut ctx, &mut stream, &mut buf, &peers, server).await?;

        // In case the participant send an exit message along with the known
        // peers already.
        if !buf.is_empty() {
            if relay_responses(&mut responses, &mut buf)? {
                // Participant closed connection.
                return Ok(());
            }
        }

        // TODO: close connection cleanly, sending `EXIT_COORDINATOR`.

        // FIXME: rather then restarting this actor on connection errors, try to
        // reconnect ourselves this way we can keep `responses` `HashMap` alive.
        loop {
            match select(ctx.receive_next(), buf.read_from(&mut stream)).await {
                Either::Left((msg, _)) => {
                    debug!("coordinator relay received a message: {:?}", msg);
                    let req_id = next_id(&mut req_id);
                    let wbuf = buf.split_write(MAX_REQ_SIZE).1;
                    write_message(&mut ctx, &mut stream, wbuf, &mut responses, req_id, msg).await?;
                }
                Either::Right((Ok(0), _)) => {
                    // Return an error to restart this actor.
                    return Err(io::Error::from(io::ErrorKind::UnexpectedEof)
                        .describe("participant dispatcher closed connection unexpectedly"));
                }
                Either::Right((Ok(_), _)) => {
                    // Read one or more requests from the stream.
                    if relay_responses(&mut responses, &mut buf)? {
                        // Participant closed connection.
                        return stream
                            .write_all(EXIT_COORDINATOR)
                            .await
                            .map_err(|err| err.describe("writing exit message"));
                    }
                }
                // Read error.
                Either::Right((Err(err), _)) => {
                    return Err(err.describe("reading from peer connection"));
                }
            }
        }
    }

    /// Message relayed to the peer the [relay actor] is connected to.
    ///
    /// [relay actor]: actor()
    #[derive(Debug)]
    pub enum Message {
        /// Add the blob with [`Key`].
        ///
        /// Returns the peer's time at which the blob was added.
        AddBlob(RpcMessage<(ConsensusId, Key), Result<SystemTime, Error>>),
        /// Commit to storing blob with [`Key`] at the provided timestamp.
        CommitStoreBlob(RpcMessage<(ConsensusId, Key, SystemTime), Result<(), Error>>),
        /// Abort storing blob with [`Key`].
        AbortStoreBlob(RpcMessage<(ConsensusId, Key), Result<(), Error>>),

        /// Remove the blob with [`Key`].
        ///
        /// Returns the peer's time at which the blob was remove.
        RemoveBlob(RpcMessage<(ConsensusId, Key), Result<SystemTime, Error>>),
        /// Commit to removing blob with [`Key`] at the provided timestamp.
        CommitRemoveBlob(RpcMessage<(ConsensusId, Key, SystemTime), Result<(), Error>>),
        /// Abort removing blob with [`Key`].
        AbortRemoveBlob(RpcMessage<(ConsensusId, Key), Result<(), Error>>),
    }

    impl Message {
        /// Convert this `Message` into a [`Request`] and a [`RpcResponder`].
        fn convert(self, request_id: usize) -> (Request, RpcResponder) {
            let (consensus_id, key, op, response) = match self {
                Message::AddBlob(RpcMessage { request, response }) => (
                    request.0,
                    request.1,
                    Operation::AddBlob,
                    RpcResponder::AddBlob(response),
                ),
                Message::CommitStoreBlob(RpcMessage { request, response }) => (
                    request.0,
                    request.1,
                    Operation::CommitStoreBlob(request.2),
                    RpcResponder::CommitStoreBlob(response),
                ),
                Message::AbortStoreBlob(RpcMessage { request, response }) => (
                    request.0,
                    request.1,
                    Operation::AbortStoreBlob,
                    RpcResponder::AbortStoreBlob(response),
                ),
                Message::RemoveBlob(RpcMessage { request, response }) => (
                    request.0,
                    request.1,
                    Operation::RemoveBlob,
                    RpcResponder::RemoveBlob(response),
                ),
                Message::CommitRemoveBlob(RpcMessage { request, response }) => (
                    request.0,
                    request.1,
                    Operation::CommitRemoveBlob(request.2),
                    RpcResponder::CommitRemoveBlob(response),
                ),
                Message::AbortRemoveBlob(RpcMessage { request, response }) => (
                    request.0,
                    request.1,
                    Operation::AbortRemoveBlob,
                    RpcResponder::AbortRemoveBlob(response),
                ),
            };
            let request = Request {
                id: request_id,
                consensus_id,
                key,
                op,
            };
            (request, response)
        }
    }

    /// Error return to [`Message`].
    #[derive(Debug)]
    pub enum Error {
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

    /// Macro to create stand-alone types for [`Message`] variants. This is
    /// required because the most of them have the same request and return types
    /// when using RPC.
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
                concat!("Message type to use with [`ActorRef::rpc`] for [`Message::", $doc, "`]."),
                #[derive(Debug, Clone)]
                pub(crate) struct $name(pub $inner_type);
            }
        };
        (struct $name: ident, $inner_type1: ty, $inner_type2: ty, $doc: expr) => {
            doc_comment! {
                concat!("Message type to use with [`ActorRef::rpc`] for [`Message::", $doc, "`]."),
                #[derive(Debug, Clone)]
                pub(crate) struct $name(pub $inner_type1, pub $inner_type2);
            }
        };
        (impl $name: ident, $ty: ty, $return_type: ty, $( $field: tt ),*) => {
            impl From<RpcMessage<(ConsensusId, $ty), Result<$return_type, Error>>> for Message {
                fn from(msg: RpcMessage<(ConsensusId, $ty), Result<$return_type, Error>>) -> Message {
                    Message::$name(RpcMessage {
                        request: (msg.request.0, $( (msg.request.1).$field ),* ),
                        response: msg.response,
                    })
                }
            }
        };
    }

    msg_types!(AddBlob(Key) -> SystemTime);
    msg_types!(CommitStoreBlob(Key, SystemTime) -> ());
    msg_types!(AbortStoreBlob(Key) -> ());
    msg_types!(RemoveBlob(Key) -> SystemTime);
    msg_types!(CommitRemoveBlob(Key, SystemTime) -> ());
    msg_types!(AbortRemoveBlob(Key) -> ());

    /// Start a participant connection to `remote` address.
    async fn connect_to_participant(
        ctx: &mut actor::Context<Message, ThreadSafe>,
        remote: SocketAddr,
        buf: &mut Buffer,
        server: &SocketAddr,
    ) -> crate::Result<TcpStream> {
        trace!(
            "coordinator relay connecting to peer participant: remote_address={}",
            remote
        );
        let mut wait = START_WAIT;
        let mut i = 1;
        let mut stream = loop {
            match TcpStream::connect(ctx, remote) {
                Ok(mut stream) => {
                    // Work around https://github.com/Thomasdezeeuw/heph/issues/287.
                    //
                    // We've got a connection, but it might not be connected
                    // yet. So first we'll de-schedule ourselves, waiting for an
                    // event from the OS.
                    wait_for_wakeup().await;
                    // After that we try to write the connection magic to test
                    // the connection.
                    match stream.write_all(PARTICIPANT_MAGIC).await {
                        Ok(()) => break stream,
                        // Not yet connected, try again.
                        Err(err) => {
                            debug!(
                                "failed to connect to peer, but trying again ({}/{} tries): {}",
                                i, CONNECT_TRIES, err
                            );
                        }
                    }
                }
                Err(err) if i >= CONNECT_TRIES => return Err(err.describe("connecting to peer")),
                Err(err) => {
                    debug!(
                        "failed to connect to peer, but trying again ({}/{} tries): {}",
                        i, CONNECT_TRIES, err
                    );
                }
            }

            // Wait a moment before trying to connect again.
            Timer::timeout(ctx, wait).await;
            // Wait a little longer next time.
            wait *= 2;
            i += 1;
        };

        // Need space for the magic bytes and a IPv6 address (max. 45 bytes).
        let mut wbuf = buf.split_write(PARTICIPANT_MAGIC.len() + 45).1;
        // The address of the `coordinator::server`.
        serde_json::to_writer(&mut wbuf, server)
            .map_err(|err| io::Error::from(err).describe("serializing server address"))?;

        trace!(
            "coordinator relay writing setup to peer participant: remote_address={}, server_address={}",
            remote,
            server,
        );
        match Deadline::timeout(ctx, IO_TIMEOUT, stream.write_all(wbuf.as_bytes())).await {
            Ok(()) => Ok(stream),
            Err(err) => return Err(err.describe("writing peer connection setup")),
        }
    }

    /// Returns a [`Future`] that yields once, waiting for another event to wake
    /// up the future.
    const fn wait_for_wakeup() -> YieldOnce {
        YieldOnce(false)
    }

    /// [`Future`] that return `Poll::Pending` when polled the first time.
    struct YieldOnce(bool);

    impl Future for YieldOnce {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, _: &mut task::Context) -> Poll<Self::Output> {
            if self.0 {
                // Yielded already.
                Poll::Ready(())
            } else {
                // Not yet yielded.
                self.0 = true;
                Poll::Pending
            }
        }
    }

    /// Read the known peers from the `stream`, starting a new [`relay`] actor for
    /// each and adding it to `peers`.
    async fn read_known_peers(
        ctx: &mut actor::Context<Message, ThreadSafe>,
        stream: &mut TcpStream,
        buf: &mut Buffer,
        peers: &Peers,
        server: SocketAddr,
    ) -> crate::Result<()> {
        trace!("coordinator relay reading known peers from connection");
        loop {
            match Deadline::timeout(ctx, IO_TIMEOUT, buf.read_from(&mut *stream)).await {
                Ok(0) => {
                    return Err(io::Error::from(io::ErrorKind::UnexpectedEof)
                        .describe("reading known peers"))
                }
                Ok(..) => {}
                Err(err) => return Err(err.describe("reading known peers")),
            }

            // TODO: reuse `Deserializer` in relay actor, would require us to
            // have access to the `R`eader in the type.
            let mut iter =
                serde_json::Deserializer::from_slice(buf.as_bytes()).into_iter::<Vec<SocketAddr>>();
            // This is a bit weird, using an iterator for a single item. But the
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

    /// return id++;
    fn next_id(id: &mut usize) -> usize {
        let i = *id;
        *id += 1;
        i
    }

    /// Enum to collect all possible [`RpcResponse`]s from [`Message`].
    #[derive(Debug)]
    enum RpcResponder {
        /// Response for [`Message::AddBlob`].
        AddBlob(RpcResponse<Result<SystemTime, Error>>),
        /// Response for [`Message::CommitStoreBlob`].
        CommitStoreBlob(RpcResponse<Result<(), Error>>),
        /// Response for [`Message::AbortStoreBlob`].
        AbortStoreBlob(RpcResponse<Result<(), Error>>),

        /// Response for [`Message::RemoveBlob`].
        RemoveBlob(RpcResponse<Result<SystemTime, Error>>),
        /// Response for [`Message::CommitRemoveBlob`].
        CommitRemoveBlob(RpcResponse<Result<(), Error>>),
        /// Response for [`Message::AbortRemoveBlob`].
        AbortRemoveBlob(RpcResponse<Result<(), Error>>),
    }

    impl RpcResponder {
        /// Relay the `vote` to the RPC callee.
        fn respond(self, vote: ConsensusVote) -> Result<(), SendError> {
            match self {
                RpcResponder::AddBlob(rpc_response) | RpcResponder::RemoveBlob(rpc_response) => {
                    let response = match vote {
                        ConsensusVote::Commit(timestamp) => Ok(timestamp),
                        ConsensusVote::Abort => Err(Error::Abort),
                        ConsensusVote::Fail => Err(Error::Failed),
                    };
                    rpc_response.respond(response)
                }
                RpcResponder::CommitStoreBlob(rpc_response)
                | RpcResponder::AbortStoreBlob(rpc_response)
                | RpcResponder::CommitRemoveBlob(rpc_response)
                | RpcResponder::AbortRemoveBlob(rpc_response) => {
                    let response = match vote {
                        ConsensusVote::Commit(..) => Ok(()),
                        ConsensusVote::Abort => Err(Error::Abort),
                        ConsensusVote::Fail => Err(Error::Failed),
                    };
                    rpc_response.respond(response)
                }
            }
        }
    }

    /// Writes `msg` to `stream`.
    async fn write_message<'b>(
        ctx: &mut actor::Context<Message, ThreadSafe>,
        stream: &mut TcpStream,
        mut wbuf: WriteBuffer<'b>,
        responses: &mut HashMap<usize, RpcResponder>,
        id: usize,
        msg: Message,
    ) -> crate::Result<()> {
        let (request, response) = msg.convert(id);
        trace!(
            "coordinator relay writing request to peer participant: {:?}",
            request
        );
        serde_json::to_writer(&mut wbuf, &request)
            .map_err(|err| io::Error::from(err).describe("serializing request"))?;
        match Deadline::timeout(ctx, IO_TIMEOUT, stream.write_all(&wbuf.as_bytes())).await {
            Ok(()) => {
                responses.insert(id, response);
                Ok(())
            }
            Err(err) => Err(err.describe("reading known peers")),
        }
    }

    /// Read one or more requests in the `buf`fer and relay the responses to the
    /// correct actor in `responses`.
    ///
    /// Return `Ok(true)` if the participant wants to close the connection.
    /// Returns `Ok(false)` if more responses are to be expected.
    fn relay_responses(
        responses: &mut HashMap<usize, RpcResponder>,
        buf: &mut Buffer,
    ) -> crate::Result<bool> {
        let mut de = serde_json::Deserializer::from_slice(buf.as_bytes()).into_iter::<Response>();

        loop {
            match de.next() {
                Some(Ok(response)) => {
                    debug!("coordinator relay received a response: {:?}", response);

                    match responses.remove(&response.request_id) {
                        Some(rpc_response) => {
                            if let Err(err) = rpc_response.respond(response.vote) {
                                warn!(
                                    "failed to relay peer response to actor: {}: request.id={}",
                                    err, response.request_id
                                );
                            }
                        }
                        None => {
                            warn!("got an unexpected response: {:?}", response);
                            continue;
                        }
                    }
                }
                // Read a partial response, we'll get it next time.
                Some(Err(ref err)) if err.is_eof() => break,
                Some(Err(err)) => {
                    let bytes_processed = de.byte_offset();
                    buf.processed(bytes_processed);

                    if buf.as_bytes() == EXIT_PARTICIPANT {
                        // Participant wants to close the connection.
                        buf.processed(EXIT_PARTICIPANT.len());
                        return Ok(true);
                    } else {
                        return Err(io::Error::from(err).describe("deserialising peer response"));
                    }
                }
                // No more responses.
                None => break,
            }
        }

        let bytes_processed = de.byte_offset();
        buf.processed(bytes_processed);
        Ok(false)
    }
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
    use std::net::SocketAddr;
    use std::time::Duration;

    use futures_util::io::AsyncWriteExt;
    use heph::actor::context::ThreadSafe;
    use heph::net::TcpStream;
    use heph::timer::Deadline;
    use heph::{actor, ActorRef};
    use log::{debug, warn};

    use crate::buffer::Buffer;
    use crate::error::Describe;
    use crate::peer::Response;
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

    /// Function to change the log target and module for the warning message,
    /// the [`peer::switcher`] has little to do with the error.
    pub(crate) async fn run_actor(
        ctx: actor::Context<Response, ThreadSafe>,
        stream: TcpStream,
        buf: Buffer,
        db_ref: ActorRef<db::Message>,
        server: SocketAddr,
        remote: SocketAddr,
    ) {
        if let Err(err) = actor(ctx, stream, buf, db_ref).await {
            warn!(
                "coordinator server failed: {}: remote={}, server={}",
                err, remote, server
            );
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
                            Ok(()) => continue,
                            Err(err) => return Err(err.describe("writing no blob length")),
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
                    Ok(()) => {}
                    Err(err) => return Err(err.describe("writing blob length")),
                }
                let f = Deadline::timeout(&mut ctx, IO_TIMEOUT, stream.write_all(blob.bytes()));
                match f.await {
                    Ok(()) => {}
                    Err(err) => return Err(err.describe("writing blob bytes")),
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
}
