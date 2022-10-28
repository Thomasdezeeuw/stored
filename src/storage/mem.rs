//! Simple in-memory storage implementation.
//!
//! Nothing special, simply a `HashMap` with `Arc<[u8]>` as blob.
//!
//! A new in-memory storage can be created using [`new`]. It returns a
//! [`Handle`], which can be converted into [`Storage`] on the thread that needs
//! it.

use std::future::{ready, Future, Ready};
use std::hash::BuildHasherDefault;
use std::io::IoSlice;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{self, Poll};
use std::time::Duration;

use heph::actor::ActorFuture;
use heph::actor_ref::rpc::RpcError;
use heph::actor_ref::{ActorRef, Rpc, RpcMessage};
use heph::supervisor::NoSupervisor;
use heph::{actor, from_message};

use crate::key::{Key, KeyHasher};
use crate::protocol::Connection;
use crate::storage::{self, AddError};

/// Create a new in-memory storage.
///
/// Returns a [`Future`] that handles the write requests to the storage. It must
/// be run otherwise write requests will never be processed and will stall for
/// ever.
pub fn new() -> (Handle, impl Future) {
    let (w, handle) = hashmap::with_hasher(BuildHasherDefault::default());
    let (future, writer) = ActorFuture::new(
        NoSupervisor,
        writer as fn(_, _) -> _,
        Writer { inner: w },
        (),
    )
    .unwrap(); // SAFETY: `NewActor::Error = !` thus can never panic.
    (Handle { writer, handle }, future)
}

/// BLOB (Binary Large OBject) stored in-memory.
pub struct Blob(Arc<[u8]>);

impl Clone for Blob {
    fn clone(&self) -> Blob {
        Blob(self.0.clone())
    }
}

impl storage::Blob for Blob {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn write<'a, C>(
        &'a self,
        header: &'a [u8],
        trailer: &'a [u8],
        mut conn: C,
        timeout: Duration,
    ) -> Self::Write<'a, C>
    where
        C: Connection + 'a,
    {
        async move {
            let mut bufs = [
                IoSlice::new(header),
                IoSlice::new(&*self.0),
                IoSlice::new(trailer),
            ];
            conn.write_vectored(&mut bufs, timeout).await
        }
    }

    type Write<'a, C> = impl Future<Output = Result<(), C::Error>> + 'a
    where
        Self: 'a,
        C: Connection + 'a;
}

/// Actor that handles write access to the storage.
async fn writer<RT>(mut ctx: actor::Context<WriteRequest, RT>, mut writer: Writer) {
    while let Ok(request) = ctx.receive_next().await {
        let _ = match request {
            WriteRequest::Add(msg) => msg.handle(|blob| writer.add_blob(blob)),
            WriteRequest::Remove(msg) => msg.handle(|key| writer.remove_blob(key)),
        };
    }
}

enum WriteRequest {
    /// Add `Blob` to storage. Returns `Ok(Key)` if the blob was added,
    /// `Err(key)` if the blob is already stored.
    Add(RpcMessage<Blob, Result<Key, Key>>),
    /// Remove blob with `Key` from the storage. Returns true if the blob was
    /// removed, false if the blob was not in the storage.
    Remove(RpcMessage<Key, bool>),
}

from_message!(WriteRequest::Add(Blob) -> Result<Key, Key>);
from_message!(WriteRequest::Remove(Key) -> bool);

/// Write access to the storage.
struct Writer {
    inner: hashmap::Writer<Key, Blob, BuildHasherDefault<KeyHasher>>,
}

impl Writer {
    /// Add `blob` to storage.
    fn add_blob(&mut self, blob: Blob) -> Result<Key, Key> {
        let key = Key::for_blob(&blob.0);
        if self.inner.contains_key(&key) {
            Err(key)
        } else {
            self.inner.insert(key.clone(), blob);
            self.inner.flush();
            Ok(key)
        }
    }

    /// Remove blob with `key`.
    fn remove_blob(&mut self, key: Key) -> bool {
        let existed = self.inner.remove(key).is_some();
        self.inner.flush();
        existed
    }
}

/// Handle to the [`Storage`] that can be send across thread bounds.
///
/// Can be be converted into `Storage` using `Storage::from(handle)`.
#[derive(Clone)]
pub struct Handle {
    writer: ActorRef<WriteRequest>,
    handle: hashmap::Handle<Key, Blob, BuildHasherDefault<KeyHasher>>,
}

/// In-memory storage, pinned to a thread.
pub struct Storage {
    writer: ActorRef<WriteRequest>,
    reader: hashmap::Reader<Key, Blob, BuildHasherDefault<KeyHasher>>,
}

/// Turn a [`Handle`] into a [`Storage`].
impl From<Handle> for Storage {
    fn from(handle: Handle) -> Storage {
        Storage {
            writer: handle.writer,
            reader: handle.handle.into_reader(),
        }
    }
}

impl storage::Storage for Storage {
    type Blob = Blob;

    type Error = RpcError;

    fn len(&self) -> usize {
        self.reader.len()
    }

    fn data_size(&self) -> u64 {
        // TODO: not ideal. Maybe add `Reader::for_each_value` method?
        let mut total = 0;
        for key in self.reader.keys() {
            if let Some(blob) = self.reader.get(&key) {
                total += blob.0.len() as u64;
            }
        }
        total
    }

    fn total_size(&self) -> u64 {
        self.data_size()
    }

    fn lookup<'a>(&'a self, key: Key) -> Self::Lookup<'a> {
        ready(Ok(self.reader.get(&key)))
    }

    type Lookup<'a> = Ready<Result<Option<Self::Blob>, Self::Error>>;

    fn contains<'a>(&'a self, key: Key) -> Self::Contains<'a> {
        ready(Ok(self.reader.contains_key(&key)))
    }

    type Contains<'a> = Ready<Result<bool, Self::Error>>;

    fn add_blob<'a>(&'a mut self, blob: &[u8]) -> Self::AddBlob<'a> {
        let blob = Blob(blob.into());
        AddBlob(self.writer.rpc(blob))
    }

    type AddBlob<'a> = AddBlob<'a>;

    fn remove_blob<'a>(&'a mut self, key: Key) -> Self::RemoveBlob<'a> {
        RemoveBlob(self.writer.rpc(key))
    }

    type RemoveBlob<'a> = RemoveBlob<'a>;
}

/// [`Future`] returned by `Storage::add_blob`.
pub struct AddBlob<'a>(Rpc<'a, WriteRequest, Result<Key, Key>>);

impl<'a> Future for AddBlob<'a> {
    type Output = Result<Key, AddError<RpcError>>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut task::Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.0).poll(ctx) {
            Poll::Ready(Ok(Ok(key))) => Poll::Ready(Ok(key)),
            Poll::Ready(Ok(Err(key))) => Poll::Ready(Err(AddError::AlreadyStored(key))),
            Poll::Ready(Err(err)) => Poll::Ready(Err(AddError::Err(err))),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// [`Future`] returned by `Storage::remove_blob`.
pub struct RemoveBlob<'a>(Rpc<'a, WriteRequest, bool>);

impl<'a> Future for RemoveBlob<'a> {
    type Output = Result<bool, RpcError>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut task::Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(ctx)
    }
}
