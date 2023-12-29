//! Simple in-memory storage implementation.
//!
//! Nothing special, simply a `HashMap` with `Arc<[u8]>` as blob.
//!
//! A new in-memory storage can be created using [`new`]. It returns a
//! [`Handle`], which can be converted into [`Storage`] on the thread that needs
//! it.

use std::future::Future;
use std::hash::BuildHasherDefault;
use std::io;
use std::sync::Arc;

use heph::actor_ref::rpc::RpcError;
use heph::actor_ref::{ActorRef, RpcMessage};
use heph::supervisor::NoSupervisor;
use heph::{actor, from_message, ActorFuture};
use heph_rt::io::{Buf, Write};
use left_right::hashmap;

use crate::key::{Key, KeyHasher};
use crate::storage::{self, AddError};

/// Create a new in-memory storage.
///
/// Returns a [`Future`] that handles the write requests to the storage. It must
/// be run otherwise write requests will never be processed and will stall for
/// ever.
pub fn new() -> (Handle, impl Future<Output = ()>) {
    let (w, handle) = hashmap::with_hasher(BuildHasherDefault::default());
    let (future, writer) =
        ActorFuture::new(NoSupervisor, writer as fn(_, _) -> _, Writer { inner: w }).unwrap(); // SAFETY: `NewActor::Error = !` thus can never panic.
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

    async fn write<H, T, C>(self, header: H, trailer: T, mut conn: C) -> Result<(H, T), io::Error>
    where
        H: Buf,
        T: Buf,
        C: Write,
    {
        let bufs = (header, self.0, trailer);
        let bufs = conn.write_vectored_all(bufs).await?;
        Ok((bufs.0, bufs.2))
    }
}

/// Actor that handles write access to the storage.
async fn writer<RT>(mut ctx: actor::Context<WriteRequest, RT>, mut writer: Writer) {
    while let Ok(request) = ctx.receive_next().await {
        // Don't care about about whether or not the other end got the response.
        let _ = match request {
            WriteRequest::Add(msg) => {
                msg.handle(|blob| async { writer.add_blob(blob).await })
                    .await
            }
            WriteRequest::Remove(msg) => {
                msg.handle(|key| async { writer.remove_blob(key).await })
                    .await
            }
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
    async fn add_blob(&mut self, blob: Blob) -> Result<Key, Key> {
        let key = Key::for_blob(&blob.0);
        if self.inner.contains_key(&key) {
            Err(key)
        } else {
            self.inner.insert(key.clone(), blob);
            self.inner.flush().await;
            Ok(key)
        }
    }

    /// Remove blob with `key`.
    async fn remove_blob(&mut self, key: Key) -> bool {
        let existed = self.inner.remove(key).is_some();
        self.inner.flush().await;
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
#[derive(Clone)]
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

    async fn lookup(&self, key: Key) -> Result<Option<Self::Blob>, Self::Error> {
        Ok(self.reader.get_cloned(&key))
    }

    async fn contains(&self, key: Key) -> Result<bool, Self::Error> {
        Ok(self.reader.contains_key(&key))
    }

    async fn add_blob(&mut self, blob: &[u8]) -> Result<Key, AddError<Self::Error>> {
        match self.writer.rpc(Blob(blob.into())).await {
            Ok(Ok(key)) => Ok(key),
            Ok(Err(key)) => Err(AddError::AlreadyStored(key)),
            Err(err) => Err(AddError::Err(err)),
        }
    }

    async fn remove_blob(&mut self, key: Key) -> Result<bool, Self::Error> {
        self.writer.rpc(key).await
    }
}
