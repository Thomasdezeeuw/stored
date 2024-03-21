//! Simple in-memory storage implementation.
//!
//! A new in-memory storage can be created using [`new`]. It returns a
//! [`Handle`], which can be converted into [`Storage`] on the thread that needs
//! it.

use std::future::Future;
use std::io;
use std::sync::Arc;

use heph::actor::actor_fn;
use heph::actor_ref::rpc::RpcError;
use heph::actor_ref::{ActorRef, RpcMessage};
use heph::future::{ActorFutureBuilder, InboxSize};
use heph::supervisor::NoSupervisor;
use heph::{actor, from_message};
use heph_rt::io::{Buf, Write};

use crate::key::Key;
use crate::storage::{self, index, AddError};

/// Create a new in-memory storage.
///
/// Returns a [`Future`] that handles the write requests to the storage. It must
/// be run otherwise write requests will never be processed and will stall for
/// ever.
pub fn new() -> (Handle, impl Future<Output = ()>) {
    let (w, handle) = index::new();
    let (future, writer) = ActorFutureBuilder::new()
        .with_inbox_size(InboxSize::MAX)
        .build(NoSupervisor, actor_fn(writer), w)
        .unwrap(); // SAFETY: `NewActor::Error = !` thus can never panic.
    (Handle { writer, handle }, future)
}

/// BLOB (Binary Large OBject) stored in-memory.
pub type Blob = Arc<[u8]>;

impl storage::Blob for Blob {
    fn len(&self) -> usize {
        (**self).len()
    }

    async fn write<H, T, C>(self, header: H, trailer: T, conn: &mut C) -> io::Result<(H, T)>
    where
        H: Buf,
        T: Buf,
        C: Write,
    {
        let bufs = (header, self, trailer);
        let bufs = conn.write_vectored_all(bufs).await?;
        Ok((bufs.0, bufs.2))
    }

    type BlobBytes = std::async_iter::FromIter<std::option::IntoIter<Arc<[u8]>>>;

    fn bytes(self) -> Self::BlobBytes {
        std::async_iter::from_iter(Some(self))
    }
}

/// Actor that handles write access to the storage.
async fn writer<RT>(mut ctx: actor::Context<WriteRequest, RT>, mut writer: index::Writer<Blob>) {
    while let Ok(request) = ctx.receive_next().await {
        // Don't care about about whether or not the other end got the response.
        let _ = match request {
            WriteRequest::Add(msg) => {
                msg.handle(|(key, blob)| async {
                    let added = writer.add_blob(key, blob);
                    if added {
                        writer.flush_changes().await;
                    }
                    added
                })
                .await
            }
            WriteRequest::Remove(msg) => {
                msg.handle(|key| async {
                    let key = key; // Move into closure.
                    let removed = writer.remove_blob(&key);
                    if removed {
                        writer.flush_changes().await;
                    }
                    removed
                })
                .await
            }
        };
    }
}

enum WriteRequest {
    /// Add `Blob` to storage. Returns true if the blob was added, false if the
    /// blob is already stored.
    Add(RpcMessage<(Key, Blob), bool>),
    /// Remove blob with `Key` from the storage. Returns true if the blob was
    /// removed, false if the blob was not in the storage.
    Remove(RpcMessage<Key, bool>),
}

from_message!(WriteRequest::Add((Key, Blob)) -> bool);
from_message!(WriteRequest::Remove(Key) -> bool);

/// Handle to the [`Storage`] that can be send across thread bounds.
///
/// Can be be converted into `Storage` using `Storage::from(handle)`.
#[derive(Clone)]
pub struct Handle {
    writer: ActorRef<WriteRequest>,
    handle: index::Handle<Blob>,
}

/// In-memory storage, pinned to a thread.
///
/// See the [`Storage`] implementation.
///
/// [`Storage`]: storage::Storage
#[derive(Clone)]
pub struct Storage {
    writer: ActorRef<WriteRequest>,
    index: index::Index<Blob>,
}

/// Turn a [`Handle`] into a [`Storage`].
impl From<Handle> for Storage {
    fn from(handle: Handle) -> Storage {
        Storage {
            writer: handle.writer,
            index: index::Index::from(handle.handle),
        }
    }
}

impl storage::Storage for Storage {
    type Blob = Blob;

    type Error = RpcError;

    fn len(&self) -> usize {
        self.index.len()
    }

    async fn lookup(&self, key: Key) -> Result<Option<Self::Blob>, Self::Error> {
        Ok(self.index.entry(&key).map(|entry| entry.blob.clone()))
    }

    async fn contains(&self, key: Key) -> Result<bool, Self::Error> {
        Ok(self.index.contains(&key))
    }

    async fn add_blob(&mut self, blob: &[u8]) -> Result<Key, AddError<Self::Error>> {
        let key = Key::for_blob(blob);
        if self.index.contains(&key) {
            Err(AddError::AlreadyStored(key))
        } else {
            match self.writer.rpc((key.clone(), blob.into())).await {
                Ok(true) => Ok(key),
                Ok(false) => Err(AddError::AlreadyStored(key)),
                Err(err) => Err(AddError::Err(err)),
            }
        }
    }

    async fn remove_blob(&mut self, key: Key) -> Result<bool, Self::Error> {
        if !self.index.contains(&key) {
            Ok(false)
        } else {
            self.writer.rpc(key).await
        }
    }
}
