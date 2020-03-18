//! Module containing the value cache related types.

use std::sync::Arc;

use heph::actor::sync::SyncContext;
use heph::supervisor::NoSupervisor;
use heph::{ActorRef, Runtime, RuntimeError};
use log::trace;

use crate::Key;

// TODO: use different hashing algorithm?

/// Type used for the value.
pub type Value = Arc<[u8]>;

/// Handle to the cache.
#[derive(Clone)]
pub struct Cache {
    actor_ref: ActorRef<Message>,
    handle: evmap::ReadHandle<Key, Value>,
}

impl Cache {
    /// Store `value`, returning the key.
    pub fn store(&mut self, value: Value) -> Key {
        let key = Key::for_blob(&*value);
        let msg = Message::Store(key.clone(), value);
        let _ = self.actor_ref.send(msg);
        todo!("Cache::store: get the stored cache value")
    }

    /// Retrieve the value stored for `key`.
    pub fn retrieve(&mut self, key: &Key) -> Option<Value> {
        self.handle.get_and(&key, |value| value[0].clone())
    }

    /// Remove the value stored for `key`.
    pub fn remove(&mut self, key: Key) {
        let msg = Message::Remove(key);
        let _ = self.actor_ref.send(msg);
        todo!("Cache::remove: get the ok that the value is removed")
    }
}

/// Start the value cache.
///
/// This will start a synchronous actor that controls the value cache. The
/// returned actor reference can be used to control the cache and the returned
/// `Cache` is a read handle to the cache.
pub fn start<S>(runtime: &mut Runtime<S>) -> Result<Cache, RuntimeError> {
    let (handle, cache) = evmap::new();
    let cache_master = cache_master as fn(_, _) -> _;
    runtime
        .spawn_sync_actor(NoSupervisor, cache_master, cache)
        .map(|actor_ref| Cache { actor_ref, handle })
}

/// Message used to control the value cache.
#[derive(Debug)]
enum Message {
    /// Store a new value in the cache.
    Store(Key, Value),
    /// Remove a value from the cache.
    Remove(Key),
}

/// Synchronous actor that control the cache.
fn cache_master(
    mut ctx: SyncContext<Message>,
    mut cache: evmap::WriteHandle<Key, Value>,
) -> Result<(), !> {
    trace!("cache starting");

    while let Ok(msg) = ctx.receive_next() {
        match msg {
            Message::Store(key, value) => {
                trace!("storing in cache: key={}", key);
                cache.insert(key, value);
            }
            Message::Remove(key) => {
                trace!("removing {} from cache", key);
                cache.empty(key);
            }
        }

        // TODO: optimise this so we don't flush every write, but only every nth
        // writes, or if no messages are pending.
        cache.refresh();
    }

    trace!("cache shutting down");
    Ok(())
}
