//! Module containing the value cache related types and actor.

use std::sync::Arc;

use heph::actor::sync::SyncContext;
use heph::actor_ref::{ActorRef, Sync};
use heph::supervisor::NoSupervisor;
use heph::system::{ActorSystem, RuntimeError};
use log::trace;

use crate::Key;

/// Type used for the value.
pub type Value = Arc<[u8]>;

#[derive(Clone)]
pub struct CacheRef {
    handle: evmap::ReadHandle<Key, Value, (), evmap::FxHashBuilder>,
    actor_ref: ActorRef<Message, Sync>,
}

impl CacheRef {
    /// Get the value with `key`.
    pub fn get(&mut self, key: &Key) -> Option<Value> {
        self.handle.get_and(key, |values| values[0].clone())
    }

    /// Asynchronously store `value`, returning its key.
    pub fn async_add(&mut self, value: Value) -> Key {
        let key = Key::for_value(&*value);
        self.actor_ref <<= Message::Add(key.clone(), value);
        key
    }

    /// Asynchronously remove the value with `key`.
    pub fn async_remove(&mut self, key: Key) {
        self.actor_ref <<= Message::Remove(key);
    }
}

/// Write handle to the cache.
type WCache = evmap::WriteHandle<Key, Value, (), evmap::FxHashBuilder>;

/// Start the value cache.
///
/// This will start a synchronous actor that controls the value cache. The
/// returned actor reference can be used to control the cache and the returned
/// `Cache` is a read handle to the cache.
pub fn start<S>(system: &mut ActorSystem<S>) -> Result<CacheRef, RuntimeError> {
    // Create the cache. Read handles go to all actors and the `cache_master`
    // will have write access.
    let (cache, cache_write_handle) = evmap::Options::default()
        .with_hasher(evmap::FxHashBuilder::default())
        .construct();

    // Spawn our cache master, which has write access to the value cache.
    let cache_master = cache_master as fn(_, _) -> _;
    system
        .spawn_sync_actor(NoSupervisor, cache_master, cache_write_handle)
        .map(|actor_ref| CacheRef {
            handle: cache,
            actor_ref,
        })
}

/// Message used to control the value cache.
#[derive(Debug)]
enum Message {
    /// Add a new value to the cache.
    Add(Key, Value),
    /// Remove a value from the cache.
    Remove(Key),
}

/// The actor that controls the cache.
fn cache_master(mut ctx: SyncContext<Message>, mut cache: WCache) -> Result<(), !> {
    trace!("cache master starting");
    while let Ok(msg) = ctx.receive_next() {
        match msg {
            Message::Add(key, value) => {
                trace!("adding {} to cache: {:?}", key, value);
                cache.insert(key, value).refresh();
            },
            Message::Remove(key) => {
                trace!("removing {} from cache", key);
                cache.empty(key).refresh();
            },
        }
    }

    trace!("cache master shutting down");
    Ok(())
}
