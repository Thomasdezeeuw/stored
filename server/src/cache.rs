//! Module containing the value cache related types and actor.

use std::sync::Arc;

use heph::actor::sync::SyncContext;
use heph::actor_ref::{ActorRef, Sync};
use heph::supervisor::NoSupervisor;
use heph::system::{ActorSystem, RuntimeError};
use log::trace;

use crate::Key;

/// Read handle to the cache.
pub type Cache = evmap::ReadHandle<Key, Value, (), evmap::FxHashBuilder>;

/// Write handle to the cache.
type WCache = evmap::WriteHandle<Key, Value, (), evmap::FxHashBuilder>;

/// Type used for the value.
pub type Value = Arc<[u8]>;

/// Start the value cache.
///
/// This will start a synchronous actor that controls the value cache. The
/// returned actor reference can be used to control the cache and the returned
/// `Cache` is a read handle to the cache.
pub fn start<S>(system: &mut ActorSystem<S>) -> Result<(ActorRef<Message, Sync>, Cache), RuntimeError> {
    // Create the cache. Read handles go to all actors and the `cache_master`
    // will have write access.
    let (cache, cache_write_handle) = evmap::Options::default()
        .with_hasher(evmap::FxHashBuilder::default())
        .construct();

    // Spawn our cache master, which has write access to the value cache.
    let cache_master = cache_master as fn(_, _) -> _;
    system.spawn_sync_actor(NoSupervisor, cache_master, cache_write_handle)
        .map(|actor_ref| (actor_ref, cache))
}

/// Message used to control the value cache.
#[derive(Debug)]
pub enum Message {
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
