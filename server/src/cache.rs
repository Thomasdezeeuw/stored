//! Module containing the value cache related types and actor.

use std::collections::HashMap;
use std::sync::Arc;

use heph::actor::sync::SyncContext;
use heph::actor_ref::{ActorRef, Map, Sync};
use heph::supervisor::NoSupervisor;
use heph::system::{ActorSystem, RuntimeError};
use log::trace;

use crate::Key;

/// Type used for the value.
pub type Value = Arc<[u8]>;

/// Start the value cache.
///
/// This will start a synchronous actor that controls the value cache. The
/// returned actor reference can be used to control the cache and the returned
/// `Cache` is a read handle to the cache.
pub fn start<S>(system: &mut ActorSystem<S>) -> Result<ActorRef<Sync<Message>>, RuntimeError> {
    // Spawn our cache master, which has write access to the value cache.
    let cache_master = cache_master as fn(_) -> _;
    system.spawn_sync_actor(NoSupervisor, cache_master, ())
}

/// Response to a [`Message`].
pub enum Response {
    Found(Value),
    NotFound,
    Ok,
}

/// Message used to control the value cache.
#[derive(Debug)]
pub enum Message {
    /// Store a new value in the cache.
    Store(Key, Value, ActorRef<Map<Response>>),
    /// Retrieve a value from the cache.
    Retrieve(Key, ActorRef<Map<Response>>),
    /// Remove a value from the cache.
    Remove(Key, ActorRef<Map<Response>>),
}

/// The actor that controls the cache.
fn cache_master(mut ctx: SyncContext<Message>) -> Result<(), !> {
    // TODO: use different hashing algorithm.
    let mut cache = HashMap::new();

    trace!("cache master starting");
    while let Ok(msg) = ctx.receive_next() {
        match msg {
            Message::Store(key, value, mut actor_ref) => {
                trace!("storing {} in cache: {:?}", key, value);
                cache.insert(key, value);
                actor_ref <<= Response::Ok;
            },
            Message::Retrieve(key, mut actor_ref) => {
                let value = cache.get(&key);
                match value {
                    Some(value) => actor_ref <<= Response::Found(Arc::clone(&value)),
                    // TODO: read cache from disk if not found?
                    None => actor_ref <<= Response::NotFound,
                }
            },
            Message::Remove(key, mut actor_ref) => {
                trace!("removing {} from cache", key);
                cache.remove(&key);
                actor_ref <<= Response::Ok;
            },
        }
    }

    trace!("cache master shutting down");
    Ok(())
}
