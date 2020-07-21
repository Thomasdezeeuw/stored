//! Actor that gets started once we've created a fully connected network of
//! peers. It will synchronise the blob stored between all peers.

use std::sync::Arc;

use heph::actor::context::ThreadSafe;
use heph::actor::messages::Start;
use heph::actor_ref::ActorGroup;
use heph::{actor, ActorRef};
use log::{debug, error};
use parking_lot::RwLock;

use crate::db;
use crate::op::full_sync;
use crate::peer::Peers;
use crate::util::CountDownLatch;

pub async fn actor(
    mut ctx: actor::Context<!, ThreadSafe>,
    mut db_ref: ActorRef<db::Message>,
    start_http_ref: Arc<RwLock<ActorGroup<Start>>>,
    start: Arc<CountDownLatch>,
    peers: Peers,
) -> Result<(), !> {
    debug!("waiting until synchronisation actor can start");
    // Wait until all worker threads are started and all peers are connected.
    start.wait().await;
    debug!("synchronising stored blobs with peers");

    match full_sync(&mut ctx, &mut db_ref, &peers).await {
        Ok(()) => {
            debug!("completed synchronisation with peers, starting HTTP actor");
            if let Err(err) = start_http_ref.read().send(Start(())) {
                error!("failed to start HTTP listener: {}", err);
            }
        }
        Err(()) => {
            error!("failed to synchronise stored blobs with peers, aborting start");
        }
    }

    Ok(())
}
