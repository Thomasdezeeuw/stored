//! Actor that gets started once we've created a fully connected network of
//! peers. It will synchronise the blob stored between all peers.

use std::sync::Arc;

use futures_util::future::{select, Either};
use heph::actor::context::ThreadSafe;
use heph::actor::messages::Start;
use heph::actor_ref::ActorGroup;
use heph::timer::Timer;
use heph::{actor, ActorRef};
use log::{debug, error, info};
use parking_lot::RwLock;

use crate::op::full_sync;
use crate::peer::Peers;
use crate::util::CountDownLatch;
use crate::{config, db, timeout};

pub async fn actor(
    mut ctx: actor::Context<!, ThreadSafe>,
    mut db_ref: ActorRef<db::Message>,
    start_http_ref: Arc<RwLock<ActorGroup<Start>>>,
    start: Arc<CountDownLatch>,
    peers: Peers,
) -> Result<(), !> {
    debug!("waiting until all peers are connect before starting full synchronisation");
    loop {
        let timeout = Timer::timeout(&mut ctx, timeout::BEFORE_FULL_SYNC);
        match select(start.wait(), timeout).await {
            // All peers connected, we can start.
            Either::Left(..) => break,
            Either::Right(..) => {
                // After a while the user might wonder why we aren't serving any
                // requests yet. So let the user know to which peers are not
                // connected yet.
                let unconnected_peers = peers.unconnected();
                if unconnected_peers.is_empty() {
                    // FIXME: this should never happen.
                    break;
                }

                info!(
                    "waiting for peers to be connected: unconnected_peers={}",
                    // Reuse pretty formatting.
                    config::Peers(unconnected_peers)
                );
            }
        }
    }

    info!("starting full synchronisation");
    match full_sync(&mut ctx, &mut db_ref, &peers).await {
        Ok(()) => {
            debug!("completed full synchronisation, starting HTTP actor");
            if let Err(err) = start_http_ref.read().try_send(Start(())) {
                error!("failed to start HTTP listener: {}", err);
            }
        }
        Err(()) => {
            error!("failed to synchronise stored blobs with peers, aborting start");
        }
    }

    Ok(())
}
