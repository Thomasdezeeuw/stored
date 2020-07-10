//! Actor that gets started once we've created a fully connected network of
//! peers. It will synchronise the blob stored between all peers.

use heph::actor::context::ThreadSafe;
use heph::{actor, ActorRef};
use log::{debug, error};

use crate::op::full_sync;
use crate::peer::Peers;
use crate::{db, Buffer};

pub async fn actor(
    mut ctx: actor::Context<!, ThreadSafe>,
    mut db_ref: ActorRef<db::Message>,
    peers: Peers,
) -> Result<(), !> {
    debug!("synchronising stored blobs with peers");

    let mut buf = Buffer::new();
    match full_sync(&mut ctx, &mut db_ref, &peers, &mut buf).await {
        Ok(()) => {
            debug!("completed synchronisation with peers, starting HTTP actor");
            // TODO: only start HTTP actor at this point.
        }
        Err(()) => {
            error!("failed to synchronise stored blobs with peers, aborted start");
        }
    }

    Ok(())
}
