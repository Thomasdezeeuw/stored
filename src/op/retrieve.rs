//! Module with the `Retrieve` state machine.

use std::future::Future;
use std::pin::Pin;
use std::task::{self, Poll};

use heph::actor_ref::rpc::Rpc;
use heph::timer::Timer;
use heph::{actor, ActorRef};
use log::{debug, error};

use crate::op::DB_TIMEOUT;
use crate::storage::BlobEntry;
use crate::{db, Key};

/// State machine that runs a health check.
pub struct Retrieve {
    // Retrieve only has a single state.
    rpc: Rpc<Option<BlobEntry>>,
    timer: Timer,
}

impl Retrieve {
    /// Start a retrieve operation.
    ///
    /// Returns an error if the database actor can't be accessed.
    pub fn start<M>(
        ctx: &mut actor::Context<M>,
        db_ref: &mut ActorRef<db::Message>,
        key: Key,
    ) -> Result<Retrieve, ()> {
        debug!("running retrieve operation");
        match db_ref.rpc(ctx, key) {
            Ok(rpc) => Ok(Retrieve {
                rpc,
                timer: Timer::timeout(ctx, DB_TIMEOUT),
            }),
            Err(err) => {
                error!("error making RPC call to database: {}", err);
                Err(())
            }
        }
    }
}

impl Future for Retrieve {
    /// Returns an error if the database doesn't respond.
    type Output = Result<Option<BlobEntry>, ()>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut task::Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.rpc).poll(ctx) {
            Poll::Pending => Pin::new(&mut self.timer).poll(ctx).map(|_| {
                error!("timeout waiting for RPC response from database");
                Err(())
            }),
            Poll::Ready(Ok(blob)) => Poll::Ready(Ok(blob)),
            Poll::Ready(Err(err)) => {
                error!("error waiting for RPC response from database: {}", err);
                Poll::Ready(Err(()))
            }
        }
    }
}
