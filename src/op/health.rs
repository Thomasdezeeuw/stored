//! Module with the `Health` state machine.

use std::future::Future;
use std::pin::Pin;
use std::task::{self, Poll};
use std::time::Duration;

use heph::actor_ref::rpc::Rpc;
use heph::timer::Timer;
use heph::{actor, ActorRef};
use log::{debug, error};

use crate::db::{self, HealthCheck, HealthOk};

// TODO: base this on something.
const TIMEOUT: Duration = Duration::from_secs(1);

/// State machine that runs a health check.
pub struct Health {
    // Health check only has a single state.
    rpc: Rpc<HealthOk>,
    timer: Timer,
}

impl Health {
    /// Start a health check operation.
    ///
    /// Returns an error if the database actor can't be accessed.
    pub fn start<M>(
        ctx: &mut actor::Context<M>,
        db_ref: &mut ActorRef<db::Message>,
    ) -> Result<Health, ()> {
        debug!("running health check");
        match db_ref.rpc(ctx, HealthCheck) {
            Ok(rpc) => Ok(Health {
                rpc,
                timer: Timer::timeout(ctx, TIMEOUT),
            }),
            Err(err) => {
                error!("error making RPC call to database: {}", err);
                Err(())
            }
        }
    }
}

impl Future for Health {
    /// Returns an error if the database doesn't respond.
    type Output = Result<(), ()>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut task::Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.rpc).poll(ctx) {
            Poll::Pending => Pin::new(&mut self.timer).poll(ctx).map(|_| {
                error!("timeout waiting for RPC response from database");
                Err(())
            }),
            Poll::Ready(Ok(..)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(err)) => {
                error!("error waiting for RPC response from database: {}", err);
                Poll::Ready(Err(()))
            }
        }
    }
}
