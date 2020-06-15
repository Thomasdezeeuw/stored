//! Module with the `Health` state machine.

use std::future::Future;
use std::pin::Pin;
use std::task::{self, Poll};

use heph::{actor, ActorRef};
use log::debug;

use crate::db::{self, HealthCheck, HealthOk};
use crate::op::{db_rpc, DbRpc};

/// State machine that runs a health check.
pub struct Health {
    // Health check only has a single state.
    rpc: DbRpc<HealthOk>,
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
        db_rpc(ctx, db_ref, HealthCheck).map(|rpc| Health { rpc })
    }
}

impl Future for Health {
    /// Returns an error if the database doesn't respond.
    type Output = Result<HealthOk, ()>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut task::Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.rpc).poll(ctx)
    }
}
