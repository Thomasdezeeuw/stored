//! Module with the `Retrieve` state machine.

use std::future::Future;
use std::pin::Pin;
use std::task::{self, Poll};

use heph::{actor, ActorRef};
use log::debug;

use crate::op::{db_rpc, DbRpc};
use crate::storage::BlobEntry;
use crate::{db, Key};

/// State machine that runs a health check.
pub struct Retrieve {
    // Retrieve only has a single state.
    rpc: DbRpc<Option<BlobEntry>>,
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
        db_rpc(ctx, db_ref, key).map(|rpc| Retrieve { rpc })
    }
}

impl Future for Retrieve {
    /// Returns an error if the database doesn't respond.
    type Output = Result<Option<BlobEntry>, ()>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut task::Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.rpc).poll(ctx)
    }
}
