//! Module with state machines for the supported operations.

use std::future::Future;
use std::pin::Pin;
use std::task::{self, Poll};
use std::time::Duration;

use heph::actor_ref::rpc::{Rpc, RpcMessage};
use heph::timer::Timer;
use heph::{actor, ActorRef};
use log::error;

use crate::db;

pub mod health;
pub mod retrieve;

pub use health::Health;
pub use retrieve::Retrieve;

/// Timeout for connecting to the database.
// TODO: base this on something.
const DB_TIMEOUT: Duration = Duration::from_secs(1);

/// Make a RPC to the database (`db_ref`) applying the `DB_TIMEOUT`.
fn db_rpc<M, Req, Res>(
    ctx: &mut actor::Context<M>,
    db_ref: &mut ActorRef<db::Message>,
    request: Req,
) -> Result<DbRpc<Res>, ()>
where
    db::Message: From<RpcMessage<Req, Res>>,
{
    match db_ref.rpc(ctx, request) {
        Ok(rpc) => Ok(DbRpc {
            rpc,
            timer: Timer::timeout(ctx, DB_TIMEOUT),
        }),
        Err(err) => {
            error!("error making RPC call to database: {}", err);
            Err(())
        }
    }
}

/// Wrapper around [`Rpc`] that adds a [`DB_TIMEOUT`], logging if it hits any
/// errors.
///
/// See [`db_rpc`].
struct DbRpc<Res> {
    rpc: Rpc<Res>,
    timer: Timer,
}

impl<Res> Future for DbRpc<Res> {
    /// Returns an error if the database doesn't respond.
    type Output = Result<Res, ()>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut task::Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.rpc).poll(ctx) {
            Poll::Ready(Ok(res)) => Poll::Ready(Ok(res)),
            Poll::Ready(Err(err)) => {
                error!("error waiting for RPC response from database: {}", err);
                Poll::Ready(Err(()))
            }
            Poll::Pending => Pin::new(&mut self.timer).poll(ctx).map(|_| {
                error!("timeout waiting for RPC response from database");
                Err(())
            }),
        }
    }
}
