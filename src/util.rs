//! Utilities, a module that shouldn't exists (here).

use std::future::Future;
use std::pin::Pin;
use std::task::{self, Poll};

/// Returns a [`Future`] that yields once, waiting for another event to wake
/// up the future.
pub(crate) const fn wait_for_wakeup() -> YieldOnce {
    YieldOnce(false)
}

/// [`Future`] that return `Poll::Pending` when polled the first time, without
/// waking itself.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub(crate) struct YieldOnce(bool);

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _: &mut task::Context) -> Poll<Self::Output> {
        if self.0 {
            // Yielded already.
            Poll::Ready(())
        } else {
            // Not yet yielded.
            self.0 = true;
            Poll::Pending
        }
    }
}
