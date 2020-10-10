//! Utilities, a module that shouldn't exists (here).

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{self, Poll};

use parking_lot::Mutex;

/// Wait until a `count` number of operations have completed in other
/// threads/futures.
///
/// # Notes
///
/// Can only be used once and only a single future can [`wait`] on the latch.
///
/// [`wait`]: CountDownLatch::wait
#[derive(Debug)]
pub struct CountDownLatch {
    /// Number of operations still uncompleted.
    count: AtomicUsize,
    /// Set to some in [`WaitOnLatch`] if the count is not zero, waked by
    /// [`CountDownLatch::decrease`] if `Some` and the count is zero.
    waker: Mutex<Option<task::Waker>>,
}

impl CountDownLatch {
    /// Create a new `CountDownLatch`.
    pub const fn new(count: usize) -> CountDownLatch {
        CountDownLatch {
            count: AtomicUsize::new(count),
            waker: Mutex::new(None),
        }
    }

    /// Returns the current count.
    ///
    /// # Notes
    ///
    /// Doing anything based on this function will always result in a bit of
    /// race.
    pub fn get_count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    /// Decrease the count on the latch.
    ///
    /// This is safe to call more then `count` times (the `count` this
    /// `CountDownLatch` was created with).
    pub fn decrease(&self) {
        if self.count.fetch_sub(1, Ordering::Release) == 1 {
            let mut guard = self.waker.lock();
            let waker = guard.take();
            drop(guard);
            if let Some(waker) = waker {
                waker.wake()
            }
        }
    }

    /// Increase the count on the latch.
    ///
    /// # Notes
    ///
    /// This function is hard to use correctly. Be sure that the count is
    /// at least one before calling this function, otherwise the future waiting
    /// might not wait for this future (that increased the count).
    pub fn increase(&self) {
        self.count.fetch_add(1, Ordering::Release);
    }

    /// Wait until the latch is counted to zero.
    ///
    /// # Notes
    ///
    /// Can only be called by a single future.
    pub const fn wait(&self) -> WaitOnLatch {
        WaitOnLatch { latch: self }
    }
}

/// [`Future`] returned by [`CountDownLatch::wait`].
pub struct WaitOnLatch<'l> {
    latch: &'l CountDownLatch,
}

impl<'l> Future for WaitOnLatch<'l> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, ctx: &mut task::Context) -> Poll<Self::Output> {
        if self.latch.get_count() == 0 {
            Poll::Ready(())
        } else {
            let waker = ctx.waker().clone();
            *self.latch.waker.lock() = Some(waker);

            // Check again to not miss any wake-ups.
            if self.latch.get_count() == 0 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }
}
