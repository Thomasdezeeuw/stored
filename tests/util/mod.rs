use std::future::Future;
use std::pin::Pin;
use std::task::{self, Poll};

use futures_util::task::noop_waker;

pub fn poll_wait<Fut>(mut future: Pin<&mut Fut>) -> Fut::Output
where
    Fut: Future,
{
    // This is not great.
    let waker = noop_waker();
    let mut ctx = task::Context::from_waker(&waker);
    loop {
        match future.as_mut().poll(&mut ctx) {
            Poll::Ready(result) => return result,
            Poll::Pending => continue,
        }
    }
}
