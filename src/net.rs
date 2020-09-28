use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use heph::actor;
use heph::rt::RuntimeAccess;
use heph::timer::Timer;
use log::debug;

use crate::util::wait_for_wakeup;

pub use heph::net::TcpStream;

// Work around https://github.com/Thomasdezeeuw/heph/issues/287. FIXME: this
// needs to be moved into Heph, see
// https://github.com/Thomasdezeeuw/heph/issues/297.
pub(crate) async fn tcp_connect<M, K>(
    ctx: &mut actor::Context<M, K>,
    address: SocketAddr,
) -> io::Result<TcpStream>
where
    K: RuntimeAccess,
{
    // This is ugly code and it doesn't belong here.
    //
    // This relates directly Mio and `kqueue(2)` and `epoll(2)`. To do a
    // non-blocking TCP connect properly we need to a couple of this.
    //
    // 1. Setup a socket and call `connect(2)`. Mio (via Heph) does this for us.
    //    However it doesn't mean the socket is connected, as we can't determine
    //    that without blocking.
    // 2. To determine if a socket is connected we need to wait for a
    //    kqueue/epoll event (done in `wait_for_wakeup`). But that doesn't tell
    //    us whether or not the socket is connected, to determine this we use
    //    `getpeername` (`TcpStream::peer_addr`).
    //    However if we get a writable (and thus get scheduled) and the
    //    `getpeername` fails it doesn't actually mean the socket will never
    //    connect properly. Which leads us to step 3.
    // 3. Because we need a readable event, and we can't determine if we have
    //    one of those, we wait for another event and check `getpeername` again.
    //
    // Caveats:
    // * If we get scheduled by something other than an event for this
    //   connection we might waste one of our checks, causing us to discard
    //   valid, but not yet connected, sockets.
    // * On some platforms we might not get a second event, causing us to wait
    //   for ever.
    //
    // Sources:
    // * https://cr.yp.to/docs/connect.html
    // * https://stackoverflow.com/questions/17769964/linux-sockets-non-blocking-connect

    let mut stream = TcpStream::connect(ctx, address)?;

    // Ensure the `TcpStream` is connected, see the comments above.
    wait_for_wakeup().await;
    if let Ok(..) = stream.peer_addr() {
        // If we can get an peer address the stream is connected.
        return Ok(stream);
    }

    // Wait for another event.
    wait_for_wakeup().await;
    match stream.peer_addr() {
        Ok(..) => return Ok(stream),
        Err(err) => match stream.take_error() {
            Ok(None) => Err(err),
            Ok(Some(err)) => Err(err),
            Err(..) => Err(err),
        },
    }
}

// FIXME: move this to a new `net2` module in Heph, see
// https://github.com/Thomasdezeeuw/heph/issues/298.
pub(crate) async fn tcp_connect_retry<M, K>(
    ctx: &mut actor::Context<M, K>,
    address: SocketAddr,
    wait: Duration,
    max_tries: usize,
) -> io::Result<TcpStream>
where
    K: RuntimeAccess,
{
    let mut wait = wait;
    let mut i = 1;
    loop {
        match tcp_connect(ctx, address).await {
            Ok(stream) => return Ok(stream),
            Err(err) if i >= max_tries => return Err(err),
            Err(err) => {
                debug!(
                    "failed to connect to peer, but trying again ({}/{} tries): {}",
                    i, max_tries, err
                );
            }
        }

        // Wait a moment before trying to connect again.
        Timer::timeout(ctx, wait).await;
        // Wait a little longer next time.
        wait *= 2;
        i += 1;
    }
}
