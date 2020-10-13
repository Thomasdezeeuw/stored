use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use heph::actor;
use heph::net::TcpStream;
use heph::rt::RuntimeAccess;
use heph::timer::Timer;
use log::debug;

// FIXME: move this to a new `net2` module in Heph, see
// https://github.com/Thomasdezeeuw/heph/issues/298.
pub(crate) async fn tcp_connect_retry<M, K>(
    ctx: &mut actor::Context<M, K>,
    address: SocketAddr,
    wait: Duration,
    max_tries: usize,
) -> io::Result<TcpStream>
where
    actor::Context<M, K>: RuntimeAccess,
{
    let mut wait = wait;
    let mut i = 1;
    loop {
        // NOTE: the `?` returns the errors encountered with local operations,
        // e.g. creating a socket and registering it with `mio::Poll`, these
        // operations we don't want to retry.
        match TcpStream::connect(ctx, address)?.await {
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
