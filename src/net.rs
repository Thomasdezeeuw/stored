use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;
use std::{io, ptr};

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

/// Get all local known IP address.
pub(crate) fn local_address() -> io::Result<Vec<IpAddr>> {
    let mut addresses = Vec::new();
    let mut interfaces: *mut libc::ifaddrs = ptr::null_mut();

    if unsafe { libc::getifaddrs(&mut interfaces) } == -1 {
        return Err(io::Error::last_os_error());
    }

    // Loop over the interfaces.
    let mut ifa = interfaces;
    while let Some(interface) = unsafe { ifa.as_ref() } {
        if let Some(addr) = unsafe { interface.ifa_addr.as_ref() } {
            match addr.sa_family as libc::c_int {
                libc::AF_INET => {
                    let address = IpAddr::V4(unsafe {
                        // (sockaddr -> sockaddr_in).sin_addr -> in_addr -> Ipv4Addr.
                        *(&(*(addr as *const _ as *const libc::sockaddr_in)).sin_addr
                            as *const libc::in_addr as *const Ipv4Addr)
                    });
                    addresses.push(address);
                }
                libc::AF_INET6 => {
                    let address = IpAddr::V6(unsafe {
                        // (sockaddr -> sockaddr_in6).sockaddr_in -> sockaddr_in -> Ipv6Addr.
                        *(&(*(addr as *const _ as *const libc::sockaddr_in6)).sin6_addr
                            as *const libc::in6_addr as *const Ipv6Addr)
                    });
                    addresses.push(address);
                }
                // Only interested in IP interfaces.
                _ => {}
            }
        }

        // Next interface.
        ifa = interface.ifa_next;
    }

    unsafe { libc::freeifaddrs(interfaces) };

    Ok(addresses)
}
