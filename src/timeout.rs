//! Timeouts for various interactions.

use std::time::Duration;

// Timeouts for client interaction.

/// Timeout used for reading operations.
pub const CLIENT_READ: Duration = Duration::from_secs(10);
/// Timeout used when reading a second request.
pub const CLIENT_ALIVE: Duration = Duration::from_secs(120);

/// Returns the timeout for a write of `size` bytes.
///
/// Currently the timeout is 3 seconds per 1 MB.
pub const fn client_write(size: u64) -> Duration {
    Duration::from_secs(as_megabytes(size) * 3)
}

// Timeouts for peer interaction.

/// Timeout used for reading operations.
///
/// See [`peer_read`] for larger writes.
pub const PEER_READ: Duration = Duration::from_secs(2);
/// Timeout used when reading a second request.
pub const PEER_ALIVE: Duration = Duration::from_secs(120);
/// Timeout used for writing operations.
///
/// See [`peer_write`] for larger writes.
pub const PEER_WRITE: Duration = Duration::from_secs(1);
/// Timeout used in making an Remote Procedure Call.
pub const PEER_RPC: Duration = Duration::from_secs(5);
/// Time participants wait for a consensus phase.
pub const PEER_CONSENSUS: Duration = Duration::from_secs(10);
/// Time to wait between connection tries when connecting to a peer, should be
/// doubled after each try.
pub const PEER_CONNECT: Duration = Duration::from_millis(500);
/// Time the [`peer::sync`] actor wait before logging we're still waiting for
/// peer to be connected.
pub const BEFORE_FULL_SYNC: Duration = Duration::from_secs(5);

// Put a limit on the functions below: currently for a 1 GB blob the timeout
// would be ~17 minutes.

/// Returns the timeout for a read of `size` bytes.
///
/// Currently the timeout is 2 seconds per 1 MB.
pub const fn peer_read(size: u64) -> Duration {
    Duration::from_secs(as_megabytes(size) * 2)
}

/// Returns the timeout for a write of `size` bytes.
///
/// Currently the timeout is 1 second per 1 MB.
pub const fn peer_write(size: u64) -> Duration {
    Duration::from_secs(as_megabytes(size) * 1)
}

// Timeouts for database interaction.

/// Timeout used in database interaction, mainly in [`op::db_rpc`].
// TODO: base this on something.
pub const DB: Duration = Duration::from_secs(2);

/// Returns the size in megabytes from `size_in_bytes` + 1.
const fn as_megabytes(size_in_bytes: u64) -> u64 {
    const BYTES_PER_MEGABYTE: u64 = 1024 * 1024;
    (size_in_bytes / BYTES_PER_MEGABYTE) + 1
}
