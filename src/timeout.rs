//! Timeouts for various interactions.

use std::time::Duration;

// Timeouts for client interaction.

/// Timeout used for reading operations.
pub const CLIENT_READ: Duration = Duration::from_secs(10);
/// Timeout used when reading a second request.
pub const CLIENT_ALIVE: Duration = Duration::from_secs(120);
/// Timeout used for writing operations.
pub const CLIENT_WRITE: Duration = Duration::from_secs(5);

// Timeouts for peer interaction.

/// Timeout used for reading operations.
pub const PEER_READ: Duration = Duration::from_secs(2);
/// Timeout used when reading a second request.
pub const PEER_ALIVE: Duration = Duration::from_secs(120);
/// Timeout used for writing operations.
pub const PEER_WRITE: Duration = Duration::from_secs(1);
/// Time participants wait for a consensus phase.
pub const PEER_CONSENSUS: Duration = Duration::from_secs(10);
/// Time to wait between connection tries when connecting to a peer, should be
/// doubled after each try.
pub const PEER_CONNECT: Duration = Duration::from_millis(500);

// Timeouts for database interaction.

/// Timeout used in database interaction, mainly in [`op::db_rpc`].
// TODO: base this on something.
pub const DB: Duration = Duration::from_millis(500);
