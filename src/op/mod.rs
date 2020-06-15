//! Module with state machine for support operations.

use std::time::Duration;

pub mod health;
pub mod retrieve;

pub use health::Health;
pub use retrieve::Retrieve;

/// Timeout for connecting to the database.
// TODO: base this on something.
const DB_TIMEOUT: Duration = Duration::from_secs(1);
