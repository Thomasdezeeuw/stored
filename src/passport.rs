//! Request passport.
//!
//! A request passport is used to track the request progress and timing.

use std::error::Error;
use std::mem::size_of;
use std::ops::Range;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use std::{array, fmt};

use getrandom::getrandom;
use log::warn;

/// Request passport.
///
/// Collection of events and the time at which they occurred.
#[derive(Debug)]
pub struct Passport {
    id: Uuid,
    start: Instant,
    marks: Vec<Mark>,
}

impl Passport {
    /// Create an empty `Passport` with an zero id.
    pub fn empty() -> Passport {
        Passport {
            id: Uuid::empty(),
            start: Instant::now(),
            marks: Vec::new(),
        }
    }

    /// Create a new `Passport` with unique id.
    pub fn new() -> Passport {
        Passport {
            id: Uuid::new(),
            start: Instant::now(),
            marks: Vec::new(),
        }
    }

    /// Set the id of the request.
    pub fn set_id(&mut self, id: Uuid) {
        self.id = id;
    }

    /// Returns the `Uuid` for the this passport.
    pub fn id(&self) -> &Uuid {
        &self.id
    }

    /// Returns the time elapsed between the start and the last mark.
    ///
    /// If no marks have been made it will return a duration of zero.
    pub fn elapsed(&self) -> Duration {
        match self.marks.last() {
            Some(mark) => mark.timestamp - self.start,
            // TODO: replace with `Duration::zero` once stablised.
            None => Duration::new(0, 0),
        }
    }

    /// Mark the passport with a new `event`.
    pub fn mark(&mut self, event: Event) {
        let mark = Mark {
            timestamp: Instant::now(),
            event,
        };
        self.marks.push(mark);
    }

    /// Reuse the passport for another request.
    pub fn reset(&mut self) {
        self.id = Uuid::empty();
        self.start = Instant::now();
        self.marks.clear();
    }
}

impl fmt::Display for Passport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{request_id=\"{}\", events=[", self.id)?;
        let mut last_time = self.start;
        let mut first = true;
        for mark in self.marks.iter() {
            if first {
                first = false;
            } else {
                write!(f, ", ")?;
            }
            let timestamp = mark.timestamp();
            write!(
                f,
                "{{event=\"{}\", duration=\"{:?}\"}}",
                mark.event(),
                timestamp - last_time,
            )?;
            last_time = timestamp;
        }
        write!(f, "]}}")
    }
}

/// Universally Unique Identifier.
///
/// # Notes
///
/// Loosely follows [RFC4122].
///
/// [RFC4122]: http://tools.ietf.org/html/rfc4122
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct Uuid {
    bytes: [u8; 16], // 128 bits.
}

// Initial ids used by [`Uuid::new`] to generate new unique ids. Can be
// optionally initialised by [`Uuid::initialise`].
// `fetch_add` (used by `Uuid::new`) wraps around so any number is fine.
static ID0: AtomicU64 = AtomicU64::new(9396178701149223067);
static ID1: AtomicU64 = AtomicU64::new(6169990013871724815);

impl Uuid {
    /// Returns an empty `Uuid`, containing all zeros.
    pub const fn empty() -> Uuid {
        Uuid { bytes: [0; 16] }
    }

    /// Initialise the starting [`Uuid`] based on which pseudo-random ids are
    /// generated using [`Uuid::new`].
    pub fn initialise() {
        let mut bytes = [0; size_of::<u64>() * 2];
        if let Err(err) = getrandom(&mut bytes) {
            // We can continue on as the correct operation doesn't depend on the
            // randomness of `Uuid`.
            warn!("failied to initialise `Uuid` properly: {}", err);
        } else {
            let mut ids = bytes.array_chunks();
            let id0 = u64::from_ne_bytes(*ids.next().unwrap());
            let id1 = u64::from_ne_bytes(*ids.next().unwrap());
            ID0.store(id0, Ordering::Relaxed);
            ID1.store(id1, Ordering::Relaxed);
        }
    }

    /// Returns a new `Uuid`, unique during the running of the process.
    ///
    /// Call [`Uuid::initialise`] to initialise the starting id. Not calling
    /// `initialise` doesn't result in an error, but does make the ids
    /// predictable.
    pub fn new() -> Uuid {
        // Oh no! Don't look here. Okay you got me... these aren't random bytes.
        // But we really only need 128 unique bits, not actually random bits.
        //
        // I tested this with 10 million `Uuid`s and they were all unique. If
        // you have a machine that can process that many requests in a time
        // where the timestamp can't differentiate the logs enough open an
        // issue.
        let bytes1 = ID1.fetch_add(92478483931537517, Ordering::Relaxed);
        let bytes0 = ID0.fetch_add(bytes1, Ordering::Relaxed);
        let mut bytes = ((bytes0 as u128) + ((bytes1 as u128) << 64)).to_ne_bytes();

        // Set the variant to RFC4122 (section 4.1.1).
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        // Set the version to 4 (random) (section 4.1.3).
        bytes[6] = (bytes[6] & 0x0f) | (4 << 4);

        Uuid { bytes }
    }

    /// Parses an `Uuid` from `input`.
    ///
    /// Expects 32 bytes in hex formatted, 36 bytes if the `input` is
    /// hyphenated.
    pub fn parse_bytes(input: &[u8]) -> Result<Uuid, ParseUuidErr> {
        match input.len() {
            32 => from_hex(input),
            36 => from_hex_hyphenated(input),
            _ => Err(ParseUuidErr(())),
        }
    }
}

impl fmt::Debug for Uuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for Uuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Always force a length of 32.
        write!(f, "{:032x}", u128::from_be_bytes(self.bytes))
    }
}

/// Error returned by the [`FromStr`] implementation for [`Uuid`].
pub struct ParseUuidErr(());

impl fmt::Debug for ParseUuidErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ParseUuidErr").finish()
    }
}

impl fmt::Display for ParseUuidErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid request id")
    }
}

impl Error for ParseUuidErr {}

impl FromStr for Uuid {
    type Err = ParseUuidErr;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Uuid::parse_bytes(input.as_bytes())
    }
}

/// `input` must be 32 bytes long.
fn from_hex(input: &[u8]) -> Result<Uuid, ParseUuidErr> {
    let mut bytes = [0; 16];
    for (idx, chunk) in input.chunks_exact(2).enumerate() {
        let lower = from_hex_byte(chunk[1]).map_err(ParseUuidErr)?;
        let higher = from_hex_byte(chunk[0]).map_err(ParseUuidErr)?;
        bytes[idx] = lower | (higher << 4);
    }
    Ok(Uuid { bytes })
}

/// `input` must be 36 bytes long.
fn from_hex_hyphenated(input: &[u8]) -> Result<Uuid, ParseUuidErr> {
    let mut bytes = [0; 16];
    let mut idx = 0;

    // Groups of 8, 4, 4, 4, 12 bytes.
    let groups: [Range<usize>; 5] = [0..8, 9..13, 14..18, 19..23, 24..36];

    for group in array::IntoIter::new(groups) {
        let group_end = group.end;
        for chunk in input[group].chunks_exact(2) {
            let lower = from_hex_byte(chunk[1]).map_err(ParseUuidErr)?;
            let higher = from_hex_byte(chunk[0]).map_err(ParseUuidErr)?;
            bytes[idx] = lower | (higher << 4);
            idx += 1;
        }

        if let Some(b) = input.get(group_end) {
            if *b != b'-' {
                return Err(ParseUuidErr(()));
            }
        }
    }

    Ok(Uuid { bytes })
}

fn from_hex_byte(b: u8) -> Result<u8, ()> {
    match b {
        b'A'..=b'F' => Ok(b - b'A' + 10),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'0'..=b'9' => Ok(b - b'0'),
        _ => Err(()),
    }
}

/// A mark in the request [`Passport`].
///
/// This marks that an [`Event`] took place at a given [time].
///
/// [time]: Mark::timestamp
#[derive(Copy, Clone, Debug)]
pub struct Mark {
    timestamp: Instant,
    event: Event,
}

impl Mark {
    /// Returns the time at which the event took place.
    pub fn timestamp(self) -> Instant {
        self.timestamp
    }

    /// The event that took place.
    pub fn event(self) -> Event {
        self.event
    }
}

/// Macro to create the [`Event`] type.
macro_rules! events {
    ($( $name: ident => $msg: expr ),+ $(,)*) => {
        /// The type of event marked in the [`Passport`].
        ///
        /// This is effectively a `&'static str`, but that is 16 bytes, this is
        /// only 1. Use the [`fmt::Display`] implementation to get the event
        /// description.
        #[derive(Copy, Clone, Debug)]
        #[allow(missing_docs)]
        pub enum Event {
            $( $name, )+
        }

        impl fmt::Display for Event {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let msg = match self {
                    $( Event::$name => $msg, )+
                };
                f.write_str(msg)
            }
        }
    };
}

events!(
    // # HTTP.
    ParsedHttpRequest => "read and parsed HTTP request heading",
    ReadHttpBody => "read HTTP body",
    WrittenHttpResponse => "written HTTP response",

    // # Peer interaction.
    // Server.
    ReadPeerRequest => "read peer request header",
    ReadPeerKey => "read key from peer",
    ReadPeerDateSince => "reading date since to retrieve keys",
    ReadPeerMetadata => "read blob metadata from peer",
    ReadPeerBlob => "read blob from peer",
    ConnectedToPeerServer => "connected to peer server",
    WrittenPeerResponse => "written peer response",
    // Store/remove blob.
    WrittenRequestBlobRequest => "written request blob peer request",
    ReadRequestBlobResponse => "read request blob response",
    ReadRequestBlobResponseBlob => "read blob from request blob response",
    ConsensusPhaseOneComplete => "consensus phase one complete",
    ConsensusFailed => "consensus failed",

    // # Ops.
    // Store blob.
    AddedBlob => "added blob to storage",
    FailedToAddBlob => "failed to add blob to storage",
    CommittedStoringBlob => "committed to storing blob query",
    FailedToCommitStoringBlob => "failed to commit storing blob query",
    AbortedStoringBlob => "aborted store blob query",
    FailedToAbortStoringBlob => "failed to abort store blob query",
    // Streaming blob.
    StreamingBlob => "streaming blob to storage",
    StreamedBlob => "streamed blob to storage",
    FailedToStreamBlob => "failed to stream blob to storage",
    // Retrieve blob.
    RetrievedBlob => "retrieved blob",
    FailedToRetrieveBlob => "failed to retrieve blob",
    // Contains blob.
    ContainsBlob => "checked if blob is stored",
    FailedToCheckIfContainsBlob => "failed to check if blob is stored",
    // Remove blob.
    PreppedRemoveBlob => "prepared storage for removing blob",
    FailedToPrepRemoveBlob => "failed to prepare storage for removing blob",
    CommittedRemovingBlob => "committed to removing blob query",
    FailedToCommitRemovingBlob => "failed to commit removing blob query",
    AbortedRemovingBlob => "aborted remove blob query",
    FailedToAbortRemovingBlob => "failed to abort remove blob query",
    // Retrieve uncommitted blob.
    RetrievedUncommittedBlob => "retrieved uncommitted blob",
    FailedToRetrieveUncommittedBlob => "failed to retrieve uncommitted blob",
    // Retrieve uncommitted blob.
    RetrievedStoreBlobQuery => "retrieved store blob query",
    FailedToRetrieveStoreBlobQuery => "failed to retrieve store blob query",
    // Retrieve keys.
    RetrievedKeys => "retrieved keys",
    FailedToRetrieveKeys => "failed to retrieve keys",
    // Retrieve entries.
    RetrievedEntries => "retrieved entries",
    FailedToRetrieveEntries => "failed to retrieve entries",
    // Health check.
    HealthCheckComplete => "health check complete",
    HealthCheckFailed => "health check failed",
    // Sync stored blob.
    SyncedStoredBlob => "synced stored blob",
    FailedToSyncStoredBlob => "failed to sync stored blob",
    // Sync removed blob.
    SyncedRemovedBlob => "synced removed blob",
    FailedToSyncRemovedBlob => "failed to sync removed blob",
    // Consensus.
    StartingConsensus => "starting consensus",
    ConsensusPhaseOneResults => "got consensus phase one results",
    ConsensusPhaseTwoResults => "got consensus phase two results",
    ConsensusCommitted => "coordinator and participants committed",
    AbortedConsensusRun => "aborted a consensus run",
);

#[cfg(test)]
mod tests {
    use std::cmp::min;
    use std::mem::size_of;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;

    use super::{Event, Mark, Uuid};

    #[test]
    fn sizes() {
        assert_eq!(size_of::<&'static str>(), 16);
        assert_eq!(size_of::<Event>(), 1);

        #[cfg(any(target_os = "ios", target_os = "macos"))]
        assert_eq!(size_of::<Mark>(), 16);
        #[cfg(any(target_os = "freebsd", target_os = "linux"))]
        assert_eq!(size_of::<Mark>(), 24);
    }

    #[test]
    fn uuid_from_str() {
        let tests = &[
            "00000000000000000000000000000000",
            "ffffffffffffffffffffffffffffffff",
            "f9ed4675f1c53513c61a3b3b4e25b4c0",
            "30f14c6c1fc85cba12bfd093aa8f90e3",
            "6a7f7b2f889b4ae8b849db1f635c971c",
            "a0a86a102a3a4852ae80893e5d4e8348",
            "2bcf9cec771740e39bcfb438b93b7770",
            "0e748c6370ef4d45ab9796510ddcdf5a",
        ];

        for test in tests {
            let wanted = test;
            let input = test;
            let got = Uuid::from_str(input)
                .unwrap_or_else(|err| panic!("failed to parse UUID: {}: input='{}'", err, input));
            assert_eq!(got.to_string(), *wanted, "input: '{}'", input);

            // Hyphenated.
            let mut input = String::with_capacity(36);
            input.push_str(&test[0..8]);
            input.push('-');
            input.push_str(&test[8..12]);
            input.push('-');
            input.push_str(&test[12..16]);
            input.push('-');
            input.push_str(&test[16..20]);
            input.push('-');
            input.push_str(&test[20..32]);

            let got = Uuid::from_str(&*input)
                .unwrap_or_else(|err| panic!("failed to parse UUID: {}: input='{}'", err, input));
            assert_eq!(got.to_string(), *wanted, "input: '{}'", input);
        }
    }

    #[test]
    fn fmt_uuid() {
        let uuid = Uuid::new();
        let string = uuid.to_string();
        assert_eq!(string.len(), 32);
    }

    #[test]
    #[ignore]
    fn test_relaxed_ordering() {
        // Test required for `Uuid::new`.

        const N_THREADS: usize = 8;
        const N_ADDS: usize = 1000000;

        static N: AtomicU64 = AtomicU64::new(0);

        let barrier = Arc::new(Barrier::new(N_THREADS));
        let mut handles = Vec::with_capacity(N_THREADS);
        for _ in 0..N_THREADS {
            let barrier = barrier.clone();
            let handle = thread::spawn(move || {
                let mut numbers = Vec::with_capacity(N_ADDS);
                barrier.wait();

                for _ in 0..N_ADDS {
                    let n = N.fetch_add(1, Ordering::Relaxed);
                    numbers.push(n);
                }
                numbers
            });
            handles.push(handle);
        }

        let mut numbers = handles
            .into_iter()
            .flat_map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        numbers.sort_unstable();

        for idx in 0..numbers.len() - 1 {
            let number = numbers[idx];
            if numbers[idx + 1] == number {
                panic!(
                    "numbers not unique: {:?}",
                    &numbers[idx..min(idx + 10, numbers.len())]
                );
            }
        }
    }

    #[test]
    #[ignore]
    fn uuids_are_unique_10_mln() {
        const N: usize = 10_000_000;
        let mut uuids: Vec<Uuid> = (0..N).map(|_| Uuid::new()).collect();
        uuids.sort_unstable();
        let mut iter = uuids.into_iter();
        while let Some(uuid) = iter.next() {
            if let Some(next) = iter.as_slice().first() {
                if uuid == *next {
                    panic!("failed to generate {} unique keys", N);
                }
            }
        }
    }
}
