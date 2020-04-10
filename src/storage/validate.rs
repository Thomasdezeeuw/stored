//! Module with utilities to validate `Storage`.

use std::cmp::min;
use std::num::NonZeroUsize;
use std::path::Path;
use std::{io, thread};

use crossbeam_channel::{Receiver, Sender};

use crate::storage::{Blob, Storage};
use crate::Key;

/// Corruption found in [`Storage`].
///
/// These can be found using [`validate_storage`].
#[derive(Debug)]
pub struct Corruption {
    key: Key,
}

impl Corruption {
    /// Create a new corruption for `key`.
    fn for_key(key: Key) -> Corruption {
        Corruption { key }
    }

    /// Returns the `Key` for the blob that is corrupted.
    pub fn key(&self) -> &Key {
        &self.key
    }
}

/// Convenience function to open `Storage` at `path` and call
/// [`validate_storage`] on it.
pub fn validate<P: AsRef<Path>>(path: P, n_threads: NonZeroUsize) -> io::Result<Vec<Corruption>> {
    Storage::open(path).and_then(|mut storage| validate_storage(&mut storage, n_threads))
}

/// Validate `storage`, ensuring that all blobs are still intact and match there
/// keys.
///
/// Starts `n_threads` threads to validate the storage.
pub fn validate_storage(
    storage: &mut Storage,
    n_threads: NonZeroUsize,
) -> io::Result<Vec<Corruption>> {
    let channel_size = min(1000, storage.len());
    let (entries, entry_recv) = crossbeam_channel::bounded(channel_size);
    let (corruption_send, corruptions) = crossbeam_channel::unbounded();

    let threads: Vec<thread::JoinHandle<()>> = (0..n_threads.get())
        .into_iter()
        .map(|_| {
            let entries = entry_recv.clone();
            let corruptions = corruption_send.clone();
            thread::spawn(move || validate_blobs(entries, corruptions))
        })
        .collect();

    // Send out work to the workers.
    for (key, blob) in storage.blobs.iter() {
        // TODO: handle error properly.
        entries.send((key.clone(), blob.clone())).unwrap();
    }

    // Ensure we wait on ourselves.
    drop(entries);
    // Help out in processing the entries.
    validate_blobs(entry_recv, corruption_send);

    // Wait until all threads are done.
    for handle in threads {
        // TODO: handle error properly.
        handle.join().unwrap();
    }

    // And return the corruptions found.
    Ok(corruptions.iter().collect())
}

/// Validates all `entries`, sending any `corruptions` found back.
fn validate_blobs(entries: Receiver<(Key, Blob)>, corruptions: Sender<Corruption>) {
    for (key, blob) in entries {
        let got = Key::for_blob(blob.bytes());
        if got != key {
            if let Err(err) = corruptions.send(Corruption::for_key(key)) {
                log::error!(
                    "failed to send corruption for blob: {}",
                    err.into_inner().key()
                );
                break;
            }
        }
    }
}
