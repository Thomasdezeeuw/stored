//! Simple in-memory storage implementation.
//!
//! Nothing special, simply a `HashMap` with `Arc<[u8]>` as blob.

use std::hash::BuildHasherDefault;
use std::sync::Arc;

use crate::key::{Key, KeyHasher};
use crate::storage::{AddError, Blob, Read, Write};

/// Create a new in-memory storage.
pub fn new() -> (Writer, Handle) {
    let (w, h) = hashmap::with_hasher(BuildHasherDefault::default());
    (Writer { inner: w }, Handle { inner: h })
}

/// BLOB (Binary Large OBject) stored in-memory.
pub struct InMemoryBlob(Arc<[u8]>);

impl Clone for InMemoryBlob {
    fn clone(&self) -> InMemoryBlob {
        InMemoryBlob(self.0.clone())
    }
}

impl Blob for InMemoryBlob {
    fn len(&self) -> usize {
        self.0.len()
    }
}

/// Write access to the storage.
pub struct Writer {
    inner: hashmap::Writer<Key, InMemoryBlob, BuildHasherDefault<KeyHasher>>,
}

impl Write for Writer {
    type Error = !;

    fn add_blob(&mut self, blob: &[u8]) -> Result<Key, AddError<Self::Error>> {
        let key = Key::for_blob(blob);
        if self.inner.contains_key(&key) {
            Err(AddError::AlreadyStored(key))
        } else {
            let blob = InMemoryBlob(Arc::from(blob));
            self.inner.insert(key.clone(), blob);
            self.inner.flush();
            Ok(key)
        }
    }

    fn remove_blob(&mut self, key: Key) -> Result<bool, Self::Error> {
        let existed = self.inner.remove(key).is_some();
        self.inner.flush();
        Ok(existed)
    }
}

/// Handle to the storage that can be send across thread bounds.
pub struct Handle {
    inner: hashmap::Handle<Key, InMemoryBlob, BuildHasherDefault<KeyHasher>>,
}

impl Handle {
    /// Turn this `Handle` into a `Reader`.
    pub fn into_reader(self) -> Reader {
        Reader {
            inner: self.inner.into_reader(),
        }
    }
}

/// Read access to the storage.
pub struct Reader {
    inner: hashmap::Reader<Key, InMemoryBlob, BuildHasherDefault<KeyHasher>>,
}

impl Read for Reader {
    type Blob = InMemoryBlob;

    type Error = !;

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn data_size(&self) -> u64 {
        // TODO: not ideal. Maybe add `Reader::for_each_value` method?
        let mut total = 0;
        for key in self.inner.keys() {
            if let Some(blob) = self.inner.get(&key) {
                total += blob.len() as u64;
            }
        }
        total
    }

    fn total_size(&self) -> u64 {
        self.data_size()
    }

    fn lookup(&self, key: &Key) -> Result<Option<Self::Blob>, Self::Error> {
        Ok(self.inner.get(key))
    }

    fn contains(&self, key: &Key) -> Result<bool, Self::Error> {
        Ok(self.inner.contains_key(key))
    }
}
