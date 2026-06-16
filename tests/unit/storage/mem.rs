//! In-memory storage tests.

use stored::key::{Key, key};
use stored::storage::{AddError, Storage, mem};

use heph_rt::spawn::options::FutureOptions;
use heph_rt::test::spawn_future;

use crate::util::block_on;

const BLOB: &[u8] = b"Hello world";
const KEY: Key = key!(
    "b7f783baed8297f0db917462184ff4f08e69c2d5e5f79a942600f9725f58ce1f29c18139bf80b06c0fff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47"
);

#[test]
fn add_blob() {
    let (handle, writer_future) = mem::new();
    spawn_future(writer_future, FutureOptions::default());
    let mut storage = mem::Storage::from(handle);

    assert_eq!(storage.len(), 0);
    assert_eq!(block_on(storage.lookup(KEY)).unwrap(), None);
    assert!(!block_on(storage.contains(KEY)).unwrap());

    assert_eq!(block_on(storage.add_blob(BLOB)).unwrap(), KEY);

    assert_eq!(storage.len(), 1);
    assert_eq!(
        block_on(storage.lookup(KEY)).unwrap().as_deref(),
        Some(BLOB)
    );
    assert!(block_on(storage.contains(KEY)).unwrap());
}

#[test]
fn add_blob_already_stored() {
    let (handle, writer_future) = mem::new();
    spawn_future(writer_future, FutureOptions::default());
    let mut storage = mem::Storage::from(handle);

    assert_eq!(block_on(storage.add_blob(BLOB)).unwrap(), KEY);
    assert!(matches!(
        block_on(storage.add_blob(BLOB)).unwrap_err(),
        AddError::AlreadyStored(KEY)
    ));

    assert_eq!(storage.len(), 1);
    assert_eq!(
        block_on(storage.lookup(KEY)).unwrap().as_deref(),
        Some(BLOB)
    );
    assert!(block_on(storage.contains(KEY)).unwrap());
}

#[test]
fn remove_blob() {
    let (handle, writer_future) = mem::new();
    spawn_future(writer_future, FutureOptions::default());
    let mut storage = mem::Storage::from(handle);

    assert_eq!(block_on(storage.add_blob(BLOB)).unwrap(), KEY);
    assert_eq!(block_on(storage.remove_blob(KEY)), Ok(true));

    assert_eq!(storage.len(), 0);
    assert_eq!(block_on(storage.lookup(KEY)).unwrap().as_deref(), None);
    assert!(!block_on(storage.contains(KEY)).unwrap());
}

#[test]
fn remove_blob_not_stored() {
    let (handle, writer_future) = mem::new();
    spawn_future(writer_future, FutureOptions::default());
    let mut storage = mem::Storage::from(handle);

    assert_eq!(block_on(storage.remove_blob(KEY)), Ok(false));

    assert_eq!(storage.len(), 0);
    assert_eq!(block_on(storage.lookup(KEY)).unwrap().as_deref(), None);
    assert!(!block_on(storage.contains(KEY)).unwrap());
}
