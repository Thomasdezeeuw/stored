//! Storage index tests.

use stored::key::{key, Key};
use stored::storage::index;

use heph_rt::test::block_on_future;

use crate::util::block_on;

const BLOB: &[u8] = b"Hello world";
const KEY: Key = key!("b7f783baed8297f0db917462184ff4f08e69c2d5e5f79a942600f9725f58ce1f29c18139bf80b06c0fff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47");

#[test]
fn writer_add_blob() {
    let (mut writer, handle) = index::new();
    let index = index::Index::from(handle);

    // Should be empty when newly created.
    assert_eq!(index.len(), 0);
    assert!(index.entry(&KEY).is_none());
    assert_eq!(index.blob(&KEY), None);
    assert_eq!(index.contains(&KEY), false);
    assert!(!writer.contains(&KEY));

    // Add a new blob.
    assert!(writer.add_blob(KEY, BLOB));
    // Should not yet be visible to the index/readers.
    assert_eq!(index.len(), 0);
    assert!(index.entry(&KEY).is_none());
    assert_eq!(index.blob(&KEY), None);
    assert_eq!(index.contains(&KEY), false);
    // The writer does have access.
    assert!(writer.contains(&KEY));

    // Make the changes visible.
    block_on(writer.flush_changes());
    // Should be visible to the reader now.
    assert_eq!(index.len(), 1);
    let entry = index.entry(&KEY).unwrap();
    assert_eq!(entry.key, KEY);
    assert_eq!(entry.blob, BLOB);
    assert_eq!(index.blob(&KEY), Some(BLOB));
    assert_eq!(index.contains(&KEY), true);
}

#[test]
fn writer_add_blob_already_stored() {
    let (mut writer, handle) = index::new();
    let index = index::Index::from(handle);

    // Add a new blob.
    assert!(writer.add_blob(KEY, BLOB));

    // Make the changes visible.
    block_on(writer.flush_changes());

    // Add the same blob and flush again.
    assert!(!writer.add_blob(KEY, BLOB));
    block_on(writer.flush_changes());

    assert_eq!(index.len(), 1);
    let entry = index.entry(&KEY).unwrap();
    assert_eq!(entry.key, KEY);
    assert_eq!(entry.blob, BLOB);
    assert_eq!(index.blob(&KEY), Some(BLOB));
    assert_eq!(index.contains(&KEY), true);
}

#[test]
fn writer_add_blob_already_stored_no_flush() {
    let (mut writer, handle) = index::new();
    let index = index::Index::from(handle);

    // Add a new blob.
    assert!(writer.add_blob(KEY, BLOB));
    // Add the same blob and then flush.
    assert!(!writer.add_blob(KEY, BLOB));
    block_on(writer.flush_changes());

    assert_eq!(index.len(), 1);
    let entry = index.entry(&KEY).unwrap();
    assert_eq!(entry.key, KEY);
    assert_eq!(entry.blob, BLOB);
    assert_eq!(index.blob(&KEY), Some(BLOB));
    assert_eq!(index.contains(&KEY), true);
}

#[test]
fn writer_remove_blob() {
    let (mut writer, handle) = index::new();
    let index = index::Index::from(handle);

    // Add a new blob.
    assert!(writer.add_blob(KEY, BLOB));

    // Make the changes visible.
    block_on(writer.flush_changes());
    // Should not yet be visible to the index/readers.
    assert_eq!(index.len(), 1);
    let entry = index.entry(&KEY).unwrap();
    assert_eq!(entry.key, KEY);
    assert_eq!(entry.blob, BLOB);
    assert_eq!(index.blob(&KEY), Some(BLOB));
    assert_eq!(index.contains(&KEY), true);

    // Remove the blob.
    assert!(writer.remove_blob(&KEY));

    // Make the changes visible.
    block_on(writer.flush_changes());
    // Should be empty again.
    assert_eq!(index.len(), 0);
    assert!(index.entry(&KEY).is_none());
    assert_eq!(index.blob(&KEY), None);
    assert_eq!(index.contains(&KEY), false);
}

#[test]
fn writer_remove_blob_no_flusr() {
    let (mut writer, handle) = index::new();
    let index = index::Index::from(handle);

    // Add a new blob.
    assert!(writer.add_blob(KEY, BLOB));

    // Remove the blob without flushing in between.
    assert!(writer.remove_blob(&KEY));

    // Make the changes visible.
    block_on(writer.flush_changes());
    // Should be empty again.
    assert_eq!(index.len(), 0);
    assert!(index.entry(&KEY).is_none());
    assert_eq!(index.blob(&KEY), None);
    assert_eq!(index.contains(&KEY), false);
}

#[test]
fn writer_remove_blob_no_stored() {
    let (mut writer, handle) = index::new();
    let index = index::Index::<&[u8]>::from(handle);

    // Remove a blob that is not stored.
    assert!(!writer.remove_blob(&KEY));

    // This should be a no-op.
    block_on(writer.flush_changes());
    // Should be empty again.
    assert_eq!(index.len(), 0);
    assert!(index.entry(&KEY).is_none());
    assert_eq!(index.blob(&KEY), None);
    assert_eq!(index.contains(&KEY), false);
}

#[test]
fn snapshot_isolation() {
    let (mut writer, handle) = index::new();
    let index = index::Index::from(handle);

    // Add a new blob.
    assert!(writer.add_blob(KEY, BLOB));
    block_on(writer.flush_changes());
    assert_eq!(index.contains(&KEY), true);

    let snapshot = index.snapshot();

    // Remove the blob.
    assert!(writer.remove_blob(&KEY));
    block_on(writer.flush_changes());
    assert_eq!(index.contains(&KEY), false);

    // Snapshot sould still contain the blob.
    assert_eq!(snapshot.len(), 1);
    let entry = snapshot.entry(&KEY).unwrap();
    assert_eq!(entry.key, KEY);
    assert_eq!(entry.blob, BLOB);
    assert_eq!(snapshot.blob(&KEY), Some(&BLOB));
    assert_eq!(snapshot.contains(&KEY), true);
}
