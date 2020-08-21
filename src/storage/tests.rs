use std::env;
use std::fs::{create_dir_all, remove_dir_all, remove_file};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::SystemTime;

use lazy_static::lazy_static;

use crate::storage::{
    AddResult, Blob, BlobEntry, DateTime, Entry, RemoveBlob, RemoveResult, StoreBlob, DATA_MAGIC,
    INDEX_MAGIC,
};
use crate::Key;

impl BlobEntry {
    fn unwrap(self) -> Blob {
        match self {
            BlobEntry::Stored(blob) => blob,
            BlobEntry::Removed(_) => panic!("unwrapped a removed BlobEntry"),
        }
    }

    fn unwrap_removed(self) -> SystemTime {
        match self {
            BlobEntry::Stored(_) => panic!("expected BlobEntry to be removed"),
            BlobEntry::Removed(time) => time,
        }
    }

    fn is_removed(&self) -> bool {
        match self {
            BlobEntry::Stored(_) => false,
            BlobEntry::Removed(_) => true,
        }
    }
}

impl AddResult {
    fn unwrap(self) -> StoreBlob {
        match self {
            AddResult::Ok(query) => query,
            AddResult::AlreadyStored(key) => panic!("blob already stored: {}", key),
            AddResult::Err(err) => panic!("unexpected I/O error: {}", err),
        }
    }
}

impl RemoveResult {
    fn unwrap(self) -> RemoveBlob {
        match self {
            RemoveResult::Ok(query) => query,
            RemoveResult::NotStored(_) => panic!("blob not stored"),
        }
    }
}

#[test]
fn magic_headers() {
    assert_eq!(DATA_MAGIC.len(), 16);
    assert_eq!(INDEX_MAGIC.len(), 16);
}

/// Returns the path to the test data `file`.
fn test_data_path(file: &str) -> PathBuf {
    Path::new("./tests/data/").join(file)
}

// Database locks.
lazy_static! {
    static ref DB_001: Mutex<()> = Mutex::new(());
    static ref DB_008: Mutex<()> = Mutex::new(());
    static ref DB_009: Mutex<()> = Mutex::new(());
}

// Data stored in "001.db" test data.
const DATA: [&[u8]; 2] = [b"Hello world", b"Hello mars"];

// Entries in "001.db" test data.
fn test_entries() -> [Entry; 2] {
    // `Key::for_blob` isn't const, otherwise this could be a constant.
    [
        Entry {
            key: Key::for_blob(DATA[0]),
            offset: (DATA_MAGIC.len() as u64).to_be(),
            length: (DATA[0].len() as u32).to_be(),
            time: DateTime {
                seconds: 5u64.to_be(),
                subsec_nanos: 0u32.to_be(),
            },
        },
        Entry {
            key: Key::for_blob(DATA[1]),
            offset: ((DATA_MAGIC.len() + DATA[0].len()) as u64).to_be(),
            length: (DATA[1].len() as u32).to_be(),
            time: DateTime {
                seconds: 50u64.to_be(),
                subsec_nanos: 100u32.to_be(),
            },
        },
    ]
}

struct TempFile {
    path: PathBuf,
}

impl AsRef<Path> for TempFile {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = remove_file(&self.path);
    }
}

fn temp_file(name: &str) -> TempFile {
    let mut path = stored_temp_dir();
    path.push(name);
    // Remove the old database from previous tests.
    let _ = remove_file(&path);
    TempFile { path }
}

struct TempDir {
    path: PathBuf,
}

impl AsRef<Path> for TempDir {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl Deref for TempDir {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = remove_dir_all(&self.path);
    }
}

fn temp_dir(name: &str) -> TempDir {
    let mut path = stored_temp_dir();
    path.push(name);
    // Remove the old database from previous tests.
    let _ = remove_dir_all(&path);
    TempDir { path }
}

fn stored_temp_dir() -> PathBuf {
    let mut path = env::temp_dir();
    path.push("stored");
    if let Err(err) = create_dir_all(&path) {
        panic!(
            "failed to create temporary directory ('{}'): {}",
            path.display(),
            err
        );
    } else {
        path
    }
}

mod date_time {
    use std::mem::size_of;
    use std::ptr;
    use std::time::{Duration, SystemTime};

    use crate::storage::{DateTime, ModifiedTime};

    #[test]
    fn bits_valid() {
        const NANOS_PER_SEC: u32 = 1_000_000_000;
        assert!(NANOS_PER_SEC & DateTime::REMOVED_BIT == 0);
        assert!(NANOS_PER_SEC & DateTime::INVALID_BIT == 0);
    }

    #[test]
    fn is_and_mark_removed() {
        let tests = [
            (
                DateTime {
                    seconds: 1u64.to_be(),
                    subsec_nanos: 0u32.to_be(),
                },
                false,
            ),
            (
                DateTime {
                    seconds: (u64::MAX / 2).to_be(),
                    subsec_nanos: 0u32.to_be(),
                },
                false,
            ),
            (
                DateTime {
                    seconds: 0u64.to_be(),
                    subsec_nanos: (1000000 | DateTime::REMOVED_BIT).to_be(),
                },
                true,
            ),
        ];

        for (input, is_removed) in &tests {
            assert_eq!(input.is_removed(), *is_removed, "input: {:?}", input);
            let removed = input.mark_removed();
            assert_eq!(removed.is_removed(), true, "input: {:?}", removed);
        }
    }

    #[test]
    fn is_valid() {
        let tests = [
            (
                DateTime {
                    seconds: 1u64.to_be(),
                    subsec_nanos: 0u32.to_be(),
                },
                false,
            ),
            (
                DateTime {
                    seconds: (u64::MAX / 2).to_be(),
                    subsec_nanos: 0u32.to_be(),
                },
                false,
            ),
            (
                DateTime {
                    seconds: 0u64.to_be(),
                    subsec_nanos: (1000000 | DateTime::REMOVED_BIT).to_be(),
                },
                false,
            ),
            (DateTime::INVALID, true),
            (
                DateTime {
                    seconds: 0u64.to_be(),
                    subsec_nanos: u32::max_value(),
                },
                true,
            ),
        ];

        for (input, is_invalid) in &tests {
            assert_eq!(input.is_invalid(), *is_invalid, "input: {:?}", input);
        }
    }

    #[test]
    fn from_sys_time() {
        let tests = [
            (
                Duration::from_secs(1),
                DateTime {
                    seconds: 1u64.to_be(),
                    subsec_nanos: 0u32.to_be(),
                },
            ),
            (
                Duration::from_secs(u64::MAX / 2),
                DateTime {
                    seconds: (u64::MAX / 2).to_be(),
                    subsec_nanos: 0u32.to_be(),
                },
            ),
            (
                Duration::from_millis(1),
                DateTime {
                    seconds: 0u64.to_be(),
                    subsec_nanos: 1000000u32.to_be(),
                },
            ),
        ];

        for (add, want) in &tests {
            let time = SystemTime::UNIX_EPOCH + *add;
            let got: DateTime = time.into();
            assert_eq!(got, *want);
        }
    }

    #[test]
    fn from_and_into_modified_time() {
        let tests = [
            (
                DateTime {
                    seconds: 1u64.to_be(),
                    subsec_nanos: 0u32.to_be(),
                },
                false,
                false,
                Duration::from_secs(1),
            ),
            (
                DateTime {
                    seconds: (u64::MAX / 2).to_be(),
                    subsec_nanos: 0u32.to_be(),
                },
                false,
                false,
                Duration::from_secs(u64::MAX / 2),
            ),
            (
                DateTime {
                    seconds: 0u64.to_be(),
                    subsec_nanos: (1000000 | DateTime::REMOVED_BIT).to_be(),
                },
                true,
                false,
                Duration::from_millis(1),
            ),
            (DateTime::INVALID, false, true, Duration::from_millis(0)),
        ];

        for (input, is_removed, is_invalid, add) in &tests {
            let got: ModifiedTime = (*input).into();
            let want = SystemTime::UNIX_EPOCH + *add;
            if *is_removed {
                assert!(input.is_removed());
                assert_eq!(got, ModifiedTime::Removed(want), "input: {:?}", input);
            } else if *is_invalid {
                assert!(input.is_invalid());
                assert_eq!(got, ModifiedTime::Invalid, "input: {:?}", input);
            } else {
                assert_eq!(got, ModifiedTime::Created(want), "input: {:?}", input);
            }

            let round_trip: DateTime = got.into();
            assert_eq!(*input, round_trip, "input: {:?}", input);
        }
    }

    #[test]
    fn from_bytes() {
        let tests = &[
            DateTime::from(SystemTime::now()),
            DateTime::from(SystemTime::now()).mark_removed(),
            DateTime::from(SystemTime::UNIX_EPOCH),
            DateTime::from(SystemTime::UNIX_EPOCH).mark_removed(),
            DateTime::INVALID,
        ];

        fn copy(dst: &mut [u8], src: &DateTime) {
            assert!(dst.len() >= size_of::<DateTime>());
            let src: *const u8 = src as *const _ as *const _;
            unsafe { ptr::copy_nonoverlapping(src, dst.as_mut_ptr(), size_of::<DateTime>()) }
        }

        for time in tests {
            let mut buf = [0; size_of::<DateTime>()];
            copy(&mut buf, &time);

            // `DateTime` format should be they same when reading from disk (or
            // a in-memory buffer).
            let got = DateTime::from_bytes(&buf).unwrap();
            assert_eq!(*time, got);

            buf.copy_from_slice(time.as_bytes());
            let got = DateTime::from_bytes(&buf).unwrap();
            assert_eq!(*time, got);
        }
    }
}

mod index {
    use std::io::{Seek, SeekFrom};
    use std::mem::size_of;
    use std::{fs, io};

    use crate::storage::{DateTime, Entry, Index, Key, ModifiedTime, DATA_MAGIC, INDEX_MAGIC};

    use super::{temp_file, test_data_path, test_entries, DATA, DB_001};

    #[test]
    fn entry_size() {
        // Size and layout is fixed.
        assert_eq!(size_of::<Entry>(), 88);
    }

    #[test]
    fn new_entry() {
        for entry in test_entries().iter() {
            let created_at = if let ModifiedTime::Created(t) = entry.modified_time() {
                t
            } else {
                panic!("expected only created at entries");
            };
            let got = Entry::new(
                entry.key().clone(),
                entry.offset(),
                entry.length(),
                created_at,
            );
            assert_eq!(got, *entry);
        }
    }

    #[test]
    fn crate_empty_index_file() {
        let path = temp_file("empty_index.index");
        fs::write(&path, INDEX_MAGIC).unwrap();

        let mut index = Index::open(&path).unwrap();

        let entries = index.entries().unwrap();
        assert_eq!(entries.len(), 0);
        let mut entries = entries.iter();
        assert!(entries.next().is_none());
    }

    #[test]
    fn existing_empty_index_file() {
        let path = test_data_path("011.db/index");
        fs::write(&path, INDEX_MAGIC).unwrap();

        let mut index = Index::open(&path).unwrap();

        let entries = index.entries().unwrap();
        assert_eq!(entries.len(), 0);
        let mut entries = entries.iter();
        assert!(entries.next().is_none());
    }

    #[test]
    fn entries() {
        let path = test_data_path("001.db/index");
        let _guard = DB_001.lock().unwrap();

        let mut index = Index::open(&path).unwrap();

        let entries = index.entries().unwrap();
        assert_eq!(entries.len(), 2);
        let wanted = test_entries();
        for (i, entry) in entries.iter() {
            assert_eq!(entry, &wanted[i.0]);
        }
    }

    #[test]
    fn create_index() {
        let path = temp_file("create_index.index");
        let mut index = Index::open(&path).unwrap();

        // Magic header should be written.
        let got = fs::read(&path).unwrap();
        assert_eq!(got, INDEX_MAGIC);

        // Should be empty.
        {
            let entries = index.entries().unwrap();
            let mut entries = entries.iter();
            assert!(entries.next().is_none());
        }

        // Add some entries.
        let test_entries = test_entries();
        for entry in &test_entries {
            index.add_entry(entry).unwrap();
        }

        // To be sure start reading from the start of the file.
        index.file.seek(SeekFrom::Start(0)).unwrap();

        // Check the entries we've just added.
        let entries = index.entries().unwrap();
        assert_eq!(entries.len(), 2);
        for (i, entry) in entries.iter() {
            assert_eq!(entry, &test_entries[i.0]);
        }
    }

    #[test]
    fn missing_magic() {
        let path = test_data_path("002.db/index");
        match Index::open(&path) {
            Ok(_) => panic!("expected to fail opening index file"),
            Err(err) => {
                assert_eq!(err.kind(), io::ErrorKind::InvalidData);
                assert!(err
                    .to_string()
                    .contains("missing magic header in index file"));
            }
        }
    }

    #[test]
    fn incorrect_length() {
        let path = test_data_path("003.db/index");
        match Index::open(&path) {
            Ok(_) => panic!("expected to fail to access index entries"),
            Err(err) => {
                assert_eq!(err.kind(), io::ErrorKind::InvalidData);
                assert!(err.to_string().contains("incorrect index file size"));
            }
        }
    }

    #[test]
    fn invalid_index_entries() {
        let path = test_data_path("010.db/index");
        let mut index = Index::open(&path).unwrap();
        let entries = index.entries().unwrap();
        assert_eq!(entries.len(), 2);
        let entries = entries.as_slice();

        let want1 = Entry {
            key: Key::for_blob(DATA[0]),
            offset: (DATA_MAGIC.len() as u64).to_be(),
            length: (DATA[0].len() as u32).to_be(),
            time: DateTime::INVALID,
        };
        assert_eq!(entries[0], want1);
        assert!(entries[0].time.is_invalid());
        assert!(!entries[0].time.is_removed());

        let want2 = Entry {
            key: Key::for_blob(DATA[0]),
            offset: ((DATA_MAGIC.len() + DATA[0].len()) as u64).to_be(),
            length: (DATA[0].len() as u32).to_be(),
            time: DateTime {
                seconds: 1587374820u64.to_be(),
                subsec_nanos: 177578000u32.to_be(),
            },
        };
        assert_eq!(entries[1], want2);
        assert!(!entries[1].time.is_invalid());
        assert!(!entries[1].time.is_removed());
    }
}

mod data {
    use std::ptr::NonNull;
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use std::{fs, io, thread};

    use crate::storage::{
        is_page_aligned, mmap, munmap, next_page_aligned, Data, MmapAreaControl, DATA_MAGIC,
        DATA_PRE_ALLOC_BYTES,
    };

    use super::{temp_file, test_data_path, test_entries, DATA, DB_001};

    #[test]
    fn create_data_file() {
        let path = temp_file("empty_data.data");
        fs::write(&path, DATA_MAGIC).unwrap();

        let data = Data::open(&path).unwrap();
        assert_eq!(data.used_bytes, DATA_MAGIC.len() as u64);

        assert_eq!(data.areas.len(), 1);
        let area = data.areas[0].area();
        assert_eq!(area.mmap_length, DATA_PRE_ALLOC_BYTES);
        assert_eq!(area.mmap_offset, 0);
        assert_eq!(area.ref_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn existing_empty_data_file() {
        let path = test_data_path("011.db/data");
        fs::write(&path, DATA_MAGIC).unwrap();

        let data = Data::open(&path).unwrap();
        assert_eq!(data.used_bytes, DATA_MAGIC.len() as u64);

        assert_eq!(data.areas.len(), 1);
        let area = data.areas[0].area();
        assert_eq!(area.mmap_length, DATA_PRE_ALLOC_BYTES);
        assert_eq!(area.mmap_offset, 0);
        assert_eq!(area.ref_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn open_data_file() {
        let path = test_data_path("001.db/data");
        let _guard = DB_001.lock().unwrap();
        let data = Data::open(&path).unwrap();

        assert_eq!(
            data.used_bytes(),
            (DATA.iter().map(|d| d.len()).sum::<usize>() + DATA_MAGIC.len()) as u64
        );
        assert_eq!(data.areas.len(), 1);
        let area = data.areas[0].area();
        assert_eq!(area.mmap_offset, 0);
        assert_eq!(area.mmap_length, DATA_PRE_ALLOC_BYTES);

        for (i, entry) in test_entries().iter().enumerate() {
            let blob = data.slice(entry.offset(), entry.length() as usize).unwrap();
            assert_eq!(blob.as_slice(), DATA[i]);
        }
    }

    #[test]
    fn add_blob_grow_in_area() {
        let path = temp_file("add_blob_grow_in_area.data");
        let mut data = Data::open(&path).unwrap();
        assert_eq!(data.areas.len(), 1);
        assert_eq!(data.used_bytes(), DATA_MAGIC.len() as u64);

        // Adding a first blob should create a new area.
        let (blob1, offset) = data.add_blob(DATA[0]).unwrap();
        assert_eq!(offset, DATA_MAGIC.len() as u64);
        assert_eq!(blob1.as_slice(), DATA[0]);

        assert_eq!(data.used_bytes(), (DATA[0].len() + DATA_MAGIC.len()) as u64);
        assert_eq!(data.areas.len(), 1);
        let mmap_area = data.areas[0].area();
        assert_eq!(mmap_area.mmap_offset, 0);
        assert_eq!(mmap_area.mmap_length, DATA_PRE_ALLOC_BYTES);

        // Adding a second blob should fit in the existing area.
        let (blob2, offset) = data.add_blob(DATA[1]).unwrap();
        // Should be placed after the first blob.
        assert_eq!(
            unsafe { blob1.address.as_ptr().add(DATA[0].len()) },
            blob2.address.as_ptr()
        );
        assert_eq!(offset, (DATA[0].len() + DATA_MAGIC.len()) as u64);
        assert_eq!(blob2.as_slice(), DATA[1]);

        assert_eq!(
            data.used_bytes(),
            (DATA.iter().map(|d| d.len()).sum::<usize>() + DATA_MAGIC.len()) as u64
        );
        assert_eq!(data.areas.len(), 1);
        let mmap_area = data.areas[0].area();
        assert_eq!(mmap_area.mmap_offset, 0);
        assert_eq!(mmap_area.mmap_length, DATA_PRE_ALLOC_BYTES);

        drop(data);
        // Blobs must still be valid.
        assert_eq!(blob1.as_slice(), DATA[0]);
        assert_eq!(blob2.as_slice(), DATA[1]);
    }

    #[test]
    #[ignore = "test is flaky due to the required memory placement"]
    fn add_blob_grow_area() {
        let path = temp_file("add_blob_grow_area.data");
        let mut data = Data::open(&path).unwrap();
        assert_eq!(data.areas.len(), 1);
        assert_eq!(data.used_bytes(), DATA_MAGIC.len() as u64);

        // Adding a first blob should create a new area.
        let (blob1, offset) = data.add_blob(DATA[0]).unwrap();
        assert_eq!(offset, DATA_MAGIC.len() as u64);
        assert_eq!(blob1.as_slice(), DATA[0]);

        assert_eq!(data.used_bytes(), (DATA[0].len() + DATA_MAGIC.len()) as u64);
        assert_eq!(data.areas.len(), 1);
        let mmap_area = data.areas[0].area();
        assert_eq!(mmap_area.mmap_offset, 0);
        assert_eq!(mmap_area.mmap_length, DATA_PRE_ALLOC_BYTES);

        // Force the are to grow.
        const LARGE_BLOB: &[u8] = &[5; DATA_PRE_ALLOC_BYTES];
        let (blob2, offset) = data.add_blob(LARGE_BLOB).unwrap();
        // Should be placed after the first blob.
        assert_eq!(
            unsafe { blob1.address.as_ptr().add(DATA[0].len()) },
            blob2.address.as_ptr()
        );
        assert_eq!(offset, (DATA[0].len() + DATA_MAGIC.len()) as u64);
        assert_eq!(blob2.as_slice(), LARGE_BLOB);

        assert_eq!(
            data.used_bytes(),
            (DATA_MAGIC.len() + DATA[0].len() + LARGE_BLOB.len()) as u64
        );
        assert_eq!(data.areas.len(), 1);
        let mmap_area = data.areas[0].area();
        assert_eq!(mmap_area.mmap_offset, 0);
        assert_eq!(mmap_area.mmap_length, 3 * DATA_PRE_ALLOC_BYTES);

        drop(data);
        // Blobs must still be valid.
        assert_eq!(blob1.as_slice(), DATA[0]);
        assert_eq!(blob2.as_slice(), LARGE_BLOB);
    }

    /// Length used in `create_dummy_area_after` to create dummy `mmap` areas.
    const DUMMY_LENGTH: usize = 100;

    fn create_dummy_area_after(areas: &[MmapAreaControl]) -> NonNull<libc::c_void> {
        let area = areas.last().unwrap().area();
        let end_address = area.mmap_address.as_ptr() as usize + area.mmap_length;
        let want_dummy_address = if is_page_aligned(end_address) {
            end_address as *mut libc::c_void
        } else {
            next_page_aligned(end_address) as *mut libc::c_void
        };

        for _ in 0..5 {
            let dummy_address = mmap(
                want_dummy_address,
                DUMMY_LENGTH,
                libc::PROT_READ,
                libc::MAP_PRIVATE | libc::MAP_ANON,
                0,
                0,
            )
            .unwrap();

            if dummy_address.as_ptr() == want_dummy_address {
                // Success!
                return dummy_address;
            } else {
                // Wrong page, try again.
                munmap(dummy_address.as_ptr(), DUMMY_LENGTH).unwrap();
                thread::sleep(Duration::from_millis(50));
            }
        }

        panic!("OS didn't give us the memory we need for this test. NOT A TEST FAILURE");
    }

    #[test]
    #[ignore = "test is flaky due to the required memory placement"]
    fn add_blob_new_area() {
        let path = temp_file("add_blob_new_area.data");
        let mut data = Data::open(&path).unwrap();
        assert_eq!(data.areas.len(), 1);
        assert_eq!(data.used_bytes(), DATA_MAGIC.len() as u64);

        // Adding a first blob should create a new area.
        let (blob1, offset) = data.add_blob(DATA[0]).unwrap();
        assert_eq!(offset, DATA_MAGIC.len() as u64);
        assert_eq!(blob1.as_slice(), DATA[0]);

        assert_eq!(data.used_bytes(), (DATA[0].len() + DATA_MAGIC.len()) as u64);
        assert_eq!(data.areas.len(), 1);
        let mmap_area = data.areas[0].area();
        assert_eq!(mmap_area.mmap_offset, 0);
        assert_eq!(mmap_area.mmap_length, DATA_PRE_ALLOC_BYTES);

        let dummy_address = create_dummy_area_after(&data.areas);

        // Force the are to grow.
        const LARGE_BLOB: &[u8] = &[8; DATA_PRE_ALLOC_BYTES];
        let (blob2, offset) = data.add_blob(LARGE_BLOB).unwrap();
        // Should be placed after the first blob.
        assert_ne!(
            unsafe { blob1.address.as_ptr().add(DATA[0].len()) },
            blob2.address.as_ptr()
        );
        assert_eq!(offset, (DATA[0].len() + DATA_MAGIC.len()) as u64);
        assert_eq!(blob2.as_slice(), LARGE_BLOB);

        assert_eq!(
            data.used_bytes(),
            (DATA_PRE_ALLOC_BYTES + LARGE_BLOB.len()) as u64
        );
        assert_eq!(data.areas.len(), 2);
        let mmap_area1 = data.areas[0].area();
        assert_eq!(mmap_area1.mmap_offset, 0);
        assert_eq!(mmap_area1.mmap_length, DATA_PRE_ALLOC_BYTES);
        let mmap_area2 = data.areas[1].area();
        assert_eq!(mmap_area2.mmap_offset, DATA_PRE_ALLOC_BYTES as libc::off_t);
        assert_eq!(mmap_area2.mmap_length, 2 * DATA_PRE_ALLOC_BYTES);

        drop(data);
        // Blobs must still be valid.
        assert_eq!(blob1.as_slice(), DATA[0]);
        assert_eq!(blob2.as_slice(), LARGE_BLOB);
        munmap(dummy_address.as_ptr(), DUMMY_LENGTH).unwrap();
    }

    #[test]
    fn data_file_shrinks_on_drop() {
        let path = temp_file("data_file_shrinks_on_drop.data");
        let mut data = Data::open(&path).unwrap();
        assert_eq!(data.areas.len(), 1);
        assert_eq!(data.used_bytes(), DATA_MAGIC.len() as u64);

        // Adding a first blob should create a new area.
        let _ = data.add_blob(DATA[0]).unwrap();
        let _ = data.add_blob(DATA[1]).unwrap();
        let want_size = (DATA_MAGIC.len() + DATA[0].len() + DATA[1].len()) as u64;
        assert_eq!(data.used_bytes(), want_size);

        // While open it allocate additional space.
        let metadata = fs::metadata(&path).unwrap();
        assert_eq!(metadata.len(), DATA_PRE_ALLOC_BYTES as u64);

        drop(data);
        // Data file must shrink into to the correct size.
        let metadata = fs::metadata(&path).unwrap();
        assert_eq!(metadata.len(), want_size);
    }

    #[test]
    fn missing_magic() {
        let path = test_data_path("002.db/data");
        match Data::open(&path) {
            Ok(_) => panic!("expected to fail opening data file"),
            Err(err) => {
                assert_eq!(err.kind(), io::ErrorKind::InvalidData);
                assert!(err
                    .to_string()
                    .contains("missing magic header in data file"));
            }
        }
    }
}

mod storage {
    use std::mem::size_of;
    use std::time::{Duration, SystemTime};
    use std::{fs, io};

    use crate::storage::{
        AddResult, Blob, BlobEntry, Entry, EntryIndex, ModifiedTime, Query, RemoveResult, Storage,
        DATA_MAGIC, INDEX_MAGIC,
    };
    use crate::Key;

    use super::{temp_dir, test_data_path, test_entries, DATA, DB_001, DB_009};

    #[test]
    fn blob_size() {
        // TODO: SystemTime has a different size on different OSes, this will
        // fail somewhere.
        assert_eq!(size_of::<Blob>(), 40);
    }

    #[test]
    fn open_database() {
        let path = test_data_path("001.db");
        let _guard = DB_001.lock().unwrap();
        let storage = Storage::open(&path).unwrap();

        let want_data_length = DATA_MAGIC.len() as u64
            + test_entries()
                .iter()
                .map(|e| e.length() as u64)
                .sum::<u64>();
        let want_index_length =
            (INDEX_MAGIC.len() + test_entries().len() * size_of::<Entry>()) as u64;

        assert_eq!(storage.len(), DATA.len());
        assert_eq!(storage.data_size(), want_data_length);
        assert_eq!(storage.index_size(), want_index_length);
        assert_eq!(storage.total_size(), want_data_length + want_index_length);

        for (i, entry) in test_entries().iter().enumerate() {
            assert!(storage.contains(entry.key()));
            let got = storage.lookup(entry.key()).unwrap().unwrap();
            let want = DATA[i];
            assert_eq!(got.bytes(), want);
            assert_eq!(
                ModifiedTime::Created(got.created_at()),
                entry.modified_time()
            );
        }

        drop(storage);

        let data_metadata = fs::metadata(path.join("data")).unwrap();
        assert_eq!(data_metadata.len(), want_data_length);
        let index_metadata = fs::metadata(path.join("index")).unwrap();
        assert_eq!(index_metadata.len(), want_index_length);
    }

    #[test]
    fn add_multiple_blobs() {
        let path = temp_dir("add_multiple_blobs.db");
        let mut storage = Storage::open(&path).unwrap();

        assert_eq!(storage.len(), 0);
        assert_eq!(storage.data_size(), DATA_MAGIC.len() as u64);
        assert_eq!(storage.index_size(), INDEX_MAGIC.len() as u64);
        assert_eq!(
            storage.total_size(),
            (DATA_MAGIC.len() + INDEX_MAGIC.len()) as u64
        );

        let blobs = [DATA[0], DATA[1], &[1; 100], &[2; 200], b"Store my data!"];

        let mut keys = Vec::with_capacity(blobs.len());
        for blob in blobs.iter().copied() {
            let query = storage.add_blob(blob).unwrap();
            let key = query.key().clone();
            storage.commit(query, SystemTime::now()).unwrap();

            let got = storage.lookup(&key).unwrap().unwrap();
            assert_eq!(got.bytes(), blob);
            // TODO: check created_at data? SystemTime is not monotonic so the
            // test will be flaky.
            keys.push(key);
        }

        for (i, key) in keys.into_iter().enumerate() {
            let got = storage.lookup(&key).unwrap().unwrap();
            assert_eq!(got.bytes(), blobs[i]);

            // Check the EntryIndex.
            let (entry_index, blob_entry) = storage.blobs.get(&key).unwrap();
            assert_eq!(*entry_index, EntryIndex(i));
            match blob_entry {
                BlobEntry::Stored(blob) => assert_eq!(blob.bytes(), blobs[i]),
                BlobEntry::Removed(_) => panic!("unexpected blob entry"),
            }
        }

        assert_eq!(storage.len(), blobs.len());
        let want_data_size =
            (blobs.iter().map(|b| b.len()).sum::<usize>() + DATA_MAGIC.len()) as u64;
        assert_eq!(storage.data_size(), want_data_size);
        let want_index_size = (blobs.len() * size_of::<Entry>()) as u64 + INDEX_MAGIC.len() as u64;
        assert_eq!(storage.index_size(), want_index_size);
        assert_eq!(storage.total_size(), want_index_size + want_data_size);

        drop(storage);
        let data_metadata = fs::metadata(path.join("data")).unwrap();
        assert_eq!(data_metadata.len(), want_data_size);
        let index_metadata = fs::metadata(path.join("index")).unwrap();
        assert_eq!(index_metadata.len(), want_index_size);
    }

    #[test]
    fn add_same_blob_twice() {
        let path = temp_dir("add_same_blob_twice.db");
        let mut storage = Storage::open(&path).unwrap();

        assert_eq!(storage.len(), 0);
        assert_eq!(storage.data_size(), DATA_MAGIC.len() as u64);
        assert_eq!(storage.index_size(), INDEX_MAGIC.len() as u64);
        assert_eq!(
            storage.total_size(),
            (DATA_MAGIC.len() + INDEX_MAGIC.len()) as u64
        );

        let blob = DATA[0];
        let want_key = Key::for_blob(blob);

        // First time is normal procedure.
        let query = storage.add_blob(blob).unwrap();
        let key = query.key().clone();
        storage.commit(query, SystemTime::now()).unwrap();
        assert_eq!(key, want_key);

        // Second time it should already be present.
        match storage.add_blob(blob) {
            AddResult::AlreadyStored(got_key) => {
                assert_eq!(got_key, want_key);
            }
            AddResult::Ok(_) | AddResult::Err(_) => panic!("unexpected add_blob result"),
        }

        // `blobs` should be unchanged.
        let (entry_index, blob_entry) = storage.blobs.get(&key).unwrap();
        assert_eq!(*entry_index, EntryIndex(0));
        match blob_entry {
            BlobEntry::Stored(got) => assert_eq!(got.bytes(), blob),
            BlobEntry::Removed(_) => panic!("unexpected blob entry"),
        }
    }

    #[test]
    fn add_query_can_outlive_storage() {
        let path = temp_dir("add_query_can_outlive_storage.db");
        let mut storage = Storage::open(&path).unwrap();

        let blob = DATA[0];
        let query = storage.add_blob(blob).unwrap();

        // Dropping the storage before the query should not panic.
        drop(storage);
        drop(query);
    }

    #[test]
    fn uncommitted_blobs() {
        let path = temp_dir("uncommitted_blobs.db");
        let mut storage = Storage::open(&path).unwrap();

        assert_eq!(storage.len(), 0);
        assert_eq!(storage.data_size(), DATA_MAGIC.len() as u64);
        assert_eq!(storage.index_size(), INDEX_MAGIC.len() as u64);
        assert_eq!(
            storage.total_size(),
            (DATA_MAGIC.len() + INDEX_MAGIC.len()) as u64
        );

        let blob = DATA[0];
        let key = Key::for_blob(blob);

        let query = storage.add_blob(blob).unwrap();

        // Blob shouldn't be accessible.
        assert_eq!(storage.len(), 0);
        assert!(storage.lookup(&key).is_none());
        // We can lookup the uncommitted blob.
        assert_eq!(storage.lookup_uncommitted(&key).unwrap().bytes(), blob);
        // But is should be stored.
        assert_eq!(storage.data_size(), (DATA_MAGIC.len() + blob.len()) as u64);

        // After committing the blob should be accessible.
        let got_key = query.key().clone();
        storage.commit(query, SystemTime::now()).unwrap();
        assert_eq!(got_key, key);
        assert_eq!(storage.lookup(&key).unwrap().unwrap().bytes(), blob);
        assert!(storage.lookup_uncommitted(&key).is_none());

        // `blobs` should be unchanged.
        let (entry_index, blob_entry) = storage.blobs.get(&key).unwrap();
        assert_eq!(*entry_index, EntryIndex(0));
        match blob_entry {
            BlobEntry::Stored(got) => assert_eq!(got.bytes(), blob),
            BlobEntry::Removed(_) => panic!("unexpected blob entry"),
        }
    }

    #[test]
    fn keys_separate_from_index() {
        let path = test_data_path("001.db");
        let _guard = DB_001.lock().unwrap();
        let mut storage = Storage::open(&path).unwrap();

        let keys = storage.keys().unwrap();
        let iter = keys.into_iter();
        assert_eq!(iter.len(), 2);
        for (got, want) in iter.zip(test_entries().iter()) {
            assert_eq!(got, &want.key);
        }
    }

    #[test]
    fn keys_can_outlive_storage() {
        let path = test_data_path("009.db");
        let _guard = DB_009.lock().unwrap();
        let mut storage = Storage::open(&path).unwrap();

        let keys = storage.keys().unwrap();

        // After dropping the index the entries should still be usable.
        drop(storage);

        let iter = keys.into_iter();
        assert_eq!(iter.len(), 2);
        let want = &[
            Key::for_blob(b"Hello world"),
            // Include keys for removed blobs.
            Key::for_blob(b"Hello mars"),
        ];
        for (got, want) in iter.zip(want.iter()) {
            assert_eq!(got, want);
        }
    }

    #[test]
    fn empty_keys() {
        let path = temp_dir("empty_keys.db");
        let mut storage = Storage::open(&path).unwrap();

        let keys = storage.keys().unwrap();
        let mut iter = keys.into_iter();
        assert_eq!(iter.len(), 0);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn concurrently_adding_blobs() {
        let path = temp_dir("concurrently_adding_blobs.db");
        let mut storage = Storage::open(&path).unwrap();

        assert_eq!(storage.len(), 0);
        assert_eq!(storage.data_size(), DATA_MAGIC.len() as u64);
        assert_eq!(storage.index_size(), INDEX_MAGIC.len() as u64);
        assert_eq!(
            storage.total_size(),
            (DATA_MAGIC.len() + INDEX_MAGIC.len()) as u64
        );

        let blob1 = DATA[0];
        let key1 = Key::for_blob(blob1);
        let blob2 = DATA[1];
        let key2 = Key::for_blob(blob2);

        let query1 = storage.add_blob(blob1).unwrap();

        // Blob shouldn't be accessible.
        assert_eq!(storage.len(), 0);
        assert!(storage.lookup(&key1).is_none());
        assert_eq!(storage.lookup_uncommitted(&key1).unwrap().bytes(), blob1);
        // But is should be stored.
        assert_eq!(storage.data_size(), (DATA_MAGIC.len() + blob1.len()) as u64);

        let query2 = storage.add_blob(blob2).unwrap();

        assert_eq!(storage.len(), 0);
        assert!(storage.lookup(&key2).is_none());
        assert_eq!(storage.lookup_uncommitted(&key2).unwrap().bytes(), blob2);
        assert_eq!(
            storage.data_size(),
            (DATA_MAGIC.len() + blob1.len() + blob2.len()) as u64
        );

        // We should be able to commit in any order.
        let got_key2 = query2.key().clone();
        storage.commit(query2, SystemTime::now()).unwrap();
        assert_eq!(got_key2, key2);
        assert_eq!(storage.lookup(&key2).unwrap().unwrap().bytes(), blob2);
        assert!(storage.lookup_uncommitted(&key2).is_none());

        // After committing the blob should be accessible.
        let got_key1 = query1.key().clone();
        storage.commit(query1, SystemTime::now()).unwrap();
        assert!(storage.lookup_uncommitted(&key1).is_none());
        assert_eq!(got_key1, key1);
        assert_eq!(storage.lookup(&key1).unwrap().unwrap().bytes(), blob1);
    }

    #[test]
    fn concurrently_adding_same_blob() {
        let path = temp_dir("concurrently_adding_same_blob.db");
        let mut storage = Storage::open(&path).unwrap();

        assert_eq!(storage.len(), 0);
        assert_eq!(storage.data_size(), DATA_MAGIC.len() as u64);
        assert_eq!(storage.index_size(), INDEX_MAGIC.len() as u64);
        assert_eq!(
            storage.total_size(),
            (DATA_MAGIC.len() + INDEX_MAGIC.len()) as u64
        );

        let blob = DATA[0];
        let key = Key::for_blob(blob);

        let query1 = storage.add_blob(blob).unwrap();
        let uncommitted_blob1 = storage.lookup_uncommitted(&key).unwrap();
        let query2 = storage.add_blob(blob).unwrap();
        let uncommitted_blob2 = storage.lookup_uncommitted(&key).unwrap();
        // The uncommitted blob should be reused.
        assert_eq!(uncommitted_blob1.offset, uncommitted_blob2.offset);
        assert_eq!(
            uncommitted_blob1.mmap_slice.address,
            uncommitted_blob2.mmap_slice.address
        );
        assert_eq!(
            uncommitted_blob1.mmap_slice.length,
            uncommitted_blob2.mmap_slice.length
        );
        assert_eq!(uncommitted_blob1.bytes(), blob);

        assert_eq!(storage.len(), 0);
        assert!(storage.lookup(&key).is_none());
        assert_eq!(
            storage.data_size(),
            // Blob should only be stored once.
            (DATA_MAGIC.len() + blob.len()) as u64
        );

        // We should be able to commit in any order.
        let got_key1 = query2.key().clone();
        storage.commit(query2, SystemTime::now()).unwrap();
        assert_eq!(got_key1, key);
        let got_blob1 = storage.lookup(&key).unwrap().unwrap();
        assert_eq!(got_blob1.bytes(), blob);

        let got_key2 = query1.key().clone();
        storage.commit(query1, SystemTime::now()).unwrap();
        assert_eq!(got_key2, key);
        let got_blob2 = storage.lookup(&key).unwrap().unwrap();
        // Blob should not be changed.
        assert_eq!(got_blob2.bytes(), got_blob1.bytes());
        assert_eq!(got_blob2.created_at(), got_blob1.created_at());

        // Only a single blob should be in the database, and thus a single
        // index, but the data should be stored twice (two queries).
        assert_eq!(storage.len(), 1);
        assert_eq!(
            storage.data_size(),
            // Blob should only be stored once.
            (DATA_MAGIC.len() + blob.len()) as u64
        );
        assert_eq!(
            storage.index_size(),
            (size_of::<Entry>() as u64) + INDEX_MAGIC.len() as u64
        );
    }

    #[test]
    fn aborting_adding_blob() {
        let path = temp_dir("aborting_adding_blob.db");
        let mut storage = Storage::open(&path).unwrap();

        assert_eq!(storage.len(), 0);
        assert_eq!(storage.data_size(), DATA_MAGIC.len() as u64);
        assert_eq!(storage.index_size(), INDEX_MAGIC.len() as u64);
        assert_eq!(
            storage.total_size(),
            (DATA_MAGIC.len() + INDEX_MAGIC.len()) as u64
        );

        let blob = DATA[0];
        let key = Key::for_blob(blob);

        let query = storage.add_blob(blob).unwrap();

        // Blob shouldn't be accessible.
        assert_eq!(storage.len(), 0);
        assert!(storage.lookup(&key).is_none());
        // But is should be stored.
        assert_eq!(storage.data_size(), (DATA_MAGIC.len() + blob.len()) as u64);

        // After committing the blob should be accessible.
        storage.abort(query).unwrap();
        assert_eq!(storage.len(), 0);
        assert!(storage.lookup(&key).is_none());
        assert_eq!(storage.index_size(), INDEX_MAGIC.len() as u64);
    }

    #[test]
    fn blob_can_outlive_storage() {
        // `Blob` can outlive `Data`.
        let path = test_data_path("001.db");
        let _guard = DB_001.lock().unwrap();
        let storage = Storage::open(&path).unwrap();

        let entries = test_entries();
        let entry = &entries[0];
        let got = storage.lookup(entry.key()).unwrap().unwrap();

        // After we drop the storage the `Blob` (assigned to `got`) should still
        // be valid.
        drop(storage);

        let want = DATA[0];
        assert_eq!(got.bytes(), want);
        assert_eq!(
            ModifiedTime::Created(got.created_at()),
            entry.modified_time()
        );
    }

    #[test]
    fn cloned_blob_can_outlive_storage() {
        // `Blob` can outlive `Data`.
        let path = test_data_path("001.db");
        let _guard = DB_001.lock().unwrap();
        let storage = Storage::open(&path).unwrap();

        let entries = test_entries();
        let entry = &entries[0];
        let got = storage.lookup(entry.key()).unwrap().unwrap();

        // After we drop the storage the `Blob` (assigned to `got`) should still
        // be valid.
        drop(storage);

        let want = DATA[0];
        assert_eq!(got.bytes(), want);
        assert_eq!(
            ModifiedTime::Created(got.created_at()),
            entry.modified_time()
        );

        let got2 = got.clone();
        drop(got);
        assert_eq!(got2.bytes(), want);
        assert_eq!(
            ModifiedTime::Created(got2.created_at()),
            entry.modified_time()
        );
    }

    fn add_blobs(storage: &mut Storage, blobs: &[&[u8]]) -> Vec<Key> {
        let mut keys = Vec::with_capacity(blobs.len());
        for blob in blobs.iter().copied() {
            let query = storage.add_blob(blob).unwrap();
            let key = query.key().clone();
            storage.commit(query, SystemTime::now()).unwrap();
            keys.push(key);
        }
        keys
    }

    #[test]
    fn remove_multiple_blobs() {
        let path = temp_dir("remove_multiple_blobs.db");
        let mut storage = Storage::open(&path).unwrap();

        assert_eq!(storage.len(), 0);
        assert_eq!(storage.data_size(), DATA_MAGIC.len() as u64);
        assert_eq!(storage.index_size(), INDEX_MAGIC.len() as u64);
        assert_eq!(
            storage.total_size(),
            (DATA_MAGIC.len() + INDEX_MAGIC.len()) as u64
        );

        let blobs = [DATA[0], DATA[1], &[1; 100], &[2; 200], b"Store my data!"];
        let keys = add_blobs(&mut storage, &blobs);
        assert_eq!(storage.len(), 5);

        for (i, key) in keys.into_iter().enumerate() {
            assert_eq!(storage.len(), blobs.len() - i);
            let query = storage.remove_blob(key.clone()).unwrap();

            // Uncommitted so the blob is still available.
            let got = storage.lookup(&key).unwrap().unwrap();
            assert_eq!(got.bytes(), blobs[i]);

            let removed_at = SystemTime::now();
            storage.commit(query, removed_at).unwrap();
            assert!(storage.lookup(&key).unwrap().is_removed());
            assert_eq!(storage.len(), blobs.len() - i - 1);

            // Check the EntryIndex and BlobEntry.
            let (entry_index, blob_entry) = storage.blobs.get(&key).unwrap();
            assert_eq!(*entry_index, EntryIndex(i));
            match blob_entry {
                BlobEntry::Stored(_) => panic!("unexpected blob entry"),
                BlobEntry::Removed(got) => assert_eq!(*got, removed_at),
            }
        }

        assert_eq!(storage.len(), 0);
    }

    #[test]
    fn remove_blob_never_stored() {
        let path = temp_dir("remove_blob_never_stored.db");
        let mut storage = Storage::open(&path).unwrap();

        assert_eq!(storage.len(), 0);
        assert_eq!(storage.data_size(), DATA_MAGIC.len() as u64);
        assert_eq!(storage.index_size(), INDEX_MAGIC.len() as u64);
        assert_eq!(
            storage.total_size(),
            (DATA_MAGIC.len() + INDEX_MAGIC.len()) as u64
        );

        let key = Key::for_blob(DATA[0]);
        match storage.remove_blob(key) {
            RemoveResult::Ok(_) => panic!("expected blob not to be present"),
            RemoveResult::NotStored(Some(_)) => panic!("didn't expect the blob to be ever stored"),
            RemoveResult::NotStored(None) => (),
        };

        assert_eq!(storage.len(), 0);
    }

    #[test]
    fn remove_blob_removed() {
        let path = temp_dir("remove_blob_removed.db");
        let mut storage = Storage::open(&path).unwrap();

        assert_eq!(storage.len(), 0);
        assert_eq!(storage.data_size(), DATA_MAGIC.len() as u64);
        assert_eq!(storage.index_size(), INDEX_MAGIC.len() as u64);
        assert_eq!(
            storage.total_size(),
            (DATA_MAGIC.len() + INDEX_MAGIC.len()) as u64
        );

        let keys = add_blobs(&mut storage, &[DATA[0]]);
        assert_eq!(storage.len(), 1);
        let key = &keys[0];

        let removed_at = SystemTime::now();
        match storage.remove_blob(key.clone()) {
            RemoveResult::Ok(query) => {
                let got = storage.commit(query, removed_at).unwrap();
                assert_eq!(got, removed_at);
            }
            RemoveResult::NotStored(_) => panic!("expected blob to be present"),
        };
        assert_eq!(storage.len(), 0);

        // Should be marked as removed.
        let (entry_index, blob_entry) = storage.blobs.get(&key).unwrap();
        assert_eq!(*entry_index, EntryIndex(0));
        match blob_entry {
            BlobEntry::Stored(_) => panic!("unexpected blob entry"),
            BlobEntry::Removed(got) => assert_eq!(*got, removed_at),
        }
        match storage.remove_blob(key.clone()) {
            RemoveResult::Ok(_) => panic!("expected blob not to be present"),
            RemoveResult::NotStored(Some(got)) => assert_eq!(got, removed_at),
            RemoveResult::NotStored(None) => panic!("expected the blob to be removed"),
        };
        assert_eq!(storage.len(), 0);
    }

    #[test]
    fn abort_remove_blob() {
        let path = temp_dir("abort_remove_blob.db");
        let mut storage = Storage::open(&path).unwrap();

        assert_eq!(storage.len(), 0);
        assert_eq!(storage.data_size(), DATA_MAGIC.len() as u64);
        assert_eq!(storage.index_size(), INDEX_MAGIC.len() as u64);
        assert_eq!(
            storage.total_size(),
            (DATA_MAGIC.len() + INDEX_MAGIC.len()) as u64
        );

        let keys = add_blobs(&mut storage, &[DATA[0]]);
        assert_eq!(storage.len(), 1);
        let key = &keys[0];

        let query = storage.remove_blob(key.clone()).unwrap();

        assert!(storage.lookup(key).is_some());
        storage.abort(query).unwrap();
        assert!(storage.lookup(key).is_some());
        assert_eq!(storage.len(), 1);
    }

    #[test]
    fn concurrently_removing_blobs() {
        let path = temp_dir("concurrently_removing_blobs.db");
        let mut storage = Storage::open(&path).unwrap();

        assert_eq!(storage.len(), 0);
        assert_eq!(storage.data_size(), DATA_MAGIC.len() as u64);
        assert_eq!(storage.index_size(), INDEX_MAGIC.len() as u64);
        assert_eq!(
            storage.total_size(),
            (DATA_MAGIC.len() + INDEX_MAGIC.len()) as u64
        );

        let keys = add_blobs(&mut storage, &[DATA[0], DATA[1]]);
        assert_eq!(storage.len(), 2);
        let key1 = &keys[0];
        let key2 = &keys[1];

        let query1 = storage.remove_blob(key1.clone()).unwrap();
        let query2 = storage.remove_blob(key2.clone()).unwrap();

        let removed_at2 = SystemTime::now();
        let got2 = storage.commit(query2, removed_at2).unwrap();
        assert_eq!(got2, removed_at2);
        assert_eq!(storage.len(), 1);

        let removed_at1 = SystemTime::now();
        let got1 = storage.commit(query1, removed_at1).unwrap();
        assert_eq!(got1, removed_at1);
        assert_eq!(storage.len(), 0);
    }

    #[test]
    fn concurrently_removing_same_blob() {
        let path = temp_dir("concurrently_removing_same_blob.db");
        let mut storage = Storage::open(&path).unwrap();

        assert_eq!(storage.len(), 0);
        assert_eq!(storage.data_size(), DATA_MAGIC.len() as u64);
        assert_eq!(storage.index_size(), INDEX_MAGIC.len() as u64);
        assert_eq!(
            storage.total_size(),
            (DATA_MAGIC.len() + INDEX_MAGIC.len()) as u64
        );

        let keys = add_blobs(&mut storage, &[DATA[0]]);
        let key = &keys[0];

        assert_eq!(storage.len(), 1);

        let query1 = storage.remove_blob(key.clone()).unwrap();
        let query2 = storage.remove_blob(key.clone()).unwrap();

        let removed_at = SystemTime::now();
        let got = storage.commit(query1, removed_at).unwrap();
        let removed_at2 = SystemTime::now();
        assert_ne!(removed_at, removed_at2);
        let got2 = storage.commit(query2, removed_at2).unwrap();

        // Second query shouldn't change the removed at time.
        assert_eq!(got, removed_at);
        assert_eq!(got2, removed_at);

        assert_eq!(storage.len(), 0);
    }

    #[test]
    fn remove_query_can_outlive_storage() {
        let path = temp_dir("remove_query_can_outlive_storage.db");
        let mut storage = Storage::open(&path).unwrap();

        let keys = add_blobs(&mut storage, &[DATA[0]]);
        let key = &keys[0];
        let query = storage.remove_blob(key.clone()).unwrap();

        // Dropping the storage before the query should not panic.
        drop(storage);
        drop(query);
    }

    #[test]
    fn open_storage_with_removed_blobs() {
        let path = test_data_path("009.db");
        let _guard = DB_009.lock().unwrap();
        let storage = Storage::open(&path).unwrap();

        assert_eq!(storage.len(), 1);
        assert_eq!(storage.blobs.len(), 2);

        let key1 = Key::for_blob(DATA[0]);
        let key2 = Key::for_blob(DATA[1]);

        for (key, (entry_index, blob_entry)) in storage.blobs.iter() {
            if *key == key1 {
                assert_eq!(*entry_index, EntryIndex(0));
                let blob = match blob_entry {
                    BlobEntry::Stored(blob) => blob,
                    BlobEntry::Removed(_) => panic!("unexpected remove value"),
                };
                let created_at = SystemTime::UNIX_EPOCH + Duration::new(1586715570, 92000000);
                assert_eq!(blob.bytes(), DATA[0]);
                assert_eq!(blob.created_at(), created_at);
            } else if *key == key2 {
                assert_eq!(*key, Key::for_blob(DATA[1]));
                assert_eq!(*entry_index, EntryIndex(1));
                let got = match blob_entry {
                    BlobEntry::Stored(_) => panic!("unexpected remove value"),
                    BlobEntry::Removed(time) => time,
                };
                let removed_at = SystemTime::UNIX_EPOCH + Duration::new(1586715570, 92010000);
                assert_eq!(*got, removed_at);
            } else {
                panic!("unexpected key found: {}", key);
            }
        }

        assert!(storage.lookup(&key1).is_some());
        assert!(storage.lookup(&key2).unwrap().is_removed());
    }

    #[test]
    #[ignore = "index file get created on demand"]
    fn missing_index_file() {
        let path = test_data_path("004.db");
        match Storage::open(&path) {
            Ok(_) => panic!("expected to fail opening storage"),
            Err(err) => {
                assert_eq!(err.kind(), io::ErrorKind::InvalidData);
                assert!(err.to_string().contains("missing index file"));
            }
        }
    }

    #[test]
    #[ignore = "data file get created on demand"]
    fn missing_data_file() {
        let path = test_data_path("005.db");
        match Storage::open(&path) {
            Ok(_) => panic!("expected to fail opening storage"),
            Err(err) => {
                assert_eq!(err.kind(), io::ErrorKind::InvalidData);
                assert!(err.to_string().contains("missing data file"));
            }
        }
    }

    #[test]
    fn incorrect_index_entries() {
        let path = test_data_path("006.db");
        match Storage::open(&path) {
            Ok(_) => panic!("expected to fail opening storage"),
            Err(err) => {
                assert_eq!(err.kind(), io::ErrorKind::InvalidData);
                assert!(err.to_string().contains("incorrect index entry"));
            }
        }
    }

    #[test]
    fn database_locking() {
        let path = temp_dir("database_locking.db");
        let _storage1 = Storage::open(&path).unwrap();

        match Storage::open(&path) {
            Ok(_) => panic!("expected to fail opening storage"),
            Err(err) => {
                assert_eq!(err.kind(), io::ErrorKind::Other);
                assert!(err.to_string().contains("database already in used"));
            }
        }
    }

    #[test]
    fn add_blob_after_removed() {
        let path = temp_dir("add_blob_after_removed.db");
        let mut storage = Storage::open(&path).unwrap();

        assert_eq!(storage.len(), 0);
        assert_eq!(storage.data_size(), DATA_MAGIC.len() as u64);
        assert_eq!(storage.index_size(), INDEX_MAGIC.len() as u64);
        assert_eq!(
            storage.total_size(),
            (DATA_MAGIC.len() + INDEX_MAGIC.len()) as u64
        );

        let blob = DATA[0];
        let key = add_blobs(&mut storage, &[blob]).pop().unwrap();
        assert_eq!(storage.len(), 1);

        let query = storage.remove_blob(key.clone()).unwrap();
        let removed_at = SystemTime::now();
        storage.commit(query, removed_at).unwrap();
        assert_eq!(storage.len(), 0);

        // We should be able to overwrite a removed blob.
        let query = storage.add_blob(blob).unwrap();
        let got_key = query.key().clone();
        storage.commit(query, SystemTime::now()).unwrap();
        assert_eq!(got_key, key);

        assert_eq!(storage.len(), 1);
        let got = storage.lookup(&key).unwrap().unwrap();
        assert_eq!(got.bytes(), blob);
    }

    #[test]
    fn database_with_invalid_index_entries() {
        let path = test_data_path("010.db");
        let storage = Storage::open(&path).unwrap();

        assert_eq!(storage.len(), 1);
        assert_eq!(
            storage.data_size(),
            (DATA_MAGIC.len() + (DATA[0].len() * 2)) as u64
        );
        assert_eq!(
            storage.index_size(),
            (INDEX_MAGIC.len() + (size_of::<Entry>() * 2)) as u64
        );

        let want = DATA[0];
        let key = Key::for_blob(&want);
        assert_eq!(storage.lookup(&key).unwrap().unwrap().bytes(), want);
    }

    #[test]
    fn store_blob() {
        let path = temp_dir("store_blob.db");
        let mut storage = Storage::open(&path).unwrap();

        assert_eq!(storage.len(), 0);
        assert_eq!(storage.data_size(), DATA_MAGIC.len() as u64);
        assert_eq!(storage.index_size(), INDEX_MAGIC.len() as u64);
        assert_eq!(
            storage.total_size(),
            (DATA_MAGIC.len() + INDEX_MAGIC.len()) as u64
        );

        let now = SystemTime::now();
        let blobs = &[
            (DATA[0], SystemTime::UNIX_EPOCH, SystemTime::UNIX_EPOCH),
            (DATA[1], now, now),
            // Same blob as first one, should keep original timestamp.
            (
                DATA[0],
                SystemTime::UNIX_EPOCH + Duration::from_secs(10),
                SystemTime::UNIX_EPOCH,
            ),
        ];

        let mut keys = Vec::with_capacity(blobs.len());
        for (i, (blob, timestamp, want)) in blobs.iter().copied().enumerate() {
            let got = storage.store_blob(blob, timestamp).unwrap();
            assert_eq!(got, want);
            if timestamp == want {
                keys.push((i, Key::for_blob(blob)));
            }
        }

        for (i, key) in keys {
            let got = storage.lookup(&key).unwrap().unwrap();
            assert_eq!(got.bytes(), blobs[i].0);

            // Check the EntryIndex.
            let (entry_index, blob_entry) = storage.blobs.get(&key).unwrap();
            assert_eq!(*entry_index, EntryIndex(i));
            match blob_entry {
                BlobEntry::Stored(blob) => {
                    assert_eq!(blob.bytes(), blobs[i].0);
                    assert_eq!(blob.created_at(), blobs[i].2);
                }
                BlobEntry::Removed(_) => panic!("unexpected blob entry"),
            }
        }

        assert_eq!(storage.len(), 2);
        let want_data_size = (DATA_MAGIC.len() + DATA[0].len() + DATA[1].len()) as u64;
        assert_eq!(storage.data_size(), want_data_size);
        let want_index_size = (2 * size_of::<Entry>()) as u64 + INDEX_MAGIC.len() as u64;
        assert_eq!(storage.index_size(), want_index_size);
        assert_eq!(storage.total_size(), want_index_size + want_data_size);

        drop(storage);
        let data_metadata = fs::metadata(path.join("data")).unwrap();
        assert_eq!(data_metadata.len(), want_data_size);
        let index_metadata = fs::metadata(path.join("index")).unwrap();
        assert_eq!(index_metadata.len(), want_index_size);
    }

    #[test]
    fn store_removed_blob() {
        let path = temp_dir("store_removed_blob.db");
        let mut storage = Storage::open(&path).unwrap();

        assert_eq!(storage.len(), 0);
        assert_eq!(storage.data_size(), DATA_MAGIC.len() as u64);
        assert_eq!(storage.index_size(), INDEX_MAGIC.len() as u64);
        assert_eq!(
            storage.total_size(),
            (DATA_MAGIC.len() + INDEX_MAGIC.len()) as u64
        );

        let query = storage.add_blob(DATA[0]).unwrap();
        storage.commit(query, SystemTime::now()).unwrap();

        storage
            .store_blob(DATA[1], SystemTime::UNIX_EPOCH + Duration::from_secs(1))
            .unwrap();
        let query = storage.remove_blob(Key::for_blob(DATA[1])).unwrap();
        storage
            .commit(query, SystemTime::UNIX_EPOCH + Duration::from_secs(100))
            .unwrap();

        let now = SystemTime::now();
        let blobs = &[
            (Key::for_blob(b"never stored blob"), now, now, 2),
            // Already stored.
            (
                Key::for_blob(DATA[0]),
                SystemTime::UNIX_EPOCH,
                SystemTime::UNIX_EPOCH,
                0,
            ),
            // Previously removed.
            (
                Key::for_blob(DATA[1]),
                SystemTime::UNIX_EPOCH + Duration::from_secs(2),
                SystemTime::UNIX_EPOCH + Duration::from_secs(100),
                1,
            ),
            // Same blob as first one, should keep original timestamp.
            (
                Key::for_blob(DATA[0]),
                SystemTime::UNIX_EPOCH + Duration::from_secs(10),
                SystemTime::UNIX_EPOCH,
                0,
            ),
        ];

        let mut keys = Vec::with_capacity(blobs.len());
        for (i, (key, timestamp, want, entry_index)) in blobs.iter().cloned().enumerate() {
            let got = storage.store_removed_blob(key.clone(), timestamp).unwrap();
            assert_eq!(got, want);
            if timestamp == want {
                keys.push((i, entry_index, key));
            }
        }

        for (i, want_entry_index, key) in keys {
            let got = storage.lookup(&key).unwrap().unwrap_removed();
            assert_eq!(got, blobs[i].2);

            // Check the EntryIndex.
            let (entry_index, blob_entry) = storage.blobs.get(&key).unwrap();
            assert_eq!(*entry_index, EntryIndex(want_entry_index));
            match blob_entry {
                BlobEntry::Stored(_) => panic!("unexpected blob entry"),
                BlobEntry::Removed(got) => assert_eq!(*got, blobs[i].2),
            }
        }

        assert_eq!(storage.len(), 0);
        let want_data_size = (DATA_MAGIC.len() + DATA[0].len() + DATA[1].len()) as u64;
        assert_eq!(storage.data_size(), want_data_size);
        let want_index_size = (3 * size_of::<Entry>()) as u64 + INDEX_MAGIC.len() as u64;
        assert_eq!(storage.index_size(), want_index_size);
        assert_eq!(storage.total_size(), want_index_size + want_data_size);

        drop(storage);
        let data_metadata = fs::metadata(path.join("data")).unwrap();
        assert_eq!(data_metadata.len(), want_data_size);
        let index_metadata = fs::metadata(path.join("index")).unwrap();
        assert_eq!(index_metadata.len(), want_index_size);
    }

    #[test]
    fn round_trip() {
        let path = temp_dir("round_trip.db");
        let mut storage = Storage::open(&path).unwrap();

        // TODO: add aborting commits.
        const BLOBS: [&[u8]; 4] = [
            b"added data",
            b"added and removed data",
            b"added and removed and added again data",
            b"stored data from sync",
        ];

        // 1. Add a blob.
        let query = storage.add_blob(BLOBS[0]).unwrap();
        let time1 = SystemTime::now();
        let key1 = query.key().clone();
        storage.commit(query, time1).unwrap();

        // 2. Add and remove the same blob.
        let query = storage.add_blob(BLOBS[1]).unwrap();
        let key2 = query.key().clone();
        storage.commit(query, SystemTime::now()).unwrap();
        let query = storage.remove_blob(key2.clone()).unwrap();
        let time2 = SystemTime::now();
        storage.commit(query, time2).unwrap();

        // 3. Add and remove the same blob, then add it again.
        let query = storage.add_blob(BLOBS[2]).unwrap();
        let key3 = query.key().clone();
        storage.commit(query, SystemTime::now()).unwrap();
        let query = storage.remove_blob(key3.clone()).unwrap();
        storage.commit(query, SystemTime::now()).unwrap();
        let query = storage.add_blob(BLOBS[2]).unwrap();
        let time3 = SystemTime::now();
        storage.commit(query, time3).unwrap();

        // 4. Stored blob added in a single step for synchronisation.
        let time4 = storage.store_blob(BLOBS[3], SystemTime::now()).unwrap();
        let key4 = Key::for_blob(BLOBS[3]);
        // Blob already stored, shouldn't change anything.
        let got = storage.store_blob(BLOBS[2], SystemTime::now()).unwrap();
        assert_eq!(got, time3);

        // 5. Store removed blob for synchronisation.
        let key5 = Key::for_blob(b"removed data from sync");
        let time5 = storage
            .store_removed_blob(key5.clone(), SystemTime::now())
            .unwrap();

        // Close the database and open it again.
        drop(storage);
        let storage = Storage::open(&path).unwrap();

        // 1. Blob should be present.
        let got1 = storage.lookup(&key1).unwrap().unwrap();
        assert_eq!(got1.bytes(), BLOBS[0]);
        assert_eq!(got1.created_at(), time1);

        // 2. Blob should be marked as removed.
        let got2 = storage.lookup(&key2).unwrap().unwrap_removed();
        assert_eq!(got2, time2);

        // 3. Should be present.
        let got3 = storage.lookup(&key3).unwrap().unwrap();
        assert_eq!(got3.bytes(), BLOBS[2]);
        assert_eq!(got3.created_at(), time3);

        // 4. Should be present.
        let got4 = storage.lookup(&key4).unwrap().unwrap();
        assert_eq!(got4.bytes(), BLOBS[3]);
        assert_eq!(got4.created_at(), time4);

        // 5. Should still be removed.
        let got5 = storage.lookup(&key5).unwrap().unwrap_removed();
        assert_eq!(got5, time5);

        // Check the lengths of the files.
        assert_eq!(storage.len(), 3);
        let blobs_len: u64 = BLOBS.iter().map(|b| b.len() as u64).sum::<u64>() +
            // Stored twice in step 3.
            BLOBS[2].len() as u64;
        assert_eq!(storage.data_size(), DATA_MAGIC.len() as u64 + blobs_len);
        // Step 3 has two entries, step 5 has no blob.
        let want_entries_len = BLOBS.len() + 1 + 1;
        assert_eq!(
            storage.index_size(),
            (INDEX_MAGIC.len() + want_entries_len * size_of::<Entry>()) as u64
        );
    }
}

mod validate {
    use std::num::NonZeroUsize;

    use crate::storage::validate;

    use super::{test_data_path, test_entries, DB_008};

    #[test]
    fn validate_database() {
        let path = test_data_path("008.db");
        let _guard = DB_008.lock().unwrap();

        let corruptions = validate(&path, NonZeroUsize::new(1).unwrap()).unwrap();

        // "Hello world" was changed to "Hello pluto".
        assert_eq!(corruptions.len(), 1);
        assert_eq!(*corruptions[0].key(), test_entries()[0].key);
    }
}

mod mmap {
    use std::mem::size_of;

    use crate::storage::{
        is_page_aligned, next_page_aligned, prev_page_aligned, DATA_PRE_ALLOC_BYTES, PAGE_BITS,
        PAGE_SIZE,
    };

    #[test]
    fn page_size() {
        let got = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        assert_eq!(got, PAGE_SIZE as libc::c_long, "incorrect page size");
    }

    #[test]
    fn pre_alloc_is_page_aligned() {
        assert!(is_page_aligned(DATA_PRE_ALLOC_BYTES));
    }

    #[test]
    fn test_is_page_aligned() {
        assert!(is_page_aligned(0));
        for n_bits in 1..PAGE_BITS {
            assert!(!is_page_aligned(1 << n_bits));
            assert!(!is_page_aligned((1 << n_bits) + 1));
        }

        for n_bits in PAGE_BITS..(size_of::<usize>() * 8) {
            assert!(is_page_aligned(1 << n_bits));
            assert!(!is_page_aligned((1 << n_bits) - 1));
        }
    }

    #[test]
    fn test_prev_page_aligned() {
        assert_eq!(prev_page_aligned(0), 0);
        assert_eq!(prev_page_aligned(1), 0);
        assert_eq!(prev_page_aligned(10), 0);
        assert_eq!(prev_page_aligned(100), 0);
        assert_eq!(prev_page_aligned(PAGE_SIZE - 1), 0);
        assert_eq!(prev_page_aligned(PAGE_SIZE), PAGE_SIZE);
        assert_eq!(prev_page_aligned(PAGE_SIZE + 1), PAGE_SIZE);
        assert_eq!(prev_page_aligned(2 * PAGE_SIZE - 1), PAGE_SIZE);
        assert_eq!(prev_page_aligned(2 * PAGE_SIZE), 2 * PAGE_SIZE);
        assert_eq!(prev_page_aligned(2 * PAGE_SIZE + 1), 2 * PAGE_SIZE);
        assert_eq!(
            prev_page_aligned(2 * PAGE_SIZE + (PAGE_SIZE - 1)),
            2 * PAGE_SIZE
        );
    }

    #[test]
    fn test_next_page_aligned() {
        assert_eq!(next_page_aligned(0), PAGE_SIZE);
        assert_eq!(next_page_aligned(1), PAGE_SIZE);
        assert_eq!(next_page_aligned(10), PAGE_SIZE);
        assert_eq!(next_page_aligned(100), PAGE_SIZE);
        assert_eq!(next_page_aligned(PAGE_SIZE - 1), PAGE_SIZE);
        assert_eq!(next_page_aligned(PAGE_SIZE), 2 * PAGE_SIZE);
        assert_eq!(next_page_aligned(PAGE_SIZE + 1), 2 * PAGE_SIZE);
        assert_eq!(next_page_aligned(2 * PAGE_SIZE - 1), 2 * PAGE_SIZE);
        assert_eq!(next_page_aligned(2 * PAGE_SIZE), 3 * PAGE_SIZE);
        assert_eq!(next_page_aligned(2 * PAGE_SIZE + 1), 3 * PAGE_SIZE);
        assert_eq!(
            next_page_aligned(2 * PAGE_SIZE + (PAGE_SIZE - 1)),
            3 * PAGE_SIZE
        );
    }
}
