use std::env;
use std::fs::{remove_dir_all, remove_file};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use lazy_static::lazy_static;

use crate::Key;

use super::*;

#[test]
fn magic_headers() {
    assert_eq!(DATA_MAGIC.len(), 16);
    assert_eq!(INDEX_MAGIC.len(), 16);
}

/// Returns the path to the test data `file`.
fn test_data_path(file: &str) -> PathBuf {
    Path::new("./tests/data/").join(file)
}

lazy_static! {
    /// Locks database located at "tests/data/001.db".
    static ref DB_001: Mutex<()> = Mutex::new(());
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
            created_at: DateTime {
                seconds: 5u64.to_be(),
                subsec_nanos: 0u32.to_be(),
            },
        },
        Entry {
            key: Key::for_blob(DATA[1]),
            offset: ((DATA_MAGIC.len() + DATA[0].len()) as u64).to_be(),
            length: (DATA[1].len() as u32).to_be(),
            created_at: DateTime {
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
    let path = env::temp_dir().join(name);
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
    let path = env::temp_dir().join(name);
    TempDir { path }
}

mod date_time {
    use std::mem::size_of;
    use std::ptr;
    use std::time::{Duration, SystemTime};

    use super::DateTime;

    #[test]
    fn from_and_into_sys_time() {
        let tests = [
            (
                DateTime {
                    seconds: 1u64.to_be(),
                    subsec_nanos: 0u32.to_be(),
                },
                Duration::from_secs(1),
            ),
            (
                DateTime {
                    seconds: (u64::MAX / 2).to_be(),
                    subsec_nanos: 0u32.to_be(),
                },
                Duration::from_secs(u64::MAX / 2),
            ),
        ];

        for (input, add) in &tests {
            let got: SystemTime = (*input).into();
            let want = SystemTime::UNIX_EPOCH + *add;
            assert_eq!(got, want);

            let round_trip: DateTime = got.into();
            assert_eq!(*input, round_trip);
        }
    }

    #[test]
    fn from_bytes() {
        let tests = [SystemTime::now().into(), SystemTime::UNIX_EPOCH.into()];

        fn copy(dst: &mut [u8], src: &DateTime) {
            assert!(dst.len() >= size_of::<DateTime>());
            let src: *const u8 = src as *const _ as *const _;
            unsafe { ptr::copy_nonoverlapping(src, dst.as_mut_ptr(), size_of::<DateTime>()) }
        }

        for time in &tests {
            let mut buf = [0; size_of::<DateTime>()];
            copy(&mut buf, &time);

            // `DateTime` format should be they same when reading from disk (or
            // a in-memory buffer).
            let got: &DateTime = unsafe { &*(&buf as *const _ as *const _) };
            assert_eq!(got, time);
        }
    }
}

mod index {
    use std::io::{Seek, SeekFrom};
    use std::mem::size_of;
    use std::{fs, io};

    use super::{temp_file, test_data_path, test_entries, Entry, Index, DB_001, INDEX_MAGIC};

    #[test]
    fn entry_size() {
        // Size and layout is fixed.
        assert_eq!(size_of::<Entry>(), 88);
    }

    #[test]
    fn new_entry() {
        for entry in test_entries().iter() {
            let got = Entry::new(
                entry.key().clone(),
                entry.offset(),
                entry.length(),
                entry.created_at(),
            );
            assert_eq!(got, *entry);
        }
    }

    #[test]
    fn empty_index() {
        let path = temp_file("empty_index.index");
        fs::write(&path, INDEX_MAGIC).unwrap();

        let mut index = Index::open(&path).unwrap();

        let mut entries = index.entries().unwrap();
        assert_eq!(entries.len(), 0);
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
        for (i, entry) in entries.enumerate() {
            assert_eq!(entry, &wanted[i]);
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
        let mut entries = index.entries().unwrap();
        assert_eq!(entries.len(), 0);
        assert!(entries.next().is_none());
        drop(entries);

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
        for (i, entry) in entries.enumerate() {
            assert_eq!(entry, &test_entries[i]);
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
    fn invalid_length() {
        let path = test_data_path("003.db/index");
        let mut index = Index::open(&path).unwrap();
        let res = index.entries();
        match res {
            Ok(_) => panic!("expected to fail to access index entries"),
            Err(err) => {
                assert_eq!(err.kind(), io::ErrorKind::InvalidData);
                assert!(err.to_string().contains("invalid index file size"));
            }
        }
    }
}

mod data {
    use std::mem::{size_of, ManuallyDrop};
    use std::ptr::NonNull;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use std::{fs, io, slice, thread};

    use super::{
        is_page_aligned, mmap, munmap, next_page_aligned, temp_file, test_data_path, test_entries,
        Data, MmapArea, MmapAreaControl, DATA, DATA_MAGIC, DB_001, PAGE_BITS, PAGE_SIZE,
    };

    #[test]
    fn page_size() {
        let got = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        assert_eq!(got, PAGE_SIZE as libc::c_long, "incorrect page size");
    }

    #[test]
    fn in_area() {
        // Don't want to drop the area as the address is invalid to unmap.
        let mut area = ManuallyDrop::new(MmapArea {
            mmap_address: NonNull::dangling(),
            mmap_length: 0,
            offset: 0,
            length: 0,
            ref_count: AtomicUsize::new(1),
        });

        let tests: &[((u64, libc::size_t), (u64, u32), bool)] = &[
            ((0, 10), (0, 5), true),
            ((0, 5), (0, 5), true),
            ((0, 0), (0, 0), true),
            // Incorrect offset.
            ((10, 10), (0, 5), false),
            ((10, 10), (0, 20), false),
            // Offset ok, too large.
            ((0, 0), (0, 1), false),
            ((0, 10), (0, 12), false),
        ];

        for ((offset, length), (blob_offset, blob_length), want) in tests.iter().copied() {
            area.offset = offset;
            area.length = length;

            let got = area.in_area(blob_offset, blob_length);
            assert_eq!(got, want);
        }
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

    #[test]
    fn empty_data() {
        let path = temp_file("empty_data.data");
        fs::write(&path, DATA_MAGIC).unwrap();

        let data = Data::open(&path).unwrap();
        assert_eq!(data.length, DATA_MAGIC.len() as u64);
        // Should already `mmap` the magic header bytes.
        assert_eq!(data.areas.len(), 1);
        let area = &data.areas[0];
        assert_eq!(area.mmap_length, DATA_MAGIC.len());
        assert_eq!(area.offset, 0);
        assert_eq!(area.length, DATA_MAGIC.len());
        assert_eq!(area.ref_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn open_data_file() {
        let path = test_data_path("001.db/data");
        let _guard = DB_001.lock().unwrap();
        let data = Data::open(&path).unwrap();

        assert_eq!(
            data.file_length(),
            (DATA.iter().map(|d| d.len()).sum::<usize>() + DATA_MAGIC.len()) as u64
        );
        assert_eq!(data.areas.len(), 1);
        let mmap_area = data.areas.first().unwrap();
        assert_eq!(mmap_area.offset, 0);
        assert_eq!(
            mmap_area.length,
            DATA.iter().map(|d| d.len()).sum::<usize>() + DATA_MAGIC.len()
        );

        for (i, entry) in test_entries().iter().enumerate() {
            let (blob_address, _) = data.address_for(entry.offset(), entry.length()).unwrap();
            let blob =
                unsafe { slice::from_raw_parts(blob_address.as_ptr(), entry.length() as usize) };

            assert_eq!(blob, DATA[i]);
        }
    }

    #[test]
    fn add_blob_grow_area() {
        let path = temp_file("add_blob_grow_area.data");
        let mut data = Data::open(&path).unwrap();
        assert_eq!(data.areas.len(), 1);
        assert_eq!(data.file_length(), DATA_MAGIC.len() as u64);

        // Adding a first blob should create a new area.
        let (offset, address1, _) = data.add_blob(DATA[0]).unwrap();
        assert_eq!(offset, DATA_MAGIC.len() as u64);
        let got1 = unsafe { slice::from_raw_parts(address1.as_ptr(), DATA[0].len()) };
        assert_eq!(got1, DATA[0]);

        assert_eq!(
            data.file_length(),
            (DATA[0].len() + DATA_MAGIC.len()) as u64
        );
        assert_eq!(data.areas.len(), 1);
        let mmap_area = data.areas.first().unwrap();
        assert_eq!(mmap_area.offset, 0);
        assert_eq!(mmap_area.length, DATA[0].len() + DATA_MAGIC.len());

        // Adding a second should grow the existing area.
        let (offset, address2, _) = data.add_blob(DATA[1]).unwrap();
        // Check area grown.
        assert_eq!(
            unsafe { address1.as_ptr().add(DATA[0].len()) },
            address2.as_ptr()
        );
        assert_eq!(offset, (DATA[0].len() + DATA_MAGIC.len()) as u64);
        let got2 = unsafe { slice::from_raw_parts(address2.as_ptr(), DATA[1].len()) };
        assert_eq!(got2, DATA[1]);

        assert_eq!(
            data.file_length(),
            (DATA.iter().map(|d| d.len()).sum::<usize>() + DATA_MAGIC.len()) as u64
        );
        assert_eq!(data.areas.len(), 1);
        let mmap_area = data.areas.first().unwrap();
        assert_eq!(mmap_area.offset, 0);
        assert_eq!(
            mmap_area.length,
            DATA.iter().map(|d| d.len()).sum::<usize>() + DATA_MAGIC.len()
        );

        // Original address must still be valid.
        assert_eq!(got1, DATA[0]);
    }

    /// Length used in `create_dummy_area_after` to create dummy `mmap` areas.
    const DUMMY_LENGTH: usize = 100;

    fn create_dummy_area_after(areas: &[MmapAreaControl]) -> *mut libc::c_void {
        let area = areas.last().unwrap();
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

            if dummy_address == want_dummy_address {
                // Success!
                return dummy_address;
            } else {
                // Wrong page, try again.
                munmap(dummy_address, DUMMY_LENGTH).unwrap();
                thread::sleep(Duration::from_millis(10));
            }
        }

        panic!("OS didn't give us the memory we need for this test. NOT A TEST FAILURE");
    }

    #[test]
    #[ignore = "test is flaky due to the required memory placement"]
    fn add_blob_new_area() {
        const DATA1: [u8; PAGE_SIZE] = [1; PAGE_SIZE];
        const DATA2: [u8; PAGE_SIZE] = [2; PAGE_SIZE];
        const DATA3: [u8; PAGE_SIZE] = [3; PAGE_SIZE];

        let tests = &[
            // Perfect fit.
            (PAGE_SIZE, PAGE_SIZE, PAGE_SIZE),
            // Too small a space leftover.
            (PAGE_SIZE - 1, PAGE_SIZE, PAGE_SIZE),
            (PAGE_SIZE, PAGE_SIZE - 1, PAGE_SIZE),
            (PAGE_SIZE, PAGE_SIZE, PAGE_SIZE - 1),
            (1, PAGE_SIZE, PAGE_SIZE),
            (PAGE_SIZE, 1, PAGE_SIZE),
            (PAGE_SIZE, PAGE_SIZE, 1),
            // Misc.
            (PAGE_SIZE - 1, 2, PAGE_SIZE),
        ];

        for (s1, s2, s3) in tests.iter().copied() {
            let blob1 = &DATA1[0..s1];
            let blob2 = &DATA2[0..s2];
            let blob3 = &DATA3[0..s3];

            let path = temp_file("add_blob_new_area.data");
            let mut data = Data::open(&path).unwrap();

            // Adding a first blob should create a new area.
            let (offset, address1, _) = data.add_blob(blob1).unwrap();
            assert_eq!(offset, 0);
            let got1 = unsafe { slice::from_raw_parts(address1.as_ptr(), blob1.len()) };
            assert_eq!(got1, blob1);

            assert_eq!(data.file_length(), blob1.len() as u64);
            assert_eq!(data.areas.len(), 1);
            let mmap_area = &data.areas[0];
            assert_eq!(mmap_area.offset, 0);
            assert_eq!(mmap_area.length, blob1.len());

            // Create an new `mmap`ing so that the area created above can't be
            // extended.
            let dummy_address1 = create_dummy_area_after(&data.areas);

            // Adding a second blob should create a new area.
            let (offset, address2, _) = data.add_blob(blob2).unwrap();
            assert_eq!(offset, blob1.len() as u64);
            let got2 = unsafe { slice::from_raw_parts(address2.as_ptr(), blob2.len()) };
            assert_eq!(got2, blob2);
            // Check that we didn't overwrite our dummy.
            assert_ne!(address2.as_ptr(), dummy_address1 as *mut _);

            assert_eq!(data.file_length(), (blob1.len() + blob2.len()) as u64);
            assert_eq!(data.areas.len(), 2);
            let mmap_area = &data.areas[0];
            assert_eq!(mmap_area.offset, 0);
            assert_eq!(mmap_area.length, blob1.len());
            let mmap_area = &data.areas[1];
            assert_eq!(mmap_area.offset, blob1.len() as u64);
            assert_eq!(mmap_area.length, blob2.len());

            let dummy_address2 = create_dummy_area_after(&data.areas);
            // Adding a second blob should create a new area.
            let (offset, address3, _) = data.add_blob(blob3).unwrap();
            assert_eq!(offset, (blob1.len() + blob2.len()) as u64);
            let got3 = unsafe { slice::from_raw_parts(address3.as_ptr(), blob3.len()) };
            assert_eq!(got3, blob3);
            // Check that we didn't overwrite our dummy.
            assert_ne!(address3.as_ptr(), dummy_address2 as *mut _);

            assert_eq!(
                data.file_length(),
                (blob1.len() + blob2.len() + blob3.len()) as u64
            );
            assert_eq!(data.areas.len(), 3);
            let mmap_area = &data.areas[0];
            assert_eq!(mmap_area.offset, 0);
            assert_eq!(mmap_area.length, blob1.len());
            let mmap_area = &data.areas[1];
            assert_eq!(mmap_area.offset, blob1.len() as u64);
            assert_eq!(mmap_area.length, blob2.len());
            let mmap_area = &data.areas[2];
            assert_eq!(mmap_area.offset, (blob1.len() + blob2.len()) as u64);
            assert_eq!(mmap_area.length, blob3.len());

            // All returned addresses must still be valid.
            assert_eq!(got1, blob1);
            assert_eq!(got2, blob2);

            munmap(dummy_address1, DUMMY_LENGTH).unwrap();
            munmap(dummy_address2, DUMMY_LENGTH).unwrap();
        }
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
    use std::time::SystemTime;
    use std::{fs, io};

    use super::{
        temp_dir, test_data_path, test_entries, AddBlob, AddResult, Blob, Entry, Storage, DATA,
        DATA_MAGIC, DB_001, INDEX_MAGIC,
    };
    use crate::Key;

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

        assert_eq!(storage.len(), DATA.len());
        let data_metadata = fs::metadata(path.join("data")).unwrap();
        assert_eq!(storage.data_size(), data_metadata.len());
        let index_metadata = fs::metadata(path.join("index")).unwrap();
        assert_eq!(storage.index_size(), index_metadata.len());
        assert_eq!(
            storage.total_size(),
            data_metadata.len() + index_metadata.len()
        );

        for (i, entry) in test_entries().iter().enumerate() {
            let got = storage.lookup(entry.key()).unwrap();
            let want = DATA[i];
            assert_eq!(got.bytes(), want);
            assert_eq!(got.created_at(), entry.created_at());
        }
    }

    fn unwrap(result: AddResult) -> AddBlob {
        use AddResult::*;
        match result {
            Ok(query) => query,
            AlreadyPresent(key) => panic!("blob already present: {}", key),
            Err(err) => panic!("unexpected error: {}", err),
        }
    }

    #[test]
    fn add_blobs() {
        let path = temp_dir("add_blobs.db");
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
            let query = unwrap(storage.add_blob(blob));
            let key = storage.commit(query, SystemTime::now()).unwrap();

            let got = storage.lookup(&key).unwrap();
            assert_eq!(got.bytes(), blob);
            // TODO: check created_at data? SystemTime is not monotonic so the
            // test will be flaky.
            keys.push(key);
        }

        for (i, key) in keys.into_iter().enumerate() {
            let got = storage.lookup(&key).unwrap();
            assert_eq!(got.bytes(), blobs[i]);
        }

        assert_eq!(storage.len(), blobs.len());
        let data_metadata = fs::metadata(path.join("data")).unwrap();
        let want_data_size =
            (blobs.iter().map(|b| b.len()).sum::<usize>() + DATA_MAGIC.len()) as u64;
        assert_eq!(storage.data_size(), data_metadata.len());
        assert_eq!(storage.data_size(), want_data_size);
        let index_metadata = fs::metadata(path.join("index")).unwrap();
        let want_index_size = (blobs.len() * size_of::<Entry>()) as u64 + INDEX_MAGIC.len() as u64;
        assert_eq!(storage.index_size(), index_metadata.len());
        assert_eq!(storage.index_size(), want_index_size);
        assert_eq!(
            storage.total_size(),
            data_metadata.len() + index_metadata.len()
        );
        assert_eq!(storage.total_size(), want_index_size + want_data_size,);
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
        let query = unwrap(storage.add_blob(blob));
        let key = storage.commit(query, SystemTime::now()).unwrap();
        assert_eq!(key, want_key);

        // Second time it should already be present.
        match storage.add_blob(blob) {
            AddResult::AlreadyPresent(got_key) => {
                assert_eq!(got_key, want_key);
            }
            AddResult::Ok(_) | AddResult::Err(_) => panic!("unexpected add_blob result"),
        }
    }

    #[test]
    fn query_can_outlive_storage() {
        let path = temp_dir("query_can_outlive_storage.db");
        let mut storage = Storage::open(&path).unwrap();

        let blob = DATA[0];
        let query = unwrap(storage.add_blob(blob));

        // Dropping the storage before the query should not panic.
        drop(storage);
        drop(query);
    }

    #[test]
    fn uncommited_blobs() {
        let path = temp_dir("uncommited_blobs.db");
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

        let query = unwrap(storage.add_blob(blob));

        // Blob shouldn't be accessible.
        assert_eq!(storage.len(), 0);
        assert!(storage.lookup(&key).is_none());
        // But is should be stored.
        assert_eq!(storage.data_size(), (DATA_MAGIC.len() + blob.len()) as u64);

        // After committing the blob should be accessible.
        let got_key = storage.commit(query, SystemTime::now()).unwrap();
        assert_eq!(got_key, key);
        assert_eq!(storage.lookup(&key).unwrap().bytes(), blob);
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

        let query1 = unwrap(storage.add_blob(blob1));

        // Blob shouldn't be accessible.
        assert_eq!(storage.len(), 0);
        assert!(storage.lookup(&key1).is_none());
        // But is should be stored.
        assert_eq!(storage.data_size(), (DATA_MAGIC.len() + blob1.len()) as u64);

        let query2 = unwrap(storage.add_blob(blob2));

        assert_eq!(storage.len(), 0);
        assert!(storage.lookup(&key2).is_none());
        assert_eq!(
            storage.data_size(),
            (DATA_MAGIC.len() + blob1.len() + blob2.len()) as u64
        );

        // We should be able to commit in any order.
        let got_key2 = storage.commit(query2, SystemTime::now()).unwrap();
        assert_eq!(got_key2, key2);
        assert_eq!(storage.lookup(&key2).unwrap().bytes(), blob2);

        // After committing the blob should be accessible.
        let got_key1 = storage.commit(query1, SystemTime::now()).unwrap();
        assert_eq!(got_key1, key1);
        assert_eq!(storage.lookup(&key1).unwrap().bytes(), blob1);
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

        let query1 = unwrap(storage.add_blob(blob));
        let query2 = unwrap(storage.add_blob(blob));

        assert_eq!(storage.len(), 0);
        assert!(storage.lookup(&key).is_none());
        assert_eq!(
            storage.data_size(),
            (DATA_MAGIC.len() + blob.len() + blob.len()) as u64
        );

        // We should be able to commit in any order.
        let got_key1 = storage.commit(query2, SystemTime::now()).unwrap();
        assert_eq!(got_key1, key);
        let got_blob1 = storage.lookup(&key).unwrap();
        assert_eq!(got_blob1.bytes(), blob);

        let got_key2 = storage.commit(query1, SystemTime::now()).unwrap();
        assert_eq!(got_key2, key);
        let got_blob2 = storage.lookup(&key).unwrap();
        // Blob should not be changed.
        assert_eq!(got_blob2.bytes(), got_blob1.bytes());
        assert_eq!(got_blob2.created_at(), got_blob1.created_at());

        // Only a single blob should be in the database, and thus a single
        // index, but the data should be stored twice (two queries).
        assert_eq!(storage.len(), 1);
        assert_eq!(
            storage.data_size(),
            (DATA_MAGIC.len() + (blob.len() * 2)) as u64
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

        let query = unwrap(storage.add_blob(blob));

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
        let got = storage.lookup(entry.key()).unwrap();

        // After we drop the storage the `Blob` (assigned to `got`) should still
        // be valid.
        drop(storage);

        let want = DATA[0];
        assert_eq!(got.bytes(), want);
        assert_eq!(got.created_at(), entry.created_at());
    }

    #[test]
    fn cloned_blob_can_outlive_storage() {
        // `Blob` can outlive `Data`.
        let path = test_data_path("001.db");
        let _guard = DB_001.lock().unwrap();
        let storage = Storage::open(&path).unwrap();

        let entries = test_entries();
        let entry = &entries[0];
        let got = storage.lookup(entry.key()).unwrap();

        // After we drop the storage the `Blob` (assigned to `got`) should still
        // be valid.
        drop(storage);

        let want = DATA[0];
        assert_eq!(got.bytes(), want);
        assert_eq!(got.created_at(), entry.created_at());

        let got2 = got.clone();
        drop(got);
        assert_eq!(got2.bytes(), want);
        assert_eq!(got2.created_at(), entry.created_at());
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
    fn invalid_index_entries() {
        let path = test_data_path("006.db");
        match Storage::open(&path) {
            Ok(_) => panic!("expected to fail opening storage"),
            Err(err) => {
                assert_eq!(err.kind(), io::ErrorKind::InvalidData);
                assert!(err.to_string().contains("invalid index entry"));
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
}
