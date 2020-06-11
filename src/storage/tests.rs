use std::fs::{remove_dir_all, remove_file};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::{env, fs};

use lazy_static::lazy_static;

use crate::Key;

use super::*;

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
    fn unwrap(self) -> AddBlob {
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
    let path = env::temp_dir().join(name);
    // Remove the old database from previous tests.
    let _ = fs::remove_dir_all(&path);
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
    // Remove the old database from previous tests.
    let _ = fs::remove_dir_all(&path);
    TempDir { path }
}

mod date_time {
    use std::mem::size_of;
    use std::ptr;
    use std::time::{Duration, SystemTime};

    use super::{DateTime, ModifiedTime};

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

    use super::{
        temp_file, test_data_path, test_entries, DateTime, Entry, Index, Key, ModifiedTime, DATA,
        DATA_MAGIC, DB_001, INDEX_MAGIC,
    };

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
    fn empty_index() {
        let path = temp_file("empty_index.index");
        fs::write(&path, INDEX_MAGIC).unwrap();

        let mut index = Index::open(&path).unwrap();

        let entries = index.entries().unwrap();
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
        let mut index = Index::open(&path).unwrap();
        let res = index.entries();
        match res {
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

        println!("entries: {:#?}", (&*entries));

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
    use std::time::{Duration, SystemTime};
    use std::{fs, io};

    use super::{
        temp_dir, test_data_path, test_entries, AddResult, Blob, BlobEntry, Entry, EntryIndex,
        ModifiedTime, RemoveResult, Storage, DATA, DATA_MAGIC, DB_001, INDEX_MAGIC,
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
            let got = storage.lookup(entry.key()).unwrap().unwrap();
            let want = DATA[i];
            assert_eq!(got.bytes(), want);
            assert_eq!(
                ModifiedTime::Created(got.created_at()),
                entry.modified_time()
            );
        }
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
        assert_eq!(uncommitted_blob1.length, uncommitted_blob2.length);
        assert_eq!(uncommitted_blob1.address, uncommitted_blob2.address);
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
    fn round_trip() {
        let path = temp_dir("round_trip.db");
        let mut storage = Storage::open(&path).unwrap();

        // TODO: add aborting commits.
        const BLOBS: [&[u8]; 3] = [
            b"added data",
            b"added and removed data",
            b"added and removed and added again data",
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

        // Check the lengths of the files.
        assert_eq!(storage.len(), 2);
        let blobs_len: u64 = BLOBS.iter().map(|b| b.len() as u64).sum::<u64>() +
            // Stored twice in step 3.
            BLOBS[2].len() as u64;
        assert_eq!(storage.data_size(), DATA_MAGIC.len() as u64 + blobs_len);
        let want_entries_len = 4; // Step 3 has two entries.
        assert_eq!(
            storage.index_size(),
            (INDEX_MAGIC.len() + want_entries_len * size_of::<Entry>()) as u64
        );
    }
}

mod validate {
    use std::num::NonZeroUsize;

    use super::{test_data_path, test_entries, validate, DB_008};

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
