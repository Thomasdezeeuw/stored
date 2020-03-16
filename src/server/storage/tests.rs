use std::env::temp_dir;
use std::fs::remove_file;
use std::path::{Path, PathBuf};

use crate::Key;

use super::*;

/// Returns the path to the test data `file`.
fn test_data_path(file: &str) -> PathBuf {
    Path::new("./tests/data/").join(file)
}

// Data stored in "001.db" test data.
const DATA: [&[u8]; 2] = [b"Hello world", b"Hello mars"];

// Entries in "001.db" test data.
fn test_entries() -> [Entry; 2] {
    // `Key::for_value` isn't const, otherwise this could be a constant.
    [
        Entry {
            key: Key::for_value(DATA[0]),
            offset: 0,
            length: DATA[0].len() as u32,
            created: DateTime {
                seconds: 5,
                subsec_nanos: 0,
            },
        },
        Entry {
            key: Key::for_value(DATA[1]),
            offset: DATA[0].len() as u64,
            length: DATA[1].len() as u32,
            created: DateTime {
                seconds: 50,
                subsec_nanos: 100,
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
    let path = temp_dir().join(name);
    TempFile { path }
}

mod date_time {
    use std::mem::size_of;
    use std::ptr;
    use std::time::{Duration, SystemTime};

    use super::DateTime;

    const MARGIN: Duration = Duration::from_millis(1);

    #[test]
    fn now() {
        let now1 = SystemTime::now();
        let now2 = DateTime::now();

        let diff = now1
            .duration_since(now2.into())
            .unwrap_or_else(|err| err.duration());
        assert_eq!(diff.as_secs(), 0);
        assert!(diff.as_nanos() < MARGIN.as_nanos());
    }

    #[test]
    fn from_and_into_sys_time() {
        let tests = [
            (
                DateTime {
                    seconds: 1,
                    subsec_nanos: 0,
                },
                Duration::from_secs(1),
            ),
            (
                DateTime {
                    seconds: u64::MAX / 2,
                    subsec_nanos: 0,
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
        let tests = [DateTime::now(), SystemTime::UNIX_EPOCH.into()];

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

    use super::{temp_file, test_data_path, test_entries, Entry, Index};

    #[test]
    fn entry_size() {
        // Size and layout is fixed.
        assert_eq!(size_of::<Entry>(), 88);
    }

    #[test]
    fn entries() {
        let path = test_data_path("001.db/index");

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
}

mod data {
    use std::mem::ManuallyDrop;
    use std::ptr::NonNull;
    use std::slice;

    use super::{
        mmap, munmap, temp_file, test_data_path, test_entries, Data, MmapArea, DATA, PAGE_SIZE,
    };

    #[test]
    fn in_area() {
        // Don't want to drop the area as the address is invalid to unmap.
        let mut area = ManuallyDrop::new(MmapArea {
            address: NonNull::dangling(),
            offset: 0,
            length: 0,
        });

        let tests: &[((libc::off_t, libc::size_t), (u64, u32), bool)] = &[
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

        for ((offset, length), (value_offset, value_length), want) in tests.iter().copied() {
            area.offset = offset;
            area.length = length;

            let got = area.in_area(value_offset, value_length);
            assert_eq!(got, want);
        }
    }

    #[test]
    fn open_data_file() {
        let path = test_data_path("001.db/data");
        let data = Data::open(&path).unwrap();

        assert_eq!(data.areas.len(), 1);
        let mmap_area = data.areas.first().unwrap();
        assert_eq!(mmap_area.offset, 0);
        assert_eq!(mmap_area.length, DATA.iter().map(|d| d.len()).sum());

        for (i, entry) in test_entries().iter().enumerate() {
            let value_address = data.address_for(entry.offset, entry.length).unwrap();
            let value =
                unsafe { slice::from_raw_parts(value_address.as_ptr(), entry.length as usize) };

            assert_eq!(value, DATA[i]);
        }
    }

    #[test]
    fn add_value_grow_area() {
        let path = temp_file("add_value_grow_area.data");
        let mut data = Data::open(&path).unwrap();
        assert_eq!(data.areas.len(), 0);

        // Adding a first value should create a new area.
        let (offset, address1) = data.add_value(DATA[0]).unwrap();
        assert_eq!(offset, 0);
        let got1 = unsafe { slice::from_raw_parts(address1.as_ptr(), DATA[0].len()) };
        assert_eq!(got1, DATA[0]);

        assert_eq!(data.areas.len(), 1);
        let mmap_area = data.areas.first().unwrap();
        assert_eq!(mmap_area.offset, 0);
        assert_eq!(mmap_area.length, DATA[0].len());

        // Adding a second should grow the existing area.
        let (offset, address2) = data.add_value(DATA[1]).unwrap();
        // Check area grown.
        assert_eq!(
            unsafe { address1.as_ptr().add(DATA[0].len()) },
            address2.as_ptr()
        );
        assert_eq!(offset, DATA[0].len() as u64);
        let got2 = unsafe { slice::from_raw_parts(address2.as_ptr(), DATA[1].len()) };
        assert_eq!(got2, DATA[1]);

        assert_eq!(data.areas.len(), 1);
        let mmap_area = data.areas.first().unwrap();
        assert_eq!(mmap_area.offset, 0);
        assert_eq!(mmap_area.length, DATA.iter().map(|d| d.len()).sum());

        // Original address must still be valid.
        assert_eq!(got1, DATA[0]);
    }

    #[test]
    fn add_value_new_area() {
        let path = temp_file("add_value_new_area.data");
        let mut data = Data::open(&path).unwrap();

        const DATA: &[u8] = &[1; PAGE_SIZE];
        const DATA2: &[u8] = super::DATA[0];

        // Adding a first value should create a new area.
        let (offset, address1) = data.add_value(DATA).unwrap();
        assert_eq!(offset, 0);
        let got1 = unsafe { slice::from_raw_parts(address1.as_ptr(), DATA.len()) };
        assert_eq!(got1, DATA);

        assert_eq!(data.areas.len(), 1);
        let mmap_area = data.areas.first().unwrap();
        assert_eq!(mmap_area.offset, 0);
        assert_eq!(mmap_area.length, DATA.len());

        // Create an new mmaping so that the area created above can't be
        // extended.
        // NOTE: address must be page aligned.
        let want_dummy_address = unsafe { address1.as_ptr().add(DATA.len()).cast() };
        let dummy_length = 100;
        let dummy_address = mmap(
            want_dummy_address,
            dummy_length,
            libc::PROT_READ,
            libc::MAP_PRIVATE | libc::MAP_ANON | libc::MAP_FIXED,
            0,
            0,
        )
        .unwrap();
        assert_eq!(dummy_address, want_dummy_address);

        // Adding a second should grow the existing area.
        let (offset, address2) = data.add_value(DATA2).unwrap();
        // Check new area created.
        assert_ne!(
            unsafe { address1.as_ptr().add(DATA.len()) },
            address2.as_ptr()
        );
        assert_eq!(offset, DATA.len() as u64);
        let got2 = unsafe { slice::from_raw_parts(address2.as_ptr(), DATA2.len()) };
        assert_eq!(got2, DATA2);

        assert_eq!(data.areas.len(), 2);
        let mmap_area = data.areas.first().unwrap();
        assert_eq!(mmap_area.offset, 0);
        assert_eq!(mmap_area.length, DATA.len());
        let mmap_area = &data.areas[1];
        assert_eq!(mmap_area.offset, DATA.len() as libc::off_t);
        assert_eq!(mmap_area.length, DATA2.len());

        // Original address must still be valid.
        assert_eq!(got1, DATA);

        munmap(dummy_address, dummy_length).unwrap();
    }
}
