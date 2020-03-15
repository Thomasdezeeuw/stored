use super::*;

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
    use std::env::temp_dir;
    use std::fs::remove_file;
    use std::io::{Seek, SeekFrom};
    use std::mem::size_of;
    use std::path::{Path, PathBuf};

    use crate::Key;

    use super::{DateTime, Entry, Index};

    #[test]
    fn entry_size() {
        // Size and layout is fixed.
        assert_eq!(size_of::<Entry>(), 88);
    }

    /// Returns the path to the test data `file`.
    fn test_data_path(file: &str) -> PathBuf {
        Path::new("./tests/data/").join(file)
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

    // Entries in "001.index" test data.
    fn test_entries() -> [Entry; 2] {
        const DATA1: &[u8] = b"Hello world";
        const DATA2: &[u8] = b"Hello mars";

        // `Key::for_value` isn't const, otherwise this could be a constant.
        [
            Entry {
                key: Key::for_value(DATA1),
                offset: 0,
                length: DATA1.len() as u32,
                created: DateTime {
                    seconds: 5,
                    subsec_nanos: 0,
                },
            },
            Entry {
                key: Key::for_value(DATA2),
                offset: DATA1.len() as u64,
                length: DATA2.len() as u32,
                created: DateTime {
                    seconds: 50,
                    subsec_nanos: 100,
                },
            },
        ]
    }

    #[test]
    fn entries() {
        let path = test_data_path("001.index");

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
