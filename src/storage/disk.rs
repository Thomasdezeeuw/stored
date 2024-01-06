//! On-disk storage implementation.
//!
//! # Limitations
//!
//! An assumption this implementation makes is that the underlying storage is
//! reliable, that is if bytes are written and synced (using `fsync(2)`) to disk
//! we expect those bytes stored properly and to be returned (read) as is,
//! without corruption. File systems implement various methods to avoid and
//! recover from corruption, for that reason we don't and instead depend on the
//! file system to take of this for us.
//!
//! This implementation does **NOT** work on filesystems that don't implement
//! proper file locking using `flock(2)`. From what I can tell that's mostly
//! network filesystems. Failing to implement `flock(2)` properly can lead to
//! two open stores overwriting their blobs and index entries, most likely
//! leading to corruption.
//!
//! This implementation assumes, possibly incorrectly, that writes are atomic.
//! In other words either the entire write succeeds, and the new data is in
//! place, or fails, and the data remains unchanged. For most Copy On Write
//! (COW) filesystems this should be the case, but it's likely not the case for
//! non-COW filesystems. *This needs more work*.
//!
//! # Design
//!
//! The storage is split into two files: the data file and the index file. Both
//! files are append-only logs [^1]. The index file determines what blobs are
//! actually in the storage, this means that even though the bytes might be in
//! the data file it doesn't mean that those bytes are in the storage. The index
//! file has the final say in what blobs are and aren't in the storage. The idea
//! of splitting the storing of values (blobs) from the keys in different files
//! is taking from the paper [WiscKey: Separating Keys from Values in
//! SSD-conscious Storage] by Lanyue Lu et al.
//!
//! [WiscKey: Separating Keys from Values in SSD-conscious Storage]: https://www.usenix.org/conference/fast16/technical-sessions/presentation/lu
//!
//! [^1]: Exception here is the compaction maintenance routine, which removes
//!       all deleted blobs and index entries from the files. *Note: currently
//!       not implemented*.
//!
//! ## Storing blobs
//!
//! Storing new blobs to the storage is done in two phases.
//!
//! First, by appending the blob's bytes to the data file. We ensure that the
//! blob is fully synced to disk.
//!
//! Second, we add a new entry to the index file, ensuring the entry is also
//! fully synced to disk, and add an entry to the in-memory index.
//!
//! Only after those two steps are completed is a blob considered stored.
//!
//! ## Removing blobs
//!
//! Just like storing blobs, removing blobs is done in two phases.
//!
//! In the first phase the in-memory index is checked if the blob is present, if
//! not we're done quickly. If it is preset we write a "deleted" entry to the
//! index file and sync it to disk.
//!
//! In the second phase we remove the blob from the in-memory index.
//!
//! Note that we do not remove the blob from the data file. This is done
//! seperately in the compaction maintenance routine.
//!
//! ## In case of failures
//!
//! Any storage implementation should be resistant against failures of various
//! kinds.
//!
//! As per the documentation above storing blobs is a two step process. If there
//! is a failure in step one, for example if the application crashes before the
//! all bytes are written to disk, it means that we have bytes in the data file
//! which don't have an entry in the index file. This is fine, the index file
//! determines what blobs are in the storage, and as there is no entry in the
//! index that points to these possibly invalid bytes the storage is not
//! corrupted.
//!
//! If there is a failure is step two we need to make a choose: either dropping
//! a (possibly) corrupt entry or trying to restore it. Currently we'll fail to
//! open the storage.

// TODO: add header with magic bytes to the data and index files and check that when opening.

use std::async_iter::{AsyncIterator, IntoAsyncIterator};
use std::cmp::min;
use std::future::Future;
use std::hash::BuildHasherDefault;
use std::io;
use std::mem::{self, size_of};
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{self, Poll};

use heph::actor::{self, actor_fn};
use heph::actor_ref::rpc::RpcError;
use heph::actor_ref::{ActorRef, RpcMessage};
use heph::future::{ActorFutureBuilder, InboxSize};
use heph::messages::from_message;
use heph::supervisor::SupervisorStrategy;
use heph_rt::fs::File;
use heph_rt::io::{Buf, Read, Write};
use heph_rt::Access;
use left_right::hashmap;
use log::{debug, error, trace};

use crate::io::syscall;
use crate::key::{Key, KeyHasher};
use crate::storage::{self, AddError};

/// Buffer size for reading blob data.
const READ_BLOB_BUF_SIZE: usize = 4096;

/// Open an existing store or create a new one at `path`.
///
/// `path` must be a directory with the `index` and `data` files.
///
/// # Notes
///
/// The already stored blobs are added to the index asynchronously, which means
/// that when this function returns the index is not yet filled.
pub fn open<RT: Access + Clone>(
    rt: RT,
    path: PathBuf,
) -> io::Result<(Handle, impl Future<Output = ()>)> {
    // In-memory index of the stored blobs.
    let (write_index, read_index) = hashmap::with_hasher(BuildHasherDefault::default());
    // Create the writing and reading sides of the storage.
    let w = Writer::open(&rt, path, write_index)?;
    let reader = Arc::new(w.new_reader()?);

    // Actor that handles the writes.
    let (future, writer) = ActorFutureBuilder::new()
        .with_rt(rt)
        .with_inbox_size(InboxSize::MAX)
        .build(writer_supervisor, actor_fn(writer), w)
        .unwrap(); // SAFETY: `NewActor::Error = !` thus can never panic.

    let handle = Handle {
        reader,
        writer,
        index: read_index,
    };

    Ok((handle, future))
}

/// [`Supervisor`] for the [`writer`] actor.
///
/// [`Supervisor`]: heph::supervisor::Supervisor
fn writer_supervisor<A>(err: io::Error) -> SupervisorStrategy<A> {
    error!("storage I/O error: {err}");
    SupervisorStrategy::Stop
}

/// Actor that handles write access to the storage.
async fn writer<RT: Access>(
    mut ctx: actor::Context<WriteRequest, RT>,
    mut writer: Writer,
) -> io::Result<()> {
    trace!("adding existing blobs to in-memory index");
    writer.index.read_entries().await?;

    while let Ok(request) = ctx.receive_next().await {
        // Don't care about about whether or not the other end got the response.
        let _ = match request {
            WriteRequest::Add(msg) => {
                msg.try_handle(|(blob, key)| async { writer.add_blob(blob, key).await })
                    .await?
            }
            WriteRequest::Remove(msg) => {
                msg.try_handle(|key| async { writer.remove_blob(key).await })
                    .await?
            }
        };
    }
    Ok(())
}

/// Message for the [`writer`].
enum WriteRequest {
    /// Add a blob to the storage.
    ///
    /// Returns `Ok(Key)` if the blob was added, `Err(key)` if the blob is
    /// already stored.
    Add(RpcMessage<(Box<[u8]>, Key), Result<Key, Key>>),
    /// Remove blob with `Key` from the storage.
    ///
    /// Returns true if the blob was removed, false if the blob was not in the
    /// storage.
    Remove(RpcMessage<Key, bool>),
}

from_message!(WriteRequest::Add((Box<[u8]>, Key)) -> Result<Key, Key>);
from_message!(WriteRequest::Remove(Key) -> bool);

/// Writing side of the store.
struct Writer {
    data: Data,
    index: Index,
}

impl Writer {
    /// Open storage at `path`.
    ///
    /// If an existing store is to be opened `path` must be a directory with the
    /// `index` and `data` files. If the directory does not exists it will be
    /// created (same goes for the files).
    fn open<RT: Access>(
        rt: &RT,
        path: PathBuf,
        write_index: hashmap::Writer<Key, Entry, BuildHasherDefault<KeyHasher>>,
    ) -> io::Result<Writer> {
        // Ensure the directory exists.
        debug!(path = log::as_display!(path.display()); "creating directory for storage");
        std::fs::create_dir_all(&path)?;

        let data_path = path.join("data");
        trace!(path = log::as_display!(data_path.display()); "opening data file");
        let data_file = open_file(data_path)?;
        let data = Data {
            offset: data_file.metadata()?.len(),
            file: File::from_std(rt, data_file),
        };

        let index_path = path.join("data");
        trace!(path = log::as_display!(index_path.display()); "opening index file");
        let index_file = open_file(index_path)?;
        let index = Index {
            offset: index_file.metadata()?.len(),
            file: File::from_std(rt, index_file),
            index: write_index,
        };

        Ok(Writer { data, index })
    }

    /// Create a new [`Reader`] for this `Writer`.
    fn new_reader(&self) -> io::Result<Reader> {
        let data_file = self.data.file.try_clone()?;
        Ok(Reader { data_file })
    }

    /// Add `blob` to storage.
    async fn add_blob(&mut self, blob: Box<[u8]>, key: Key) -> io::Result<Result<Key, Key>> {
        debug_assert_eq!(key, Key::for_blob(&blob));

        if !self.index.index.contains_key(&key) {
            return Ok(Err(key));
        }

        // First we write the blob to the data file and sync it.
        let length = blob.len() as u64;
        let offset = self.data.write_blob(blob).await?;
        // Second we write an entry to the index file and sync that.
        // Third add the blob entry to the in-memory index and flush it.
        self.index.write_entry(key.clone(), offset, length).await?;

        Ok(Ok(key))
    }

    /// Remove blob with `key`.
    async fn remove_blob(&mut self, key: Key) -> io::Result<bool> {
        self.index.remove_entry(key).await
        // NOTE: we leave the blob in place in the data file. We remove the
        // bytes at a larger stage in compaction.
    }
}

/// Open index or data file.
///
/// Opens the file for reading, writing and locks it.
fn open_file(path: PathBuf) -> io::Result<std::fs::File> {
    // TODO: look at O_DSYNC and O_SYNC.
    // TODO: look at O_DIRECT.
    let file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)?;

    lock_file(&file)?;

    Ok(file)
}

/// Lock `file` using `flock(2)`.
///
/// # Notes
///
/// Once the `file` is dropped it's automatically unlocked. The locked is
/// maintain when the file descriptor is `dup`licated (`try_clone`d).
fn lock_file(file: &std::fs::File) -> io::Result<()> {
    debug!(file = log::as_debug!(file); "locking file");
    loop {
        match syscall!(flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB)) {
            Ok(_) => return Ok(()),
            Err(err) => match err.kind() {
                io::ErrorKind::WouldBlock => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "storage already in use",
                    ))
                }
                io::ErrorKind::Interrupted => continue, // Try again.
                _ => return Err(err),
            },
        }
    }
}

/// Handle to the data file.
///
/// This file contains all the BLOBs. Where the blobs start and end is defined
/// in the [`Index`] file.
struct Data {
    /// Data file opened for reading and writing.
    file: File,
    /// Current offset to write the next blob to.
    offset: u64,
}

impl Data {
    /// Write `blob` to disk and `fsync` it.
    ///
    /// Returns the offset at which the blob was written.
    async fn write_blob(&mut self, blob: Box<[u8]>) -> io::Result<u64> {
        let offset = self.offset;
        let blob = self.file.write_all_at(blob, offset).await?;
        self.offset += blob.len() as u64;
        self.file.sync_all().await?;
        Ok(offset)
    }
}

/// Handle to the index file.
///
/// This file contains the index which references to the BLOBs in the [`Data`]
/// file.
struct Index {
    /// Index file opened for reading and writing in append-only mode.
    file: File,
    /// In-memory index of blobs.
    index: hashmap::Writer<Key, Entry, BuildHasherDefault<KeyHasher>>,
    /// Current offset to write the next entry to.
    offset: u64,
}

impl Index {
    /// Index entry size on disk.
    ///
    /// [`Key`] + offset (`u64`) + length (`u64`).
    const ENTRY_SIZE: usize = size_of::<Key>() + size_of::<u64>() + size_of::<u32>();

    /// Read entries from the index file and add them to the in-memory index.
    async fn read_entries(&mut self) -> io::Result<()> {
        let metadata = self.file.metadata().await?;
        let file_size = metadata.len();

        if file_size % Self::ENTRY_SIZE as u64 != 0 {
            // TODO: validate the storage and attempt to use it anyway?
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "index file seems corrupted, it has additional bytes after the last valid entry",
            ));
        }

        // Add existing entries to the index.
        if file_size != 0 {
            self.index.reserve(file_size as usize / Index::ENTRY_SIZE);

            let mut buf = Vec::with_capacity(2 * 4096);
            loop {
                buf = self.file.read(buf).await?;
                if buf.is_empty() {
                    break;
                }

                let mut left = buf.as_slice();
                while let Some(disk_entry) = DiskEntry::from_disk(left) {
                    let key = disk_entry.key();
                    if disk_entry.is_deleted() {
                        self.index.remove(key);
                    } else {
                        self.index.insert(key, disk_entry.to_entry());
                    }

                    left = &left[size_of::<DiskEntry>()..];
                }

                buf.drain(0..buf.len() - left.len());
            }
        }

        self.index.flush().await;
        Ok(())
    }

    /// Writes a new entry to the index for blob with `key`.
    async fn write_entry(&mut self, key: Key, offset: u64, length: u64) -> io::Result<()> {
        let disk_entry = Box::new(DiskEntry::new(key.clone(), offset, length));
        self.file.write_all_at(disk_entry, self.offset).await?;
        self.offset += size_of::<DiskEntry>() as u64;

        let entry = Entry { offset, length };
        self.index.insert(key, entry);
        self.index.flush().await;

        Ok(())
    }

    /// Writes a new entry to the index that deletes the blob with `key`.
    async fn remove_entry(&mut self, key: Key) -> io::Result<bool> {
        let is_removed = self.index.remove(key.clone()).is_some();
        if is_removed {
            let entry = Box::new(DiskEntry::new_deleted(key));
            self.file.write_all(entry).await?;
            self.index.flush().await;
        }
        Ok(is_removed)
    }
}

/// Entry in the [`Index`] on disk.
#[repr(C)]
struct DiskEntry {
    /// Key (hashmap) of the blob.
    key: Key,
    /// Offset into the data file in big-endian.
    offset: u64,
    /// Length of the blob in bytes in big-endian.
    ///
    /// If this is 0 it means the blob is deleted.
    length: u64,
}

impl DiskEntry {
    /// Returns a new `DiskEntry`.
    const fn new(key: Key, offset: u64, length: u64) -> DiskEntry {
        DiskEntry {
            key,
            offset: offset.to_be(),
            length: length.to_be(),
        }
    }

    /// Returns a new `DiskEntry` that marks `key` as deleted.
    const fn new_deleted(key: Key) -> DiskEntry {
        DiskEntry::new(key, 0, 0)
    }

    /// Returns a reference to a `DiskEntry` in `buf`, in any.
    ///
    /// Returns `None` if `buf` is not large enough.
    fn from_disk(buf: &[u8]) -> Option<&DiskEntry> {
        if buf.len() < size_of::<Self>() {
            return None;
        }
        // SAFETY: the reference to `[u8]` ensure that the address is valid (not
        // null, etc.). We check the length of the slice above to ensure we
        // don't access memory we shouldn't. Both `Key` and `u64` do not have
        // any invalid bit representations, so any slice of bytes is valid as
        // `DiskEntry` (from a UB pov).
        // TODO: does this need alignment?
        Some(unsafe { &*(buf.as_ptr().cast()) })
    }

    fn to_entry(&self) -> Entry {
        Entry {
            offset: u64::from_be_bytes(self.offset.to_ne_bytes()),
            length: u64::from_be_bytes(self.length.to_ne_bytes()),
        }
    }

    fn key(&self) -> Key {
        // NOTE: key has the same layout regardless of big or little endian.
        self.key.clone()
    }

    /// Returns true if the entry is deleted.
    fn is_deleted(&self) -> bool {
        // NOTE: 0 has the same layout on for big and little endian.
        self.length == 0
    }
}

// SAFETY: due to the heap allocation of the `Box` we can ensure that the
// returned pointer is valid and has a static and `Unpin` lifetime.
unsafe impl Buf for Box<DiskEntry> {
    unsafe fn parts(&self) -> (*const u8, usize) {
        let this: &DiskEntry = self;
        // SAFETY: the layout of `DiskEntry` is valid as slice of bytes due to
        // `repr(C)`.
        ((this as *const DiskEntry).cast(), size_of::<Self>())
    }
}

/// Entry in the [`Index`].
#[derive(Clone)]
struct Entry {
    /// Offset into the data file.
    offset: u64,
    /// Length of the blob in bytes.
    length: u64,
}

/// Reading side of the store.
struct Reader {
    /// [`Data`] file, read only.
    ///
    /// NOTE: the file descriptor is duplicated from the writing side fd, so
    /// it's not actually read only, but we threat it as such.
    data_file: File,
}

impl Reader {
    fn try_clone(&self) -> io::Result<Reader> {
        Ok(Reader {
            data_file: self.data_file.try_clone()?,
        })
    }
}

/// Handle to the [`Storage`] that can be send across thread bounds.
///
/// Can be be converted into `Storage` using `Storage::try_from(handle)`.
#[derive(Clone)]
pub struct Handle {
    reader: Arc<Reader>,
    writer: ActorRef<WriteRequest>,
    index: hashmap::Handle<Key, Entry, BuildHasherDefault<KeyHasher>>,
}

/// On-disk storage, pinned to a thread.
#[derive(Clone)]
pub struct Storage {
    // We use a `Rc` instead of `Arc` to avoid contention on the same memory (in
    // the `Arc` counts).
    reader: Rc<Reader>,
    writer: ActorRef<WriteRequest>,
    index: hashmap::Reader<Key, Entry, BuildHasherDefault<KeyHasher>>,
}

/// Turn a [`Handle`] into a [`Storage`].
impl TryFrom<Handle> for Storage {
    type Error = io::Error;

    fn try_from(handle: Handle) -> Result<Storage, Self::Error> {
        Ok(Storage {
            reader: Rc::new(handle.reader.try_clone()?),
            writer: handle.writer,
            index: handle.index.into_reader(),
        })
    }
}

impl storage::Storage for Storage {
    type Blob = BlobRef;

    type Error = RpcError;

    fn len(&self) -> usize {
        self.index.len()
    }

    async fn lookup(&self, key: Key) -> Result<Option<Self::Blob>, Self::Error> {
        Ok(self.index.get_cloned(&key).map(|entry| BlobRef {
            reader: self.reader.clone(),
            entry,
        }))
    }

    async fn contains(&self, key: Key) -> Result<bool, Self::Error> {
        Ok(self.index.contains_key(&key))
    }

    async fn add_blob(&mut self, blob: &[u8]) -> Result<Key, AddError<Self::Error>> {
        let key = Key::for_blob(blob);
        if self.index.contains_key(&key) {
            Err(AddError::AlreadyStored(key))
        } else {
            match self.writer.rpc((blob.into(), key)).await {
                Ok(Ok(key)) => Ok(key),
                Ok(Err(key)) => Err(AddError::AlreadyStored(key)),
                Err(err) => Err(AddError::Err(err)),
            }
        }
    }

    async fn remove_blob(&mut self, key: Key) -> Result<bool, Self::Error> {
        self.writer.rpc(key).await
    }
}

/// Reference to a BLOB (Binary Large OBject) stored on disk.
pub struct BlobRef {
    reader: Rc<Reader>,
    entry: Entry,
}

impl storage::Blob for BlobRef {
    type Buf = Vec<u8>;
    type AsyncIter = BlobBytes<File>;

    fn len(&self) -> usize {
        self.entry.length as usize
    }

    async fn write<H, T, C>(self, header: H, trailer: T, conn: &mut C) -> io::Result<(H, T)>
    where
        H: Buf,
        T: Buf,
        C: Write,
    {
        let header = conn.write_all(header).await?;
        // TODO: optimise this, e.g. by using `sendfile(2)`.
        let mut offset = self.entry.offset;
        let mut left = self.entry.length as usize;
        let mut buf = Vec::with_capacity(min(READ_BLOB_BUF_SIZE, left));
        loop {
            buf = self.reader.data_file.read_at(buf, offset).await?;
            let written = buf.len();
            if written == 0 {
                break;
            }
            offset += written as u64;
            left -= written;

            buf = conn.write_all(buf).await?;

            if left == 0 {
                break;
            }
            buf.clear();
        }
        let trailer = conn.write_all(trailer).await?;
        Ok((header, trailer))
    }
}

impl IntoAsyncIterator for BlobRef {
    type Item = Vec<u8>;
    type IntoAsyncIter = BlobBytes<File>;

    fn into_async_iter(self) -> Self::IntoAsyncIter {
        BlobBytes {
            future: None,
            reader: self.reader,
            offset: self.entry.offset,
            left: self.entry.length,
        }
    }
}

/// Async iterator of a blob's bytes.
#[allow(private_bounds)] // Stupid workaround trait.
pub struct BlobBytes<F: WorkAroundReadAt + 'static = File> {
    /// Reading future.
    ///
    /// NOTE: the `'static` lifetime is wrong, its lifetime is tied to the
    /// `data_file` in `Reader`.
    /// NOTE: due to the above `future` MUST be declared before `reader`.
    future: Option<F::Future<'static>>,
    reader: Rc<Reader>,
    /// Current offset into the data file.
    offset: u64,
    /// Length of the blob left.
    left: u64,
}

impl AsyncIterator for BlobBytes {
    type Item = Vec<u8>;

    fn poll_next(self: Pin<&mut Self>, ctx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        if self.left == 0 {
            return Poll::Ready(None);
        }

        // SAFETY: only `future` is actually `!Unpin`, which we do not move.
        let this = unsafe { Pin::get_unchecked_mut(self) };
        let fut = unsafe {
            Pin::new_unchecked(this.future.get_or_insert_with(|| {
                let buf = Vec::with_capacity(min(READ_BLOB_BUF_SIZE, this.left as usize));
                let fut = WorkAroundReadAt::read_at(&this.reader.data_file, buf, this.offset);
                // SAFETY: this not safe. It's to work around the lifetime
                // issue, see the `future` field.
                mem::transmute(fut)
            }))
        };

        match fut.poll(ctx) {
            Poll::Ready(Ok(buf)) => {
                this.offset += buf.len() as u64;
                this.left -= buf.len() as u64;
                Poll::Ready(Some(buf))
            }
            Poll::Ready(Err(err)) => {
                error!(error = log::as_error!(err), offset = this.offset; "error reading blob from disk");
                this.left = 0;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.left == 0 {
            (0, Some(0))
        } else {
            let reads = self.left as usize / READ_BLOB_BUF_SIZE;
            (reads, Some(reads))
        }
    }
}

/// This trait is a work around for not being able to name the type of the
/// `Future` returned by an `async` function, which means you can't store that
/// future in a structure, which is what we need to do.
trait WorkAroundReadAt {
    type Future<'a>: Future<Output = io::Result<Vec<u8>>> + 'a
    where
        Self: 'a;
    fn read_at<'a>(&'a self, buf: Vec<u8>, offset: u64) -> Self::Future<'a>;
}

impl WorkAroundReadAt for File {
    type Future<'a> = impl Future<Output = io::Result<Vec<u8>>> + 'a;
    fn read_at<'a>(&'a self, buf: Vec<u8>, offset: u64) -> Self::Future<'a> {
        self.read_at(buf, offset)
    }
}
