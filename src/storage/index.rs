//! In-memory index.
//!
//! The implementation is effectively a left-right hash map based on Hash Array
//! Mapped Trie (HAMT). Some resources:
//! * <https://en.wikipedia.org/wiki/Hash_array_mapped_trie>,
//! * <https://idea.popcount.org/2012-07-25-introduction-to-hamt>,
//! * Ideal Hash Trees by Phil Bagwell
//! * Fast And Space Efficient Trie Searches by Phil Bagwell
//!
//! The structure is implemented as a tree with `N_BRANCHES` (16) pointers on
//! each level. A pointer can point to a `Branch`, which again contains multiple
//! pointers, or point to [`Entry`].
//!
//! Indexing into the structure is done using the [`Key`] of the blob, the bytes
//! are used as the index of the branch `LEVEL_SHIFT` (4) bits at the time.
//!
//! Since the read operations block the writes from being applied the [`Index`]
//! (reading side) can get one cloned entry (via [`Index::entry`]) at a time. To
//! create a consistent via it's possible to create a [`Snapshot`] (via
//! [`Index::snapshot`]), which does not block the writer (it has it's own
//! lifetime).
//!
//! The main types are [`Index`] and [`Writer`], creation is done via [`new`].

// TODO: look into replacing `Arc` with our own reference counting, removing the
// weak count *might* provided performance benefits, or it might not.

use std::fmt;
use std::marker::PhantomData;
use std::mem::replace;
use std::ptr::NonNull;
use std::sync::Arc;

use left_right::operation::OverwriteOperation;

use crate::key::Key;

/// Number of bits to shift per level.
const LEVEL_SHIFT: usize = 4;
/// Number of branches per level of the tree, must be a power of 2.
const N_BRANCHES: usize = 1 << LEVEL_SHIFT; // 16
/// Number of bits to mask per level.
const LEVEL_MASK: usize = (1 << LEVEL_SHIFT) - 1;

/// Create a new index.
pub fn new<B>() -> (Writer<B>, Handle<B>) {
    let root = Arc::new(Root {
        root: Branch::new(),
        length: 0,
    });
    let (writer, handle) = left_right::new_cloned(root);
    (Writer { writer }, Handle { handle })
}

/// Writing side of the index.
///
/// # Notes
///
/// All changes made have to be [flushed] before they become visible to the
/// readers.
///
/// [flushed]: Writer::flush_changes
pub struct Writer<B> {
    writer: left_right::Writer<Arc<Root<B>>, OverwriteOperation<Arc<Root<B>>>>,
}

impl<B> Writer<B> {
    /// Add `blob` to the index.
    pub fn add_blob(&mut self, key: Key, blob: B) -> Result<Key, Key> {
        // If the blob is already stored we're done quickly.
        if self.writer.entry(&key).is_none() {
            return Err(key);
        }

        // Get our own copy of the root that we can freely modify.
        let mut root = self.writer.clone();
        let result = Arc::make_mut(&mut root).add_blob(key, blob);
        if result.is_ok() {
            self.writer.apply(OverwriteOperation::new(root));
        }
        result
    }

    /// Remove blob with `key`.
    pub fn remove_blob(&mut self, key: &Key) -> bool {
        // If the blob is not stored we're done quickly.
        if self.writer.entry(key).is_none() {
            return false;
        }

        // Get our own copy of the root that we can freely modify.
        let mut root = self.writer.clone();
        let removed = Arc::make_mut(&mut root).remove_blob(key);
        if removed {
            self.writer.apply(OverwriteOperation::new(root));
        }
        removed
    }

    /// Flush all previously applied changes so that the readers can see them
    pub async fn flush_changes(&mut self) {
        self.writer.flush().await
    }
}

/// Handle to the [`Index`] that can be send across thread bounds.
///
/// Can be be converted into `Index` using `Index::from(handle)`.
#[derive(Clone)]
pub struct Handle<B> {
    handle: left_right::Handle<Arc<Root<B>>>,
}

/// In-memory blob index.
pub struct Index<B> {
    reader: left_right::Reader<Arc<Root<B>>>,
}

impl<B> Index<B> {
    /// Returns the number of blobs in the index.
    pub fn len(&self) -> usize {
        // SAFETY: we're ensuring that we're the only reader in this type.
        unsafe { self.reader.read().length }
    }

    /// Create a consistent snapshot of the index.
    pub fn snapshot(&self) -> Snapshot<B> {
        let root = self.reader.clone_value();
        Snapshot { root }
    }

    /// Lookup the entry with `key`.
    pub fn entry(&self, key: &Key) -> Option<Arc<Entry<B>>> {
        // SAFETY: we're ensuring that we're the only reader in this type.
        let root = unsafe { self.reader.read() };
        match root.entry(key) {
            Some(entry) => unsafe {
                // SAFETY: we increment the strong count so that we can use
                // `Arc::from_raw` without deallocating the original entry.
                // We also ensure that the pointer comes from `Arc::into_raw`.
                Arc::increment_strong_count(entry);
                Some(Arc::from_raw(entry))
            },
            None => None,
        }
    }
}

/// Turn a [`Handle`] into a [`Index`].
impl<B> From<Handle<B>> for Index<B> {
    fn from(handle: Handle<B>) -> Index<B> {
        Index {
            reader: handle.handle.into_reader(),
        }
    }
}

/// Snapshot of an [`Index`].
#[derive(Debug)]
pub struct Snapshot<B> {
    root: Arc<Root<B>>,
}

impl<B> Snapshot<B> {
    /// Returns the number of blobs in the index.
    pub fn len(&self) -> usize {
        self.root.length
    }

    /// Lookup the entry with `key`.
    pub fn entry(&self, key: &Key) -> Option<&Entry<B>> {
        self.root.entry(key)
    }

    /// Lookup the blob with `key`.
    pub fn lookup(&self, key: &Key) -> Option<&B> {
        match self.root.entry(key) {
            Some(entry) => Some(&entry.blob),
            None => None,
        }
    }
}

impl<B> Clone for Snapshot<B> {
    fn clone(&self) -> Self {
        Snapshot {
            root: self.root.clone(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.root.clone_from(&source.root);
    }
}

#[derive(Debug)]
struct Root<B> {
    root: Branch<B>,
    length: usize,
}

impl<B> Root<B> {
    fn entry(&self, key: &Key) -> Option<&Entry<B>> {
        let mut branch = &self.root;
        for idx in key_indices(key) {
            // SAFETY: masking so that we always have a valid index.
            match &branch.branches[idx] {
                Some(ptr) => {
                    if let Some(b) = ptr.as_branch() {
                        branch = b;
                    } else {
                        // SAFETY: not a branch, so must be an entry.
                        let entry = ptr.as_entry().unwrap();
                        return (entry.key == *key).then(|| entry);
                    }
                }
                None => return None,
            }
        }
        None
    }

    /// Add `blob` to the index.
    pub fn add_blob(&mut self, key: Key, blob: B) -> Result<Key, Key> {
        debug_assert!(self.entry(&key).is_none());
        let mut current = &mut self.root;
        let mut indices = key_indices(&key).enumerate();
        while let Some((n, idx)) = indices.next() {
            // SAFETY: masking so that we always have a valid index.
            match &mut current.branches[idx] {
                Some(ptr) if ptr.is_branch() => {
                    // Get our own copy we can safely modify.
                    // SAFETY: check the index and branch above, so unwraps
                    // are safe.
                    current = current.branches[idx]
                        .as_mut()
                        .unwrap()
                        .as_branch_mut()
                        .unwrap();
                }
                Some(ptr) => {
                    // Entry with the same index.
                    // Replace the entry with a new branch.
                    let existing_entry = replace(ptr, Arc::new(Branch::new()).into());
                    // SAFETY: just set the branch, so these unwraps are safe.
                    current = current.branches[idx]
                        .as_mut()
                        .unwrap()
                        .as_branch_mut()
                        .unwrap();

                    // Get an index iterator for the existing entry, and do some
                    // debug checks.
                    let existing_key = &existing_entry.as_entry().unwrap().key;
                    debug_assert_ne!(*existing_key, key);
                    let mut existing_indices = key_indices(existing_key);
                    let _ = existing_indices.advance_by(n);
                    let existing_idx = existing_indices.next();
                    debug_assert_eq!(existing_idx, Some(idx));
                    debug_assert_eq!(existing_indices.size_hint(), indices.size_hint());

                    // With the index iterator of the existing and new entries
                    // find an index where they are different and build up
                    // branch up to that point.
                    // NOTE: this will fail with both keys are the same.
                    let mut both_indices = std::iter::zip(&mut indices, &mut existing_indices);
                    while let Some(((_, idx), existing_idx)) = both_indices.next() {
                        if idx == existing_idx {
                            // Same index, so add a new branch and continue with
                            // that.
                            // SAFETY: setting the branch, so the unwrap is
                            // safe.
                            current = current.branches[idx]
                                .insert(Arc::new(Branch::new()).into())
                                .as_branch_mut()
                                .unwrap();
                            continue;
                        }

                        // Rustc can't figure out we don't need these any more,
                        // but they do cause lifetime issues, we have to
                        // manually drop them to fix those.
                        drop(both_indices);
                        drop(existing_indices);
                        drop(indices);

                        // Add the existing entry.
                        current.branches[existing_idx] = Some(existing_entry.into());
                        // Add the new entry.
                        let entry = Entry {
                            key: key.clone(),
                            blob,
                        };
                        current.branches[idx] = Some(Arc::new(entry).into());
                        return Ok(key);
                    }

                    // If we hit this it means that the key for the new entry
                    // and the existing entry where the same, which should never
                    // happen.
                    unreachable!();
                }
                None => {
                    let entry = Entry {
                        key: key.clone(),
                        blob,
                    };
                    current.branches[idx] = Some(Arc::new(entry).into());
                    drop(indices);
                    return Ok(key);
                }
            }
        }

        drop(indices);
        Err(key)
    }

    /// Remove blob with `key`.
    ///
    /// Returns true if the blob was removed, false if the blob wasn't stored.
    fn remove_blob(&mut self, key: &Key) -> bool {
        let mut current = &mut self.root;
        for idx in key_indices(key) {
            // SAFETY: masking so that we always have a valid index.
            match &mut current.branches[idx] {
                Some(ptr) if ptr.is_branch() => {
                    // Get our own copy we can safely modify.
                    // SAFETY: check the index and branch above, so unwraps
                    // are safe.
                    current = current.branches[idx]
                        .as_mut()
                        .unwrap()
                        .as_branch_mut()
                        .unwrap();
                }
                Some(ptr) => {
                    // SAFETY: not a branch, so must be an entry.
                    let entry = ptr.as_entry().unwrap();
                    if entry.key == *key {
                        // Remove the blob.
                        current.branches[idx] = None;
                        return true;
                    } else {
                        // Blob not stored, no changes needed.
                        return false;
                    }
                }
                // Blob not stored, no changes needed.
                None => return false,
            }
        }
        false
    }
}

/// Iterator for the indices for `key`.
fn key_indices<'a>(key: &'a Key) -> impl Iterator<Item = usize> + 'a {
    let iter = key.as_bytes().into_iter().flat_map(|b| {
        [
            (*b as usize) & LEVEL_MASK,
            ((*b as usize) >> LEVEL_SHIFT) & LEVEL_MASK,
        ]
    });
    debug_assert_eq!(iter.size_hint(), (2 * Key::LENGTH, Some(2 * Key::LENGTH)));
    iter
}

impl<B> Clone for Root<B> {
    fn clone(&self) -> Self {
        Root {
            root: self.root.clone(),
            length: self.length.clone(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.root.clone_from(&source.root);
        self.length.clone_from(&source.length);
    }
}

struct Branch<B> {
    branches: [Option<Pointer<B>>; N_BRANCHES],
}

impl<B> Branch<B> {
    const fn new() -> Branch<B> {
        Branch {
            branches: [
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None,
            ],
        }
    }
}

impl<B> Clone for Branch<B> {
    fn clone(&self) -> Self {
        Branch {
            branches: self.branches.clone(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.branches.clone_from(&source.branches);
    }
}

impl<B: fmt::Debug> fmt::Debug for Branch<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entry(&"0000", &self.branches[0])
            .entry(&"0001", &self.branches[1])
            .entry(&"0010", &self.branches[2])
            .entry(&"0011", &self.branches[3])
            .entry(&"0100", &self.branches[4])
            .entry(&"0101", &self.branches[5])
            .entry(&"0110", &self.branches[6])
            .entry(&"0111", &self.branches[7])
            .entry(&"1000", &self.branches[8])
            .entry(&"1001", &self.branches[9])
            .entry(&"1010", &self.branches[10])
            .entry(&"1011", &self.branches[11])
            .entry(&"1100", &self.branches[12])
            .entry(&"1101", &self.branches[13])
            .entry(&"1110", &self.branches[14])
            .entry(&"1111", &self.branches[15])
            .finish()
    }
}

/// [`Index`] entry.
#[derive(Debug, Clone)]
pub struct Entry<B> {
    pub key: Key,
    pub blob: B,
}

/// Tagged pointer to either a [`Branch`] or [`Entry`], both contained in an
/// [`Arc`].
struct Pointer<B> {
    /// SAFETY: this pointer MUST come from `Arc::into_raw`.
    tagged_ptr: NonNull<()>,
    _phantom_data: PhantomData<*const B>,
}

/// Number of bits used for the tag in `Pointer`.
const POINTER_TAG_BITS: usize = 1;
const POINTER_TAG_MASK: usize = !(1 << POINTER_TAG_BITS);
/// Tags used for the `Pointer`.
const BRANCH_TAG: usize = 0b0;
const ENTRY_TAG: usize = 0b1;

impl<B> Pointer<B> {
    /// Returns `true` is the tagged pointer points to an [`Entry`].
    fn is_entry(&self) -> bool {
        (self.tagged_ptr.as_ptr().addr() & ENTRY_TAG) != 0
    }

    /// Returns `true` is the tagged pointer points to an [`Branch`].
    fn is_branch(&self) -> bool {
        (self.tagged_ptr.as_ptr().addr() & BRANCH_TAG) != 0
    }

    fn as_entry(&self) -> Option<&Entry<B>> {
        if self.is_entry() {
            // SAFETY: Just checked that the pointer is of the correct type.
            Some(unsafe { &*self.as_ptr().cast() })
        } else {
            None
        }
    }

    fn as_branch(&self) -> Option<&Branch<B>> {
        if !self.is_entry() {
            // SAFETY: Just checked that the pointer is of the correct type.
            Some(unsafe { &*self.as_ptr().cast() })
        } else {
            None
        }
    }

    /// This clones the `Arc` internally.
    fn as_branch_mut(&mut self) -> Option<&mut Branch<B>> {
        if let Some(branch) = self.as_branch() {
            *self = Arc::new(branch.clone()).into();
            // SAFETY: just written a branch to `self`, to which we have unique
            // access.
            Some(unsafe { &mut *self.as_ptr().cast() })
        } else {
            None
        }
    }

    /// Returns the raw pointer without its tag.
    fn as_ptr(&self) -> *mut () {
        self.tagged_ptr
            .as_ptr()
            .map_addr(|addr| addr & !POINTER_TAG_MASK)
    }
}

impl<B> From<Arc<Branch<B>>> for Pointer<B> {
    fn from(branch: Arc<Branch<B>>) -> Pointer<B> {
        let ptr = Arc::into_raw(branch);
        assert!(core::mem::align_of::<Branch<B>>() > POINTER_TAG_BITS);
        let ptr = ptr.map_addr(|addr| addr | BRANCH_TAG).cast_mut().cast();
        Pointer {
            tagged_ptr: unsafe { NonNull::new_unchecked(ptr) },
            _phantom_data: PhantomData,
        }
    }
}

impl<B> From<Arc<Entry<B>>> for Pointer<B> {
    fn from(entry: Arc<Entry<B>>) -> Pointer<B> {
        let ptr = Arc::into_raw(entry);
        assert!(core::mem::align_of::<Entry<B>>() > POINTER_TAG_BITS);
        let ptr = ptr.map_addr(|addr| addr | ENTRY_TAG).cast_mut().cast();
        Pointer {
            tagged_ptr: unsafe { NonNull::new_unchecked(ptr) },
            _phantom_data: PhantomData,
        }
    }
}

impl<B> Clone for Pointer<B> {
    fn clone(&self) -> Self {
        let ptr = self.as_ptr();
        // SAFETY: checked the type.
        if self.is_entry() {
            unsafe { Arc::<Entry<B>>::increment_strong_count(ptr.cast()) };
        } else {
            unsafe { Arc::<Branch<B>>::increment_strong_count(ptr.cast()) };
        }
        Pointer {
            tagged_ptr: self.tagged_ptr,
            _phantom_data: PhantomData,
        }
    }
}

impl<B: fmt::Debug> fmt::Debug for Pointer<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ptr = self.as_ptr();
        if self.is_entry() {
            unsafe { &*ptr.cast::<Entry<B>>() }.fmt(f)
        } else {
            unsafe { &*ptr.cast::<Branch<B>>() }.fmt(f)
        }
    }
}

impl<B> Drop for Pointer<B> {
    fn drop(&mut self) {
        let ptr = self.as_ptr();
        if self.is_entry() {
            drop(unsafe { Arc::<Entry<B>>::from_raw(ptr.cast()) })
        } else {
            drop(unsafe { Arc::<Branch<B>>::from_raw(ptr.cast()) })
        }
    }
}
