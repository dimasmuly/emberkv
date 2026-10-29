use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, BufWriter, Write};
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use tracing::{debug, error, info, warn};

use crate::batch::{BatchOp, WriteBatch};
use crate::clock::Clock;
use crate::error::{Error, Result};
use crate::options::{Options, SyncPolicy};
use crate::record::{self, Op, Spec};
use crate::segment::{self, ScannedRecord, SegmentId};
use crate::types::{CheckpointReport, CompactionReport, EngineStats, Entry, Expiry, Mutation};

const LOCK_FILE: &str = "LOCK";
const FIRST_SEGMENT: SegmentId = 1;
/// `ValuePtr::expires_at` value for keys without an expiry.
const NEVER: u64 = u64::MAX;

/// Location of the latest record for a key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ValuePtr {
    segment: SegmentId,
    offset: u64,
    len: u32,
    expires_at: u64,
}

impl ValuePtr {
    fn bytes(self) -> u64 {
        u64::from(self.len)
    }

    fn is_expired(self, now: u64) -> bool {
        self.expires_at <= now
    }

    fn expiry(self) -> Option<u64> {
        (self.expires_at != NEVER).then_some(self.expires_at)
    }
}

type Index = BTreeMap<Vec<u8>, ValuePtr>;

/// A batch operation after expiry resolution: `(key, Some((value, expires_at)))`
/// for a put, `(key, None)` for a delete.
type ResolvedOp<'a> = (&'a [u8], Option<(&'a [u8], u64)>);

/// A handle to an open database. Cheap to clone; all clones share state and
/// may be used from any number of threads.
///
/// # Concurrency
///
/// * Writers are serialized by a single mutex, which also guarantees that the
///   index is updated in log order.
/// * Readers never take the writer lock: they look up the index under a
///   shared lock and then issue a positional read (`pread`).
/// * Compaction and checkpoints run concurrently with both.
///
/// Lock order (to stay deadlock-free): `compaction` → `writer` → `index` → `readers`.
#[derive(Clone)]
pub struct Engine {
    inner: Arc<Inner>,
}

struct Inner {
    dir: PathBuf,
    options: Options,
    clock: Arc<dyn Clock>,
    index: RwLock<Index>,
    /// Read handles for every segment currently referenced by the index.
    readers: RwLock<HashMap<SegmentId, Arc<File>>>,
    writer: Mutex<Writer>,
    compaction: Mutex<()>,
    /// Bytes of all records in all segments.
    total_bytes: AtomicU64,
    /// Bytes of records the index points to; `total - live` is reclaimable.
    live_bytes: AtomicU64,
    /// Set after an fsync failure; see `Inner::poison`.
    poisoned: AtomicBool,
    /// Held for the lifetime of the engine; the OS releases it on close.
    _lock: File,
}

struct Writer {
    id: SegmentId,
    file: File,
    len: u64,
    next_id: SegmentId,
    scratch: Vec<u8>,
}

impl Engine {
    /// Opens (or creates) a database in `dir`, replaying all segments to
    /// rebuild the in-memory index.
    ///
    /// Writes always go to a fresh segment, so files that existed before
    /// `open` are never appended to. That keeps hard-linked checkpoints safe
    /// to open (see [`Engine::checkpoint`]).
    ///
    /// # Errors
    ///
    /// * [`Error::Locked`] if another engine already owns `dir`.
    /// * [`Error::Corruption`] if a *sealed* segment fails validation. Damage
    ///   at the tail of the newest segment is expected after a crash and is
    ///   repaired by truncation instead.
    pub fn open(dir: impl AsRef<Path>, options: Options) -> Result<Self> {
        options.validate()?;
        let dir = dir.as_ref().to_path_buf();
        let started = Instant::now();
        fs::create_dir_all(&dir)?;
        let lock = acquire_dir_lock(&dir)?;

        let listing = segment::list(&dir)?;
        for stale in &listing.stale_tmp {
            warn!(path = %stale.display(), "removing output of an interrupted compaction");
            fs::remove_file(stale)?;
        }

        let clock = Arc::clone(&options.clock);
        let mut replay = Replay::new(clock.now_ms());
        let mut readers = HashMap::new();
        let newest = listing.segments.last().copied();
        let mut removed_empty = false;

        for &id in &listing.segments {
            let path = segment::path(&dir, id);
            replay.begin_segment();
            let outcome = segment::scan(&path, |rec| replay.visit(id, &rec))?;
            let damage = outcome.damage.or_else(|| replay.unfinished_batch());

            if let Some(damage) = damage {
                if Some(id) != newest {
                    return Err(Error::corruption(id, damage.offset, damage.reason));
                }
                warn!(
                    segment = id,
                    offset = damage.offset,
                    dropped_bytes = outcome.file_len - replay.consistent_len,
                    reason = %damage.reason,
                    "truncating torn tail of the newest segment",
                );
                let file = OpenOptions::new().write(true).open(&path)?;
                file.set_len(replay.consistent_len)?;
                file.sync_all()?;
            }
            // Every open starts a fresh segment, so restarts without writes
            // would otherwise pile up empty files that slow down the next open.
            if fs::metadata(&path)?.len() == 0 {
                fs::remove_file(&path)?;
                removed_empty = true;
                continue;
            }
            readers.insert(id, Arc::new(File::open(&path)?));
        }
        if removed_empty {
            segment::sync_dir(&dir)?;
        }

        let active = newest.map_or(FIRST_SEGMENT, |id| id + 1);
        let (file, reader) = segment::create(&dir, active)?;
        readers.insert(active, Arc::new(reader));
        let writer = Writer { id: active, file, len: 0, next_id: active + 1, scratch: Vec::new() };

        info!(
            dir = %dir.display(),
            keys = replay.index.len(),
            segments = readers.len(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "engine opened",
        );

        Ok(Self {
            inner: Arc::new(Inner {
                dir,
                options,
                clock,
                index: RwLock::new(replay.index),
                readers: RwLock::new(readers),
                writer: Mutex::new(writer),
                compaction: Mutex::new(()),
                total_bytes: AtomicU64::new(replay.total_bytes),
                live_bytes: AtomicU64::new(replay.live_bytes),
                poisoned: AtomicBool::new(false),
                _lock: lock,
            }),
        })
    }

    /// Returns the value stored under `key`, if it exists and has not expired.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.get_entry(key)?.map(|entry| entry.value))
    }

    /// Like [`get`](Engine::get), but also returns the key's expiry.
    pub fn get_entry(&self, key: &[u8]) -> Result<Option<Entry>> {
        self.inner.read_entry(key)
    }

    /// Stores `value` under `key` with no expiry, replacing any previous value.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_with_expiry(key, value, Expiry::Never)
    }

    /// Stores `value` under `key`, expiring it after `ttl`.
    pub fn put_with_ttl(&self, key: &[u8], value: &[u8], ttl: Duration) -> Result<()> {
        self.put_with_expiry(key, value, Expiry::After(ttl))
    }

    /// Stores `value` under `key` with the given expiry. An expiry that is
    /// already in the past behaves like [`delete`](Engine::delete).
    pub fn put_with_expiry(&self, key: &[u8], value: &[u8], expiry: Expiry) -> Result<()> {
        let mut writer = self.inner.writer.lock();
        self.inner.put_locked(&mut writer, key, value, expiry, self.now_ms())
    }

    /// Removes `key`. Returns whether a live (unexpired) value existed.
    pub fn delete(&self, key: &[u8]) -> Result<bool> {
        let mut writer = self.inner.writer.lock();
        self.inner.delete_locked(&mut writer, key, self.now_ms())
    }

    /// Atomically reads `key`, decides what to do with it, and applies that
    /// decision: no other write to any key can happen in between. This is
    /// the building block for counters, rate limiters and compare-and-swap.
    ///
    /// `f` receives the current entry (`None` if absent or expired) and
    /// returns a [`Mutation`] plus any value to hand back to the caller.
    ///
    /// `f` runs while the writer lock is held, so it must be quick and must
    /// not call back into this engine's write methods (that would deadlock).
    ///
    /// ```
    /// use emberkv_engine::{Engine, Expiry, Mutation, Options};
    ///
    /// # fn main() -> emberkv_engine::Result<()> {
    /// # let dir = tempfile::tempdir()?;
    /// let db = Engine::open(dir.path(), Options::default())?;
    /// let increment = |entry: Option<&emberkv_engine::Entry>| {
    ///     let n: u64 = entry.map_or(0, |e| String::from_utf8_lossy(&e.value).parse().unwrap_or(0));
    ///     let value = (n + 1).to_string().into_bytes();
    ///     (Mutation::Put { value, expiry: Expiry::KeepExisting }, n + 1)
    /// };
    /// assert_eq!(db.update(b"visits", increment)?, 1);
    /// assert_eq!(db.update(b"visits", increment)?, 2);
    /// # Ok(())
    /// # }
    /// ```
    pub fn update<T>(&self, key: &[u8], f: impl FnOnce(Option<&Entry>) -> (Mutation, T)) -> Result<T> {
        let inner = &*self.inner;
        let mut writer = inner.writer.lock();
        inner.check_writable()?;
        let now = self.now_ms();
        let current = inner.read_entry(key)?;
        let (mutation, output) = f(current.as_ref());
        match mutation {
            Mutation::Keep => {}
            Mutation::Put { value, expiry } => inner.put_locked(&mut writer, key, &value, expiry, now)?,
            Mutation::Delete => {
                inner.delete_locked(&mut writer, key, now)?;
            }
        }
        Ok(output)
    }

    /// Applies every operation in `batch` atomically. See [`WriteBatch`].
    pub fn write_batch(&self, batch: &WriteBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let inner = &*self.inner;
        let mut size = record::encoded_len(1, 4, false);
        for op in &batch.ops {
            size += match op {
                BatchOp::Put { key, value, .. } => {
                    inner.validate(key, value)?;
                    record::encoded_len(key.len(), value.len(), true)
                }
                BatchOp::Delete { key } => {
                    inner.validate(key, &[])?;
                    record::encoded_len(key.len(), 0, false)
                }
            };
        }
        if size > inner.options.max_batch_bytes {
            return Err(Error::BatchTooLarge { len: size, max: inner.options.max_batch_bytes });
        }

        let mut writer = inner.writer.lock();
        let now = self.now_ms();
        // `None` = delete. Expiries resolve against the pre-batch state; a
        // put that would already be expired is written as a delete.
        let resolved: Vec<ResolvedOp<'_>> = batch
            .ops
            .iter()
            .map(|op| match op {
                BatchOp::Put { key, value, expiry } => {
                    let expires_at = inner.resolve_expiry(*expiry, key, now);
                    (key.as_slice(), (expires_at > now).then_some((value.as_slice(), expires_at)))
                }
                BatchOp::Delete { key } => (key.as_slice(), None),
            })
            .collect();

        inner.append_batch(&mut writer, resolved)
    }

    /// Atomically reads and writes any number of keys: every write in `f`
    /// becomes visible together, crash-safely, and no other write can
    /// happen between `f`'s reads and its writes. It generalizes
    /// [`update`](Engine::update) to several keys, for example "reserve
    /// stock on three items or none".
    ///
    /// All reads and expiries inside `f` use one instant,
    /// [`Transaction::now_ms`]. That instant is taken from the engine's
    /// clock, so a replicated state machine can make it identical on every
    /// replica by driving a [`ManualClock`](crate::ManualClock).
    ///
    /// If `f` returns an error, nothing is written. `f` runs under the writer
    /// lock, so it must be quick and must not call this engine's write
    /// methods (that would deadlock).
    ///
    /// ```
    /// use emberkv_engine::{Engine, Expiry, Options};
    ///
    /// # fn main() -> emberkv_engine::Result<()> {
    /// # let dir = tempfile::tempdir()?;
    /// let db = Engine::open(dir.path(), Options::default())?;
    /// db.put(b"stock:a", b"2")?;
    /// db.put(b"stock:b", b"0")?;
    ///
    /// // Reserve one of each, or nothing.
    /// let reserved = db.transaction(|tx| {
    ///     let mut levels = Vec::new();
    ///     for key in [&b"stock:a"[..], b"stock:b"] {
    ///         let n: u32 = tx.get(key)?.map_or(0, |e| String::from_utf8_lossy(&e.value).parse().unwrap_or(0));
    ///         if n == 0 {
    ///             return Ok(false);
    ///         }
    ///         levels.push((key, n - 1));
    ///     }
    ///     for (key, n) in levels {
    ///         tx.put(key, n.to_string().as_bytes(), Expiry::KeepExisting)?;
    ///     }
    ///     Ok(true)
    /// })?;
    /// assert!(!reserved);
    /// assert_eq!(db.get(b"stock:a")?.as_deref(), Some(&b"2"[..]));
    /// # Ok(())
    /// # }
    /// ```
    pub fn transaction<T>(&self, f: impl FnOnce(&mut Transaction<'_>) -> Result<T>) -> Result<T> {
        let inner = &*self.inner;
        let mut writer = inner.writer.lock();
        inner.check_writable()?;
        let mut tx = Transaction { inner, now: self.now_ms(), writes: BTreeMap::new() };
        let output = f(&mut tx)?;
        let Transaction { now, mut writes, .. } = tx;
        if writes.len() <= 1 {
            // A single record is atomic on its own; skip the batch framing.
            if let Some((key, put)) = writes.pop_first() {
                match put {
                    Some((value, expires_at)) => {
                        inner.put_locked(&mut writer, &key, &value, Expiry::AtUnixMs(expires_at), now)?;
                    }
                    None => {
                        inner.delete_locked(&mut writer, &key, now)?;
                    }
                }
            }
            return Ok(output);
        }
        let mut size = record::encoded_len(1, 4, false);
        for (key, put) in &writes {
            size += record::encoded_len(key.len(), put.as_ref().map_or(0, |(v, _)| v.len()), put.is_some());
        }
        if size > inner.options.max_batch_bytes {
            return Err(Error::BatchTooLarge { len: size, max: inner.options.max_batch_bytes });
        }
        let resolved: Vec<ResolvedOp<'_>> = writes
            .iter()
            .map(|(key, put)| (key.as_slice(), put.as_ref().map(|(v, at)| (v.as_slice(), *at))))
            .collect();
        inner.append_batch(&mut writer, resolved)?;
        Ok(output)
    }

    /// Returns up to `limit` live entries whose keys fall in `range`, in key order.
    pub fn scan_entries<R>(&self, range: R, limit: usize) -> Result<Vec<(Vec<u8>, Entry)>>
    where
        R: RangeBounds<[u8]>,
    {
        if is_empty_range(range.start_bound(), range.end_bound()) || limit == 0 {
            return Ok(Vec::new());
        }
        // Resolve file handles while holding the index lock so a concurrent
        // compaction cannot delete a segment between lookup and read.
        let located = {
            let index = self.inner.index.read();
            index
                .range::<[u8], _>(range)
                .filter(|(_, ptr)| self.inner.is_live(**ptr))
                .take(limit)
                .map(|(key, &ptr)| Ok((key.clone(), ptr, self.inner.reader(ptr.segment)?)))
                .collect::<Result<Vec<_>>>()?
        };
        located
            .into_iter()
            .map(|(key, ptr, file)| {
                let value = read_value(&file, &key, ptr)?;
                Ok((key, Entry { value, expires_at_ms: ptr.expiry() }))
            })
            .collect()
    }

    /// Returns up to `limit` live `(key, value)` pairs in `range`, in key order.
    pub fn scan<R>(&self, range: R, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>>
    where
        R: RangeBounds<[u8]>,
    {
        Ok(self.scan_entries(range, limit)?.into_iter().map(|(key, entry)| (key, entry.value)).collect())
    }

    /// Returns up to `limit` live entries whose keys start with `prefix`.
    pub fn scan_prefix(&self, prefix: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let upper = prefix_upper_bound(prefix);
        let end = match &upper {
            Some(upper) => Bound::Excluded(upper.as_slice()),
            None => Bound::Unbounded,
        };
        self.scan((Bound::Included(prefix), end), limit)
    }

    /// Number of indexed keys. Expired keys count until they are purged by
    /// [`purge_expired`](Engine::purge_expired) or compaction.
    pub fn len(&self) -> usize {
        self.inner.index.read().len()
    }

    /// Whether the index holds no keys.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Removes expired keys from the index and returns how many. Their bytes
    /// become garbage for compaction to reclaim. No tombstones are written:
    /// replay treats an expired record as absent anyway.
    pub fn purge_expired(&self) -> usize {
        let now = self.now_ms();
        let expired: Vec<(Vec<u8>, ValuePtr)> = self
            .inner
            .index
            .read()
            .iter()
            .filter(|(_, ptr)| ptr.is_expired(now))
            .map(|(key, &ptr)| (key.clone(), ptr))
            .collect();
        if expired.is_empty() {
            return 0;
        }
        let mut index = self.inner.index.write();
        expired.into_iter().filter(|(key, ptr)| self.inner.remove_if_unchanged(&mut index, key, *ptr)).count()
    }

    /// Forces all acknowledged writes to stable storage.
    pub fn sync(&self) -> Result<()> {
        let writer = self.inner.writer.lock();
        self.inner.check_writable()?;
        writer.file.sync_data().map_err(|err| self.inner.poison(writer.id, err))
    }

    /// Whether writes are disabled after an fsync failure.
    pub fn is_poisoned(&self) -> bool {
        self.inner.poisoned.load(Ordering::SeqCst)
    }

    /// The engine's current time (milliseconds since the Unix epoch), as used
    /// for expiry decisions.
    pub fn now_ms(&self) -> u64 {
        self.inner.clock.now_ms()
    }

    /// Current statistics. Cheap enough to call from a metrics loop.
    pub fn stats(&self) -> EngineStats {
        EngineStats {
            keys: self.inner.index.read().len(),
            segments: self.inner.readers.read().len(),
            total_bytes: self.inner.total_bytes.load(Ordering::Relaxed),
            live_bytes: self.inner.live_bytes.load(Ordering::Relaxed),
        }
    }

    /// Directory this engine stores its data in.
    pub fn path(&self) -> &Path {
        &self.inner.dir
    }

    /// Merges every sealed segment into one new segment containing only live,
    /// unexpired records, then deletes the inputs. Reads and writes proceed
    /// throughout; only one compaction runs at a time.
    ///
    /// Crash safety (see `docs/adr/0004-online-compaction.md`):
    ///
    /// 1. The output id is reserved *below* the new active segment, so replay
    ///    order still puts newer writes after compacted data.
    /// 2. The output is written to a `.tmp` file, synced, then atomically
    ///    renamed. A crash before the rename leaves only a `.tmp` file, which
    ///    `open` discards.
    /// 3. Inputs are deleted in ascending id order, so a crash midway never
    ///    leaves a `put` on disk whose later tombstone was already deleted.
    pub fn compact(&self) -> Result<CompactionReport> {
        let inner = &*self.inner;
        let _exclusive = inner.compaction.lock();
        let started = Instant::now();

        // Phase 1: seal the active segment and reserve the output id.
        let output_id = {
            let mut writer = inner.writer.lock();
            inner.check_writable()?;
            let output_id = writer.next_id;
            writer.next_id += 1;
            inner.rotate(&mut writer)?;
            output_id
        };

        // Phase 2: snapshot the inputs and the records they still serve.
        let now = self.now_ms();
        let (inputs, live, expired) = {
            let index = inner.index.read();
            let readers = inner.readers.read();
            let mut inputs: Vec<(SegmentId, Arc<File>)> = readers
                .iter()
                .filter(|(id, _)| **id < output_id)
                .map(|(id, file)| (*id, Arc::clone(file)))
                .collect();
            inputs.sort_unstable_by_key(|(id, _)| *id);
            let mut live = Vec::new();
            let mut expired = Vec::new();
            for (key, &ptr) in index.iter().filter(|(_, ptr)| ptr.segment < output_id) {
                if ptr.is_expired(now) {
                    expired.push((key.clone(), ptr));
                } else {
                    live.push((key.clone(), ptr, inner.reader(ptr.segment)?));
                }
            }
            (inputs, live, expired)
        };
        let bytes_before =
            inputs.iter().map(|(_, file)| file.metadata().map(|m| m.len())).sum::<io::Result<u64>>()?;

        // Phase 3: copy live records into the output segment.
        let tmp = segment::tmp_path(&inner.dir, output_id);
        let (moved, bytes_after) = match copy_live_records(&tmp, output_id, live) {
            Ok(copied) => copied,
            Err(err) => {
                let _ = fs::remove_file(&tmp);
                return Err(err);
            }
        };
        let output_path = segment::path(&inner.dir, output_id);
        fs::rename(&tmp, &output_path)?;
        segment::sync_dir(&inner.dir)?;

        // Phase 4: publish. The reader must exist before any pointer refers
        // to it, and inputs are unregistered only after no pointer does.
        inner.readers.write().insert(output_id, Arc::new(File::open(&output_path)?));
        let mut records_moved = 0;
        let mut records_expired = 0;
        {
            let mut index = inner.index.write();
            for (key, old, new) in moved {
                // Skip keys overwritten or deleted while we were copying.
                if let Some(current) = index.get_mut(key.as_slice())
                    && *current == old
                {
                    *current = new;
                    records_moved += 1;
                }
            }
            for (key, ptr) in expired {
                if inner.remove_if_unchanged(&mut index, &key, ptr) {
                    records_expired += 1;
                }
            }
        }
        {
            let mut readers = inner.readers.write();
            for (id, _) in &inputs {
                readers.remove(id);
            }
        }
        inner.total_bytes.fetch_add(bytes_after, Ordering::Relaxed);
        inner.total_bytes.fetch_sub(bytes_before, Ordering::Relaxed);

        // Phase 5: delete inputs, oldest first, stopping at the first failure.
        // Unix keeps unlinked files readable for in-flight readers holding an
        // `Arc<File>`, so this never races with `get`.
        for (id, _) in &inputs {
            if let Err(err) = fs::remove_file(segment::path(&inner.dir, *id)) {
                warn!(segment = id, error = %err, "failed to delete compacted segment; will retry next compaction");
                break;
            }
        }
        segment::sync_dir(&inner.dir)?;

        let report = CompactionReport {
            segments_compacted: inputs.len(),
            records_moved,
            records_expired,
            bytes_before,
            bytes_after,
            duration: started.elapsed(),
        };
        debug!(?report, "compaction finished");
        Ok(report)
    }

    /// Writes a consistent snapshot of the database into `dest`, which must
    /// not exist yet. Open `dest` with [`Engine::open`] to restore it.
    ///
    /// The active segment is sealed first, so the checkpoint consists only of
    /// immutable files. Those are hard-linked (falling back to a copy across
    /// filesystems), which makes a checkpoint nearly free in time and space.
    /// Writes continue during the checkpoint; they land in the new active
    /// segment and are not part of it.
    pub fn checkpoint(&self, dest: impl AsRef<Path>) -> Result<CheckpointReport> {
        let inner = &*self.inner;
        let dest = dest.as_ref();
        let started = Instant::now();
        // Compaction deletes segments; keep the set stable while linking.
        let _exclusive = inner.compaction.lock();
        let active = {
            let mut writer = inner.writer.lock();
            inner.check_writable()?;
            inner.rotate(&mut writer)?;
            writer.id
        };

        fs::create_dir(dest)?;
        let mut ids: Vec<SegmentId> =
            inner.readers.read().keys().copied().filter(|id| *id < active).collect();
        ids.sort_unstable();
        let mut bytes = 0;
        for &id in &ids {
            let (src, dst) = (segment::path(&inner.dir, id), segment::path(dest, id));
            if fs::hard_link(&src, &dst).is_err() {
                fs::copy(&src, &dst)?;
                File::open(&dst)?.sync_all()?;
            }
            bytes += fs::metadata(&dst)?.len();
        }
        segment::sync_dir(dest)?;

        let report = CheckpointReport { segments: ids.len(), bytes, duration: started.elapsed() };
        info!(dest = %dest.display(), ?report, "checkpoint written");
        Ok(report)
    }
}

/// The view handed to [`Engine::transaction`]. Reads see the transaction's
/// own earlier writes; writes are buffered and applied atomically when the
/// transaction function returns `Ok`.
pub struct Transaction<'a> {
    inner: &'a Inner,
    now: u64,
    /// Last write per key: `Some((value, expires_at))` or `None` for delete.
    writes: BTreeMap<Vec<u8>, Option<(Vec<u8>, u64)>>,
}

impl Transaction<'_> {
    /// The instant at which this transaction reads and resolves expiries.
    pub fn now_ms(&self) -> u64 {
        self.now
    }

    /// The live entry under `key`, including this transaction's own writes.
    pub fn get(&self, key: &[u8]) -> Result<Option<Entry>> {
        match self.writes.get(key) {
            Some(Some((value, expires_at))) => {
                Ok(Some(Entry { value: value.clone(), expires_at_ms: expiry_field(*expires_at) }))
            }
            Some(None) => Ok(None),
            None => self.inner.read_entry_at(key, self.now),
        }
    }

    /// Stores `value` under `key` when the transaction commits. An expiry
    /// at or before [`now_ms`](Transaction::now_ms) makes this a delete.
    pub fn put(&mut self, key: &[u8], value: &[u8], expiry: Expiry) -> Result<()> {
        self.inner.validate(key, value)?;
        let now = self.now;
        let expires_at = match expiry {
            Expiry::Never => NEVER,
            Expiry::After(ttl) => now.saturating_add(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX)),
            Expiry::AtUnixMs(at) => at,
            Expiry::KeepExisting => self.get(key)?.and_then(|e| e.expires_at_ms).unwrap_or(NEVER),
        };
        let write = (expires_at > now).then(|| (value.to_vec(), expires_at));
        self.writes.insert(key.to_vec(), write);
        Ok(())
    }

    /// Removes `key` when the transaction commits. Returns whether a live
    /// value existed (as seen by this transaction).
    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        self.inner.validate(key, &[])?;
        let existed = self.get(key)?.is_some();
        self.writes.insert(key.to_vec(), None);
        Ok(existed)
    }
}

impl fmt::Debug for Transaction<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transaction").field("now", &self.now).field("writes", &self.writes.len()).finish()
    }
}

impl fmt::Debug for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Engine")
            .field("dir", &self.inner.dir)
            .field("options", &self.inner.options)
            .finish_non_exhaustive()
    }
}

impl Inner {
    fn validate(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        if key.len() > self.options.max_key_bytes {
            return Err(Error::KeyTooLarge { len: key.len(), max: self.options.max_key_bytes });
        }
        if value.len() > self.options.max_value_bytes {
            return Err(Error::ValueTooLarge { len: value.len(), max: self.options.max_value_bytes });
        }
        Ok(())
    }

    fn check_writable(&self) -> Result<()> {
        if self.poisoned.load(Ordering::SeqCst) { Err(Error::Poisoned) } else { Ok(()) }
    }

    /// After a failed fsync the kernel may have dropped dirty pages, so a
    /// retry could "succeed" without the data being durable. The only safe
    /// response is to refuse further writes until the engine is reopened.
    fn poison(&self, segment: SegmentId, err: io::Error) -> Error {
        error!(error = %err, segment, "fsync failed; disabling writes");
        self.poisoned.store(true, Ordering::SeqCst);
        Error::Io(err)
    }

    fn reader(&self, id: SegmentId) -> Result<Arc<File>> {
        self.readers
            .read()
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::corruption(id, 0, "index refers to an unknown segment"))
    }

    /// Whether `ptr` has not expired. Reads the clock only for keys that have
    /// an expiry, so workloads that never use TTL never pay for it.
    fn is_live(&self, ptr: ValuePtr) -> bool {
        ptr.expires_at == NEVER || ptr.expires_at > self.clock.now_ms()
    }

    fn read_entry(&self, key: &[u8]) -> Result<Option<Entry>> {
        self.read_entry_if(key, |ptr| self.is_live(ptr))
    }

    /// Like `read_entry`, but judges expiry at a fixed instant.
    fn read_entry_at(&self, key: &[u8], now: u64) -> Result<Option<Entry>> {
        self.read_entry_if(key, |ptr| !ptr.is_expired(now))
    }

    fn read_entry_if(&self, key: &[u8], live: impl Fn(ValuePtr) -> bool) -> Result<Option<Entry>> {
        let located = {
            let index = self.index.read();
            match index.get(key) {
                Some(&ptr) if live(ptr) => Some((ptr, self.reader(ptr.segment)?)),
                _ => None,
            }
        };
        located
            .map(|(ptr, file)| Ok(Entry { value: read_value(&file, key, ptr)?, expires_at_ms: ptr.expiry() }))
            .transpose()
    }

    /// Turns an [`Expiry`] into an absolute deadline (`NEVER` for none).
    fn resolve_expiry(&self, expiry: Expiry, key: &[u8], now: u64) -> u64 {
        match expiry {
            Expiry::Never => NEVER,
            Expiry::After(ttl) => now.saturating_add(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX)),
            Expiry::AtUnixMs(at) => at,
            Expiry::KeepExisting => self
                .index
                .read()
                .get(key)
                .filter(|ptr| !ptr.is_expired(now))
                .map_or(NEVER, |ptr| ptr.expires_at),
        }
    }

    fn put_locked(
        &self,
        writer: &mut Writer,
        key: &[u8],
        value: &[u8],
        expiry: Expiry,
        now: u64,
    ) -> Result<()> {
        self.validate(key, value)?;
        let expires_at = self.resolve_expiry(expiry, key, now);
        if expires_at <= now {
            self.delete_locked(writer, key, now)?;
            return Ok(());
        }
        let mut len = 0;
        let (segment, offset) = self.append(writer, |buf| {
            len = record::append(buf, &Spec::put(key, value, expiry_field(expires_at))) as u32;
        })?;
        self.install(&mut self.index.write(), key, Some(ValuePtr { segment, offset, len, expires_at }));
        Ok(())
    }

    fn delete_locked(&self, writer: &mut Writer, key: &[u8], now: u64) -> Result<bool> {
        let current = self.index.read().get(key).copied();
        match current {
            None => Ok(false),
            Some(ptr) if ptr.is_expired(now) => {
                // Already invisible; forgetting it needs no tombstone.
                self.remove_if_unchanged(&mut self.index.write(), key, ptr);
                Ok(false)
            }
            Some(_) => {
                self.append(writer, |buf| {
                    record::append(buf, &Spec::delete(key));
                })?;
                self.install(&mut self.index.write(), key, None);
                Ok(true)
            }
        }
    }

    /// Writes `resolved` as one batch (members plus a commit record) in a
    /// single append, then installs it under one index lock, so readers see
    /// all of it or none of it.
    fn append_batch(&self, writer: &mut Writer, resolved: Vec<ResolvedOp<'_>>) -> Result<()> {
        let mut lens = Vec::with_capacity(resolved.len());
        let (segment, base) = self.append(writer, |buf| {
            for (key, put) in &resolved {
                let spec = match put {
                    Some((value, expires_at)) => Spec::put(key, value, expiry_field(*expires_at)),
                    None => Spec::delete(key),
                };
                lens.push(record::append(buf, &spec.in_batch()) as u32);
            }
            record::append_commit(buf, resolved.len() as u32);
        })?;

        let mut index = self.index.write();
        let mut offset = base;
        for ((key, put), len) in resolved.into_iter().zip(lens) {
            let ptr = put.map(|(_, expires_at)| ValuePtr { segment, offset, len, expires_at });
            self.install(&mut index, key, ptr);
            offset += u64::from(len);
        }
        Ok(())
    }

    /// Encodes records with `encode` and appends them in a single write,
    /// returning the segment and offset of the first byte.
    fn append(&self, writer: &mut Writer, encode: impl FnOnce(&mut Vec<u8>)) -> Result<(SegmentId, u64)> {
        self.check_writable()?;
        writer.scratch.clear();
        encode(&mut writer.scratch);
        let len = writer.scratch.len() as u64;
        // Rotation happens before the write, so a batch never spans segments.
        if writer.len > 0 && writer.len + len > self.options.max_segment_bytes {
            self.rotate(writer)?;
        }

        let offset = writer.len;
        if let Err(err) = writer.file.write_all(&writer.scratch) {
            // Roll back a partial write so the next append starts on a record
            // boundary. If even that fails, the tail is unknown: stop writing.
            return Err(match writer.file.set_len(offset) {
                Ok(()) => err.into(),
                Err(_) => self.poison(writer.id, err),
            });
        }
        if self.options.sync == SyncPolicy::Always {
            writer.file.sync_data().map_err(|err| self.poison(writer.id, err))?;
        }
        writer.len += len;
        self.total_bytes.fetch_add(len, Ordering::Relaxed);
        Ok((writer.id, offset))
    }

    /// Points `key` at `ptr` (or removes it) and keeps `live_bytes` in step.
    fn install(&self, index: &mut Index, key: &[u8], ptr: Option<ValuePtr>) {
        let previous = match ptr {
            Some(ptr) => {
                self.live_bytes.fetch_add(ptr.bytes(), Ordering::Relaxed);
                insert(index, key, ptr)
            }
            None => index.remove(key),
        };
        if let Some(previous) = previous {
            self.live_bytes.fetch_sub(previous.bytes(), Ordering::Relaxed);
        }
    }

    /// Removes `key` only if it still points at `ptr` (a compare-and-delete).
    fn remove_if_unchanged(&self, index: &mut Index, key: &[u8], ptr: ValuePtr) -> bool {
        if index.get(key) != Some(&ptr) {
            return false;
        }
        index.remove(key);
        self.live_bytes.fetch_sub(ptr.bytes(), Ordering::Relaxed);
        true
    }

    /// Seals the active segment and starts a new one with `writer.next_id`.
    fn rotate(&self, writer: &mut Writer) -> Result<()> {
        // Sealed segments are always durable, regardless of the sync policy.
        writer.file.sync_data().map_err(|err| self.poison(writer.id, err))?;
        let id = writer.next_id;
        let (file, reader) = segment::create(&self.dir, id)?;
        self.readers.write().insert(id, Arc::new(reader));
        debug!(sealed = writer.id, active = id, "rotated active segment");
        writer.id = id;
        writer.file = file;
        writer.len = 0;
        writer.next_id = id + 1;
        Ok(())
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if !*self.poisoned.get_mut()
            && let Err(err) = self.writer.get_mut().file.sync_data()
        {
            warn!(error = %err, "failed to sync active segment on close");
        }
    }
}

/// Index state rebuilt while replaying segments during [`Engine::open`].
struct Replay {
    now: u64,
    index: Index,
    total_bytes: u64,
    live_bytes: u64,
    /// Members of the batch being read; applied only when its commit arrives.
    pending: Vec<(Vec<u8>, Option<ValuePtr>, u32)>,
    batch_start: Option<u64>,
    /// End of the last standalone record or committed batch in this segment.
    consistent_len: u64,
}

impl Replay {
    fn new(now: u64) -> Self {
        Self {
            now,
            index: Index::new(),
            total_bytes: 0,
            live_bytes: 0,
            pending: Vec::new(),
            batch_start: None,
            consistent_len: 0,
        }
    }

    fn begin_segment(&mut self) {
        self.pending.clear();
        self.batch_start = None;
        self.consistent_len = 0;
    }

    fn visit(&mut self, segment: SegmentId, rec: &ScannedRecord<'_>) -> Result<(), String> {
        let end = rec.offset + u64::from(rec.len);
        if rec.op == Op::Commit {
            let count = record::commit_count(rec.value) as usize;
            if self.batch_start.is_none() || count != self.pending.len() {
                return Err(format!(
                    "commit for {count} records follows {} batch records",
                    self.pending.len()
                ));
            }
            for (key, ptr, len) in std::mem::take(&mut self.pending) {
                self.total_bytes += u64::from(len);
                self.apply(&key, ptr);
            }
            self.total_bytes += u64::from(rec.len);
            self.batch_start = None;
            self.consistent_len = end;
            return Ok(());
        }

        let ptr = (rec.op == Op::Put).then(|| ValuePtr {
            segment,
            offset: rec.offset,
            len: rec.len,
            expires_at: rec.expires_at.unwrap_or(NEVER),
        });
        if rec.batch {
            self.batch_start.get_or_insert(rec.offset);
            self.pending.push((rec.key.to_vec(), ptr, rec.len));
        } else {
            if self.batch_start.is_some() {
                return Err("record interleaved with an uncommitted batch".into());
            }
            self.total_bytes += u64::from(rec.len);
            self.apply(rec.key, ptr);
            self.consistent_len = end;
        }
        Ok(())
    }

    /// A batch with no commit at the end of a segment was torn by a crash.
    fn unfinished_batch(&self) -> Option<segment::Damage> {
        self.batch_start.map(|offset| segment::Damage { offset, reason: "batch has no commit record".into() })
    }

    fn apply(&mut self, key: &[u8], ptr: Option<ValuePtr>) {
        // An expired put still shadows older values, but is itself invisible.
        let ptr = ptr.filter(|ptr| !ptr.is_expired(self.now));
        let previous = match ptr {
            Some(ptr) => {
                self.live_bytes += ptr.bytes();
                insert(&mut self.index, key, ptr)
            }
            None => self.index.remove(key),
        };
        if let Some(previous) = previous {
            self.live_bytes -= previous.bytes();
        }
    }
}

fn expiry_field(expires_at: u64) -> Option<u64> {
    (expires_at != NEVER).then_some(expires_at)
}

/// Inserts or replaces, allocating a key copy only for new keys.
fn insert(index: &mut Index, key: &[u8], ptr: ValuePtr) -> Option<ValuePtr> {
    match index.get_mut(key) {
        Some(slot) => Some(std::mem::replace(slot, ptr)),
        None => index.insert(key.to_vec(), ptr),
    }
}

fn read_value(file: &File, key: &[u8], ptr: ValuePtr) -> Result<Vec<u8>> {
    let mut buf = vec![0; ptr.len as usize];
    segment::read_exact_at(file, &mut buf, ptr.offset)?;
    let record = record::decode(&buf).map_err(|err| Error::corruption(ptr.segment, ptr.offset, err))?;
    if record.op != Op::Put || record.key != key || record.expires_at.unwrap_or(NEVER) != ptr.expires_at {
        return Err(Error::corruption(ptr.segment, ptr.offset, "index points at an unrelated record"));
    }
    // Reuse the read buffer instead of copying the value out of it.
    let value_start = buf.len() - record.value.len();
    buf.drain(..value_start);
    Ok(buf)
}

type MovedRecord = (Vec<u8>, ValuePtr, ValuePtr);

fn copy_live_records(
    tmp: &Path,
    output_id: SegmentId,
    live: Vec<(Vec<u8>, ValuePtr, Arc<File>)>,
) -> Result<(Vec<MovedRecord>, u64)> {
    let file = OpenOptions::new().write(true).create_new(true).open(tmp)?;
    let mut out = BufWriter::with_capacity(1 << 20, file);
    let mut moved = Vec::with_capacity(live.len());
    let mut buf = Vec::new();
    let mut offset = 0u64;
    for (key, old, file) in live {
        buf.resize(old.len as usize, 0);
        segment::read_exact_at(&file, &mut buf, old.offset)?;
        // Never propagate damaged data into a fresh segment.
        let in_batch =
            record::decode(&buf).map_err(|err| Error::corruption(old.segment, old.offset, err))?.batch;
        // The batch's commit record is not copied, so members become
        // standalone records; otherwise replay would wait for a commit forever.
        if in_batch {
            record::clear_batch_flag(&mut buf);
        }
        out.write_all(&buf)?;
        moved.push((key, old, ValuePtr { segment: output_id, offset, ..old }));
        offset += old.bytes();
    }
    let file = out.into_inner().map_err(io::IntoInnerError::into_error)?;
    file.sync_all()?;
    Ok((moved, offset))
}

fn acquire_dir_lock(dir: &Path) -> Result<File> {
    let file = OpenOptions::new().create(true).truncate(false).write(true).open(dir.join(LOCK_FILE))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(TryLockError::WouldBlock) => Err(Error::Locked(dir.to_path_buf())),
        Err(TryLockError::Error(err)) => Err(err.into()),
    }
}

/// Smallest key strictly greater than every key starting with `prefix`, or
/// `None` if no such key exists (empty prefix or all `0xFF`). Useful for
/// building the exclusive upper bound of a prefix range.
pub fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    while let Some(last) = upper.pop() {
        if last < u8::MAX {
            upper.push(last + 1);
            return Some(upper);
        }
    }
    None
}

/// `BTreeMap::range` panics on inverted ranges; treat them as empty instead.
fn is_empty_range(start: Bound<&[u8]>, end: Bound<&[u8]>) -> bool {
    match (start, end) {
        (Bound::Included(s), Bound::Included(e)) => s > e,
        (Bound::Included(s) | Bound::Excluded(s), Bound::Excluded(e))
        | (Bound::Excluded(s), Bound::Included(e)) => s >= e,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_upper_bound_handles_carry() {
        assert_eq!(prefix_upper_bound(b"abc"), Some(b"abd".to_vec()));
        assert_eq!(prefix_upper_bound(&[0x61, 0xFF]), Some(vec![0x62]));
        assert_eq!(prefix_upper_bound(&[0xFF, 0xFF]), None);
        assert_eq!(prefix_upper_bound(b""), None);
    }

    #[test]
    fn inverted_ranges_are_empty() {
        let (a, b) = (&b"a"[..], &b"b"[..]);
        assert!(is_empty_range(Bound::Included(b), Bound::Included(a)));
        assert!(is_empty_range(Bound::Included(a), Bound::Excluded(a)));
        assert!(!is_empty_range(Bound::Included(a), Bound::Included(a)));
        assert!(!is_empty_range(Bound::Unbounded, Bound::Excluded(a)));
    }
}
