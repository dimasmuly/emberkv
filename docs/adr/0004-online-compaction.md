# ADR-0004: Crash-safe online compaction

**Status:** Accepted

## Context

Without compaction, disk usage grows without bound under overwrites and deletes. Compaction must:

1. not block reads or writes for its duration;
2. leave a correct database if the process dies at *any* instruction;
3. never resurrect deleted keys.

## Decision

A five-phase protocol (details and diagram in [ARCHITECTURE.md](../ARCHITECTURE.md#compaction)):

1. **Seal and reserve.** Under the writer lock, reserve `output_id = next_id`, then rotate the active segment to `output_id + 1`. Every input has an id `< output_id`, and every write made during compaction lands at `> output_id`.
2. **Snapshot.** Under the index read lock, collect `(key, ptr, Arc<File>)` for every live record in an input segment.
3. **Copy.** Write records (re-validating each CRC) to `{output_id}.log.tmp`, `fsync`, `rename` to `.log`, then `fsync` the directory.
4. **Publish.** Register the reader. Swap each index pointer **only if it still equals the snapshot**. Unregister the input readers.
5. **Delete inputs in ascending id order**, stopping at the first failure.

### Why the output id sits *below* the new active segment

Recovery replays in id order and a later record wins. If the output had the highest id, then after a crash a stale compacted value would replay *after* a newer write made during compaction, and that newer write would be lost.

### Why delete oldest-first

A tombstone always lives in a later segment than the put it shadows. Compaction drops both, since a deleted key is absent from the index. If a crash interrupts deletion, oldest-first guarantees that any surviving put still has its (later) tombstone on disk. Deleting in arbitrary order could remove the tombstone and keep the put, bringing a deleted key back to life.

### Why tombstones can be dropped at all

Compaction always takes *every* sealed segment, so no older segment can exist that still holds a put the tombstone needs to shadow.

## Alternatives considered

* **Stop-the-world compaction**: simpler, but stalls all traffic for a time proportional to data size. Rejected.
* **Per-segment compaction by garbage ratio**: less write amplification, but each compacted segment then needs its own position in the replay order (e.g. generation numbers). Deferred to the roadmap. The current design keeps correctness easy to argue.

## Consequences

* Writers are blocked only for the brief rotation in phase 1 and the in-memory pointer swap in phase 4. Readers are never blocked on disk I/O.
* Phase 2 clones every live key while holding the index read lock. That is O(keys) memory and briefly blocks writers. This is acceptable at Bitcask scale (all keys already fit in RAM) and is the first thing to optimize if profiling shows it.
* The design relies on POSIX semantics (unlinked files stay readable through open handles), so Windows is not supported.
* All of this is exercised by the model-based test (random interleavings of compact and reopen) and by the concurrency stress test (compaction looping under live readers and writers).
