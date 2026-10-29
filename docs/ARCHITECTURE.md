# Architecture

This document explains how emberkv works and why it is built this way. Each major decision has a short ADR in [`adr/`](adr). The HTTP API is specified in [API.md](API.md), the binary protocol in [PROTOCOL.md](PROTOCOL.md), and deployment in [INTEGRATION.md](INTEGRATION.md).

## Contents

1. [On-disk layout](#on-disk-layout)
2. [Write path](#write-path)
3. [Read path](#read-path)
4. [Recovery](#recovery)
5. [Expiry](#expiry)
6. [Compaction](#compaction)
7. [Checkpoints](#checkpoints)
8. [Concurrency](#concurrency)
9. [Server](#server)
10. [Failure modes](#failure-modes)

## On-disk layout

```text
data/
├── LOCK                          # flock()ed for the lifetime of the engine
├── 00000000000000000004.log      # compaction output (sealed)
├── 00000000000000000005.log      # sealed
└── 00000000000000000006.log      # active, append-only
```

A **segment** is a plain sequence of records. Ids are zero-padded so lexical order equals numeric order. **Replay order is id order, and a later record for a key always wins.** Every crash-safety argument below depends on this one invariant.

```text
+-----------+----------+-------------+---------------+-------------------+---------+-----------+
| crc32 (4) | kind (1) | key_len (4) | value_len (4) | expires_at (0|8)  | key ... | value ... |
+-----------+----------+-------------+---------------+-------------------+---------+-----------+
  kind = op (1 put, 2 delete, 3 batch commit) | 0x40 EXPIRES | 0x80 BATCH
```

The CRC covers everything after itself. Lengths are checked against hard format limits (64 KiB keys, 64 MiB values) *before* allocating, so a corrupted length cannot cause a huge allocation. Records with no flags are byte-identical to v0.1, so old data directories still open.

## Write path

```text
put(k, v, expiry)
  └─ lock writer
       ├─ resolve expiry to an absolute deadline (already past → behave like delete)
       ├─ encode record(s) into a reusable scratch buffer
       ├─ rotate if active + bytes > max_segment_bytes    (fsync old, create new, fsync dir)
       ├─ write_all (one syscall)          ── on failure: truncate back to the record boundary
       ├─ fdatasync if SyncPolicy::Always  ── on failure: POISON (ADR-0003)
       └─ lock index (write) → index[k] = (segment, offset, len, expires_at); update live_bytes
```

The index is updated **while the writer lock is still held**. Without that, two concurrent `put(k)` calls could append in one order and update the index in the opposite order.

The other write paths reuse the same machinery:

* **`delete`** appends a tombstone only if a live value exists. An *expired* value is simply forgotten.
* **`write_batch`** encodes every member with the `BATCH` flag plus a final `COMMIT`, writes them in **one** `write_all`, and installs them under **one** index lock ([ADR-0007](adr/0007-atomic-batches.md)).
* **`update(key, f)`** reads the current entry, calls `f`, and applies its `Mutation`, all under the writer lock. This is the primitive behind counters, rate limits and `If-Match` / `If-None-Match`.

## Read path

```text
get(k)
  ├─ lock index (read): ptr = index[k]; skip if expired; file = readers[ptr.segment] (Arc<File>)
  ├─ unlock
  ├─ pread(file, ptr.offset, ptr.len)      // no shared cursor, no lock
  └─ verify CRC + op + key + expiry, return value
```

Readers never touch the writer lock, and a slow disk read never holds the index lock. The `Arc<File>` is cloned *under* the index lock. That way a concurrent compaction can unlink the segment without breaking an in-flight read: POSIX keeps unlinked files readable through open descriptors. The clock is read only for keys that have an expiry ([ADR-0006](adr/0006-ttl-and-injectable-clock.md)).

## Recovery

`Engine::open`:

1. `flock` the `LOCK` file. A second opener gets `Error::Locked`.
2. Delete `*.log.tmp` files. They are output of a compaction that crashed before its rename.
3. Scan every segment in id order, validating each record. Standalone records are applied immediately. Batch members are buffered until their `COMMIT`. Expired puts count as absent but still shadow older values.
4. If validation fails, or a batch has no commit:
   * in the **newest** segment, treat it as a torn tail from a crash mid-append. Truncate to the end of the last standalone record or committed batch and continue;
   * in any **sealed** segment, fail with `Error::Corruption`. Sealed segments were fsynced before sealing, so damage there means hardware or operator error.
5. Delete segments that are empty.
6. **Always start a fresh active segment**, never appending to a file that existed before `open`. This keeps hard-linked checkpoints safe ([ADR-0009](adr/0009-checkpoints.md)).

The model-based test checks that the stats rebuilt by recovery equal the ones maintained incrementally.

## Expiry

Expiry deadlines live in the record and in the index. Expired keys are invisible immediately. Their space is reclaimed in two lazy steps: the server's sweep (`purge_expired`, every 10 s) drops them from the index, and compaction then drops their records. No tombstones are needed, because an expired record stays expired on any later replay. Time comes from an injectable `Clock`; tests use `ManualClock`.

## Compaction

Overwrites, deletes and expiries leave garbage behind. Compaction rewrites the live records and drops everything else, **while reads and writes continue**.

```text
 time ─────────────────────────────────────────────────────────────────────────▶
 writer:  [seg 1][seg 2][seg 3 active] │ [seg 5 active  ......................]
 compact:                    1. reserve id 4, rotate active → 5
                             2. snapshot live + expired ptrs in segs < 4      (index read lock)
                             3. copy live records → 4.log.tmp (clear BATCH flag), fsync, rename, fsync dir
                             4. publish: add reader 4 → swap ptrs if unchanged → drop expired → drop readers 1..3
                             5. unlink 1, 2, 3 in ascending order, fsync dir
```

| Step | Crash here leaves | Why recovery is still correct |
|---|---|---|
| 1 | an empty seg 5 | removed on next open |
| 3, before rename | `4.log.tmp` | deleted on open; inputs untouched |
| 3, after rename | seg 4 *and* segs 1–3 | 4 duplicates the latest values from 1–3 and replays after them. Writes made during compaction are in 5+. |
| 5, partway | seg 4 and a **suffix** of 1–3 | Oldest-first deletion means a tombstone (always later) outlives the puts it shadows |

Batch members are copied as standalone records, because their commit marker is not copied. Reserving the output id *below* the new active segment is what keeps replay order correct ([ADR-0004](adr/0004-online-compaction.md)).

## Checkpoints

`checkpoint(dest)` holds the compaction lock, seals the active segment, and hard-links every sealed segment into `dest`. The result is a consistent, immediately openable data directory that costs almost no time or space. Sharing files is safe because sealed segments are immutable and `open` never appends to existing files ([ADR-0009](adr/0009-checkpoints.md)).

## Concurrency

| Lock | Type | Guards | Held by |
|---|---|---|---|
| `compaction` | `Mutex<()>` | one compaction or checkpoint at a time | `compact`, `checkpoint` |
| `writer` | `Mutex<Writer>` | active file, offsets, id allocation | `put`, `delete`, `update`, `write_batch`, `sync`, rotations |
| `index` | `RwLock<BTreeMap>` | key → location + expiry | everyone, briefly |
| `readers` | `RwLock<HashMap>` | segment id → `Arc<File>` | lookups, rotation, compaction publish |

**Lock order: `compaction` → `writer` → `index` → `readers`.** Every code path takes locks in this order, so deadlock is impossible. `update`'s closure runs under the writer lock, so it must not call back into the engine's write methods (documented). `purge_expired` and compaction remove expired keys with a compare-and-delete under the index lock, without the writer lock.

## Server

```text
                         ┌──────────────────────── emberkv-server ────────────────────────┐
HTTP clients ──────────▶ │ axum router → request-id, trace, timeout, body limit          │
                         │   Caller extractor (Bearer → Principal) → scope check → handler│
binary clients ────────▶ │ accept loop (Semaphore) → per-conn task → AUTH gate (admin)   │
                         │                     │                                          │
                         │       spawn_blocking(engine op)  ◀── all disk I/O off the     │
                         │                                        async worker threads    │
                         │ background: sync_loop (100 ms) · expiry_loop (10 s)            │
                         │             compaction_loop (30 s, policy-driven)              │
                         └────────────────────────────────────────────────────────────────┘
```

* **Backpressure.** Binary-protocol connections take a permit *before* `accept()`. HTTP bodies are size-limited, and each request has a timeout.
* **Graceful shutdown.** One `CancellationToken` stops the accept loop, flips `/readyz` to 503, lets hyper drain HTTP connections, and tells binary connections to finish their current request. A `TaskTracker` waits up to `shutdown_grace`, then the engine is fsynced.
* **Auth** ([ADR-0008](adr/0008-http-api-and-auth.md)). Token → SHA-256 → principal lookup. HTTP checks namespace and access level per route. The binary protocol requires an admin token. Without an auth file, binding a non-loopback address is refused.

## Failure modes

| Failure | Behavior |
|---|---|
| Power loss mid-append | Torn tail truncated on restart; at most the in-flight write (or one sync interval under `periodic`) is lost |
| Power loss mid-batch | The whole batch is rolled back on restart; none of it is visible |
| `write` fails (e.g. ENOSPC) | File truncated back to the record boundary; error returned; engine stays usable |
| `fsync` fails | Writer **poisoned**: writes fail with `Poisoned`, HTTP returns `503`, `/readyz` turns `503`; reads continue; restart required |
| Bit rot in a value | `get` returns `Corruption`; HTTP returns `500`; details in the log only |
| Corruption in a sealed segment | Startup fails loudly with the segment id and offset |
| Crash during compaction or checkpoint | Recoverable at every step (see tables above) |
| Two processes, one directory | Second gets `Locked` |
| Concurrent writers on one key | `If-Match` makes the loser get `412`; nothing is silently overwritten |
| Client sends garbage | Binary: error frame, then disconnect. HTTP: structured `400` |
| Missing or wrong token | `401`; out-of-scope namespace `403`; failed binary `AUTH` closes the connection |
| Server started on a public address without auth | Refuses to start |
| SIGTERM | Stop accepting, report not-ready, finish in-flight requests, fsync, exit 0 |
