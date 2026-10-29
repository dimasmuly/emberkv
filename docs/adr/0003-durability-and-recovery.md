# ADR-0003: Durability policy, fsync failure handling, and recovery rules

**Status:** Accepted

## Context

"Durable" is not one setting. Different users need different points on the latency/safety curve, and the failure behavior of `fsync` itself is subtle.

Measured on the development machine (Apple M2, macOS, where Rust's `sync_data` issues `F_FULLFSYNC`):

| `--sync` | 100% put throughput | p50 |
|---|---|---|
| `always` | 338 req/s | 96 ms |
| `periodic` (100 ms) | 74,000 req/s | 0.38 ms |

## Decisions

### 1. Make durability a policy, not a constant

* Engine: `SyncPolicy::{Always, Never}` plus an explicit `Engine::sync()`. The engine never spawns threads.
* Server: `always | periodic(interval) | never`. `periodic` is the default: a timer calls `sync()`, bounding loss to one interval while giving ~200× the throughput of `always`.
* **Sealed segments are always fsynced** (on rotation and compaction, including the directory). Whatever the policy, only the active segment's tail is ever at risk.
* Graceful shutdown always ends with a final `sync()`.

### 2. Poison the write path when fsync fails

After a failed `fsync`, Linux may have *dropped* the dirty pages and cleared the error, so a retry can "succeed" without the data reaching disk. This is the problem PostgreSQL hit in 2018 ("fsyncgate"). Retrying is therefore unsound.

On any fsync failure the writer sets `poisoned = true`. All further writes return `Error::Poisoned`, which the server maps to `UNAVAILABLE`. Reads keep working, and an operator restart re-establishes the true on-disk state through recovery.

A failed `write` (e.g. ENOSPC) is different: the engine truncates back to the last record boundary and stays usable. It poisons itself only if that truncation also fails.

### 3. Tail truncation vs. fatal corruption

* **Invalid record in the newest segment**: expected after a crash mid-append. Truncate to the last valid record, log a warning with the number of bytes dropped, and continue.
* **Invalid record in a sealed segment**: sealed segments were fsynced, so this cannot be a torn write. It means hardware failure, bit rot, or operator error. **Refuse to start** and report the segment and offset. Skipping the record would silently drop acknowledged writes, and possibly resurrect deleted keys whose tombstone was in the damaged range.

### 4. Verify on read

Every `get` re-checks the CRC, kind and key of the record it reads. The cost is one CRC32 over the record (SIMD-accelerated by `crc32fast`), which is small next to the `pread` itself. In exchange, bit rot surfaces as an error instead of wrong data.

## Consequences

* Operators choose their trade-off explicitly; the default is reasonable and documented.
* The `always` row above is the strongest argument for **group commit** (roadmap): queue concurrent writers, issue one fsync for the batch, and acknowledge all of them. This keeps `always` semantics at close to `periodic` throughput.
