# ADR-0002: Ordered in-memory index (`BTreeMap`)

**Status:** Accepted

## Context

Classic Bitcask uses a hash table, so it supports only point lookups. Real applications almost always need some form of "list things under a prefix", for example `user:42:*` or `session:*`.

## Decision

Use `BTreeMap<Vec<u8>, ValuePtr>` behind a `parking_lot::RwLock`, and expose `scan(range, limit)` and `scan_prefix(prefix, limit)`.

## Alternatives considered

* **`HashMap`**: O(1) lookups instead of O(log n), but range queries would need a full scan. Rejected: the gap is a few dozen nanoseconds on a path dominated by a ~800 ns `pread`.
* **Concurrent skiplist / sharded maps** (e.g. `crossbeam-skiplist`, `dashmap`): less contention on the index. Not needed yet, because writers are already serialized by the single writer lock and readers share an `RwLock` read guard. To be revisited if profiling shows index contention, which would most likely come after group commit (ADR-0003) raises write concurrency.

## Consequences

* Index work is cheap: a `get` miss, which never touches disk, takes 18 ns. `scan_prefix` over 100 entries takes 42 µs, mostly in its 100 `pread`s.
* `BTreeMap::range` panics on inverted bounds. The engine checks for that and returns an empty result, so the public API cannot panic on user input.
* The prefix upper bound is computed by incrementing the last non-`0xFF` byte, which handles carries and the all-`0xFF` edge case (unit-tested).
* Scans resolve every file handle under the index read lock, then read with no locks held. Writers are blocked only for the in-memory walk, never for disk I/O.
