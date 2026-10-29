# ADR-0006: Key expiry (TTL) in the engine, with an injectable clock

**Status:** Accepted

## Context

The main product use cases (OTPs, sessions, rate-limit windows, idempotency keys) are all *short-lived*. Without TTL, every client must remember to delete keys, and anything it forgets leaks forever.

## Decision

**Expiry is part of the record format.** A put may carry an 8-byte absolute deadline (unix ms), flagged by bit `0x40` of the kind byte. The index keeps the deadline next to each key's location, so `get` decides expiry without touching disk.

**Expiry is lazy, and it never writes.**

* `get` / `scan` treat an expired key as absent.
* A background sweep (`purge_expired`, every 10 s) removes expired keys from the index, which turns their bytes into garbage for the compaction policy.
* Compaction drops expired records instead of copying them.
* Replay treats an expired put as absent, though it still *shadows* older puts of the same key.

No tombstone is ever written for an expiry. That is safe because an expired record stays expired forever: replaying it at any later time gives the same answer. The same ascending-deletion argument as for compaction applies (ADR-0004). A put's older versions live in segments with smaller ids, so they can never outlive it.

**Time is injected.** The engine reads time only through `Options::clock` (`SystemClock` by default). Tests use `ManualClock` to move time explicitly. That is how the model-based test covers expiry deterministically, with no sleeps or flakiness.

**TTL costs nothing when unused.** The clock is read only for keys that *have* an expiry. The first implementation read it on every lookup, which made a `get` miss go from 18 ns to 41 ns. Benchmarks caught the regression, and the fix restored 17 ns.

## Alternatives considered

* **TTL as a value envelope in the server layer**: no format change, but compaction could not drop expired data, and every embedded user of the engine would re-implement it. Rejected.
* **A secondary expiry index** (`BTreeMap<(deadline, key)>`) would make sweeps O(expired) instead of O(keys). It costs another copy of every expiring key in RAM. Deferred: the sweep is a linear walk of an in-memory index once every 10 s, done on the blocking pool without holding the writer lock. It should be measured before it is optimized.

## Consequences

* `len()` and `stats().keys` may include expired keys until the next sweep (documented).
* Expiry follows the wall clock. If the clock jumps backwards, recently expired keys can become visible again until compaction removes them. Run NTP with slewing, as for any TTL store.
* Format compatibility: v0.1 records have no flags, so v0.1 data directories open unchanged.
