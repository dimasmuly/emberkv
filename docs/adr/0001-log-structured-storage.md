# ADR-0001: Log-structured hash storage (Bitcask model)

**Status:** Accepted

## Context

We need a persistent key-value engine that is (a) correct under crashes, (b) fast for point reads and writes, and (c) small enough that one engineer can reason about every line. The main candidates:

| Design | Writes | Point reads | Range scans | Complexity | Memory |
|---|---|---|---|---|---|
| **B-tree** (in-place pages) | random I/O; needs WAL + page cache | O(log n) I/Os | excellent | high (splits, WAL, checkpoints) | bounded |
| **LSM tree** | sequential | memtable + bloom filters + levels | good | high (levels, compaction strategies, bloom tuning) | bounded |
| **Log + in-memory hash index (Bitcask)** | sequential, 1 syscall | **exactly 1 I/O** | via ordered index (ADR-0002) | **low** | **all keys in RAM** |

## Decision

Use the Bitcask model: append-only segment files with an in-memory index that maps each key to `(segment, offset, len)`.

## Consequences

**Positive**
* Every write is one sequential append, which is the fastest pattern on both SSDs and HDDs.
* Every read is exactly one positional read. Latency is predictable, with no read amplification.
* Crash recovery is simple: the log *is* the database. There is no separate WAL to keep consistent with data pages.
* Compaction is a straightforward copy of live records (ADR-0004).

**Negative (accepted)**
* **All keys must fit in RAM.** Each entry costs the key plus roughly 100 bytes of overhead (the `Vec` header, its heap allocation, the 32-byte location and expiry, and B-tree node slack). 10 M keys of 32 bytes therefore need on the order of 1–1.5 GB. That suits many workloads (sessions, metadata, caches with persistence) and rules out others. A deployment that outgrows this should move to an LSM engine. This trade-off is stated in the README rather than hidden.
* Startup time is proportional to data size, because every segment is replayed. **Mitigation (roadmap):** hint files written at compaction time, holding just `(key, location)`, to make startup proportional to the number of keys instead of total bytes.
* Space amplification grows with overwrite rate until compaction runs. This is measured by `fragmentation()` and acted on by the server's compaction policy.
