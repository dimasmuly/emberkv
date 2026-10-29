# ADR-0007: Atomic batches via flagged records and a commit marker

**Status:** Accepted

## Context

Real flows touch several keys that must change together: "create the order, write the outbox entry, delete the cart" or "enable these three flags". With independent writes, a crash or a concurrent reader can observe half of the change.

## Decision

A batch is written as **one contiguous append**:

```text
[put k1 | BATCH] [put k2 | BATCH] [delete k3 | BATCH] [COMMIT count=3]
```

* Every member record carries the `BATCH` flag (`0x80`) and its own CRC.
* A final `COMMIT` record (op 3) states how many members precede it.
* The whole batch is encoded into one buffer and written with a single `write_all` under the writer lock. Rotation happens *before* the write, so **a batch never spans segments**.
* The index is updated for all members under **one** index write lock, so concurrent readers see all of the batch or none of it.

**Replay** buffers batch members and applies them only when a matching `COMMIT` arrives:

| What replay finds | Meaning | Action |
|---|---|---|
| Members + matching commit | Committed batch | Apply all |
| Members at end of newest segment, no commit | Crash mid-write | Truncate to before the batch |
| Same, in a sealed segment | Impossible for our writer, so corruption | Refuse to open |
| Commit count ≠ members, or a plain record between members | Corruption | Same rules as any damage (ADR-0003) |

**Compaction** copies live members as *standalone* records by clearing the `BATCH` flag (same length, recomputed CRC). It does not copy the commit marker. Without this, replay would wait forever for a commit that is not in the compacted segment. The model test exercises exactly this, with batches followed by compaction and reopen.

## Alternatives considered

* **Write-ahead "intent" record, then normal records.** This doubles the write volume and needs a cleanup protocol.
* **One "batch" record containing all operations.** It saves headers, but the index would then point *inside* records (sub-offsets), complicating reads, verification and compaction. Rejected in favor of keeping "one key → one record".

## Consequences

* Atomicity covers crashes and concurrent readers. It is not isolation for read-modify-write: two batches built from stale reads can still conflict, and `update` / ETags (ADR-0008) handle that.
* Batch size is bounded by `max_batch_bytes` (32 MiB) and, over HTTP, by 1000 operations. Both are checked before anything is written.
* Deletes inside a batch always write a tombstone, even for absent keys. That costs a little space and keeps the logic obviously correct.
