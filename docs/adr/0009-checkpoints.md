# ADR-0009: Online checkpoints via hard links

**Status:** Accepted

## Context

A data service needs backups taken **while it serves traffic**, without long pauses or doubling disk usage, and restorable with no special tooling.

## Decision

`Engine::checkpoint(dest)`:

1. Takes the compaction lock, so the set of segment files cannot change.
2. Under the writer lock, **seals the active segment** (fsync, start a new one). Every segment below the new active id is now immutable.
3. **Hard-links** each sealed segment into `dest` (copying and fsyncing instead if `dest` is on another filesystem), then fsyncs `dest`.

Writes continue during step 3; they go to the new active segment, which is not part of the checkpoint. The checkpoint is an ordinary data directory: restoring means `Engine::open(dest)`.

### Why hard links are safe here

A hard link shares the file's contents. That is safe only if neither side ever modifies a shared file:

* Sealed segments are never modified by the live engine. Compaction *unlinks* them, which only removes the live directory's name for the file.
* **`Engine::open` never appends to a pre-existing segment**; it always starts a fresh one. Without this rule, opening a checkpoint and writing to it would append into the very file that the live database (or another checkpoint) also points to. This was a deliberate change in v0.2, and a test proves isolation in both directions.
* Torn-tail truncation on open only ever applies to the newest segment. In a checkpoint that is a sealed, complete file, so it is never truncated.

### Cost of "always start a fresh segment"

Every `open` creates one file and fsyncs the directory. Opening 100k keys went from 33 ms to 38 ms on the development machine. Restarts without writes used to leave empty segments behind; `open` now deletes empty segments, and a test guards this.

## Alternatives considered

* **Copying files**: simple, but O(data) time and space on every backup.
* **Filesystem snapshots** (LVM, ZFS, EBS): excellent, but environment-specific. Hard-link checkpoints are a portable baseline that composes with them: ship the checkpoint directory off-host afterwards.
* **Logical dump (iterate and write JSON)**: portable across format versions, but slow and not point-in-time without extra machinery.

## Consequences

* A checkpoint initially costs almost no extra space; space is shared until compaction rewrites the live copy.
* Checkpoints live on the same disk until shipped elsewhere, so they protect against operator error and corruption, not disk loss. The integration guide says so.
* The HTTP endpoint writes only under `--backup-dir`, with server-generated names.
