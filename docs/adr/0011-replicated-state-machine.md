# ADR-0011: Writes as deterministic commands; the Raft log stored in emberkv itself

**Status:** Accepted. Network transport and server wiring follow in the next step.

## Context

ADR-0010 provides a Raft core that agrees on a sequence of opaque commands. To replicate the server, three questions have to be answered:

1. **What is a command?** Handlers used to call the engine directly. `update` closures read `now` from the wall clock and decide the result on the spot.
2. **Where does the Raft log live?** It must be durable before any vote or acknowledgement leaves the node.
3. **How does a replica know where it stopped?** Counters and rate limits are not idempotent, so applying an entry twice after a crash is a correctness bug, not just a performance cost.

## Decision

### Every write is a `Command`, applied by one function

`state_machine.rs` defines `Command`: `Put`, `Delete`, `Batch`, `Increment` and `RateLimit`, with TTLs kept *relative*. `apply(tx, command)` executes a command inside an engine **transaction**, a new primitive that performs multi-key read-modify-write atomically at one fixed instant (`Transaction::now_ms`). The HTTP and binary front-ends only parse requests into commands. An `Executor` either applies a command locally (standalone) or proposes it to Raft (replicated). Every write therefore has exactly one implementation. The full HTTP, auth and protocol suites pass unchanged on top of it.

Domain refusals, such as a failed `If-Match`, a non-integer counter or a denied rate limit, are `Outcome`s, not errors. They are deterministic, so every replica reaches the same verdict.

### A replicated clock

On a replica, the data engine's clock is a `ManualClock` that only applied commands move. The leader stamps each proposal with its wall clock, and applying the entry sets `clock = max(clock, stamp)`. As a result:

* TTL deadlines, rate-limit windows and expiry checks inside `apply` are identical on every replica, however late a follower applies the entry.
* Background purges and compaction only drop keys that are expired at the replicated time. A lagging follower therefore never purges a key that a not-yet-applied command still reads.
* The clock is monotonic even when a new leader's wall clock is behind the old leader's.

Verified by mutation: switching `apply` to the local wall clock makes `replicas_apply_identical_state` fail with "replicas diverged".

### The Raft log is an emberkv engine

`LogStore` stores the hard state and `log/{index:020}` entries in a second engine opened with `SyncPolicy::Always`. The log therefore inherits CRC-checked records, torn-tail truncation, poisoning on fsync failure (ADR-0003) and atomic batches. Truncating a suffix and appending the replacement are a single batch. The hard state is written first, so no entry is ever durable without its term.

One `Ready` is persisted with one batch, so with a single fsync. The driver drains every waiting request before it builds a `Ready`, so concurrent writers share that fsync. This gives the group commit the roadmap asked for.

### Exactly-once apply

Each entry's effects and an `applied = (index, clock)` marker are written in **the same transaction**. After a crash the data engine holds some consistent prefix, possibly older than the log because the data engine may use a relaxed fsync policy: the log is the write-ahead log. The replica restarts from its marker and re-applies only later entries. The marker key starts with `\0`, which no namespaced key can.

### One driver thread per node

A dedicated thread owns the Raft core, the log store and the write side of the data engine. For each `Ready` it persists, then sends, then applies, as the core's contract requires. Callers wait on a oneshot keyed by `(index, term)`:

* An entry applied at that index with the same term is the caller's outcome.
* A different term at that index means the proposal was overwritten. The caller gets `Dropped`, which is definitely not applied and safe to retry.
* A leader that loses its quorum keeps the proposal pending, because its fate is genuinely unknown. The caller's timeout decides.

Linearizable reads wait for ReadIndex confirmation, then for the local state machine to reach that index.

## Evidence

`src/replica/tests.rs` runs replicas on real storage over an in-memory transport:

* 300 concurrent mixed commands (puts with TTL, increments, rate limits, batches, deletes). Every replica's full keyspace, including expiry deadlines, ends up byte-identical, and exactly `limit` rate-limited requests pass.
* 50 acknowledged writes, then the leader is stopped, then 50 more through the new leader, then the old leader restarts from disk. All replicas converge on 100.
* A restarted replica applies nothing twice: the 21st increment returns 21.
* Followers refuse writes and reads, naming the leader. An isolated leader never acknowledges a write.
* 20/20 repeated runs pass. Engine transactions joined the model-based test (1,000 random histories with TTL, aborts, compaction and reopen).

## Consequences

* A single-key write in replicated mode costs two records (the effect plus the marker) in one batch. The Raft log adds one fsynced write per batch of requests.
* Reads are served at the wall clock (a key past its deadline is hidden even if the replicated clock lags while idle). Writes are evaluated at the replicated clock. Under clock skew between nodes, a key can therefore be hidden from reads slightly before a write treats it as expired. The gap is bounded by the skew.
* The log is still unbounded until snapshots exist (next steps). Admin compaction and checkpoints remain per-node operations.
