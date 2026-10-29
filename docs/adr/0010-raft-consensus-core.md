# ADR-0010: A sans-IO Raft core, verified by deterministic simulation

**Status:** Accepted (consensus core). Wiring it into the server is the next step.

## Context

A single emberkv node is a single point of failure ([README](../../README.md#limitations--roadmap)). Kiln ERP's sessions fail closed when emberkv is down, so an emberkv restart signs every user out of work for its duration. Replicating every write to a majority of nodes removes that outage. A follower can take over within one election timeout, without losing an acknowledged write.

Consensus bugs rarely appear in example-based tests. They need a particular interleaving of message loss, delay, partition and crash, which a test on real sockets and timers can neither produce on purpose nor reproduce once seen.

## Decision

**Write Raft ourselves, as a pure state machine** (`crates/emberkv-raft`), in the style of etcd's `raft` package:

* The core performs no I/O and reads no clock. Inputs are `tick()`, `step(message)`, `propose(command)` and `read_index(ctx)`. All output is collected into a `Ready`: what to persist, what to send, what to apply, which reads are confirmed. The driver carries it out in that order. Persisting before sending is what makes a vote or an acknowledgement durable before anyone relies on it.
* **Elections** use randomized timeouts. Two extensions from Ongaro's thesis are included:
  * **Pre-vote** (§9.6): before incrementing its term, a candidate asks whether it could win. A node returning from a partition therefore cannot depose a healthy leader by arriving with an inflated term.
  * **Check-quorum**: a leader that has not heard from a majority within an election timeout steps down. While a follower has a live leader, it also ignores vote requests.
* **Replication** tracks each follower in one of two states. In *probe*, the leader sends one append per heartbeat and walks back on rejection, skipping a whole conflicting term per round trip. In *replicate*, it pipelines optimistically, bounded by `max_inflight_entries`. Heartbeats in replicate state anchor at the acknowledged index, so in-flight entries never cause spurious rejections. A follower that stops acknowledging falls back to probe.
* A new leader appends a **no-op**. Commitment is decided only for entries of the leader's own term (§5.4.2, "Figure 8"). The no-op commits all earlier entries indirectly.
* **Linearizable reads use ReadIndex** (§6.4). The leader records its commit index and confirms with a quorum of heartbeats that it still leads, with no log write. The read is served once the state machine has applied that index. All reads that arrive before the next `Ready` share one heartbeat round. Reads that arrive before the leader commits in its own term wait for that commit.
* **Membership is static.** Joint consensus and snapshots are separate steps (see Consequences).

**Verification is a deterministic simulator** (`tests/simulation.rs`). Each seed draws a 3- or 5-node cluster and a fault profile:

* message drop (up to 25%), duplication, delay and reordering;
* repartitions, including isolating the current leader;
* crash and restart, keeping only what was persisted.

Throughout the run, clients propose and read. After every step the simulator checks:

* election safety;
* one vote per term;
* leader completeness;
* state machine safety, with apply order and no gaps;
* log matching;
* that no read is served below a write already acknowledged when the read was issued.

Then the faults stop. The cluster must elect one stable leader and apply every later proposal on every node. Everything derives from the seed, including the nodes' timers, so a failure prints a one-line reproduction.

## Evidence

* 5,000 seeds in about 10 s (release build). They cover about 25,000 elections, 2.2 million commits, 690,000 confirmed reads, 17,600 crashes and 32 million messages.
* **Mutation testing**: each of these classic bugs was injected on its own, and every one was caught within 3,000 seeds:

| Injected bug | Caught by |
|---|---|
| Commit older-term entries by counting replicas (Figure 8) | stale read |
| Vote for candidates with stale logs | leader completeness |
| Vote twice in one term | one-vote-per-term check |
| Serve reads before committing in the leader's own term | stale read |
| Confirm reads without a quorum | stale read |
| Follower commit not clamped to verified entries | core panic (commit past log end) |
| Truncate entries that already match | core safety assertion |
| Count the candidate's own vote twice | leader completeness |

* `tests/scenarios.rs` pins down behaviour in readable form: pre-vote, check-quorum step-down, read failover and restart from persisted state. With pre-vote and check-quorum switched off, three of these tests fail. That confirms the tests depend on those features.

## Alternatives considered

* **`openraft` or `raft-rs`.** Both are mature, and either would be the right call for a product on a deadline. Using one would leave the most interesting part of this project, and the part most worth demonstrating, as somebody else's code.
* **Primary/backup with asynchronous replication.** It is simpler, but it loses acknowledged writes on failover and needs an external arbiter to avoid split-brain. Both are unacceptable for sessions and idempotency keys.
* **Tests on real sockets with fault injection (Jepsen style).** This is valuable as a final check, and it is planned against the integrated server. As the primary tool it is too slow and not reproducible.

## Consequences

* The log lives in memory until the driver persists it. Without **snapshots and log compaction** it grows without bound, so snapshots (built on engine checkpoints, ADR-0009) are required before production use.
* **Membership changes** need joint consensus or single-server changes. Until then, the voter set is fixed at startup.
* No leadership transfer yet. A rolling restart costs one election timeout per leader restart.
* The simulator assumes durable storage. Fsync-before-ack is the driver's contract, and it will be tested with the crash-injection approach of ADR-0003 when the storage is written.
