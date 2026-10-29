# emberkv

[![CI](https://github.com/dimasmuly/emberkv/actions/workflows/ci.yml/badge.svg)](https://github.com/dimasmuly/emberkv/actions/workflows/ci.yml)
![MSRV](https://img.shields.io/badge/MSRV-1.89-blue)
![unsafe](https://img.shields.io/badge/unsafe-forbidden-success)

**A crash-safe key-value data service written in Rust.** It provides a JSON/HTTP API for application backends, TTLs, atomic batches, counters, rate limiting, optimistic concurrency, API-key auth, and online backups. Underneath is a log-structured storage engine, written from scratch.

emberkv is built for the small, hot, short-lived state that web and mobile products keep re-implementing: one-time passwords, sessions, rate limits, idempotency keys, and feature flags. It is small enough to read in a weekend, and it handles the problems production systems have to handle: torn writes, bit rot, fsync failures, lost updates, compaction under live traffic, backpressure, graceful shutdown, and secure defaults.

```bash
# Store a one-time password for 5 minutes
curl -X PUT "localhost:8080/v1/namespaces/otp/keys/user:42?ttl=5m" \
     -H "Authorization: Bearer $TOKEN" -d '{"code":"481516","attempts":0}'

# At most 3 login attempts per minute per user
curl -X POST localhost:8080/v1/namespaces/limits/rate-limits/login:user-42 \
     -H "Authorization: Bearer $TOKEN" -d '{"limit":3,"window":"1m"}'
# {"allowed":true,"limit":3,"remaining":2,"reset_at":1792727587229}
```

```ts
// From a Node.js / Deno / Bun backend
const kv = new EmberKV({ baseUrl: "http://emberkv:8080", token: process.env.EMBERKV_TOKEN });
const sessions = kv.namespace("sessions");
await sessions.put(sessionId, { userId, device: "android" }, { ttl: "7d" });
```

---

## What's inside

```text
 web app ─┐                      ┌────────────────────── emberkv-server ───────────────────────┐
 mobile ──┼─▶ your backend ─────▶│ HTTP/JSON API (axum)        binary TCP protocol (tokio)       │
 (never   │   (TS SDK / any      │  auth: API keys, scopes      admin & high-throughput path    │
 direct)  ┘    HTTP client)      │  namespaces · TTL · batch · counters · rate limits · ETags   │
                                 │  background: group fsync · TTL sweep · compaction · metrics  │
                                 └───────────────────────────────┬──────────────────────────────┘
                                                                 ▼
                                 ┌────────────────────── emberkv-engine ───────────────────────┐
                                 │ append-only segments + CRC32 · ordered in-memory index       │
                                 │ crash recovery · atomic batches · expiry · online compaction │
                                 │ hard-link checkpoints · fsync poisoning                       │
                                 └──────────────────────────────────────────────────────────────┘
```

| Component | Role |
|---|---|
| [`emberkv-engine`](crates/emberkv-engine) | Storage engine. Synchronous, embeddable, and needs no async runtime. |
| [`emberkv-raft`](crates/emberkv-raft) | Raft consensus core (pre-vote, check-quorum, ReadIndex) with no I/O, verified by deterministic simulation. Server integration is in progress. |
| [`emberkv-server`](crates/emberkv-server) | HTTP API, binary protocol, auth, background maintenance, telemetry. |
| [`emberkv-protocol`](crates/emberkv-protocol) | Binary wire format and `tokio-util` codec. |
| [`emberkv-client`](crates/emberkv-client) | Async Rust client for the binary protocol. |
| [`emberkv-cli`](crates/emberkv-cli) | `ekv`: admin commands, token minting, HDR-histogram load generator. |
| [`sdk/typescript`](sdk/typescript) | Zero-dependency TypeScript client for the HTTP API. |

## Features

| Feature | Details | Docs |
|---|---|---|
| **JSON/HTTP API** | Namespaced keys, keyset pagination, structured errors, request IDs | [API.md](docs/API.md) |
| **TTL** | Per-key expiry (`?ttl=5m`). Expired data is dropped by compaction with no tombstones. Expiry is checked against an injectable clock | [ADR-0006](docs/adr/0006-ttl-and-injectable-clock.md) |
| **Atomic batches** | Many puts and deletes become visible together or not at all, including after a crash | [ADR-0007](docs/adr/0007-atomic-batches.md) |
| **Optimistic concurrency** | `ETag` + `If-Match` / `If-None-Match: *` stop lost updates and give create-only writes | [API.md](docs/API.md#conditional-writes) |
| **Counters & rate limiting** | Atomic read-modify-write in the engine. Fixed-window limits with `retry_after_ms` | [API.md](docs/API.md#rate-limits) |
| **Auth** | Hashed API keys, `read` / `read_write` / `admin`, scoped to namespaces. **Refuses to listen publicly without auth** | [ADR-0008](docs/adr/0008-http-api-and-auth.md) |
| **Backups** | Online checkpoints via hard links: consistent and nearly free, restored with a plain `open` | [ADR-0009](docs/adr/0009-checkpoints.md) |
| **Crash safety** | CRC32 per record, torn-tail truncation, fatal on sealed-segment damage, fsync poisoning | [ADR-0003](docs/adr/0003-durability-and-recovery.md) |
| **Online compaction** | Reclaims space under live traffic. Crash-safe at every step | [ADR-0004](docs/adr/0004-online-compaction.md) |
| **Operations** | Prometheus metrics, JSON logs, `/healthz` + `/readyz`, graceful drain on SIGTERM, Docker | [INTEGRATION.md](docs/INTEGRATION.md) |

## Quick start

```bash
# 1. Mint an API key for your backend (the token is shown once)
cargo run -q --release -p emberkv-cli -- token --name my-backend \
    --access read_write --namespaces sessions,otp,limits,idempotency,flags > key.txt
sed -n '/^\[\[key\]\]/,$p' key.txt > auth.toml
export TOKEN=$(grep -o 'ekv_[0-9a-f]*' key.txt)

# 2. Run the server: HTTP on :8080, binary protocol on :7878, metrics on :9464
cargo run --release -p emberkv-server -- --auth-file auth.toml \
    --backup-dir ./backups --metrics-listen 127.0.0.1:9464

# 3. Use it
curl -X PUT "localhost:8080/v1/namespaces/sessions/keys/abc?ttl=7d" \
     -H "Authorization: Bearer $TOKEN" -d '{"userId":42}'
curl localhost:8080/v1/namespaces/sessions/keys/abc -H "Authorization: Bearer $TOKEN"

# 4. Run the end-to-end product flows (OTP, sessions, idempotency, flags)
cd sdk/typescript && EMBERKV_TOKEN=$TOKEN node examples/app-backend.ts
```

With Docker Compose, see [`deploy/`](deploy).

## Performance

Measured on an Apple M2 laptop (8 cores, 8 GB, macOS). Client and server ran on the same machine, so treat these as indicative numbers. To reproduce, run `cargo bench -p emberkv-engine --bench engine`, `ekv bench`, and `ab -k`.

| Path | Workload | Throughput | Latency |
|---|---|---|---|
| Engine | `get` 128 B (hit / miss) | — | **0.76 µs** / 17 ns |
| Engine | `put` 128 B / 4 KiB | — | 1.8 µs / 6.6 µs |
| Engine | open + replay 100k keys | ~2.6 M records/s | 38 ms |
| HTTP API | `GET` JSON doc, 32 conns, keep-alive | **85 k req/s** | p99 ≤ 1 ms |
| HTTP API | `PUT` JSON doc, 32 conns | 75 k req/s | p99 ≤ 3 ms |
| Binary protocol | 80% get / 20% put, 32 conns | 84 k req/s | p99 0.97 ms (get) |
| Binary protocol | 100% put, `--sync periodic` | 74 k req/s | p99 2.5 ms |
| Binary protocol | 100% put, `--sync always` | 338 req/s | p50 96 ms |

The last row is deliberate. macOS `F_FULLFSYNC` costs ~3 ms, and every writer currently waits its turn. **Group commit** would remove most of that gap, and it is first on the roadmap.

Two regressions appeared during v0.2 and were fixed before release. TTL support briefly more than doubled `get`-miss latency (fixed by reading the clock only for keys that have an expiry). Restarting without writes piled up empty segments (fixed by pruning them on open).

## Testing

```bash
cargo test --workspace        # 87 tests
EMBERKV_SIM_SEEDS=5000 cargo test --release -p emberkv-raft --test simulation   # 5,000 cluster histories, ~10 s
cd sdk/typescript && npm run typecheck && npm run example   # needs a running server
```

| Kind | What it proves |
|---|---|
| **Deterministic cluster simulation** ([`simulation.rs`](crates/emberkv-raft/tests/simulation.rs)) | 3- and 5-node Raft clusters under message loss, duplication, reordering, partitions and crash-restart. Election safety, one vote per term, leader completeness, state machine safety, log matching and linearizable reads are checked after every step. Liveness is checked after healing. Every failure reproduces from its seed. **Mutation-tested**: 8 classic Raft bugs injected, 8 caught ([ADR-0010](docs/adr/0010-raft-consensus-core.md#evidence)). |
| **Replicas on real storage** ([`replica/tests.rs`](crates/emberkv-server/src/replica/tests.rs)) | 300 concurrent mixed writes leave every replica's keyspace byte-identical. Acknowledged writes survive a leader crash. A restarted replica applies nothing twice. A minority never acknowledges. |
| **Model-based property test** ([`model.rs`](crates/emberkv-engine/tests/model.rs)) | 128 random histories of put / TTL put / delete / batch / read-modify-write / multi-key transaction (including aborts) / time travel / purge / reopen / compact. Each is checked against a reference model on a `ManualClock`. Accounting rebuilt from disk must match the incremental accounting. |
| **Crash & corruption injection** ([`recovery.rs`](crates/emberkv-engine/tests/recovery.rs)) | Torn headers, torn bodies, **torn batches**, bit flips, bit rot at read time, stale compaction output, locking, checkpoint isolation, segment pruning |
| **Concurrency stress** ([`concurrency.rs`](crates/emberkv-engine/tests/concurrency.rs)) | Writers, readers and a compactor looping concurrently. No torn or foreign values. The final state survives a reopen. |
| **Protocol properties** ([`codec.rs`](crates/emberkv-protocol/tests/codec.rs)) | Round-trips under arbitrary TCP chunking, exact lengths, fuzzed input never panics |
| **HTTP end-to-end** ([`http.rs`](crates/emberkv-server/tests/http.rs)) | JSON lifecycle, TTL, lost-update prevention, pagination, atomic batches, counters, rate limits, structured errors |
| **Auth end-to-end** ([`auth.rs`](crates/emberkv-server/tests/auth.rs)) | 401/403 matrix, scope enforcement, admin-only binary protocol, refusal to expose without auth, backup → restore |
| **SDK end-to-end** ([`app-backend.ts`](sdk/typescript/examples/app-backend.ts)) | OTP login, sessions, idempotency keys and feature flags against a live server |

## Design decisions

| ADR | Decision |
|---|---|
| [0001](docs/adr/0001-log-structured-storage.md) | Log-structured hash storage (Bitcask) over an LSM tree or B-tree |
| [0002](docs/adr/0002-ordered-in-memory-index.md) | Ordered `BTreeMap` index for prefix scans and pagination |
| [0003](docs/adr/0003-durability-and-recovery.md) | Configurable fsync, poisoning on fsync failure, tail truncation vs. fatal corruption |
| [0004](docs/adr/0004-online-compaction.md) | Crash-safe online compaction protocol |
| [0005](docs/adr/0005-protocol-and-threading.md) | Binary protocol; tokio for networking, a blocking pool for disk |
| [0006](docs/adr/0006-ttl-and-injectable-clock.md) | Expiry in the record format; lazy expiry; injectable clock |
| [0007](docs/adr/0007-atomic-batches.md) | Atomic batches via flagged records and a commit marker |
| [0008](docs/adr/0008-http-api-and-auth.md) | HTTP API design, namespaces, ETags, hashed API keys, secure defaults |
| [0009](docs/adr/0009-checkpoints.md) | Hard-link checkpoints; never append to pre-existing segments |
| [0010](docs/adr/0010-raft-consensus-core.md) | Own sans-IO Raft core, verified by deterministic simulation and mutation testing |
| [0011](docs/adr/0011-replicated-state-machine.md) | Writes as deterministic commands on a replicated clock; the Raft log stored in emberkv itself; exactly-once apply |

## Used by

**[Kiln ERP](https://github.com/dimasmuly/kiln-erp)** is a Next.js 16 + PostgreSQL ERP that uses emberkv in production style through the TypeScript SDK. It stores sessions (token hashes only), login rate limits, exactly-once form submissions (`If-None-Match: *`), document edit leases with heartbeats, and a shared dashboard cache, each with a designed fallback for when emberkv is unavailable. Its System page shows live engine statistics, and its integration tests run against a real `emberkv-server`. See its [ADR on the emberkv boundary](https://github.com/dimasmuly/kiln-erp/blob/main/docs/adr/0002-emberkv-boundary.md).

## Limitations & roadmap

emberkv is a single-node system. The gaps below are known and documented on purpose:

- [ ] **Group commit**: one fsync for many concurrent writers.
- [ ] **TLS**: today, run it on a private network or behind a TLS-terminating proxy or service mesh ([INTEGRATION.md](docs/INTEGRATION.md#security-checklist)).
- [ ] **Replication / HA**: in progress. Done: the Raft core ([ADR-0010](docs/adr/0010-raft-consensus-core.md)), and a replicated state machine with a durable Raft log, group commit and exactly-once apply ([ADR-0011](docs/adr/0011-replicated-state-machine.md)). Still to do: node-to-node transport and leader forwarding, snapshots, then multi-endpoint failover in the SDK. Until then a single node is a single point of failure, and checkpoints give backups, not failover.
- [ ] **Hint files** for faster startup on large datasets.
- [ ] All keys must fit in RAM (Bitcask trade-off, [ADR-0001](docs/adr/0001-log-structured-storage.md)).
- [ ] Linux and macOS only (compaction relies on POSIX unlink-while-open).

## License

MIT. See [LICENSE](LICENSE).
