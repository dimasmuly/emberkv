# ADR-0005: Custom binary protocol; async network, blocking storage

**Status:** Accepted

## Context

The server needs a wire format and a threading model. The storage engine does blocking file I/O, including fsync, which can take milliseconds.

## Decision 1: a minimal length-prefixed binary protocol

Alternatives:

* **HTTP/JSON**: universal and curl-friendly, but it pays for parsing and base64 for binary values on every request. That is a poor fit for a sub-millisecond KV path.
* **gRPC**: strong tooling and schema evolution, but it brings in HTTP/2, protobuf code generation, and a large dependency tree for six operations.
* **RESP (Redis protocol)**: works with existing tools, but text framing and its semantics invite confusion with real Redis.

Chosen: `u32 length + opcode + fields` ([PROTOCOL.md](../PROTOCOL.md)). It is about 500 lines including the codec and docs, zero-copy (`Bytes`), easy to harden (bounds checked before allocating), and easy to property-test. Unknown error codes decode to `ErrorCode::Other` instead of failing, so new codes can be added without breaking old clients. Observability over HTTP is still provided, separately, by the Prometheus endpoint.

## Decision 2: tokio for networking, the blocking pool for storage

* The engine is **synchronous**. It stays simple, embeddable without a runtime, and testable with plain threads.
* The server runs every engine call except `PING` through `tokio::task::spawn_blocking`. An fsync can therefore never stall an async worker thread, and with it the other connections on that worker.

Cost: one thread-pool hop per request (a few µs). For page-cache hits that is significant relative to the 0.8 µs engine `get`, but small next to network RTT. **Possible optimization:** run reads inline (they almost always hit the page cache) and send only writes and scans to the pool. Deferred until measured under realistic network latency.

## Decision 3: bounded everything

| Resource | Bound |
|---|---|
| Connections | `Semaphore` acquired *before* `accept()` (backpressure, not load shedding) |
| Frame size | rejected from the 4-byte header, before buffering |
| Scan size | `min(client limit, server max_scan_limit)`; response re-checked against frame limit |
| Idle connections | `idle_timeout` |
| Shutdown | `shutdown_grace`, then abandon stragglers |

## Consequences

* No unbounded memory or task growth is reachable from the network.
* The client must treat a timeout as fatal for its connection, since a late reply would be mistaken for the next request's answer. The client implements this as `ClientError::Broken`.
