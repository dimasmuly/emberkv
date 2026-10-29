# ADR-0008: JSON/HTTP API, namespaces, optimistic concurrency, and API keys

**Status:** Accepted

## Context

v0.1 spoke only a custom binary protocol. That is ideal for throughput, but no web or mobile backend can use it without a bespoke client in its own language. Adoption needs an interface every stack already speaks, plus the safety features a shared service needs: isolation between features, authorization, and protection against concurrent writers.

## Decisions

### 1. HTTP/JSON as the product-facing API; binary protocol stays for admin and throughput

The HTTP API uses `axum`, and every engine call still goes through the blocking pool (ADR-0005). It reached ~85 k GET/s and ~75 k PUT/s on a laptop, which is about the same as the binary protocol. HTTP parsing is not the bottleneck at this scale.

Values on the HTTP API must be **JSON documents** (validated on write). That gives typed responses, inline values in list results, and catches client bugs early. Bytes written through the binary protocol are still listable, base64-encoded.

### 2. Namespaces as key prefixes

`{namespace}/{key}` in the engine. Namespace names cannot contain `/`, so the mapping is unambiguous, and each namespace is one contiguous key range: listing it is a range scan. Namespaces are the unit of access control.

### 3. Optimistic concurrency with ETags (RFC 9110 conditional requests)

The ETag is the first 64 bits of SHA-256(value), so no version number has to be stored. `If-Match` and `If-None-Match: *` are evaluated **inside `Engine::update`**, under the writer lock, so check-and-write is atomic. Standard HTTP semantics mean any HTTP client already knows how to use them. They enable lost-update prevention, create-only writes (idempotency keys), and single-use OTP consumption.

Value-derived ETags have one subtle property: A → B → A gives the same ETag (ABA). For the intended uses (detecting that *the content* changed) that is acceptable, and it is documented.

### 4. Counters and rate limits as server-side primitives

Both are thin layers over `Engine::update`. Doing read-modify-write in the client over HTTP would need a retry loop with `If-Match` on every call. Rate-limit denials **do not consume quota** and return `200` with `allowed: false`. The service answers a question; the caller decides the HTTP status for *its* client.

### 5. API keys: hashed, scoped, secure by default

* Tokens are 256-bit random (`ekv token`); the auth file stores only SHA-256 hashes. A leaked config does not leak credentials. Lookup by hash also avoids timing side channels from comparing secrets.
* Access levels `read < read_write < admin`, scoped to namespaces (`["*"]` for all).
* The binary protocol addresses the raw keyspace across namespaces, so it accepts only **admin** tokens. A failed `AUTH` closes the connection.
* **Secure by default**: without an auth file, the server refuses to bind any non-loopback address unless the operator passes `--allow-unauthenticated-remote`. Misconfiguration fails loudly at startup instead of silently exposing data.
* Admin checkpoints are written only inside `--backup-dir`, with server-chosen names, so the API cannot be used to write arbitrary paths.

## Alternatives considered

* **gRPC**: strong contracts, but browsers, PHP and many mobile backend stacks need extra tooling. HTTP/JSON wins on reach.
* **JWT / OAuth**: valuable when identities come from an IdP. Here the callers are a handful of services, and static, revocable, hashed keys are simpler to operate and reason about.
* **Version counters for ETags**: avoids ABA, but costs 8 bytes per record and a format change. Deferred until a use case needs it.

## Consequences

* No TLS in-process: deploy on a private network or behind a TLS-terminating proxy or mesh ([INTEGRATION.md](../INTEGRATION.md#security-checklist)).
* Key names appear in request URIs and therefore access logs. Do not put secrets in keys.
