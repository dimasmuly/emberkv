# Integrating emberkv into a web & mobile product

This guide shows how a typical product (a web app, Android and iOS apps, one backend API) adopts emberkv safely. It covers where emberkv fits, which features to move to it, how to deploy and secure it, and how to run it.

## 1. Where it fits

```text
┌───────────┐     ┌────────────┐
│ Web app   │     │ Mobile app │        Clients NEVER talk to emberkv directly:
└─────┬─────┘     └─────┬──────┘        tokens would leak, and namespaces are
      │  HTTPS (your API) │              not designed for end-user isolation.
      └────────┬─────────┘
               ▼
       ┌───────────────┐   HTTP + Bearer token (private network)   ┌───────────┐
       │  Your backend │ ─────────────────────────────────────────▶ │  emberkv  │
       │  (any stack)  │                                            └───────────┘
       │               │ ─────────▶ PostgreSQL / MySQL  ← system of record (users, orders, payments)
       └───────────────┘
```

emberkv **complements** your main database; it does not replace it. Use it for state that is small, hot, often short-lived, and that you could lose without corrupting your business records:

| Use case | Namespace | Primitive | Why emberkv |
|---|---|---|---|
| OTP / verification codes | `otp` | `PUT ?ttl=5m`, `If-Match` to consume once | Self-expiring; atomic single use |
| Login / OTP-request throttling | `limits` | `rate-limits` | Atomic counter + window in one call |
| Sessions / refresh tokens | `sessions` | `PUT ?ttl=7d`, `DELETE` on logout | Fast lookups on every request |
| Idempotency keys for mobile retries | `idempotency` | `If-None-Match: *` + TTL | Flaky networks retry; act exactly once |
| Feature flags & remote config | `flags` | `batch`, list | Atomic multi-flag rollouts |
| Counters (views, quotas) | `stats` | `counters` | No read-modify-write races |

Keep **payments, medical or personal records, orders and anything legally retained** in your primary database. Unlike PostgreSQL, emberkv is single-node, has no replication, and does no point-in-time recovery.

## 2. Rollout plan (low risk first)

1. **Shadow phase.** Run emberkv next to production. Write rate-limit counters to it but keep enforcing with your current mechanism, and compare decisions in logs.
2. **First real feature: rate limiting.** If emberkv is down, *fail open* (allow the request and log it). Users notice nothing.
3. **OTP and idempotency keys.** These are short-lived, so losing them only means "request a new code" or "retry".
4. **Sessions**, only after you have watched it in production. Losing sessions logs users out, which is annoying but recoverable.
5. Never move a system-of-record table to emberkv.

Every call site should have a timeout (the SDK defaults to 5 s) and a defined fallback.

## 3. Deploy

```bash
# Mint keys (store tokens in your secret manager; only hashes go to the server)
ekv token --name app-backend --access read_write \
    --namespaces sessions,otp,limits,idempotency,flags
ekv token --name ops --access admin
```

[`deploy/docker-compose.yml`](../deploy/docker-compose.yml) runs the server with a persistent volume, an auth file, backups, metrics and a health check. In Kubernetes, use the same image with:

- a **StatefulSet with 1 replica** and a `ReadWriteOnce` volume at `/data` (the engine takes an exclusive lock on its directory);
- `livenessProbe: /healthz` and `readinessProbe: /readyz` on port 8080;
- `terminationGracePeriodSeconds` greater than `EMBERKV_SHUTDOWN_GRACE_SECS` (default 15), so SIGTERM drains cleanly;
- the auth file mounted from a Secret.

## 4. Security checklist

- [ ] Run with `--auth-file`. Without it, the server **refuses** to bind non-loopback addresses unless you pass `--allow-unauthenticated-remote`.
- [ ] One key per service, scoped to the namespaces it needs; `read` where it only reads.
- [ ] `admin` keys only for operators and CI, never for the app.
- [ ] Keep emberkv on a private network. It speaks plain HTTP, so cross untrusted networks only through a TLS-terminating proxy or service-mesh mTLS.
- [ ] Do not put secrets or personal data in **keys**. Keys are part of request URIs, so they appear in access logs. Values are fine.
- [ ] Rotate keys by adding the new `[[key]]`, deploying, switching clients, then removing the old entry.

## 5. Operate

| Task | How |
|---|---|
| Backup | `POST /v1/admin/checkpoints` (cron), then ship `--backup-dir/checkpoint-*` off the host. Checkpoints are hard links, so they are instant and cost no extra space until data diverges. |
| Restore | Stop the server and point `--data-dir` at a checkpoint directory (or copy it into place), then start. |
| Monitor | Prometheus on `--metrics-listen`: request rate and latency by `transport`/`op`, `emberkv_disk_bytes`, `emberkv_live_bytes`, `emberkv_expired_keys_total`, `emberkv_compactions_total` |
| Alert on | `/readyz` failing (fsync failure poisons writes until restart); p99 latency; disk usage growing while live bytes stay flat (compaction not keeping up) |
| Capacity | All keys live in RAM: roughly 100 bytes plus key length per key. 1 M keys of ~40 bytes is about 140 MB. Values live on disk and in the page cache. |
| Durability | `--sync periodic` (default) can lose up to 100 ms of writes on power loss; `--sync always` loses none, but each write waits for an fsync. |

## 6. Client code

TypeScript / JavaScript backends: use [`sdk/typescript`](../sdk/typescript). [`examples/app-backend.ts`](../sdk/typescript/examples/app-backend.ts) implements OTP login, sessions, idempotency keys and feature flags, and asserts their behavior against a live server.

Any other stack (PHP/Laravel, Go, Python, Kotlin, Dart): the API is plain HTTP+JSON. See [API.md](API.md). The patterns translate directly:

```text
issue OTP:   POST rate-limits/otp-request:{phone}  → if allowed: PUT otp/{user}?ttl=5m
verify OTP:  GET otp/{user} (keep ETag) → match? DELETE with If-Match: <etag>   (single use)
                                        → wrong? PUT attempts+1 with If-Match   (no lost counts)
mobile retry: PUT idempotency/{request-id} with If-None-Match: * → 412 means "already handled"
```
