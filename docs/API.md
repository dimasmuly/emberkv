# HTTP API reference (v1)

Base URL: `http://<host>:8080` (`--http-listen`). All request and response bodies are JSON.

## Conventions

**Authentication.** Every `/v1/...` route needs `Authorization: Bearer <token>` when the server runs with `--auth-file`. Tokens come from `ekv token`. `/healthz` and `/readyz` never need a token.

| Access | Can do |
|---|---|
| `read` | `GET` keys and lists in its namespaces |
| `read_write` | The above, plus writes, batches, counters, rate limits in its namespaces |
| `admin` | Everything, in every namespace, plus `/v1/admin/*` and the binary protocol |

**Namespaces.** 1–64 characters of `a-z 0-9 _ -`, starting with a letter or digit. They group keys (`sessions`, `otp`, `flags`) and are the unit of access control.

**Keys.** Any non-empty UTF-8 string, percent-encoded in the path. Slashes are allowed.

**Durations** (`ttl`, `window`): `500ms`, `30s`, `15m`, `2h`, `7d`. A bare number means seconds.

**Times** (`expires_at`, `reset_at`, `x-expires-at`): Unix milliseconds.

**Errors.** Errors produced by emberkv have this shape. Switch on `code`, which is stable; `message` is for humans. A few framework-level rejections (an unparseable query string, a wrong HTTP method, a request timeout) return a plain-text body with the status code alone.

```json
{ "error": { "code": "precondition_failed", "message": "the key changed since you read it" } }
```

| Status | `code` |
|---|---|
| 400 | `invalid_json`, `invalid_namespace`, `invalid_key`, `invalid_ttl`, `invalid_window`, `invalid_batch`, `invalid_rate_limit`, `invalid_header` |
| 401 | `unauthenticated` (also sends `WWW-Authenticate: Bearer`) |
| 403 | `forbidden` |
| 404 | `not_found` |
| 408 | request timed out (`--http-request-timeout-secs`, default 10) |
| 409 | `not_a_counter`, `counter_overflow`, `backups_disabled` |
| 412 | `precondition_failed` |
| 413 | `too_large` |
| 500 | `internal` (details are in the server log, never in the response) |
| 503 | `unavailable` (storage failed; also returned by `/readyz` while draining) |

Every response carries an `x-request-id` header (generated, or propagated from the request) for correlating with server logs.

---

## Keys

### `PUT /v1/namespaces/{ns}/keys/{key}`

Stores a JSON document. Query: `ttl` (optional).

```bash
curl -X PUT "$URL/v1/namespaces/otp/keys/user:42?ttl=5m" -H "Authorization: Bearer $T" \
     -d '{"code":"481516","attempts":0}'
```

`204 No Content`, with `ETag: "34cd07aa1e07c47a"`.

### `GET /v1/namespaces/{ns}/keys/{key}`

`200` with the stored document. Headers: `ETag`, plus `x-expires-at` if the key has a TTL. `404` if absent or expired.

### `DELETE /v1/namespaces/{ns}/keys/{key}`

`204` if deleted, `404` if it did not exist. Supports `If-Match`.

### `GET /v1/namespaces/{ns}/keys`

Lists keys in key order. Query: `prefix`, `limit` (1–10000, default 100), `cursor`.

```json
{
  "items": [
    { "key": "o1", "value": { "id": 1 }, "expires_at": null, "etag": "\"…\"" }
  ],
  "next_cursor": "o2"
}
```

To fetch the next page, pass `next_cursor` as `cursor`; it is `null` on the last page. Pagination is **keyset-based**, so pages stay correct while keys change underneath. Values that are not JSON (written through the binary protocol) appear as `value_base64` instead of `value`.

### Conditional writes

`PUT` and `DELETE` honor RFC 9110 preconditions. The check-and-write is atomic in the engine.

| Header | Effect | Use for |
|---|---|---|
| `If-None-Match: *` | Write only if the key does **not** exist | Create-only, idempotency keys, distributed "claim" |
| `If-Match: "<etag>"` | Write only if the current version has this ETag | Read-modify-write without lost updates |
| `If-Match: *` | Write only if the key exists | Update-only |

On failure the response is `412 precondition_failed` and nothing is written. ETags are derived from the value's SHA-256, so equal values have equal ETags.

---

## Batches

### `POST /v1/namespaces/{ns}/batch`

Applies 1–1000 operations **atomically**: every operation becomes visible at once, or none does, even across a crash. The whole batch is validated before anything is written.

```json
{ "operations": [
  { "op": "put",    "key": "order:7",   "value": { "status": "paid" } },
  { "op": "put",    "key": "receipt:7", "value": { "total": 120 }, "ttl": "30d" },
  { "op": "delete", "key": "cart:7" }
] }
```

`200 {"applied": 3}`

---

## Counters

### `POST /v1/namespaces/{ns}/counters/{key}`

Atomically adds `by` (default `1`, may be negative) to an integer counter, creating it at 0 if absent. `ttl` applies only when the counter is created. The body may be empty.

```json
{ "by": 1, "ttl": "1d" }
```

`200 {"value": 6, "expires_at": 1792727587229}`. Returns `409 not_a_counter` if the key holds something else.

## Rate limits

### `POST /v1/namespaces/{ns}/rate-limits/{key}`

Fixed-window limiter: at most `limit` units per `window` per key. `cost` defaults to 1. **Denied calls do not consume quota.**

```json
{ "limit": 3, "window": "1m" }
```

```json
{ "allowed": false, "limit": 3, "remaining": 0, "reset_at": 1792727587229, "retry_after_ms": 41230 }
```

The response is always `200`; your backend decides what a denial means (typically answering its own client with `429` and `Retry-After`). Choose keys per subject and action, for example `login:user-42`, `otp-request:+62812…`, `api:ip-203.0.113.9`.

---

## Admin (`admin` access)

| Route | Description |
|---|---|
| `GET /v1/admin/stats` | Keys, segments, disk/live/dead bytes, fragmentation |
| `POST /v1/admin/compact` | Run compaction now; returns what was reclaimed |
| `POST /v1/admin/checkpoints` | Write a consistent backup into `--backup-dir`; `201 {"name": "checkpoint-…"}`. Clients cannot choose the path. |

## Probes (no auth)

| Route | Meaning |
|---|---|
| `GET /healthz` | The process is up (liveness) |
| `GET /readyz` | `200` if the server can accept writes; `503` while draining for shutdown or after a storage failure (readiness) |
