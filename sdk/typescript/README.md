# @emberkv/client

Zero-dependency TypeScript client for the emberkv HTTP API, with a **kit** of ready-made application features. It runs on Node.js 22.18+, Deno, Bun and edge runtimes, anywhere `fetch` and WebCrypto exist. Use it from your **backend**; never ship tokens to browsers or mobile apps.

## Kit: features with no setup

```ts
import { createKit } from "@emberkv/client";

const kit = createKit({ baseUrl: process.env.EMBERKV_URL!, token: process.env.EMBERKV_TOKEN });

const cookie = await kit.sessions.create(user.id, { userAgent });          // "<userId>.<secret>"
const session = await kit.sessions.resolve(cookie);                        // record | null
const limit = await kit.rateLimit.check(`login:${email}`, { limit: 5, window: "15m" });
const { result, replayed } = await kit.idempotency.once("order", key, () => createOrder(input));
const lease = await kit.leases.acquire("invoice:42", { id: user.id, name: user.name });
const report = await kit.cache.readThrough("sales:today", { ttl: "1m", load: computeReport });
```

| Feature | Stored as | When emberkv is down |
|---|---|---|
| `sessions` | `<userId>:<sha256(secret)>` with a sliding TTL (7 days by default). Tokens are never stored | **fails closed** (throws) |
| `rateLimit` | fixed-window counter under a hashed subject (emails and IPs are personal data) | fails open |
| `idempotency` | create-only claim, then the stored result for 24 h | fails open |
| `leases` | create-only key with a TTL, renewed by heartbeat; released only by its owner | fails open |
| `cache` | `{ value, computedAt }` with a TTL; explicit `invalidate` | bypassed |

Every fallback calls `onDegraded(feature, error)`, which logs one JSON line by default. Namespaces default to `sessions`, `limits`, `idempotency`, `locks` and `cache`; override them with `createKit({ namespaces: { … } })`. [`examples/kit.ts`](examples/kit.ts) asserts every feature and every failure mode against a live server. [Kiln ERP](https://github.com/dimasmuly/kiln-erp) runs on this kit.

## Client

```ts
import { EmberKV } from "@emberkv/client";

const kv = new EmberKV({ baseUrl: process.env.EMBERKV_URL!, token: process.env.EMBERKV_TOKEN });

const otp = kv.namespace("otp");
await otp.put("user:42", { code: "481516", attempts: 0 }, { ttl: "5m" });

const entry = await otp.get<{ code: string }>("user:42");      // { value, etag, expiresAt } | null
await otp.delete("user:42", { ifMatch: entry!.etag });          // consume exactly once

const decision = await kv.namespace("limits").rateLimit("login:user-42", { limit: 5, window: "1m" });
if (!decision.allowed) { /* respond 429, Retry-After: decision.retryAfterMs */ }

const firstTime = await kv.namespace("idempotency").putIfAbsent(requestId, { state: "processing" }, { ttl: "24h" });
```

Errors are thrown as `EmberKVError` with a stable `code` (`precondition_failed`, `forbidden`, ...) and a `retryable` flag.

[`examples/app-backend.ts`](examples/app-backend.ts) implements OTP login, sessions, idempotency keys and feature flags, with assertions:

```bash
npm install && npm run typecheck
EMBERKV_URL=http://127.0.0.1:8080 EMBERKV_TOKEN=ekv_... npm run example
EMBERKV_URL=http://127.0.0.1:8080 EMBERKV_TOKEN=<admin token> npm run example:kit
```

## Publishing

The repository exports the TypeScript source, so sibling projects use it with no build step. `npm run build:package` assembles the npm package (compiled JavaScript and type declarations) in `dist/`. Then run `cd dist && npm publish --access public`.
