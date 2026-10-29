# @emberkv/client

Zero-dependency TypeScript client for the emberkv HTTP API. It runs on Node.js 22.18+, Deno, Bun and edge runtimes, anywhere `fetch` exists. Use it from your **backend**; never ship tokens to browsers or mobile apps.

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
```
