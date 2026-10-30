# Quickstart: emberkv in your app in 5 minutes

*Bahasa Indonesia: [QUICKSTART.id.md](QUICKSTART.id.md)*

emberkv runs as a separate service next to your backend. It needs no config file: one environment variable turns on authentication.

## 1. Run the server

```bash
docker run -d --name emberkv -p 8080:8080 -v emberkv-data:/data \
  -e EMBERKV_ADMIN_TOKEN="$(openssl rand -hex 24)" \
  ghcr.io/dimasmuly/emberkv:latest
docker inspect emberkv --format '{{range .Config.Env}}{{println .}}{{end}}' | grep ADMIN_TOKEN   # your token
```

Or add it to an existing `compose.yaml`:

```yaml
services:
  emberkv:
    image: ghcr.io/dimasmuly/emberkv:latest
    environment:
      EMBERKV_ADMIN_TOKEN: ${EMBERKV_TOKEN}   # 16+ characters, from your .env
    volumes: [emberkv-data:/data]
    ports: ["127.0.0.1:8080:8080"]            # loopback only: emberkv speaks plain HTTP
volumes:
  emberkv-data:
```

The token is a full-access key, which suits a single app. With several apps, give each its own scoped key ([INTEGRATION.md](INTEGRATION.md)).

## 2. TypeScript / JavaScript (Node, Bun, Deno, edge)

```bash
npm install @emberkv/client
```

The SDK is one dependency-free file. Before the npm release, you can also copy [`sdk/typescript/src/index.ts`](../sdk/typescript/src/index.ts) into your project.

```ts
import { createKit } from "@emberkv/client";

const kit = createKit({ baseUrl: "http://127.0.0.1:8080", token: process.env.EMBERKV_TOKEN });
```

That one object gives you every feature, already designed and tested in production use:

| Feature | One call | When emberkv is down |
|---|---|---|
| **Sessions** | `kit.sessions.create(userId, { userAgent })` returns a cookie token; `resolve(token)`, `list(userId)`, `destroyAll(userId)` | fails closed (throws): nobody can be authenticated |
| **Rate limits** | `kit.rateLimit.check("login:" + email, { limit: 5, window: "15m" })` | fails open, reported |
| **Exactly-once requests** | `kit.idempotency.once("payment", key, () => charge())` runs once; duplicates get the stored result | fails open, reported |
| **Edit leases** | `kit.leases.acquire("invoice:42", { id, name })` tells others who is editing | fails open, reported |
| **Shared cache** | `kit.cache.readThrough("report", { ttl: "1m", load })`, `invalidate("report")` | bypassed, reported |

Each fallback calls `onDegraded(feature, error)`, which writes a JSON warning by default. [`examples/kit.ts`](../sdk/typescript/examples/kit.ts) exercises all of them, including the failure modes. For raw key-value access, use `kit.client.namespace("anything")`.

### Express

```ts
app.post("/login", async (req, res) => {
  const limit = await kit.rateLimit.check(`login:${req.body.email}`, { limit: 5, window: "15m" });
  if (!limit.allowed) return res.status(429).set("Retry-After", String(Math.ceil(limit.retryAfterMs / 1000))).end();
  const user = await verifyPassword(req.body.email, req.body.password);
  if (!user) return res.status(401).end();
  res.cookie("session", await kit.sessions.create(user.id), { httpOnly: true, secure: true, sameSite: "lax" });
  res.status(204).end();
});

app.post("/orders", async (req, res) => {
  const { result, replayed } = await kit.idempotency.once("order", req.get("Idempotency-Key")!, () => createOrder(req.body));
  res.status(replayed ? 200 : 201).json(result);
});
```

## 3. Any other language: the HTTP API

Everything is plain HTTP and JSON ([API.md](API.md)). Namespaces need no setup and appear on first write.

```bash
T=your-token
curl -X PUT "localhost:8080/v1/namespaces/otp/keys/user:42?ttl=5m" -H "Authorization: Bearer $T" -d '{"code":"481516"}'
curl localhost:8080/v1/namespaces/otp/keys/user:42 -H "Authorization: Bearer $T"
curl -X POST localhost:8080/v1/namespaces/limits/rate-limits/login:ada -H "Authorization: Bearer $T" \
     -d '{"limit":5,"window":"15m"}'                      # {"allowed":true,"remaining":4,...}
curl -X PUT localhost:8080/v1/namespaces/jobs/keys/run-1 -H "Authorization: Bearer $T" \
     -H "If-None-Match: *" -d '{"state":"started"}'      # 412 if someone else claimed it first
```

**PHP / Laravel**

```php
$kv = Http::withToken(env('EMBERKV_TOKEN'))->baseUrl('http://emberkv:8080/v1/namespaces');
$kv->withBody(json_encode(['code' => $code]))->put("otp/keys/user:{$id}?ttl=5m");
$decision = $kv->post("limits/rate-limits/login:{$email}", ['limit' => 5, 'window' => '15m'])->json();
if (! $decision['allowed']) abort(429);
```

**Python**

```python
kv = requests.Session(); kv.headers["Authorization"] = f"Bearer {os.environ['EMBERKV_TOKEN']}"
base = "http://emberkv:8080/v1/namespaces"
kv.put(f"{base}/otp/keys/user:42", params={"ttl": "5m"}, json={"code": "481516"})
allowed = kv.post(f"{base}/limits/rate-limits/login:ada", json={"limit": 5, "window": "15m"}).json()["allowed"]
```

**Go**

```go
req, _ := http.NewRequest("PUT", base+"/otp/keys/user:42?ttl=5m", strings.NewReader(`{"code":"481516"}`))
req.Header.Set("Authorization", "Bearer "+os.Getenv("EMBERKV_TOKEN"))
resp, err := http.DefaultClient.Do(req) // 204 No Content
```

## Before production

* Keep emberkv on a private network. It speaks plain HTTP, so terminate TLS in front of it if traffic leaves the host.
* Give each app its own scoped key instead of the admin token ([INTEGRATION.md](INTEGRATION.md#security-checklist)).
* Mount `/data` on a volume. Set `EMBERKV_BACKUP_DIR` and call `POST /v1/admin/checkpoints` for backups.
* Store only state you can rebuild or afford to lose on a single-node failure until replication ships ([roadmap](../README.md#limitations--roadmap)).
