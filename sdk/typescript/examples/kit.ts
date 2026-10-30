/**
 * Every kit feature against a live server, with assertions, so this doubles
 * as an end-to-end test. With zero-config auth:
 *
 *   EMBERKV_ADMIN_TOKEN=dev-token-0123456789 emberkv-server
 *   EMBERKV_TOKEN=dev-token-0123456789 node examples/kit.ts
 */

import assert from "node:assert/strict";

import { createKit, IdempotencyError } from "../src/index.ts";

const kit = createKit<{ userAgent: string }>({
  baseUrl: process.env.EMBERKV_URL ?? "http://127.0.0.1:8080",
  token: process.env.EMBERKV_TOKEN,
});
const run = crypto.randomUUID().slice(0, 8);

// --- sessions ----------------------------------------------------------------
const userId = `user-${run}`;
const token = await kit.sessions.create(userId, { userAgent: "kit-example" });
const session = await kit.sessions.resolve(token);
assert.equal(session?.userId, userId);
assert.equal(session?.userAgent, "kit-example");
assert.equal(await kit.sessions.resolve(`${userId}.${"x".repeat(43)}`), null, "a forged token resolves to nothing");
await kit.sessions.create(userId, { userAgent: "second-device" });
assert.equal((await kit.sessions.list(userId)).length, 2);
assert.equal(await kit.sessions.destroyAll(userId), 2);
assert.equal(await kit.sessions.resolve(token), null, "signed out everywhere");
console.log("✓ sessions: create, resolve, forged token rejected, list, sign out everywhere");

// --- rate limits -----------------------------------------------------------------
const rule = { limit: 3, window: "1m" };
const decisions = [];
for (let i = 0; i < 5; i++) decisions.push(await kit.rateLimit.check(`login:${run}@example.com`, rule));
assert.deepEqual(decisions.map((d) => d.allowed), [true, true, true, false, false]);
assert.ok(decisions[4].retryAfterMs > 0);
console.log("✓ rate limit: 3 allowed, then denied with retry-after");

// --- idempotency -------------------------------------------------------------------
let charges = 0;
const charge = () => kit.idempotency.once("payment", `pay-${run}`, async () => ({ chargeId: ++charges }));
const [first, second] = await Promise.allSettled([charge(), charge()]);
const results = [first, second].map((r) => (r.status === "fulfilled" ? r.value.result.chargeId : (r.reason as IdempotencyError).code));
assert.equal(charges, 1, "concurrent duplicates never run twice");
assert.ok(results.includes(1));
const replay = await charge();
assert.deepEqual(replay, { result: { chargeId: 1 }, replayed: true });
console.log("✓ idempotency: concurrent duplicate blocked, retry replays the stored result");

// --- leases ----------------------------------------------------------------------
const doc = `invoice:${run}`;
assert.deepEqual(await kit.leases.acquire(doc, { id: "alice", name: "Alice" }), { acquired: true, degraded: false });
const blocked = await kit.leases.acquire(doc, { id: "bob", name: "Bob" });
assert.ok(!blocked.acquired && blocked.holder.name === "Alice", "Bob sees that Alice is editing");
assert.equal(await kit.leases.renew(doc, { id: "bob" }), false);
await kit.leases.release(doc, "bob"); // not his: a no-op
assert.equal((await kit.leases.holder(doc))?.ownerId, "alice");
await kit.leases.release(doc, "alice");
assert.equal((await kit.leases.acquire(doc, { id: "bob", name: "Bob" })).acquired, true);
console.log("✓ leases: exclusive, visible holder, release only by the owner");

// --- cache -----------------------------------------------------------------------
let loads = 0;
const load = async () => ({ revenue: 42, n: ++loads });
const miss = await kit.cache.readThrough(`report:${run}`, { ttl: "1m", load });
const hit = await kit.cache.readThrough(`report:${run}`, { ttl: "1m", load });
assert.deepEqual([miss.source, hit.source, loads], ["origin", "cache", 1]);
await kit.cache.invalidate(`report:${run}`);
assert.equal((await kit.cache.readThrough(`report:${run}`, { ttl: "1m", load })).source, "origin");
console.log("✓ cache: miss, hit, invalidate");

// --- failure modes ---------------------------------------------------------------------
const events: string[] = [];
const offline = createKit({ baseUrl: "http://127.0.0.1:1", timeoutMs: 300, onDegraded: (feature) => events.push(feature) });
assert.equal((await offline.rateLimit.check("x", rule)).degraded, true, "rate limits fail open");
assert.deepEqual(await offline.idempotency.once("x", `key-${run}`, async () => "ran"), { result: "ran", replayed: false });
assert.equal((await offline.cache.readThrough("x", { ttl: "1m", load })).source, "origin", "the cache is bypassed");
await assert.rejects(offline.sessions.resolve(token), "sessions fail closed");
assert.deepEqual(events, ["rate-limit", "idempotency", "cache-read", "cache-write"]);
console.log("✓ emberkv down: sessions fail closed; limits, idempotency and cache degrade and report it");
