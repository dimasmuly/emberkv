/**
 * The kind of code a web/mobile product's backend would run against emberkv.
 * Each flow asserts its own behavior, so this doubles as an end-to-end test:
 *
 *   EMBERKV_URL=http://127.0.0.1:8080 EMBERKV_TOKEN=ekv_... node examples/app-backend.ts
 */

import assert from "node:assert/strict";
import { randomInt, randomUUID } from "node:crypto";

import { EmberKV, EmberKVError } from "../src/index.ts";

const kv = new EmberKV({ baseUrl: process.env.EMBERKV_URL ?? "http://127.0.0.1:8080", token: process.env.EMBERKV_TOKEN });
const otp = kv.namespace("otp");
const limits = kv.namespace("limits");
const sessions = kv.namespace("sessions");
const idempotency = kv.namespace("idempotency");
const flags = kv.namespace("flags");

interface OtpRecord {
  code: string;
  attempts: number;
}

// --- 1. Login with a one-time password -------------------------------------

/** Issues a 6-digit OTP valid for 5 minutes, at most 3 requests per 10 minutes. */
async function requestOtp(userId: string): Promise<string | null> {
  const decision = await limits.rateLimit(`otp-request:${userId}`, { limit: 3, window: "10m" });
  if (!decision.allowed) return null; // the app would answer HTTP 429 here
  const code = String(randomInt(0, 1_000_000)).padStart(6, "0");
  await otp.put(userId, { code, attempts: 0 } satisfies OtpRecord, { ttl: "5m" });
  return code; // in production: send by SMS / WhatsApp, never return it
}

/**
 * Verifies an OTP. `ifMatch` makes check-and-consume atomic: if two requests
 * race with the right code, only one of them can delete the record.
 */
async function verifyOtp(userId: string, code: string): Promise<boolean> {
  const entry = await otp.get<OtpRecord>(userId);
  if (!entry || entry.value.attempts >= 5) return false;
  try {
    if (entry.value.code === code) {
      return await otp.delete(userId, { ifMatch: entry.etag });
    }
    const next = { ...entry.value, attempts: entry.value.attempts + 1 };
    // Keep the original deadline instead of extending it on every wrong guess.
    const remainingMs = (entry.expiresAt ?? Date.now()) - Date.now();
    await otp.put(userId, next, { ifMatch: entry.etag, ttl: `${Math.max(remainingMs, 1)}ms` });
    return false;
  } catch (err) {
    if (err instanceof EmberKVError && err.code === "precondition_failed") return false; // lost a race
    throw err;
  }
}

const user = `user-${randomUUID().slice(0, 8)}`;
const code = await requestOtp(user);
assert.ok(code);
const wrongCode = code === "000000" ? "111111" : "000000";
assert.equal(await verifyOtp(user, wrongCode), false, "wrong code is rejected");
assert.equal(await verifyOtp(user, code), true, "right code is accepted");
assert.equal(await verifyOtp(user, code), false, "an OTP works only once");
await requestOtp(user);
await requestOtp(user);
assert.equal(await requestOtp(user), null, "4th OTP request within 10 minutes is rate limited");
console.log("✓ OTP login: TTL, attempt counting, single use, request rate limit");

// --- 2. Sessions --------------------------------------------------------------

const sessionId = randomUUID();
await sessions.put(sessionId, { userId: user, device: "android", createdAt: Date.now() }, { ttl: "7d" });
const session = await sessions.get<{ userId: string }>(sessionId);
assert.equal(session?.value.userId, user);
assert.ok(session.expiresAt && session.expiresAt > Date.now());
await sessions.delete(sessionId); // logout
assert.equal(await sessions.get(sessionId), null);
console.log("✓ sessions: create with 7-day TTL, read, logout");

// --- 3. Idempotent requests from flaky mobile networks -----------------------

/** Runs `work` at most once per request id, even if the app retries. */
async function once<T>(requestId: string, work: () => Promise<T>): Promise<T | "duplicate"> {
  const claimed = await idempotency.putIfAbsent(requestId, { state: "processing" }, { ttl: "24h" });
  if (!claimed) return "duplicate";
  const result = await work();
  await idempotency.put(requestId, { state: "done", result }, { ttl: "24h" });
  return result;
}

const requestId = randomUUID(); // sent by the mobile app with each attempt
let charged = 0;
const pay = async () => ++charged;
assert.equal(await once(requestId, pay), 1);
assert.equal(await once(requestId, pay), "duplicate", "a retried request is not processed twice");
assert.equal(charged, 1);
console.log("✓ idempotency keys: retries are processed once");

// --- 4. Feature flags with atomic rollout -------------------------------------

await flags.batch([
  { op: "put", key: "new-booking-flow", value: { enabled: true, rollout: 0.2 } },
  { op: "put", key: "dark-mode", value: { enabled: true } },
  { op: "delete", key: "legacy-checkout" },
]);
const enabled: string[] = [];
for await (const flag of flags.scan<{ enabled: boolean }>()) {
  if (flag.value?.enabled) enabled.push(flag.key);
}
assert.deepEqual(enabled.sort(), ["dark-mode", "new-booking-flow"]);
console.log("✓ feature flags: atomic multi-flag update, paginated scan");

console.log("\nall flows passed");
