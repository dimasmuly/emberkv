/**
 * Zero-dependency TypeScript client for the emberkv HTTP API.
 *
 * Runs anywhere `fetch` exists: Node.js 18+, Deno, Bun, edge runtimes.
 * Use it from your **backend** only. Tokens must never ship to browsers or
 * mobile apps; those talk to your backend, which talks to emberkv.
 *
 * @example
 * const kv = new EmberKV({ baseUrl: "http://emberkv:8080", token: process.env.EMBERKV_TOKEN });
 * const otp = kv.namespace("otp");
 * await otp.put("user:42", { code: "481516" }, { ttl: "5m" });
 */

export interface EmberKVOptions {
  /** Base URL of the HTTP API, e.g. `http://127.0.0.1:8080`. */
  baseUrl: string;
  /** API token from `ekv token`. Omit only if the server has auth disabled. */
  token?: string;
  /** Per-request timeout in milliseconds. Default 5000. */
  timeoutMs?: number;
  /** Custom fetch implementation (for tests or instrumentation). */
  fetch?: typeof fetch;
}

/** A stored value with its metadata. */
export interface Entry<T> {
  value: T;
  /** Opaque version tag. Pass it as `ifMatch` to update only if unchanged. */
  etag: string;
  /** Expiry in Unix milliseconds, or `null` for keys that never expire. */
  expiresAt: number | null;
}

export interface PutOptions {
  /** Time to live, e.g. `"30s"`, `"5m"`, `"2h"`, `"7d"`. */
  ttl?: string;
  /** Write only if the current version has this ETag (optimistic locking). */
  ifMatch?: string;
  /** `"*"`: write only if the key does not exist yet (create-only). */
  ifNoneMatch?: "*";
}

export interface ListOptions {
  prefix?: string;
  /** Page size, 1–10000. Default 100. */
  limit?: number;
  /** `nextCursor` from the previous page. */
  cursor?: string;
}

export interface ListItem<T> {
  key: string;
  /** Present when the stored value is JSON. */
  value?: T;
  /** Present instead of `value` for non-JSON bytes. */
  valueBase64?: string;
  etag: string;
  expiresAt: number | null;
}

export interface Page<T> {
  items: ListItem<T>[];
  nextCursor: string | null;
}

export type BatchOperation =
  | { op: "put"; key: string; value: unknown; ttl?: string }
  | { op: "delete"; key: string };

export interface RateLimitRule {
  /** Maximum units per window. */
  limit: number;
  /** Window length, e.g. `"1m"`. */
  window: string;
  /** Units this request consumes. Default 1. */
  cost?: number;
}

export interface RateLimitDecision {
  allowed: boolean;
  limit: number;
  remaining: number;
  /** When the window resets, in Unix milliseconds. */
  resetAt: number | null;
  /** How long to wait before retrying (only when denied). */
  retryAfterMs?: number;
}

/** An error response from emberkv. `code` is stable; switch on it, not on `message`. */
export class EmberKVError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(`emberkv ${status} ${code}: ${message}`);
    this.name = "EmberKVError";
    this.status = status;
    this.code = code;
  }

  /** True for failures worth retrying (server busy or restarting). */
  get retryable(): boolean {
    return this.status === 503 || this.status === 408 || this.status === 0;
  }
}

export class EmberKV {
  #baseUrl: string;
  #token: string | undefined;
  #timeoutMs: number;
  #fetch: typeof fetch;

  constructor(options: EmberKVOptions) {
    this.#baseUrl = options.baseUrl.replace(/\/+$/, "");
    this.#token = options.token;
    this.#timeoutMs = options.timeoutMs ?? 5000;
    this.#fetch = options.fetch ?? globalThis.fetch.bind(globalThis);
  }

  /** A handle for keys inside `name` (lowercase letters, digits, `_`, `-`). */
  namespace(name: string): Namespace {
    return new Namespace(this, name);
  }

  /** Whether the server can accept writes (for readiness checks). */
  async ready(): Promise<boolean> {
    try {
      const response = await this.request("GET", "/readyz", { auth: false });
      return response.ok;
    } catch {
      return false;
    }
  }

  /** Engine statistics. Requires an admin token. */
  async stats(): Promise<Record<string, number>> {
    return (await this.#json("GET", "/v1/admin/stats")) as Record<string, number>;
  }

  /** Writes a backup checkpoint on the server. Requires an admin token. */
  async checkpoint(): Promise<{ name: string; segments: number; bytes: number }> {
    return (await this.#json("POST", "/v1/admin/checkpoints")) as { name: string; segments: number; bytes: number };
  }

  /** @internal Low-level request; throws `EmberKVError` on non-2xx except 404 when `allow404`. */
  async request(
    method: string,
    path: string,
    init: { body?: unknown; headers?: Record<string, string>; auth?: boolean; allow404?: boolean } = {},
  ): Promise<Response> {
    const headers: Record<string, string> = { ...init.headers };
    if (init.auth !== false && this.#token) headers.authorization = `Bearer ${this.#token}`;
    if (init.body !== undefined) headers["content-type"] = "application/json";

    let response: Response;
    try {
      response = await this.#fetch(this.#baseUrl + path, {
        method,
        headers,
        body: init.body === undefined ? undefined : JSON.stringify(init.body),
        signal: AbortSignal.timeout(this.#timeoutMs),
      });
    } catch (cause) {
      throw new EmberKVError(0, "network_error", cause instanceof Error ? cause.message : String(cause));
    }
    if (response.ok || (init.allow404 && response.status === 404)) return response;

    let code = "http_error";
    let message = response.statusText;
    try {
      const body = (await response.json()) as { error?: { code?: string; message?: string } };
      code = body.error?.code ?? code;
      message = body.error?.message ?? message;
    } catch {
      // Non-JSON error body (e.g. from a proxy); keep the HTTP status text.
    }
    throw new EmberKVError(response.status, code, message);
  }

  async #json(method: string, path: string, body?: unknown): Promise<unknown> {
    const response = await this.request(method, path, { body });
    return response.json();
  }
}

export class Namespace {
  readonly name: string;
  #kv: EmberKV;

  constructor(kv: EmberKV, name: string) {
    this.#kv = kv;
    this.name = name;
  }

  #keyPath(key: string): string {
    return `/v1/namespaces/${this.name}/keys/${encodeURIComponent(key)}`;
  }

  /** Returns the entry, or `null` if the key is absent or expired. */
  async get<T = unknown>(key: string): Promise<Entry<T> | null> {
    const response = await this.#kv.request("GET", this.#keyPath(key), { allow404: true });
    if (response.status === 404) return null;
    const expires = response.headers.get("x-expires-at");
    return {
      value: (await response.json()) as T,
      etag: response.headers.get("etag") ?? "",
      expiresAt: expires === null ? null : Number(expires),
    };
  }

  /**
   * Stores a JSON value and returns its new ETag. With `ifMatch` or
   * `ifNoneMatch`, throws `EmberKVError` with code `precondition_failed`
   * if the condition does not hold.
   */
  async put(key: string, value: unknown, options: PutOptions = {}): Promise<string> {
    const query = options.ttl ? `?ttl=${encodeURIComponent(options.ttl)}` : "";
    const headers: Record<string, string> = {};
    if (options.ifMatch) headers["if-match"] = options.ifMatch;
    if (options.ifNoneMatch) headers["if-none-match"] = options.ifNoneMatch;
    const response = await this.#kv.request("PUT", this.#keyPath(key) + query, { body: value, headers });
    return response.headers.get("etag") ?? "";
  }

  /** Stores the value only if the key does not exist. Returns false if it did. */
  async putIfAbsent(key: string, value: unknown, options: { ttl?: string } = {}): Promise<boolean> {
    try {
      await this.put(key, value, { ...options, ifNoneMatch: "*" });
      return true;
    } catch (err) {
      if (err instanceof EmberKVError && err.code === "precondition_failed") return false;
      throw err;
    }
  }

  /** Deletes the key. Returns false if it did not exist. */
  async delete(key: string, options: { ifMatch?: string } = {}): Promise<boolean> {
    const headers: Record<string, string> = options.ifMatch ? { "if-match": options.ifMatch } : {};
    const response = await this.#kv.request("DELETE", this.#keyPath(key), { headers, allow404: true });
    return response.status !== 404;
  }

  /** One page of keys in key order. */
  async list<T = unknown>(options: ListOptions = {}): Promise<Page<T>> {
    const params = new URLSearchParams();
    if (options.prefix) params.set("prefix", options.prefix);
    if (options.limit) params.set("limit", String(options.limit));
    if (options.cursor) params.set("cursor", options.cursor);
    const query = params.size > 0 ? `?${params}` : "";
    const response = await this.#kv.request("GET", `/v1/namespaces/${this.name}/keys${query}`);
    const body = (await response.json()) as {
      items: { key: string; value?: T; value_base64?: string; etag: string; expires_at: number | null }[];
      next_cursor: string | null;
    };
    return {
      items: body.items.map((item) => ({
        key: item.key,
        value: item.value,
        valueBase64: item.value_base64,
        etag: item.etag,
        expiresAt: item.expires_at,
      })),
      nextCursor: body.next_cursor,
    };
  }

  /** Iterates every matching key, fetching pages lazily. */
  async *scan<T = unknown>(options: Omit<ListOptions, "cursor"> = {}): AsyncGenerator<ListItem<T>> {
    let cursor: string | undefined;
    do {
      const page = await this.list<T>({ ...options, cursor });
      yield* page.items;
      cursor = page.nextCursor ?? undefined;
    } while (cursor);
  }

  /** Applies all operations atomically: all succeed or none do. */
  async batch(operations: BatchOperation[]): Promise<number> {
    const response = await this.#kv.request("POST", `/v1/namespaces/${this.name}/batch`, { body: { operations } });
    return ((await response.json()) as { applied: number }).applied;
  }

  /** Atomically adds `by` (default 1) to an integer counter. */
  async increment(key: string, options: { by?: number; ttl?: string } = {}): Promise<{ value: number; expiresAt: number | null }> {
    const path = `/v1/namespaces/${this.name}/counters/${encodeURIComponent(key)}`;
    const response = await this.#kv.request("POST", path, { body: options });
    const body = (await response.json()) as { value: number; expires_at: number | null };
    return { value: body.value, expiresAt: body.expires_at };
  }

  /** Fixed-window rate limiting. Denied calls do not consume quota. */
  async rateLimit(key: string, rule: RateLimitRule): Promise<RateLimitDecision> {
    const path = `/v1/namespaces/${this.name}/rate-limits/${encodeURIComponent(key)}`;
    const response = await this.#kv.request("POST", path, { body: rule });
    const body = (await response.json()) as {
      allowed: boolean;
      limit: number;
      remaining: number;
      reset_at: number | null;
      retry_after_ms?: number;
    };
    return {
      allowed: body.allowed,
      limit: body.limit,
      remaining: body.remaining,
      resetAt: body.reset_at,
      retryAfterMs: body.retry_after_ms,
    };
  }
}

// ================================================================ kit

/**
 * Ready-made application features on top of the client: sessions, rate
 * limiting, exactly-once requests, edit leases and a read-through cache.
 *
 * These are the patterns Kiln ERP runs in production, extracted so any
 * backend gets them with no extra setup. Each feature has a deliberate
 * behaviour for when emberkv is unreachable (see `onDegraded`): sessions
 * fail closed, everything else fails open or bypasses, because locking
 * every user out over a throttle store restarting is the worse outage.
 *
 * @example
 * const kit = createKit({ baseUrl: "http://127.0.0.1:8080", token: process.env.EMBERKV_TOKEN });
 * const token = await kit.sessions.create(user.id, { userAgent });
 * const { allowed } = await kit.rateLimit.check(`login:${email}`, { limit: 5, window: "15m" });
 */

export interface KitNamespaces {
  sessions: string;
  limits: string;
  idempotency: string;
  locks: string;
  cache: string;
}

export interface KitOptions extends Partial<EmberKVOptions> {
  /** An existing client; otherwise one is built from `baseUrl` and `token`. */
  client?: EmberKV;
  /** Namespace names, if the defaults (`sessions`, `limits`, …) clash with yours. */
  namespaces?: Partial<KitNamespaces>;
  /**
   * Called whenever a feature falls back because emberkv is unreachable.
   * Defaults to one structured `console.warn` line per event.
   */
  onDegraded?: (feature: string, error: unknown) => void;
}

export const DEFAULT_NAMESPACES: KitNamespaces = {
  sessions: "sessions",
  limits: "limits",
  idempotency: "idempotency",
  locks: "locks",
  cache: "cache",
};

/** True when emberkv could not be reached or is not accepting requests. */
export function isUnavailable(error: unknown): boolean {
  return error instanceof EmberKVError && error.retryable;
}

async function sha256Hex(text: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, "0")).join("");
}

/**
 * Keys appear in emberkv's access logs, so hash anything personal (emails,
 * IPs) or secret before it becomes part of a key.
 */
export async function opaqueKey(value: string): Promise<string> {
  return (await sha256Hex(value)).slice(0, 32);
}

function randomToken(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(32));
  return btoa(String.fromCharCode(...bytes)).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function ttlSeconds(seconds: number): string {
  return `${Math.max(1, Math.round(seconds))}s`;
}

// ---------------------------------------------------------------- sessions

export interface SessionOptions {
  /** Session lifetime, sliding. Default 7 days. */
  ttlSeconds?: number;
  /** Extend the TTL at most this often. Default 1 day. */
  renewAfterSeconds?: number;
}

export type SessionRecord<M extends object = Record<string, unknown>> = M & {
  userId: string;
  createdAt: number;
  renewedAt: number;
};

export interface ActiveSession<M extends object = Record<string, unknown>> {
  /** A short, non-secret identifier, safe to show ("signed in on …"). */
  id: string;
  record: SessionRecord<M>;
  expiresAt: number | null;
}

const USER_ID = /^[A-Za-z0-9_-]{1,128}$/;
const SESSION_TOKEN = /^([A-Za-z0-9_-]{1,128})\.([A-Za-z0-9_-]{43})$/;

/**
 * Server-side sessions. The token handed to the client is
 * `<userId>.<secret>`; emberkv stores the session under
 * `<userId>:<sha256(secret)>`, so a leaked log or backup never contains a
 * usable token, and every session of a user can be listed or revoked with
 * one prefix scan.
 */
export class Sessions<M extends object = Record<string, unknown>> {
  readonly #ns: Namespace;
  readonly #ttl: number;
  readonly #renewAfterMs: number;

  constructor(ns: Namespace, options: SessionOptions = {}) {
    this.#ns = ns;
    this.#ttl = options.ttlSeconds ?? 7 * 24 * 60 * 60;
    this.#renewAfterMs = (options.renewAfterSeconds ?? 24 * 60 * 60) * 1000;
  }

  async #key(userId: string, secret: string): Promise<string> {
    return `${userId}:${await sha256Hex(secret)}`;
  }

  /** Creates a session and returns the token to put in a cookie. */
  async create(userId: string, metadata?: M): Promise<string> {
    if (!USER_ID.test(userId)) throw new TypeError("userId must be 1-128 characters of A-Z, a-z, 0-9, '_' or '-'");
    const secret = randomToken();
    const now = Date.now();
    const record = { ...metadata, userId, createdAt: now, renewedAt: now } as SessionRecord<M>;
    await this.#ns.put(await this.#key(userId, secret), record, { ttl: ttlSeconds(this.#ttl) });
    return `${userId}.${secret}`;
  }

  /**
   * The session behind `token`, or `null` if it is malformed, expired or
   * revoked. Throws (fails closed) if emberkv is unreachable: without the
   * store nobody can tell who the caller is.
   */
  async resolve(token: string): Promise<SessionRecord<M> | null> {
    const match = SESSION_TOKEN.exec(token);
    if (!match) return null;
    const [, userId, secret] = match;
    const key = await this.#key(userId, secret);
    const entry = await this.#ns.get<SessionRecord<M>>(key);
    if (!entry || entry.value.userId !== userId) return null;
    if (Date.now() - entry.value.renewedAt > this.#renewAfterMs) {
      // `ifMatch` makes concurrent renewals harmless: one wins, the rest no-op.
      const renewed = { ...entry.value, renewedAt: Date.now() };
      await this.#ns.put(key, renewed, { ttl: ttlSeconds(this.#ttl), ifMatch: entry.etag }).catch(() => undefined);
    }
    return entry.value;
  }

  /** Signs out one session. */
  async destroy(token: string): Promise<void> {
    const match = SESSION_TOKEN.exec(token);
    if (match) await this.#ns.delete(await this.#key(match[1], match[2]));
  }

  /** Every live session of a user, most recently active first. */
  async list(userId: string): Promise<ActiveSession<M>[]> {
    const sessions: ActiveSession<M>[] = [];
    for await (const item of this.#ns.scan<SessionRecord<M>>({ prefix: `${userId}:` })) {
      if (item.value) sessions.push({ id: item.key.slice(userId.length + 1, userId.length + 13), record: item.value, expiresAt: item.expiresAt });
    }
    return sessions.sort((a, b) => b.record.renewedAt - a.record.renewedAt);
  }

  /** Signs a user out everywhere. Returns how many sessions were removed. */
  async destroyAll(userId: string): Promise<number> {
    const keys: string[] = [];
    for await (const item of this.#ns.scan({ prefix: `${userId}:` })) keys.push(item.key);
    for (let i = 0; i < keys.length; i += 1000) {
      await this.#ns.batch(keys.slice(i, i + 1000).map((key) => ({ op: "delete" as const, key })));
    }
    return keys.length;
  }
}

// ---------------------------------------------------------------- rate limits

export interface LimitDecision {
  allowed: boolean;
  retryAfterMs: number;
  /** True when emberkv was unreachable and the request was let through. */
  degraded: boolean;
}

/**
 * Fixed-window rate limiting. Subjects are hashed before they become keys.
 * Fails **open**: if emberkv is down the request proceeds and the event is
 * reported through `onDegraded`.
 */
export class RateLimiter {
  readonly #ns: Namespace;
  readonly #degraded: (feature: string, error: unknown) => void;

  constructor(ns: Namespace, onDegraded: (feature: string, error: unknown) => void) {
    this.#ns = ns;
    this.#degraded = onDegraded;
  }

  async check(subject: string, rule: RateLimitRule): Promise<LimitDecision> {
    try {
      const decision = await this.#ns.rateLimit(await opaqueKey(subject), rule);
      return { allowed: decision.allowed, retryAfterMs: decision.retryAfterMs ?? 0, degraded: false };
    } catch (error) {
      if (!isUnavailable(error)) throw error;
      this.#degraded("rate-limit", error);
      return { allowed: true, retryAfterMs: 0, degraded: true };
    }
  }

  /** Checks several limits at once; allowed only if every one allows. */
  async checkAll(checks: Array<[subject: string, rule: RateLimitRule]>): Promise<LimitDecision> {
    const decisions = await Promise.all(checks.map(([subject, rule]) => this.check(subject, rule)));
    return {
      allowed: decisions.every((d) => d.allowed),
      retryAfterMs: Math.max(0, ...decisions.map((d) => d.retryAfterMs)),
      degraded: decisions.some((d) => d.degraded),
    };
  }
}

// ---------------------------------------------------------------- idempotency

/** Thrown by `Idempotency.once`; `code` says why. */
export class IdempotencyError extends Error {
  readonly code: "invalid_key" | "in_progress";

  constructor(code: "invalid_key" | "in_progress", message: string) {
    super(message);
    this.name = "IdempotencyError";
    this.code = code;
  }
}

export interface IdempotencyOptions {
  /** How long a claim blocks duplicates while the first request runs. Default 2 minutes. */
  pendingTtl?: string;
  /** How long the result is replayed to duplicates. Default 24 hours. */
  resultTtl?: string;
}

type Claim<T> = { state: "pending"; at: number } | { state: "done"; at: number; result: T };

const IDEMPOTENCY_KEY = /^[A-Za-z0-9_-]{8,128}$/;

/**
 * Exactly-once request handling. The first request with a given key claims
 * it with an atomic create-only write, runs, and stores its result. A double
 * click, a client retry or a re-submitted form gets the stored result back
 * instead of creating a second order or charging twice.
 */
export class Idempotency {
  readonly #ns: Namespace;
  readonly #degraded: (feature: string, error: unknown) => void;
  readonly #pendingTtl: string;
  readonly #resultTtl: string;

  constructor(ns: Namespace, onDegraded: (feature: string, error: unknown) => void, options: IdempotencyOptions = {}) {
    this.#ns = ns;
    this.#degraded = onDegraded;
    this.#pendingTtl = options.pendingTtl ?? "2m";
    this.#resultTtl = options.resultTtl ?? "24h";
  }

  /**
   * Runs `run` once per `(scope, key)`. Fails open if emberkv is down: your
   * own database constraints are then the only duplicate protection.
   * Results must be JSON-serializable.
   */
  async once<T>(scope: string, key: string, run: () => Promise<T>): Promise<{ result: T; replayed: boolean }> {
    if (!IDEMPOTENCY_KEY.test(key)) throw new IdempotencyError("invalid_key", "the idempotency key must be 8-128 URL-safe characters");
    const id = `${scope}:${key}`;
    let claimed: boolean;
    try {
      claimed = await this.#ns.putIfAbsent(id, { state: "pending", at: Date.now() } satisfies Claim<T>, { ttl: this.#pendingTtl });
    } catch (error) {
      if (!isUnavailable(error)) throw error;
      this.#degraded("idempotency", error);
      return { result: await run(), replayed: false };
    }

    if (!claimed) {
      const existing = await this.#ns.get<Claim<T>>(id);
      if (existing?.value.state === "done") return { result: existing.value.result, replayed: true };
      throw new IdempotencyError("in_progress", "this request is already being processed");
    }

    try {
      const result = await run();
      await this.#ns
        .put(id, { state: "done", at: Date.now(), result } satisfies Claim<T>, { ttl: this.#resultTtl })
        .catch((error: unknown) => this.#degraded("idempotency-store", error));
      return { result, replayed: false };
    } catch (error) {
      // Release the claim so the caller can fix the input and try again.
      await this.#ns.delete(id).catch(() => undefined);
      throw error;
    }
  }
}

// ---------------------------------------------------------------- leases

export interface LeaseOwner {
  id: string;
  /** Shown to others ("Budi is editing"). */
  name?: string;
}

export interface LeaseHolder {
  ownerId: string;
  name: string | null;
  since: number;
}

export type LeaseResult =
  | { acquired: true; degraded: boolean }
  | { acquired: false; holder: LeaseHolder; expiresAt: number | null };

/**
 * Advisory leases ("someone else is editing this document"): a create-only
 * key with a TTL, renewed by a heartbeat. If the holder crashes, the lease
 * simply expires. Leases are a courtesy: use a version check in your
 * database for correctness. Fails open (acquired, `degraded: true`).
 */
export class Leases {
  readonly #ns: Namespace;
  readonly #degraded: (feature: string, error: unknown) => void;
  readonly #ttl: string;

  constructor(ns: Namespace, onDegraded: (feature: string, error: unknown) => void, ttlSecondsValue = 60) {
    this.#ns = ns;
    this.#degraded = onDegraded;
    this.#ttl = ttlSeconds(ttlSecondsValue);
  }

  async acquire(resource: string, owner: LeaseOwner): Promise<LeaseResult> {
    try {
      const mine: LeaseHolder = { ownerId: owner.id, name: owner.name ?? null, since: Date.now() };
      if (await this.#ns.putIfAbsent(resource, mine, { ttl: this.#ttl })) return { acquired: true, degraded: false };
      const current = await this.#ns.get<LeaseHolder>(resource);
      if (!current) return this.acquire(resource, owner); // expired between the two calls
      if (current.value.ownerId !== owner.id) return { acquired: false, holder: current.value, expiresAt: current.expiresAt };
      // Our own lease (another tab): refresh it.
      await this.#ns.put(resource, current.value, { ttl: this.#ttl, ifMatch: current.etag }).catch(() => undefined);
      return { acquired: true, degraded: false };
    } catch (error) {
      if (!isUnavailable(error)) throw error;
      this.#degraded("leases", error);
      return { acquired: true, degraded: true };
    }
  }

  /** Extends our lease, or re-takes it if it lapsed and is free. False only if someone else holds it. */
  async renew(resource: string, owner: LeaseOwner): Promise<boolean> {
    try {
      const current = await this.#ns.get<LeaseHolder>(resource);
      if (!current) return (await this.acquire(resource, owner)).acquired;
      if (current.value.ownerId !== owner.id) return false;
      await this.#ns.put(resource, current.value, { ttl: this.#ttl, ifMatch: current.etag });
      return true;
    } catch (error) {
      if (error instanceof EmberKVError && error.code === "precondition_failed") return this.renew(resource, owner);
      if (isUnavailable(error)) return true;
      throw error;
    }
  }

  /** Releases our lease; never deletes one that someone else took a moment ago. */
  async release(resource: string, ownerId: string): Promise<void> {
    try {
      const current = await this.#ns.get<LeaseHolder>(resource);
      if (current?.value.ownerId === ownerId) await this.#ns.delete(resource, { ifMatch: current.etag });
    } catch (error) {
      if (!isUnavailable(error) && !(error instanceof EmberKVError && error.code === "precondition_failed")) throw error;
    }
  }

  async holder(resource: string): Promise<LeaseHolder | null> {
    try {
      return (await this.#ns.get<LeaseHolder>(resource))?.value ?? null;
    } catch (error) {
      if (isUnavailable(error)) return null;
      throw error;
    }
  }
}

// ---------------------------------------------------------------- cache

export interface CacheRead<T> {
  value: T;
  computedAt: number;
  /** `"cache"` for a hit, `"origin"` when `load` ran. */
  source: "cache" | "origin";
}

/**
 * A read-through cache shared by every instance of your app, unlike an
 * in-process cache. Invalidate explicitly when the source changes; the TTL
 * is a safety net. Bypassed (computed from origin) when emberkv is down.
 */
export class Cache {
  readonly #ns: Namespace;
  readonly #degraded: (feature: string, error: unknown) => void;

  constructor(ns: Namespace, onDegraded: (feature: string, error: unknown) => void) {
    this.#ns = ns;
    this.#degraded = onDegraded;
  }

  async readThrough<T>(
    key: string,
    options: { ttl: string; load: () => Promise<T>; isFresh?: (value: T) => boolean },
  ): Promise<CacheRead<T>> {
    try {
      const hit = await this.#ns.get<{ value: T; computedAt: number }>(key);
      if (hit && (options.isFresh?.(hit.value.value) ?? true)) return { ...hit.value, source: "cache" };
    } catch (error) {
      if (!isUnavailable(error)) throw error;
      this.#degraded("cache-read", error);
    }
    const fresh = { value: await options.load(), computedAt: Date.now() };
    await this.#ns.put(key, fresh, { ttl: options.ttl }).catch((error: unknown) => this.#degraded("cache-write", error));
    return { ...fresh, source: "origin" };
  }

  /** Best effort: a failed invalidation only delays freshness by one TTL. */
  async invalidate(...keys: string[]): Promise<void> {
    if (keys.length === 0) return;
    await this.#ns
      .batch(keys.map((key) => ({ op: "delete" as const, key })))
      .catch((error: unknown) => this.#degraded("cache-invalidate", error));
  }
}

// ---------------------------------------------------------------- kit

export interface Kit<M extends object = Record<string, unknown>> {
  client: EmberKV;
  sessions: Sessions<M>;
  rateLimit: RateLimiter;
  idempotency: Idempotency;
  leases: Leases;
  cache: Cache;
}

function logDegraded(feature: string, error: unknown): void {
  const detail = error instanceof Error ? error.message : String(error);
  console.warn(JSON.stringify({ level: "warn", msg: "emberkv degraded", feature, detail }));
}

/** Every feature, wired to one client, with sensible defaults. */
export function createKit<M extends object = Record<string, unknown>>(
  options: KitOptions & { sessions?: SessionOptions; idempotency?: IdempotencyOptions; leaseTtlSeconds?: number },
): Kit<M> {
  const client =
    options.client ??
    new EmberKV({
      baseUrl: options.baseUrl ?? "http://127.0.0.1:8080",
      token: options.token,
      timeoutMs: options.timeoutMs ?? 1500,
      fetch: options.fetch,
    });
  const names = { ...DEFAULT_NAMESPACES, ...options.namespaces };
  const degraded = options.onDegraded ?? logDegraded;
  return {
    client,
    sessions: new Sessions<M>(client.namespace(names.sessions), options.sessions),
    rateLimit: new RateLimiter(client.namespace(names.limits), degraded),
    idempotency: new Idempotency(client.namespace(names.idempotency), degraded, options.idempotency),
    leases: new Leases(client.namespace(names.locks), degraded, options.leaseTtlSeconds),
    cache: new Cache(client.namespace(names.cache), degraded),
  };
}
