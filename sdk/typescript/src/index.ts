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
