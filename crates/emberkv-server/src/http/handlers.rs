use std::ops::Bound;
use std::time::Instant;

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use emberkv_engine::{Entry, Expiry, Mutation, WriteBatch, prefix_upper_bound};
use serde::de::{DeserializeOwned, IgnoredAny};
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};
use tracing::error;

use super::{ApiError, AppState, Caller};
use crate::auth::Access;
use crate::keyspace;
use crate::telemetry;

const DEFAULT_PAGE_SIZE: u32 = 100;
const MAX_BATCH_OPS: usize = 1_000;
const EXPIRES_AT: &str = "x-expires-at";

// ---------------------------------------------------------------- helpers

/// Runs blocking engine work on the blocking pool and records metrics.
async fn run<T: Send + 'static>(
    op: &'static str,
    work: impl FnOnce() -> Result<T, ApiError> + Send + 'static,
) -> Result<T, ApiError> {
    let started = Instant::now();
    let result = tokio::task::spawn_blocking(work).await.unwrap_or_else(|err| {
        error!(op, error = %err, "request handler panicked");
        Err(ApiError::internal())
    });
    telemetry::record_request("http", op, result.is_err(), started.elapsed());
    result
}

/// Validates the namespace and key, returning the engine key.
fn target(namespace: &str, key: &str) -> Result<Vec<u8>, ApiError> {
    keyspace::validate_namespace(namespace).map_err(|msg| ApiError::bad_request("invalid_namespace", msg))?;
    if key.is_empty() {
        return Err(ApiError::bad_request("invalid_key", "key must not be empty"));
    }
    Ok(keyspace::storage_key(namespace, key))
}

fn check_namespace(namespace: &str) -> Result<(), ApiError> {
    keyspace::validate_namespace(namespace).map_err(|msg| ApiError::bad_request("invalid_namespace", msg))
}

fn parse_ttl(ttl: Option<&str>) -> Result<Expiry, ApiError> {
    match ttl {
        None => Ok(Expiry::Never),
        Some(text) => keyspace::parse_duration(text)
            .map(Expiry::After)
            .map_err(|msg| ApiError::bad_request("invalid_ttl", msg)),
    }
}

fn parse_body<T: DeserializeOwned>(body: &[u8]) -> Result<T, ApiError> {
    serde_json::from_slice(body).map_err(|err| ApiError::bad_request("invalid_json", err.to_string()))
}

fn is_json(bytes: &[u8]) -> bool {
    std::str::from_utf8(bytes).is_ok_and(|text| serde_json::from_str::<IgnoredAny>(text).is_ok())
}

/// The stored value as embedded JSON, or the original bytes if it is not JSON
/// (e.g. written through the binary protocol).
fn json_value(bytes: Vec<u8>) -> Result<Box<RawValue>, Vec<u8>> {
    match String::from_utf8(bytes) {
        Ok(text) => serde_json::from_str::<Box<RawValue>>(&text).map_err(|_| text.into_bytes()),
        Err(err) => Err(err.into_bytes()),
    }
}

/// A strong validator derived from the value itself, so no version counter
/// has to be stored: equal values have equal ETags.
fn etag(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    let hex: String = digest[..8].iter().map(|b| format!("{b:02x}")).collect();
    format!("\"{hex}\"")
}

/// Conditional-request headers for writes (RFC 9110 §13).
enum Precondition {
    /// `If-Match`: the current ETag must be one of these (or `*` = exists).
    IfMatch(Vec<String>),
    /// `If-None-Match: *`: the key must not exist (create-only).
    IfNoneMatchAny,
}

impl Precondition {
    fn from_headers(headers: &HeaderMap) -> Result<Option<Self>, ApiError> {
        let read =
            |name| {
                headers.get(name).map(|v: &HeaderValue| v.to_str().map(str::trim)).transpose().map_err(|_| {
                    ApiError::bad_request("invalid_header", "precondition headers must be ASCII")
                })
            };
        match (read(header::IF_MATCH)?, read(header::IF_NONE_MATCH)?) {
            (Some(_), Some(_)) => Err(ApiError::bad_request(
                "invalid_header",
                "send either If-Match or If-None-Match, not both",
            )),
            (Some(tags), None) => {
                Ok(Some(Self::IfMatch(tags.split(',').map(|t| t.trim().to_owned()).collect())))
            }
            (None, Some("*")) => Ok(Some(Self::IfNoneMatchAny)),
            (None, Some(_)) => {
                Err(ApiError::bad_request("invalid_header", "only `If-None-Match: *` is supported"))
            }
            (None, None) => Ok(None),
        }
    }

    fn allows(&self, current: Option<&Entry>) -> bool {
        match self {
            Self::IfNoneMatchAny => current.is_none(),
            Self::IfMatch(tags) => {
                current.is_some_and(|entry| tags.iter().any(|tag| tag == "*" || *tag == etag(&entry.value)))
            }
        }
    }
}

// ---------------------------------------------------------------- probes

pub(crate) async fn healthz() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

/// Ready = able to accept writes. Load balancers should stop routing here
/// while draining for shutdown or after a storage failure.
pub(crate) async fn readyz(State(state): State<AppState>) -> Response {
    if state.engine.is_poisoned() || state.shutdown.is_cancelled() {
        (StatusCode::SERVICE_UNAVAILABLE, Json(json!({ "status": "unavailable" }))).into_response()
    } else {
        Json(json!({ "status": "ready" })).into_response()
    }
}

pub(crate) async fn not_found() -> ApiError {
    ApiError::not_found("no such route; see docs/API.md")
}

// ---------------------------------------------------------------- keys

pub(crate) async fn get_key(
    State(state): State<AppState>,
    caller: Caller,
    Path((namespace, key)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let storage_key = target(&namespace, &key)?;
    caller.require(&namespace, Access::Read)?;
    let engine = state.engine;
    let entry = run("get", move || Ok(engine.get_entry(&storage_key)?)).await?;
    let entry = entry.ok_or_else(|| ApiError::not_found(format!("key {key:?} not found")))?;

    let mut headers = HeaderMap::new();
    let content_type = if is_json(&entry.value) { "application/json" } else { "application/octet-stream" };
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    headers
        .insert(header::ETAG, HeaderValue::try_from(etag(&entry.value)).map_err(|_| ApiError::internal())?);
    if let Some(expires_at) = entry.expires_at_ms {
        headers.insert(EXPIRES_AT, HeaderValue::from(expires_at));
    }
    Ok((headers, entry.value).into_response())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PutParams {
    ttl: Option<String>,
}

pub(crate) async fn put_key(
    State(state): State<AppState>,
    caller: Caller,
    Path((namespace, key)): Path<(String, String)>,
    Query(params): Query<PutParams>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let storage_key = target(&namespace, &key)?;
    caller.require(&namespace, Access::ReadWrite)?;
    if !is_json(&body) {
        return Err(ApiError::bad_request("invalid_json", "the request body must be a JSON document"));
    }
    let expiry = parse_ttl(params.ttl.as_deref())?;
    let precondition = Precondition::from_headers(&headers)?;
    let value = body.to_vec();
    let tag = etag(&value);
    let engine = state.engine;

    run("put", move || match precondition {
        None => Ok(engine.put_with_expiry(&storage_key, &value, expiry)?),
        Some(precondition) => engine.update(&storage_key, |current| {
            if precondition.allows(current) {
                (Mutation::Put { value, expiry }, Ok(()))
            } else {
                (Mutation::Keep, Err(ApiError::precondition_failed()))
            }
        })?,
    })
    .await?;
    Ok((StatusCode::NO_CONTENT, [(header::ETAG, tag)]).into_response())
}

pub(crate) async fn delete_key(
    State(state): State<AppState>,
    caller: Caller,
    Path((namespace, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let storage_key = target(&namespace, &key)?;
    caller.require(&namespace, Access::ReadWrite)?;
    let precondition = match Precondition::from_headers(&headers)? {
        Some(Precondition::IfNoneMatchAny) => {
            return Err(ApiError::bad_request("invalid_header", "If-None-Match is not supported on DELETE"));
        }
        other => other,
    };
    let engine = state.engine;
    let existed = run("delete", move || match precondition {
        None => Ok(engine.delete(&storage_key)?),
        Some(precondition) => engine.update(&storage_key, |current| {
            if precondition.allows(current) {
                (Mutation::Delete, Ok(true))
            } else {
                (Mutation::Keep, Err(ApiError::precondition_failed()))
            }
        })?,
    })
    .await?;
    if existed {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::not_found(format!("key {key:?} not found")))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ListParams {
    prefix: Option<String>,
    cursor: Option<String>,
    limit: Option<u32>,
}

#[derive(Serialize)]
struct ListItem {
    key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<Box<RawValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    value_base64: Option<String>,
    expires_at: Option<u64>,
    etag: String,
}

#[derive(Serialize)]
struct ListResponse {
    items: Vec<ListItem>,
    next_cursor: Option<String>,
}

/// Keyset pagination: `cursor` is the last key of the previous page, so
/// pages stay correct while keys are inserted or deleted concurrently.
pub(crate) async fn list_keys(
    State(state): State<AppState>,
    caller: Caller,
    Path(namespace): Path<String>,
    Query(params): Query<ListParams>,
) -> Result<Json<serde_json::Value>, ApiError> {
    check_namespace(&namespace)?;
    caller.require(&namespace, Access::Read)?;
    let limit = params.limit.unwrap_or(DEFAULT_PAGE_SIZE).clamp(1, state.max_scan_limit) as usize;
    let prefix = keyspace::storage_key(&namespace, params.prefix.as_deref().unwrap_or(""));
    let start = match params.cursor {
        Some(cursor) => {
            let after = keyspace::storage_key(&namespace, &cursor);
            if after >= prefix { Bound::Excluded(after) } else { Bound::Included(prefix.clone()) }
        }
        None => Bound::Included(prefix.clone()),
    };
    // Never `None`: the prefix ends with '/', which has a successor.
    let end = prefix_upper_bound(&prefix);

    let engine = state.engine;
    let mut entries = run("scan", move || {
        let end = end.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
        Ok(engine.scan_entries((start.as_ref().map(Vec::as_slice), end), limit + 1)?)
    })
    .await?;

    let has_more = entries.len() > limit;
    entries.truncate(limit);
    let next_cursor =
        has_more.then(|| entries.last().map(|(k, _)| keyspace::user_key(&namespace, k))).flatten();
    let items = entries
        .into_iter()
        .map(|(storage_key, entry)| {
            let etag = etag(&entry.value);
            let (value, value_base64) = match json_value(entry.value) {
                Ok(json) => (Some(json), None),
                Err(bytes) => (None, Some(base64::engine::general_purpose::STANDARD.encode(bytes))),
            };
            ListItem {
                key: keyspace::user_key(&namespace, &storage_key),
                value,
                value_base64,
                expires_at: entry.expires_at_ms,
                etag,
            }
        })
        .collect();
    let response = ListResponse { items, next_cursor };
    serde_json::to_value(response).map(Json).map_err(|_| ApiError::internal())
}

// ---------------------------------------------------------------- batch

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchRequest {
    operations: Vec<BatchOperation>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchOperation {
    op: BatchOpKind,
    key: String,
    #[serde(default)]
    value: Option<Box<RawValue>>,
    #[serde(default)]
    ttl: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum BatchOpKind {
    Put,
    Delete,
}

pub(crate) async fn batch(
    State(state): State<AppState>,
    caller: Caller,
    Path(namespace): Path<String>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, ApiError> {
    check_namespace(&namespace)?;
    caller.require(&namespace, Access::ReadWrite)?;
    let request: BatchRequest = parse_body(&body)?;
    let count = request.operations.len();
    if count == 0 || count > MAX_BATCH_OPS {
        return Err(ApiError::bad_request(
            "invalid_batch",
            format!("a batch needs 1-{MAX_BATCH_OPS} operations"),
        ));
    }

    let mut batch = WriteBatch::new();
    for (i, op) in request.operations.into_iter().enumerate() {
        let key = target(&namespace, &op.key)?;
        let invalid = |msg: &str| ApiError::bad_request("invalid_batch", format!("operation {i}: {msg}"));
        match op.op {
            BatchOpKind::Put => {
                let value = op.value.ok_or_else(|| invalid("put requires a non-null `value`"))?;
                let expiry = parse_ttl(op.ttl.as_deref())?;
                batch.put_with_expiry(key, value.get().as_bytes(), expiry);
            }
            BatchOpKind::Delete => {
                if op.value.is_some() || op.ttl.is_some() {
                    return Err(invalid("delete takes no `value` or `ttl`"));
                }
                batch.delete(key);
            }
        }
    }

    let engine = state.engine;
    run("batch", move || Ok(engine.write_batch(&batch)?)).await?;
    Ok(Json(json!({ "applied": count })))
}

// ---------------------------------------------------------------- counters

#[derive(Deserialize)]
#[serde(deny_unknown_fields, default)]
struct IncrementRequest {
    by: i64,
    ttl: Option<String>,
}

impl Default for IncrementRequest {
    fn default() -> Self {
        Self { by: 1, ttl: None }
    }
}

fn parse_counter<T: std::str::FromStr>(value: &[u8]) -> Option<T> {
    std::str::from_utf8(value).ok()?.trim().parse().ok()
}

/// Atomically adds `by` to an integer counter, creating it at 0 if absent.
/// `ttl` applies only when the counter is created.
pub(crate) async fn increment(
    State(state): State<AppState>,
    caller: Caller,
    Path((namespace, key)): Path<(String, String)>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, ApiError> {
    let storage_key = target(&namespace, &key)?;
    caller.require(&namespace, Access::ReadWrite)?;
    let request: IncrementRequest =
        if body.is_empty() { IncrementRequest::default() } else { parse_body(&body)? };
    let ttl = request.ttl.as_deref().map(keyspace::parse_duration).transpose();
    let ttl = ttl.map_err(|msg| ApiError::bad_request("invalid_ttl", msg))?;

    let engine = state.engine;
    let (value, expires_at) = run("increment", move || {
        engine.update(&storage_key, |current| {
            let now = engine.now_ms();
            let (count, expires_at) = match current {
                None => (0, ttl.map(|ttl| now.saturating_add(ttl.as_millis() as u64))),
                Some(entry) => match parse_counter::<i64>(&entry.value) {
                    Some(count) => (count, entry.expires_at_ms),
                    None => {
                        let err = ApiError::conflict("not_a_counter", "the key holds a non-integer value");
                        return (Mutation::Keep, Err(err));
                    }
                },
            };
            let Some(next) = count.checked_add(request.by) else {
                return (
                    Mutation::Keep,
                    Err(ApiError::conflict("counter_overflow", "the counter would overflow")),
                );
            };
            let expiry = match (current, expires_at) {
                (Some(_), _) => Expiry::KeepExisting,
                (None, Some(at)) => Expiry::AtUnixMs(at),
                (None, None) => Expiry::Never,
            };
            (Mutation::Put { value: next.to_string().into_bytes(), expiry }, Ok((next, expires_at)))
        })?
    })
    .await?;
    Ok(Json(json!({ "value": value, "expires_at": expires_at })))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RateLimitRequest {
    limit: u64,
    window: String,
    #[serde(default = "one")]
    cost: u64,
}

fn one() -> u64 {
    1
}

#[derive(Serialize)]
struct RateLimitDecision {
    allowed: bool,
    limit: u64,
    remaining: u64,
    reset_at: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_after_ms: Option<u64>,
}

/// Fixed-window rate limiting: at most `limit` units of `cost` per `window`
/// per key. Denied requests do not consume quota. Always returns 200; the
/// caller decides what a denial means (e.g. respond 429 to its own client).
pub(crate) async fn rate_limit(
    State(state): State<AppState>,
    caller: Caller,
    Path((namespace, key)): Path<(String, String)>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, ApiError> {
    let storage_key = target(&namespace, &key)?;
    caller.require(&namespace, Access::ReadWrite)?;
    let request: RateLimitRequest = parse_body(&body)?;
    if request.limit == 0 || request.cost == 0 {
        return Err(ApiError::bad_request("invalid_rate_limit", "`limit` and `cost` must be positive"));
    }
    let window = keyspace::parse_duration(&request.window)
        .map_err(|msg| ApiError::bad_request("invalid_window", msg))?
        .as_millis() as u64;
    let RateLimitRequest { limit, cost, .. } = request;

    let engine = state.engine;
    let decision = run("rate_limit", move || {
        engine.update(&storage_key, |current| {
            let now = engine.now_ms();
            let (used, reset_at) = match current {
                None => (0, Some(now.saturating_add(window))),
                Some(entry) => match parse_counter::<u64>(&entry.value) {
                    Some(used) => (used, entry.expires_at_ms),
                    None => {
                        let err = ApiError::conflict("not_a_counter", "the key holds a non-integer value");
                        return (Mutation::Keep, Err(err));
                    }
                },
            };
            let next = used.saturating_add(cost);
            if next > limit {
                let retry_after_ms = reset_at.map(|at| at.saturating_sub(now));
                let remaining = limit.saturating_sub(used);
                let denied = RateLimitDecision { allowed: false, limit, remaining, reset_at, retry_after_ms };
                return (Mutation::Keep, Ok(denied));
            }
            let expiry = match (current, reset_at) {
                (None, Some(at)) => Expiry::AtUnixMs(at),
                _ => Expiry::KeepExisting,
            };
            let allowed = RateLimitDecision {
                allowed: true,
                limit,
                remaining: limit - next,
                reset_at,
                retry_after_ms: None,
            };
            (Mutation::Put { value: next.to_string().into_bytes(), expiry }, Ok(allowed))
        })?
    })
    .await?;
    serde_json::to_value(decision).map(Json).map_err(|_| ApiError::internal())
}

// ---------------------------------------------------------------- admin

pub(crate) async fn stats(
    State(state): State<AppState>,
    caller: Caller,
) -> Result<Json<serde_json::Value>, ApiError> {
    caller.require_admin()?;
    let stats = state.engine.stats();
    Ok(Json(json!({
        "keys": stats.keys,
        "segments": stats.segments,
        "total_bytes": stats.total_bytes,
        "live_bytes": stats.live_bytes,
        "dead_bytes": stats.dead_bytes(),
        "fragmentation": stats.fragmentation(),
    })))
}

pub(crate) async fn compact(
    State(state): State<AppState>,
    caller: Caller,
) -> Result<Json<serde_json::Value>, ApiError> {
    caller.require_admin()?;
    let engine = state.engine;
    let report = run("compact", move || Ok(engine.compact()?)).await?;
    telemetry::record_compaction(&report);
    Ok(Json(json!({
        "segments_compacted": report.segments_compacted,
        "records_moved": report.records_moved,
        "records_expired": report.records_expired,
        "reclaimed_bytes": report.reclaimed_bytes(),
        "duration_ms": report.duration.as_millis() as u64,
    })))
}

/// Writes a checkpoint into the configured backup directory. Clients never
/// choose the path, so this endpoint cannot be used to write files elsewhere.
pub(crate) async fn checkpoint(
    State(state): State<AppState>,
    caller: Caller,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    caller.require_admin()?;
    let backup_dir = state.backup_dir.ok_or_else(|| {
        ApiError::conflict("backups_disabled", "start the server with --backup-dir to enable checkpoints")
    })?;
    let engine = state.engine;
    let name = format!("checkpoint-{}", engine.now_ms());
    let dest = backup_dir.join(&name);
    let report = run("checkpoint", move || {
        std::fs::create_dir_all(&backup_dir).map_err(|err| {
            error!(error = %err, dir = %backup_dir.display(), "cannot create backup directory");
            ApiError::internal()
        })?;
        Ok(engine.checkpoint(&dest)?)
    })
    .await?;
    Ok((
        StatusCode::CREATED,
        Json(json!({
            "name": name,
            "segments": report.segments,
            "bytes": report.bytes,
            "duration_ms": report.duration.as_millis() as u64,
        })),
    ))
}
