use std::ops::Bound;
use std::time::Instant;

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use emberkv_engine::prefix_upper_bound;
use serde::de::{DeserializeOwned, IgnoredAny};
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::value::RawValue;
use tracing::error;

use super::{ApiError, AppState, Caller};
use crate::auth::Access;
use crate::keyspace;
use crate::state_machine::{BatchOp, Command, Condition, Outcome, etag};
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

/// Hands a write to the executor (local engine or Raft) and records metrics.
async fn write(state: &AppState, op: &'static str, command: Command) -> Result<Outcome, ApiError> {
    let started = Instant::now();
    let result = state.executor.execute(command).await.map_err(ApiError::from);
    telemetry::record_request("http", op, result.is_err(), started.elapsed());
    result
}

fn not_a_counter() -> ApiError {
    ApiError::conflict("not_a_counter", "the key holds a non-integer value")
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

/// A TTL in milliseconds; relative, so it is resolved when the write applies.
fn parse_ttl(ttl: Option<&str>) -> Result<Option<u64>, ApiError> {
    ttl.map(|text| {
        keyspace::parse_duration(text)
            .map(|d| d.as_millis() as u64)
            .map_err(|msg| ApiError::bad_request("invalid_ttl", msg))
    })
    .transpose()
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

/// Conditional-request headers for writes (RFC 9110 §13): `If-Match` (the
/// current ETag must be one of these, or `*` = exists) and `If-None-Match: *`
/// (the key must not exist).
struct Precondition;

impl Precondition {
    fn from_headers(headers: &HeaderMap) -> Result<Option<Condition>, ApiError> {
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
                Ok(Some(Condition::IfMatch(tags.split(',').map(|t| t.trim().to_owned()).collect())))
            }
            (None, Some("*")) => Ok(Some(Condition::IfNoneMatch)),
            (None, Some(_)) => {
                Err(ApiError::bad_request("invalid_header", "only `If-None-Match: *` is supported"))
            }
            (None, None) => Ok(None),
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
    let ttl_ms = parse_ttl(params.ttl.as_deref())?;
    let condition = Precondition::from_headers(&headers)?;
    let value = body.to_vec();
    let tag = etag(&value);
    let command = Command::Put { key: storage_key, value, ttl_ms, condition };
    match write(&state, "put", command).await? {
        Outcome::Written => Ok((StatusCode::NO_CONTENT, [(header::ETAG, tag)]).into_response()),
        Outcome::PreconditionFailed => Err(ApiError::precondition_failed()),
        other => Err(unexpected(&other)),
    }
}

pub(crate) async fn delete_key(
    State(state): State<AppState>,
    caller: Caller,
    Path((namespace, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let storage_key = target(&namespace, &key)?;
    caller.require(&namespace, Access::ReadWrite)?;
    let condition = match Precondition::from_headers(&headers)? {
        Some(Condition::IfNoneMatch) => {
            return Err(ApiError::bad_request("invalid_header", "If-None-Match is not supported on DELETE"));
        }
        other => other,
    };
    match write(&state, "delete", Command::Delete { key: storage_key, condition }).await? {
        Outcome::Deleted { existed: true } => Ok(StatusCode::NO_CONTENT),
        Outcome::Deleted { existed: false } => Err(ApiError::not_found(format!("key {key:?} not found"))),
        Outcome::PreconditionFailed => Err(ApiError::precondition_failed()),
        other => Err(unexpected(&other)),
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

    let mut ops = Vec::with_capacity(count);
    for (i, op) in request.operations.into_iter().enumerate() {
        let key = target(&namespace, &op.key)?;
        let invalid = |msg: &str| ApiError::bad_request("invalid_batch", format!("operation {i}: {msg}"));
        match op.op {
            BatchOpKind::Put => {
                let value = op.value.ok_or_else(|| invalid("put requires a non-null `value`"))?;
                let ttl_ms = parse_ttl(op.ttl.as_deref())?;
                ops.push(BatchOp { key, value: Some(value.get().as_bytes().to_vec()), ttl_ms });
            }
            BatchOpKind::Delete => {
                if op.value.is_some() || op.ttl.is_some() {
                    return Err(invalid("delete takes no `value` or `ttl`"));
                }
                ops.push(BatchOp { key, value: None, ttl_ms: None });
            }
        }
    }

    match write(&state, "batch", Command::Batch { ops }).await? {
        Outcome::Written => Ok(Json(json!({ "applied": count }))),
        other => Err(unexpected(&other)),
    }
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
    let ttl_ms = parse_ttl(request.ttl.as_deref())?;
    let command = Command::Increment { key: storage_key, by: request.by, ttl_ms };
    match write(&state, "increment", command).await? {
        Outcome::Counter { value, expires_at } => {
            Ok(Json(json!({ "value": value, "expires_at": expires_at })))
        }
        Outcome::NotACounter => Err(not_a_counter()),
        Outcome::CounterOverflow => Err(ApiError::conflict("counter_overflow", "the counter would overflow")),
        other => Err(unexpected(&other)),
    }
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
    let command = Command::RateLimit { key: storage_key, limit, window_ms: window, cost };
    match write(&state, "rate_limit", command).await? {
        Outcome::RateLimit(decision) => {
            serde_json::to_value(decision).map(Json).map_err(|_| ApiError::internal())
        }
        Outcome::NotACounter => Err(not_a_counter()),
        other => Err(unexpected(&other)),
    }
}

/// A command produced an outcome its handler does not expect: a bug.
fn unexpected(outcome: &Outcome) -> ApiError {
    error!(?outcome, "unexpected outcome for command");
    ApiError::internal()
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
