//! JSON/HTTP API for application backends. The full reference is `docs/API.md`.

mod error;
mod handlers;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::{DefaultBodyLimit, FromRequestParts};
use axum::http::request::Parts;
use axum::http::{StatusCode, header};
use axum::routing::{get, post};
use emberkv_engine::Engine;
use tokio_util::sync::CancellationToken;
use tower::ServiceBuilder;
use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

use crate::auth::{Access, Authenticator, Principal};
use crate::executor::Executor;
pub(crate) use error::ApiError;

#[derive(Clone)]
pub(crate) struct AppState {
    /// For reads and admin operations; writes go through `executor`.
    pub(crate) engine: Engine,
    pub(crate) executor: Executor,
    pub(crate) auth: Option<Arc<Authenticator>>,
    pub(crate) backup_dir: Option<PathBuf>,
    pub(crate) shutdown: CancellationToken,
    pub(crate) max_scan_limit: u32,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct HttpLimits {
    pub(crate) max_body_bytes: usize,
    pub(crate) request_timeout: Duration,
}

pub(crate) fn router(state: AppState, limits: HttpLimits) -> Router {
    let api = Router::new()
        .route("/v1/namespaces/{ns}/keys", get(handlers::list_keys))
        .route(
            "/v1/namespaces/{ns}/keys/{*key}",
            get(handlers::get_key).put(handlers::put_key).delete(handlers::delete_key),
        )
        .route("/v1/namespaces/{ns}/batch", post(handlers::batch))
        .route("/v1/namespaces/{ns}/counters/{*key}", post(handlers::increment))
        .route("/v1/namespaces/{ns}/rate-limits/{*key}", post(handlers::rate_limit))
        .route("/v1/admin/stats", get(handlers::stats))
        .route("/v1/admin/compact", post(handlers::compact))
        .route("/v1/admin/checkpoints", post(handlers::checkpoint));

    Router::new()
        .route("/healthz", get(handlers::healthz))
        .route("/readyz", get(handlers::readyz))
        .merge(api)
        .fallback(handlers::not_found)
        .layer(DefaultBodyLimit::max(limits.max_body_bytes))
        .layer(
            ServiceBuilder::new()
                .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
                .layer(TraceLayer::new_for_http())
                .layer(PropagateRequestIdLayer::x_request_id())
                .layer(TimeoutLayer::with_status_code(StatusCode::REQUEST_TIMEOUT, limits.request_timeout)),
        )
        .with_state(state)
}

/// The authenticated caller. `None` inside means authentication is disabled.
pub(crate) struct Caller(Option<Arc<Principal>>);

impl FromRequestParts<AppState> for Caller {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, ApiError> {
        let Some(auth) = &state.auth else {
            return Ok(Self(None));
        };
        let token = parts
            .headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .ok_or_else(ApiError::unauthenticated)?;
        auth.authenticate(token.trim().as_bytes())
            .map(|principal| Self(Some(principal)))
            .ok_or_else(ApiError::unauthenticated)
    }
}

impl Caller {
    pub(crate) fn require(&self, namespace: &str, access: Access) -> Result<(), ApiError> {
        match &self.0 {
            Some(principal) if !principal.can(namespace, access) => Err(ApiError::forbidden(format!(
                "key {:?} lacks {access:?} access to namespace {namespace:?}",
                principal.name()
            ))),
            _ => Ok(()),
        }
    }

    pub(crate) fn require_admin(&self) -> Result<(), ApiError> {
        match &self.0 {
            Some(principal) if !principal.is_admin() => {
                Err(ApiError::forbidden(format!("key {:?} is not an admin key", principal.name())))
            }
            _ => Ok(()),
        }
    }
}
