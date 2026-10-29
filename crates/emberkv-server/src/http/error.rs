use axum::Json;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use emberkv_engine::Error as EngineError;
use serde_json::json;
use tracing::error;

/// Every error leaves the API as `{"error": {"code": "...", "message": "..."}}`.
/// `code` is stable and machine-readable; `message` is for humans.
#[derive(Debug)]
pub(crate) struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    pub(crate) fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self { status, code, message: message.into() }
    }

    pub(crate) fn bad_request(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, code, message)
    }

    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", message)
    }

    pub(crate) fn unauthenticated() -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthenticated", "missing or invalid bearer token")
    }

    pub(crate) fn forbidden(message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, "forbidden", message)
    }

    pub(crate) fn conflict(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, code, message)
    }

    pub(crate) fn precondition_failed() -> Self {
        Self::new(StatusCode::PRECONDITION_FAILED, "precondition_failed", "the key changed since you read it")
    }

    pub(crate) fn internal() -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", "internal error")
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(json!({ "error": { "code": self.code, "message": self.message } }));
        let mut response = (self.status, body).into_response();
        if self.status == StatusCode::UNAUTHORIZED {
            response.headers_mut().insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        response
    }
}

/// Client mistakes are reported verbatim; internal failures are logged with
/// full detail but surfaced to clients generically.
impl From<EngineError> for ApiError {
    fn from(err: EngineError) -> Self {
        match err {
            EngineError::EmptyKey => Self::bad_request("invalid_key", err.to_string()),
            EngineError::KeyTooLarge { .. }
            | EngineError::ValueTooLarge { .. }
            | EngineError::BatchTooLarge { .. } => {
                Self::new(StatusCode::PAYLOAD_TOO_LARGE, "too_large", err.to_string())
            }
            EngineError::Poisoned => {
                error!(error = %err, "storage engine is poisoned");
                Self::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "unavailable",
                    "storage is read-only after an I/O failure",
                )
            }
            other => {
                error!(error = %other, "storage error");
                Self::internal()
            }
        }
    }
}
