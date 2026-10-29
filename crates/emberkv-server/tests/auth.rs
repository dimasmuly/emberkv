//! Authentication, authorization, secure defaults, and admin operations.

mod common;

use std::sync::Arc;

use common::{TestServer, test_config};
use emberkv_client::{ClientError, ErrorCode};
use emberkv_engine::{Engine, Options};
use emberkv_server::auth::{Authenticator, hash_token};
use emberkv_server::{Server, ServerConfig};
use reqwest::StatusCode;
use reqwest::header::WWW_AUTHENTICATE;
use serde_json::{Value, json};

const READER: &str = "ekv_reader_token";
const WRITER: &str = "ekv_writer_token";
const ADMIN: &str = "ekv_admin_token";

fn authenticator() -> Arc<Authenticator> {
    let toml = format!(
        r#"
        [[key]]
        name = "flags-reader"
        token_sha256 = "{}"
        access = "read"
        namespaces = ["flags"]

        [[key]]
        name = "app-backend"
        token_sha256 = "{}"
        access = "read_write"
        namespaces = ["sessions", "flags"]

        [[key]]
        name = "ops"
        token_sha256 = "{}"
        access = "admin"
        "#,
        hash_token(READER),
        hash_token(WRITER),
        hash_token(ADMIN),
    );
    Arc::new(Authenticator::from_toml(&toml).unwrap())
}

fn secured(dir: &std::path::Path) -> ServerConfig {
    ServerConfig {
        data_dir: dir.join("data"),
        backup_dir: Some(dir.join("backups")),
        auth: Some(authenticator()),
        ..test_config()
    }
}

#[tokio::test]
async fn http_requires_tokens_and_enforces_scopes() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start_with(secured(dir.path())).await;
    let http = reqwest::Client::new();
    let session = server.url("/v1/namespaces/sessions/keys/s1");
    let flag = server.url("/v1/namespaces/flags/keys/new-checkout");

    let anonymous = http.get(&session).send().await.unwrap();
    assert_eq!(anonymous.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(anonymous.headers()[WWW_AUTHENTICATE], "Bearer");
    let forged = http.get(&session).bearer_auth("ekv_guess").send().await.unwrap();
    assert_eq!(forged.status(), StatusCode::UNAUTHORIZED);

    let write = http.put(&flag).bearer_auth(WRITER).json(&json!({ "enabled": true })).send().await.unwrap();
    assert_eq!(write.status(), StatusCode::NO_CONTENT);
    let read = http.get(&flag).bearer_auth(READER).send().await.unwrap();
    assert_eq!(read.status(), StatusCode::OK);

    let reader_writes = http.put(&flag).bearer_auth(READER).json(&json!(false)).send().await.unwrap();
    assert_eq!(reader_writes.status(), StatusCode::FORBIDDEN);
    let out_of_scope = http.get(&session).bearer_auth(READER).send().await.unwrap();
    assert_eq!(out_of_scope.status(), StatusCode::FORBIDDEN);
    let other_ns =
        http.get(server.url("/v1/namespaces/billing/keys/x")).bearer_auth(WRITER).send().await.unwrap();
    assert_eq!(other_ns.status(), StatusCode::FORBIDDEN);

    let stats = server.url("/v1/admin/stats");
    assert_eq!(http.get(&stats).bearer_auth(WRITER).send().await.unwrap().status(), StatusCode::FORBIDDEN);
    assert_eq!(http.get(&stats).bearer_auth(ADMIN).send().await.unwrap().status(), StatusCode::OK);

    // Probes stay open so orchestrators can check health without secrets.
    assert_eq!(http.get(server.url("/healthz")).send().await.unwrap().status(), StatusCode::OK);
    server.stop().await;
}

#[tokio::test]
async fn binary_protocol_is_admin_only() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start_with(secured(dir.path())).await;

    let mut anonymous = server.client().await;
    anonymous.ping().await.unwrap();
    let err = anonymous.get("x").await.unwrap_err();
    assert!(matches!(err, ClientError::Server { code: ErrorCode::Unauthorized, .. }), "{err}");

    let mut non_admin = server.client().await;
    let err = non_admin.authenticate(WRITER).await.unwrap_err();
    assert!(matches!(err, ClientError::Server { code: ErrorCode::Unauthorized, .. }), "{err}");
    assert!(non_admin.ping().await.is_err(), "a failed AUTH closes the connection");

    let mut admin = server.client().await;
    admin.authenticate(ADMIN).await.unwrap();
    admin.put("sessions/raw", "value").await.unwrap();
    assert_eq!(admin.get("sessions/raw").await.unwrap().as_deref(), Some(&b"value"[..]));
    server.stop().await;
}

#[tokio::test]
async fn refuses_to_expose_an_unauthenticated_server() {
    let dir = tempfile::tempdir().unwrap();
    let config = ServerConfig {
        data_dir: dir.path().to_path_buf(),
        http_listen: Some("0.0.0.0:0".parse().unwrap()),
        ..test_config()
    };
    let err = Server::bind(config.clone()).await.unwrap_err();
    assert!(err.to_string().contains("without authentication"), "{err}");

    // Explicit opt-in is honored.
    let opted_in = ServerConfig { allow_unauthenticated_remote: true, ..config };
    let server = TestServer::start_with(opted_in).await;
    server.stop().await;
}

#[tokio::test]
async fn admin_checkpoint_produces_a_restorable_backup() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start_with(secured(dir.path())).await;
    let http = reqwest::Client::new();

    let url = server.url("/v1/namespaces/sessions/keys/s1");
    http.put(&url).bearer_auth(WRITER).json(&json!({ "user": 42 })).send().await.unwrap();

    let denied = http.post(server.url("/v1/admin/checkpoints")).bearer_auth(WRITER).send().await.unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    let created = http.post(server.url("/v1/admin/checkpoints")).bearer_auth(ADMIN).send().await.unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    let name = created.json::<Value>().await.unwrap()["name"].as_str().unwrap().to_owned();

    // Written after the checkpoint, so it must not be in it.
    http.put(server.url("/v1/namespaces/sessions/keys/s2"))
        .bearer_auth(WRITER)
        .json(&json!(1))
        .send()
        .await
        .unwrap();
    server.stop().await;

    let restored = Engine::open(dir.path().join("backups").join(name), Options::default()).unwrap();
    assert_eq!(restored.get(b"sessions/s1").unwrap(), Some(br#"{"user":42}"#.to_vec()));
    assert_eq!(restored.get(b"sessions/s2").unwrap(), None);
}
