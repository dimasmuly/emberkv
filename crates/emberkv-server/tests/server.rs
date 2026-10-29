//! End-to-end tests: a real server on an ephemeral port, driven by the real client.

mod common;

use std::time::Duration;

use common::{TestServer, test_config};
use emberkv_client::{Client, ClientError, ErrorCode};
use emberkv_server::ServerConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::test]
async fn crud_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start(dir.path()).await;
    let mut client = server.client().await;

    client.ping().await.unwrap();
    assert_eq!(client.get("missing").await.unwrap(), None);
    client.put("lang", "rust").await.unwrap();
    assert_eq!(client.get("lang").await.unwrap().as_deref(), Some(&b"rust"[..]));
    client.put("lang", "rust 2024").await.unwrap();
    assert_eq!(client.get("lang").await.unwrap().as_deref(), Some(&b"rust 2024"[..]));
    assert!(client.delete("lang").await.unwrap());
    assert!(!client.delete("lang").await.unwrap());
    assert_eq!(client.get("lang").await.unwrap(), None);

    server.stop().await;
}

#[tokio::test]
async fn scan_prefix_respects_order_and_limit() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start(dir.path()).await;
    let mut client = server.client().await;

    for key in ["user:3", "user:1", "order:1", "user:2"] {
        client.put(key, key.to_uppercase()).await.unwrap();
    }
    let users = client.scan_prefix("user:", 10).await.unwrap();
    let keys: Vec<_> = users.iter().map(|(k, _)| k.as_ref()).collect();
    assert_eq!(keys, [&b"user:1"[..], b"user:2", b"user:3"]);
    assert_eq!(users[0].1.as_ref(), b"USER:1");
    assert_eq!(client.scan_prefix("user:", 2).await.unwrap().len(), 2);
    assert_eq!(client.scan_prefix("", 100).await.unwrap().len(), 4);

    server.stop().await;
}

#[tokio::test]
async fn data_survives_restart() {
    let dir = tempfile::tempdir().unwrap();

    let server = TestServer::start(dir.path()).await;
    let mut client = server.client().await;
    for i in 0..100 {
        client.put(format!("k{i}"), format!("v{i}")).await.unwrap();
    }
    client.delete("k0").await.unwrap();
    server.stop().await;

    let server = TestServer::start(dir.path()).await;
    let mut client = server.client().await;
    assert_eq!(client.get("k0").await.unwrap(), None);
    assert_eq!(client.get("k99").await.unwrap().as_deref(), Some(&b"v99"[..]));
    assert_eq!(client.stats().await.unwrap().keys, 99);
    server.stop().await;
}

#[tokio::test]
async fn concurrent_clients() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start(dir.path()).await;

    let workers: Vec<_> = (0..16)
        .map(|w| {
            let addr = server.addr;
            tokio::spawn(async move {
                let mut client = Client::connect(addr).await.unwrap();
                for i in 0..50 {
                    let key = format!("w{w}:k{i}");
                    client.put(key.clone(), key.clone()).await.unwrap();
                    assert_eq!(client.get(key.clone()).await.unwrap().as_deref(), Some(key.as_bytes()));
                }
            })
        })
        .collect();
    for worker in workers {
        worker.await.unwrap();
    }

    let mut client = server.client().await;
    assert_eq!(client.stats().await.unwrap().keys, 16 * 50);
    server.stop().await;
}

#[tokio::test]
async fn invalid_requests_get_typed_errors() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start(dir.path()).await;
    let mut client = server.client().await;

    let err = client.put("", "value").await.unwrap_err();
    assert!(matches!(err, ClientError::Server { code: ErrorCode::BadRequest, .. }), "{err}");

    // A server-side validation error leaves the connection usable.
    client.put("still", "works").await.unwrap();
    server.stop().await;
}

#[tokio::test]
async fn oversized_frame_is_rejected_and_connection_closed() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start_with(ServerConfig {
        data_dir: dir.path().to_path_buf(),
        max_frame_bytes: 1024,
        ..test_config()
    })
    .await;

    let mut raw = TcpStream::connect(server.addr).await.unwrap();
    raw.write_all(&u32::MAX.to_be_bytes()).await.unwrap();
    let mut reply = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), raw.read_to_end(&mut reply))
        .await
        .expect("server should hang up")
        .unwrap();
    assert!(!reply.is_empty(), "expected an error frame before the hang-up");
    assert_eq!(reply[4], 0xFF, "error opcode");

    // The server itself is unaffected.
    server.client().await.ping().await.unwrap();
    server.stop().await;
}

#[tokio::test]
async fn shutdown_closes_idle_connections_promptly() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start(dir.path()).await;
    let mut idle = server.client().await;
    idle.ping().await.unwrap();

    let started = std::time::Instant::now();
    server.stop().await;
    assert!(started.elapsed() < Duration::from_secs(1), "drain waited on an idle connection");
    assert!(idle.ping().await.is_err());
}
