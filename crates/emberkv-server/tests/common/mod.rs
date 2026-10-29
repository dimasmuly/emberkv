//! Shared harness: a real server on ephemeral ports, stopped gracefully.

#![allow(dead_code)] // each test binary uses a different subset

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use emberkv_client::Client;
use emberkv_server::{Server, ServerConfig, SyncMode};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub struct TestServer {
    pub addr: SocketAddr,
    pub http: Option<SocketAddr>,
    stop: oneshot::Sender<()>,
    handle: JoinHandle<anyhow::Result<()>>,
}

impl TestServer {
    pub async fn start(dir: &Path) -> Self {
        Self::start_with(ServerConfig { data_dir: dir.to_path_buf(), ..test_config() }).await
    }

    pub async fn start_with(config: ServerConfig) -> Self {
        let server = Server::bind(config).await.expect("bind");
        let addr = server.local_addr().unwrap();
        let http = server.local_http_addr();
        let (stop, stopped) = oneshot::channel::<()>();
        let handle = tokio::spawn(server.run(async move {
            let _ = stopped.await;
        }));
        Self { addr, http, stop, handle }
    }

    pub async fn client(&self) -> Client {
        Client::connect(self.addr).await.expect("connect")
    }

    /// Base URL of the HTTP API.
    pub fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.http.expect("HTTP API enabled"))
    }

    pub async fn stop(self) {
        let _ = self.stop.send(());
        tokio::time::timeout(Duration::from_secs(5), self.handle)
            .await
            .expect("server did not shut down in time")
            .unwrap()
            .unwrap();
    }
}

pub fn test_config() -> ServerConfig {
    ServerConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        http_listen: Some("127.0.0.1:0".parse().unwrap()),
        sync: SyncMode::Never,
        shutdown_grace: Duration::from_secs(2),
        ..ServerConfig::default()
    }
}
