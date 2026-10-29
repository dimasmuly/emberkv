use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, ensure};
use emberkv_engine::{Engine, Options, SyncPolicy};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{Instrument, debug, error, info, info_span, warn};

use crate::config::{ServerConfig, SyncMode};
use crate::connection::{self, ConnectionSettings};
use crate::http::{self, AppState, HttpLimits};
use crate::maintenance;
use crate::telemetry::ConnectionGauge;

/// A bound, ready-to-run server.
///
/// Splitting [`bind`](Server::bind) from [`run`](Server::run) lets callers
/// learn the real ports (useful with port `0` in tests) before serving.
#[derive(Debug)]
pub struct Server {
    listener: TcpListener,
    http_listener: Option<TcpListener>,
    engine: Engine,
    config: ServerConfig,
}

impl Server {
    /// Opens the storage engine (replaying its log) and binds the listeners.
    ///
    /// # Errors
    ///
    /// Besides I/O failures, refuses to start when authentication is off and
    /// a listener is reachable from other machines, unless
    /// [`ServerConfig::allow_unauthenticated_remote`] is set.
    pub async fn bind(config: ServerConfig) -> anyhow::Result<Self> {
        let exposed = config.unauthenticated_exposure();
        ensure!(
            exposed.is_empty(),
            "refusing to listen on {exposed:?} without authentication; configure an auth file, \
             or explicitly allow unauthenticated remote access if this network is trusted"
        );

        let options =
            Options::default().max_segment_bytes(config.max_segment_bytes).sync(match config.sync {
                SyncMode::Always => SyncPolicy::Always,
                SyncMode::Periodic(_) | SyncMode::Never => SyncPolicy::Never,
            });
        let dir = config.data_dir.clone();
        let engine = tokio::task::spawn_blocking(move || Engine::open(dir, options))
            .await?
            .with_context(|| format!("opening data directory {}", config.data_dir.display()))?;
        let listener =
            TcpListener::bind(config.listen).await.with_context(|| format!("binding {}", config.listen))?;
        let http_listener = match config.http_listen {
            Some(addr) => Some(TcpListener::bind(addr).await.with_context(|| format!("binding {addr}"))?),
            None => None,
        };
        Ok(Self { listener, http_listener, engine, config })
    }

    /// The address the binary protocol is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// The address the HTTP API is bound to, if enabled.
    pub fn local_http_addr(&self) -> Option<SocketAddr> {
        self.http_listener.as_ref().and_then(|listener| listener.local_addr().ok())
    }

    /// Serves until `shutdown` completes, then drains gracefully:
    ///
    /// 1. stop accepting new connections and report not-ready on `/readyz`;
    /// 2. let every connection finish the request it is processing;
    /// 3. wait up to `shutdown_grace` for that, then give up on stragglers;
    /// 4. fsync the engine so every acknowledged write is durable.
    pub async fn run(self, shutdown: impl Future<Output = ()> + Send) -> anyhow::Result<()> {
        let Self { listener, http_listener, engine, config } = self;
        let cancel = CancellationToken::new();
        let tasks = TaskTracker::new();
        let limiter = Arc::new(Semaphore::new(config.max_connections));
        let settings = ConnectionSettings::from(&config);

        if config.auth.is_none() {
            warn!("authentication is DISABLED; any client that can connect has full access");
        }
        if let SyncMode::Periodic(every) = config.sync {
            tasks.spawn(maintenance::sync_loop(engine.clone(), every, cancel.clone()));
        }
        tasks.spawn(maintenance::compaction_loop(engine.clone(), config.compaction, cancel.clone()));
        tasks.spawn(maintenance::expiry_loop(engine.clone(), config.expiry_sweep_interval, cancel.clone()));

        if let Some(http_listener) = http_listener {
            let state = AppState {
                engine: engine.clone(),
                auth: config.auth.clone(),
                backup_dir: config.backup_dir.clone(),
                shutdown: cancel.clone(),
                max_scan_limit: config.max_scan_limit,
            };
            let limits = HttpLimits {
                max_body_bytes: config.max_http_body_bytes,
                request_timeout: config.http_request_timeout,
            };
            let app = http::router(state, limits);
            info!(addr = %http_listener.local_addr()?, "serving HTTP API");
            let stop = cancel.clone();
            tasks.spawn(async move {
                let served =
                    axum::serve(http_listener, app).with_graceful_shutdown(stop.cancelled_owned()).await;
                if let Err(err) = served {
                    error!(error = %err, "HTTP server failed");
                }
            });
        }

        info!(addr = %listener.local_addr()?, "accepting binary-protocol connections");
        tokio::pin!(shutdown);
        loop {
            // Backpressure: take a connection slot *before* accepting, so at
            // capacity we stop calling accept() instead of over-committing.
            let permit = tokio::select! {
                biased;
                () = &mut shutdown => break,
                permit = Arc::clone(&limiter).acquire_owned() => permit?,
            };
            let (stream, peer) = tokio::select! {
                biased;
                () = &mut shutdown => break,
                accepted = listener.accept() => match accepted {
                    Ok(accepted) => accepted,
                    Err(err) => {
                        // Typically EMFILE/ENFILE; back off instead of spinning.
                        warn!(error = %err, "accept failed");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                },
            };
            if let Err(err) = stream.set_nodelay(true) {
                debug!(%peer, error = %err, "failed to set TCP_NODELAY");
            }

            let engine = engine.clone();
            let auth = config.auth.clone();
            let cancel = cancel.child_token();
            tasks.spawn(
                async move {
                    let _permit = permit;
                    let _gauge = ConnectionGauge::open();
                    debug!("connection opened");
                    match connection::serve(stream, engine, auth, cancel, settings).await {
                        Ok(()) => debug!("connection closed"),
                        Err(err) => debug!(error = %err, "connection closed with error"),
                    }
                }
                .instrument(info_span!("conn", %peer)),
            );
        }

        drop(listener);
        info!(open_tasks = tasks.len(), "shutdown requested; draining");
        cancel.cancel();
        tasks.close();
        if tokio::time::timeout(config.shutdown_grace, tasks.wait()).await.is_err() {
            warn!(remaining = tasks.len(), "grace period elapsed; abandoning remaining tasks");
        }

        tokio::task::spawn_blocking(move || engine.sync()).await?.context("final sync")?;
        info!("shutdown complete");
        Ok(())
    }
}
