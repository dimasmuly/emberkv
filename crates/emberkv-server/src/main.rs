use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use emberkv_server::auth::Authenticator;
use emberkv_server::telemetry::{self, LogFormat};
use emberkv_server::{CompactionPolicy, Server, ServerConfig, SyncMode};
use tracing::{error, info};

const MIB: u64 = 1024 * 1024;

/// emberkv: a crash-safe, log-structured key-value server.
#[derive(Debug, Parser)]
#[command(name = "emberkv-server", version, about)]
struct Cli {
    /// Address for the binary protocol.
    #[arg(long, env = "EMBERKV_LISTEN", default_value = "127.0.0.1:7878")]
    listen: SocketAddr,

    /// Address for the JSON/HTTP API.
    #[arg(long, env = "EMBERKV_HTTP_LISTEN", default_value = "127.0.0.1:8080")]
    http_listen: SocketAddr,

    /// Turn the HTTP API off.
    #[arg(long, env = "EMBERKV_DISABLE_HTTP")]
    disable_http: bool,

    /// Directory holding segment files.
    #[arg(long, env = "EMBERKV_DATA_DIR", default_value = "./data")]
    data_dir: PathBuf,

    /// TOML file with API keys (see `ekv token`). Without it, auth is off and
    /// the server only listens on loopback addresses.
    #[arg(long, env = "EMBERKV_AUTH_FILE")]
    auth_file: Option<PathBuf>,

    /// Allow listening on non-loopback addresses without authentication.
    /// Only for networks where every client is trusted.
    #[arg(long, env = "EMBERKV_ALLOW_UNAUTHENTICATED_REMOTE")]
    allow_unauthenticated_remote: bool,

    /// Directory for admin-triggered checkpoints (POST /v1/admin/checkpoints).
    #[arg(long, env = "EMBERKV_BACKUP_DIR")]
    backup_dir: Option<PathBuf>,

    /// Serve Prometheus metrics on this address (e.g. 127.0.0.1:9464).
    #[arg(long, env = "EMBERKV_METRICS_LISTEN")]
    metrics_listen: Option<SocketAddr>,

    /// Durability policy.
    #[arg(long, env = "EMBERKV_SYNC", value_enum, default_value_t = SyncArg::Periodic)]
    sync: SyncArg,

    /// Interval for `--sync periodic`.
    #[arg(long, env = "EMBERKV_SYNC_INTERVAL_MS", default_value_t = 100)]
    sync_interval_ms: u64,

    /// Maximum concurrent binary-protocol connections.
    #[arg(long, env = "EMBERKV_MAX_CONNECTIONS", default_value_t = 1024)]
    max_connections: usize,

    /// Segment rotation size in MiB.
    #[arg(long, env = "EMBERKV_MAX_SEGMENT_MIB", default_value_t = 64)]
    max_segment_mib: u64,

    /// Compact when at least this fraction of disk usage is garbage.
    #[arg(long, env = "EMBERKV_COMPACTION_MIN_FRAGMENTATION", default_value_t = 0.5)]
    compaction_min_fragmentation: f64,

    /// ...and at least this many MiB are garbage.
    #[arg(long, env = "EMBERKV_COMPACTION_MIN_DEAD_MIB", default_value_t = 64)]
    compaction_min_dead_mib: u64,

    /// How often to evaluate the compaction policy, in seconds.
    #[arg(long, env = "EMBERKV_COMPACTION_INTERVAL_SECS", default_value_t = 30)]
    compaction_interval_secs: u64,

    /// How often to purge expired keys, in seconds.
    #[arg(long, env = "EMBERKV_EXPIRY_SWEEP_SECS", default_value_t = 10)]
    expiry_sweep_secs: u64,

    /// Close idle binary-protocol connections after this many seconds.
    #[arg(long, env = "EMBERKV_IDLE_TIMEOUT_SECS", default_value_t = 300)]
    idle_timeout_secs: u64,

    /// Deadline for a single HTTP request, in seconds.
    #[arg(long, env = "EMBERKV_HTTP_REQUEST_TIMEOUT_SECS", default_value_t = 10)]
    http_request_timeout_secs: u64,

    /// Seconds to wait for in-flight requests on shutdown.
    #[arg(long, env = "EMBERKV_SHUTDOWN_GRACE_SECS", default_value_t = 15)]
    shutdown_grace_secs: u64,

    /// Log output format.
    #[arg(long, env = "EMBERKV_LOG_FORMAT", value_enum, default_value_t = LogFormat::Pretty)]
    log_format: LogFormat,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum SyncArg {
    Always,
    Periodic,
    Never,
}

impl Cli {
    fn into_config(self) -> anyhow::Result<ServerConfig> {
        let auth = self.auth_file.as_deref().map(Authenticator::load).transpose()?.map(Arc::new);
        Ok(ServerConfig {
            listen: self.listen,
            http_listen: (!self.disable_http).then_some(self.http_listen),
            data_dir: self.data_dir,
            sync: match self.sync {
                SyncArg::Always => SyncMode::Always,
                SyncArg::Periodic => SyncMode::Periodic(Duration::from_millis(self.sync_interval_ms.max(1))),
                SyncArg::Never => SyncMode::Never,
            },
            max_segment_bytes: self.max_segment_mib.max(1) * MIB,
            max_connections: self.max_connections.max(1),
            idle_timeout: Duration::from_secs(self.idle_timeout_secs),
            shutdown_grace: Duration::from_secs(self.shutdown_grace_secs),
            http_request_timeout: Duration::from_secs(self.http_request_timeout_secs.max(1)),
            compaction: CompactionPolicy {
                check_interval: Duration::from_secs(self.compaction_interval_secs.max(1)),
                min_fragmentation: self.compaction_min_fragmentation.clamp(0.0, 1.0),
                min_dead_bytes: self.compaction_min_dead_mib * MIB,
            },
            expiry_sweep_interval: Duration::from_secs(self.expiry_sweep_secs.max(1)),
            auth,
            allow_unauthenticated_remote: self.allow_unauthenticated_remote,
            backup_dir: self.backup_dir,
            ..ServerConfig::default()
        })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    telemetry::init_tracing(cli.log_format);
    if let Some(addr) = cli.metrics_listen {
        telemetry::install_metrics_exporter(addr)?;
        info!(%addr, "serving Prometheus metrics at /metrics");
    }

    let config = cli.into_config()?;
    info!(?config, "starting emberkv-server {}", env!("CARGO_PKG_VERSION"));
    let server = Server::bind(config).await?;
    server.run(shutdown_signal()).await
}

/// Resolves on Ctrl-C or SIGTERM (what Docker/Kubernetes send on stop).
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            error!(error = %err, "cannot listen for Ctrl-C");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut sigterm) => {
                sigterm.recv().await;
            }
            Err(err) => {
                error!(error = %err, "cannot listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => info!("received Ctrl-C"),
        () = terminate => info!("received SIGTERM"),
    }
}
