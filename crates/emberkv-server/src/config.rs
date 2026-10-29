use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use emberkv_engine::EngineStats;
use emberkv_protocol::DEFAULT_MAX_FRAME_BYTES;

use crate::auth::Authenticator;

/// How the server trades durability for write latency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// fsync before acknowledging every write. Safest, slowest.
    Always,
    /// fsync on a timer. A crash loses at most one interval of acknowledged
    /// writes; throughput is close to `Never`. A sensible default.
    Periodic(Duration),
    /// Leave flushing to the OS.
    Never,
}

/// When background compaction kicks in.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CompactionPolicy {
    /// How often to evaluate the policy.
    pub check_interval: Duration,
    /// Minimum garbage ratio (`dead / total`) before compacting.
    pub min_fragmentation: f64,
    /// Minimum garbage in bytes, so tiny databases are left alone.
    pub min_dead_bytes: u64,
}

impl CompactionPolicy {
    /// Whether the given stats warrant a compaction.
    pub fn should_compact(&self, stats: &EngineStats) -> bool {
        stats.dead_bytes() >= self.min_dead_bytes && stats.fragmentation() >= self.min_fragmentation
    }
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(30),
            min_fragmentation: 0.5,
            min_dead_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Everything needed to run a [`Server`](crate::Server).
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Address for the binary protocol.
    pub listen: SocketAddr,
    /// Address for the JSON/HTTP API; `None` disables it.
    pub http_listen: Option<SocketAddr>,
    /// Directory holding segment files.
    pub data_dir: PathBuf,
    /// Durability policy.
    pub sync: SyncMode,
    /// Segment rotation size.
    pub max_segment_bytes: u64,
    /// Concurrent binary-protocol connection cap. Beyond it the server stops
    /// accepting and lets the kernel backlog absorb (and eventually refuse)
    /// new clients.
    pub max_connections: usize,
    /// Binary-protocol connections idle for longer than this are closed.
    pub idle_timeout: Duration,
    /// How long shutdown waits for in-flight requests before giving up.
    pub shutdown_grace: Duration,
    /// Largest accepted binary-protocol request / produced response.
    pub max_frame_bytes: usize,
    /// Largest accepted HTTP request body.
    pub max_http_body_bytes: usize,
    /// Deadline for a single HTTP request.
    pub http_request_timeout: Duration,
    /// Upper bound on entries returned by one scan or list page.
    pub max_scan_limit: u32,
    /// Background compaction policy.
    pub compaction: CompactionPolicy,
    /// How often expired keys are purged from the index.
    pub expiry_sweep_interval: Duration,
    /// API keys. `None` disables authentication.
    pub auth: Option<Arc<Authenticator>>,
    /// Without `auth`, the server refuses to listen on non-loopback addresses
    /// unless this is set. Secure by default.
    pub allow_unauthenticated_remote: bool,
    /// Where admin-triggered checkpoints are written; `None` disables them.
    pub backup_dir: Option<PathBuf>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([127, 0, 0, 1], 7878)),
            http_listen: None,
            data_dir: PathBuf::from("./data"),
            sync: SyncMode::Periodic(Duration::from_millis(100)),
            max_segment_bytes: 64 * 1024 * 1024,
            max_connections: 1024,
            idle_timeout: Duration::from_secs(300),
            shutdown_grace: Duration::from_secs(15),
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            max_http_body_bytes: 16 * 1024 * 1024,
            http_request_timeout: Duration::from_secs(10),
            max_scan_limit: 10_000,
            compaction: CompactionPolicy::default(),
            expiry_sweep_interval: Duration::from_secs(10),
            auth: None,
            allow_unauthenticated_remote: false,
            backup_dir: None,
        }
    }
}

impl ServerConfig {
    /// Addresses reachable from other machines while authentication is off.
    pub(crate) fn unauthenticated_exposure(&self) -> Vec<SocketAddr> {
        if self.auth.is_some() || self.allow_unauthenticated_remote {
            return Vec::new();
        }
        std::iter::once(self.listen).chain(self.http_listen).filter(|addr| !addr.ip().is_loopback()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats(total_bytes: u64, live_bytes: u64) -> EngineStats {
        EngineStats { keys: 1, segments: 1, total_bytes, live_bytes }
    }

    #[test]
    fn compaction_requires_both_ratio_and_volume() {
        let policy = CompactionPolicy { min_fragmentation: 0.5, min_dead_bytes: 100, ..Default::default() };
        assert!(policy.should_compact(&stats(1000, 400)));
        assert!(!policy.should_compact(&stats(1000, 600)), "ratio too low");
        assert!(!policy.should_compact(&stats(150, 60)), "too little garbage");
        assert!(!policy.should_compact(&stats(0, 0)));
    }

    #[test]
    fn unauthenticated_remote_exposure_is_detected() {
        let local = ServerConfig { http_listen: Some(([127, 0, 0, 1], 8080).into()), ..Default::default() };
        assert!(local.unauthenticated_exposure().is_empty());

        let public = ServerConfig { http_listen: Some(([0, 0, 0, 0], 8080).into()), ..Default::default() };
        assert_eq!(public.unauthenticated_exposure().len(), 1);

        let opted_in = ServerConfig { allow_unauthenticated_remote: true, ..public };
        assert!(opted_in.unauthenticated_exposure().is_empty());
    }
}
