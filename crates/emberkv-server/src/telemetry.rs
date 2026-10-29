//! Logging and Prometheus metrics.
//!
//! Metric calls are no-ops until [`install_metrics_exporter`] runs, so tests
//! and embedders pay nothing for instrumentation they do not use.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Context;
use emberkv_engine::{CompactionReport, EngineStats};
use metrics::{Unit, counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
use tracing_subscriber::EnvFilter;

const REQUESTS: &str = "emberkv_requests_total";
const REQUEST_DURATION: &str = "emberkv_request_duration_seconds";
const CONNECTIONS: &str = "emberkv_connections_active";
const KEYS: &str = "emberkv_keys";
const SEGMENTS: &str = "emberkv_segments";
const DISK_BYTES: &str = "emberkv_disk_bytes";
const LIVE_BYTES: &str = "emberkv_live_bytes";
const COMPACTIONS: &str = "emberkv_compactions_total";
const RECLAIMED: &str = "emberkv_compaction_reclaimed_bytes_total";
const EXPIRED: &str = "emberkv_expired_keys_total";

/// Log output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum LogFormat {
    /// Human-friendly, for terminals.
    Pretty,
    /// One JSON object per line, for log pipelines.
    Json,
}

/// Installs the global tracing subscriber. Honors `RUST_LOG`.
pub fn init_tracing(format: LogFormat) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    match format {
        LogFormat::Pretty => builder.init(),
        LogFormat::Json => builder.json().flatten_event(true).init(),
    }
}

/// Serves Prometheus metrics at `http://{addr}/metrics`.
pub fn install_metrics_exporter(addr: SocketAddr) -> anyhow::Result<()> {
    // Sub-millisecond buckets: most requests are served from the page cache.
    let buckets =
        [0.000_05, 0.000_1, 0.000_25, 0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1.0];
    PrometheusBuilder::new()
        .with_http_listener(addr)
        .set_buckets_for_metric(Matcher::Full(REQUEST_DURATION.into()), &buckets)?
        .install()
        .with_context(|| format!("starting metrics exporter on {addr}"))?;

    describe_counter!(REQUESTS, Unit::Count, "Requests handled, by transport, op and status");
    describe_histogram!(REQUEST_DURATION, Unit::Seconds, "Request latency, by transport and op");
    describe_gauge!(CONNECTIONS, Unit::Count, "Open client connections");
    describe_gauge!(KEYS, Unit::Count, "Live keys");
    describe_gauge!(SEGMENTS, Unit::Count, "Segment files on disk");
    describe_gauge!(DISK_BYTES, Unit::Bytes, "Bytes used by all segments");
    describe_gauge!(LIVE_BYTES, Unit::Bytes, "Bytes used by reachable records");
    describe_counter!(COMPACTIONS, Unit::Count, "Completed compactions");
    describe_counter!(RECLAIMED, Unit::Bytes, "Disk space reclaimed by compaction");
    describe_counter!(EXPIRED, Unit::Count, "Expired keys purged from the index");
    Ok(())
}

pub(crate) fn record_request(transport: &'static str, op: &'static str, is_error: bool, elapsed: Duration) {
    let status = if is_error { "error" } else { "ok" };
    counter!(REQUESTS, "transport" => transport, "op" => op, "status" => status).increment(1);
    histogram!(REQUEST_DURATION, "transport" => transport, "op" => op).record(elapsed.as_secs_f64());
}

pub(crate) fn record_expired(count: usize) {
    counter!(EXPIRED).increment(count as u64);
}

pub(crate) fn record_engine_stats(stats: &EngineStats) {
    gauge!(KEYS).set(stats.keys as f64);
    gauge!(SEGMENTS).set(stats.segments as f64);
    gauge!(DISK_BYTES).set(stats.total_bytes as f64);
    gauge!(LIVE_BYTES).set(stats.live_bytes as f64);
}

pub(crate) fn record_compaction(report: &CompactionReport) {
    counter!(COMPACTIONS).increment(1);
    counter!(RECLAIMED).increment(report.reclaimed_bytes());
}

/// Tracks one open connection for as long as it is alive.
#[derive(Debug)]
pub(crate) struct ConnectionGauge(());

impl ConnectionGauge {
    pub(crate) fn open() -> Self {
        gauge!(CONNECTIONS).increment(1.0);
        Self(())
    }
}

impl Drop for ConnectionGauge {
    fn drop(&mut self) {
        gauge!(CONNECTIONS).decrement(1.0);
    }
}
