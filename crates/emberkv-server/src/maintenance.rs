//! Background tasks: periodic fsync and policy-driven compaction.

use std::time::Duration;

use emberkv_engine::Engine;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::config::CompactionPolicy;
use crate::telemetry;

pub(crate) async fn sync_loop(engine: Engine, every: Duration, shutdown: CancellationToken) {
    let mut ticker = tokio::time::interval(every);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            _ = ticker.tick() => {}
        }
        let engine = engine.clone();
        match tokio::task::spawn_blocking(move || engine.sync()).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => error!(error = %err, "periodic sync failed"),
            Err(err) => error!(error = %err, "periodic sync panicked"),
        }
    }
}

/// Purges expired keys so their space becomes visible to the compaction
/// policy (which looks at garbage ratio) and the key count stays honest.
pub(crate) async fn expiry_loop(engine: Engine, every: Duration, shutdown: CancellationToken) {
    let mut ticker = tokio::time::interval(every);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            _ = ticker.tick() => {}
        }
        let engine = engine.clone();
        match tokio::task::spawn_blocking(move || engine.purge_expired()).await {
            Ok(0) => {}
            Ok(purged) => {
                telemetry::record_expired(purged);
                debug!(purged, "purged expired keys");
            }
            Err(err) => error!(error = %err, "expiry sweep panicked"),
        }
    }
}

pub(crate) async fn compaction_loop(engine: Engine, policy: CompactionPolicy, shutdown: CancellationToken) {
    let mut ticker = tokio::time::interval(policy.check_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            _ = ticker.tick() => {}
        }
        let stats = engine.stats();
        telemetry::record_engine_stats(&stats);
        if !policy.should_compact(&stats) {
            continue;
        }

        info!(
            fragmentation = format_args!("{:.2}", stats.fragmentation()),
            dead_bytes = stats.dead_bytes(),
            "starting compaction",
        );
        let worker = engine.clone();
        match tokio::task::spawn_blocking(move || worker.compact()).await {
            Ok(Ok(report)) => {
                telemetry::record_compaction(&report);
                info!(
                    segments = report.segments_compacted,
                    reclaimed_bytes = report.reclaimed_bytes(),
                    duration_ms = report.duration.as_millis() as u64,
                    "compaction finished",
                );
            }
            Ok(Err(err)) => error!(error = %err, "compaction failed"),
            Err(err) => error!(error = %err, "compaction panicked"),
        }
    }
}
