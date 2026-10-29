//! Public value types used by the [`Engine`](crate::Engine) API.

use std::time::Duration;

/// A stored value together with its metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// The stored bytes.
    pub value: Vec<u8>,
    /// When the key expires, in milliseconds since the Unix epoch.
    pub expires_at_ms: Option<u64>,
}

/// When a written key should expire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Expiry {
    /// The key never expires.
    #[default]
    Never,
    /// The key expires this long after the write.
    After(Duration),
    /// The key expires at an absolute time (milliseconds since the Unix epoch).
    AtUnixMs(u64),
    /// Keep the expiry of the value being replaced (or never, if there is none).
    KeepExisting,
}

/// What [`Engine::update`](crate::Engine::update) should do with a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mutation {
    /// Leave the key untouched.
    Keep,
    /// Store a new value.
    Put {
        /// The new value.
        value: Vec<u8>,
        /// Expiry for the new value.
        expiry: Expiry,
    },
    /// Remove the key.
    Delete,
}

/// Point-in-time engine statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineStats {
    /// Number of indexed keys. Expired keys count until they are purged.
    pub keys: usize,
    /// Number of segment files on disk.
    pub segments: usize,
    /// Bytes used by all records on disk.
    pub total_bytes: u64,
    /// Bytes used by records that are still reachable.
    pub live_bytes: u64,
}

impl EngineStats {
    /// Bytes that compaction could reclaim.
    pub fn dead_bytes(&self) -> u64 {
        self.total_bytes.saturating_sub(self.live_bytes)
    }

    /// Fraction of disk usage that is garbage, in `0.0..=1.0`.
    pub fn fragmentation(&self) -> f64 {
        if self.total_bytes == 0 { 0.0 } else { self.dead_bytes() as f64 / self.total_bytes as f64 }
    }
}

/// What a call to [`Engine::compact`](crate::Engine::compact) accomplished.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionReport {
    /// Segments merged and deleted.
    pub segments_compacted: usize,
    /// Live records copied into the new segment.
    pub records_moved: usize,
    /// Expired keys dropped instead of copied.
    pub records_expired: usize,
    /// Size of the input segments.
    pub bytes_before: u64,
    /// Size of the output segment.
    pub bytes_after: u64,
    /// Wall-clock time spent.
    pub duration: Duration,
}

impl CompactionReport {
    /// Disk space returned to the operating system.
    pub fn reclaimed_bytes(&self) -> u64 {
        self.bytes_before.saturating_sub(self.bytes_after)
    }
}

/// What a call to [`Engine::checkpoint`](crate::Engine::checkpoint) produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointReport {
    /// Segment files in the checkpoint.
    pub segments: usize,
    /// Total size of those files.
    pub bytes: u64,
    /// Wall-clock time spent.
    pub duration: Duration,
}
