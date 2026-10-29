use std::io;
use std::path::PathBuf;

/// Convenience alias used throughout the engine.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Everything that can go wrong inside the storage engine.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// An operating-system level I/O failure.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// On-disk data failed validation (bad checksum, impossible lengths, ...).
    #[error("data corruption in segment {segment} at offset {offset}: {reason}")]
    Corruption {
        /// Segment that contains the damaged record.
        segment: u64,
        /// Byte offset of the damaged record inside the segment.
        offset: u64,
        /// Human-readable description of what failed validation.
        reason: String,
    },

    /// Keys must contain at least one byte.
    #[error("key must not be empty")]
    EmptyKey,

    /// The key exceeds [`Options::max_key_bytes`](crate::Options::max_key_bytes).
    #[error("key is {len} bytes; the limit is {max}")]
    KeyTooLarge {
        /// Size of the rejected key.
        len: usize,
        /// Configured limit.
        max: usize,
    },

    /// The value exceeds [`Options::max_value_bytes`](crate::Options::max_value_bytes).
    #[error("value is {len} bytes; the limit is {max}")]
    ValueTooLarge {
        /// Size of the rejected value.
        len: usize,
        /// Configured limit.
        max: usize,
    },

    /// The encoded [`WriteBatch`](crate::WriteBatch) exceeds
    /// [`Options::max_batch_bytes`](crate::Options::max_batch_bytes).
    #[error("batch is {len} bytes; the limit is {max}")]
    BatchTooLarge {
        /// Encoded size of the rejected batch.
        len: usize,
        /// Configured limit.
        max: usize,
    },

    /// Another process (or another `Engine` in this process) owns the directory.
    #[error("data directory {} is locked by another process", .0.display())]
    Locked(PathBuf),

    /// The options passed to [`Engine::open`](crate::Engine::open) are invalid.
    #[error("invalid options: {0}")]
    InvalidOptions(String),

    /// A previous `fsync` failed, so the state of the page cache can no longer
    /// be trusted. Writes are refused until the engine is reopened.
    #[error("write path disabled after an unrecoverable I/O failure; reopen the engine")]
    Poisoned,
}

impl Error {
    pub(crate) fn corruption(segment: u64, offset: u64, reason: impl std::fmt::Display) -> Self {
        Self::Corruption { segment, offset, reason: reason.to_string() }
    }
}
