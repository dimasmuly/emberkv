use std::sync::Arc;

use crate::clock::{Clock, SystemClock};
use crate::error::{Error, Result};
use crate::record::{MAX_KEY_LEN, MAX_VALUE_LEN};

/// Controls when appended records are forced to stable storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncPolicy {
    /// `fdatasync` after every write: an acknowledged write survives power loss.
    #[default]
    Always,
    /// Never sync implicitly. The caller decides when to call
    /// [`Engine::sync`](crate::Engine::sync) (e.g. on a timer, for group-commit
    /// style batching). A crash may lose recently acknowledged writes, but it
    /// can never corrupt data that was already synced.
    Never,
}

/// Tuning knobs for [`Engine::open`](crate::Engine::open).
///
/// ```
/// use emberkv_engine::{Options, SyncPolicy};
///
/// let options = Options::default()
///     .max_segment_bytes(16 * 1024 * 1024)
///     .sync(SyncPolicy::Never);
/// assert_eq!(options.get_sync(), SyncPolicy::Never);
/// ```
#[derive(Debug, Clone)]
pub struct Options {
    pub(crate) max_segment_bytes: u64,
    pub(crate) sync: SyncPolicy,
    pub(crate) max_key_bytes: usize,
    pub(crate) max_value_bytes: usize,
    pub(crate) max_batch_bytes: usize,
    pub(crate) clock: Arc<dyn Clock>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            max_segment_bytes: 64 * 1024 * 1024,
            sync: SyncPolicy::default(),
            max_key_bytes: 16 * 1024,
            max_value_bytes: 16 * 1024 * 1024,
            max_batch_bytes: 32 * 1024 * 1024,
            clock: Arc::new(SystemClock),
        }
    }
}

impl Options {
    /// Size at which the active segment is sealed and a new one is started.
    #[must_use]
    pub fn max_segment_bytes(mut self, bytes: u64) -> Self {
        self.max_segment_bytes = bytes;
        self
    }

    /// Durability policy; see [`SyncPolicy`].
    #[must_use]
    pub fn sync(mut self, policy: SyncPolicy) -> Self {
        self.sync = policy;
        self
    }

    /// Largest accepted key, at most [`MAX_KEY_LEN`].
    #[must_use]
    pub fn max_key_bytes(mut self, bytes: usize) -> Self {
        self.max_key_bytes = bytes;
        self
    }

    /// Largest accepted value, at most [`MAX_VALUE_LEN`].
    #[must_use]
    pub fn max_value_bytes(mut self, bytes: usize) -> Self {
        self.max_value_bytes = bytes;
        self
    }

    /// Largest accepted [`WriteBatch`](crate::WriteBatch), in encoded bytes.
    #[must_use]
    pub fn max_batch_bytes(mut self, bytes: usize) -> Self {
        self.max_batch_bytes = bytes;
        self
    }

    /// Time source for key expiry. Defaults to [`SystemClock`].
    #[must_use]
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// The configured durability policy.
    pub fn get_sync(&self) -> SyncPolicy {
        self.sync
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.max_segment_bytes == 0 {
            return Err(Error::InvalidOptions("max_segment_bytes must be positive".into()));
        }
        if self.max_key_bytes == 0 || self.max_key_bytes > MAX_KEY_LEN {
            return Err(Error::InvalidOptions(format!("max_key_bytes must be in 1..={MAX_KEY_LEN}")));
        }
        if self.max_value_bytes > MAX_VALUE_LEN {
            return Err(Error::InvalidOptions(format!("max_value_bytes must be at most {MAX_VALUE_LEN}")));
        }
        if self.max_batch_bytes == 0 {
            return Err(Error::InvalidOptions("max_batch_bytes must be positive".into()));
        }
        Ok(())
    }
}
