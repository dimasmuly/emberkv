//! Time source for key expiry.
//!
//! The engine never calls `SystemTime::now()` directly. Injecting the clock
//! makes expiry deterministic in tests and simulations (see [`ManualClock`]).

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A source of wall-clock time in milliseconds since the Unix epoch.
pub trait Clock: Send + Sync + fmt::Debug {
    /// Current time in milliseconds since the Unix epoch.
    fn now_ms(&self) -> u64;
}

/// The operating system's wall clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
    }
}

/// A clock that only moves when told to. For tests and simulations.
///
/// ```
/// use std::sync::Arc;
/// use std::time::Duration;
/// use emberkv_engine::{Clock, ManualClock};
///
/// let clock = Arc::new(ManualClock::new(1_000));
/// clock.advance(Duration::from_secs(2));
/// assert_eq!(clock.now_ms(), 3_000);
/// ```
#[derive(Debug, Default)]
pub struct ManualClock(AtomicU64);

impl ManualClock {
    /// Creates a clock reading `start_ms`.
    pub fn new(start_ms: u64) -> Self {
        Self(AtomicU64::new(start_ms))
    }

    /// Moves the clock forward.
    pub fn advance(&self, by: Duration) {
        self.0.fetch_add(by.as_millis() as u64, Ordering::SeqCst);
    }

    /// Sets the clock to an absolute time.
    pub fn set(&self, now_ms: u64) {
        self.0.store(now_ms, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}
