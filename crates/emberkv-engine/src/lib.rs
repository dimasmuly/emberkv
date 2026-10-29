//! # emberkv-engine
//!
//! A crash-safe, log-structured key-value storage engine in the spirit of
//! [Bitcask](https://riak.com/assets/bitcask-intro.pdf).
//!
//! * **Writes** are appended to the *active segment* file, so every write is
//!   a single sequential `write(2)` (plus an optional `fdatasync`).
//! * **Reads** go through an in-memory ordered index that maps each key to the
//!   exact location of its latest record, so a read is one positional read.
//! * **Recovery** replays segments in id order. A torn tail on the active
//!   segment (e.g. power loss mid-write) is detected via CRC32 and truncated;
//!   damage anywhere else is reported as [`Error::Corruption`], never ignored.
//! * **Compaction** rewrites live records from sealed segments into a fresh
//!   segment while reads and writes continue, and is crash-safe at every step.
//! * **TTL**: keys can expire; expired data is dropped by compaction.
//! * **Atomic batches** and **atomic read-modify-write** ([`Engine::update`]).
//! * **Checkpoints**: consistent, near-instant backups via hard links.
//!
//! See `docs/ARCHITECTURE.md` in the repository for the full design.
//!
//! ## Example
//!
//! ```
//! use emberkv_engine::{Engine, Options};
//!
//! # fn main() -> emberkv_engine::Result<()> {
//! let dir = tempfile::tempdir()?;
//! let db = Engine::open(dir.path(), Options::default())?;
//!
//! db.put(b"user:1", b"ada")?;
//! db.put(b"user:2", b"grace")?;
//! assert_eq!(db.get(b"user:1")?.as_deref(), Some(&b"ada"[..]));
//!
//! let users = db.scan_prefix(b"user:", 10)?;
//! assert_eq!(users.len(), 2);
//!
//! assert!(db.delete(b"user:1")?);
//! assert_eq!(db.get(b"user:1")?, None);
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]

mod batch;
mod clock;
mod engine;
mod error;
mod options;
mod record;
mod segment;
mod types;

pub use batch::WriteBatch;
pub use clock::{Clock, ManualClock, SystemClock};
pub use engine::{Engine, prefix_upper_bound};
pub use error::{Error, Result};
pub use options::{Options, SyncPolicy};
pub use record::{MAX_KEY_LEN, MAX_VALUE_LEN};
pub use types::{CheckpointReport, CompactionReport, EngineStats, Entry, Expiry, Mutation};
