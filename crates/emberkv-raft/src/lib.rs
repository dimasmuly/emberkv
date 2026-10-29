//! # emberkv-raft
//!
//! The [Raft](https://raft.github.io/raft.pdf) consensus algorithm as a pure
//! state machine, with no threads, sockets, files or clocks.
//!
//! * **Elections** with randomized timeouts, plus **pre-vote** (a node cut
//!   off by a partition cannot force a new term when it returns) and
//!   **check-quorum** (a leader that loses its quorum steps down).
//! * **Log replication** with per-follower probing and pipelining, fast
//!   conflict back-off, and flow control.
//! * **Linearizable reads** via ReadIndex: no log write per read, just one
//!   round of heartbeats that a batch of reads can share.
//!
//! Being free of I/O is what makes the core testable: `tests/simulation.rs`
//! runs whole clusters on a simulated network that drops, duplicates,
//! delays and reorders messages, partitions nodes and crashes them, and
//! checks Raft's safety properties after every step. A failing seed
//! reproduces exactly.
//!
//! ## Driving a node
//!
//! The driver owns storage and transport. It calls [`Raft::tick`] on a
//! timer and [`Raft::step`] for every incoming message, then processes the
//! [`Ready`] returned by [`Raft::ready`] in the documented order: persist,
//! send, apply.
//!
//! ```
//! use emberkv_raft::{Config, HardState, Payload, Raft, Role};
//!
//! # fn main() -> Result<(), emberkv_raft::Error> {
//! // A single-node cluster elects itself and commits on its own.
//! let mut node = Raft::new(&Config::new(1, [1]), HardState::default(), Vec::new(), 0)?;
//! while node.status().role != Role::Leader {
//!     node.tick();
//! }
//! let (index, _term) = node.propose(b"set x 1".to_vec())?;
//!
//! let ready = node.ready();
//! // 1. persist ready.hard_state and ready.entries  2. send ready.messages
//! // 3. apply ready.committed, in order:
//! let applied: Vec<_> = ready.committed.iter().map(|e| (e.index, &e.payload)).collect();
//! assert_eq!(applied.last(), Some(&(index, &Payload::Command(b"set x 1".to_vec()))));
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]

mod config;
mod error;
mod raft;
mod rng;
mod types;

pub use config::Config;
pub use error::Error;
pub use raft::Raft;
pub use types::{
    Entry, Envelope, HardState, Index, Message, NodeId, Payload, ReadState, Ready, Role, Status, Term,
};
