//! Raft replication: every write is committed to a majority of nodes'
//! logs before it is applied, so losing a minority of nodes loses no
//! acknowledged write. See `docs/adr/0011-replicated-state-machine.md`.

mod log_store;
mod node;
#[cfg(test)]
mod tests;

pub(crate) use node::{Replica, ReplicaConfig, ReplicaError, Transport};
