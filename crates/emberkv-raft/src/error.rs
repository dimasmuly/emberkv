use crate::types::{Index, NodeId};

/// Errors returned by the Raft core.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The configuration is unusable.
    #[error("invalid Raft config: {0}")]
    Config(&'static str),

    /// The persisted state handed to [`Raft::new`](crate::Raft::new) is inconsistent.
    #[error("invalid persisted Raft state at index {index}: {reason}")]
    Storage {
        /// The offending log position.
        index: Index,
        /// What failed validation.
        reason: &'static str,
    },

    /// Proposals and reads must go to the leader.
    #[error("this node is not the leader (leader: {leader:?})")]
    NotLeader {
        /// The leader, if this node knows it, so the caller can redirect.
        leader: Option<NodeId>,
    },
}
