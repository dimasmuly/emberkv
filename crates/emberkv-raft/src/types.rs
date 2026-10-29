//! Values exchanged between the Raft core, its peers and its driver.

/// Identifies a voting member of the cluster. Must be non-zero.
pub type NodeId = u64;

/// A Raft term: a logical clock that increases with every election.
pub type Term = u64;

/// Position of an entry in the replicated log. The first entry has index 1;
/// index 0 stands for "before the first entry".
pub type Index = u64;

/// One entry of the replicated log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Term of the leader that created the entry.
    pub term: Term,
    /// Position in the log.
    pub index: Index,
    /// What the entry carries.
    pub payload: Payload,
}

/// The content of a log entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Payload {
    /// Appended by every new leader. Committing it commits everything before
    /// it, which is how a leader learns the commit index of earlier terms.
    Noop,
    /// An opaque state-machine command, applied in log order on every node.
    Command(Vec<u8>),
}

/// The state a node must persist before it sends any message.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HardState {
    /// Latest term this node has seen.
    pub term: Term,
    /// Candidate this node voted for in `term`, if any.
    pub voted_for: Option<NodeId>,
}

/// A message between two Raft nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// Asks for a vote. With `pre_vote`, the candidate asks whether it
    /// *could* win at `term` without disrupting anyone: receivers do not
    /// change their own term or vote.
    Vote {
        /// The term the candidate is (or would be) campaigning in.
        term: Term,
        /// Index of the candidate's last log entry.
        last_log_index: Index,
        /// Term of the candidate's last log entry.
        last_log_term: Term,
        /// Whether this is a pre-vote.
        pre_vote: bool,
    },
    /// The answer to [`Message::Vote`].
    VoteResponse {
        /// The voter's term, or the pre-vote term if a pre-vote was granted.
        term: Term,
        /// Whether the vote was granted.
        granted: bool,
        /// Echoes the request's `pre_vote`.
        pre_vote: bool,
    },
    /// Replicates entries; with no entries it is a heartbeat.
    Append {
        /// The leader's term.
        term: Term,
        /// Index of the entry just before `entries`.
        prev_log_index: Index,
        /// Term of the entry at `prev_log_index`.
        prev_log_term: Term,
        /// Entries to store, in order.
        entries: Vec<Entry>,
        /// The leader's commit index.
        commit: Index,
        /// Read round this message belongs to; echoed in the response so the
        /// leader can confirm it still leads (see [`Raft::read_index`](crate::Raft::read_index)).
        read_seq: u64,
    },
    /// The answer to [`Message::Append`].
    AppendResponse {
        /// The follower's term.
        term: Term,
        /// Whether the follower's log matched at `prev_log_index`.
        success: bool,
        /// On success, the index of the last entry known to match the
        /// leader. On failure, a hint: the leader can resume from `index + 1`.
        index: Index,
        /// Echoes the request's `read_seq`.
        read_seq: u64,
    },
}

impl Message {
    /// The sender's term.
    pub fn term(&self) -> Term {
        match self {
            Message::Vote { term, .. }
            | Message::VoteResponse { term, .. }
            | Message::Append { term, .. }
            | Message::AppendResponse { term, .. } => *term,
        }
    }
}

/// A message addressed to a peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    /// Sending node.
    pub from: NodeId,
    /// Receiving node.
    pub to: NodeId,
    /// The message itself.
    pub message: Message,
}

/// What a node is currently doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Follows a leader, or waits for one.
    Follower,
    /// Asking peers whether an election could succeed, before starting one.
    PreCandidate,
    /// Running an election in its current term.
    Candidate,
    /// Accepts proposals and replicates them.
    Leader,
}

/// A confirmed linearizable read: once the state machine has applied
/// `index`, the read identified by `ctx` may be served from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadState {
    /// The caller's identifier from [`Raft::read_index`](crate::Raft::read_index).
    pub ctx: u64,
    /// Commit index at the time the read was received.
    pub index: Index,
}

/// Everything the driver must do after calling into the core, in order.
///
/// 1. Persist `hard_state`, then `entries`: delete every stored entry whose
///    index is at least `entries[0].index`, then append `entries`. Both must
///    be durable (fsynced) before step 2.
/// 2. Send `messages`.
/// 3. Apply `committed` to the state machine, in order.
/// 4. Serve each of `reads` once the state machine has applied its index,
///    and fail each of `aborted_reads` (leadership was lost).
#[derive(Debug, Default)]
#[must_use = "a Ready must be persisted, sent and applied"]
pub struct Ready {
    /// New term or vote, if it changed.
    pub hard_state: Option<HardState>,
    /// Log entries to persist, replacing any stored suffix from the first one.
    pub entries: Vec<Entry>,
    /// Messages to send to peers.
    pub messages: Vec<Envelope>,
    /// Newly committed entries to apply.
    pub committed: Vec<Entry>,
    /// Reads whose leadership check succeeded.
    pub reads: Vec<ReadState>,
    /// Reads that can no longer be confirmed by this node.
    pub aborted_reads: Vec<u64>,
}

impl Ready {
    /// Whether there is nothing to do.
    pub fn is_empty(&self) -> bool {
        self.hard_state.is_none()
            && self.entries.is_empty()
            && self.messages.is_empty()
            && self.committed.is_empty()
            && self.reads.is_empty()
            && self.aborted_reads.is_empty()
    }
}

/// A snapshot of a node's state, for monitoring and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Status {
    /// This node.
    pub id: NodeId,
    /// Current role.
    pub role: Role,
    /// Current term.
    pub term: Term,
    /// Leader of the current term, if known.
    pub leader: Option<NodeId>,
    /// Highest index known to be committed.
    pub commit: Index,
    /// Highest index handed to the driver for applying.
    pub applied: Index,
    /// Index of the last entry in this node's log.
    pub last_index: Index,
}
