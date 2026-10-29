//! Static configuration of one Raft node.

use crate::error::Error;
use crate::types::NodeId;

/// Configuration of one node. Time is measured in *ticks*: the driver calls
/// [`Raft::tick`](crate::Raft::tick) at a fixed interval (for example every 100 ms).
#[derive(Debug, Clone)]
pub struct Config {
    /// This node. Must be one of `voters`.
    pub id: NodeId,
    /// Every voting member, including this node. The membership is static.
    pub voters: Vec<NodeId>,
    /// Minimum ticks without hearing from a leader before campaigning. The
    /// actual timeout is randomized in `[election_ticks, 2 * election_ticks)`
    /// so that nodes rarely campaign at the same time.
    pub election_ticks: u32,
    /// Ticks between heartbeats. Must be smaller than `election_ticks`.
    pub heartbeat_ticks: u32,
    /// Upper bound on entries in one append message.
    pub max_append_entries: u64,
    /// Upper bound on entries sent to one follower but not yet acknowledged.
    pub max_inflight_entries: u64,
    /// Ask "could I win?" before starting an election, so a node that was
    /// partitioned away cannot force a new term when it comes back.
    pub pre_vote: bool,
    /// A leader that has not heard from a quorum for `election_ticks` steps
    /// down, and followers that recently heard from a leader ignore
    /// candidates. Required for ReadIndex to stay fast and safe in practice.
    pub check_quorum: bool,
    /// Seed for the election-timeout randomization. Use a random value in
    /// production and a fixed one in tests.
    pub seed: u64,
}

impl Config {
    /// A configuration with production defaults.
    pub fn new(id: NodeId, voters: impl IntoIterator<Item = NodeId>) -> Self {
        Config {
            id,
            voters: voters.into_iter().collect(),
            election_ticks: 10,
            heartbeat_ticks: 2,
            max_append_entries: 64,
            max_inflight_entries: 512,
            pre_vote: true,
            check_quorum: true,
            seed: 0,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.id == 0 || self.voters.contains(&0) {
            return Err(Error::Config("node ids must be non-zero"));
        }
        if !self.voters.contains(&self.id) {
            return Err(Error::Config("this node must be one of the voters"));
        }
        let mut sorted = self.voters.clone();
        sorted.sort_unstable();
        sorted.dedup();
        if sorted.len() != self.voters.len() {
            return Err(Error::Config("voters must be unique"));
        }
        if self.heartbeat_ticks == 0 || self.election_ticks <= self.heartbeat_ticks {
            return Err(Error::Config("election_ticks must exceed heartbeat_ticks, which must be non-zero"));
        }
        if self.max_append_entries == 0 || self.max_inflight_entries < self.max_append_entries {
            return Err(Error::Config(
                "max_inflight_entries must be at least max_append_entries, which must be non-zero",
            ));
        }
        Ok(())
    }
}
