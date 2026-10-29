//! The Raft state machine. It performs no I/O: every input is a method call
//! and every output is collected into the next [`Ready`].

use std::collections::{BTreeMap, VecDeque};
use std::mem;

use crate::config::Config;
use crate::error::Error;
use crate::rng::SplitMix64;
use crate::types::{
    Entry, Envelope, HardState, Index, Message, NodeId, Payload, ReadState, Ready, Role, Status, Term,
};

/// How the leader is replicating to one follower.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Replication {
    /// The follower's match point is unknown: send one append per heartbeat
    /// and walk `next` back on each rejection.
    Probe,
    /// The follower is in sync: stream entries optimistically.
    Replicate,
}

/// The leader's view of one follower.
#[derive(Debug)]
struct Progress {
    /// Highest index known to be replicated on the follower.
    matched: Index,
    /// Next index to send.
    next: Index,
    state: Replication,
    /// In `Probe`, an append is outstanding until the next heartbeat.
    probe_sent: bool,
    /// `matched` advanced since the previous heartbeat. A replicating
    /// follower that stops acknowledging has probably lost a message.
    acked_since_beat: bool,
    /// Heard from since the last check-quorum round.
    active: bool,
    /// Highest read round the follower has acknowledged in this term.
    read_ack: u64,
}

#[derive(Debug)]
struct PendingRead {
    ctx: u64,
    index: Index,
    seq: u64,
}

/// One Raft node.
///
/// Drive it by calling [`tick`](Raft::tick) on a timer, [`step`](Raft::step)
/// for every message from a peer, and [`propose`](Raft::propose) /
/// [`read_index`](Raft::read_index) for client requests. After any of those,
/// collect the work with [`ready`](Raft::ready) and carry it out in the order
/// documented on [`Ready`].
#[derive(Debug)]
pub struct Raft {
    id: NodeId,
    peers: Vec<NodeId>,
    quorum: usize,
    election_ticks: u32,
    heartbeat_ticks: u32,
    max_append_entries: u64,
    max_inflight_entries: u64,
    pre_vote: bool,
    check_quorum: bool,
    rng: SplitMix64,

    // Persistent state (mirrored in the driver's storage).
    term: Term,
    voted_for: Option<NodeId>,
    /// `log[i]` holds the entry with index `i + 1`.
    log: Vec<Entry>,

    // Volatile state.
    role: Role,
    leader: Option<NodeId>,
    commit: Index,
    applied: Index,
    election_elapsed: u32,
    election_timeout: u32,
    heartbeat_elapsed: u32,
    votes: BTreeMap<NodeId, bool>,
    progress: BTreeMap<NodeId, Progress>,

    // ReadIndex.
    read_seq: u64,
    /// A read round has been started since the last `ready()`; later reads
    /// can join it, because its messages have not been sent yet.
    read_round_open: bool,
    pending_reads: VecDeque<PendingRead>,
    /// Reads received before this leader committed an entry of its own term.
    reads_awaiting_commit: Vec<u64>,

    // Output collected for the next `Ready`.
    hard_state_dirty: bool,
    unstable_from: Option<Index>,
    messages: Vec<Envelope>,
    ready_reads: Vec<ReadState>,
    aborted_reads: Vec<u64>,
}

impl Raft {
    /// Restores a node from its persisted state.
    ///
    /// `applied` is the last index the state machine has durably applied
    /// (0 for a fresh node). Entries up to it count as committed and are not
    /// handed out again.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] for an unusable config, [`Error::Storage`] if the log
    /// is not contiguous from index 1, its terms decrease or exceed
    /// `hard_state.term`, or `applied` is beyond its end.
    pub fn new(
        config: &Config,
        hard_state: HardState,
        log: Vec<Entry>,
        applied: Index,
    ) -> Result<Self, Error> {
        config.validate()?;
        for (i, entry) in log.iter().enumerate() {
            let index = i as Index + 1;
            if entry.index != index {
                return Err(Error::Storage { index, reason: "log is not contiguous from index 1" });
            }
            if i > 0 && entry.term < log[i - 1].term {
                return Err(Error::Storage { index, reason: "log terms decrease" });
            }
            if entry.term > hard_state.term {
                return Err(Error::Storage { index, reason: "entry term is newer than the persisted term" });
            }
        }
        if applied > log.len() as Index {
            return Err(Error::Storage {
                index: applied,
                reason: "applied index is beyond the end of the log",
            });
        }

        let peers: Vec<NodeId> = config.voters.iter().copied().filter(|&v| v != config.id).collect();
        let mut raft = Raft {
            id: config.id,
            quorum: config.voters.len() / 2 + 1,
            peers,
            election_ticks: config.election_ticks,
            heartbeat_ticks: config.heartbeat_ticks,
            max_append_entries: config.max_append_entries,
            max_inflight_entries: config.max_inflight_entries,
            pre_vote: config.pre_vote,
            check_quorum: config.check_quorum,
            rng: SplitMix64::new(config.seed ^ config.id.wrapping_mul(0x9E37_79B9_7F4A_7C15)),
            term: hard_state.term,
            voted_for: hard_state.voted_for,
            log,
            role: Role::Follower,
            leader: None,
            commit: applied,
            applied,
            election_elapsed: 0,
            election_timeout: 0,
            heartbeat_elapsed: 0,
            votes: BTreeMap::new(),
            progress: BTreeMap::new(),
            read_seq: 0,
            read_round_open: false,
            pending_reads: VecDeque::new(),
            reads_awaiting_commit: Vec::new(),
            hard_state_dirty: false,
            unstable_from: None,
            messages: Vec::new(),
            ready_reads: Vec::new(),
            aborted_reads: Vec::new(),
        };
        raft.reset_election_timer();
        Ok(raft)
    }

    /// Advances logical time by one tick.
    pub fn tick(&mut self) {
        self.election_elapsed += 1;
        if self.role != Role::Leader {
            if self.election_elapsed >= self.election_timeout {
                self.campaign();
            }
            return;
        }

        self.heartbeat_elapsed += 1;
        if self.election_elapsed >= self.election_ticks {
            self.election_elapsed = 0;
            if self.check_quorum && !self.quorum_recently_active() {
                self.become_follower(self.term, None);
                return;
            }
            for pr in self.progress.values_mut() {
                pr.active = false;
            }
        }
        if self.heartbeat_elapsed >= self.heartbeat_ticks {
            self.heartbeat_elapsed = 0;
            self.heartbeat();
        }
    }

    /// Handles a message from peer `from`. Messages from unknown nodes are
    /// ignored, as are stale ones.
    pub fn step(&mut self, from: NodeId, message: Message) {
        if from == self.id || !self.peers.contains(&from) {
            return;
        }
        let term = message.term();
        if term > self.term {
            match message {
                // A pre-vote never changes the receiver's term.
                Message::Vote { pre_vote: true, .. } => {}
                // Granted pre-votes carry the term we *would* campaign in.
                Message::VoteResponse { pre_vote: true, granted: true, .. } => {}
                // We have a live leader; this candidate is the disruptive one.
                Message::Vote { .. } if self.in_leader_lease() => return,
                Message::Append { .. } => self.become_follower(term, Some(from)),
                _ => self.become_follower(term, None),
            }
        } else if term < self.term {
            match message {
                // Tell a deposed leader that it has been replaced.
                Message::Append { .. } => {
                    self.send(
                        from,
                        Message::AppendResponse { term: self.term, success: false, index: 0, read_seq: 0 },
                    );
                }
                Message::Vote { pre_vote: true, .. } => {
                    self.send(
                        from,
                        Message::VoteResponse { term: self.term, granted: false, pre_vote: true },
                    );
                }
                _ => {}
            }
            return;
        }

        match message {
            Message::Vote { term, last_log_index, last_log_term, pre_vote } => {
                self.handle_vote(from, term, last_log_index, last_log_term, pre_vote);
            }
            Message::VoteResponse { term, granted, pre_vote } => {
                self.handle_vote_response(from, term, granted, pre_vote)
            }
            Message::Append { prev_log_index, prev_log_term, entries, commit, read_seq, .. } => {
                self.handle_append(from, prev_log_index, prev_log_term, entries, commit, read_seq);
            }
            Message::AppendResponse { success, index, read_seq, .. } => {
                self.handle_append_response(from, success, index, read_seq);
            }
        }
    }

    /// Appends a command to the log if this node is the leader. Returns the
    /// entry's index and term: the command took effect once an entry with
    /// that index *and term* is committed. If a different term shows up at
    /// that index, the proposal was lost in a leader change.
    ///
    /// # Errors
    ///
    /// [`Error::NotLeader`] with the known leader, if any.
    pub fn propose(&mut self, command: Vec<u8>) -> Result<(Index, Term), Error> {
        if self.role != Role::Leader {
            return Err(Error::NotLeader { leader: self.leader });
        }
        let index = self.append(Payload::Command(command));
        self.broadcast_append(false);
        Ok((index, self.term))
    }

    /// Starts a linearizable read identified by `ctx`.
    ///
    /// The leader records its commit index, then confirms with a quorum of
    /// heartbeats that it was still the leader when the read arrived. The
    /// result is a [`ReadState`] in a later [`Ready`]; if leadership is lost
    /// first, `ctx` appears in [`Ready::aborted_reads`] instead.
    ///
    /// # Errors
    ///
    /// [`Error::NotLeader`] with the known leader, if any.
    pub fn read_index(&mut self, ctx: u64) -> Result<(), Error> {
        if self.role != Role::Leader {
            return Err(Error::NotLeader { leader: self.leader });
        }
        // Until it commits an entry of its own term, a new leader may not
        // know the true commit index of the previous term.
        if self.term_at(self.commit) != Some(self.term) {
            self.reads_awaiting_commit.push(ctx);
        } else {
            self.start_read(ctx);
        }
        Ok(())
    }

    /// Whether [`ready`](Raft::ready) would return any work.
    pub fn has_ready(&self) -> bool {
        self.hard_state_dirty
            || self.unstable_from.is_some()
            || !self.messages.is_empty()
            || self.commit > self.applied
            || !self.ready_reads.is_empty()
            || !self.aborted_reads.is_empty()
    }

    /// Takes the work accumulated since the previous call.
    pub fn ready(&mut self) -> Ready {
        let hard_state = mem::take(&mut self.hard_state_dirty)
            .then_some(HardState { term: self.term, voted_for: self.voted_for });
        let entries = match self.unstable_from.take() {
            Some(from) => self.log[(from - 1) as usize..].to_vec(),
            None => Vec::new(),
        };
        let committed = if self.commit > self.applied {
            let entries = self.log[self.applied as usize..self.commit as usize].to_vec();
            self.applied = self.commit;
            entries
        } else {
            Vec::new()
        };
        self.read_round_open = false;
        Ready {
            hard_state,
            entries,
            messages: mem::take(&mut self.messages),
            committed,
            reads: mem::take(&mut self.ready_reads),
            aborted_reads: mem::take(&mut self.aborted_reads),
        }
    }

    /// The node's current state.
    pub fn status(&self) -> Status {
        Status {
            id: self.id,
            role: self.role,
            term: self.term,
            leader: self.leader,
            commit: self.commit,
            applied: self.applied,
            last_index: self.last_index(),
        }
    }

    // ----- elections -------------------------------------------------------

    fn campaign(&mut self) {
        if self.pre_vote {
            self.become_pre_candidate();
        } else {
            self.become_candidate();
        }
    }

    fn become_pre_candidate(&mut self) {
        self.leave_leadership();
        self.role = Role::PreCandidate;
        self.leader = None;
        self.reset_election_timer();
        self.votes.clear();
        self.votes.insert(self.id, true);
        if self.vote_won() {
            self.become_candidate();
        } else {
            self.request_votes(true);
        }
    }

    fn become_candidate(&mut self) {
        self.leave_leadership();
        self.term += 1;
        self.voted_for = Some(self.id);
        self.hard_state_dirty = true;
        self.role = Role::Candidate;
        self.leader = None;
        self.reset_election_timer();
        self.votes.clear();
        self.votes.insert(self.id, true);
        if self.vote_won() {
            self.become_leader();
        } else {
            self.request_votes(false);
        }
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader = Some(self.id);
        self.election_elapsed = 0;
        self.heartbeat_elapsed = 0;
        let next = self.last_index() + 1;
        self.progress = self
            .peers
            .iter()
            .map(|&peer| {
                let pr = Progress {
                    matched: 0,
                    next,
                    state: Replication::Probe,
                    probe_sent: false,
                    acked_since_beat: false,
                    active: false,
                    read_ack: 0,
                };
                (peer, pr)
            })
            .collect();
        self.append(Payload::Noop);
        self.broadcast_append(false);
    }

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        self.leave_leadership();
        if term > self.term {
            self.term = term;
            self.voted_for = None;
            self.hard_state_dirty = true;
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.reset_election_timer();
        self.votes.clear();
    }

    /// Drops leader-only state. Reads that were not yet confirmed can no
    /// longer be, so their callers must retry elsewhere.
    fn leave_leadership(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        self.progress.clear();
        self.aborted_reads.extend(self.pending_reads.drain(..).map(|r| r.ctx));
        self.aborted_reads.append(&mut self.reads_awaiting_commit);
    }

    fn request_votes(&mut self, pre_vote: bool) {
        let term = if pre_vote { self.term + 1 } else { self.term };
        let (last_log_index, last_log_term) = (self.last_index(), self.last_term());
        for i in 0..self.peers.len() {
            let to = self.peers[i];
            self.send(to, Message::Vote { term, last_log_index, last_log_term, pre_vote });
        }
    }

    fn handle_vote(
        &mut self,
        from: NodeId,
        term: Term,
        last_log_index: Index,
        last_log_term: Term,
        pre_vote: bool,
    ) {
        let up_to_date = last_log_term > self.last_term()
            || (last_log_term == self.last_term() && last_log_index >= self.last_index());
        if pre_vote {
            // `term` is the candidate's term + 1. Granting changes nothing here.
            let granted = term > self.term && up_to_date && !self.in_leader_lease();
            let term = if granted { term } else { self.term };
            self.send(from, Message::VoteResponse { term, granted, pre_vote: true });
            return;
        }
        let can_vote = self.voted_for == Some(from) || (self.voted_for.is_none() && self.leader.is_none());
        let granted = can_vote && up_to_date;
        if granted {
            self.voted_for = Some(from);
            self.hard_state_dirty = true;
            self.election_elapsed = 0;
        }
        self.send(from, Message::VoteResponse { term: self.term, granted, pre_vote: false });
    }

    fn handle_vote_response(&mut self, from: NodeId, term: Term, granted: bool, pre_vote: bool) {
        let counts = match (self.role, pre_vote) {
            (Role::PreCandidate, true) => !granted || term == self.term + 1,
            (Role::Candidate, false) => term == self.term,
            _ => false,
        };
        if !counts {
            return;
        }
        self.votes.insert(from, granted);
        if self.vote_won() {
            if self.role == Role::PreCandidate {
                self.become_candidate();
            } else {
                self.become_leader();
            }
        } else if self.votes.values().filter(|&&g| !g).count() >= self.quorum {
            self.become_follower(self.term, None);
        }
    }

    fn vote_won(&self) -> bool {
        self.votes.values().filter(|&&g| g).count() >= self.quorum
    }

    /// A follower that heard from its leader within the minimum election
    /// timeout, or a leader inside its check-quorum window.
    fn in_leader_lease(&self) -> bool {
        self.check_quorum && self.leader.is_some() && self.election_elapsed < self.election_ticks
    }

    fn quorum_recently_active(&self) -> bool {
        1 + self.progress.values().filter(|pr| pr.active).count() >= self.quorum
    }

    fn reset_election_timer(&mut self) {
        self.election_elapsed = 0;
        self.election_timeout = self.election_ticks + self.rng.below(self.election_ticks);
    }

    // ----- replication -----------------------------------------------------

    fn append(&mut self, payload: Payload) -> Index {
        let index = self.last_index() + 1;
        self.log.push(Entry { term: self.term, index, payload });
        self.mark_unstable(index);
        if self.peers.is_empty() {
            self.maybe_commit();
        }
        index
    }

    fn heartbeat(&mut self) {
        let last = self.last_index();
        for pr in self.progress.values_mut() {
            if pr.state == Replication::Replicate && !pr.acked_since_beat && pr.matched < last {
                pr.state = Replication::Probe;
                pr.next = pr.matched + 1;
            }
            pr.acked_since_beat = false;
            pr.probe_sent = false;
        }
        self.broadcast_append(true);
    }

    fn broadcast_append(&mut self, heartbeat: bool) {
        for i in 0..self.peers.len() {
            let to = self.peers[i];
            self.send_append(to, heartbeat);
        }
    }

    /// Sends the next batch of entries to `to`. With `heartbeat`, sends a
    /// message even if there is nothing new, to assert leadership and carry
    /// the commit index and read round.
    fn send_append(&mut self, to: NodeId, heartbeat: bool) {
        let last = self.last_index();
        let (max_batch, max_inflight) = (self.max_append_entries, self.max_inflight_entries);
        let Some(pr) = self.progress.get_mut(&to) else { return };
        let (prev, count) = match pr.state {
            Replication::Probe => {
                if pr.probe_sent && !heartbeat {
                    return;
                }
                pr.probe_sent = true;
                (pr.next - 1, (last + 1 - pr.next).min(max_batch))
            }
            Replication::Replicate => {
                let inflight = pr.next - 1 - pr.matched;
                let count = (last + 1 - pr.next).min(max_batch).min(max_inflight.saturating_sub(inflight));
                if count > 0 {
                    let prev = pr.next - 1;
                    pr.next += count;
                    (prev, count)
                } else if heartbeat {
                    // Anchor at the acknowledged point, which the follower is
                    // known to have, so in-flight entries cause no rejection.
                    (pr.matched, 0)
                } else {
                    return;
                }
            }
        };
        let entries = self.log[prev as usize..(prev + count) as usize].to_vec();
        let prev_log_term = self.term_at(prev).expect("prev_log_index lies within the leader's log");
        let message = Message::Append {
            term: self.term,
            prev_log_index: prev,
            prev_log_term,
            entries,
            commit: self.commit,
            read_seq: self.read_seq,
        };
        self.send(to, message);
    }

    fn handle_append(
        &mut self,
        from: NodeId,
        prev_log_index: Index,
        prev_log_term: Term,
        entries: Vec<Entry>,
        commit: Index,
        read_seq: u64,
    ) {
        if self.role != Role::Follower {
            self.become_follower(self.term, Some(from));
        }
        self.leader = Some(from);
        self.election_elapsed = 0;

        let reject =
            |hint: Index, term: Term| Message::AppendResponse { term, success: false, index: hint, read_seq };
        match self.term_at(prev_log_index) {
            None => {
                let message = reject(self.last_index(), self.term);
                self.send(from, message);
                return;
            }
            Some(conflict_term) if conflict_term != prev_log_term => {
                // Skip the whole conflicting term in one round trip. Entries
                // up to `commit` are known to match, so never hint below it.
                let mut hint = prev_log_index - 1;
                while hint > self.commit && self.term_at(hint) == Some(conflict_term) {
                    hint -= 1;
                }
                let message = reject(hint, self.term);
                self.send(from, message);
                return;
            }
            Some(_) => {}
        }

        let last_new = prev_log_index + entries.len() as Index;
        for entry in entries {
            match self.term_at(entry.index) {
                Some(term) if term == entry.term => continue,
                Some(_) => {
                    assert!(
                        entry.index > self.commit,
                        "Raft safety violation: leader {from} overwrites committed entry {} on node {}",
                        entry.index,
                        self.id
                    );
                    self.log.truncate(entry.index as usize - 1);
                }
                None => {}
            }
            self.mark_unstable(entry.index);
            self.log.push(entry);
        }
        // Only entries verified to match the leader may be marked committed.
        let commit = commit.min(last_new);
        if commit > self.commit {
            self.commit = commit;
        }
        self.send(
            from,
            Message::AppendResponse { term: self.term, success: true, index: last_new, read_seq },
        );
    }

    fn handle_append_response(&mut self, from: NodeId, success: bool, index: Index, read_seq: u64) {
        if self.role != Role::Leader {
            return;
        }
        let last = self.last_index();
        let Some(pr) = self.progress.get_mut(&from) else { return };
        pr.active = true;
        pr.read_ack = pr.read_ack.max(read_seq);
        if success {
            if index > pr.matched {
                pr.matched = index;
                pr.acked_since_beat = true;
            }
            if pr.state == Replication::Probe {
                pr.state = Replication::Replicate;
                pr.next = pr.matched + 1;
            } else {
                pr.next = pr.next.max(pr.matched + 1);
            }
            self.maybe_commit();
        } else {
            // A late rejection may refer to an older message; never move
            // below what the follower has already acknowledged.
            pr.next = (index + 1).min(pr.next).clamp(pr.matched + 1, last + 1);
            pr.state = Replication::Probe;
            pr.probe_sent = false;
        }
        self.send_append(from, false);
        self.advance_reads();
    }

    /// Commits the highest index stored on a quorum, but only if it belongs
    /// to the current term (Raft §5.4.2): older entries are committed
    /// indirectly, by committing a newer one.
    fn maybe_commit(&mut self) {
        let mut matched: Vec<Index> = self.progress.values().map(|pr| pr.matched).collect();
        matched.push(self.last_index());
        matched.sort_unstable_by(|a, b| b.cmp(a));
        let candidate = matched[self.quorum - 1];
        if candidate > self.commit && self.term_at(candidate) == Some(self.term) {
            self.commit = candidate;
            for ctx in mem::take(&mut self.reads_awaiting_commit) {
                self.start_read(ctx);
            }
        }
    }

    // ----- reads -----------------------------------------------------------

    fn start_read(&mut self, ctx: u64) {
        if self.peers.is_empty() {
            self.ready_reads.push(ReadState { ctx, index: self.commit });
            return;
        }
        if !self.read_round_open {
            self.read_round_open = true;
            self.read_seq += 1;
            self.broadcast_append(true);
        }
        self.pending_reads.push_back(PendingRead { ctx, index: self.commit, seq: self.read_seq });
    }

    fn advance_reads(&mut self) {
        while let Some(front) = self.pending_reads.front() {
            let acks = 1 + self.progress.values().filter(|pr| pr.read_ack >= front.seq).count();
            if acks < self.quorum {
                break;
            }
            let read = self.pending_reads.pop_front().expect("front exists");
            self.ready_reads.push(ReadState { ctx: read.ctx, index: read.index });
        }
    }

    // ----- log helpers -----------------------------------------------------

    fn last_index(&self) -> Index {
        self.log.len() as Index
    }

    fn last_term(&self) -> Term {
        self.log.last().map_or(0, |e| e.term)
    }

    fn term_at(&self, index: Index) -> Option<Term> {
        if index == 0 { Some(0) } else { self.log.get(index as usize - 1).map(|e| e.term) }
    }

    fn mark_unstable(&mut self, index: Index) {
        self.unstable_from = Some(self.unstable_from.map_or(index, |from| from.min(index)));
    }

    fn send(&mut self, to: NodeId, message: Message) {
        self.messages.push(Envelope { from: self.id, to, message });
    }
}
