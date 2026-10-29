//! Deterministic simulation of whole Raft clusters.
//!
//! Each seed builds a cluster of 3 or 5 nodes on a simulated network that
//! drops, duplicates, delays and reorders messages, splits the cluster into
//! partitions and crashes and restarts nodes (keeping only what they
//! persisted). Clients propose commands and issue linearizable reads
//! throughout. After every step the simulator checks Raft's guarantees:
//!
//! * **Election safety**: at most one leader per term.
//! * **One vote per term**: a node never changes its vote within a term.
//! * **Leader completeness**: a new leader's log holds every entry committed
//!   in earlier terms.
//! * **State machine safety**: every node applies the same entry at each
//!   index, in index order, without gaps.
//! * **Log matching**: two logs that agree on an entry agree on everything
//!   before it.
//! * **Linearizable reads**: a read is served at an index no lower than any
//!   write that was acknowledged before the read was issued.
//!
//! Then the faults stop, and the cluster must converge (**liveness**): a
//! single stable leader, and every command proposed to it is applied by
//! every node.
//!
//! Everything, including the nodes' election timers, derives from the seed,
//! so a failure message names a seed that reproduces it exactly:
//!
//! ```text
//! EMBERKV_SIM_SEED=1234 cargo test -p emberkv-raft --test simulation
//! EMBERKV_SIM_SEEDS=5000 cargo test -p emberkv-raft --test simulation --release
//! ```

use std::collections::BTreeMap;

use emberkv_raft::{Config, Entry, Envelope, Error, HardState, Index, NodeId, Payload, Raft, Role, Term};

const CHAOS_TICKS: u64 = 1_500;
const HEAL_TICKS: u64 = 600;
/// After healing, the cluster gets this long to settle before it must be stable.
const SETTLE_TICKS: u64 = 150;
/// Proposals stop this long before the end, so the last ones can commit.
const QUIET_TICKS: u64 = 150;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn chance(&mut self, p: f64) -> bool {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64 <= p
    }

    fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[self.below(items.len() as u64) as usize]
    }
}

/// Fault rates for one seed, drawn at random so seeds cover gentle and
/// hostile networks alike.
#[derive(Debug, Clone, Copy)]
struct Faults {
    drop: f64,
    duplicate: f64,
    max_latency: u64,
    repartition: f64,
    crash: f64,
    pre_vote: bool,
    check_quorum: bool,
}

impl Faults {
    fn random(rng: &mut Rng) -> Self {
        Faults {
            drop: rng.pick(&[0.0, 0.02, 0.1, 0.25]),
            duplicate: rng.pick(&[0.0, 0.02, 0.1]),
            max_latency: rng.pick(&[1, 2, 4]),
            repartition: rng.pick(&[0.0, 0.005, 0.02]),
            crash: rng.pick(&[0.0, 0.002, 0.01]),
            pre_vote: rng.below(4) != 0,
            check_quorum: rng.below(4) != 0,
        }
    }
}

/// One node: the Raft core plus what it persisted and what it applied.
/// A crash drops `raft`; `hard_state`, `log` and `applied` survive.
struct Node {
    raft: Option<Raft>,
    hard_state: HardState,
    log: Vec<Entry>,
    applied: Vec<Entry>,
    restarts: u64,
}

struct Packet {
    at: u64,
    envelope: Envelope,
}

#[derive(Debug, Default)]
struct Stats {
    elections: u64,
    committed: u64,
    reads_served: u64,
    reads_aborted: u64,
    messages: u64,
    dropped: u64,
    crashes: u64,
}

struct Sim {
    seed: u64,
    rng: Rng,
    now: u64,
    faults: Faults,
    chaos: bool,
    ids: Vec<NodeId>,
    nodes: BTreeMap<NodeId, Node>,
    network: Vec<Packet>,
    /// Partition group of each node; only nodes in the same group can talk.
    group: BTreeMap<NodeId, u8>,

    leaders: BTreeMap<Term, NodeId>,
    /// Tick at which each new leader was first observed.
    elections_at: Vec<u64>,
    /// Every entry any node has applied, by index.
    committed: BTreeMap<Index, Entry>,
    /// Highest index applied anywhere: the newest acknowledged write.
    max_applied: Index,
    /// Outstanding reads: ctx → the newest acknowledged write when issued.
    read_floor: BTreeMap<u64, Index>,
    /// Accepted proposals: (index, term, payload, tick).
    proposals: Vec<(Index, Term, Payload, u64)>,
    next_ctx: u64,
    next_command: u64,
    stats: Stats,
}

impl Sim {
    fn new(seed: u64) -> Self {
        let mut rng = Rng(seed);
        let size = if rng.below(2) == 0 { 3 } else { 5 };
        let faults = Faults::random(&mut rng);
        let ids: Vec<NodeId> = (1..=size).collect();
        let mut sim = Sim {
            seed,
            rng,
            now: 0,
            faults,
            chaos: true,
            nodes: BTreeMap::new(),
            network: Vec::new(),
            group: ids.iter().map(|&id| (id, 0)).collect(),
            ids,
            leaders: BTreeMap::new(),
            elections_at: Vec::new(),
            committed: BTreeMap::new(),
            max_applied: 0,
            read_floor: BTreeMap::new(),
            proposals: Vec::new(),
            next_ctx: 1,
            next_command: 1,
            stats: Stats::default(),
        };
        for i in 0..sim.ids.len() {
            let id = sim.ids[i];
            let node = Node {
                raft: None,
                hard_state: HardState::default(),
                log: Vec::new(),
                applied: Vec::new(),
                restarts: 0,
            };
            sim.nodes.insert(id, node);
            sim.start(id);
        }
        sim
    }

    #[track_caller]
    fn fail(&self, what: &str) -> ! {
        panic!(
            "\nseed {} (tick {}, {} nodes, {:?}): {what}\nreproduce: EMBERKV_SIM_SEED={} cargo test -p emberkv-raft --test simulation\n",
            self.seed,
            self.now,
            self.ids.len(),
            self.faults,
            self.seed
        );
    }

    fn config(&self, id: NodeId, restarts: u64) -> Config {
        let mut config = Config::new(id, self.ids.iter().copied());
        config.election_ticks = 12;
        config.heartbeat_ticks = 3;
        config.max_append_entries = 8;
        config.max_inflight_entries = 32;
        config.pre_vote = self.faults.pre_vote;
        config.check_quorum = self.faults.check_quorum;
        config.seed = self.seed ^ (restarts << 32);
        config
    }

    fn start(&mut self, id: NodeId) {
        let node = &self.nodes[&id];
        let config = self.config(id, node.restarts);
        let raft = Raft::new(&config, node.hard_state, node.log.clone(), node.applied.len() as Index)
            .unwrap_or_else(|e| self.fail(&format!("node {id} failed to restart: {e}")));
        self.nodes.get_mut(&id).expect("node").raft = Some(raft);
    }

    fn alive(&self) -> Vec<NodeId> {
        self.ids.iter().copied().filter(|id| self.nodes[id].raft.is_some()).collect()
    }

    fn run(&mut self) {
        for _ in 0..CHAOS_TICKS {
            self.inject_faults();
            self.tick();
        }
        // Heal: everyone up, one partition, a reliable network.
        self.chaos = false;
        for id in self.ids.clone() {
            self.group.insert(id, 0);
            if self.nodes[&id].raft.is_none() {
                self.restart(id);
            }
        }
        let heal_start = self.now;
        let proposals_before = self.proposals.len();
        for _ in 0..HEAL_TICKS {
            self.tick();
        }
        self.check_liveness(heal_start, proposals_before);
        self.check_log_matching();
    }

    fn inject_faults(&mut self) {
        if self.rng.chance(self.faults.repartition) {
            match self.rng.below(3) {
                0 => self.ids.iter().for_each(|&id| {
                    self.group.insert(id, 0);
                }),
                1 => {
                    // Isolate the current leader, if there is one.
                    let leader = self.alive().into_iter().find(|id| self.role(*id) == Some(Role::Leader));
                    for &id in &self.ids {
                        self.group.insert(id, u8::from(Some(id) == leader));
                    }
                }
                _ => {
                    for i in 0..self.ids.len() {
                        let g = self.rng.below(2) as u8;
                        self.group.insert(self.ids[i], g);
                    }
                }
            }
        }
        if self.rng.chance(self.faults.crash) {
            let id = self.rng.pick(&self.ids);
            if self.nodes[&id].raft.is_some() {
                self.nodes.get_mut(&id).expect("node").raft = None;
                self.stats.crashes += 1;
            } else {
                self.restart(id);
            }
        }
    }

    fn restart(&mut self, id: NodeId) {
        self.nodes.get_mut(&id).expect("node").restarts += 1;
        self.start(id);
        self.drain(id);
    }

    fn role(&self, id: NodeId) -> Option<Role> {
        self.nodes[&id].raft.as_ref().map(|r| r.status().role)
    }

    fn tick(&mut self) {
        for id in self.alive() {
            self.nodes.get_mut(&id).and_then(|n| n.raft.as_mut()).expect("alive").tick();
            self.drain(id);
        }

        // Deliver due packets in send order; random latency already reorders.
        let now = self.now;
        let (due, later): (Vec<Packet>, Vec<Packet>) = self.network.drain(..).partition(|p| p.at <= now);
        self.network = later;
        for packet in due {
            let Envelope { from, to, message } = packet.envelope;
            if self.group[&from] != self.group[&to] {
                self.stats.dropped += 1;
                continue;
            }
            let Some(raft) = self.nodes.get_mut(&to).and_then(|n| n.raft.as_mut()) else { continue };
            raft.step(from, message);
            self.drain(to);
        }

        self.clients();
        self.now += 1;
    }

    /// Clients send requests to random live nodes, as if they did not know
    /// who leads. Non-leaders refuse, which is fine.
    fn clients(&mut self) {
        let alive = self.alive();
        if alive.is_empty() {
            return;
        }
        let quiet = !self.chaos && self.now >= CHAOS_TICKS + HEAL_TICKS - QUIET_TICKS;
        for _ in 0..2 {
            if quiet || !self.rng.chance(0.5) {
                continue;
            }
            let id = self.rng.pick(&alive);
            let command = format!("cmd-{}", self.next_command).into_bytes();
            self.next_command += 1;
            let raft = self.nodes.get_mut(&id).and_then(|n| n.raft.as_mut()).expect("alive");
            match raft.propose(command.clone()) {
                Ok((index, term)) => self.proposals.push((index, term, Payload::Command(command), self.now)),
                Err(Error::NotLeader { .. }) => {}
                Err(e) => self.fail(&format!("unexpected propose error: {e}")),
            }
            self.drain(id);
        }
        if self.rng.chance(0.3) {
            let id = self.rng.pick(&alive);
            let ctx = self.next_ctx;
            self.next_ctx += 1;
            let raft = self.nodes.get_mut(&id).and_then(|n| n.raft.as_mut()).expect("alive");
            if raft.read_index(ctx).is_ok() {
                self.read_floor.insert(ctx, self.max_applied);
            }
            self.drain(id);
        }
    }

    fn send(&mut self, envelope: Envelope) {
        self.stats.messages += 1;
        if self.chaos && self.rng.chance(self.faults.drop) {
            self.stats.dropped += 1;
            return;
        }
        if self.chaos && self.rng.chance(self.faults.duplicate) {
            let at = self.now + 1 + self.rng.below(self.faults.max_latency);
            self.network.push(Packet { at, envelope: envelope.clone() });
        }
        let at = self.now + 1 + self.rng.below(self.faults.max_latency);
        self.network.push(Packet { at, envelope });
    }

    /// Carries out every `Ready` of node `id` exactly as a real driver
    /// would (persist, send, apply), checking invariants along the way.
    fn drain(&mut self, id: NodeId) {
        loop {
            let Some(raft) = self.nodes.get_mut(&id).and_then(|n| n.raft.as_mut()) else { return };
            if !raft.has_ready() {
                break;
            }
            let ready = raft.ready();

            // 1. Persist.
            let node = self.nodes.get_mut(&id).expect("node");
            if let Some(hs) = ready.hard_state {
                let old = node.hard_state;
                if hs.term < old.term {
                    self.fail(&format!("node {id} persisted term {} after term {}", hs.term, old.term));
                }
                if hs.term == old.term && old.voted_for.is_some() && hs.voted_for != old.voted_for {
                    self.fail(&format!(
                        "node {id} changed its vote in term {}: {:?} -> {:?}",
                        hs.term, old.voted_for, hs.voted_for
                    ));
                }
                node.hard_state = hs;
            }
            if let Some(first) = ready.entries.first().map(|e| e.index) {
                let node = self.nodes.get_mut(&id).expect("node");
                if first > node.log.len() as Index + 1 {
                    self.fail(&format!("node {id} persisted entry {first} past the end of its log"));
                }
                if first <= node.applied.len() as Index {
                    self.fail(&format!("node {id} rewrote applied entry {first}"));
                }
                let node = self.nodes.get_mut(&id).expect("node");
                node.log.truncate(first as usize - 1);
                node.log.extend(ready.entries);
            }

            // 2. Send.
            for envelope in ready.messages {
                self.send(envelope);
            }

            // 3. Apply.
            for entry in ready.committed {
                let applied = self.nodes[&id].applied.len() as Index;
                if entry.index != applied + 1 {
                    self.fail(&format!("node {id} applied entry {} after {applied}", entry.index));
                }
                match self.committed.get(&entry.index) {
                    Some(existing) if *existing != entry => self.fail(&format!(
                        "state machine safety: node {id} applied {entry:?} at {}, another node applied {existing:?}",
                        entry.index
                    )),
                    Some(_) => {}
                    None => {
                        self.committed.insert(entry.index, entry.clone());
                        self.stats.committed += 1;
                    }
                }
                self.max_applied = self.max_applied.max(entry.index);
                self.nodes.get_mut(&id).expect("node").applied.push(entry);
            }

            // 4. Reads.
            for read in ready.reads {
                let floor =
                    self.read_floor.remove(&read.ctx).unwrap_or_else(|| self.fail("read confirmed twice"));
                if read.index < floor {
                    self.fail(&format!(
                        "stale read: node {id} served read {} at index {}, but index {floor} was already acknowledged",
                        read.ctx, read.index
                    ));
                }
                self.stats.reads_served += 1;
            }
            for ctx in ready.aborted_reads {
                self.read_floor.remove(&ctx);
                self.stats.reads_aborted += 1;
            }
        }
        self.observe_leadership(id);
    }

    fn observe_leadership(&mut self, id: NodeId) {
        let Some(status) = self.nodes[&id].raft.as_ref().map(|r| r.status()) else { return };
        if status.role != Role::Leader {
            return;
        }
        match self.leaders.get(&status.term) {
            Some(&other) if other != id => {
                self.fail(&format!("election safety: nodes {other} and {id} both lead term {}", status.term))
            }
            Some(_) => {}
            None => {
                self.leaders.insert(status.term, id);
                self.elections_at.push(self.now);
                self.stats.elections += 1;
                let log = &self.nodes[&id].log;
                for (index, entry) in self.committed.range(..) {
                    if entry.term < status.term && log.get(*index as usize - 1) != Some(entry) {
                        self.fail(&format!(
                            "leader completeness: node {id} leads term {} without committed entry {index}",
                            status.term
                        ));
                    }
                }
            }
        }
    }

    fn check_liveness(&self, heal_start: u64, proposals_before: usize) {
        let statuses: Vec<_> =
            self.ids.iter().map(|id| self.nodes[id].raft.as_ref().expect("all up").status()).collect();
        let leaders: Vec<_> = statuses.iter().filter(|s| s.role == Role::Leader).collect();
        if leaders.len() != 1 {
            self.fail(&format!("liveness: {} leaders after healing: {statuses:?}", leaders.len()));
        }
        let leader = leaders[0];
        if statuses.iter().any(|s| s.term != leader.term || s.leader != Some(leader.id)) {
            self.fail(&format!("liveness: nodes disagree on the leader after healing: {statuses:?}"));
        }
        if let Some(at) = self.elections_at.iter().find(|&&t| t > heal_start + SETTLE_TICKS) {
            self.fail(&format!(
                "stability: a new leader was elected at tick {at}, after the healed cluster settled"
            ));
        }
        let applied: Vec<_> = statuses.iter().map(|s| self.nodes[&s.id].applied.len()).collect();
        if applied.iter().any(|&a| a as Index != leader.commit) {
            self.fail(&format!(
                "liveness: nodes did not converge on commit {}: applied {applied:?}",
                leader.commit
            ));
        }
        for (index, term, payload, at) in &self.proposals[proposals_before..] {
            if *at < heal_start + SETTLE_TICKS || *term != leader.term {
                continue;
            }
            match self.committed.get(index) {
                Some(entry) if entry.term == *term && entry.payload == *payload => {}
                _ => self.fail(&format!(
                    "liveness: proposal at index {index}, term {term} (tick {at}) never committed"
                )),
            }
        }
    }

    fn check_log_matching(&self) {
        for (i, &a) in self.ids.iter().enumerate() {
            for &b in &self.ids[i + 1..] {
                let (la, lb) = (&self.nodes[&a].log, &self.nodes[&b].log);
                let Some(last_match) = (0..la.len().min(lb.len())).rev().find(|&k| la[k].term == lb[k].term)
                else {
                    continue;
                };
                if la[..=last_match] != lb[..=last_match] {
                    self.fail(&format!(
                        "log matching: nodes {a} and {b} agree at {} but differ before it",
                        last_match + 1
                    ));
                }
            }
        }
    }
}

fn seeds() -> Vec<u64> {
    if let Some(seed) = std::env::var("EMBERKV_SIM_SEED").ok().and_then(|s| s.parse().ok()) {
        return vec![seed];
    }
    let count = std::env::var("EMBERKV_SIM_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(200);
    (0..count).collect()
}

#[test]
fn raft_survives_partitions_crashes_and_message_chaos() {
    let mut total = Stats::default();
    for seed in seeds() {
        let mut sim = Sim::new(seed);
        sim.run();
        let s = &sim.stats;
        total.elections += s.elections;
        total.committed += s.committed;
        total.reads_served += s.reads_served;
        total.reads_aborted += s.reads_aborted;
        total.messages += s.messages;
        total.dropped += s.dropped;
        total.crashes += s.crashes;
    }
    // Guard against a simulator that silently stopped exercising anything.
    assert!(total.committed > 0 && total.reads_served > 0 && total.elections > 0, "{total:?}");
    println!("{total:?}");
}
