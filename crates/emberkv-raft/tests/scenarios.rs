//! Hand-written scenarios for behaviour that the simulation checks only
//! statistically. Messages are delivered instantly and losslessly, except
//! between nodes that a test has cut apart.

use std::collections::{BTreeMap, BTreeSet};

use emberkv_raft::{Config, Entry, Error, HardState, NodeId, Payload, Raft, ReadState, Role};

struct Cluster {
    nodes: BTreeMap<NodeId, Raft>,
    /// Pairs that cannot talk, in both directions.
    cut: BTreeSet<(NodeId, NodeId)>,
    applied: BTreeMap<NodeId, Vec<Entry>>,
    reads: Vec<(NodeId, ReadState)>,
    aborted: Vec<u64>,
}

impl Cluster {
    fn new(size: u64) -> Self {
        let ids: Vec<NodeId> = (1..=size).collect();
        let nodes = ids
            .iter()
            .map(|&id| {
                let mut config = Config::new(id, ids.iter().copied());
                config.seed = 7;
                (id, Raft::new(&config, HardState::default(), Vec::new(), 0).expect("valid"))
            })
            .collect();
        Cluster {
            nodes,
            cut: BTreeSet::new(),
            applied: ids.iter().map(|&id| (id, Vec::new())).collect(),
            reads: Vec::new(),
            aborted: Vec::new(),
        }
    }

    fn node(&mut self, id: NodeId) -> &mut Raft {
        self.nodes.get_mut(&id).expect("node")
    }

    fn isolate(&mut self, id: NodeId) {
        for &other in self.nodes.keys() {
            if other != id {
                self.cut.insert((id, other));
                self.cut.insert((other, id));
            }
        }
    }

    fn heal(&mut self) {
        self.cut.clear();
    }

    /// Delivers messages until no node has anything left to do.
    fn settle(&mut self) {
        loop {
            let mut outbox = Vec::new();
            for (&id, raft) in &mut self.nodes {
                if !raft.has_ready() {
                    continue;
                }
                let ready = raft.ready();
                self.applied.get_mut(&id).expect("node").extend(ready.committed);
                self.reads.extend(ready.reads.into_iter().map(|r| (id, r)));
                self.aborted.extend(ready.aborted_reads);
                outbox.extend(ready.messages);
            }
            if outbox.is_empty() {
                return;
            }
            for envelope in outbox {
                if !self.cut.contains(&(envelope.from, envelope.to)) {
                    self.node(envelope.to).step(envelope.from, envelope.message);
                }
            }
        }
    }

    fn tick(&mut self, ticks: u32) {
        for _ in 0..ticks {
            for raft in self.nodes.values_mut() {
                raft.tick();
            }
            self.settle();
        }
    }

    fn leader(&self) -> Option<NodeId> {
        self.nodes.values().map(Raft::status).find(|s| s.role == Role::Leader).map(|s| s.id)
    }

    fn elect(&mut self) -> NodeId {
        for _ in 0..100 {
            self.tick(1);
            if let Some(leader) = self.leader() {
                return leader;
            }
        }
        panic!("no leader elected");
    }
}

fn commands(entries: &[Entry]) -> Vec<&[u8]> {
    entries
        .iter()
        .filter_map(|e| match &e.payload {
            Payload::Command(c) => Some(c.as_slice()),
            Payload::Noop => None,
        })
        .collect()
}

#[test]
fn three_nodes_elect_a_leader_and_apply_the_same_commands() {
    let mut cluster = Cluster::new(3);
    let leader = cluster.elect();
    cluster.node(leader).propose(b"a".to_vec()).expect("leader");
    cluster.node(leader).propose(b"b".to_vec()).expect("leader");
    cluster.tick(3); // a heartbeat carries the new commit index to followers
    for (id, applied) in &cluster.applied {
        assert_eq!(commands(applied), [b"a", b"b"], "node {id}");
    }
}

#[test]
fn followers_refuse_proposals_and_name_the_leader() {
    let mut cluster = Cluster::new(3);
    let leader = cluster.elect();
    cluster.tick(3);
    let follower = (1..=3).find(|&id| id != leader).expect("follower");
    assert_eq!(cluster.node(follower).propose(b"x".to_vec()), Err(Error::NotLeader { leader: Some(leader) }));
}

#[test]
fn pre_vote_stops_a_returning_node_from_forcing_a_new_term() {
    let mut cluster = Cluster::new(3);
    let leader = cluster.elect();
    let term = cluster.node(leader).status().term;
    let loner = (1..=3).find(|&id| id != leader).expect("follower");

    cluster.isolate(loner);
    cluster.tick(200); // many election timeouts: without pre-vote, its term would climb
    assert_eq!(cluster.node(loner).status().term, term, "pre-votes never bump the term");

    cluster.heal();
    cluster.tick(10);
    assert_eq!(cluster.leader(), Some(leader), "the healthy leader keeps its job");
    assert_eq!(cluster.node(leader).status().term, term);
}

#[test]
fn check_quorum_makes_an_isolated_leader_step_down() {
    let mut cluster = Cluster::new(3);
    let old = cluster.elect();
    cluster.isolate(old);
    cluster.tick(40);
    assert_ne!(cluster.node(old).status().role, Role::Leader, "a leader without a quorum must step down");
    let new = cluster.leader().expect("the majority elects a new leader");
    assert_ne!(new, old);
}

#[test]
fn reads_need_a_quorum_and_fail_over_cleanly() {
    let mut cluster = Cluster::new(3);
    let leader = cluster.elect();
    cluster.node(leader).propose(b"a".to_vec()).expect("leader");
    cluster.settle();

    cluster.node(leader).read_index(1).expect("leader");
    cluster.settle();
    let commit = cluster.node(leader).status().commit;
    assert_eq!(cluster.reads, [(leader, ReadState { ctx: 1, index: commit })]);

    // Cut off, the leader cannot confirm it still leads, so the read waits
    // and is aborted when the leader steps down. It is never served stale.
    cluster.isolate(leader);
    cluster.node(leader).read_index(2).expect("still thinks it leads");
    cluster.tick(40);
    assert_eq!(cluster.reads.len(), 1);
    assert_eq!(cluster.aborted, [2]);
}

#[test]
fn a_restarted_node_resumes_from_its_persisted_state() {
    let config = Config::new(1, [1]);
    let mut node = Raft::new(&config, HardState::default(), Vec::new(), 0).expect("valid");
    while node.status().role != Role::Leader {
        node.tick();
    }
    node.propose(b"a".to_vec()).expect("leader");
    let ready = node.ready();
    let hard_state = ready.hard_state.expect("vote persisted");

    // Restart having applied everything: nothing is handed out twice.
    let mut node = Raft::new(&config, hard_state, ready.entries, 2).expect("valid");
    while node.status().role != Role::Leader {
        node.tick();
    }
    node.propose(b"b".to_vec()).expect("leader");
    let ready = node.ready();
    assert!(ready.committed.iter().all(|e| e.index > 2));
    assert_eq!(commands(&ready.committed), [b"b"]);
}

#[test]
fn inconsistent_persisted_state_is_rejected() {
    let config = Config::new(1, [1, 2, 3]);
    let entry = |term, index| Entry { term, index, payload: Payload::Noop };
    let hs = HardState { term: 2, voted_for: None };
    let new = |log, applied| Raft::new(&config, hs, log, applied).map(|_| ());

    assert!(matches!(new(vec![entry(1, 2)], 0), Err(Error::Storage { index: 1, .. })));
    assert!(matches!(new(vec![entry(2, 1), entry(1, 2)], 0), Err(Error::Storage { index: 2, .. })));
    assert!(matches!(new(vec![entry(3, 1)], 0), Err(Error::Storage { index: 1, .. })));
    assert!(matches!(new(vec![entry(1, 1)], 2), Err(Error::Storage { index: 2, .. })));
    assert!(matches!(Raft::new(&Config::new(4, [1, 2, 3]), hs, vec![], 0), Err(Error::Config(_))));
    assert!(matches!(Raft::new(&Config::new(1, [1, 1, 2]), hs, vec![], 0), Err(Error::Config(_))));
}
