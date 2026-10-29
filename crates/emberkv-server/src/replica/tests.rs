//! Replicas on real storage, wired by an in-memory transport.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use emberkv_engine::{Engine, Entry, Options, SyncPolicy};
use emberkv_raft::{Envelope, NodeId, Role};
use futures::future::join_all;
use tempfile::TempDir;

use super::{Replica, ReplicaConfig, ReplicaError, Transport};
use crate::state_machine::{BatchOp, Command, Outcome};

#[derive(Default)]
struct Network {
    replicas: Mutex<BTreeMap<NodeId, Replica>>,
    isolated: Mutex<BTreeSet<NodeId>>,
}

struct MemoryTransport(Arc<Network>);

impl Transport for MemoryTransport {
    fn send(&self, envelope: Envelope) {
        let isolated = self.0.isolated.lock().unwrap();
        if isolated.contains(&envelope.from) || isolated.contains(&envelope.to) {
            return;
        }
        drop(isolated);
        if let Some(replica) = self.0.replicas.lock().unwrap().get(&envelope.to) {
            replica.deliver(envelope.from, envelope.message);
        }
    }
}

struct Cluster {
    voters: Vec<NodeId>,
    network: Arc<Network>,
    dirs: BTreeMap<NodeId, TempDir>,
    running: BTreeMap<NodeId, (Replica, Engine, JoinHandle<()>)>,
}

impl Cluster {
    fn new(size: u64) -> Self {
        let voters: Vec<NodeId> = (1..=size).collect();
        let dirs = voters.iter().map(|&id| (id, tempfile::tempdir().unwrap())).collect();
        let mut cluster =
            Cluster { voters: voters.clone(), network: Arc::default(), dirs, running: BTreeMap::new() };
        for id in voters {
            cluster.start(id);
        }
        cluster
    }

    fn start(&mut self, id: NodeId) {
        let config = ReplicaConfig {
            id,
            voters: self.voters.clone(),
            dir: self.dirs[&id].path().to_path_buf(),
            tick: Duration::from_millis(5),
            election_ticks: 10,
            heartbeat_ticks: 2,
            data_options: Options::default().sync(SyncPolicy::Never),
        };
        let (replica, engine, handle) =
            Replica::start(&config, MemoryTransport(self.network.clone())).unwrap();
        self.network.replicas.lock().unwrap().insert(id, replica.clone());
        self.running.insert(id, (replica, engine, handle));
    }

    /// Stops a node the hard way: its thread ends and only disk survives.
    fn stop(&mut self, id: NodeId) {
        let (replica, engine, handle) = self.running.remove(&id).unwrap();
        self.network.replicas.lock().unwrap().remove(&id);
        replica.shutdown();
        handle.join().unwrap();
        drop(engine);
    }

    fn replica(&self, id: NodeId) -> &Replica {
        &self.running[&id].0
    }

    async fn leader(&self) -> NodeId {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let leaders: Vec<_> = self
                .running
                .iter()
                .filter(|(_, (r, _, _))| r.status().role == Role::Leader)
                .map(|(&id, (r, _, _))| (id, r.status().term))
                .collect();
            // Only trust a leader whose term no running node has moved past.
            let max_term = self.running.values().map(|(r, _, _)| r.status().term).max().unwrap_or(0);
            if let Some(&(id, _)) = leaders.iter().find(|(_, term)| *term == max_term) {
                return id;
            }
            assert!(Instant::now() < deadline, "no leader elected");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Waits until every running replica has applied the same index, then
    /// returns each one's full state.
    async fn converged_state(&self) -> Vec<Vec<(Vec<u8>, Entry)>> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let applied: BTreeSet<_> = self.running.values().map(|(r, _, _)| r.status().applied).collect();
            let commit = self.running.values().map(|(r, _, _)| r.status().commit).max().unwrap_or(0);
            if applied.len() == 1 && applied.first() == Some(&commit) {
                return self
                    .running
                    .values()
                    .map(|(_, engine, _)| engine.scan_entries(.., usize::MAX).unwrap())
                    .collect();
            }
            assert!(
                Instant::now() < deadline,
                "replicas did not converge: applied {applied:?}, commit {commit}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

fn increment(key: &str) -> Command {
    Command::Increment { key: format!("ns/{key}").into_bytes(), by: 1, ttl_ms: None }
}

fn counter(engine: &Engine, key: &str) -> Option<String> {
    engine.get(format!("ns/{key}").as_bytes()).unwrap().map(|v| String::from_utf8(v).unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn replicas_apply_identical_state() {
    let cluster = Cluster::new(3);
    let leader = cluster.replica(cluster.leader().await).clone();

    // Concurrent proposals of every kind, so many share one fsync.
    let commands = (0..300).map(|i| match i % 5 {
        0 => increment("hits"),
        1 => Command::Put {
            key: format!("ns/k{i}").into_bytes(),
            value: b"{}".to_vec(),
            ttl_ms: Some(3_600_000),
            condition: None,
        },
        2 => Command::RateLimit { key: b"ns/login".to_vec(), limit: 25, window_ms: 60_000, cost: 1 },
        3 => Command::Batch {
            ops: vec![
                BatchOp { key: format!("ns/b{i}").into_bytes(), value: Some(b"1".to_vec()), ttl_ms: None },
                BatchOp { key: format!("ns/k{}", i - 2).into_bytes(), value: None, ttl_ms: None },
            ],
        },
        _ => Command::Delete { key: format!("ns/b{}", i - 1).into_bytes(), condition: None },
    });
    let outcomes = join_all(commands.map(|c| leader.propose(c))).await;
    let allowed = outcomes.iter().filter(|o| matches!(o, Ok(Outcome::RateLimit(d)) if d.allowed)).count();
    assert!(outcomes.iter().all(Result::is_ok), "{outcomes:?}");
    assert_eq!(allowed, 25, "exactly `limit` requests pass, however they interleave");

    let states = cluster.converged_state().await;
    assert!(states.windows(2).all(|w| w[0] == w[1]), "replicas diverged");
    assert_eq!(counter(&cluster.running[&1].1, "hits").as_deref(), Some("60"));
}

#[tokio::test(flavor = "multi_thread")]
async fn acknowledged_writes_survive_the_leader_crashing() {
    let mut cluster = Cluster::new(3);
    let old = cluster.leader().await;
    for _ in 0..50 {
        cluster.replica(old).propose(increment("n")).await.unwrap();
    }

    cluster.stop(old);
    let new = cluster.leader().await;
    assert_ne!(new, old);
    for _ in 0..50 {
        cluster.replica(new).propose(increment("n")).await.unwrap();
    }

    // The old leader comes back, catches up, and agrees.
    cluster.start(old);
    let states = cluster.converged_state().await;
    assert!(states.windows(2).all(|w| w[0] == w[1]), "replicas diverged");
    for (_, engine, _) in cluster.running.values() {
        assert_eq!(counter(engine, "n").as_deref(), Some("100"));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_replica_never_applies_an_entry_twice() {
    let mut cluster = Cluster::new(1);
    for _ in 0..20 {
        cluster.replica(cluster.leader().await).propose(increment("n")).await.unwrap();
    }
    cluster.stop(1);
    cluster.start(1);
    let leader = cluster.leader().await;
    let outcome = cluster.replica(leader).propose(increment("n")).await.unwrap();
    assert_eq!(outcome, Outcome::Counter { value: 21, expires_at: None });
}

#[tokio::test(flavor = "multi_thread")]
async fn only_the_leader_accepts_writes_and_serves_linearizable_reads() {
    let cluster = Cluster::new(3);
    let leader = cluster.leader().await;
    cluster.replica(leader).propose(increment("n")).await.unwrap();
    cluster.converged_state().await;
    let follower = *cluster.voters.iter().find(|&&id| id != leader).unwrap();

    let refused = cluster.replica(follower).propose(increment("n")).await;
    assert_eq!(refused, Err(ReplicaError::NotLeader { leader: Some(leader) }));
    assert_eq!(
        cluster.replica(follower).read_barrier().await,
        Err(ReplicaError::NotLeader { leader: Some(leader) })
    );
    cluster.replica(leader).read_barrier().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_minority_cannot_commit() {
    let cluster = Cluster::new(3);
    let leader = cluster.leader().await;
    cluster.network.isolated.lock().unwrap().insert(leader);
    // The isolated leader accepts the proposal but can never commit it;
    // after it steps down (check-quorum), the proposal fails instead of hanging.
    let result =
        tokio::time::timeout(Duration::from_secs(1), cluster.replica(leader).propose(increment("lost")))
            .await;
    assert!(!matches!(result, Ok(Ok(_))), "a write without a quorum must not be acknowledged: {result:?}");
}
