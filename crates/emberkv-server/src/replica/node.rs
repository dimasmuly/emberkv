//! The thread that drives one Raft node: it owns the [`Raft`] core, its
//! [`LogStore`] and the data engine's write side.
//!
//! Requests arrive on a channel. Each loop iteration drains every request
//! that is waiting, then processes the resulting `Ready` once: one fsync of
//! the Raft log covers every proposal in it (group commit). The driver then
//! sends messages and applies committed entries. Every applied entry writes
//! its index into the data engine in the same transaction as its effects,
//! so after a crash a replica resumes exactly where its state left off and
//! never applies an entry twice.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::mpsc::{self, RecvTimeoutError, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use emberkv_engine::{Clock, Engine, Expiry, ManualClock, Options};
use emberkv_raft::{
    Config, Entry, Envelope, Error as RaftError, Index, Message, NodeId, Payload, Raft, Status, Term,
};
use tokio::sync::oneshot;
use tracing::{error, info};

use super::log_store::LogStore;
use crate::state_machine::{self, Command, Outcome};

/// Data-engine key holding `applied index u64 | replicated clock u64`.
/// Namespaced keys start with `[a-z0-9]`, so no client key can collide.
pub(crate) const APPLIED_KEY: &[u8] = b"\0emberkv/applied";
/// Requests handled per loop iteration, so one burst cannot starve ticks.
const MAX_REQUESTS_PER_ROUND: usize = 1_024;

/// Sends Raft messages to peers. Must not block.
pub(crate) trait Transport: Send + 'static {
    fn send(&self, envelope: Envelope);
}

#[derive(Debug, Clone)]
pub(crate) struct ReplicaConfig {
    pub(crate) id: NodeId,
    pub(crate) voters: Vec<NodeId>,
    /// Holds `raft/` (the log) and `data/` (the state machine).
    pub(crate) dir: PathBuf,
    pub(crate) tick: Duration,
    pub(crate) election_ticks: u32,
    pub(crate) heartbeat_ticks: u32,
    /// Options for the data engine. Its clock is replaced by the replicated clock.
    pub(crate) data_options: Options,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ReplicaError {
    #[error("not the leader")]
    NotLeader { leader: Option<NodeId> },
    #[error("the write was lost in a leader change and did not take effect; retry it")]
    Dropped,
    #[error("the command was rejected: {0}")]
    Rejected(String),
    #[error("the replica has stopped")]
    Stopped,
}

enum Request {
    Propose { command: Command, reply: oneshot::Sender<Result<Outcome, ReplicaError>> },
    Read { reply: oneshot::Sender<Result<(), ReplicaError>> },
    Message { from: NodeId, message: Message },
    Shutdown,
}

/// A cheap, cloneable handle to a running replica.
#[derive(Debug, Clone)]
pub(crate) struct Replica {
    requests: mpsc::Sender<Request>,
    status: Arc<Mutex<Status>>,
}

impl Replica {
    /// Opens the replica's storage, restores its Raft state and starts the
    /// driver thread. Returns the handle and the data engine, which callers
    /// may read from (but must never write to directly).
    pub(crate) fn start(
        config: &ReplicaConfig,
        transport: impl Transport,
    ) -> anyhow::Result<(Self, Engine, thread::JoinHandle<()>)> {
        std::fs::create_dir_all(&config.dir)?;
        let (log, persisted) = LogStore::open(&config.dir.join("raft"))?;
        let clock = Arc::new(ManualClock::new(0));
        let data = Engine::open(config.dir.join("data"), config.data_options.clone().clock(clock.clone()))?;
        let (applied, applied_clock) = read_applied(&data)?;
        clock.set(applied_clock);

        let mut raft_config = Config::new(config.id, config.voters.iter().copied());
        raft_config.election_ticks = config.election_ticks;
        raft_config.heartbeat_ticks = config.heartbeat_ticks;
        raft_config.seed = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_nanos() as u64);
        let raft = Raft::new(&raft_config, persisted.hard_state, persisted.entries, applied)?;
        info!(id = config.id, applied, term = persisted.hard_state.term, "replica starting");

        let (requests, inbox) = mpsc::channel();
        let status = Arc::new(Mutex::new(raft.status()));
        let driver = Driver {
            raft,
            log,
            data: data.clone(),
            clock,
            transport,
            inbox,
            tick: config.tick,
            status: status.clone(),
            proposals: BTreeMap::new(),
            reads: BTreeMap::new(),
            waiting_reads: Vec::new(),
            next_read: 1,
            applied,
        };
        let handle =
            thread::Builder::new().name(format!("raft-{}", config.id)).spawn(move || driver.run())?;
        Ok((Self { requests, status }, data, handle))
    }

    /// Replicates `command` and returns its outcome once it is applied here.
    pub(crate) async fn propose(&self, command: Command) -> Result<Outcome, ReplicaError> {
        let (reply, response) = oneshot::channel();
        self.requests.send(Request::Propose { command, reply }).map_err(|_| ReplicaError::Stopped)?;
        response.await.unwrap_or(Err(ReplicaError::Stopped))
    }

    /// Resolves once this replica may serve a linearizable read from its
    /// data engine (ReadIndex: leadership confirmed, state caught up).
    pub(crate) async fn read_barrier(&self) -> Result<(), ReplicaError> {
        let (reply, response) = oneshot::channel();
        self.requests.send(Request::Read { reply }).map_err(|_| ReplicaError::Stopped)?;
        response.await.unwrap_or(Err(ReplicaError::Stopped))
    }

    /// Hands a message from a peer to this replica.
    pub(crate) fn deliver(&self, from: NodeId, message: Message) {
        let _ = self.requests.send(Request::Message { from, message });
    }

    pub(crate) fn status(&self) -> Status {
        *self.status.lock().expect("status lock")
    }

    pub(crate) fn shutdown(&self) {
        let _ = self.requests.send(Request::Shutdown);
    }
}

fn read_applied(data: &Engine) -> emberkv_engine::Result<(Index, u64)> {
    Ok(match data.get(APPLIED_KEY)? {
        Some(bytes) if bytes.len() == 16 => (
            u64::from_le_bytes(bytes[..8].try_into().expect("8 bytes")),
            u64::from_le_bytes(bytes[8..].try_into().expect("8 bytes")),
        ),
        _ => (0, 0),
    })
}

/// A log entry's command, stamped with the proposer's wall clock.
fn encode_proposal(at_ms: u64, command: &Command) -> Vec<u8> {
    let mut out = at_ms.to_le_bytes().to_vec();
    out.extend_from_slice(&command.encode());
    out
}

fn decode_proposal(bytes: &[u8]) -> Option<(u64, Command)> {
    let at_ms = u64::from_le_bytes(bytes.get(..8)?.try_into().ok()?);
    Some((at_ms, Command::decode(&bytes[8..]).ok()?))
}

fn wall_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

struct Driver<T> {
    raft: Raft,
    log: LogStore,
    data: Engine,
    /// The replicated clock: the data engine's notion of "now", advanced
    /// only by applied commands so that every replica sees the same time.
    clock: Arc<ManualClock>,
    transport: T,
    inbox: mpsc::Receiver<Request>,
    tick: Duration,
    status: Arc<Mutex<Status>>,
    /// Proposals waiting to be applied, by log index.
    proposals: BTreeMap<Index, (Term, oneshot::Sender<Result<Outcome, ReplicaError>>)>,
    /// Reads waiting for their ReadIndex confirmation, by ctx.
    reads: BTreeMap<u64, oneshot::Sender<Result<(), ReplicaError>>>,
    /// Confirmed reads waiting for the state machine to reach their index.
    waiting_reads: Vec<(Index, oneshot::Sender<Result<(), ReplicaError>>)>,
    next_read: u64,
    applied: Index,
}

impl<T: Transport> Driver<T> {
    fn run(mut self) {
        let mut next_tick = Instant::now() + self.tick;
        loop {
            let wait = next_tick.saturating_duration_since(Instant::now());
            let mut stop = match self.inbox.recv_timeout(wait) {
                Ok(request) => self.handle(request),
                Err(RecvTimeoutError::Timeout) => false,
                Err(RecvTimeoutError::Disconnected) => true,
            };
            for _ in 0..MAX_REQUESTS_PER_ROUND {
                if stop {
                    break;
                }
                match self.inbox.try_recv() {
                    Ok(request) => stop = self.handle(request),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => stop = true,
                }
            }
            if stop {
                break;
            }
            let now = Instant::now();
            if now >= next_tick {
                self.raft.tick();
                next_tick = (next_tick + self.tick).max(now);
            }
            if let Err(err) = self.process_ready() {
                // The Raft log or the state machine can no longer be written.
                // Continuing could acknowledge writes that are not durable.
                error!(error = %err, "replica storage failed; stopping this node");
                break;
            }
        }
        self.fail_all(&ReplicaError::Stopped);
        info!("replica stopped");
    }

    /// Returns whether the driver should stop.
    fn handle(&mut self, request: Request) -> bool {
        match request {
            Request::Propose { command, reply } => {
                match self.raft.propose(encode_proposal(wall_ms(), &command)) {
                    Ok((index, term)) => {
                        self.proposals.insert(index, (term, reply));
                    }
                    Err(RaftError::NotLeader { leader }) => {
                        let _ = reply.send(Err(ReplicaError::NotLeader { leader }));
                    }
                    Err(err) => {
                        let _ = reply.send(Err(ReplicaError::Rejected(err.to_string())));
                    }
                }
            }
            Request::Read { reply } => {
                let ctx = self.next_read;
                self.next_read += 1;
                match self.raft.read_index(ctx) {
                    Ok(()) => {
                        self.reads.insert(ctx, reply);
                    }
                    Err(RaftError::NotLeader { leader }) => {
                        let _ = reply.send(Err(ReplicaError::NotLeader { leader }));
                    }
                    Err(err) => {
                        let _ = reply.send(Err(ReplicaError::Rejected(err.to_string())));
                    }
                }
            }
            Request::Message { from, message } => self.raft.step(from, message),
            Request::Shutdown => return true,
        }
        false
    }

    fn process_ready(&mut self) -> anyhow::Result<()> {
        while self.raft.has_ready() {
            let ready = self.raft.ready();
            // 1. Persist (fsynced) before anything leaves this node.
            self.log.persist(ready.hard_state, &ready.entries)?;
            // 2. Send.
            for envelope in ready.messages {
                self.transport.send(envelope);
            }
            // 3. Apply.
            for entry in ready.committed {
                self.apply(entry)?;
            }
            // 4. Reads.
            for read in ready.reads {
                if let Some(reply) = self.reads.remove(&read.ctx) {
                    self.waiting_reads.push((read.index, reply));
                }
            }
            for ctx in ready.aborted_reads {
                if let Some(reply) = self.reads.remove(&ctx) {
                    let _ = reply.send(Err(ReplicaError::NotLeader { leader: self.raft.status().leader }));
                }
            }
        }
        self.serve_reads();
        *self.status.lock().expect("status lock") = self.raft.status();
        Ok(())
    }

    fn apply(&mut self, entry: Entry) -> anyhow::Result<()> {
        let index = entry.index;
        let outcome = match entry.payload {
            Payload::Noop => None,
            Payload::Command(bytes) => {
                let Some((at_ms, command)) = decode_proposal(&bytes) else {
                    anyhow::bail!("entry {index} holds a command this version cannot decode");
                };
                // Monotonic even if a new leader's wall clock is behind.
                let now = at_ms.max(self.clock.now_ms());
                self.clock.set(now);
                let marker = applied_marker(index, now);
                let result = self.data.transaction(|tx| {
                    let outcome = state_machine::apply(tx, &command)?;
                    tx.put(APPLIED_KEY, &marker, Expiry::Never)?;
                    Ok(outcome)
                });
                let result = match result {
                    Ok(outcome) => Ok(outcome),
                    // Deterministic refusals (oversized keys or values) are
                    // the same on every replica: record the index and move on.
                    Err(err) if is_deterministic(&err) => {
                        self.data.put(APPLIED_KEY, &marker)?;
                        Err(ReplicaError::Rejected(err.to_string()))
                    }
                    Err(err) => return Err(err.into()),
                };
                Some(result)
            }
        };
        self.applied = index;

        if let Some((term, reply)) = self.proposals.remove(&index) {
            let result = if term == entry.term {
                outcome.unwrap_or(Err(ReplicaError::Dropped))
            } else {
                Err(ReplicaError::Dropped)
            };
            let _ = reply.send(result);
        }
        // Anything proposed at a lower index was overwritten by another leader.
        while let Some(entry) = self.proposals.first_entry() {
            if *entry.key() > index {
                break;
            }
            let (_, reply) = entry.remove();
            let _ = reply.send(Err(ReplicaError::Dropped));
        }
        Ok(())
    }

    /// Releases confirmed reads whose index the state machine has reached.
    fn serve_reads(&mut self) {
        let mut i = 0;
        while i < self.waiting_reads.len() {
            if self.waiting_reads[i].0 <= self.applied {
                let (_, reply) = self.waiting_reads.swap_remove(i);
                let _ = reply.send(Ok(()));
            } else {
                i += 1;
            }
        }
    }

    fn fail_all(&mut self, error: &ReplicaError) {
        for (_, (_, reply)) in std::mem::take(&mut self.proposals) {
            let _ = reply.send(Err(error.clone()));
        }
        for (_, reply) in std::mem::take(&mut self.reads) {
            let _ = reply.send(Err(error.clone()));
        }
        for (_, reply) in self.waiting_reads.drain(..) {
            let _ = reply.send(Err(error.clone()));
        }
    }
}

fn applied_marker(index: Index, clock: u64) -> Vec<u8> {
    let mut out = index.to_le_bytes().to_vec();
    out.extend_from_slice(&clock.to_le_bytes());
    out
}

fn is_deterministic(err: &emberkv_engine::Error) -> bool {
    matches!(
        err,
        emberkv_engine::Error::EmptyKey
            | emberkv_engine::Error::KeyTooLarge { .. }
            | emberkv_engine::Error::ValueTooLarge { .. }
            | emberkv_engine::Error::BatchTooLarge { .. }
    )
}
