//! Durable Raft state: the hard state and the log.
//!
//! It lives in a dedicated emberkv engine opened with `SyncPolicy::Always`,
//! so it inherits the engine's CRC-checked records, torn-tail recovery and
//! atomic batches. One `persist` call is one or a few batches and therefore
//! one fsync per batch, however many entries it carries: concurrent writers
//! get group commit for free.
//!
//! Keys: `hard_state` → `term u64 | voted_for u64 (0 = none)`, and
//! `log/{index:020}` → `term u64 | tag u8 (0 noop, 1 command) | command`.

use std::path::Path;

use emberkv_engine::{Engine, Options, SyncPolicy, WriteBatch};
use emberkv_raft::{Entry, HardState, Index, Payload};

const HARD_STATE: &[u8] = b"hard_state";
const LOG_PREFIX: &[u8] = b"log/";
/// Keeps each batch well under the engine's `max_batch_bytes`.
const MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug)]
pub(crate) struct LogStore {
    engine: Engine,
    last_index: Index,
}

/// What a node persisted before it stopped.
#[derive(Debug)]
pub(crate) struct Persisted {
    pub(crate) hard_state: HardState,
    pub(crate) entries: Vec<Entry>,
}

fn log_key(index: Index) -> Vec<u8> {
    format!("log/{index:020}").into_bytes()
}

fn encode_entry(entry: &Entry) -> Vec<u8> {
    let mut out = entry.term.to_le_bytes().to_vec();
    match &entry.payload {
        Payload::Noop => out.push(0),
        Payload::Command(command) => {
            out.push(1);
            out.extend_from_slice(command);
        }
    }
    out
}

fn decode_entry(index: Index, value: &[u8]) -> emberkv_engine::Result<Entry> {
    let corrupt =
        |reason: &str| emberkv_engine::Error::Corruption { segment: 0, offset: index, reason: reason.into() };
    if value.len() < 9 {
        return Err(corrupt("Raft log entry is truncated"));
    }
    let term = u64::from_le_bytes(value[..8].try_into().expect("8 bytes"));
    let payload = match value[8] {
        0 => Payload::Noop,
        1 => Payload::Command(value[9..].to_vec()),
        _ => return Err(corrupt("unknown Raft log entry tag")),
    };
    Ok(Entry { term, index, payload })
}

impl LogStore {
    /// Opens (or creates) the store and returns what it holds.
    pub(crate) fn open(dir: &Path) -> emberkv_engine::Result<(Self, Persisted)> {
        let engine = Engine::open(dir, Options::default().sync(SyncPolicy::Always))?;
        let hard_state = match engine.get(HARD_STATE)? {
            None => HardState::default(),
            Some(bytes) if bytes.len() == 16 => {
                let term = u64::from_le_bytes(bytes[..8].try_into().expect("8 bytes"));
                let voted_for = u64::from_le_bytes(bytes[8..].try_into().expect("8 bytes"));
                HardState { term, voted_for: (voted_for != 0).then_some(voted_for) }
            }
            Some(_) => {
                return Err(emberkv_engine::Error::Corruption {
                    segment: 0,
                    offset: 0,
                    reason: "Raft hard state has the wrong length".into(),
                });
            }
        };
        let entries = engine
            .scan_prefix(LOG_PREFIX, usize::MAX)?
            .into_iter()
            .map(|(key, value)| {
                let index = std::str::from_utf8(&key[LOG_PREFIX.len()..])
                    .ok()
                    .and_then(|digits| digits.parse().ok())
                    .unwrap_or(0);
                decode_entry(index, &value)
            })
            .collect::<emberkv_engine::Result<Vec<_>>>()?;
        let last_index = entries.last().map_or(0, |e| e.index);
        Ok((Self { engine, last_index }, Persisted { hard_state, entries }))
    }

    /// Makes `hard_state` and `entries` durable, replacing any stored entries
    /// from `entries[0].index` on (see [`emberkv_raft::Ready`]).
    ///
    /// The hard state is written first: an entry must never be durable
    /// without the term it belongs to. If several batches are needed, the
    /// first also deletes the replaced suffix, so a crash in between leaves a
    /// consistent prefix of the new log.
    pub(crate) fn persist(
        &mut self,
        hard_state: Option<HardState>,
        entries: &[Entry],
    ) -> emberkv_engine::Result<()> {
        if let Some(hs) = hard_state {
            let mut value = hs.term.to_le_bytes().to_vec();
            value.extend_from_slice(&hs.voted_for.unwrap_or(0).to_le_bytes());
            self.engine.put(HARD_STATE, &value)?;
        }
        let Some(first) = entries.first().map(|e| e.index) else { return Ok(()) };

        let mut batch = WriteBatch::new();
        for stale in first..=self.last_index {
            batch.delete(log_key(stale));
        }
        let mut size = 0;
        for entry in entries {
            let value = encode_entry(entry);
            size += value.len() + 32;
            batch.put(log_key(entry.index), value);
            if size >= MAX_BATCH_BYTES {
                self.engine.write_batch(&batch)?;
                batch = WriteBatch::new();
                size = 0;
            }
        }
        self.engine.write_batch(&batch)?;
        self.last_index = entries.last().map_or(first - 1, |e| e.index);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(term: u64, index: u64) -> Entry {
        Entry { term, index, payload: Payload::Command(format!("{term}/{index}").into_bytes()) }
    }

    #[test]
    fn persisted_state_survives_reopen_and_suffixes_are_replaced() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut store, persisted) = LogStore::open(dir.path()).unwrap();
            assert_eq!(persisted.hard_state, HardState::default());
            assert!(persisted.entries.is_empty());

            let hs = HardState { term: 2, voted_for: Some(3) };
            store.persist(Some(hs), &[entry(1, 1), entry(1, 2), entry(2, 3), entry(2, 4)]).unwrap();
            // A new leader overwrites the uncommitted tail from index 3.
            store.persist(None, &[Entry { term: 2, index: 3, payload: Payload::Noop }]).unwrap();
        }
        let (_, persisted) = LogStore::open(dir.path()).unwrap();
        assert_eq!(persisted.hard_state, HardState { term: 2, voted_for: Some(3) });
        assert_eq!(
            persisted.entries,
            [entry(1, 1), entry(1, 2), Entry { term: 2, index: 3, payload: Payload::Noop }]
        );
    }

    #[test]
    fn large_appends_are_split_into_several_batches() {
        let dir = tempfile::tempdir().unwrap();
        let big = |index| Entry { term: 1, index, payload: Payload::Command(vec![7; 3 * 1024 * 1024]) };
        {
            let (mut store, _) = LogStore::open(dir.path()).unwrap();
            store
                .persist(Some(HardState { term: 1, voted_for: None }), &(1..=6).map(big).collect::<Vec<_>>())
                .unwrap();
        }
        let (_, persisted) = LogStore::open(dir.path()).unwrap();
        assert_eq!(persisted.entries.len(), 6);
        assert_eq!(persisted.entries[5], big(6));
    }
}
