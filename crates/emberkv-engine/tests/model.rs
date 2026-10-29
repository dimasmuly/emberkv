//! Model-based property test: drive the engine and a simple in-memory model
//! with the same random operations and require that they never disagree.
//!
//! Operations cover every write path (put, put with TTL, delete, atomic
//! batch, read-modify-write, multi-key transactions, including aborted
//! ones) plus time passing, purging, reopening and
//! compaction. A `ManualClock` makes expiry deterministic. Tiny segments
//! force constant rotation, so segment boundaries, recovery and compaction
//! are exercised on almost every case.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use emberkv_engine::{Clock, Engine, Expiry, ManualClock, Mutation, Options, SyncPolicy, WriteBatch};
use proptest::collection::vec;
use proptest::prelude::*;

#[derive(Debug, Clone)]
enum Op {
    Put(u8, Vec<u8>),
    PutTtl(u8, Vec<u8>, u16),
    Delete(u8),
    Get(u8),
    ScanPrefix(u8),
    Batch(Vec<(u8, Option<Vec<u8>>)>),
    Increment(u8),
    Transaction(Vec<TxStep>, bool),
    Advance(u16),
    Purge,
    Reopen,
    Compact,
}

/// One step inside a transaction; the `bool` on `Op::Transaction` aborts it.
#[derive(Debug, Clone)]
enum TxStep {
    Get(u8),
    Put(u8, Vec<u8>, Option<u16>),
    Delete(u8),
    Increment(u8),
}

fn tx_step() -> impl Strategy<Value = TxStep> {
    let key = || any::<u8>().prop_map(|k| k % 24);
    prop_oneof![
        key().prop_map(TxStep::Get),
        (key(), value(), proptest::option::of(1u16..200)).prop_map(|(k, v, ttl)| TxStep::Put(k, v, ttl)),
        key().prop_map(TxStep::Delete),
        key().prop_map(TxStep::Increment),
    ]
}

fn value() -> impl Strategy<Value = Vec<u8>> {
    vec(any::<u8>(), 0..32)
}

fn op() -> impl Strategy<Value = Op> {
    let key = || any::<u8>().prop_map(|k| k % 24);
    prop_oneof![
        6 => (key(), value()).prop_map(|(k, v)| Op::Put(k, v)),
        3 => (key(), value(), 1u16..200).prop_map(|(k, v, ttl)| Op::PutTtl(k, v, ttl)),
        2 => key().prop_map(Op::Delete),
        3 => key().prop_map(Op::Get),
        1 => (0u8..3).prop_map(Op::ScanPrefix),
        2 => vec((key(), proptest::option::of(value())), 1..6).prop_map(Op::Batch),
        2 => key().prop_map(Op::Increment),
        2 => (vec(tx_step(), 1..8), any::<bool>()).prop_map(|(steps, abort)| Op::Transaction(steps, abort)),
        2 => (0u16..300).prop_map(Op::Advance),
        1 => Just(Op::Purge),
        1 => Just(Op::Reopen),
        1 => Just(Op::Compact),
    ]
}

/// Keys share prefixes (`k0…`, `k1…`, `k2…`) so prefix scans are meaningful.
fn key(k: u8) -> Vec<u8> {
    format!("k{}-{k:02}", k % 3).into_bytes()
}

fn parse_counter(value: &[u8]) -> u64 {
    std::str::from_utf8(value).ok().and_then(|s| s.parse().ok()).unwrap_or(0)
}

/// Reference implementation: a map of `key -> (value, expires_at)`.
#[derive(Default)]
struct Model {
    entries: BTreeMap<Vec<u8>, (Vec<u8>, Option<u64>)>,
}

impl Model {
    fn get(&self, key: &[u8], now: u64) -> Option<&(Vec<u8>, Option<u64>)> {
        self.entries.get(key).filter(|(_, exp)| exp.is_none_or(|at| at > now))
    }

    fn remove(&mut self, key: &[u8], now: u64) -> bool {
        let live = self.get(key, now).is_some();
        self.entries.remove(key);
        live
    }

    fn purge(&mut self, now: u64) {
        self.entries.retain(|_, (_, exp)| exp.is_none_or(|at| at > now));
    }

    fn visible(&self, now: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.entries.keys().filter_map(|k| self.get(k, now).map(|(v, _)| (k.clone(), v.clone()))).collect()
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn engine_matches_model(ops in vec(op(), 1..150)) {
        let dir = tempfile::tempdir().unwrap();
        let clock = Arc::new(ManualClock::new(1_000_000));
        let options = Options::default()
            .max_segment_bytes(256)
            .sync(SyncPolicy::Never)
            .clock(clock.clone());
        let mut engine = Engine::open(dir.path(), options.clone()).unwrap();
        let mut model = Model::default();
        let now = || clock.now_ms();

        for op in ops {
            match op {
                Op::Put(k, v) => {
                    engine.put(&key(k), &v).unwrap();
                    model.entries.insert(key(k), (v, None));
                }
                Op::PutTtl(k, v, ttl) => {
                    engine.put_with_ttl(&key(k), &v, Duration::from_millis(ttl.into())).unwrap();
                    model.entries.insert(key(k), (v, Some(now() + u64::from(ttl))));
                }
                Op::Delete(k) => {
                    let existed = engine.delete(&key(k)).unwrap();
                    prop_assert_eq!(existed, model.remove(&key(k), now()));
                }
                Op::Get(k) => {
                    let expected = model.get(&key(k), now()).map(|(v, _)| v.clone());
                    prop_assert_eq!(engine.get(&key(k)).unwrap(), expected);
                }
                Op::ScanPrefix(p) => {
                    let prefix = format!("k{p}");
                    let expected: Vec<_> = model
                        .visible(now())
                        .into_iter()
                        .filter(|(k, _)| k.starts_with(prefix.as_bytes()))
                        .collect();
                    prop_assert_eq!(engine.scan_prefix(prefix.as_bytes(), usize::MAX).unwrap(), expected);
                }
                Op::Batch(ops) => {
                    let mut batch = WriteBatch::new();
                    for (k, v) in &ops {
                        match v {
                            Some(v) => batch.put(key(*k), v.clone()),
                            None => batch.delete(key(*k)),
                        };
                    }
                    engine.write_batch(&batch).unwrap();
                    for (k, v) in ops {
                        match v {
                            Some(v) => { model.entries.insert(key(k), (v, None)); }
                            None => { model.entries.remove(&key(k)); }
                        }
                    }
                }
                Op::Increment(k) => {
                    let next = engine.update(&key(k), |current| {
                        let next = current.map_or(0, |e| parse_counter(&e.value)) + 1;
                        let value = next.to_string().into_bytes();
                        (Mutation::Put { value, expiry: Expiry::KeepExisting }, next)
                    }).unwrap();
                    let (expected, expiry) = match model.get(&key(k), now()) {
                        Some((v, exp)) => (parse_counter(v) + 1, *exp),
                        None => (1, None),
                    };
                    prop_assert_eq!(next, expected);
                    model.entries.insert(key(k), (expected.to_string().into_bytes(), expiry));
                }
                Op::Transaction(steps, abort) => {
                    // The model applies the steps to a copy, kept only on commit.
                    let mut staged = Model { entries: model.entries.clone() };
                    let t = now();
                    let result = engine.transaction(|tx| {
                        assert_eq!(tx.now_ms(), t);
                        for step in &steps {
                            match step {
                                TxStep::Get(k) => {
                                    let expected = staged.get(&key(*k), t).map(|(v, _)| v.clone());
                                    assert_eq!(tx.get(&key(*k))?.map(|e| e.value), expected);
                                }
                                TxStep::Put(k, v, ttl) => {
                                    let expiry = ttl.map_or(Expiry::Never, |ms| Expiry::After(Duration::from_millis(ms.into())));
                                    tx.put(&key(*k), v, expiry)?;
                                    staged.entries.insert(key(*k), (v.clone(), ttl.map(|ms| t + u64::from(ms))));
                                }
                                TxStep::Delete(k) => {
                                    assert_eq!(tx.delete(&key(*k))?, staged.remove(&key(*k), t));
                                }
                                TxStep::Increment(k) => {
                                    let current = tx.get(&key(*k))?;
                                    let next = current.map_or(0, |e| parse_counter(&e.value)) + 1;
                                    tx.put(&key(*k), next.to_string().as_bytes(), Expiry::KeepExisting)?;
                                    let expiry = staged.get(&key(*k), t).and_then(|(_, exp)| *exp);
                                    staged.entries.insert(key(*k), (next.to_string().into_bytes(), expiry));
                                }
                            }
                        }
                        if abort { Err(emberkv_engine::Error::EmptyKey) } else { Ok(()) }
                    });
                    prop_assert_eq!(result.is_ok(), !abort);
                    if !abort {
                        model = staged;
                    }
                }
                Op::Advance(ms) => clock.advance(Duration::from_millis(ms.into())),
                Op::Purge => {
                    engine.purge_expired();
                    model.purge(now());
                    prop_assert_eq!(engine.len(), model.entries.len());
                }
                Op::Reopen => {
                    // Purge first so both sides agree on which keys are indexed.
                    engine.purge_expired();
                    let before = engine.stats();
                    drop(engine);
                    engine = Engine::open(dir.path(), options.clone()).unwrap();
                    // Accounting rebuilt from disk must match the incremental one.
                    let after = engine.stats();
                    prop_assert_eq!(
                        (after.keys, after.total_bytes, after.live_bytes),
                        (before.keys, before.total_bytes, before.live_bytes)
                    );
                }
                Op::Compact => {
                    engine.purge_expired();
                    let live_before = engine.stats().live_bytes;
                    engine.compact().unwrap();
                    prop_assert_eq!(engine.stats().live_bytes, live_before);
                }
            }
        }

        let expected = model.visible(now());
        prop_assert_eq!(&engine.scan(.., usize::MAX).unwrap(), &expected);
        drop(engine);
        let engine = Engine::open(dir.path(), options).unwrap();
        prop_assert_eq!(&engine.scan(.., usize::MAX).unwrap(), &expected);
    }
}
