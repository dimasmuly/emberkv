//! Stress test: writers, readers and a compactor hammering one engine.

use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use emberkv_engine::{Engine, Options, SyncPolicy};

const WRITERS: usize = 4;
const KEYS_PER_WRITER: usize = 300;
const ROUNDS: usize = 5;

fn key(writer: usize, i: usize) -> Vec<u8> {
    format!("w{writer}-{i:05}").into_bytes()
}

fn value(writer: usize, i: usize, round: usize) -> Vec<u8> {
    format!("w{writer}-{i:05}-r{round}").into_bytes()
}

#[test]
fn concurrent_writes_reads_and_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let options = Options::default().max_segment_bytes(8 * 1024).sync(SyncPolicy::Never);
    let engine = Engine::open(dir.path(), options.clone()).unwrap();
    let done = AtomicBool::new(false);

    thread::scope(|s| {
        let writers: Vec<_> = (0..WRITERS)
            .map(|w| {
                let engine = engine.clone();
                s.spawn(move || {
                    for round in 0..ROUNDS {
                        for i in 0..KEYS_PER_WRITER {
                            engine.put(&key(w, i), &value(w, i, round)).unwrap();
                        }
                    }
                })
            })
            .collect();

        for r in 0..2 {
            let (engine, done) = (engine.clone(), &done);
            s.spawn(move || {
                let mut i = r;
                while !done.load(Ordering::Relaxed) {
                    let (w, k) = (i % WRITERS, i % KEYS_PER_WRITER);
                    // A value may be absent or from any round, but it must
                    // never be torn or belong to a different key.
                    if let Some(v) = engine.get(&key(w, k)).unwrap() {
                        assert!(v.starts_with(&key(w, k)), "foreign value for key: {v:?}");
                    }
                    i += 7;
                }
            });
        }

        let compactor = {
            let (engine, done) = (engine.clone(), &done);
            s.spawn(move || {
                let mut runs = 0;
                while !done.load(Ordering::Relaxed) {
                    engine.compact().unwrap();
                    runs += 1;
                }
                runs
            })
        };

        for writer in writers {
            writer.join().unwrap();
        }
        done.store(true, Ordering::Relaxed);
        assert!(compactor.join().unwrap() > 0);
    });

    let verify = |engine: &Engine| {
        assert_eq!(engine.len(), WRITERS * KEYS_PER_WRITER);
        for w in 0..WRITERS {
            for i in 0..KEYS_PER_WRITER {
                assert_eq!(engine.get(&key(w, i)).unwrap(), Some(value(w, i, ROUNDS - 1)));
            }
        }
    };
    verify(&engine);
    let stats = engine.stats();
    drop(engine);

    let engine = Engine::open(dir.path(), options).unwrap();
    verify(&engine);
    let reopened = engine.stats();
    // Reopening adds a fresh active segment; the data accounting must match.
    assert_eq!(
        (reopened.keys, reopened.total_bytes, reopened.live_bytes),
        (stats.keys, stats.total_bytes, stats.live_bytes)
    );
}
