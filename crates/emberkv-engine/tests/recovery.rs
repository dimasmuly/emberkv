//! Crash-recovery and corruption-handling tests.
//!
//! These simulate what a power cut or a bad disk leaves behind by editing
//! segment files directly between `Engine` lifetimes.

use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use std::sync::Arc;
use std::time::Duration;

use emberkv_engine::{Engine, Error, ManualClock, Options, SyncPolicy, WriteBatch};

fn options() -> Options {
    Options::default().sync(SyncPolicy::Always)
}

fn segments(dir: &Path) -> Vec<PathBuf> {
    let mut paths: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "log"))
        .collect();
    paths.sort();
    paths
}

fn fill(engine: &Engine, n: usize) {
    for i in 0..n {
        engine.put(format!("key-{i:04}").as_bytes(), format!("value-{i}").as_bytes()).unwrap();
    }
}

fn assert_filled(engine: &Engine, n: usize) {
    assert_eq!(engine.len(), n);
    for i in 0..n {
        let value = engine.get(format!("key-{i:04}").as_bytes()).unwrap();
        assert_eq!(value, Some(format!("value-{i}").into_bytes()), "key-{i:04}");
    }
}

fn append_raw(path: &Path, bytes: &[u8]) {
    OpenOptions::new().append(true).open(path).unwrap().write_all(bytes).unwrap();
}

fn flip_byte(path: &Path, offset: u64) {
    let mut file = OpenOptions::new().read(true).write(true).open(path).unwrap();
    let mut byte = [0u8];
    file.seek(SeekFrom::Start(offset)).unwrap();
    std::io::Read::read_exact(&mut file, &mut byte).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&[byte[0] ^ 0x40]).unwrap();
}

#[test]
fn data_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = Engine::open(dir.path(), options()).unwrap();
        fill(&engine, 500);
        assert!(engine.delete(b"key-0000").unwrap());
    }
    let engine = Engine::open(dir.path(), options()).unwrap();
    assert_eq!(engine.len(), 499);
    assert_eq!(engine.get(b"key-0000").unwrap(), None);
    assert_eq!(engine.get(b"key-0499").unwrap(), Some(b"value-499".to_vec()));
}

#[test]
fn torn_header_at_tail_is_truncated() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = Engine::open(dir.path(), options()).unwrap();
        fill(&engine, 100);
    }
    let active = segments(dir.path()).pop().unwrap();
    let clean_len = fs::metadata(&active).unwrap().len();
    append_raw(&active, &[0xDE, 0xAD, 0xBE, 0xEF, 0x01]);

    let engine = Engine::open(dir.path(), options()).unwrap();
    assert_filled(&engine, 100);
    assert_eq!(fs::metadata(&active).unwrap().len(), clean_len);

    // The repaired segment must accept new writes that survive another reopen.
    engine.put(b"after-repair", b"ok").unwrap();
    drop(engine);
    let engine = Engine::open(dir.path(), options()).unwrap();
    assert_eq!(engine.get(b"after-repair").unwrap(), Some(b"ok".to_vec()));
}

#[test]
fn torn_record_body_at_tail_is_truncated() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = Engine::open(dir.path(), options()).unwrap();
        fill(&engine, 10);
    }
    // A plausible header promising a 4-byte key and 100-byte value, followed
    // by only part of the payload: exactly what a crash mid-`write` leaves.
    let mut torn = vec![0, 0, 0, 0, 1];
    torn.extend_from_slice(&4u32.to_le_bytes());
    torn.extend_from_slice(&100u32.to_le_bytes());
    torn.extend_from_slice(b"keyXpartial-value");
    append_raw(&segments(dir.path()).pop().unwrap(), &torn);

    let engine = Engine::open(dir.path(), options()).unwrap();
    assert_filled(&engine, 10);
}

#[test]
fn corruption_in_sealed_segment_is_fatal() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = Engine::open(dir.path(), options().max_segment_bytes(512)).unwrap();
        fill(&engine, 200);
    }
    let sealed = segments(dir.path()).remove(0);
    flip_byte(&sealed, 20);

    let err = Engine::open(dir.path(), options()).unwrap_err();
    assert!(matches!(err, Error::Corruption { segment: 1, .. }), "{err}");
}

#[test]
fn bit_rot_is_detected_on_read() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(dir.path(), options()).unwrap();
    engine.put(b"key", b"a value that will rot").unwrap();

    let active = segments(dir.path()).pop().unwrap();
    flip_byte(&active, fs::metadata(&active).unwrap().len() - 3);

    let err = engine.get(b"key").unwrap_err();
    assert!(matches!(err, Error::Corruption { .. }), "{err}");
}

#[test]
fn interrupted_compaction_output_is_discarded() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = Engine::open(dir.path(), options()).unwrap();
        fill(&engine, 50);
    }
    let stale = dir.path().join("00000000000000000099.log.tmp");
    fs::write(&stale, b"half-written compaction output").unwrap();

    let engine = Engine::open(dir.path(), options()).unwrap();
    assert_filled(&engine, 50);
    assert!(!stale.exists());
}

#[test]
fn directory_is_exclusively_locked() {
    let dir = tempfile::tempdir().unwrap();
    let _first = Engine::open(dir.path(), options()).unwrap();
    let err = Engine::open(dir.path(), options()).unwrap_err();
    assert!(matches!(err, Error::Locked(_)), "{err}");
}

#[test]
fn compaction_reclaims_space_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let opts = options().sync(SyncPolicy::Never).max_segment_bytes(4096);
    {
        let engine = Engine::open(dir.path(), opts.clone()).unwrap();
        for round in 0..50 {
            fill(&engine, 20);
            engine.put(b"hot", format!("round-{round}").as_bytes()).unwrap();
        }
        engine.put(b"doomed", b"x").unwrap();
        engine.delete(b"doomed").unwrap();

        let before = engine.stats();
        assert!(before.fragmentation() > 0.9, "{before:?}");

        let report = engine.compact().unwrap();
        assert!(report.reclaimed_bytes() > 0, "{report:?}");
        let after = engine.stats();
        assert!(after.total_bytes < before.total_bytes / 10, "{after:?}");
        assert_eq!(after.live_bytes, before.live_bytes);
        assert_filled_with_hot(&engine);
    }
    let engine = Engine::open(dir.path(), opts).unwrap();
    assert_filled_with_hot(&engine);
    assert_eq!(engine.get(b"doomed").unwrap(), None);
}

fn assert_filled_with_hot(engine: &Engine) {
    assert_eq!(engine.get(b"hot").unwrap(), Some(b"round-49".to_vec()));
    for i in 0..20 {
        assert!(engine.get(format!("key-{i:04}").as_bytes()).unwrap().is_some());
    }
    assert_eq!(engine.len(), 21);
}

#[test]
fn input_validation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(dir.path(), options().max_key_bytes(8).max_value_bytes(16)).unwrap();
    assert!(matches!(engine.put(b"", b"v"), Err(Error::EmptyKey)));
    assert!(matches!(engine.put(b"123456789", b"v"), Err(Error::KeyTooLarge { len: 9, max: 8 })));
    assert!(matches!(engine.put(b"k", &[0; 17]), Err(Error::ValueTooLarge { len: 17, max: 16 })));
    engine.put(b"k", b"").unwrap();
    assert_eq!(engine.get(b"k").unwrap(), Some(Vec::new()));
    assert!(!engine.delete(b"missing").unwrap());
}

#[test]
fn torn_batch_is_rolled_back_entirely() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = Engine::open(dir.path(), options()).unwrap();
        fill(&engine, 10);
        let mut batch = WriteBatch::new();
        batch.put("batch-a", "1").put("batch-b", "2").delete("key-0000");
        engine.write_batch(&batch).unwrap();
    }
    // Chop off the commit record (and a little more): the crash happened
    // after the batch members reached disk but before the commit did.
    let active = segments(dir.path()).into_iter().rev().find(|p| fs::metadata(p).unwrap().len() > 0).unwrap();
    let len = fs::metadata(&active).unwrap().len();
    OpenOptions::new().write(true).open(&active).unwrap().set_len(len - 5).unwrap();

    let engine = Engine::open(dir.path(), options()).unwrap();
    assert_filled(&engine, 10);
    assert_eq!(engine.get(b"batch-a").unwrap(), None);
    assert_eq!(engine.get(b"batch-b").unwrap(), None);
}

#[test]
fn committed_batch_survives_reopen_and_compaction() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = Engine::open(dir.path(), options()).unwrap();
        fill(&engine, 3);
        let mut batch = WriteBatch::new();
        batch.put("batch-a", "1").put("batch-b", "2").delete("key-0000");
        engine.write_batch(&batch).unwrap();
        engine.compact().unwrap();
    }
    let engine = Engine::open(dir.path(), options()).unwrap();
    assert_eq!(engine.get(b"batch-a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(engine.get(b"batch-b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(engine.get(b"key-0000").unwrap(), None);
    assert_eq!(engine.len(), 4);
}

#[test]
fn oversized_batch_is_rejected_without_side_effects() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(dir.path(), options().max_batch_bytes(100)).unwrap();
    let mut batch = WriteBatch::new();
    batch.put("a", vec![0; 50]).put("b", vec![0; 50]);
    assert!(matches!(engine.write_batch(&batch), Err(Error::BatchTooLarge { .. })));
    assert!(engine.is_empty());
}

#[test]
fn expiry_is_honored_across_reopen_and_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::new(10_000));
    let opts = options().clock(clock.clone());
    {
        let engine = Engine::open(dir.path(), opts.clone()).unwrap();
        engine.put(b"permanent", b"p").unwrap();
        engine.put_with_ttl(b"otp", b"123456", Duration::from_secs(60)).unwrap();
        let entry = engine.get_entry(b"otp").unwrap().unwrap();
        assert_eq!(entry.expires_at_ms, Some(70_000));
    }
    {
        let engine = Engine::open(dir.path(), opts.clone()).unwrap();
        assert_eq!(engine.get(b"otp").unwrap(), Some(b"123456".to_vec()));
        clock.advance(Duration::from_secs(60));
        assert_eq!(engine.get(b"otp").unwrap(), None);
        assert!(!engine.delete(b"otp").unwrap(), "an expired key is not 'deleted'");

        engine.put_with_ttl(b"session", b"s", Duration::from_secs(1)).unwrap();
        clock.advance(Duration::from_secs(1));
        let report = engine.compact().unwrap();
        assert_eq!(report.records_expired, 1);
        assert_eq!(engine.len(), 1);
    }
    let engine = Engine::open(dir.path(), opts).unwrap();
    assert_eq!(engine.scan(.., 10).unwrap(), vec![(b"permanent".to_vec(), b"p".to_vec())]);
}

#[test]
fn checkpoint_is_consistent_and_independent() {
    let root = tempfile::tempdir().unwrap();
    let (live_dir, backup_dir) = (root.path().join("live"), root.path().join("backup"));
    let engine = Engine::open(&live_dir, options()).unwrap();
    fill(&engine, 50);

    let report = engine.checkpoint(&backup_dir).unwrap();
    assert!(report.segments > 0);

    // Changes after the checkpoint, including a compaction that deletes the
    // live directory's links to the shared files, must not leak into it.
    engine.put(b"after-checkpoint", b"x").unwrap();
    engine.delete(b"key-0000").unwrap();
    engine.compact().unwrap();

    {
        let restored = Engine::open(&backup_dir, options()).unwrap();
        assert_filled(&restored, 50);
        assert_eq!(restored.get(b"after-checkpoint").unwrap(), None);
        // Writing to the restored copy must not touch the live database.
        restored.put(b"only-in-backup", b"y").unwrap();
    }
    drop(engine);
    let live = Engine::open(&live_dir, options()).unwrap();
    assert_eq!(live.get(b"only-in-backup").unwrap(), None);
    assert_eq!(live.get(b"after-checkpoint").unwrap(), Some(b"x".to_vec()));
    assert_eq!(live.len(), 50);

    assert!(engine_checkpoint_refuses_existing_dir(&live, &backup_dir));
}

fn engine_checkpoint_refuses_existing_dir(engine: &Engine, existing: &Path) -> bool {
    matches!(engine.checkpoint(existing), Err(Error::Io(err)) if err.kind() == std::io::ErrorKind::AlreadyExists)
}

#[test]
fn restarts_without_writes_do_not_accumulate_segments() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = Engine::open(dir.path(), options()).unwrap();
        fill(&engine, 10);
    }
    for _ in 0..5 {
        drop(Engine::open(dir.path(), options()).unwrap());
    }
    // One segment with data plus the current (empty) active one.
    let engine = Engine::open(dir.path(), options()).unwrap();
    assert_eq!(segments(dir.path()).len(), 2);
    assert_filled(&engine, 10);
}
