//! Integration tests for bitemporal retention / compaction.
//!
//! We use `TripleStore::insert_at_ts` to plant triples with explicit
//! transaction timestamps in the past so the retention policy sees them
//! as expired without having to actually wait.

use polargraph_core::{
    id::NodeId,
    schema::RetentionPolicy,
    temporal::{BiTemporalRange, Timestamp},
    triple::{Predicate, Triple},
    value::Value,
};
use polargraph_storage::{CompactionManager, TripleStore};
use tempfile::TempDir;

// ── helpers ───────────────────────────────────────────────────────────────────

fn open() -> (TripleStore, TempDir) {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    (store, dir)
}

fn node() -> NodeId {
    NodeId::new()
}

fn prop(subject: NodeId, pred: &str, text: &str, vt_end: i64) -> Triple {
    Triple::Property {
        subject,
        predicate: Predicate::new(pred),
        value: Value::Text(text.into()),
        temporal: BiTemporalRange {
            vt_start: Timestamp(0),
            vt_end: Timestamp(vt_end),
            tt: Timestamp(0),
        },
    }
}

fn now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[test]
fn old_triples_deleted_by_tx_age() {
    let (store, _dir) = open();
    let s = node();

    // Plant a triple with an ancient tt (1 microsecond since epoch).
    let old_triple = prop(s, "label", "old", i64::MAX);
    store.insert_at_ts(&old_triple, Timestamp(1)).unwrap();

    // Verify it exists.
    let triples = store.scan_by_subject(&s).unwrap();
    assert_eq!(triples.len(), 1, "triple should exist before retention");

    // Run retention: tx_age = 1 second → anything written more than 1 s ago is deleted.
    let policy = RetentionPolicy { tx_age_secs: 1, vt_lookback_secs: None };
    let mgr = CompactionManager::new(store.clone());
    let stats = mgr.run_retention(&policy).unwrap();

    // 6 CFs × 1 triple each = 6 entries scanned and deleted.
    assert_eq!(stats.triples_deleted, 6, "all 6 CF copies should be deleted");
    assert!(stats.triples_scanned >= 6);

    // Triple is gone.
    let after = store.scan_by_subject(&s).unwrap();
    assert!(after.is_empty(), "triple should be gone after retention");
}

#[test]
fn recent_triples_survive_tx_age_retention() {
    let (store, _dir) = open();
    let s = node();

    // Insert at current time (normal path).
    store.insert(&prop(s, "label", "current", i64::MAX)).unwrap();

    // Run retention: tx_age = 1 second. The triple was just inserted → survives.
    let policy = RetentionPolicy { tx_age_secs: 1, vt_lookback_secs: None };
    let mgr = CompactionManager::new(store.clone());
    let stats = mgr.run_retention(&policy).unwrap();

    assert_eq!(stats.triples_deleted, 0);

    let after = store.scan_by_subject(&s).unwrap();
    assert_eq!(after.len(), 1, "recent triple should survive");
}

#[test]
fn expired_vt_end_deleted_by_lookback() {
    let (store, _dir) = open();
    let s = node();

    let now = now_us();
    // Triple whose valid-time window ended 2 hours ago.
    let two_hours_ago = now - 2 * 3600 * 1_000_000;
    // Use a recent tt so the tx_age check won't fire.
    let old_vt_triple = prop(s, "label", "expired_vt", two_hours_ago);
    store.insert_at_ts(&old_vt_triple, Timestamp(now)).unwrap();

    // Run retention: vt_lookback = 1 hour. vt_end is 2 hours ago → deleted.
    let policy = RetentionPolicy {
        tx_age_secs: 1_000_000_000, // 31+ years — effectively disable tx check
        vt_lookback_secs: Some(3600),
    };
    let mgr = CompactionManager::new(store.clone());
    let stats = mgr.run_retention(&policy).unwrap();

    assert_eq!(stats.triples_deleted, 6);

    let after = store.scan_by_subject(&s).unwrap();
    assert!(after.is_empty());
}

#[test]
fn open_ended_vt_survives_lookback() {
    let (store, _dir) = open();
    let s = node();

    let now = now_us();
    let triple = prop(s, "label", "open_ended", i64::MAX);
    store.insert_at_ts(&triple, Timestamp(now)).unwrap();

    // vt_end = MAX → should never be deleted by lookback.
    let policy = RetentionPolicy {
        tx_age_secs: 1_000_000_000, // 31+ years — effectively disable tx check
        vt_lookback_secs: Some(1),
    };
    let mgr = CompactionManager::new(store.clone());
    let stats = mgr.run_retention(&policy).unwrap();

    assert_eq!(stats.triples_deleted, 0);

    let after = store.scan_by_subject(&s).unwrap();
    assert_eq!(after.len(), 1);
}

#[test]
fn retention_ignores_meta_cf() {
    // META has schema + oracle data; retention must never touch it.
    // We verify this indirectly: intern a predicate, run retention with
    // extreme settings, then verify the store still works.
    let (store, _dir) = open();
    let s = node();
    let t = prop(s, "my_pred", "val", i64::MAX);
    store.insert_at_ts(&t, Timestamp(1)).unwrap();

    let policy = RetentionPolicy { tx_age_secs: 0, vt_lookback_secs: None };
    let mgr = CompactionManager::new(store.clone());
    mgr.run_retention(&policy).unwrap();

    // If META was corrupted, opening a new scan would fail or return garbage.
    // A successful scan_by_subject (even empty) means the store is intact.
    let _ = store.scan_by_subject(&s).unwrap();
}

#[test]
fn mixed_old_and_new_triples() {
    let (store, _dir) = open();
    let old_subject = node();
    let new_subject = node();

    let now = now_us();
    let ancient_tt = Timestamp(1);
    let current_tt = Timestamp(now);

    store.insert_at_ts(&prop(old_subject, "label", "old", i64::MAX), ancient_tt).unwrap();
    store.insert_at_ts(&prop(new_subject, "label", "new", i64::MAX), current_tt).unwrap();

    let policy = RetentionPolicy { tx_age_secs: 1, vt_lookback_secs: None };
    let mgr = CompactionManager::new(store.clone());
    let stats = mgr.run_retention(&policy).unwrap();

    // Only the old triple's 6 CF copies should be deleted.
    assert_eq!(stats.triples_deleted, 6);

    assert!(store.scan_by_subject(&old_subject).unwrap().is_empty());
    assert_eq!(store.scan_by_subject(&new_subject).unwrap().len(), 1);
}
