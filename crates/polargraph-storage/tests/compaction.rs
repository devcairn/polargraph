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

fn label_of(store: &TripleStore, s: &NodeId) -> Vec<String> {
    store
        .scan_by_subject(s)
        .unwrap()
        .into_iter()
        .filter_map(|t| match t {
            Triple::Property {
                value: Value::Text(v),
                ..
            } => Some(v),
            _ => None,
        })
        .collect()
}

/// Text values of `s` that are valid right now (applies the valid-time
/// filter, so DELETE tombstones hide the fact).
fn live_labels(store: &TripleStore, s: &NodeId) -> Vec<String> {
    store
        .snapshot(Timestamp(i64::MAX - 1))
        .with_vt_as_of(now_us())
        .scan_by_subject(s)
        .unwrap()
        .into_iter()
        .filter_map(|t| match t {
            Triple::Property {
                value: Value::Text(v),
                ..
            } => Some(v),
            _ => None,
        })
        .collect()
}

#[test]
fn old_current_fact_survives_tx_age() {
    // Retention prunes history, not live state: a fact written long ago and
    // never changed is still the current value.
    let (store, _dir) = open();
    let s = node();
    store
        .insert_at_ts(&prop(s, "label", "old", i64::MAX), Timestamp(1))
        .unwrap();

    let policy = RetentionPolicy {
        tx_age_secs: 1,
        vt_lookback_secs: None,
    };
    let stats = CompactionManager::new(store.clone())
        .run_retention(&policy)
        .unwrap();

    assert_eq!(stats.triples_deleted, 0);
    assert!(stats.triples_scanned >= 6);
    assert_eq!(label_of(&store, &s), vec!["old".to_string()]);
}

#[test]
fn superseded_versions_deleted_by_tx_age() {
    let (store, _dir) = open();
    let s = node();
    store
        .insert_at_ts(&prop(s, "label", "v1", i64::MAX), Timestamp(1))
        .unwrap();
    store
        .insert_at_ts(&prop(s, "label", "v2", i64::MAX), Timestamp(2))
        .unwrap();

    let policy = RetentionPolicy {
        tx_age_secs: 1,
        vt_lookback_secs: None,
    };
    let stats = CompactionManager::new(store.clone())
        .run_retention(&policy)
        .unwrap();

    // Only v1's 6 CF copies go; v2 is the current value.
    assert_eq!(stats.triples_deleted, 6);
    assert_eq!(label_of(&store, &s), vec!["v2".to_string()]);
    assert_eq!(
        store.scan_property_history(s, "label", 10).unwrap().len(),
        1,
        "only the current version remains"
    );
}

#[test]
fn deleted_fact_is_not_resurrected_by_vt_lookback() {
    let (store, _dir) = open();
    let s = node();
    let now = now_us();
    let two_hours_ago = now - 2 * 3600 * 1_000_000;

    // Original open-ended fact, then a DELETE tombstone (same vt_start,
    // closed vt_end) committed recently — inside the tx-age window.
    store
        .insert_at_ts(&prop(s, "label", "gone", i64::MAX), Timestamp(1))
        .unwrap();
    store
        .insert_at_ts(&prop(s, "label", "gone", two_hours_ago), Timestamp(now))
        .unwrap();
    assert!(
        live_labels(&store, &s).is_empty(),
        "deleted before retention"
    );

    let policy = RetentionPolicy {
        tx_age_secs: 1_000_000_000, // tombstone is inside the window
        vt_lookback_secs: Some(3600),
    };
    CompactionManager::new(store.clone())
        .run_retention(&policy)
        .unwrap();

    assert!(
        live_labels(&store, &s).is_empty(),
        "retention must not resurrect a deleted fact"
    );
}

#[test]
fn recent_triples_survive_tx_age_retention() {
    let (store, _dir) = open();
    let s = node();

    // Insert at current time (normal path).
    store
        .insert(&prop(s, "label", "current", i64::MAX))
        .unwrap();

    // Run retention: tx_age = 1 second. The triple was just inserted → survives.
    let policy = RetentionPolicy {
        tx_age_secs: 1,
        vt_lookback_secs: None,
    };
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

    let policy = RetentionPolicy {
        tx_age_secs: 0,
        vt_lookback_secs: None,
    };
    let mgr = CompactionManager::new(store.clone());
    mgr.run_retention(&policy).unwrap();

    // If META was corrupted, opening a new scan would fail or return garbage.
    // A successful scan_by_subject (even empty) means the store is intact.
    let _ = store.scan_by_subject(&s).unwrap();
}

#[test]
fn mixed_old_and_new_corrections() {
    let (store, _dir) = open();
    let old_subject = node();
    let new_subject = node();
    let now = now_us();

    // Old subject: corrected long ago → the first version is prunable.
    store
        .insert_at_ts(&prop(old_subject, "label", "a", i64::MAX), Timestamp(1))
        .unwrap();
    store
        .insert_at_ts(&prop(old_subject, "label", "b", i64::MAX), Timestamp(2))
        .unwrap();
    // New subject: corrected just now → both versions are inside the window.
    store
        .insert_at_ts(&prop(new_subject, "label", "a", i64::MAX), Timestamp(3))
        .unwrap();
    store
        .insert_at_ts(&prop(new_subject, "label", "b", i64::MAX), Timestamp(now))
        .unwrap();

    let policy = RetentionPolicy {
        tx_age_secs: 1,
        vt_lookback_secs: None,
    };
    let stats = CompactionManager::new(store.clone())
        .run_retention(&policy)
        .unwrap();

    assert_eq!(stats.triples_deleted, 6);
    assert_eq!(label_of(&store, &old_subject), vec!["b".to_string()]);
    assert_eq!(
        store
            .scan_property_history(new_subject, "label", 10)
            .unwrap()
            .len(),
        2
    );
}
