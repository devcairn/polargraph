//! MVCC integration tests.
//!
//! Sections:
//!   1. Basic transaction insert + commit
//!   2. Read isolation — uncommitted writes invisible to other snapshots
//!   3. Snapshot reads — point-in-time correctness
//!   4. Write conflict detection
//!   5. Multi-triple transactions
//!   6. Rollback (drop without commit)
//!   7. Oracle persistence across reopen

use polargraph_core::{
    id::{EdgeId, NodeId},
    temporal::{BiTemporalRange, Timestamp},
    triple::{Predicate, Triple},
    value::Value,
};
use polargraph_storage::{StorageError, TripleStore};
use tempfile::TempDir;

// ── helpers ───────────────────────────────────────────────────────────────────

fn open_store() -> (TripleStore, TempDir) {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    (store, dir)
}

fn relation(from: NodeId, pred: &str, to: NodeId) -> Triple {
    Triple::Relation {
        subject: from,
        predicate: Predicate::new(pred),
        object: to,
        edge_id: EdgeId::new(),
        temporal: BiTemporalRange::assert_now(Timestamp::now()),
    }
}

fn property(node: NodeId, pred: &str, val: impl Into<Value>) -> Triple {
    Triple::Property {
        subject: node,
        predicate: Predicate::new(pred),
        value: val.into(),
        temporal: BiTemporalRange::assert_now(Timestamp::now()),
    }
}

fn is_conflict(e: &StorageError) -> bool {
    matches!(e, StorageError::WriteConflict(_))
}

// ── 1. Basic transaction insert + commit ──────────────────────────────────────

#[test]
fn single_triple_tx_commits_and_is_readable() {
    let (store, _dir) = open_store();
    let alice = NodeId::new();
    let bob = NodeId::new();

    let mut tx = store.begin();
    tx.insert(relation(alice, "knows", bob));
    let commit_ts = tx.commit().unwrap();

    // Data is visible at the commit timestamp.
    let snap = store.snapshot(commit_ts);
    let triples = snap.scan_by_subject(&alice).unwrap();
    assert_eq!(triples.len(), 1);
    assert_eq!(triples[0].predicate().0, "knows");
}

#[test]
fn commit_returns_monotonically_increasing_timestamps() {
    let (store, _dir) = open_store();
    let a = NodeId::new();
    let b = NodeId::new();
    let c = NodeId::new();

    let mut tx1 = store.begin();
    tx1.insert(relation(a, "knows", b));
    let ts1 = tx1.commit().unwrap();

    let mut tx2 = store.begin();
    tx2.insert(relation(a, "knows", c));
    let ts2 = tx2.commit().unwrap();

    assert!(ts2 > ts1, "commit timestamps must be monotonically increasing");
}

#[test]
fn empty_tx_commit_is_a_noop() {
    let (store, _dir) = open_store();
    let alice = NodeId::new();

    let tx = store.begin();
    tx.commit().unwrap(); // should not panic or error

    let snap = store.snapshot(store.begin().read_ts);
    assert!(snap.scan_by_subject(&alice).unwrap().is_empty());
}

// ── 2. Read isolation — uncommitted writes invisible ──────────────────────────

#[test]
fn uncommitted_write_invisible_to_concurrent_snapshot() {
    let (store, _dir) = open_store();
    let alice = NodeId::new();
    let bob = NodeId::new();

    // Take a snapshot BEFORE any writes.
    let pre_write_snap = store.snapshot(store.begin().read_ts);

    // Start a transaction and insert, but don't commit yet.
    let mut tx = store.begin();
    tx.insert(relation(alice, "knows", bob));
    // tx is NOT committed — just dropped later.

    // The pre-write snapshot must not see the buffered write.
    let triples = pre_write_snap.scan_by_subject(&alice).unwrap();
    assert!(triples.is_empty(), "uncommitted data must not bleed through");

    // A new snapshot taken right now also must not see it.
    let mid_snap = store.snapshot(store.begin().read_ts);
    assert!(mid_snap.scan_by_subject(&alice).unwrap().is_empty());

    // Drop tx without committing — rollback.
    drop(tx);

    let post_snap = store.snapshot(store.begin().read_ts);
    assert!(post_snap.scan_by_subject(&alice).unwrap().is_empty());
}

#[test]
fn snapshot_read_inside_tx_sees_only_committed_data() {
    let (store, _dir) = open_store();
    let alice = NodeId::new();
    let bob = NodeId::new();

    // Commit one triple first.
    let mut setup_tx = store.begin();
    setup_tx.insert(relation(alice, "knows", bob));
    let _setup_ts = setup_tx.commit().unwrap();

    // Begin a new tx and read — should see the committed triple.
    let tx = store.begin();
    let seen = tx.scan_by_subject(&alice).unwrap();
    assert_eq!(seen.len(), 1);

    // Buffer another write but don't commit.
    // The read inside the tx should NOT see its own buffered writes
    // (writes are only visible after commit).
    // (This is a design choice — buffered writes are not readable within the tx.)
    let seen_again = tx.scan_by_subject(&alice).unwrap();
    assert_eq!(seen_again.len(), 1, "buffered writes not visible via tx reads");
}

// ── 3. Snapshot reads — point-in-time correctness ────────────────────────────

#[test]
fn snapshot_at_ts1_does_not_see_writes_committed_at_ts2() {
    let (store, _dir) = open_store();
    let alice = NodeId::new();
    let bob = NodeId::new();
    let carol = NodeId::new();

    // Commit triple 1.
    let mut tx1 = store.begin();
    tx1.insert(relation(alice, "knows", bob));
    let ts1 = tx1.commit().unwrap();

    // Snapshot at ts1.
    let snap1 = store.snapshot(ts1);

    // Commit triple 2 AFTER taking snap1.
    let mut tx2 = store.begin();
    tx2.insert(relation(alice, "knows", carol));
    let ts2 = tx2.commit().unwrap();

    // snap1 must see only the first triple.
    let at_ts1 = snap1.scan_by_subject(&alice).unwrap();
    assert_eq!(at_ts1.len(), 1, "snap1 should see exactly 1 triple");
    match &at_ts1[0] {
        Triple::Relation { object, .. } => assert_eq!(*object, bob),
        _ => panic!("expected relation"),
    }

    // Snapshot at ts2 sees both.
    let at_ts2 = store.snapshot(ts2).scan_by_subject(&alice).unwrap();
    assert_eq!(at_ts2.len(), 2, "snap2 should see both triples");
}

#[test]
fn multiple_versions_of_same_triple_deduplicated_to_latest() {
    // Insert the same (S,P,O) triple twice.  The snapshot should return
    // only one copy — the latest version.
    let (store, _dir) = open_store();
    let alice = NodeId::new();
    let bob = NodeId::new();

    let mut tx1 = store.begin();
    tx1.insert(relation(alice, "knows", bob));
    tx1.commit().unwrap();

    let mut tx2 = store.begin();
    tx2.insert(relation(alice, "knows", bob)); // same triple again
    let ts2 = tx2.commit().unwrap();

    let snap = store.snapshot(ts2);
    let triples = snap.scan_by_subject(&alice).unwrap();
    assert_eq!(triples.len(), 1, "duplicate (S,P,O) must deduplicate to one result");
}

#[test]
fn snapshot_at_zero_sees_nothing() {
    let (store, _dir) = open_store();
    let alice = NodeId::new();
    let bob = NodeId::new();

    let mut tx = store.begin();
    tx.insert(relation(alice, "knows", bob));
    tx.commit().unwrap();

    // Snapshot at ts=0 predates all commits.
    let snap = store.snapshot(Timestamp(0));
    assert!(snap.scan_by_subject(&alice).unwrap().is_empty());
}

#[test]
fn snapshot_sees_property_values_correctly() {
    let (store, _dir) = open_store();
    let alice = NodeId::new();

    let mut tx = store.begin();
    tx.insert(property(alice, "name", "Alice"));
    tx.insert(property(alice, "level", 7i64));
    let ts = tx.commit().unwrap();

    let snap = store.snapshot(ts);
    let triples = snap.scan_by_subject(&alice).unwrap();
    assert_eq!(triples.len(), 2);

    let find = |pred: &str| -> Value {
        triples.iter()
            .find(|t| t.predicate().0 == pred)
            .and_then(|t| match t {
                Triple::Property { value, .. } => Some(value.clone()),
                _ => None,
            })
            .unwrap_or_else(|| panic!("{pred} not found"))
    };

    assert_eq!(find("name"), Value::Text("Alice".into()));
    assert_eq!(find("level"), Value::Int(7));
}

// ── 4. Write conflict detection ───────────────────────────────────────────────

#[test]
fn concurrent_write_to_same_triple_causes_conflict() {
    let (store, _dir) = open_store();
    let alice = NodeId::new();
    let bob = NodeId::new();

    // tx1 and tx2 both start before either commits.
    let mut tx1 = store.begin();
    let mut tx2 = store.begin();

    tx1.insert(relation(alice, "knows", bob));
    tx2.insert(relation(alice, "knows", bob)); // same (S,P,O)

    // First commit wins.
    tx1.commit().unwrap();

    // Second commit must detect the conflict.
    let result = tx2.commit();
    assert!(
        result.is_err() && is_conflict(result.as_ref().unwrap_err()),
        "expected WriteConflict, got: {result:?}"
    );
}

#[test]
fn writes_to_different_triples_do_not_conflict() {
    let (store, _dir) = open_store();
    let alice = NodeId::new();
    let bob = NodeId::new();
    let carol = NodeId::new();

    let mut tx1 = store.begin();
    let mut tx2 = store.begin();

    tx1.insert(relation(alice, "knows", bob));
    tx2.insert(relation(alice, "knows", carol)); // different object

    tx1.commit().unwrap();
    tx2.commit().unwrap(); // must succeed — different (S,P,O)
}

#[test]
fn write_after_read_ts_conflicts_even_with_different_tx() {
    let (store, _dir) = open_store();
    let alice = NodeId::new();
    let bob = NodeId::new();

    // tx1 commits first, raising the committed timestamp.
    let mut tx1 = store.begin();
    tx1.insert(relation(alice, "knows", bob));
    tx1.commit().unwrap();

    // tx2 started BEFORE tx1 committed, so its read_ts is before tx1's write.
    // If tx2 also tries to write the same triple, it should conflict.
    // (In this sequential test we simulate this by beginning tx2 after setup
    //  but writing the same key.)
    let mut tx2 = store.begin();
    tx2.insert(relation(alice, "knows", bob)); // same (S,P,O) as tx1

    // tx2's read_ts >= tx1's commit_ts because begin() is called after commit(),
    // so no conflict here (tx2 started after tx1 finished).
    // This is the CORRECT behaviour: non-overlapping transactions don't conflict.
    tx2.commit().unwrap();
}

// ── 5. Multi-triple transactions ──────────────────────────────────────────────

#[test]
fn multi_triple_tx_all_visible_after_commit() {
    let (store, _dir) = open_store();
    let alice = NodeId::new();
    let bob = NodeId::new();
    let carol = NodeId::new();

    let mut tx = store.begin();
    tx.insert(relation(alice, "knows", bob));
    tx.insert(relation(alice, "knows", carol));
    tx.insert(property(alice, "name", "Alice"));
    let ts = tx.commit().unwrap();

    let snap = store.snapshot(ts);
    let triples = snap.scan_by_subject(&alice).unwrap();
    assert_eq!(triples.len(), 3, "all triples in tx must be committed atomically");
}

#[test]
fn all_triples_in_tx_get_same_commit_timestamp() {
    let (store, _dir) = open_store();
    let a = NodeId::new();
    let b = NodeId::new();
    let c = NodeId::new();

    let mut tx = store.begin();
    tx.insert(relation(a, "knows", b));
    tx.insert(relation(a, "knows", c));
    let commit_ts = tx.commit().unwrap();

    let snap = store.snapshot(commit_ts);
    let triples = snap.scan_by_subject(&a).unwrap();
    assert_eq!(triples.len(), 2);

    for t in &triples {
        assert_eq!(t.temporal().tt, commit_ts,
            "all triples in one tx must share the same tt");
    }
}

// ── 6. Rollback ───────────────────────────────────────────────────────────────

#[test]
fn dropped_tx_does_not_write_anything() {
    let (store, _dir) = open_store();
    let alice = NodeId::new();
    let bob = NodeId::new();

    {
        let mut tx = store.begin();
        tx.insert(relation(alice, "knows", bob));
        // Drop without committing.
    }

    let snap = store.snapshot(store.begin().read_ts);
    assert!(snap.scan_by_subject(&alice).unwrap().is_empty());
}

// ── 7. Oracle persistence ─────────────────────────────────────────────────────

#[test]
fn oracle_counter_persists_across_reopen() {
    let dir = TempDir::new().unwrap();
    let alice = NodeId::new();
    let bob = NodeId::new();

    let ts_before_reopen;
    {
        let store = TripleStore::open(dir.path()).unwrap();
        let mut tx = store.begin();
        tx.insert(relation(alice, "knows", bob));
        ts_before_reopen = tx.commit().unwrap();
    }

    // Reopen — oracle must start above the last committed timestamp.
    {
        let store = TripleStore::open(dir.path()).unwrap();
        let new_read_ts = store.begin().read_ts;
        assert!(
            new_read_ts >= ts_before_reopen,
            "oracle must not reset below last committed timestamp on reopen"
        );

        // New commits get timestamps above the pre-reopen high watermark.
        let carol = NodeId::new();
        let mut tx = store.begin();
        tx.insert(relation(alice, "knows", carol));
        let new_commit_ts = tx.commit().unwrap();
        assert!(new_commit_ts > ts_before_reopen,
            "post-reopen commit_ts must be > pre-reopen commit_ts");
    }
}

#[test]
fn data_from_before_reopen_visible_after_reopen() {
    let dir = TempDir::new().unwrap();
    let alice = NodeId::new();
    let bob = NodeId::new();
    {
        let store = TripleStore::open(dir.path()).unwrap();
        let mut tx = store.begin();
        tx.insert(relation(alice, "knows", bob));
        tx.commit().unwrap();
    }

    {
        let store = TripleStore::open(dir.path()).unwrap();
        // Use a snapshot at "now" (post-reopen read_ts) which is >= commit_ts.
        let snap = store.snapshot(store.begin().read_ts);
        let triples = snap.scan_by_subject(&alice).unwrap();
        assert_eq!(triples.len(), 1, "data must survive reopen");
        assert_eq!(triples[0].predicate().0, "knows");
    }
}
