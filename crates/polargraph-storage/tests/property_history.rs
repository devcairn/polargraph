//! Integration tests for scan_property_history.

use polargraph_core::{
    id::NodeId,
    temporal::{BiTemporalRange, Timestamp},
    triple::{Predicate, Triple},
    value::Value,
};
use polargraph_storage::TripleStore;
use tempfile::TempDir;

fn open_store() -> (TripleStore, TempDir) {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    (store, dir)
}

fn temporal() -> BiTemporalRange {
    BiTemporalRange::assert_now(Timestamp::now())
}

fn insert_property(store: &TripleStore, subject: NodeId, pred: &str, value: Value) -> Timestamp {
    let t = Triple::Property {
        subject,
        predicate: Predicate::new(pred),
        value,
        temporal: temporal(),
    };
    let mut tx = store.begin();
    tx.insert(t);
    tx.commit().unwrap()
}

#[test]
fn property_history_empty_when_no_writes() {
    let (store, _dir) = open_store();
    let node = NodeId::new();
    let history = store.scan_property_history(node, "description", 0).unwrap();
    assert!(history.is_empty());
}

#[test]
fn property_history_single_write() {
    let (store, _dir) = open_store();
    let node = NodeId::new();
    insert_property(&store, node, "name", Value::Text("Alice".into()));

    let history = store.scan_property_history(node, "name", 0).unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].0, Value::Text("Alice".into()));
}

#[test]
fn property_history_multiple_writes_newest_first() {
    let (store, _dir) = open_store();
    let node = NodeId::new();

    let tt1 = insert_property(&store, node, "score", Value::Int(1));
    let tt2 = insert_property(&store, node, "score", Value::Int(2));
    let tt3 = insert_property(&store, node, "score", Value::Int(3));

    let history = store.scan_property_history(node, "score", 0).unwrap();
    assert_eq!(history.len(), 3);

    // Newest first.
    assert_eq!(history[0].0, Value::Int(3));
    assert_eq!(history[1].0, Value::Int(2));
    assert_eq!(history[2].0, Value::Int(1));

    // Transaction times are strictly increasing and match insertion order.
    assert!(tt1.0 < tt2.0 && tt2.0 < tt3.0);
    assert_eq!(history[0].1, tt3.0);
    assert_eq!(history[1].1, tt2.0);
    assert_eq!(history[2].1, tt1.0);
}

#[test]
fn property_history_limit_is_respected() {
    let (store, _dir) = open_store();
    let node = NodeId::new();

    for i in 0..10i64 {
        insert_property(&store, node, "counter", Value::Int(i));
    }

    let history = store.scan_property_history(node, "counter", 3).unwrap();
    assert_eq!(history.len(), 3);
    // Should be the 3 most recent.
    assert_eq!(history[0].0, Value::Int(9));
    assert_eq!(history[1].0, Value::Int(8));
    assert_eq!(history[2].0, Value::Int(7));
}

#[test]
fn property_history_does_not_mix_predicates() {
    let (store, _dir) = open_store();
    let node = NodeId::new();

    insert_property(&store, node, "name", Value::Text("Bob".into()));
    insert_property(&store, node, "age", Value::Int(42));

    let name_history = store.scan_property_history(node, "name", 0).unwrap();
    assert_eq!(name_history.len(), 1);
    assert_eq!(name_history[0].0, Value::Text("Bob".into()));

    let age_history = store.scan_property_history(node, "age", 0).unwrap();
    assert_eq!(age_history.len(), 1);
    assert_eq!(age_history[0].0, Value::Int(42));
}
