//! Storage-level integration tests for access-control triple patterns.
//!
//! These tests verify that MEMBER_OF and HAS_ACCESS triples round-trip through
//! the TripleStore correctly, and that the scan helpers used by AccessCache
//! building work as expected.

use polargraph_core::{
    id::{EdgeId, NodeId},
    schema::{
        BUILTIN_HAS_ACCESS_PRED, BUILTIN_HAS_ACCESS_TYPE_PRED, BUILTIN_MEMBER_OF_PRED,
    },
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

fn now_range() -> BiTemporalRange {
    BiTemporalRange::assert_now(Timestamp::now())
}

fn relation(from: NodeId, pred: &str, to: NodeId) -> Triple {
    Triple::Relation {
        subject: from,
        predicate: Predicate::new(pred),
        object: to,
        edge_id: EdgeId::new(),
        temporal: now_range(),
    }
}

fn property(subject: NodeId, pred: &str, value: Value) -> Triple {
    Triple::Property {
        subject,
        predicate: Predicate::new(pred),
        value,
        temporal: now_range(),
    }
}

fn commit(store: &TripleStore, triples: Vec<Triple>) {
    let mut tx = store.begin();
    for t in triples {
        tx.insert(t);
    }
    tx.commit().unwrap();
}

// ── Test 1: MEMBER_OF triple stores and scans correctly ──────────────────────

#[test]
fn member_of_relation_round_trips() {
    let (store, _dir) = open_store();
    let user = NodeId::new();
    let group = NodeId::new();

    commit(&store, vec![relation(user, BUILTIN_MEMBER_OF_PRED, group)]);

    let results = store
        .scan_by_subject_predicate(&user, BUILTIN_MEMBER_OF_PRED)
        .unwrap();

    assert_eq!(results.len(), 1, "expected exactly one MEMBER_OF triple");
    match &results[0] {
        Triple::Relation { subject, object, .. } => {
            assert_eq!(*subject, user);
            assert_eq!(*object, group);
        }
        other => panic!("expected Relation, got {other:?}"),
    }
}

// ── Test 2: HAS_ACCESS relation triple stores and scans correctly ────────────

#[test]
fn has_access_relation_round_trips() {
    let (store, _dir) = open_store();
    let group = NodeId::new();
    let node = NodeId::new();

    commit(&store, vec![relation(group, BUILTIN_HAS_ACCESS_PRED, node)]);

    let results = store
        .scan_by_subject_predicate(&group, BUILTIN_HAS_ACCESS_PRED)
        .unwrap();

    assert_eq!(results.len(), 1, "expected exactly one HAS_ACCESS triple");
    match &results[0] {
        Triple::Relation { subject, object, .. } => {
            assert_eq!(*subject, group);
            assert_eq!(*object, node);
        }
        other => panic!("expected Relation, got {other:?}"),
    }
}

// ── Test 3: HAS_ACCESS_TYPE property triple stores and scans correctly ────────

#[test]
fn has_access_type_property_round_trips() {
    let (store, _dir) = open_store();
    let group = NodeId::new();
    let type_name = "Document".to_string();

    commit(
        &store,
        vec![property(
            group,
            BUILTIN_HAS_ACCESS_TYPE_PRED,
            Value::Text(type_name.clone()),
        )],
    );

    let results = store
        .scan_by_subject_predicate(&group, BUILTIN_HAS_ACCESS_TYPE_PRED)
        .unwrap();

    assert_eq!(results.len(), 1, "expected exactly one HAS_ACCESS_TYPE triple");
    match &results[0] {
        Triple::Property { subject, value: Value::Text(tn), .. } => {
            assert_eq!(*subject, group);
            assert_eq!(*tn, type_name);
        }
        other => panic!("expected Property with Text value, got {other:?}"),
    }
}

// ── Test 4: Full access chain — user→group→node is resolvable via scans ───────

#[test]
fn full_access_chain_resolves() {
    let (store, _dir) = open_store();
    let user = NodeId::new();
    let group = NodeId::new();
    let node_a = NodeId::new();
    let node_b = NodeId::new();

    commit(
        &store,
        vec![
            relation(user, BUILTIN_MEMBER_OF_PRED, group),
            relation(group, BUILTIN_HAS_ACCESS_PRED, node_a),
            relation(group, BUILTIN_HAS_ACCESS_PRED, node_b),
        ],
    );

    // Step 1: resolve groups the user belongs to.
    let memberships = store
        .scan_by_subject_predicate(&user, BUILTIN_MEMBER_OF_PRED)
        .unwrap();
    assert_eq!(memberships.len(), 1);

    let group_id = match &memberships[0] {
        Triple::Relation { object, .. } => *object,
        _ => panic!("expected Relation"),
    };
    assert_eq!(group_id, group);

    // Step 2: resolve nodes the group has access to.
    let accesses = store
        .scan_by_subject_predicate(&group_id, BUILTIN_HAS_ACCESS_PRED)
        .unwrap();
    assert_eq!(accesses.len(), 2);

    let accessible: std::collections::HashSet<NodeId> = accesses
        .iter()
        .filter_map(|t| match t {
            Triple::Relation { object, .. } => Some(*object),
            _ => None,
        })
        .collect();

    assert!(accessible.contains(&node_a), "node_a should be accessible");
    assert!(accessible.contains(&node_b), "node_b should be accessible");
}
