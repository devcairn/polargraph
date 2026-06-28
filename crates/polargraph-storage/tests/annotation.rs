//! Integration tests for RDF-star edge annotation storage (EPA / EPO CFs).

use polargraph_core::{
    id::{EdgeId, NodeId},
    temporal::{BiTemporalRange, Timestamp},
    triple::{Predicate, Triple},
    value::Value,
};
use polargraph_storage::{EdgeAnnotationValue, TripleStore};
use tempfile::TempDir;

fn open_store() -> (TripleStore, TempDir) {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    (store, dir)
}

fn temporal() -> BiTemporalRange {
    BiTemporalRange::assert_now(Timestamp::now())
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn insert_relation(store: &TripleStore, from: NodeId, pred: &str, to: NodeId) -> EdgeId {
    let edge_id = EdgeId::new();
    let t = Triple::Relation {
        subject: from,
        predicate: Predicate::new(pred),
        object: to,
        edge_id,
        temporal: temporal(),
    };
    let mut tx = store.begin();
    tx.insert(t);
    tx.commit().unwrap();
    edge_id
}

fn insert_edge_property(store: &TripleStore, edge: EdgeId, pred: &str, value: Value) -> Timestamp {
    let t = Triple::EdgeProperty {
        edge,
        predicate: Predicate::new(pred),
        value,
        temporal: temporal(),
    };
    let mut tx = store.begin();
    tx.insert(t);
    tx.commit().unwrap()
}

fn insert_edge_relation(
    store: &TripleStore,
    edge: EdgeId,
    pred: &str,
    object: NodeId,
) -> Timestamp {
    let t = Triple::EdgeRelation {
        edge,
        predicate: Predicate::new(pred),
        object,
        temporal: temporal(),
    };
    let mut tx = store.begin();
    tx.insert(t);
    tx.commit().unwrap()
}

// ── test 1: insert scalar annotation, scan it back ───────────────────────────

#[test]
fn insert_and_scan_edge_property_annotations() {
    let (store, _dir) = open_store();
    let a = NodeId::new();
    let b = NodeId::new();

    let edge_id = insert_relation(&store, a, "knows", b);
    insert_edge_property(&store, edge_id, "confidence", Value::Float(0.95));
    let ts = insert_edge_property(
        &store,
        edge_id,
        "source",
        Value::Text("db_import".to_string()),
    );

    let annotations = store.scan_edge_annotations(edge_id, ts).unwrap();

    assert_eq!(annotations.len(), 2, "expected 2 edge annotations");

    let conf = annotations
        .iter()
        .find(|a| a.predicate.0.as_str() == "confidence")
        .expect("confidence annotation missing");
    match &conf.value {
        EdgeAnnotationValue::Scalar(Value::Float(f)) => {
            assert!((f - 0.95).abs() < 1e-6, "confidence mismatch");
        }
        other => panic!("unexpected annotation value: {:?}", other),
    }

    let src = annotations
        .iter()
        .find(|a| a.predicate.0.as_str() == "source")
        .expect("source annotation missing");
    match &src.value {
        EdgeAnnotationValue::Scalar(Value::Text(s)) => assert_eq!(s, "db_import"),
        other => panic!("unexpected annotation value: {:?}", other),
    }
}

// ── test 2: insert node annotation (EPO), scan it back ───────────────────────

#[test]
fn insert_and_scan_edge_relation_annotations() {
    let (store, _dir) = open_store();
    let alice = NodeId::new();
    let bob = NodeId::new();
    let provenance_node = NodeId::new();

    let edge_id = insert_relation(&store, alice, "trusts", bob);
    let ts = insert_edge_relation(&store, edge_id, "assertedBy", provenance_node);

    let annotations = store.scan_edge_annotations(edge_id, ts).unwrap();

    assert_eq!(annotations.len(), 1);
    let ann = &annotations[0];
    assert_eq!(ann.predicate.0.as_str(), "assertedBy");
    match ann.value {
        EdgeAnnotationValue::Node(nid) => assert_eq!(nid, provenance_node),
        _ => panic!("expected Node annotation"),
    }
}

// ── test 3: annotations on different edges don't bleed ───────────────────────

#[test]
fn annotations_are_scoped_to_edge() {
    let (store, _dir) = open_store();
    let a = NodeId::new();
    let b = NodeId::new();
    let c = NodeId::new();

    let edge1 = insert_relation(&store, a, "knows", b);
    let edge2 = insert_relation(&store, a, "knows", c);

    insert_edge_property(&store, edge1, "weight", Value::Float(1.0));
    let ts = insert_edge_property(&store, edge2, "weight", Value::Float(2.0));

    let ann1 = store.scan_edge_annotations(edge1, ts).unwrap();
    let ann2 = store.scan_edge_annotations(edge2, ts).unwrap();

    assert_eq!(ann1.len(), 1);
    assert_eq!(ann2.len(), 1);

    match &ann1[0].value {
        EdgeAnnotationValue::Scalar(Value::Float(f)) => assert!((f - 1.0).abs() < 1e-9),
        _ => panic!("wrong value for edge1"),
    }
    match &ann2[0].value {
        EdgeAnnotationValue::Scalar(Value::Float(f)) => assert!((f - 2.0).abs() < 1e-9),
        _ => panic!("wrong value for edge2"),
    }
}

// ── test 4: MVCC isolation — annotation not visible before commit ─────────────

#[test]
fn annotation_mvcc_isolation() {
    let (store, _dir) = open_store();
    let a = NodeId::new();
    let b = NodeId::new();

    let edge_id = insert_relation(&store, a, "likes", b);

    // Snapshot taken before annotation is written (use the relation's commit ts).
    let ts_before = {
        let snap = store.begin();
        snap.read_ts
    };

    let ts_after = insert_edge_property(&store, edge_id, "weight", Value::Float(0.5));

    // Old snapshot should see nothing.
    let ann_before = store.scan_edge_annotations(edge_id, ts_before).unwrap();
    assert!(
        ann_before.is_empty(),
        "annotation should not be visible at ts_before"
    );

    // New snapshot should see the annotation.
    let ann_after = store.scan_edge_annotations(edge_id, ts_after).unwrap();
    assert_eq!(
        ann_after.len(),
        1,
        "annotation should be visible at ts_after"
    );
}

// ── test 5: get_edge_annotation point lookup ─────────────────────────────────

#[test]
fn get_edge_annotation_point_lookup() {
    let (store, _dir) = open_store();
    let a = NodeId::new();
    let b = NodeId::new();

    let edge_id = insert_relation(&store, a, "knows", b);
    let ts = insert_edge_property(&store, edge_id, "score", Value::Int(42));

    let found = store.get_edge_annotation(edge_id, "score", ts).unwrap();

    let ann = found.expect("annotation not found");
    match ann.value {
        EdgeAnnotationValue::Scalar(Value::Int(i)) => assert_eq!(i, 42),
        _ => panic!("unexpected value type"),
    }
}

// ── test 6: persistence across reopen ────────────────────────────────────────

#[test]
fn annotations_persist_across_reopen() {
    let dir = TempDir::new().unwrap();
    let edge_id;
    {
        let store = TripleStore::open(dir.path()).unwrap();
        let a = NodeId::new();
        let b = NodeId::new();
        edge_id = insert_relation(&store, a, "refersTo", b);
        insert_edge_property(&store, edge_id, "certainty", Value::Float(0.8));
    }
    // Reopen — read with a "far future" timestamp to see all committed data.
    let store2 = TripleStore::open(dir.path()).unwrap();
    let ts = {
        let tx = store2.begin();
        tx.read_ts
    };
    let annotations = store2.scan_edge_annotations(edge_id, ts).unwrap();
    assert_eq!(annotations.len(), 1);
    match &annotations[0].value {
        EdgeAnnotationValue::Scalar(Value::Float(f)) => {
            assert!((f - 0.8).abs() < 1e-6)
        }
        _ => panic!("unexpected value after reopen"),
    }
}
