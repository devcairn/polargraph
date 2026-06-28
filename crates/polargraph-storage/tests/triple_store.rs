//! Integration tests for TripleStore insert + scan round-trips.
//!
//! Organised into sections:
//!   1. Basic scan methods (happy path)
//!   2. Scan isolation (wrong node returns empty)
//!   3. Property value round-trips (all Value variants)
//!   4. Edge ID preservation
//!   5. Persistence across reopen
//!   6. Edge cases (empty results, multiple same-predicate inserts)

use polargraph_core::{
    id::{EdgeId, NodeId},
    temporal::{BiTemporalRange, Timestamp},
    triple::{Predicate, Triple},
    value::Value,
};
use polargraph_storage::TripleStore;
use tempfile::TempDir;

// ── test helpers ──────────────────────────────────────────────────────────────

fn open_store() -> (TripleStore, TempDir) {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    (store, dir)
}

fn temporal() -> BiTemporalRange {
    BiTemporalRange::assert_now(Timestamp::now())
}

fn relation(from: NodeId, pred: &str, to: NodeId) -> Triple {
    Triple::Relation {
        subject: from,
        predicate: Predicate::new(pred),
        object: to,
        edge_id: EdgeId::new(),
        temporal: temporal(),
    }
}

fn relation_with_edge(from: NodeId, pred: &str, to: NodeId, edge_id: EdgeId) -> Triple {
    Triple::Relation {
        subject: from,
        predicate: Predicate::new(pred),
        object: to,
        edge_id,
        temporal: temporal(),
    }
}

fn property(node: NodeId, pred: &str, value: impl Into<Value>) -> Triple {
    Triple::Property {
        subject: node,
        predicate: Predicate::new(pred),
        value: value.into(),
        temporal: temporal(),
    }
}

// ── 1. Basic scan methods ─────────────────────────────────────────────────────

#[test]
fn scan_by_subject_returns_all_triples_for_node() {
    let (store, _dir) = open_store();
    let alice = NodeId::new();
    let bob = NodeId::new();
    let carol = NodeId::new();

    store.insert(&relation(alice, "reports-to", bob)).unwrap();
    store
        .insert(&relation(alice, "collaborates-with", carol))
        .unwrap();
    store.insert(&property(alice, "name", "Alice")).unwrap();

    let triples = store.scan_by_subject(&alice).unwrap();
    assert_eq!(triples.len(), 3);

    let preds: Vec<&str> = triples.iter().map(|t| t.predicate().0.as_str()).collect();
    assert!(preds.contains(&"reports-to"));
    assert!(preds.contains(&"collaborates-with"));
    assert!(preds.contains(&"name"));
}

#[test]
fn scan_by_subject_predicate_filters_correctly() {
    let (store, _dir) = open_store();
    let alice = NodeId::new();
    let bob = NodeId::new();
    let carol = NodeId::new();

    store.insert(&relation(alice, "reports-to", bob)).unwrap();
    store.insert(&relation(alice, "reports-to", carol)).unwrap();
    store.insert(&relation(alice, "manages", bob)).unwrap();

    let triples = store
        .scan_by_subject_predicate(&alice, "reports-to")
        .unwrap();
    assert_eq!(triples.len(), 2);
    for t in &triples {
        assert_eq!(t.predicate().0, "reports-to");
        assert_eq!(t.subject(), alice);
    }
}

#[test]
fn scan_by_predicate_returns_all_subjects() {
    let (store, _dir) = open_store();
    let a = NodeId::new();
    let b = NodeId::new();
    let c = NodeId::new();

    store.insert(&relation(a, "reports-to", b)).unwrap();
    store.insert(&relation(c, "reports-to", b)).unwrap();
    store.insert(&relation(a, "owns", c)).unwrap();

    let triples = store.scan_by_predicate("reports-to").unwrap();
    assert_eq!(triples.len(), 2);
    for t in &triples {
        assert_eq!(t.predicate().0, "reports-to");
    }
}

#[test]
fn scan_by_predicate_object_finds_all_subjects_pointing_to_object() {
    let (store, _dir) = open_store();
    let alice = NodeId::new();
    let bob = NodeId::new();
    let carol = NodeId::new();

    store.insert(&relation(alice, "reports-to", bob)).unwrap();
    store.insert(&relation(carol, "reports-to", bob)).unwrap();
    store.insert(&relation(alice, "reports-to", carol)).unwrap();

    let triples = store.scan_by_predicate_object("reports-to", &bob).unwrap();
    assert_eq!(triples.len(), 2);
    let subjects: Vec<NodeId> = triples.iter().map(|t| t.subject()).collect();
    assert!(subjects.contains(&alice));
    assert!(subjects.contains(&carol));
}

#[test]
fn scan_by_object_returns_all_incoming_edges() {
    let (store, _dir) = open_store();
    let alice = NodeId::new();
    let bob = NodeId::new();
    let carol = NodeId::new();

    store.insert(&relation(alice, "reports-to", bob)).unwrap();
    store.insert(&relation(carol, "reports-to", bob)).unwrap();
    store.insert(&relation(alice, "manages", carol)).unwrap();

    let triples = store.scan_by_object(&bob).unwrap();
    assert_eq!(triples.len(), 2);
}

// ── 2. Scan isolation ─────────────────────────────────────────────────────────

#[test]
fn scan_by_subject_for_unrelated_node_returns_empty() {
    let (store, _dir) = open_store();
    let alice = NodeId::new();
    let bob = NodeId::new();
    let unrelated = NodeId::new();

    store.insert(&relation(alice, "knows", bob)).unwrap();

    let triples = store.scan_by_subject(&unrelated).unwrap();
    assert!(triples.is_empty(), "unrelated node should have no triples");
}

#[test]
fn scan_by_object_for_unrelated_node_returns_empty() {
    let (store, _dir) = open_store();
    let alice = NodeId::new();
    let bob = NodeId::new();
    let unrelated = NodeId::new();

    store.insert(&relation(alice, "knows", bob)).unwrap();

    let triples = store.scan_by_object(&unrelated).unwrap();
    assert!(triples.is_empty());
}

#[test]
fn scan_by_predicate_object_with_wrong_object_returns_empty() {
    let (store, _dir) = open_store();
    let alice = NodeId::new();
    let bob = NodeId::new();
    let carol = NodeId::new();

    store.insert(&relation(alice, "reports-to", bob)).unwrap();

    // carol is not the object of any reports-to edge
    let triples = store
        .scan_by_predicate_object("reports-to", &carol)
        .unwrap();
    assert!(triples.is_empty());
}

#[test]
fn scan_by_predicate_unknown_returns_empty() {
    let (store, _dir) = open_store();
    let a = NodeId::new();
    let b = NodeId::new();
    store.insert(&relation(a, "knows", b)).unwrap();

    assert!(store.scan_by_predicate("nonexistent").unwrap().is_empty());
}

#[test]
fn scan_by_subject_predicate_unknown_predicate_returns_empty() {
    let (store, _dir) = open_store();
    let a = NodeId::new();
    let b = NodeId::new();
    store.insert(&relation(a, "knows", b)).unwrap();

    assert!(store
        .scan_by_subject_predicate(&a, "never-inserted")
        .unwrap()
        .is_empty());
}

#[test]
fn triples_for_node_a_do_not_bleed_into_scan_for_node_b() {
    let (store, _dir) = open_store();
    let a = NodeId::new();
    let b = NodeId::new();
    let c = NodeId::new();

    // Only insert triples for `a`
    store.insert(&relation(a, "knows", c)).unwrap();
    store.insert(&property(a, "name", "A")).unwrap();

    let b_triples = store.scan_by_subject(&b).unwrap();
    assert!(
        b_triples.is_empty(),
        "b has no triples but got {}",
        b_triples.len()
    );
}

// ── 3. Property value round-trips ─────────────────────────────────────────────

fn insert_and_scan_property(value: Value) -> Value {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    let node = NodeId::new();

    store.insert(&property(node, "v", value)).unwrap();

    let triples = store.scan_by_subject(&node).unwrap();
    assert_eq!(triples.len(), 1);
    match triples.into_iter().next().unwrap() {
        Triple::Property { value: v, .. } => v,
        _other => panic!("expected Property, got relation"),
    }
}

#[test]
fn property_null_round_trip() {
    assert_eq!(insert_and_scan_property(Value::Null), Value::Null);
}

#[test]
fn property_bool_true_round_trip() {
    assert_eq!(
        insert_and_scan_property(Value::Bool(true)),
        Value::Bool(true)
    );
}

#[test]
fn property_bool_false_round_trip() {
    assert_eq!(
        insert_and_scan_property(Value::Bool(false)),
        Value::Bool(false)
    );
}

#[test]
fn property_int_round_trip() {
    assert_eq!(insert_and_scan_property(Value::Int(42)), Value::Int(42));
    assert_eq!(
        insert_and_scan_property(Value::Int(i64::MIN)),
        Value::Int(i64::MIN)
    );
    assert_eq!(
        insert_and_scan_property(Value::Int(i64::MAX)),
        Value::Int(i64::MAX)
    );
}

#[test]
fn property_float_round_trip() {
    match insert_and_scan_property(Value::Float(std::f64::consts::E)) {
        Value::Float(f) => assert!((f - std::f64::consts::E).abs() < 1e-15),
        other => panic!("expected Float, got {other:?}"),
    }
}

#[test]
fn property_text_round_trip() {
    let s = "hello, PolarGraph 🌐".to_string();
    assert_eq!(
        insert_and_scan_property(Value::Text(s.clone())),
        Value::Text(s)
    );
}

#[test]
fn property_blob_round_trip() {
    let data = vec![0u8, 1, 127, 128, 255];
    assert_eq!(
        insert_and_scan_property(Value::Blob(data.clone())),
        Value::Blob(data)
    );
}

#[test]
fn multiple_property_types_on_same_node() {
    let (store, _dir) = open_store();
    let node = NodeId::new();

    store.insert(&property(node, "name", "Alice")).unwrap();
    store.insert(&property(node, "level", 5i64)).unwrap();
    store.insert(&property(node, "active", true)).unwrap();

    let triples = store.scan_by_subject(&node).unwrap();
    assert_eq!(triples.len(), 3);

    let find_value = |pred: &str| -> Value {
        triples
            .iter()
            .find(|t| t.predicate().0 == pred)
            .and_then(|t| match t {
                Triple::Property { value, .. } => Some(value.clone()),
                _ => None,
            })
            .unwrap_or_else(|| panic!("{pred} not found"))
    };

    assert_eq!(find_value("name"), Value::Text("Alice".into()));
    assert_eq!(find_value("level"), Value::Int(5));
    assert_eq!(find_value("active"), Value::Bool(true));
}

// ── 4. Edge ID preservation ───────────────────────────────────────────────────

#[test]
fn edge_id_survives_insert_and_scan() {
    let (store, _dir) = open_store();
    let alice = NodeId::new();
    let bob = NodeId::new();
    let original_edge_id = EdgeId::new();

    store
        .insert(&relation_with_edge(alice, "knows", bob, original_edge_id))
        .unwrap();

    let triples = store.scan_by_subject(&alice).unwrap();
    assert_eq!(triples.len(), 1);

    match &triples[0] {
        Triple::Relation { edge_id, .. } => {
            assert_eq!(
                *edge_id, original_edge_id,
                "EdgeId must survive the round-trip"
            );
        }
        Triple::Property { .. } | Triple::EdgeProperty { .. } | Triple::EdgeRelation { .. } => {
            panic!("expected Relation")
        }
    }
}

#[test]
fn two_edges_have_distinct_edge_ids_after_scan() {
    let (store, _dir) = open_store();
    let a = NodeId::new();
    let b = NodeId::new();
    let c = NodeId::new();
    let eid1 = EdgeId::new();
    let eid2 = EdgeId::new();

    store
        .insert(&relation_with_edge(a, "knows", b, eid1))
        .unwrap();
    store
        .insert(&relation_with_edge(a, "knows", c, eid2))
        .unwrap();

    let triples = store.scan_by_subject(&a).unwrap();
    assert_eq!(triples.len(), 2);

    let edge_ids: Vec<EdgeId> = triples
        .iter()
        .filter_map(|t| match t {
            Triple::Relation { edge_id, .. } => Some(*edge_id),
            _ => None,
        })
        .collect();

    assert!(edge_ids.contains(&eid1));
    assert!(edge_ids.contains(&eid2));
}

// ── 5. Persistence across reopen ─────────────────────────────────────────────

#[test]
fn triple_data_persists_after_reopen() {
    let dir = TempDir::new().unwrap();
    let alice = NodeId::new();
    let bob = NodeId::new();

    {
        let store = TripleStore::open(dir.path()).unwrap();
        store.insert(&relation(alice, "reports-to", bob)).unwrap();
        store.insert(&property(alice, "name", "Alice")).unwrap();
    }

    // Drop and reopen
    {
        let store = TripleStore::open(dir.path()).unwrap();
        let triples = store.scan_by_subject(&alice).unwrap();
        assert_eq!(triples.len(), 2, "triples must survive a reopen");

        let preds: Vec<&str> = triples.iter().map(|t| t.predicate().0.as_str()).collect();
        assert!(preds.contains(&"reports-to"));
        assert!(preds.contains(&"name"));
    }
}

#[test]
fn predicate_table_persists_across_reopen() {
    let dir = TempDir::new().unwrap();

    {
        let store = TripleStore::open(dir.path()).unwrap();
        let a = NodeId::new();
        let b = NodeId::new();
        store.insert(&relation(a, "knows", b)).unwrap();
    }

    {
        let store = TripleStore::open(dir.path()).unwrap();
        let id = store.intern_predicate("knows").unwrap();
        assert_eq!(
            store.predicate_string(id),
            Some("knows".to_owned()),
            "predicate must reload from META CF"
        );
    }
}

#[test]
fn new_predicates_after_reopen_get_new_ids() {
    let dir = TempDir::new().unwrap();

    let knows_id;
    {
        let store = TripleStore::open(dir.path()).unwrap();
        knows_id = store.intern_predicate("knows").unwrap();
    }

    {
        let store = TripleStore::open(dir.path()).unwrap();
        let manages_id = store.intern_predicate("manages").unwrap();
        // The new ID must not collide with the one assigned before reopen.
        assert_ne!(manages_id, knows_id);
        // And `knows` must still have its original ID.
        assert_eq!(store.intern_predicate("knows").unwrap(), knows_id);
    }
}

// ── 6. Edge cases ─────────────────────────────────────────────────────────────

#[test]
fn scan_on_empty_store_returns_empty() {
    let (store, _dir) = open_store();
    let node = NodeId::new();
    assert!(store.scan_by_subject(&node).unwrap().is_empty());
    assert!(store.scan_by_object(&node).unwrap().is_empty());
    assert!(store.scan_by_predicate("anything").unwrap().is_empty());
}

#[test]
fn inserting_many_triples_does_not_corrupt_scans() {
    let (store, _dir) = open_store();

    // Insert 50 nodes each connected to a hub.
    let hub = NodeId::new();
    let spokes: Vec<NodeId> = (0..50).map(|_| NodeId::new()).collect();

    for &spoke in &spokes {
        store.insert(&relation(spoke, "member-of", hub)).unwrap();
    }

    // hub should have 50 incoming edges
    let incoming = store.scan_by_object(&hub).unwrap();
    assert_eq!(incoming.len(), 50);

    // Each spoke should have exactly 1 outgoing edge
    for &spoke in &spokes {
        let out = store.scan_by_subject(&spoke).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].predicate().0, "member-of");
    }
}

#[test]
fn two_nodes_same_predicate_same_object_dont_interfere() {
    let (store, _dir) = open_store();
    let a = NodeId::new();
    let b = NodeId::new();
    let target = NodeId::new();

    store.insert(&relation(a, "points-to", target)).unwrap();
    store.insert(&relation(b, "points-to", target)).unwrap();

    // Scanning by subject returns only own triples
    assert_eq!(store.scan_by_subject(&a).unwrap().len(), 1);
    assert_eq!(store.scan_by_subject(&b).unwrap().len(), 1);

    // Scanning by object finds both
    assert_eq!(store.scan_by_object(&target).unwrap().len(), 2);
}

#[test]
fn relation_subject_field_is_correct_after_scan() {
    let (store, _dir) = open_store();
    let alice = NodeId::new();
    let bob = NodeId::new();

    store.insert(&relation(alice, "knows", bob)).unwrap();

    // Verify via every scan method that touches this triple
    let by_sub = store.scan_by_subject(&alice).unwrap();
    assert_eq!(by_sub[0].subject(), alice);

    let by_pred = store.scan_by_predicate("knows").unwrap();
    assert_eq!(by_pred[0].subject(), alice);

    let by_obj = store.scan_by_object(&bob).unwrap();
    assert_eq!(by_obj[0].subject(), alice);
}

// ── Full-text / trigram search ────────────────────────────────────────────────

#[test]
fn trigram_insert_and_search() {
    let (store, _dir) = open_store();
    let alice = NodeId::new();
    store
        .insert(&property(
            alice,
            "name",
            Value::Text("Alice Smith".to_string()),
        ))
        .unwrap();

    let ts = Timestamp(store.oracle_ts());
    let hits = store.text_search("name", "Smith", ts, None).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0], alice);
}

#[test]
fn trigram_no_match_returns_empty() {
    let (store, _dir) = open_store();
    let node = NodeId::new();
    store
        .insert(&property(node, "name", Value::Text("Alice".to_string())))
        .unwrap();

    let ts = Timestamp(store.oracle_ts());
    let hits = store.text_search("name", "Bob", ts, None).unwrap();
    assert!(hits.is_empty());
}

#[test]
fn trigram_multi_intersection() {
    let (store, _dir) = open_store();
    let alice = NodeId::new();
    let bob = NodeId::new();
    store
        .insert(&property(
            alice,
            "bio",
            Value::Text("database engineer".to_string()),
        ))
        .unwrap();
    store
        .insert(&property(
            bob,
            "bio",
            Value::Text("software developer".to_string()),
        ))
        .unwrap();

    let ts = Timestamp(store.oracle_ts());
    // "engineer" contains trigrams unique to alice's value
    let hits = store.text_search("bio", "engineer", ts, None).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0], alice);
}

#[test]
fn trigram_stale_entries_eliminated_by_snapshot_confirmation() {
    // Insert a text value, then overwrite it. The old trigrams stay in TRI CF
    // (no deletion), but text_search must not return the node when the query
    // no longer matches the live value.
    let (store, _dir) = open_store();
    let node = NodeId::new();
    store
        .insert(&property(node, "name", Value::Text("Alice".to_string())))
        .unwrap();
    // Overwrite with a completely different value
    store
        .insert(&property(node, "name", Value::Text("Charlie".to_string())))
        .unwrap();

    let ts = Timestamp(store.oracle_ts());
    // "Alice" is stale — snapshot confirmation must filter it out
    let hits_alice = store.text_search("name", "Ali", ts, None).unwrap();
    assert!(hits_alice.is_empty(), "stale entry should be filtered");

    // "Charlie" is live
    let hits_charlie = store.text_search("name", "Char", ts, None).unwrap();
    assert_eq!(hits_charlie.len(), 1);
    assert_eq!(hits_charlie[0], node);
}

#[test]
fn trigram_short_string_and_empty_string() {
    let (store, _dir) = open_store();
    let a = NodeId::new();
    let b = NodeId::new();
    // Two-character value — padded trigram
    store
        .insert(&property(a, "tag", Value::Text("ab".to_string())))
        .unwrap();
    // Empty string — no trigrams written, so no hits
    store
        .insert(&property(b, "tag", Value::Text("".to_string())))
        .unwrap();

    let ts = Timestamp(store.oracle_ts());
    // Searching for "ab" finds the two-char node
    let hits = store.text_search("tag", "ab", ts, None).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0], a);

    // Empty query produces no results (extract_trigrams returns empty vec)
    let empty_hits = store.text_search("tag", "", ts, None).unwrap();
    assert!(empty_hits.is_empty());
}

#[test]
fn trigram_predicate_isolation() {
    // Trigrams are scoped by predicate — a match on one predicate must not
    // surface results from another predicate.
    let (store, _dir) = open_store();
    let node = NodeId::new();
    store
        .insert(&property(node, "name", Value::Text("Alice".to_string())))
        .unwrap();

    let ts = Timestamp(store.oracle_ts());
    // Correct predicate finds the node
    let hits = store.text_search("name", "Ali", ts, None).unwrap();
    assert_eq!(hits.len(), 1);

    // Wrong predicate returns nothing, even though the text would match
    let miss = store.text_search("bio", "Ali", ts, None).unwrap();
    assert!(miss.is_empty());
}
