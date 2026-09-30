//! Storage format v3: value-hashed property keys, write modes, out-of-line
//! values, named graphs (`docs/design/v3-key-layout.md`).

use polargraph_core::{
    id::{GraphId, NodeId},
    schema::RetentionPolicy,
    temporal::{BiTemporalRange, Timestamp},
    triple::{Predicate, Triple},
    value::Value,
};
use polargraph_storage::{CompactionManager, StorageError, TripleStore, WriteMode};
use tempfile::TempDir;

fn open() -> (TripleStore, TempDir) {
    let dir = TempDir::new().unwrap();
    (TripleStore::open(dir.path()).unwrap(), dir)
}

fn prop(s: NodeId, p: &str, v: Value) -> Triple {
    prop_vt(s, p, v, 0, i64::MAX)
}

fn prop_vt(s: NodeId, p: &str, v: Value, vt_start: i64, vt_end: i64) -> Triple {
    Triple::Property {
        subject: s,
        predicate: Predicate::new(p),
        value: v,
        temporal: BiTemporalRange {
            vt_start: Timestamp(vt_start),
            vt_end: Timestamp(vt_end),
            tt: Timestamp(0),
        },
    }
}

fn text(s: &str) -> Value {
    Value::Text(s.into())
}

fn lang(s: &str, l: &str) -> Value {
    Value::LangText {
        text: s.into(),
        lang: l.into(),
    }
}

fn values(store: &TripleStore, s: &NodeId, p: &str) -> Vec<Value> {
    let mut out: Vec<Value> = store
        .scan_by_subject_predicate(s, p)
        .unwrap()
        .into_iter()
        .filter_map(|t| match t {
            Triple::Property { value, .. } => Some(value),
            _ => None,
        })
        .collect();
    out.sort_by_key(|v| format!("{v:?}"));
    out
}

fn commit(store: &TripleStore, writes: Vec<(Triple, GraphId, WriteMode)>) -> Timestamp {
    let mut tx = store.begin();
    for (t, g, m) in writes {
        tx.insert_in(t, g, m);
    }
    tx.commit().unwrap()
}

// ── multi-valued properties ───────────────────────────────────────────────────

#[test]
fn add_keeps_several_values_of_one_property() {
    let (store, _dir) = open();
    let s = NodeId::new();
    let d = GraphId::DEFAULT;
    commit(
        &store,
        vec![
            (prop(s, "label", lang("Acme", "en")), d, WriteMode::Add),
            (prop(s, "label", lang("Acmé", "fr")), d, WriteMode::Add),
        ],
    );
    assert_eq!(
        values(&store, &s, "label"),
        vec![lang("Acme", "en"), lang("Acmé", "fr")],
        "both labels survive — v2 kept only the last"
    );
}

#[test]
fn replace_closes_other_values_and_keeps_valid_time_history() {
    let (store, _dir) = open();
    let s = NodeId::new();
    let d = GraphId::DEFAULT;
    commit(
        &store,
        vec![
            (
                prop_vt(s, "status", text("a"), 100, i64::MAX),
                d,
                WriteMode::Add,
            ),
            (
                prop_vt(s, "status", text("b"), 100, i64::MAX),
                d,
                WriteMode::Add,
            ),
        ],
    );
    let ts = commit(
        &store,
        vec![(
            prop_vt(s, "status", text("c"), 200, i64::MAX),
            d,
            WriteMode::Replace,
        )],
    );
    assert_eq!(values(&store, &s, "status"), vec![text("c")]);

    // Before the replacement's vt_start both earlier values still hold.
    let earlier: Vec<Value> = store
        .snapshot(ts)
        .with_vt_as_of(150)
        .scan_by_subject_predicate(&s, "status")
        .unwrap()
        .into_iter()
        .filter_map(|t| match t {
            Triple::Property { value, .. } => Some(value),
            _ => None,
        })
        .collect();
    assert_eq!(earlier.len(), 2);

    // History shows the writes, not the replace's closing entries.
    let history: Vec<Value> = store
        .scan_property_history(s, "status", 10)
        .unwrap()
        .into_iter()
        .map(|(v, _)| v)
        .collect();
    assert_eq!(history.len(), 3);
    assert_eq!(history[0], text("c"));
}

#[test]
fn default_insert_replaces_like_v2_and_delete_closes_only_its_value() {
    let (store, _dir) = open();
    let s = NodeId::new();
    store.insert(&prop(s, "name", text("Alice"))).unwrap();
    store.insert(&prop(s, "name", text("Alicia"))).unwrap();
    assert_eq!(values(&store, &s, "name"), vec![text("Alicia")]);

    // Add a second value, then DELETE one: a closing write is `Add` under
    // `Auto`, so it never closes the other value.
    commit(
        &store,
        vec![(
            prop(s, "name", text("Al")),
            GraphId::DEFAULT,
            WriteMode::Add,
        )],
    );
    store
        .insert(&prop_vt(s, "name", text("Al"), 0, Timestamp::now().0))
        .unwrap();
    assert_eq!(values(&store, &s, "name"), vec![text("Alicia")]);
}

#[test]
fn value_lookup_uses_the_value_hash() {
    let (store, _dir) = open();
    let (a, b, c) = (NodeId::new(), NodeId::new(), NodeId::new());
    store.insert(&prop(a, "status", text("blocked"))).unwrap();
    store.insert(&prop(b, "status", text("active"))).unwrap();
    store.insert(&prop(c, "status", Value::Int(1))).unwrap();

    let snap = store.snapshot(Timestamp(store.oracle_ts()));
    let hits = snap
        .scan_by_predicate_value("status", &text("blocked"))
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].subject(), a);
    assert!(
        snap.scan_by_predicate_value("status", &text("1"))
            .unwrap()
            .is_empty(),
        "types are part of the value"
    );
}

// ── out-of-line values ────────────────────────────────────────────────────────

#[test]
fn large_values_round_trip_through_the_blob_cf_and_are_swept() {
    let (store, _dir) = open();
    store.set_inline_value_max_bytes(16);
    let s = NodeId::new();
    let body = "x".repeat(4_000);

    store
        .insert_at_ts(&prop(s, "body", text(&body)), Timestamp(1))
        .unwrap();
    assert_eq!(values(&store, &s, "body"), vec![text(&body)]);
    assert_eq!(
        store.scan_property_history(s, "body", 5).unwrap()[0].0,
        text(&body)
    );
    assert!(store.cf_approx_key_count("blob") <= 1);

    // Replace it (still in the ancient past), then retention: the original
    // version is pruned, and with vt_lookback the closed value goes entirely,
    // leaving its blob unreferenced.
    store
        .insert_at_ts(&prop(s, "body", text("short")), Timestamp(2))
        .unwrap();
    let stats = CompactionManager::new(store.clone())
        .run_retention(&RetentionPolicy {
            tx_age_secs: 1,
            vt_lookback_secs: Some(1),
        })
        .unwrap();
    assert_eq!(stats.blobs_deleted, 1);
    assert_eq!(values(&store, &s, "body"), vec![text("short")]);
}

#[test]
fn identical_large_values_share_one_blob() {
    let (store, _dir) = open();
    store.set_inline_value_max_bytes(16);
    let body = text(&"boilerplate ".repeat(100));
    let (a, b) = (NodeId::new(), NodeId::new());
    store.insert(&prop(a, "body", body.clone())).unwrap();
    store.insert(&prop(b, "body", body.clone())).unwrap();
    assert_eq!(values(&store, &a, "body"), vec![body.clone()]);
    assert_eq!(values(&store, &b, "body"), vec![body]);
    let sweep = store.sweep_unreferenced_blobs().unwrap();
    assert_eq!(sweep, 0, "both references keep the shared blob");
}

// ── named graphs ──────────────────────────────────────────────────────────────

#[test]
fn quads_in_named_graphs_are_partitioned_and_unioned() {
    let dir = TempDir::new().unwrap();
    let s = NodeId::new();
    let (g1, g2);
    {
        let store = TripleStore::open(dir.path()).unwrap();
        g1 = store.intern_graph("https://kb.example/g/eng").unwrap();
        g2 = store.intern_graph("https://kb.example/g/sales").unwrap();
        assert_ne!(g1, GraphId::DEFAULT);
        assert_eq!(store.intern_graph("https://kb.example/g/eng").unwrap(), g1);

        // The same fact in two graphs, plus a fact only in g2.
        commit(
            &store,
            vec![
                (prop(s, "owner", text("team-id")), g1, WriteMode::Add),
                (prop(s, "owner", text("team-id")), g2, WriteMode::Add),
                (prop(s, "tier", text("gold")), g2, WriteMode::Add),
            ],
        );
    }
    // Graph ids persist across reopen.
    let store = TripleStore::open(dir.path()).unwrap();
    assert_eq!(store.graph_id("https://kb.example/g/eng"), Some(g1));
    assert_eq!(
        store.graph_iri(g2).as_deref(),
        Some("https://kb.example/g/sales")
    );

    let snap = store.snapshot(Timestamp(store.oracle_ts()));
    assert_eq!(snap.scan_graph(g1).unwrap().len(), 1);
    assert_eq!(snap.scan_graph(g2).unwrap().len(), 2);
    assert!(snap.scan_graph(GraphId::DEFAULT).unwrap().is_empty());
    assert_eq!(
        snap.scan_by_subject(&s).unwrap().len(),
        2,
        "union reads de-duplicate the fact held in two graphs"
    );
}

#[test]
fn replace_is_scoped_to_its_graph() {
    let (store, _dir) = open();
    let g1 = store.intern_graph("urn:g1").unwrap();
    let s = NodeId::new();
    commit(
        &store,
        vec![(prop(s, "status", text("draft")), g1, WriteMode::Add)],
    );
    commit(
        &store,
        vec![(
            prop(s, "status", text("final")),
            GraphId::DEFAULT,
            WriteMode::Replace,
        )],
    );
    let snap = store.snapshot(Timestamp(store.oracle_ts()));
    assert_eq!(
        snap.scan_graph(g1).unwrap().len(),
        1,
        "g1's value untouched"
    );
}

// ── conflicts ─────────────────────────────────────────────────────────────────

#[test]
fn conflict_rules_follow_the_write_mode() {
    let (store, _dir) = open();
    let s = NodeId::new();
    let d = GraphId::DEFAULT;
    store.insert(&prop(s, "p", text("seed"))).unwrap();

    // Two concurrent Replaces of one property conflict.
    let mut t1 = store.begin();
    let mut t2 = store.begin();
    t1.insert_in(prop(s, "p", text("x")), d, WriteMode::Replace);
    t2.insert_in(prop(s, "p", text("y")), d, WriteMode::Replace);
    t1.commit().unwrap();
    assert!(matches!(t2.commit(), Err(StorageError::WriteConflict(_))));

    // Concurrent Adds of different values don't.
    let mut t1 = store.begin();
    let mut t2 = store.begin();
    t1.insert_in(prop(s, "p", text("m")), d, WriteMode::Add);
    t2.insert_in(prop(s, "p", text("n")), d, WriteMode::Add);
    t1.commit().unwrap();
    t2.commit().unwrap();

    // The same Replace in two different graphs doesn't either.
    let g1 = store.intern_graph("urn:g1").unwrap();
    let mut t1 = store.begin();
    let mut t2 = store.begin();
    t1.insert_in(prop(s, "q", text("x")), d, WriteMode::Replace);
    t2.insert_in(prop(s, "q", text("x")), g1, WriteMode::Replace);
    t1.commit().unwrap();
    t2.commit().unwrap();
}

// ── format ────────────────────────────────────────────────────────────────────

#[test]
fn new_stores_are_created_in_format_3_without_v2_cfs() {
    let dir = TempDir::new().unwrap();
    drop(TripleStore::open(dir.path()).unwrap());
    let cfs = rocksdb::DB::list_cf(&rocksdb::Options::default(), dir.path()).unwrap();
    assert!(cfs.contains(&"spog".to_string()));
    assert!(!cfs.contains(&"spo".to_string()));
    // Reopening a format-3 store works.
    TripleStore::open(dir.path()).unwrap();
}

// ── graph scopes ──────────────────────────────────────────────────────────────

#[test]
fn scoped_scans_respect_every_scope_and_report_graphs() {
    use polargraph_core::term;
    use polargraph_storage::GraphScope;

    let (store, _dir) = open();
    let g1 = store.intern_graph("urn:g1").unwrap();
    let g2 = store.intern_graph("urn:g2").unwrap();
    let (a, b) = (NodeId::new(), NodeId::new());
    let d = GraphId::DEFAULT;
    commit(
        &store,
        vec![
            (prop(a, "status", text("draft")), g1, WriteMode::Add),
            (prop(a, "status", text("final")), g2, WriteMode::Add),
            (prop(b, "status", text("final")), d, WriteMode::Add),
        ],
    );
    let snap = store.snapshot(Timestamp(store.oracle_ts()));
    let graphs_of = |scope: GraphScope, s: Option<&NodeId>| {
        let mut gs: Vec<GraphId> = snap
            .scan_scoped(s, Some("status"), None, &scope)
            .unwrap()
            .into_iter()
            .map(|(g, _)| g)
            .collect();
        gs.sort();
        gs
    };
    assert_eq!(graphs_of(GraphScope::Union, None), vec![d, g1, g2]);
    assert_eq!(graphs_of(GraphScope::Named, None), vec![g1, g2]);
    assert_eq!(graphs_of(GraphScope::One(g2), None), vec![g2]);
    assert_eq!(graphs_of(GraphScope::One(g1), Some(&a)), vec![g1]);
    assert_eq!(graphs_of(GraphScope::set(vec![g2, d]), None), vec![d, g2]);
    assert!(graphs_of(GraphScope::One(g1), Some(&b)).is_empty());

    // Bound value object through the value index, within one graph.
    let fin = polargraph_storage::keys::value_object(&text("final"));
    let hits = snap
        .scan_scoped(None, Some("status"), Some(&fin), &GraphScope::Named)
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].0, g2);

    // Graph ↔ node mapping, and the graph IRI is in the IRI dictionary.
    let n1 = store.graph_node(g1).unwrap();
    assert_eq!(n1, term::iri_to_node_id("urn:g1"));
    assert_eq!(store.graph_for_node(&n1), Some(g1));
    assert_eq!(store.graph_node(GraphId::DEFAULT), None);
    assert_eq!(store.iri_of(&n1).unwrap().as_deref(), Some("urn:g1"));
}
