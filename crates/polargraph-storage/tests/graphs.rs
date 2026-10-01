//! Named-graph management: metadata, bitemporal drop, copy and move.

use polargraph_core::{
    id::{EdgeId, GraphId, NodeId},
    temporal::{BiTemporalRange, Timestamp},
    triple::{Predicate, Triple},
    value::Value,
};
use polargraph_storage::{TripleStore, WriteMode};
use tempfile::TempDir;

fn open() -> (TripleStore, TempDir) {
    let dir = TempDir::new().unwrap();
    (TripleStore::open(dir.path()).unwrap(), dir)
}

fn rel(s: NodeId, p: &str, o: NodeId) -> Triple {
    Triple::Relation {
        subject: s,
        predicate: Predicate::new(p),
        object: o,
        edge_id: EdgeId::new(),
        temporal: BiTemporalRange::assert_now(Timestamp(0)),
    }
}

fn prop(s: NodeId, p: &str, v: &str) -> Triple {
    Triple::Property {
        subject: s,
        predicate: Predicate::new(p),
        value: Value::Text(v.into()),
        temporal: BiTemporalRange::assert_now(Timestamp(0)),
    }
}

fn fill(store: &TripleStore, g: GraphId, triples: Vec<Triple>) -> Timestamp {
    let mut tx = store.begin();
    for t in triples {
        tx.insert_in(t, g, WriteMode::Add);
    }
    tx.commit().unwrap()
}

fn live(store: &TripleStore, g: GraphId) -> usize {
    store
        .snapshot(Timestamp(store.oracle_ts()))
        .scan_graph(g)
        .unwrap()
        .len()
}

#[test]
fn create_graph_is_idempotent_and_stores_metadata() {
    let (store, _dir) = open();
    let g = store
        .create_graph(
            "urn:g:proposal-1",
            &[
                ("cb:status".into(), Value::Text("Proposed".into())),
                ("cb:graphKind".into(), Value::Text("ProposalGraph".into())),
            ],
        )
        .unwrap();
    assert_eq!(store.create_graph("urn:g:proposal-1", &[]).unwrap(), g);

    store
        .set_graph_metadata(g, &[("cb:status".into(), Value::Text("Approved".into()))])
        .unwrap();
    assert_eq!(
        store.graph_metadata(g).unwrap(),
        vec![
            ("cb:graphKind".into(), Value::Text("ProposalGraph".into())),
            ("cb:status".into(), Value::Text("Approved".into())),
        ],
        "metadata predicates are replaced, not accumulated"
    );
    // Metadata lives in the system graph, not the graph itself.
    assert_eq!(live(&store, g), 0);
    assert!(store
        .set_graph_metadata(GraphId::DEFAULT, &[("x".into(), Value::Bool(true))])
        .is_err());
}

#[test]
fn drop_graph_closes_quads_but_keeps_history() {
    let (store, _dir) = open();
    let g = store.create_graph("urn:g:records", &[]).unwrap();
    let other = store.create_graph("urn:g:other", &[]).unwrap();
    let (a, b) = (NodeId::new(), NodeId::new());
    let before = fill(
        &store,
        g,
        vec![rel(a, "mentions", b), prop(a, "title", "Q3 review")],
    );
    fill(&store, other, vec![rel(a, "mentions", b)]);

    let stats = store.graph_stats(g).unwrap();
    assert_eq!(stats.live_quads, 2);
    assert_eq!(stats.last_write_tt, before.0);

    assert_eq!(store.drop_graph(g).unwrap(), 2);
    assert_eq!(live(&store, g), 0);
    assert_eq!(live(&store, other), 1, "other graphs untouched");

    // Time travel to before the drop still sees the graph.
    let then = store
        .snapshot(Timestamp(store.oracle_ts()))
        .with_vt_as_of(before.0 - 1);
    assert_eq!(then.scan_graph(g).unwrap().len(), 2);
    let as_of_tx = store.snapshot(before);
    assert_eq!(as_of_tx.scan_graph(g).unwrap().len(), 2);
}

#[test]
fn copy_add_and_move_graphs() {
    let (store, _dir) = open();
    let src = store.create_graph("urn:g:proposal", &[]).unwrap();
    let dst = store.create_graph("urn:g:approved", &[]).unwrap();
    let (a, b, c) = (NodeId::new(), NodeId::new(), NodeId::new());
    fill(
        &store,
        src,
        vec![rel(a, "dependsOn", b), prop(a, "owner", "team")],
    );
    fill(&store, dst, vec![rel(c, "dependsOn", b)]);

    // ADD keeps the target's quads.
    assert_eq!(store.copy_graph(src, dst, false).unwrap(), 2);
    assert_eq!(live(&store, dst), 3);
    // COPY replaces them.
    assert_eq!(store.copy_graph(src, dst, true).unwrap(), 2);
    assert_eq!(live(&store, dst), 2);
    assert_eq!(live(&store, src), 2, "copy leaves the source");

    // MOVE empties the source.
    let archive = store.create_graph("urn:g:archive", &[]).unwrap();
    assert_eq!(store.move_graph(src, archive).unwrap(), 2);
    assert_eq!(live(&store, src), 0);
    assert_eq!(live(&store, archive), 2);
    assert!(
        store.graph_metadata(archive).unwrap().is_empty(),
        "no copy flag left behind"
    );

    // Relation edge ids are graph-independent (plan §2.5).
    let edge_of = |g| {
        store
            .snapshot(Timestamp(store.oracle_ts()))
            .scan_graph(g)
            .unwrap()
            .into_iter()
            .find_map(|t| match t {
                Triple::Relation { edge_id, .. } => Some(edge_id),
                _ => None,
            })
    };
    assert_eq!(edge_of(dst), edge_of(archive));
}

#[test]
fn readable_graphs_restrict_every_scan() {
    use polargraph_storage::GraphScope;
    use std::sync::Arc;

    let (store, _dir) = open();
    let open_g = store.create_graph("urn:g:open", &[]).unwrap();
    let secret = store.create_graph("urn:g:secret", &[]).unwrap();
    let (a, b, c) = (NodeId::new(), NodeId::new(), NodeId::new());
    fill(&store, GraphId::DEFAULT, vec![rel(a, "knows", b)]);
    fill(&store, open_g, vec![prop(a, "title", "public report")]);
    fill(
        &store,
        secret,
        vec![rel(a, "knows", c), prop(a, "title", "secret report")],
    );

    let mut readable = roaring::RoaringBitmap::new();
    readable.insert(GraphId::DEFAULT.0);
    readable.insert(open_g.0);
    let snap = store
        .snapshot(Timestamp(store.oracle_ts()))
        .with_readable_graphs(Arc::new(readable));

    assert_eq!(snap.scan_by_subject(&a).unwrap().len(), 2, "union read");
    assert_eq!(snap.scan_by_predicate("knows").unwrap().len(), 1);
    assert!(snap.scan_by_object(&c).unwrap().is_empty());
    assert!(snap.scan_graph(secret).unwrap().is_empty());
    assert!(snap
        .scan_scoped(Some(&a), None, None, &GraphScope::Named)
        .unwrap()
        .iter()
        .all(|(g, _)| *g == open_g));
    assert_eq!(snap.text_search("title", "report").unwrap(), vec![a]);
    assert!(snap.text_search("title", "secret").unwrap().is_empty());
    assert!(snap.can_read_graph(open_g) && !snap.can_read_graph(secret));

    // An unrestricted snapshot still sees everything.
    let all = store.snapshot(Timestamp(store.oracle_ts()));
    assert_eq!(all.scan_by_subject(&a).unwrap().len(), 4);
}

#[test]
fn graph_grants_and_access_index() {
    use polargraph_core::schema::{GraphAccessLevel as L, BUILTIN_MEMBER_OF_PRED};
    use polargraph_storage::GraphAccessIndex;

    let (store, _dir) = open();
    let g1 = store.create_graph("urn:g:1", &[]).unwrap();
    let g2 = store.create_graph("urn:g:2", &[]).unwrap();
    let (alice, bob, team) = (NodeId::new(), NodeId::new(), NodeId::new());
    fill(
        &store,
        GraphId::DEFAULT,
        vec![rel(bob, BUILTIN_MEMBER_OF_PRED, team)],
    );

    store.grant_graph_access(alice, g1, L::Read).unwrap();
    store.grant_graph_access(alice, g1, L::Write).unwrap(); // re-grant replaces
    store.grant_graph_access(team, g2, L::Admin).unwrap();
    assert!(store
        .grant_graph_access(alice, GraphId::DEFAULT, L::Read)
        .is_err());
    assert_eq!(store.graph_grants().unwrap().len(), 2);

    let index = GraphAccessIndex::build(&store).unwrap();
    let a = index.for_user(&alice);
    assert_eq!(a.level(g1), Some(L::Write));
    assert!(a.allows(g1, L::Propose) && !a.allows(g1, L::Admin));
    assert_eq!(a.level(g2), None);
    assert_eq!(
        a.level(GraphId::DEFAULT),
        Some(L::Write),
        "default graph is open"
    );
    assert!(a.readable().contains(g1.0) && !a.readable().contains(g2.0));

    let b = index.for_user(&bob);
    assert_eq!(b.level(g2), Some(L::Admin), "inherited from the group");

    let stranger = index.for_user(&NodeId::new());
    assert_eq!(stranger.readable().len(), 1, "default graph only");

    assert!(store.revoke_graph_access(alice, g1).unwrap());
    assert!(!store.revoke_graph_access(alice, g1).unwrap());
    let index = GraphAccessIndex::build(&store).unwrap();
    assert_eq!(index.for_user(&alice).level(g1), None);
    // Grants live in the system graph, not the data graphs.
    assert_eq!(live(&store, g1), 0);
}
