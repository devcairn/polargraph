//! Change log (`chg` CF) backing the Subscribe feed.

use polargraph_core::{
    id::{EdgeId, GraphId, NodeId},
    temporal::{BiTemporalRange, Timestamp},
    triple::{Predicate, Triple},
    value::Value,
};
use polargraph_storage::{GraphOp, TripleStore, WriteMode};
use tempfile::TempDir;

fn open() -> (TripleStore, TempDir) {
    let dir = TempDir::new().unwrap();
    (TripleStore::open(dir.path()).unwrap(), dir)
}

fn prop(s: NodeId, p: &str, v: &str) -> Triple {
    Triple::Property {
        subject: s,
        predicate: Predicate::new(p),
        value: Value::Text(v.into()),
        temporal: BiTemporalRange::assert_now(Timestamp(0)),
    }
}

#[test]
fn commits_are_logged_with_author_quads_and_graph_ops() {
    let (store, _dir) = open();
    // A new store logs from the start (floor = oracle time at open, 0 here).
    let floor = store.changes_floor().unwrap();

    let (a, b) = (NodeId::new(), NodeId::new());
    let mut tx = store.begin();
    tx.set_author("urn:user:alice");
    tx.insert(Triple::Relation {
        subject: a,
        predicate: Predicate::new("knows"),
        object: b,
        edge_id: EdgeId::new(),
        temporal: BiTemporalRange::assert_now(Timestamp(0)),
    });
    tx.insert(prop(a, "name", "Ann"));
    let t1 = tx.commit().unwrap();

    // A Replace closes the old value: both versions are logged.
    let mut tx = store.begin();
    tx.insert(prop(a, "name", "Anne"));
    let t2 = tx.commit().unwrap();

    let g = store
        .create_graph_by("urn:g:1", &[], "urn:user:bob")
        .unwrap();
    store.drop_graph(g).unwrap();

    let log = store.changes_after(floor, 100).unwrap();
    assert_eq!(log.len(), 4);
    assert_eq!(log[0].commit_ts, t1);
    assert_eq!(log[0].author, "urn:user:alice");
    assert_eq!(log[0].quads.len(), 2);
    assert!(log[0].quads.iter().all(|(g, _)| *g == GraphId::DEFAULT));

    assert_eq!(log[1].commit_ts, t2);
    let closed: Vec<_> = log[1]
        .quads
        .iter()
        .map(|(_, t)| (t.temporal().vt_end == Timestamp::END_OF_TIME, t.clone()))
        .collect();
    assert_eq!(closed.len(), 2);
    assert!(closed.iter().any(|(open, t)| *open
        && matches!(t, Triple::Property { value: Value::Text(v), .. } if v == "Anne")));
    assert!(closed.iter().any(|(open, t)| !*open
        && matches!(t, Triple::Property { value: Value::Text(v), .. } if v == "Ann")));

    assert_eq!(log[2].graph_ops, vec![GraphOp::Created(g)]);
    assert_eq!(log[2].author, "urn:user:bob");
    assert_eq!(log[3].graph_ops, vec![GraphOp::Dropped(g)]);

    // Resume after a point; limit.
    assert_eq!(store.changes_after(t1, 100).unwrap().len(), 3);
    assert_eq!(store.changes_after(floor, 1).unwrap().len(), 1);

    // Pruning removes old entries and raises the floor.
    assert_eq!(store.prune_changes(t2).unwrap(), 1);
    assert!(store.changes_floor().unwrap() >= t1);
    assert_eq!(store.changes_after(floor, 100).unwrap()[0].commit_ts, t2);

    // A copy is logged as one Copied op on its last commit.
    let src = store.create_graph("urn:g:src", &[]).unwrap();
    let dst = store.create_graph("urn:g:dst", &[]).unwrap();
    let mut tx = store.begin();
    tx.insert_in(prop(b, "x", "1"), src, WriteMode::Add);
    tx.commit().unwrap();
    let before = Timestamp(store.oracle_ts());
    store.copy_graph(src, dst, false).unwrap();
    let log = store.changes_after(before, 100).unwrap();
    assert_eq!(log.len(), 1);
    assert_eq!(
        log[0].graph_ops,
        vec![GraphOp::Copied {
            source: src,
            target: dst
        }]
    );
    assert_eq!(log[0].quads.len(), 1);
    assert_eq!(log[0].quads[0].0, dst);
}

#[test]
fn reopen_keeps_floor_and_log() {
    let dir = TempDir::new().unwrap();
    let floor = {
        let store = TripleStore::open(dir.path()).unwrap();
        let mut tx = store.begin();
        tx.insert(prop(NodeId::new(), "n", "v"));
        tx.commit().unwrap();
        store.changes_floor().unwrap()
    };
    let store = TripleStore::open(dir.path()).unwrap();
    assert_eq!(store.changes_floor().unwrap(), floor);
    assert_eq!(store.changes_after(floor, 10).unwrap().len(), 1);
}
