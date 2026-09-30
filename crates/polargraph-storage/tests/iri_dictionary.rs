//! IRI dictionary: hashed NodeIds can be mapped back to their IRIs.

use polargraph_core::{
    id::NodeId,
    temporal::{BiTemporalRange, Timestamp},
    term,
    triple::{Predicate, Triple},
};
use polargraph_storage::{SstImporter, TripleStore};
use tempfile::TempDir;

fn open() -> (TripleStore, TempDir) {
    let dir = TempDir::new().unwrap();
    (TripleStore::open(dir.path()).unwrap(), dir)
}

const ALICE: &str = "http://example.org/Alice";
const BOB: &str = "http://example.org/Bob";

fn knows(s: &str, o: &str) -> Triple {
    Triple::Relation {
        subject: term::iri_to_node_id(s),
        predicate: Predicate::new("http://schema.org/knows"),
        object: term::iri_to_node_id(o),
        edge_id: term::edge_id_for(s, "http://schema.org/knows", o),
        temporal: BiTemporalRange::assert_now(Timestamp::now()),
    }
}

#[test]
fn transaction_records_iris_with_its_triples() {
    let (store, _dir) = open();
    let mut tx = store.begin();
    tx.insert(knows(ALICE, BOB));
    tx.bind_iri(ALICE);
    tx.bind_iri(BOB);
    tx.commit().unwrap();

    let alice = term::iri_to_node_id(ALICE);
    assert_eq!(store.iri_of(&alice).unwrap().as_deref(), Some(ALICE));

    let unknown = NodeId::new();
    let found = store
        .iris_of(&[alice, term::iri_to_node_id(BOB), unknown])
        .unwrap();
    assert_eq!(found.len(), 2);
    assert_eq!(found[&term::iri_to_node_id(BOB)], BOB);
}

#[test]
fn uuid_iris_are_not_stored_and_rebinding_is_idempotent() {
    let (store, _dir) = open();
    let native = NodeId::new();
    let uuid_iri = term::fallback_iri(&native);

    store.bind_iris([uuid_iri.as_str(), ALICE]).unwrap();
    store.bind_iris([ALICE, ALICE]).unwrap(); // same IRI again: fine

    assert_eq!(
        store.iri_of(&native).unwrap(),
        None,
        "urn:uuid is derivable"
    );
    assert_eq!(
        store
            .iri_of(&term::iri_to_node_id(ALICE))
            .unwrap()
            .as_deref(),
        Some(ALICE)
    );
}

#[test]
fn iri_only_transaction_commits() {
    let (store, _dir) = open();
    let mut tx = store.begin();
    tx.bind_iri(ALICE);
    tx.commit().unwrap();
    assert!(store
        .iri_of(&term::iri_to_node_id(ALICE))
        .unwrap()
        .is_some());
}

#[test]
fn sst_import_records_iris() {
    let (store, dir) = open();
    let mut importer = SstImporter::new(&dir.path().join("sst")).unwrap();
    importer.add_triple(&knows(ALICE, BOB));
    importer.add_iri(ALICE);
    importer.add_iri(BOB);
    importer.finish(&store).unwrap();

    assert_eq!(
        store.iri_of(&term::iri_to_node_id(BOB)).unwrap().as_deref(),
        Some(BOB)
    );
}

#[test]
fn iris_survive_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let store = TripleStore::open(dir.path()).unwrap();
        store.bind_iris([ALICE]).unwrap();
    }
    let store = TripleStore::open(dir.path()).unwrap();
    assert_eq!(
        store
            .iri_of(&term::iri_to_node_id(ALICE))
            .unwrap()
            .as_deref(),
        Some(ALICE)
    );
}
