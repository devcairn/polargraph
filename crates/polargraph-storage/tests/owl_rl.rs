//! OWL 2 RL inference into queryable inferred graphs
//! (docs/design/step9-inference-vectors-stats.md, 9a).

use polargraph_core::{
    id::{EdgeId, GraphId, NodeId},
    schema::GraphAccessLevel,
    temporal::{BiTemporalRange, Timestamp},
    term::iri_to_node_id,
    triple::{Predicate, Triple},
};
use polargraph_storage::{close_at, owl_rl, GraphAccessIndex, GraphScope, TripleStore, WriteMode};
use tempfile::TempDir;

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDFS_DOMAIN: &str = "http://www.w3.org/2000/01/rdf-schema#domain";
const RDFS_RANGE: &str = "http://www.w3.org/2000/01/rdf-schema#range";
const RDFS_SUBCLASS_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
const RDFS_SUBPROP_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subPropertyOf";
const OWL_INVERSE_OF: &str = "http://www.w3.org/2002/07/owl#inverseOf";
const OWL_SAME_AS: &str = "http://www.w3.org/2002/07/owl#sameAs";
const OWL_SYMMETRIC_PROP: &str = "http://www.w3.org/2002/07/owl#SymmetricProperty";
const OWL_TRANSITIVE_PROP: &str = "http://www.w3.org/2002/07/owl#TransitiveProperty";

fn open_store() -> (TripleStore, TempDir) {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    (store, dir)
}

fn n(iri: &str) -> NodeId {
    iri_to_node_id(iri)
}

/// Insert `(s p o)` into `graph` ("" = default graph), recording the IRIs
/// as RDF import does (properties are named through the IRI dictionary).
fn rel(store: &TripleStore, graph: &str, s: &str, p: &str, o: &str) {
    let g = if graph.is_empty() {
        GraphId::DEFAULT
    } else {
        store.create_graph(graph, &[]).unwrap()
    };
    let mut tx = store.begin();
    for iri in [s, p, o] {
        tx.bind_iri(iri);
    }
    tx.insert_in(
        Triple::Relation {
            subject: n(s),
            predicate: Predicate::new(p),
            object: n(o),
            edge_id: EdgeId::new(),
            temporal: BiTemporalRange::assert_now(Timestamp::now()),
        },
        g,
        WriteMode::Add,
    );
    tx.commit().unwrap();
}

/// Close the live `(s p o)` in `graph`.
fn retract(store: &TripleStore, graph: &str, s: &str, p: &str, o: &str) {
    let g = if graph.is_empty() {
        GraphId::DEFAULT
    } else {
        store.graph_id(graph).unwrap()
    };
    let snap = store.snapshot(Timestamp(store.oracle_ts()));
    let live = snap
        .scan_scoped(Some(&n(s)), Some(p), Some(&n(o)), &GraphScope::One(g))
        .unwrap();
    let mut tx = store.begin();
    for (g, t) in live {
        tx.insert_in(close_at(t, Timestamp::now()), g, WriteMode::Add);
    }
    tx.commit().unwrap();
}

/// Whether `(s p o)` is live in the inferred graph `inferred_iri`.
fn inferred(store: &TripleStore, inferred_iri: &str, s: &str, p: &str, o: &str) -> bool {
    let Some(g) = store.graph_id(inferred_iri) else {
        return false;
    };
    let snap = store.snapshot(Timestamp(store.oracle_ts()));
    !snap
        .scan_scoped(Some(&n(s)), Some(p), Some(&n(o)), &GraphScope::One(g))
        .unwrap()
        .is_empty()
}

const DEFAULT_INFERRED: &str = owl_rl::INFERRED_DEFAULT_GRAPH_IRI;

#[test]
fn inferred_facts_are_ordinary_queryable_quads() {
    let (store, _d) = open_store();
    rel(
        &store,
        "",
        "http://ex/knows",
        RDFS_DOMAIN,
        "http://ex/Person",
    );
    rel(
        &store,
        "",
        "http://ex/alice",
        "http://ex/knows",
        "http://ex/bob",
    );
    let stats = owl_rl::materialize(&store, true).unwrap();
    assert_eq!(stats.asserted, 1);
    assert!(inferred(
        &store,
        DEFAULT_INFERRED,
        "http://ex/alice",
        RDF_TYPE,
        "http://ex/Person"
    ));
    // Visible to an ordinary (union) read — no special read path.
    let snap = store.snapshot(Timestamp(store.oracle_ts()));
    let types = snap
        .scan_by_subject_predicate(&n("http://ex/alice"), RDF_TYPE)
        .unwrap();
    assert_eq!(types.len(), 1);
    // ... and hidden when the inferred graphs are excluded.
    let ids = owl_rl::InferredGraphs::load(&store).ids();
    let without = snap.without_graphs(&ids);
    assert!(without
        .scan_by_subject_predicate(&n("http://ex/alice"), RDF_TYPE)
        .unwrap()
        .is_empty());
}

#[test]
fn every_rule_fires() {
    let (store, _d) = open_store();
    let ex = |l: &str| format!("http://ex/{l}");
    // Schema.
    rel(&store, "", &ex("worksFor"), RDFS_RANGE, &ex("Org"));
    rel(&store, "", &ex("Employee"), RDFS_SUBCLASS_OF, &ex("Person"));
    rel(&store, "", &ex("Person"), RDFS_SUBCLASS_OF, &ex("Agent"));
    rel(
        &store,
        "",
        &ex("manages"),
        RDFS_SUBPROP_OF,
        &ex("worksWith"),
    );
    rel(&store, "", &ex("worksWith"), RDFS_SUBPROP_OF, &ex("knows"));
    rel(&store, "", &ex("friend"), RDF_TYPE, OWL_SYMMETRIC_PROP);
    rel(&store, "", &ex("partOf"), RDF_TYPE, OWL_TRANSITIVE_PROP);
    rel(&store, "", &ex("hasPart"), OWL_INVERSE_OF, &ex("partOf"));
    // Data.
    rel(&store, "", &ex("ann"), RDF_TYPE, &ex("Employee"));
    rel(&store, "", &ex("ann"), &ex("worksFor"), &ex("acme"));
    rel(&store, "", &ex("ann"), &ex("manages"), &ex("bo"));
    rel(&store, "", &ex("ann"), &ex("friend"), &ex("cy"));
    rel(&store, "", &ex("wheel"), &ex("partOf"), &ex("car"));
    rel(&store, "", &ex("car"), &ex("partOf"), &ex("fleet"));
    rel(&store, "", &ex("a1"), OWL_SAME_AS, &ex("a2"));
    rel(&store, "", &ex("a2"), OWL_SAME_AS, &ex("a3"));
    owl_rl::materialize(&store, true).unwrap();

    let has = |s: &str, p: &str, o: &str| inferred(&store, DEFAULT_INFERRED, &ex(s), p, &ex(o));
    assert!(has("acme", RDF_TYPE, "Org"), "rdfs3");
    assert!(has("ann", RDF_TYPE, "Person"), "rdfs9");
    assert!(has("ann", RDF_TYPE, "Agent"), "rdfs9 over rdfs11");
    assert!(has("Employee", RDFS_SUBCLASS_OF, "Agent"), "rdfs11");
    assert!(has("manages", RDFS_SUBPROP_OF, "knows"), "rdfs5");
    assert!(has("ann", &ex("worksWith"), "bo"), "rdfs7");
    assert!(has("ann", &ex("knows"), "bo"), "rdfs7 over rdfs5");
    assert!(has("cy", &ex("friend"), "ann"), "prp-symp");
    assert!(has("wheel", &ex("partOf"), "fleet"), "prp-trp");
    assert!(has("car", &ex("hasPart"), "wheel"), "prp-inv2");
    assert!(has("a2", OWL_SAME_AS, "a1"), "eq-sym");
    assert!(has("a1", OWL_SAME_AS, "a3"), "eq-trans");
}

#[test]
fn facts_land_in_their_instance_graphs_inferred_graph() {
    let (store, _d) = open_store();
    // The schema lives in its own graph; the data in two others.
    rel(
        &store,
        "urn:onto",
        "http://ex/Cat",
        RDFS_SUBCLASS_OF,
        "http://ex/Animal",
    );
    rel(&store, "urn:g1", "http://ex/tom", RDF_TYPE, "http://ex/Cat");
    rel(
        &store,
        "urn:g1",
        "http://ex/a",
        "http://ex/near",
        "http://ex/b",
    );
    rel(
        &store,
        "urn:g2",
        "http://ex/b",
        "http://ex/near",
        "http://ex/c",
    );
    rel(&store, "", "http://ex/near", RDF_TYPE, OWL_TRANSITIVE_PROP);
    owl_rl::materialize(&store, true).unwrap();

    assert!(inferred(
        &store,
        "urn:pg:inferred:urn:g1",
        "http://ex/tom",
        RDF_TYPE,
        "http://ex/Animal"
    ));
    // Instance premises from two graphs → the cross graph.
    assert!(inferred(
        &store,
        owl_rl::INFERRED_CROSS_GRAPH_IRI,
        "http://ex/a",
        "http://ex/near",
        "http://ex/c"
    ));
    assert!(!inferred(
        &store,
        "urn:pg:inferred:urn:g1",
        "http://ex/a",
        "http://ex/near",
        "http://ex/c"
    ));
}

#[test]
fn reruns_are_idempotent_and_close_what_no_longer_follows() {
    let (store, _d) = open_store();
    rel(
        &store,
        "",
        "http://ex/Cat",
        RDFS_SUBCLASS_OF,
        "http://ex/Animal",
    );
    rel(&store, "", "http://ex/tom", RDF_TYPE, "http://ex/Cat");
    let first = owl_rl::materialize(&store, true).unwrap();
    assert_eq!((first.asserted, first.closed), (1, 0));
    let again = owl_rl::materialize(&store, true).unwrap();
    assert_eq!((again.asserted, again.closed), (0, 0), "no change");

    retract(&store, "", "http://ex/tom", RDF_TYPE, "http://ex/Cat");
    let after = owl_rl::materialize(&store, true).unwrap();
    assert_eq!((after.asserted, after.closed), (0, 1));
    assert!(!inferred(
        &store,
        DEFAULT_INFERRED,
        "http://ex/tom",
        RDF_TYPE,
        "http://ex/Animal"
    ));
    // Bitemporal: the inferred fact is closed, not deleted — still visible
    // at a time when it held.
    assert_eq!(after.derived_triples, 0);
}

#[test]
fn a_base_fact_is_not_duplicated_as_inferred() {
    let (store, _d) = open_store();
    rel(
        &store,
        "",
        "http://ex/Cat",
        RDFS_SUBCLASS_OF,
        "http://ex/Animal",
    );
    rel(&store, "", "http://ex/tom", RDF_TYPE, "http://ex/Cat");
    rel(&store, "", "http://ex/tom", RDF_TYPE, "http://ex/Animal");
    let stats = owl_rl::materialize(&store, true).unwrap();
    assert_eq!(stats.asserted, 0);
}

#[test]
fn inferred_graphs_inherit_readers_of_their_source() {
    let (store, _d) = open_store();
    rel(
        &store,
        "urn:g1",
        "http://ex/Cat",
        RDFS_SUBCLASS_OF,
        "http://ex/Animal",
    );
    rel(&store, "urn:g1", "http://ex/tom", RDF_TYPE, "http://ex/Cat");
    rel(
        &store,
        "urn:g1",
        "http://ex/a",
        "http://ex/near",
        "http://ex/b",
    );
    rel(
        &store,
        "urn:g2",
        "http://ex/b",
        "http://ex/near",
        "http://ex/c",
    );
    rel(
        &store,
        "urn:g2",
        "http://ex/near",
        RDF_TYPE,
        OWL_TRANSITIVE_PROP,
    );
    owl_rl::materialize(&store, true).unwrap();

    let user = n("urn:user:u");
    let g1 = store.graph_id("urn:g1").unwrap();
    store
        .grant_graph_access(user, g1, GraphAccessLevel::Read)
        .unwrap();
    let index = GraphAccessIndex::build(&store).unwrap();
    let readable = index.for_user(&user).readable();
    let inferred_g1 = store.graph_id("urn:pg:inferred:urn:g1").unwrap();
    let cross = store.graph_id(owl_rl::INFERRED_CROSS_GRAPH_IRI).unwrap();
    assert!(
        readable.contains(inferred_g1.0),
        "companion of a readable graph"
    );
    assert!(!readable.contains(cross.0), "cross graph is service-only");
    let inferred_g2 = store.graph_id("urn:pg:inferred:urn:g2");
    assert!(inferred_g2.map_or(true, |g| !readable.contains(g.0)));
}
