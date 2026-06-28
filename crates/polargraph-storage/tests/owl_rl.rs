//! Integration tests for the OWL 2 RL forward-chaining materializer.
//!
//! Well-known IRI constants mirror those in `owl_rl.rs`.

use polargraph_core::{
    id::{EdgeId, NodeId},
    temporal::{BiTemporalRange, Timestamp},
    triple::{Predicate, Triple},
};
use polargraph_storage::{owl_rl, TripleStore};
use tempfile::TempDir;

// ── well-known IRIs ───────────────────────────────────────────────────────────

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDFS_DOMAIN: &str = "http://www.w3.org/2000/01/rdf-schema#domain";
const RDFS_RANGE: &str = "http://www.w3.org/2000/01/rdf-schema#range";
const RDFS_SUBCLASS_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
#[allow(dead_code)]
const RDFS_SUBPROP_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subPropertyOf";
const OWL_INVERSE_OF: &str = "http://www.w3.org/2002/07/owl#inverseOf";
const OWL_SAME_AS: &str = "http://www.w3.org/2002/07/owl#sameAs";
const OWL_SYMMETRIC_PROP: &str = "http://www.w3.org/2002/07/owl#SymmetricProperty";

// ── helpers ───────────────────────────────────────────────────────────────────

fn open_store() -> (TripleStore, TempDir) {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    (store, dir)
}

fn temporal() -> BiTemporalRange {
    BiTemporalRange::assert_now(Timestamp::now())
}

fn insert_relation(store: &TripleStore, subject: NodeId, pred: &str, object: NodeId) {
    let t = Triple::Relation {
        subject,
        predicate: Predicate::new(pred),
        object,
        edge_id: EdgeId::new(),
        temporal: temporal(),
    };
    let mut tx = store.begin();
    tx.insert(t);
    tx.commit().unwrap();
}

/// Check whether the DRV CF contains a specific (subject, pred, object) triple.
fn derived_contains(store: &TripleStore, s: NodeId, pred: &str, o: NodeId) -> bool {
    let derived = store.scan_derived().unwrap();
    derived.iter().any(|t| {
        if let Triple::Relation {
            subject,
            predicate,
            object,
            ..
        } = t
        {
            *subject == s && predicate.0 == pred && *object == o
        } else {
            false
        }
    })
}

// ── rdfs9: subClassOf → type inheritance ─────────────────────────────────────

#[test]
fn rdfs9_subclass_type_propagation() {
    let (store, _dir) = open_store();

    let dog_class = NodeId::new();
    let animal_class = NodeId::new();
    let fido = NodeId::new();

    // Dog rdfs:subClassOf Animal
    insert_relation(&store, dog_class, RDFS_SUBCLASS_OF, animal_class);
    // Fido rdf:type Dog
    insert_relation(&store, fido, RDF_TYPE, dog_class);

    let stats = owl_rl::materialize(&store, true).unwrap();

    // Should infer: Fido rdf:type Animal
    assert!(
        derived_contains(&store, fido, RDF_TYPE, animal_class),
        "rdfs9 should infer fido:type:Animal; stats={:?}",
        stats
    );
    assert!(stats.rules_fired > 0);
    assert!(stats.derived_triples > 0);
}

// ── rdfs11: subClassOf transitivity ──────────────────────────────────────────

#[test]
fn rdfs11_subclass_transitivity() {
    let (store, _dir) = open_store();

    let poodle = NodeId::new();
    let dog = NodeId::new();
    let animal = NodeId::new();

    // Poodle subClassOf Dog, Dog subClassOf Animal
    insert_relation(&store, poodle, RDFS_SUBCLASS_OF, dog);
    insert_relation(&store, dog, RDFS_SUBCLASS_OF, animal);

    owl_rl::materialize(&store, true).unwrap();

    // Should infer: Poodle subClassOf Animal
    assert!(
        derived_contains(&store, poodle, RDFS_SUBCLASS_OF, animal),
        "rdfs11 should infer poodle subClassOf animal"
    );
}

// ── rdfs2: domain constraint → type ──────────────────────────────────────────

#[test]
fn rdfs2_domain_type_inference() {
    let (store, _dir) = open_store();

    let alice = NodeId::new();
    let bob = NodeId::new();
    let person_class = NodeId::new();

    // Use the bridge: the predicate "knows" maps to its NodeId for schema triples
    let knows_node = owl_rl::predicate_node("knows");

    // knows rdfs:domain Person
    insert_relation(&store, knows_node, RDFS_DOMAIN, person_class);
    // Alice knows Bob
    insert_relation(&store, alice, "knows", bob);

    owl_rl::materialize(&store, true).unwrap();

    // Should infer: Alice rdf:type Person
    assert!(
        derived_contains(&store, alice, RDF_TYPE, person_class),
        "rdfs2 should infer alice:type:Person from domain constraint"
    );
}

// ── rdfs3: range constraint → type ───────────────────────────────────────────

#[test]
fn rdfs3_range_type_inference() {
    let (store, _dir) = open_store();

    let alice = NodeId::new();
    let bob = NodeId::new();
    let person_class = NodeId::new();

    let knows_node = owl_rl::predicate_node("knows");

    // knows rdfs:range Person
    insert_relation(&store, knows_node, RDFS_RANGE, person_class);
    // Alice knows Bob
    insert_relation(&store, alice, "knows", bob);

    owl_rl::materialize(&store, true).unwrap();

    // Should infer: Bob rdf:type Person (the *object* gets typed by rdfs:range)
    assert!(
        derived_contains(&store, bob, RDF_TYPE, person_class),
        "rdfs3 should infer bob:type:Person from range constraint"
    );
}

// ── prp-symp: SymmetricProperty ──────────────────────────────────────────────

#[test]
fn prp_symp_symmetric_property() {
    let (store, _dir) = open_store();

    let alice = NodeId::new();
    let bob = NodeId::new();

    let likes_node = owl_rl::predicate_node("likes");
    let owl_sym_node = owl_rl::predicate_node(OWL_SYMMETRIC_PROP);

    // likes rdf:type owl:SymmetricProperty
    // owl_symmetric_node is uri_to_node_id(OWL_SYMMETRIC_PROP)
    let sym_prop_class = owl_rl::predicate_node(OWL_SYMMETRIC_PROP);
    insert_relation(&store, likes_node, RDF_TYPE, sym_prop_class);
    let _ = owl_sym_node; // used to make sure it's the same as sym_prop_class

    // Alice likes Bob
    insert_relation(&store, alice, "likes", bob);

    owl_rl::materialize(&store, true).unwrap();

    // Should infer: Bob likes Alice
    assert!(
        derived_contains(&store, bob, "likes", alice),
        "prp-symp should infer bob likes alice"
    );
}

// ── prp-inv1: inverseOf ───────────────────────────────────────────────────────

#[test]
fn prp_inv1_inverse_of() {
    let (store, _dir) = open_store();

    let alice = NodeId::new();
    let bob = NodeId::new();
    let dummy = NodeId::new();

    let knows_node = owl_rl::predicate_node("knows");
    let known_by_node = owl_rl::predicate_node("knownBy");

    // Pre-intern "knownBy" by using it as an actual predicate at least once.
    // The materializer can only infer triples using predicates that are interned
    // (bridge is built from intern table). A dummy self-loop achieves this without
    // affecting the test assertion.
    insert_relation(&store, dummy, "knownBy", dummy);

    // knows owl:inverseOf knownBy
    insert_relation(&store, knows_node, OWL_INVERSE_OF, known_by_node);
    // Alice knows Bob
    insert_relation(&store, alice, "knows", bob);

    owl_rl::materialize(&store, true).unwrap();

    // Should infer: Bob knownBy Alice
    assert!(
        derived_contains(&store, bob, "knownBy", alice),
        "prp-inv1 should infer bob knownBy alice"
    );
}

// ── eq-sym: sameAs symmetry ───────────────────────────────────────────────────

#[test]
fn eq_sym_sameas_symmetry() {
    let (store, _dir) = open_store();

    let alice = NodeId::new();
    let alice2 = NodeId::new();

    // Alice owl:sameAs Alice2
    insert_relation(&store, alice, OWL_SAME_AS, alice2);

    owl_rl::materialize(&store, true).unwrap();

    // Should infer: Alice2 owl:sameAs Alice
    assert!(
        derived_contains(&store, alice2, OWL_SAME_AS, alice),
        "eq-sym should infer alice2 sameAs alice"
    );
}

// ── clear_derived: wipe DRV CF ────────────────────────────────────────────────

#[test]
fn clear_derived_removes_all_derived_triples() {
    let (store, _dir) = open_store();

    let alice = NodeId::new();
    let alice2 = NodeId::new();
    insert_relation(&store, alice, OWL_SAME_AS, alice2);

    // Run once — produces derived triples
    owl_rl::materialize(&store, true).unwrap();
    assert!(
        !store.scan_derived().unwrap().is_empty(),
        "DRV should have entries after first run"
    );

    // Clear + empty base → no derived triples remain
    store.clear_derived().unwrap();
    assert!(
        store.scan_derived().unwrap().is_empty(),
        "DRV should be empty after clear"
    );
}

// ── incremental: second run finds fixpoint immediately ────────────────────────

#[test]
fn incremental_run_reaches_fixpoint() {
    let (store, _dir) = open_store();

    let alice = NodeId::new();
    let alice2 = NodeId::new();
    insert_relation(&store, alice, OWL_SAME_AS, alice2);

    // Full run
    let stats1 = owl_rl::materialize(&store, true).unwrap();
    assert!(stats1.iterations > 0);

    // Incremental run: DRV already contains all inferred facts → 0 new facts
    let stats2 = owl_rl::materialize(&store, false).unwrap();
    assert_eq!(
        stats2.rules_fired, 0,
        "incremental run on converged state should fire 0 rules"
    );
    assert_eq!(stats2.iterations, 0);
}
