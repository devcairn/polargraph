//! Pattern evaluator — drives storage scans using the planner's index choice.
//!
//! `evaluate(pattern, snapshot)` is the primary entry point. It calls
//! `choose_index` to pick the right CF and delegates to the appropriate
//! `Snapshot::scan_*` method. Every bind pattern has a dedicated index path;
//! no residual post-filtering is required.
//!
//! The evaluator deliberately does not know about `View`s — that layer sits
//! above and is handled by `projection::apply_view`.

use crate::planner::{
    choose_annotation_index, choose_index, AnnotationIndexChoice, AnnotationPattern, IndexChoice,
    Pattern,
};
use polargraph_core::{id::NodeId, triple::Triple, value::Value};
use polargraph_storage::{EdgeTypeRegistry, Snapshot, StorageError};
use tracing::warn;

/// Evaluate a triple pattern against a snapshot.
///
/// Returns all triples matching the pattern, visible at the snapshot's
/// timestamp. Relation triples and property triples are both returned when
/// the object slot is unbound.
pub fn evaluate(pattern: &Pattern, snapshot: &Snapshot) -> Result<Vec<Triple>, StorageError> {
    let choice = choose_index(pattern);
    execute(choice, snapshot)
}

/// Evaluate a pattern with optional schema-aware pruning.
///
/// When `registry` is `Some` and the pattern has a bound predicate whose
/// `EdgeTypeDef` carries `domain` or `range` constraints, the evaluator first
/// checks whether the bound subject/object node satisfies the type requirement
/// (`__type` property). If not, it returns an empty result immediately,
/// avoiding the hexastore scan entirely.
///
/// When `registry` is `None`, this is identical to [`evaluate`].
pub fn evaluate_with_registry(
    pattern: &Pattern,
    snapshot: &Snapshot,
    registry: Option<&EdgeTypeRegistry>,
) -> Result<Vec<Triple>, StorageError> {
    if let (Some(reg), Some(pred)) = (registry, &pattern.predicate) {
        if let Some(def) = reg.get_edge_type(pred) {
            if let (Some(domain), Some(subj)) = (&def.domain, pattern.subject) {
                if !node_has_type(subj, domain, snapshot)? {
                    return Ok(vec![]);
                }
            }
            if let (Some(range), Some(obj)) = (&def.range, pattern.object) {
                if !node_has_type(obj, range, snapshot)? {
                    return Ok(vec![]);
                }
            }
        }
    }
    evaluate(pattern, snapshot)
}

/// Returns `true` if `node` has a `__type` property equal to `expected_type`.
fn node_has_type(
    node: NodeId,
    expected_type: &str,
    snapshot: &Snapshot,
) -> Result<bool, StorageError> {
    let triples = snapshot.scan_by_subject_predicate(&node, "__type")?;
    Ok(triples.iter().any(|t| {
        matches!(
            t,
            Triple::Property { value: Value::Text(v), .. } if v == expected_type
        )
    }))
}

/// Evaluate a pattern against storage, then overlay pending (uncommitted)
/// triples from an open transaction to implement write-your-own-reads.
///
/// Pending triples that match the pattern and are not already represented in
/// the storage result are appended. Deduplication key: (subject, predicate)
/// for property triples and (subject, predicate, object) for relations.
pub fn evaluate_with_overlay(
    pattern: &Pattern,
    snapshot: &Snapshot,
    pending: &[Triple],
    registry: Option<&EdgeTypeRegistry>,
) -> Result<Vec<Triple>, StorageError> {
    let mut results = evaluate_with_registry(pattern, snapshot, registry)?;

    for triple in pending {
        if triple_matches_pattern(triple, pattern) && !results_contain_key(&results, triple) {
            results.push(triple.clone());
        }
    }

    Ok(results)
}

/// Returns `true` when `triple` satisfies every bound constraint in `pattern`.
fn triple_matches_pattern(triple: &Triple, pattern: &Pattern) -> bool {
    if let Some(s) = pattern.subject {
        if triple.subject() != s {
            return false;
        }
    }
    if let Some(pred) = &pattern.predicate {
        if triple.predicate().0 != *pred {
            return false;
        }
    }
    if let Some(obj) = pattern.object {
        match triple {
            Triple::Relation { object, .. } => {
                if *object != obj {
                    return false;
                }
            }
            // Property/annotation triples carry a scalar value, not a NodeId object.
            Triple::Property { .. } | Triple::EdgeProperty { .. } | Triple::EdgeRelation { .. } => {
                return false
            }
        }
    }
    true
}

/// Returns `true` when `results` already contains a triple with the same
/// logical key as `candidate`:
/// - Relation: (subject, predicate, object)
/// - Property: (subject, predicate)
fn results_contain_key(results: &[Triple], candidate: &Triple) -> bool {
    results.iter().any(|r| match (r, candidate) {
        (
            Triple::Relation {
                subject: rs,
                predicate: rp,
                object: ro,
                ..
            },
            Triple::Relation {
                subject: cs,
                predicate: cp,
                object: co,
                ..
            },
        ) => rs == cs && rp.0 == cp.0 && ro == co,
        (
            Triple::Property {
                subject: rs,
                predicate: rp,
                ..
            },
            Triple::Property {
                subject: cs,
                predicate: cp,
                ..
            },
        ) => rs == cs && rp.0 == cp.0,
        _ => false,
    })
}

/// Evaluate an annotation pattern against a snapshot.
///
/// Routes to EPA (by edge), EPA (by edge+pred), or PEA (by pred) based on
/// which slots are bound.
pub fn evaluate_annotations(
    pattern: &AnnotationPattern,
    snapshot: &Snapshot,
) -> Result<Vec<Triple>, StorageError> {
    match choose_annotation_index(pattern) {
        AnnotationIndexChoice::EpaByEdge { edge } => {
            snapshot.scan_edge_annotations_as_triples(edge)
        }
        AnnotationIndexChoice::EpaByEdgePred { edge, predicate } => {
            // Scan edge annotations and filter to the requested predicate.
            let all = snapshot.scan_edge_annotations_as_triples(edge)?;
            Ok(all
                .into_iter()
                .filter(|t| t.predicate().0 == predicate)
                .collect())
        }
        AnnotationIndexChoice::PeaByPred { predicate } => {
            snapshot.scan_annotations_by_predicate(&predicate)
        }
        AnnotationIndexChoice::EpaFull => {
            // Full annotation scan is not supported — return empty.
            Ok(vec![])
        }
    }
}

fn execute(choice: IndexChoice, snap: &Snapshot) -> Result<Vec<Triple>, StorageError> {
    match choice {
        // ── exact lookup ──────────────────────────────────────────────────────
        IndexChoice::SpoExact {
            subject,
            predicate,
            object,
        } => {
            // Use SP prefix scan then filter to exact object.
            let candidates = snap.scan_by_subject_predicate(&subject, &predicate)?;
            Ok(candidates
                .into_iter()
                .filter(|t| object_matches(t, &object))
                .collect())
        }

        // ── SPO prefix (S, P) ─────────────────────────────────────────────────
        IndexChoice::SpoPrefixSP { subject, predicate } => {
            snap.scan_by_subject_predicate(&subject, &predicate)
        }

        // ── SPO prefix (S) — object unbound ──────────────────────────────────
        IndexChoice::SpoPrefixS { subject } => snap.scan_by_subject(&subject),

        // ── SOP prefix (S, O) ─────────────────────────────────────────────────
        IndexChoice::SopPrefixSO { subject, object } => {
            snap.scan_by_subject_object(&subject, &object)
        }

        // ── POS prefix (P, O) ─────────────────────────────────────────────────
        IndexChoice::PosPrefixPO { predicate, object } => {
            snap.scan_by_predicate_object(&predicate, &object)
        }

        // ── PSO prefix (P) ────────────────────────────────────────────────────
        IndexChoice::PsoPrefixP { predicate } => snap.scan_by_predicate(&predicate),

        // ── OSP prefix (O) ────────────────────────────────────────────────────
        IndexChoice::OspPrefixO { object } => snap.scan_by_object(&object),

        // ── Full scan ─────────────────────────────────────────────────────────
        IndexChoice::FullScan => {
            // Full scans are valid but expensive. Log so callers notice.
            warn!("full triple scan requested — no pattern constraints");
            snap.scan_all()
        }
    }
}

/// Returns true if a triple's object slot matches `expected`.
///
/// Property triples never match a NodeId object — they carry a scalar value,
/// not a node reference.
fn object_matches(triple: &Triple, expected: &NodeId) -> bool {
    match triple {
        Triple::Relation { object, .. } => object == expected,
        Triple::Property { .. } | Triple::EdgeProperty { .. } | Triple::EdgeRelation { .. } => {
            false
        }
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use polargraph_core::{
        id::{EdgeId, NodeId},
        temporal::{BiTemporalRange, Timestamp},
        triple::{Predicate, Triple},
        value::Value,
    };
    use polargraph_storage::TripleStore;
    use tempfile::TempDir;

    fn open() -> (TripleStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let store = TripleStore::open(dir.path()).unwrap();
        (store, dir)
    }

    fn rel(from: NodeId, pred: &str, to: NodeId) -> Triple {
        Triple::Relation {
            subject: from,
            predicate: Predicate::new(pred),
            object: to,
            edge_id: EdgeId::new(),
            temporal: BiTemporalRange::assert_now(Timestamp::now()),
        }
    }

    fn prop(node: NodeId, pred: &str, val: impl Into<Value>) -> Triple {
        Triple::Property {
            subject: node,
            predicate: Predicate::new(pred),
            value: val.into(),
            temporal: BiTemporalRange::assert_now(Timestamp::now()),
        }
    }

    fn commit_and_snap(store: &TripleStore, triples: Vec<Triple>) -> Snapshot {
        let mut tx = store.begin();
        for t in triples {
            tx.insert(t);
        }
        let ts = tx.commit().unwrap();
        store.snapshot(ts)
    }

    // ── index selection exercised end-to-end ──────────────────────────────────

    #[test]
    fn subject_only_pattern() {
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();
        let carol = NodeId::new();

        let snap = commit_and_snap(
            &store,
            vec![
                rel(alice, "knows", bob),
                rel(alice, "manages", carol),
                rel(bob, "knows", carol), // different subject — should not appear
            ],
        );

        let results = evaluate(&Pattern::new().with_subject(alice), &snap).unwrap();

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|t| t.subject() == alice));
    }

    #[test]
    fn subject_predicate_pattern() {
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();
        let carol = NodeId::new();

        let snap = commit_and_snap(
            &store,
            vec![
                rel(alice, "knows", bob),
                rel(alice, "knows", carol),
                rel(alice, "manages", bob), // different predicate
            ],
        );

        let results = evaluate(
            &Pattern::new().with_subject(alice).with_predicate("knows"),
            &snap,
        )
        .unwrap();

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|t| t.predicate().0 == "knows"));
    }

    #[test]
    fn subject_predicate_object_exact() {
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();
        let carol = NodeId::new();

        let snap = commit_and_snap(
            &store,
            vec![rel(alice, "knows", bob), rel(alice, "knows", carol)],
        );

        let results = evaluate(
            &Pattern::new()
                .with_subject(alice)
                .with_predicate("knows")
                .with_object(bob),
            &snap,
        )
        .unwrap();

        assert_eq!(results.len(), 1);
        match &results[0] {
            Triple::Relation { object, .. } => assert_eq!(*object, bob),
            _ => panic!("expected relation"),
        }
    }

    #[test]
    fn predicate_only_pattern() {
        let (store, _dir) = open();
        let a = NodeId::new();
        let b = NodeId::new();
        let c = NodeId::new();

        let snap = commit_and_snap(
            &store,
            vec![rel(a, "knows", b), rel(c, "knows", b), rel(a, "manages", c)],
        );

        let results = evaluate(&Pattern::new().with_predicate("knows"), &snap).unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|t| t.predicate().0 == "knows"));
    }

    #[test]
    fn predicate_object_pattern() {
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();
        let carol = NodeId::new();

        let snap = commit_and_snap(
            &store,
            vec![
                rel(alice, "reports-to", bob),
                rel(carol, "reports-to", bob),
                rel(alice, "reports-to", carol), // different object
            ],
        );

        let results = evaluate(
            &Pattern::new().with_predicate("reports-to").with_object(bob),
            &snap,
        )
        .unwrap();

        assert_eq!(results.len(), 2);
        let subjects: Vec<NodeId> = results.iter().map(|t| t.subject()).collect();
        assert!(subjects.contains(&alice));
        assert!(subjects.contains(&carol));
    }

    #[test]
    fn object_only_pattern() {
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();
        let carol = NodeId::new();

        let snap = commit_and_snap(
            &store,
            vec![
                rel(alice, "knows", bob),
                rel(carol, "manages", bob),
                rel(alice, "knows", carol), // different object
            ],
        );

        let results = evaluate(&Pattern::new().with_object(bob), &snap).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn subject_object_pattern_uses_sop_index() {
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();
        let carol = NodeId::new();

        let snap = commit_and_snap(
            &store,
            vec![
                rel(alice, "knows", bob),
                rel(alice, "knows", carol),
                rel(alice, "manages", bob), // same object bob, different pred
            ],
        );

        // S=alice, O=bob: should return both "knows" and "manages" to bob
        let results =
            evaluate(&Pattern::new().with_subject(alice).with_object(bob), &snap).unwrap();

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|t| t.subject() == alice));
        // Carol-directed triple must not appear
        assert!(results
            .iter()
            .all(|t| matches!(t, Triple::Relation { object, .. } if *object == bob)));
    }

    #[test]
    fn full_scan_returns_all_triples() {
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();
        let carol = NodeId::new();

        let snap = commit_and_snap(
            &store,
            vec![
                rel(alice, "knows", bob),
                rel(bob, "knows", carol),
                rel(carol, "manages", alice),
            ],
        );

        let results = evaluate(&Pattern::new(), &snap).unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn property_triples_included_when_object_unbound() {
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();

        let snap = commit_and_snap(
            &store,
            vec![rel(alice, "knows", bob), prop(alice, "name", "Alice")],
        );

        // Subject-only — should return both relation and property.
        let results = evaluate(&Pattern::new().with_subject(alice), &snap).unwrap();
        assert_eq!(results.len(), 2);

        let has_relation = results.iter().any(|t| matches!(t, Triple::Relation { .. }));
        let has_property = results.iter().any(|t| matches!(t, Triple::Property { .. }));
        assert!(has_relation);
        assert!(has_property);
    }

    #[test]
    fn property_triples_excluded_when_object_bound() {
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();

        let snap = commit_and_snap(
            &store,
            vec![rel(alice, "knows", bob), prop(alice, "name", "Alice")],
        );

        // Object bound to a NodeId — property triples can't match.
        let results =
            evaluate(&Pattern::new().with_subject(alice).with_object(bob), &snap).unwrap();

        assert!(results.iter().all(|t| matches!(t, Triple::Relation { .. })));
    }

    #[test]
    fn no_match_returns_empty() {
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();
        let nobody = NodeId::new();

        let snap = commit_and_snap(&store, vec![rel(alice, "knows", bob)]);

        let results = evaluate(&Pattern::new().with_subject(nobody), &snap).unwrap();
        assert!(results.is_empty());
    }

    // ── schema-aware pruning ──────────────────────────────────────────────────

    fn open_with_edge_registry() -> (
        TripleStore,
        polargraph_storage::registry::EdgeTypeRegistry,
        TempDir,
    ) {
        let dir = TempDir::new().unwrap();
        let store = TripleStore::open(dir.path()).unwrap();
        let reg = polargraph_storage::registry::EdgeTypeRegistry::new(store.clone()).unwrap();
        (store, reg, dir)
    }

    #[test]
    fn typed_predicate_prunes_wrong_domain() {
        use polargraph_core::schema::EdgeTypeDef;

        let (store, reg, _dir) = open_with_edge_registry();

        // Register: works_at requires domain=Person, range=Company
        reg.register_edge_type(EdgeTypeDef::new(
            "works_at",
            Some("Person"),
            Some("Company"),
            vec![],
        ))
        .unwrap();

        let alice = NodeId::new();
        let acme = NodeId::new();

        // alice has __type=Robot (not Person) — works_at is constrained to Person subjects
        let snap = commit_and_snap(
            &store,
            vec![
                rel(alice, "works_at", acme),
                prop(alice, "__type", "Robot"),
                prop(acme, "__type", "Company"),
            ],
        );

        let pattern = Pattern::new()
            .with_subject(alice)
            .with_predicate("works_at");
        let results = evaluate_with_registry(&pattern, &snap, Some(&reg)).unwrap();

        // Domain mismatch: alice is Robot, not Person → pruned to empty
        assert!(
            results.is_empty(),
            "domain mismatch should prune the result set"
        );
    }

    #[test]
    fn typed_predicate_allows_correct_domain() {
        use polargraph_core::schema::EdgeTypeDef;

        let (store, reg, _dir) = open_with_edge_registry();

        reg.register_edge_type(EdgeTypeDef::new(
            "works_at",
            Some("Person"),
            Some("Company"),
            vec![],
        ))
        .unwrap();

        let alice = NodeId::new();
        let acme = NodeId::new();

        let snap = commit_and_snap(
            &store,
            vec![
                rel(alice, "works_at", acme),
                prop(alice, "__type", "Person"),
                prop(acme, "__type", "Company"),
            ],
        );

        let pattern = Pattern::new()
            .with_subject(alice)
            .with_predicate("works_at");
        let results = evaluate_with_registry(&pattern, &snap, Some(&reg)).unwrap();

        assert_eq!(
            results.len(),
            1,
            "correct domain should not prune the result"
        );
    }

    #[test]
    fn typed_predicate_prunes_wrong_range() {
        use polargraph_core::schema::EdgeTypeDef;

        let (store, reg, _dir) = open_with_edge_registry();

        reg.register_edge_type(EdgeTypeDef::new(
            "works_at",
            Some("Person"),
            Some("Company"),
            vec![],
        ))
        .unwrap();

        let alice = NodeId::new();
        let acme = NodeId::new();

        // acme has __type=Project (not Company)
        let snap = commit_and_snap(
            &store,
            vec![
                rel(alice, "works_at", acme),
                prop(alice, "__type", "Person"),
                prop(acme, "__type", "Project"),
            ],
        );

        let pattern = Pattern::new()
            .with_subject(alice)
            .with_predicate("works_at")
            .with_object(acme);
        let results = evaluate_with_registry(&pattern, &snap, Some(&reg)).unwrap();

        assert!(
            results.is_empty(),
            "range mismatch should prune the result set"
        );
    }

    #[test]
    fn untyped_predicate_unaffected_by_registry() {
        use polargraph_core::schema::EdgeTypeDef;

        let (store, reg, _dir) = open_with_edge_registry();

        // Register works_at, but query over "knows" which has no schema
        reg.register_edge_type(EdgeTypeDef::new(
            "works_at",
            Some("Person"),
            Some("Company"),
            vec![],
        ))
        .unwrap();

        let alice = NodeId::new();
        let bob = NodeId::new();

        let snap = commit_and_snap(&store, vec![rel(alice, "knows", bob)]);

        let pattern = Pattern::new().with_subject(alice).with_predicate("knows");
        let results = evaluate_with_registry(&pattern, &snap, Some(&reg)).unwrap();

        // "knows" has no schema → no pruning → normal result
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn no_registry_behaves_identically_to_evaluate() {
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();

        let snap = commit_and_snap(&store, vec![rel(alice, "knows", bob)]);
        let pattern = Pattern::new().with_subject(alice).with_predicate("knows");

        let a = evaluate(&pattern, &snap).unwrap();
        let b = evaluate_with_registry(&pattern, &snap, None).unwrap();
        assert_eq!(a.len(), b.len());
    }
}
