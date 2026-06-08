//! Pattern evaluator — drives storage scans using the planner's index choice.
//!
//! `evaluate(pattern, snapshot)` is the primary entry point. It calls
//! `choose_index` to pick the right CF and delegates to the appropriate
//! `Snapshot::scan_*` method. Every bind pattern has a dedicated index path;
//! no residual post-filtering is required.
//!
//! The evaluator deliberately does not know about `View`s — that layer sits
//! above and is handled by `projection::apply_view`.

use crate::planner::{choose_index, IndexChoice, Pattern};
use polargraph_core::{id::NodeId, triple::Triple};
use polargraph_storage::{Snapshot, StorageError};
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

fn execute(choice: IndexChoice, snap: &Snapshot) -> Result<Vec<Triple>, StorageError> {
    match choice {
        // ── exact lookup ──────────────────────────────────────────────────────
        IndexChoice::SpoExact { subject, predicate, object } => {
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
        IndexChoice::SpoPrefixS { subject } => {
            snap.scan_by_subject(&subject)
        }

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
        Triple::Property { .. } => false,
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

        let snap = commit_and_snap(&store, vec![
            rel(alice, "knows", bob),
            rel(alice, "manages", carol),
            rel(bob, "knows", carol), // different subject — should not appear
        ]);

        let results = evaluate(
            &Pattern::new().with_subject(alice),
            &snap,
        ).unwrap();

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|t| t.subject() == alice));
    }

    #[test]
    fn subject_predicate_pattern() {
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();
        let carol = NodeId::new();

        let snap = commit_and_snap(&store, vec![
            rel(alice, "knows", bob),
            rel(alice, "knows", carol),
            rel(alice, "manages", bob), // different predicate
        ]);

        let results = evaluate(
            &Pattern::new().with_subject(alice).with_predicate("knows"),
            &snap,
        ).unwrap();

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|t| t.predicate().0 == "knows"));
    }

    #[test]
    fn subject_predicate_object_exact() {
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();
        let carol = NodeId::new();

        let snap = commit_and_snap(&store, vec![
            rel(alice, "knows", bob),
            rel(alice, "knows", carol),
        ]);

        let results = evaluate(
            &Pattern::new().with_subject(alice).with_predicate("knows").with_object(bob),
            &snap,
        ).unwrap();

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

        let snap = commit_and_snap(&store, vec![
            rel(a, "knows", b),
            rel(c, "knows", b),
            rel(a, "manages", c),
        ]);

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

        let snap = commit_and_snap(&store, vec![
            rel(alice, "reports-to", bob),
            rel(carol, "reports-to", bob),
            rel(alice, "reports-to", carol), // different object
        ]);

        let results = evaluate(
            &Pattern::new().with_predicate("reports-to").with_object(bob),
            &snap,
        ).unwrap();

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

        let snap = commit_and_snap(&store, vec![
            rel(alice, "knows", bob),
            rel(carol, "manages", bob),
            rel(alice, "knows", carol), // different object
        ]);

        let results = evaluate(&Pattern::new().with_object(bob), &snap).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn subject_object_pattern_uses_sop_index() {
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();
        let carol = NodeId::new();

        let snap = commit_and_snap(&store, vec![
            rel(alice, "knows", bob),
            rel(alice, "knows", carol),
            rel(alice, "manages", bob), // same object bob, different pred
        ]);

        // S=alice, O=bob: should return both "knows" and "manages" to bob
        let results = evaluate(
            &Pattern::new().with_subject(alice).with_object(bob),
            &snap,
        ).unwrap();

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|t| t.subject() == alice));
        // Carol-directed triple must not appear
        assert!(results.iter().all(|t| matches!(t, Triple::Relation { object, .. } if *object == bob)));
    }

    #[test]
    fn full_scan_returns_all_triples() {
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();
        let carol = NodeId::new();

        let snap = commit_and_snap(&store, vec![
            rel(alice, "knows", bob),
            rel(bob, "knows", carol),
            rel(carol, "manages", alice),
        ]);

        let results = evaluate(&Pattern::new(), &snap).unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn property_triples_included_when_object_unbound() {
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();

        let snap = commit_and_snap(&store, vec![
            rel(alice, "knows", bob),
            prop(alice, "name", "Alice"),
        ]);

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

        let snap = commit_and_snap(&store, vec![
            rel(alice, "knows", bob),
            prop(alice, "name", "Alice"),
        ]);

        // Object bound to a NodeId — property triples can't match.
        let results = evaluate(
            &Pattern::new().with_subject(alice).with_object(bob),
            &snap,
        ).unwrap();

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
}
