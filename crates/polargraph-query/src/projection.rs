//! View projection engine.
//!
//! Takes raw triples from the evaluator and applies a `View` lens:
//!
//! - **Predicate filtering**: triples whose predicate is not in the view's
//!   `visible_predicates` are dropped. If `visible_predicates` is empty the
//!   view shows everything (no filter).
//!
//! - **Label remapping**: `display_label` is the string callers should render.
//!   It comes from `view.edge_presentations` if an override exists, otherwise
//!   it's the canonical predicate name.
//!
//! - **Direction hints**: `display_reversed` tells the rendering layer to flip
//!   the arrow. The underlying edge is unchanged; this is purely a visual hint.
//!
//! Property triples (scalar values, no object node) pass through the predicate
//! filter but always get `display_reversed = false` since they have no
//! directionality in the graph sense.

use polargraph_core::{triple::Triple, view::View};
use serde::{Deserialize, Serialize};

/// A triple with view-specific display metadata layered on top.
///
/// The canonical `triple` is always preserved so callers can access the real
/// predicate, subject, object, and temporal data. The `display_*` fields are
/// for rendering only.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectedTriple {
    /// The original triple exactly as stored.
    pub triple: Triple,
    /// Label to show the user for this edge in the current view.
    pub display_label: String,
    /// If true, the rendering layer should flip the arrow direction.
    pub display_reversed: bool,
}

impl ProjectedTriple {
    /// Convenience: the canonical predicate string.
    pub fn canonical_predicate(&self) -> &str {
        self.triple.predicate().0.as_str()
    }
}

/// Apply `view` to a set of triples, returning only the visible ones with
/// display metadata attached.
///
/// This is a pure function — no side effects, no I/O.
pub fn apply_view(view: &View, triples: Vec<Triple>) -> Vec<ProjectedTriple> {
    triples
        .into_iter()
        .filter(|t| view.shows_predicate(&t.predicate().0))
        .map(|t| {
            let pred = t.predicate().0.clone();
            let display_label = view.edge_label(&pred).to_owned();
            let display_reversed = match &t {
                Triple::Relation { .. } => view.is_reversed(&pred),
                Triple::Property { .. } | Triple::EdgeProperty { .. } | Triple::EdgeRelation { .. } => false,
            };
            ProjectedTriple { triple: t, display_label, display_reversed }
        })
        .collect()
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
        view::{EdgePresentation, View},
    };
    use std::collections::HashSet;

    fn temporal() -> BiTemporalRange {
        BiTemporalRange::assert_now(Timestamp::now())
    }

    fn rel(from: NodeId, pred: &str, to: NodeId) -> Triple {
        Triple::Relation {
            subject: from,
            predicate: Predicate::new(pred),
            object: to,
            edge_id: EdgeId::new(),
            temporal: temporal(),
        }
    }

    fn prop(node: NodeId, pred: &str) -> Triple {
        Triple::Property {
            subject: node,
            predicate: Predicate::new(pred),
            value: Value::Text("x".into()),
            temporal: temporal(),
        }
    }

    fn make_view(visible: &[&str]) -> View {
        let mut v = View::new("test", "Test");
        v.visible_predicates = visible.iter().map(|s| s.to_string()).collect();
        v
    }

    fn make_view_with_label(pred: &str, label: &str, reversed: bool) -> View {
        let mut v = View::new("test", "Test");
        v.edge_presentations.insert(
            pred.into(),
            EdgePresentation { label: label.into(), reverse_direction: reversed },
        );
        v
    }

    // ── predicate filtering ───────────────────────────────────────────────────

    #[test]
    fn visible_predicate_passes_through() {
        let a = NodeId::new();
        let b = NodeId::new();
        let view = make_view(&["knows"]);

        let result = apply_view(&view, vec![rel(a, "knows", b)]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].canonical_predicate(), "knows");
    }

    #[test]
    fn hidden_predicate_is_filtered_out() {
        let a = NodeId::new();
        let b = NodeId::new();
        let view = make_view(&["knows"]);

        let result = apply_view(&view, vec![rel(a, "salary-band", b)]);
        assert!(result.is_empty());
    }

    #[test]
    fn mixed_predicates_filtered_correctly() {
        let a = NodeId::new();
        let b = NodeId::new();
        let view = make_view(&["knows", "name"]);

        let triples = vec![
            rel(a, "knows", b),
            rel(a, "manages", b),   // not in view
            prop(a, "name"),
            prop(a, "salary"),      // not in view
        ];

        let result = apply_view(&view, triples);
        assert_eq!(result.len(), 2);
        let labels: Vec<&str> = result.iter().map(|r| r.canonical_predicate()).collect();
        assert!(labels.contains(&"knows"));
        assert!(labels.contains(&"name"));
    }

    #[test]
    fn empty_visible_predicates_shows_all() {
        let a = NodeId::new();
        let b = NodeId::new();
        let view = make_view(&[]); // empty → show everything

        let triples = vec![
            rel(a, "knows", b),
            rel(a, "manages", b),
            prop(a, "name"),
        ];

        let result = apply_view(&view, triples);
        assert_eq!(result.len(), 3);
    }

    // ── label remapping ───────────────────────────────────────────────────────

    #[test]
    fn label_override_applied() {
        let a = NodeId::new();
        let b = NodeId::new();
        let view = make_view_with_label("reports-to", "manages", false);

        let result = apply_view(&view, vec![rel(a, "reports-to", b)]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].display_label, "manages");
        // Canonical predicate is unchanged.
        assert_eq!(result[0].canonical_predicate(), "reports-to");
    }

    #[test]
    fn label_falls_back_to_canonical_when_no_override() {
        let a = NodeId::new();
        let b = NodeId::new();
        let view = make_view(&[]);

        let result = apply_view(&view, vec![rel(a, "knows", b)]);
        assert_eq!(result[0].display_label, "knows");
    }

    // ── direction hints ───────────────────────────────────────────────────────

    #[test]
    fn reversed_flag_set_for_relation() {
        let a = NodeId::new();
        let b = NodeId::new();
        let view = make_view_with_label("reports-to", "manages", true);

        let result = apply_view(&view, vec![rel(a, "reports-to", b)]);
        assert!(result[0].display_reversed, "edge should be reversed");
    }

    #[test]
    fn reversed_flag_false_when_not_set() {
        let a = NodeId::new();
        let b = NodeId::new();
        let view = make_view(&[]);

        let result = apply_view(&view, vec![rel(a, "knows", b)]);
        assert!(!result[0].display_reversed);
    }

    #[test]
    fn property_triples_never_reversed() {
        let a = NodeId::new();
        let view = make_view_with_label("name", "display-name", true); // reversed=true set

        let result = apply_view(&view, vec![prop(a, "name")]);
        assert_eq!(result.len(), 1);
        // Property triples ignore the reversed flag.
        assert!(!result[0].display_reversed, "property triples are never reversed");
    }

    // ── projected triple accessors ────────────────────────────────────────────

    #[test]
    fn canonical_predicate_matches_stored_predicate() {
        let a = NodeId::new();
        let b = NodeId::new();
        let view = make_view_with_label("knows", "is-friend-of", false);

        let result = apply_view(&view, vec![rel(a, "knows", b)]);
        assert_eq!(result[0].canonical_predicate(), "knows");
        assert_eq!(result[0].display_label, "is-friend-of");
    }

    #[test]
    fn apply_view_preserves_triple_fields() {
        let a = NodeId::new();
        let b = NodeId::new();
        let eid = EdgeId::new();
        let triple = Triple::Relation {
            subject: a,
            predicate: Predicate::new("knows"),
            object: b,
            edge_id: eid,
            temporal: temporal(),
        };
        let view = make_view(&[]);

        let result = apply_view(&view, vec![triple]);
        assert_eq!(result.len(), 1);
        match &result[0].triple {
            Triple::Relation { subject, object, edge_id, .. } => {
                assert_eq!(*subject, a);
                assert_eq!(*object, b);
                assert_eq!(*edge_id, eid);
            }
            _ => panic!("expected relation"),
        }
    }

    #[test]
    fn empty_input_returns_empty() {
        let view = make_view(&["anything"]);
        assert!(apply_view(&view, vec![]).is_empty());
    }

    // ── multiple label overrides in one view ──────────────────────────────────

    #[test]
    fn multiple_overrides_in_same_view() {
        let a = NodeId::new();
        let b = NodeId::new();
        let c = NodeId::new();

        let mut view = View::new("org", "Org");
        view.edge_presentations.insert(
            "reports-to".into(),
            EdgePresentation { label: "manages".into(), reverse_direction: true },
        );
        view.edge_presentations.insert(
            "owns-project".into(),
            EdgePresentation { label: "responsible-for".into(), reverse_direction: false },
        );

        let triples = vec![
            rel(a, "reports-to", b),
            rel(a, "owns-project", c),
        ];

        let result = apply_view(&view, triples);
        assert_eq!(result.len(), 2);

        let reports = result.iter().find(|r| r.canonical_predicate() == "reports-to").unwrap();
        assert_eq!(reports.display_label, "manages");
        assert!(reports.display_reversed);

        let owns = result.iter().find(|r| r.canonical_predicate() == "owns-project").unwrap();
        assert_eq!(owns.display_label, "responsible-for");
        assert!(!owns.display_reversed);
    }
}
