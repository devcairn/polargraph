//! Query planner — index selection for triple patterns.
//!
//! A `Pattern` describes which slots are bound (known) and which are free.
//! The planner maps each of the 8 possible bind combinations to the cheapest
//! column family and returns an `IndexChoice` the evaluator uses to drive the
//! actual RocksDB scan.
//!
//! # Index selection table
//!
//! | S | P | O | Chosen index | Why |
//! |---|---|---|--------------|-----|
//! | ✓ | ✓ | ✓ | SPO exact    | point lookup |
//! | ✓ | ✓ | - | SPO prefix   | (S,P) prefix is 20 bytes |
//! | ✓ | - | ✓ | SOP prefix   | (S,O) 32-byte prefix |
//! | ✓ | - | - | SPO prefix   | 16-byte subject prefix |
//! | - | ✓ | ✓ | POS prefix   | (P,O) is 20 bytes |
//! | - | ✓ | - | PSO prefix   | 4-byte predicate prefix |
//! | - | - | ✓ | OSP prefix   | 16-byte object prefix |
//! | - | - | - | SPO full scan| no constraint; O(n) |
//!
//! The (S,?,O) case uses the SOP CF with a 32-byte (subject, object) prefix,
//! returning all predicates between the given subject and object.

use polargraph_core::id::NodeId;
use serde::{Deserialize, Serialize};

// ── Pattern ───────────────────────────────────────────────────────────────────

/// A triple pattern: `None` in any slot means "match anything".
///
/// Object is always treated as a `NodeId` in the pattern — property triples
/// (where the object is a scalar `Value`) are matched by leaving the object
/// slot unbound. The evaluator returns both relation and property triples
/// whenever the object is `None`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pattern {
    pub subject: Option<NodeId>,
    pub predicate: Option<String>,
    pub object: Option<NodeId>,
}

impl Pattern {
    pub fn new() -> Self {
        Self {
            subject: None,
            predicate: None,
            object: None,
        }
    }

    pub fn with_subject(mut self, s: NodeId) -> Self {
        self.subject = Some(s);
        self
    }

    pub fn with_predicate(mut self, p: impl Into<String>) -> Self {
        self.predicate = Some(p.into());
        self
    }

    pub fn with_object(mut self, o: NodeId) -> Self {
        self.object = Some(o);
        self
    }

    /// Returns the bind mask as (subject_bound, predicate_bound, object_bound).
    pub fn bind_mask(&self) -> (bool, bool, bool) {
        (
            self.subject.is_some(),
            self.predicate.is_some(),
            self.object.is_some(),
        )
    }
}

impl Default for Pattern {
    fn default() -> Self {
        Self::new()
    }
}

// ── SchemaHints ───────────────────────────────────────────────────────────────

/// Domain/range type constraints derived from the `EdgeTypeRegistry` for a
/// pattern with a bound predicate.
///
/// When a predicate has a registered schema with `domain` or `range` set, the
/// evaluator can short-circuit the hexastore scan if the bound subject/object
/// node does not carry the expected `__type` property.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaHints {
    /// Expected `__type` of the subject (domain constraint). `None` = unconstrained.
    pub domain: Option<String>,
    /// Expected `__type` of the object (range constraint). `None` = unconstrained.
    pub range: Option<String>,
}

// ── IndexChoice ───────────────────────────────────────────────────────────────

/// Which storage scan the evaluator should perform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexChoice {
    /// SPO exact lookup: all three slots bound.
    SpoExact {
        subject: NodeId,
        predicate: String,
        object: NodeId,
    },
    /// SPO prefix on (subject, predicate).
    SpoPrefixSP { subject: NodeId, predicate: String },
    /// SPO prefix on subject only (object is unbound).
    SpoPrefixS { subject: NodeId },
    /// SOP prefix on (subject, object) — all predicates between S and O.
    SopPrefixSO { subject: NodeId, object: NodeId },
    /// POS prefix on (predicate, object).
    PosPrefixPO { predicate: String, object: NodeId },
    /// PSO prefix on predicate.
    PsoPrefixP { predicate: String },
    /// OSP prefix on object.
    OspPrefixO { object: NodeId },
    /// Full scan of SPO — no constraints. Use sparingly.
    FullScan,
}

// ── Planner ───────────────────────────────────────────────────────────────────

/// Choose the best index for a pattern. Pure function — no I/O.
pub fn choose_index(pattern: &Pattern) -> IndexChoice {
    match pattern.bind_mask() {
        // All three bound — exact lookup.
        (true, true, true) => IndexChoice::SpoExact {
            subject: pattern.subject.unwrap(),
            predicate: pattern.predicate.clone().unwrap(),
            object: pattern.object.unwrap(),
        },

        // S + P bound, O free — SPO prefix on (S,P).
        (true, true, false) => IndexChoice::SpoPrefixSP {
            subject: pattern.subject.unwrap(),
            predicate: pattern.predicate.clone().unwrap(),
        },

        // S bound, P free, O bound — SOP prefix on (S, O).
        (true, false, true) => IndexChoice::SopPrefixSO {
            subject: pattern.subject.unwrap(),
            object: pattern.object.unwrap(),
        },

        // S bound only.
        (true, false, false) => IndexChoice::SpoPrefixS {
            subject: pattern.subject.unwrap(),
        },

        // P + O bound — POS gives us all subjects pointing at this (P,O).
        (false, true, true) => IndexChoice::PosPrefixPO {
            predicate: pattern.predicate.clone().unwrap(),
            object: pattern.object.unwrap(),
        },

        // P bound only.
        (false, true, false) => IndexChoice::PsoPrefixP {
            predicate: pattern.predicate.clone().unwrap(),
        },

        // O bound only.
        (false, false, true) => IndexChoice::OspPrefixO {
            object: pattern.object.unwrap(),
        },

        // Nothing bound — full scan.
        (false, false, false) => IndexChoice::FullScan,
    }
}

// ── Annotation patterns and index selection ───────────────────────────────────

/// A pattern for querying edge annotation CFs (EPA/PEA/EPO).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnotationPattern {
    pub edge: Option<polargraph_core::id::EdgeId>,
    pub predicate: Option<String>,
}

impl AnnotationPattern {
    pub fn new() -> Self {
        Self {
            edge: None,
            predicate: None,
        }
    }
    pub fn with_edge(mut self, e: polargraph_core::id::EdgeId) -> Self {
        self.edge = Some(e);
        self
    }
    pub fn with_predicate(mut self, p: impl Into<String>) -> Self {
        self.predicate = Some(p.into());
        self
    }
}

impl Default for AnnotationPattern {
    fn default() -> Self {
        Self::new()
    }
}

/// Index choice for annotation CF scans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnnotationIndexChoice {
    /// EPA scan by edge_id (all predicates on that edge).
    EpaByEdge { edge: polargraph_core::id::EdgeId },
    /// EPA scan by (edge_id, predicate) — point lookup.
    EpaByEdgePred {
        edge: polargraph_core::id::EdgeId,
        predicate: String,
    },
    /// PEA scan by predicate (all edges with that annotation).
    PeaByPred { predicate: String },
    /// Full EPA scan — avoid; no useful prefix.
    EpaFull,
}

/// Choose the cheapest annotation index for a pattern.
pub fn choose_annotation_index(pattern: &AnnotationPattern) -> AnnotationIndexChoice {
    match (pattern.edge.is_some(), pattern.predicate.is_some()) {
        (true, true) => AnnotationIndexChoice::EpaByEdgePred {
            edge: pattern.edge.unwrap(),
            predicate: pattern.predicate.clone().unwrap(),
        },
        (true, false) => AnnotationIndexChoice::EpaByEdge {
            edge: pattern.edge.unwrap(),
        },
        (false, true) => AnnotationIndexChoice::PeaByPred {
            predicate: pattern.predicate.clone().unwrap(),
        },
        (false, false) => AnnotationIndexChoice::EpaFull,
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use polargraph_core::id::NodeId;
    use uuid::Uuid;

    fn node(seed: u8) -> NodeId {
        NodeId(Uuid::from_bytes([seed; 16]))
    }

    #[test]
    fn all_bound_chooses_spo_exact() {
        let s = node(1);
        let o = node(2);
        let p = Pattern::new()
            .with_subject(s)
            .with_predicate("knows")
            .with_object(o);
        assert!(matches!(choose_index(&p), IndexChoice::SpoExact { .. }));
    }

    #[test]
    fn s_p_bound_chooses_spo_prefix_sp() {
        let s = node(1);
        let p = Pattern::new().with_subject(s).with_predicate("knows");
        assert!(matches!(choose_index(&p), IndexChoice::SpoPrefixSP { .. }));
    }

    #[test]
    fn s_o_bound_chooses_sop_prefix_so() {
        let s = node(1);
        let o = node(2);
        let p = Pattern::new().with_subject(s).with_object(o);
        match choose_index(&p) {
            IndexChoice::SopPrefixSO { subject, object } => {
                assert_eq!(subject, s);
                assert_eq!(object, o);
            }
            other => panic!("expected SopPrefixSO, got {other:?}"),
        }
    }

    #[test]
    fn s_only_chooses_spo_prefix_s() {
        let s = node(1);
        let p = Pattern::new().with_subject(s);
        match choose_index(&p) {
            IndexChoice::SpoPrefixS { subject } => assert_eq!(subject, s),
            other => panic!("expected SpoPrefixS, got {other:?}"),
        }
    }

    #[test]
    fn p_o_bound_chooses_pos_prefix() {
        let o = node(2);
        let p = Pattern::new().with_predicate("knows").with_object(o);
        assert!(matches!(choose_index(&p), IndexChoice::PosPrefixPO { .. }));
    }

    #[test]
    fn p_only_chooses_pso_prefix() {
        let p = Pattern::new().with_predicate("knows");
        assert!(matches!(choose_index(&p), IndexChoice::PsoPrefixP { .. }));
    }

    #[test]
    fn o_only_chooses_osp_prefix() {
        let o = node(2);
        let p = Pattern::new().with_object(o);
        assert!(matches!(choose_index(&p), IndexChoice::OspPrefixO { .. }));
    }

    #[test]
    fn nothing_bound_chooses_full_scan() {
        let p = Pattern::new();
        assert!(matches!(choose_index(&p), IndexChoice::FullScan));
    }

    #[test]
    fn bind_mask_reflects_bound_slots() {
        let s = node(1);
        let o = node(2);
        assert_eq!(Pattern::new().bind_mask(), (false, false, false));
        assert_eq!(
            Pattern::new().with_subject(s).bind_mask(),
            (true, false, false)
        );
        assert_eq!(
            Pattern::new()
                .with_subject(s)
                .with_predicate("p")
                .with_object(o)
                .bind_mask(),
            (true, true, true)
        );
    }
}
