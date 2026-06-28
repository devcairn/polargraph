//! OWL 2 RL forward-chaining materializer.
//!
//! Implements a subset of the OWL 2 RL rule table (RDFS entailment + property
//! characteristic rules). Derived facts are written to the `DRV` column family.
//!
//! # Rules implemented
//!
//! | Name         | Antecedent                                                  | Consequent                 |
//! |--------------|-------------------------------------------------------------|----------------------------|
//! | rdfs2        | (?s ?p ?o), (?p rdfs:domain ?C)                             | (?s rdf:type ?C)           |
//! | rdfs3        | (?s ?p ?o), (?p rdfs:range ?C)                              | (?o rdf:type ?C)           |
//! | rdfs5        | (?p rdfs:subPropertyOf ?q), (?q rdfs:subPropertyOf ?r)      | (?p rdfs:subPropertyOf ?r) |
//! | rdfs7/spo1   | (?s ?p ?o), (?p rdfs:subPropertyOf ?q)                      | (?s ?q ?o)                 |
//! | rdfs9        | (?C rdfs:subClassOf ?D), (?s rdf:type ?C)                   | (?s rdf:type ?D)           |
//! | rdfs11       | (?C rdfs:subClassOf ?D), (?D rdfs:subClassOf ?E)            | (?C rdfs:subClassOf ?E)    |
//! | prp-symp     | (?p rdf:type owl:SymmetricProperty), (?s ?p ?o)             | (?o ?p ?s)                 |
//! | prp-trp      | (?p rdf:type owl:TransitiveProperty), (?s ?p ?m), (?m ?p ?o)| (?s ?p ?o)                 |
//! | prp-inv1     | (?p owl:inverseOf ?q), (?s ?p ?o)                          | (?o ?q ?s)                 |
//! | prp-inv2     | (?p owl:inverseOf ?q), (?s ?q ?o)                          | (?o ?p ?s)                 |
//! | eq-sym       | (?s owl:sameAs ?o)                                          | (?o owl:sameAs ?s)         |
//! | eq-trans     | (?s owl:sameAs ?m), (?m owl:sameAs ?o)                      | (?s owl:sameAs ?o)         |
//!
//! # Predicate bridge
//!
//! In OWL/RDFS, predicates are first-class resources that can appear as subjects
//! of schema triples (e.g., `knows rdfs:subPropertyOf relatedTo`). In PolarGraph
//! predicates are normally identified by their interned `PredId`; for schema
//! triples we need a stable `NodeId` to use in the subject slot.
//!
//! The bridge: for any interned predicate string `p`, its schema NodeId is
//! computed deterministically as `uri_to_node_id(p)` using xxHash3-128.
//! Importers that follow this convention (e.g. the N-Triples bulk importer) will
//! produce matching node IDs for predicate schema assertions.

use std::collections::{HashMap, HashSet};

use polargraph_core::{id::NodeId, triple::Triple};

use crate::{error::StorageError, keys::PredId, store::TripleStore};

// ── Well-known IRI constants ──────────────────────────────────────────────────

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDFS_DOMAIN: &str = "http://www.w3.org/2000/01/rdf-schema#domain";
const RDFS_RANGE: &str = "http://www.w3.org/2000/01/rdf-schema#range";
const RDFS_SUBCLASS_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
const RDFS_SUBPROP_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subPropertyOf";
const OWL_INVERSE_OF: &str = "http://www.w3.org/2002/07/owl#inverseOf";
const OWL_SAME_AS: &str = "http://www.w3.org/2002/07/owl#sameAs";
const OWL_SYMMETRIC_PROP: &str = "http://www.w3.org/2002/07/owl#SymmetricProperty";
const OWL_TRANSITIVE_PROP: &str = "http://www.w3.org/2002/07/owl#TransitiveProperty";

// ── Public types ──────────────────────────────────────────────────────────────

/// Statistics returned by a materialization run.
#[derive(Debug, Default, Clone)]
pub struct MaterializationStats {
    /// Number of new derived triples inserted across all iterations.
    pub rules_fired: u64,
    /// Total unique derived triples now in the DRV CF.
    pub derived_triples: u64,
    /// Number of fixpoint iterations performed.
    pub iterations: u32,
}

// ── NodeId for a string (xxHash3-128 → UUID) ─────────────────────────────────

/// Deterministic NodeId for a predicate or class URI.
///
/// Used to bridge between predicate strings and NodeId subjects in schema triples.
/// An importer using the same hash will produce the same NodeId for the same URI.
pub fn uri_to_node_id(uri: &str) -> NodeId {
    let hash: u128 = xxhash_rust::xxh3::xxh3_128(uri.as_bytes());
    NodeId(uuid::Uuid::from_u128(hash))
}

// ── In-memory triple indexes ──────────────────────────────────────────────────

/// Lightweight in-memory index of Relation triples for rule evaluation.
struct TripleIndex {
    /// Set for dedup: (subject, pred_id, object) already in the index.
    set: HashSet<(NodeId, PredId, NodeId)>,
    /// Index: pred_id → [(subject, object)]
    by_pred: HashMap<PredId, Vec<(NodeId, NodeId)>>,
    /// Index: (pred_id, object) → [subject]
    by_pred_obj: HashMap<(PredId, NodeId), Vec<NodeId>>,
    /// Index: (pred_id, subject) → [object]
    by_pred_subj: HashMap<(PredId, NodeId), Vec<NodeId>>,
}

impl TripleIndex {
    fn new() -> Self {
        Self {
            set: HashSet::new(),
            by_pred: HashMap::new(),
            by_pred_obj: HashMap::new(),
            by_pred_subj: HashMap::new(),
        }
    }

    fn insert(&mut self, s: NodeId, p: PredId, o: NodeId) {
        if !self.set.insert((s, p, o)) {
            return; // already present
        }
        self.by_pred.entry(p).or_default().push((s, o));
        self.by_pred_obj.entry((p, o)).or_default().push(s);
        self.by_pred_subj.entry((p, s)).or_default().push(o);
    }
}

// ── Vocab: interned PredIds for all well-known IRIs ───────────────────────────

struct Vocab {
    rdf_type: PredId,
    rdfs_domain: PredId,
    rdfs_range: PredId,
    rdfs_subclass_of: PredId,
    rdfs_subprop_of: PredId,
    owl_inverse_of: PredId,
    owl_same_as: PredId,
    /// NodeId for the owl:SymmetricProperty class (object in rdf:type triples).
    owl_symmetric_node: NodeId,
    /// NodeId for the owl:TransitiveProperty class (object in rdf:type triples).
    owl_transitive_node: NodeId,
}

impl Vocab {
    fn intern(store: &TripleStore) -> Result<Self, StorageError> {
        Ok(Self {
            rdf_type: store.intern_predicate(RDF_TYPE)?,
            rdfs_domain: store.intern_predicate(RDFS_DOMAIN)?,
            rdfs_range: store.intern_predicate(RDFS_RANGE)?,
            rdfs_subclass_of: store.intern_predicate(RDFS_SUBCLASS_OF)?,
            rdfs_subprop_of: store.intern_predicate(RDFS_SUBPROP_OF)?,
            owl_inverse_of: store.intern_predicate(OWL_INVERSE_OF)?,
            owl_same_as: store.intern_predicate(OWL_SAME_AS)?,
            owl_symmetric_node: uri_to_node_id(OWL_SYMMETRIC_PROP),
            owl_transitive_node: uri_to_node_id(OWL_TRANSITIVE_PROP),
        })
    }
}

// ── Bridge: predicate string ↔ NodeId ────────────────────────────────────────

/// Build node↔predicate bridge maps.
///
/// For every interned predicate string P, `uri_to_node_id(P)` is its canonical
/// schema NodeId. Returns `(node_to_pred_name, pred_name_to_node)`.
fn build_bridge(store: &TripleStore) -> (HashMap<NodeId, String>, HashMap<String, NodeId>) {
    let mut node_to_pred: HashMap<NodeId, String> = HashMap::new();
    let mut pred_to_node: HashMap<String, NodeId> = HashMap::new();

    let count = store.predicate_count();
    for id in 1..=(count + 1) {
        if let Some(name) = store.predicate_string(id) {
            let node = uri_to_node_id(&name);
            node_to_pred.insert(node, name.clone());
            pred_to_node.insert(name, node);
        }
    }

    (node_to_pred, pred_to_node)
}

// ── Forward-chaining materializer ─────────────────────────────────────────────

/// Run OWL 2 RL forward-chaining materialization to fixpoint.
///
/// When `clear_first` is true (the default), the DRV CF is wiped before
/// re-materializing. When false, materialization starts from the current DRV
/// state and only adds incremental new facts.
///
/// Returns statistics about the run.
pub fn materialize(
    store: &TripleStore,
    clear_first: bool,
) -> Result<MaterializationStats, StorageError> {
    if clear_first {
        store.clear_derived()?;
    }

    // Intern OWL/RDFS vocabulary predicates.
    let vocab = Vocab::intern(store)?;

    let mut stats = MaterializationStats::default();

    loop {
        // Build predicate ↔ node bridge maps from current interned predicates.
        let (node_to_pred, _pred_to_node) = build_bridge(store);

        // Collect all current triples (base + derived).
        let base = store.scan_all()?;
        let derived = store.scan_derived()?;

        // Build in-memory index of all Relation triples.
        let mut idx = TripleIndex::new();
        for triple in base.iter().chain(derived.iter()) {
            if let Triple::Relation {
                subject,
                predicate,
                object,
                ..
            } = triple
            {
                if let Some(p_id) = store.predicate_id(&predicate.0) {
                    idx.insert(*subject, p_id, *object);
                }
            }
        }

        // Collect candidate new facts (may contain duplicates; deduped below).
        let mut candidates: Vec<(NodeId, PredId, NodeId)> = Vec::new();

        // ── rdfs2: (?s ?p ?o), (?p rdfs:domain ?C) → (?s rdf:type ?C) ─────────
        if let Some(domain_pairs) = idx.by_pred.get(&vocab.rdfs_domain) {
            let domain_pairs: Vec<(NodeId, NodeId)> = domain_pairs.clone();
            for (p_node, class_node) in domain_pairs {
                if let Some(p_name) = node_to_pred.get(&p_node) {
                    if let Some(p_id) = store.predicate_id(p_name) {
                        if let Some(pairs) = idx.by_pred.get(&p_id) {
                            for &(s, _o) in pairs {
                                candidates.push((s, vocab.rdf_type, class_node));
                            }
                        }
                    }
                }
            }
        }

        // ── rdfs3: (?s ?p ?o), (?p rdfs:range ?C) → (?o rdf:type ?C) ──────────
        if let Some(range_pairs) = idx.by_pred.get(&vocab.rdfs_range) {
            let range_pairs: Vec<(NodeId, NodeId)> = range_pairs.clone();
            for (p_node, class_node) in range_pairs {
                if let Some(p_name) = node_to_pred.get(&p_node) {
                    if let Some(p_id) = store.predicate_id(p_name) {
                        if let Some(pairs) = idx.by_pred.get(&p_id) {
                            for &(_s, o) in pairs {
                                candidates.push((o, vocab.rdf_type, class_node));
                            }
                        }
                    }
                }
            }
        }

        // ── rdfs5: (?p spo ?q), (?q spo ?r) → (?p spo ?r) ─────────────────────
        if let Some(subprop_pairs) = idx.by_pred.get(&vocab.rdfs_subprop_of) {
            let subprop_pairs: Vec<(NodeId, NodeId)> = subprop_pairs.clone();
            for (p_node, q_node) in subprop_pairs {
                if let Some(r_nodes) = idx.by_pred_subj.get(&(vocab.rdfs_subprop_of, q_node)) {
                    for &r_node in r_nodes.clone().iter() {
                        candidates.push((p_node, vocab.rdfs_subprop_of, r_node));
                    }
                }
            }
        }

        // ── rdfs7/prp-spo1: (?s ?p ?o), (?p spo ?q) → (?s ?q ?o) ─────────────
        if let Some(subprop_pairs) = idx.by_pred.get(&vocab.rdfs_subprop_of) {
            let subprop_pairs: Vec<(NodeId, NodeId)> = subprop_pairs.clone();
            for (p_node, q_node) in subprop_pairs {
                if let Some(p_name) = node_to_pred.get(&p_node) {
                    if let Some(q_name) = node_to_pred.get(&q_node) {
                        let p_id_opt = store.predicate_id(p_name);
                        // intern q since it'll be used as a predicate in inferred triples
                        let q_id_opt = store.intern_predicate(q_name).ok();
                        if let (Some(p_id), Some(q_id)) = (p_id_opt, q_id_opt) {
                            if let Some(pairs) = idx.by_pred.get(&p_id) {
                                for &(s, o) in pairs.clone().iter() {
                                    candidates.push((s, q_id, o));
                                }
                            }
                        }
                    }
                }
            }
        }

        // ── rdfs9: (?C subClassOf ?D), (?s rdf:type ?C) → (?s rdf:type ?D) ─────
        if let Some(subclass_pairs) = idx.by_pred.get(&vocab.rdfs_subclass_of) {
            let subclass_pairs: Vec<(NodeId, NodeId)> = subclass_pairs.clone();
            for (c_node, d_node) in subclass_pairs {
                if let Some(subjects) = idx.by_pred_obj.get(&(vocab.rdf_type, c_node)) {
                    for &s in subjects.clone().iter() {
                        candidates.push((s, vocab.rdf_type, d_node));
                    }
                }
            }
        }

        // ── rdfs11: (?C sco ?D), (?D sco ?E) → (?C sco ?E) ────────────────────
        if let Some(subclass_pairs) = idx.by_pred.get(&vocab.rdfs_subclass_of) {
            let subclass_pairs: Vec<(NodeId, NodeId)> = subclass_pairs.clone();
            for (c_node, d_node) in subclass_pairs {
                if let Some(e_nodes) = idx.by_pred_subj.get(&(vocab.rdfs_subclass_of, d_node)) {
                    for &e_node in e_nodes.clone().iter() {
                        candidates.push((c_node, vocab.rdfs_subclass_of, e_node));
                    }
                }
            }
        }

        // ── prp-symp: (?p rdf:type owl:SymmetricProperty), (?s ?p ?o) → (?o ?p ?s)
        if let Some(sym_props) = idx
            .by_pred_obj
            .get(&(vocab.rdf_type, vocab.owl_symmetric_node))
        {
            let sym_props: Vec<NodeId> = sym_props.clone();
            for p_node in sym_props {
                if let Some(p_name) = node_to_pred.get(&p_node) {
                    if let Some(p_id) = store.predicate_id(p_name) {
                        if let Some(pairs) = idx.by_pred.get(&p_id) {
                            for &(s, o) in pairs.clone().iter() {
                                candidates.push((o, p_id, s));
                            }
                        }
                    }
                }
            }
        }

        // ── prp-trp: (?p rdf:type owl:TransitiveProperty), (?s ?p ?m), (?m ?p ?o)
        //            → (?s ?p ?o)
        if let Some(trp_props) = idx
            .by_pred_obj
            .get(&(vocab.rdf_type, vocab.owl_transitive_node))
        {
            let trp_props: Vec<NodeId> = trp_props.clone();
            for p_node in trp_props {
                if let Some(p_name) = node_to_pred.get(&p_node) {
                    if let Some(p_id) = store.predicate_id(p_name) {
                        if let Some(sm_pairs) = idx.by_pred.get(&p_id) {
                            let sm_pairs: Vec<(NodeId, NodeId)> = sm_pairs.clone();
                            for (s, m) in sm_pairs {
                                if let Some(mo_list) = idx.by_pred_subj.get(&(p_id, m)) {
                                    for &o in mo_list.clone().iter() {
                                        candidates.push((s, p_id, o));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // ── prp-inv1: (?p owl:inverseOf ?q), (?s ?p ?o) → (?o ?q ?s) ───────────
        if let Some(inv_pairs) = idx.by_pred.get(&vocab.owl_inverse_of) {
            let inv_pairs: Vec<(NodeId, NodeId)> = inv_pairs.clone();
            for (p_node, q_node) in inv_pairs {
                if let Some(p_name) = node_to_pred.get(&p_node) {
                    if let Some(q_name) = node_to_pred.get(&q_node) {
                        let p_id_opt = store.predicate_id(p_name);
                        let q_id_opt = store.intern_predicate(q_name).ok();
                        if let (Some(p_id), Some(q_id)) = (p_id_opt, q_id_opt) {
                            if let Some(pairs) = idx.by_pred.get(&p_id) {
                                for &(s, o) in pairs.clone().iter() {
                                    candidates.push((o, q_id, s));
                                }
                            }
                        }
                    }
                }
            }
        }

        // ── prp-inv2: (?p owl:inverseOf ?q), (?s ?q ?o) → (?o ?p ?s) ───────────
        if let Some(inv_pairs) = idx.by_pred.get(&vocab.owl_inverse_of) {
            let inv_pairs: Vec<(NodeId, NodeId)> = inv_pairs.clone();
            for (p_node, q_node) in inv_pairs {
                if let Some(p_name) = node_to_pred.get(&p_node) {
                    if let Some(q_name) = node_to_pred.get(&q_node) {
                        let p_id_opt = store.intern_predicate(p_name).ok();
                        let q_id_opt = store.predicate_id(q_name);
                        if let (Some(p_id), Some(q_id)) = (p_id_opt, q_id_opt) {
                            if let Some(pairs) = idx.by_pred.get(&q_id) {
                                for &(s, o) in pairs.clone().iter() {
                                    candidates.push((o, p_id, s));
                                }
                            }
                        }
                    }
                }
            }
        }

        // ── eq-sym: (?s owl:sameAs ?o) → (?o owl:sameAs ?s) ────────────────────
        if let Some(sameas_pairs) = idx.by_pred.get(&vocab.owl_same_as) {
            let sameas_pairs: Vec<(NodeId, NodeId)> = sameas_pairs.clone();
            for (s, o) in sameas_pairs {
                candidates.push((o, vocab.owl_same_as, s));
            }
        }

        // ── eq-trans: (?s owl:sameAs ?m), (?m owl:sameAs ?o) → (?s owl:sameAs ?o)
        if let Some(sameas_pairs) = idx.by_pred.get(&vocab.owl_same_as) {
            let sameas_pairs: Vec<(NodeId, NodeId)> = sameas_pairs.clone();
            for (s, m) in sameas_pairs {
                if let Some(mo_list) = idx.by_pred_subj.get(&(vocab.owl_same_as, m)) {
                    for &o in mo_list.clone().iter() {
                        candidates.push((s, vocab.owl_same_as, o));
                    }
                }
            }
        }

        // Deduplicate: filter out facts already in idx (base or derived), and
        // remove duplicates within this batch.
        let mut batch_seen: HashSet<(NodeId, PredId, NodeId)> = HashSet::new();
        let new_facts: Vec<(NodeId, PredId, NodeId)> = candidates
            .into_iter()
            .filter(|f| batch_seen.insert(*f) && !idx.set.contains(f))
            .collect();

        if new_facts.is_empty() {
            break;
        }

        let count = new_facts.len() as u64;
        store.insert_derived_batch(&new_facts)?;

        stats.rules_fired += count;
        stats.derived_triples += count;
        stats.iterations += 1;
    }

    // Final derived count from the DRV CF estimate.
    stats.derived_triples = store.estimate_derived_count();

    Ok(stats)
}

// ── Helper: look up which predicates are stored as Relation triples ───────────

/// Return the predicate string used in a `Triple::Relation`.
#[allow(dead_code)]
fn relation_predicate(triple: &Triple) -> Option<&str> {
    match triple {
        Triple::Relation { predicate, .. } => Some(&predicate.0),
        _ => None,
    }
}

/// Return the predicate NodeId via bridge for schema assertions.
///
/// Exposed for tests that need to build `(?p_node rdfs:domain ?C)` triples
/// where `p_node = uri_to_node_id(pred_string)`.
pub fn predicate_node(pred: &str) -> NodeId {
    uri_to_node_id(pred)
}
