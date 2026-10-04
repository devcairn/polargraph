//! OWL 2 RL inference into queryable **inferred graphs**
//! (`docs/design/step9-inference-vectors-stats.md`).
//!
//! Inferred facts are ordinary relation quads in companion graphs —
//! `urn:pg:inferred:<source graph IRI>` (`urn:pg:inferred:default` for the
//! default graph) and the service-only `urn:pg:inferred:cross` — written
//! through the normal commit path (author [`INFERENCE_AUTHOR`]), so every
//! reader sees them and the graph ACL applies.
//!
//! A fact's graph follows its **instance (A-box) premises**: all in source
//! graph `g` → `g`'s inferred graph; several graphs → the cross graph. Schema
//! (T-box) premises don't decide the graph, so a schema kept in its own
//! graph still yields per-graph inferences.
//!
//! # Rules
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
//! | prp-inv1     | (?p owl:inverseOf ?q), (?s ?p ?o)                           | (?o ?q ?s)                 |
//! | prp-inv2     | (?p owl:inverseOf ?q), (?s ?q ?o)                           | (?o ?p ?s)                 |
//! | eq-sym       | (?s owl:sameAs ?o)                                          | (?o owl:sameAs ?s)         |
//! | eq-trans     | (?s owl:sameAs ?m), (?m owl:sameAs ?o)                      | (?s owl:sameAs ?o)         |
//!
//! The schema (`subClassOf` / `subPropertyOf` closed transitively, domains,
//! ranges, inverses, symmetric and transitive properties) is loaded up front,
//! so every instance rule is one step; rdfs5 / rdfs11 are the schema closure.
//!
//! Properties and classes are named by node — `term::iri_to_node_id(IRI)`,
//! the mapping RDF import uses; a property node's IRI comes from the IRI
//! dictionary, or from the interned predicate with that node.
//!
//! # Runs
//!
//! - [`materialize`]: compute the closure of the base data and **diff** it
//!   against the live inferred graphs — assert what's new, close (bitemporal
//!   `vt_end`) what no longer follows.
//! - [`infer_changes`]: incremental maintenance (DRed) from the change log.

use std::collections::{HashMap, HashSet, VecDeque};

use polargraph_core::{
    id::{GraphId, NodeId},
    temporal::{BiTemporalRange, Timestamp},
    term,
    triple::{Predicate, Triple},
    value::Value,
};

use crate::{
    error::StorageError,
    graphs::close_at,
    mvcc::{Snapshot, WriteMode},
    store::{GraphScope, TripleStore},
    SYSTEM_GRAPH_IRI,
};

// ── Vocabulary ────────────────────────────────────────────────────────────────

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDFS_DOMAIN: &str = "http://www.w3.org/2000/01/rdf-schema#domain";
const RDFS_RANGE: &str = "http://www.w3.org/2000/01/rdf-schema#range";
const RDFS_SUBCLASS_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
const RDFS_SUBPROP_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subPropertyOf";
const OWL_INVERSE_OF: &str = "http://www.w3.org/2002/07/owl#inverseOf";
const OWL_SAME_AS: &str = "http://www.w3.org/2002/07/owl#sameAs";
const OWL_SYMMETRIC_PROP: &str = "http://www.w3.org/2002/07/owl#SymmetricProperty";
const OWL_TRANSITIVE_PROP: &str = "http://www.w3.org/2002/07/owl#TransitiveProperty";

/// Predicates whose facts are schema (T-box).
const TBOX_PREDICATES: [&str; 5] = [
    RDFS_DOMAIN,
    RDFS_RANGE,
    RDFS_SUBCLASS_OF,
    RDFS_SUBPROP_OF,
    OWL_INVERSE_OF,
];

/// Prefix of inferred graph IRIs.
pub const INFERRED_GRAPH_PREFIX: &str = "urn:pg:inferred:";
/// The default graph's inferred graph.
pub const INFERRED_DEFAULT_GRAPH_IRI: &str = "urn:pg:inferred:default";
/// Facts whose instance premises span graphs (service-only: no grants).
pub const INFERRED_CROSS_GRAPH_IRI: &str = "urn:pg:inferred:cross";
/// Graph metadata on an inferred graph: its source graph's IRI.
pub const INFERRED_FROM_PRED: &str = "urn:pg:inferredFrom";
/// Author of inference commits (skipped by incremental maintenance).
pub const INFERENCE_AUTHOR: &str = "urn:pg:inference";

/// META key: the last change-log commit incremental inference processed.
const META_INFERENCE_APPLIED: &[u8] = b"__inference__/applied_ts";
/// The graphs schema axioms are read from (JSON array of IRIs, "" = the
/// default graph); absent = every graph.
const META_SCHEMA_GRAPHS: &[u8] = b"__inference__/schema_graphs";

/// Commit at most this many quad writes per transaction.
const CHUNK: usize = 10_000;

/// Whether `iri` names an inferred graph.
pub fn is_inferred_graph_iri(iri: &str) -> bool {
    iri.starts_with(INFERRED_GRAPH_PREFIX)
}

// ── Facts ─────────────────────────────────────────────────────────────────────

/// Which inferred graph a fact belongs to: a source graph's, or the cross
/// graph. Base facts carry the label of their own graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Label {
    Graph(GraphId),
    Cross,
}

impl Label {
    fn combine(self, other: Label) -> Label {
        if self == other {
            self
        } else {
            Label::Cross
        }
    }
}

/// A relation fact `(s p o)` with its label.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Fact {
    pub s: NodeId,
    pub p: String,
    pub o: NodeId,
    pub label: Label,
}

impl Fact {
    fn new(s: NodeId, p: &str, o: NodeId, label: Label) -> Self {
        Self {
            s,
            p: p.to_string(),
            o,
            label,
        }
    }

    /// Schema facts drive rules instead of being instance premises.
    fn is_schema(&self) -> bool {
        TBOX_PREDICATES.contains(&self.p.as_str())
            || (self.p == RDF_TYPE
                && (self.o == term::iri_to_node_id(OWL_SYMMETRIC_PROP)
                    || self.o == term::iri_to_node_id(OWL_TRANSITIVE_PROP)))
    }
}

// ── Graphs ────────────────────────────────────────────────────────────────────

/// The store's inferred graphs and how labels map to them.
#[derive(Debug, Default, Clone)]
pub struct InferredGraphs {
    /// Inferred graph id → label.
    pub label_of: HashMap<GraphId, Label>,
    /// Label → inferred graph id (existing graphs only).
    pub graph_of: HashMap<Label, GraphId>,
    /// The system graph (never a premise).
    system: Option<GraphId>,
    /// Graphs schema (T-box) axioms are read from; `None` = every graph.
    schema_graphs: Option<HashSet<GraphId>>,
}

impl InferredGraphs {
    pub fn load(store: &TripleStore) -> Self {
        let mut out = InferredGraphs {
            system: store.graph_id(SYSTEM_GRAPH_IRI),
            schema_graphs: schema_graphs(store).ok().flatten().map(|iris| {
                iris.iter()
                    .filter_map(|iri| match iri.as_str() {
                        "" => Some(GraphId::DEFAULT),
                        iri => store.graph_id(iri),
                    })
                    .collect()
            }),
            ..Default::default()
        };
        for (id, iri) in store.list_graphs() {
            let Some(rest) = iri.strip_prefix(INFERRED_GRAPH_PREFIX) else {
                continue;
            };
            let label = match rest {
                "cross" => Label::Cross,
                "default" => Label::Graph(GraphId::DEFAULT),
                source => match store.graph_id(source) {
                    Some(g) => Label::Graph(g),
                    None => continue,
                },
            };
            out.label_of.insert(id, label);
            out.graph_of.insert(label, id);
        }
        out
    }

    /// The label of a quad in graph `g`: its source graph for an inferred
    /// graph, `g` itself for a base graph; `None` for the system graph.
    fn label(&self, g: GraphId) -> Option<Label> {
        if Some(g) == self.system {
            return None;
        }
        Some(self.label_of.get(&g).copied().unwrap_or(Label::Graph(g)))
    }

    fn is_inferred(&self, g: GraphId) -> bool {
        self.label_of.contains_key(&g)
    }

    /// Whether schema axioms in `g` drive the rules.
    fn is_schema_graph(&self, g: GraphId) -> bool {
        match &self.schema_graphs {
            Some(s) => s.contains(&g),
            None => true,
        }
    }

    /// Inferred graph ids, for excluding them from a snapshot.
    pub fn ids(&self) -> roaring::RoaringBitmap {
        self.label_of.keys().map(|g| g.0).collect()
    }

    /// `(inferred graph, source graph)` pairs (not the cross graph).
    pub fn companions(&self) -> impl Iterator<Item = (GraphId, GraphId)> + '_ {
        self.label_of.iter().filter_map(|(id, label)| match label {
            Label::Graph(src) => Some((*id, *src)),
            Label::Cross => None,
        })
    }

    /// The inferred graph for `label`, created on first use.
    fn ensure(&mut self, store: &TripleStore, label: Label) -> Result<GraphId, StorageError> {
        if let Some(g) = self.graph_of.get(&label) {
            return Ok(*g);
        }
        let (iri, source) = match label {
            Label::Cross => (INFERRED_CROSS_GRAPH_IRI.to_string(), None),
            Label::Graph(GraphId::DEFAULT) => (INFERRED_DEFAULT_GRAPH_IRI.to_string(), None),
            Label::Graph(g) => {
                let src = store.graph_iri(g).ok_or_else(|| {
                    StorageError::Validation(format!("unknown source graph {}", g.0))
                })?;
                (format!("{INFERRED_GRAPH_PREFIX}{src}"), Some(src))
            }
        };
        let metadata: Vec<(String, Value)> = source
            .map(|s| vec![(INFERRED_FROM_PRED.to_string(), Value::Text(s))])
            .unwrap_or_default();
        let g = store.create_graph_by(&iri, &metadata, INFERENCE_AUTHOR)?;
        self.label_of.insert(g, label);
        self.graph_of.insert(label, g);
        Ok(g)
    }
}

// ── Fact sources ──────────────────────────────────────────────────────────────

/// Lookups the rules need.
trait FactSource {
    /// `(s p ?o)` with labels.
    fn objects(&self, p: &str, s: NodeId) -> Result<Vec<(NodeId, Label)>, StorageError>;
    /// `(?s p o)` with labels.
    fn subjects(&self, p: &str, o: NodeId) -> Result<Vec<(NodeId, Label)>, StorageError>;
    /// Whether `f` holds with its label (or, for a cross fact, anywhere).
    fn present(&self, f: &Fact) -> Result<bool, StorageError>;
}

/// An in-memory set of facts (full materialization, deltas).
#[derive(Default)]
struct MemFacts {
    set: HashSet<Fact>,
    any: HashSet<(NodeId, String, NodeId)>,
    by_ps: HashMap<(String, NodeId), Vec<(NodeId, Label)>>,
    by_po: HashMap<(String, NodeId), Vec<(NodeId, Label)>>,
}

impl MemFacts {
    fn insert(&mut self, f: Fact) -> bool {
        if !self.set.insert(f.clone()) {
            return false;
        }
        self.any.insert((f.s, f.p.clone(), f.o));
        self.by_ps
            .entry((f.p.clone(), f.s))
            .or_default()
            .push((f.o, f.label));
        self.by_po
            .entry((f.p, f.o))
            .or_default()
            .push((f.s, f.label));
        true
    }
}

impl FactSource for MemFacts {
    fn objects(&self, p: &str, s: NodeId) -> Result<Vec<(NodeId, Label)>, StorageError> {
        Ok(self
            .by_ps
            .get(&(p.to_string(), s))
            .cloned()
            .unwrap_or_default())
    }
    fn subjects(&self, p: &str, o: NodeId) -> Result<Vec<(NodeId, Label)>, StorageError> {
        Ok(self
            .by_po
            .get(&(p.to_string(), o))
            .cloned()
            .unwrap_or_default())
    }
    fn present(&self, f: &Fact) -> Result<bool, StorageError> {
        Ok(self.set.contains(f)
            || (f.label == Label::Cross && self.any.contains(&(f.s, f.p.clone(), f.o))))
    }
}

/// The store at a snapshot, minus `removed`, plus `added`.
struct StoreFacts<'a> {
    snap: Snapshot,
    graphs: &'a InferredGraphs,
    removed: HashSet<Fact>,
    added: MemFacts,
}

impl StoreFacts<'_> {
    fn scan(
        &self,
        s: Option<NodeId>,
        p: &str,
        o: Option<NodeId>,
    ) -> Result<Vec<Fact>, StorageError> {
        let mut out = Vec::new();
        for (g, t) in self
            .snap
            .scan_scoped(s.as_ref(), Some(p), o.as_ref(), &GraphScope::Union)?
        {
            let (
                Triple::Relation {
                    subject, object, ..
                },
                Some(label),
            ) = (t, self.graphs.label(g))
            else {
                continue;
            };
            let f = Fact::new(subject, p, object, label);
            if !self.removed.contains(&f) {
                out.push(f);
            }
        }
        Ok(out)
    }
}

impl FactSource for StoreFacts<'_> {
    fn objects(&self, p: &str, s: NodeId) -> Result<Vec<(NodeId, Label)>, StorageError> {
        let mut out: Vec<_> = self
            .scan(Some(s), p, None)?
            .into_iter()
            .map(|f| (f.o, f.label))
            .collect();
        out.extend(self.added.objects(p, s)?);
        Ok(out)
    }
    fn subjects(&self, p: &str, o: NodeId) -> Result<Vec<(NodeId, Label)>, StorageError> {
        let mut out: Vec<_> = self
            .scan(None, p, Some(o))?
            .into_iter()
            .map(|f| (f.s, f.label))
            .collect();
        out.extend(self.added.subjects(p, o)?);
        Ok(out)
    }
    fn present(&self, f: &Fact) -> Result<bool, StorageError> {
        if self.added.present(f)? {
            return Ok(true);
        }
        Ok(self
            .scan(Some(f.s), &f.p, Some(f.o))?
            .iter()
            .any(|x| x.label == f.label || f.label == Label::Cross))
    }
}

// ── Schema ────────────────────────────────────────────────────────────────────

/// The schema, closed transitively, for one-step instance rules.
#[derive(Default)]
struct Schema {
    /// Property IRI → classes.
    domain: HashMap<String, Vec<NodeId>>,
    range: HashMap<String, Vec<NodeId>>,
    /// Class → properties with it as domain / range (backward rules).
    domain_inv: HashMap<NodeId, Vec<String>>,
    range_inv: HashMap<NodeId, Vec<String>>,
    /// Property → all its super-properties / its sub-properties.
    super_props: HashMap<String, Vec<String>>,
    sub_props: HashMap<String, Vec<String>>,
    /// Class → all its super-classes / its sub-classes.
    super_classes: HashMap<NodeId, Vec<NodeId>>,
    sub_classes: HashMap<NodeId, Vec<NodeId>>,
    /// `(s p o)` ⇒ `(o q s)`, and the reverse map for backward rules.
    inverse: HashMap<String, Vec<String>>,
    inverse_of: HashMap<String, Vec<String>>,
    symmetric: HashSet<String>,
    transitive: HashSet<String>,
    /// Schema closure facts (rdfs5 / rdfs11), labelled.
    closure: Vec<Fact>,
}

/// Node ↔ IRI for properties and classes.
struct Names {
    of_node: HashMap<NodeId, String>,
}

impl Names {
    fn load(store: &TripleStore) -> Self {
        let mut of_node = HashMap::new();
        for id in 1..=(store.predicate_count() + 1) {
            if let Some(name) = store.predicate_string(id) {
                of_node.insert(term::iri_to_node_id(&name), name);
            }
        }
        Names { of_node }
    }

    /// A property node's IRI: the interned predicate with that node, else
    /// the IRI dictionary.
    fn property(&self, store: &TripleStore, node: NodeId) -> Option<String> {
        if let Some(name) = self.of_node.get(&node) {
            return Some(name.clone());
        }
        store.iri_of(&node).ok().flatten()
    }
}

/// Transitive closure of labelled edges: per-graph closures keep their
/// graph's label; pairs only reachable across graphs get [`Label::Cross`].
fn labelled_closure(edges: &[(NodeId, NodeId, Label)]) -> Vec<(NodeId, NodeId, Label)> {
    fn closure(edges: &[(NodeId, NodeId)]) -> HashSet<(NodeId, NodeId)> {
        let mut next: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
        for (a, b) in edges {
            next.entry(*a).or_default().push(*b);
        }
        let mut out = HashSet::new();
        for start in next.keys() {
            let mut seen = HashSet::new();
            let mut queue: VecDeque<NodeId> = next[start].iter().copied().collect();
            while let Some(n) = queue.pop_front() {
                if seen.insert(n) {
                    out.insert((*start, n));
                    if let Some(more) = next.get(&n) {
                        queue.extend(more.iter().copied());
                    }
                }
            }
        }
        out
    }
    let mut by_label: HashMap<Label, Vec<(NodeId, NodeId)>> = HashMap::new();
    for (a, b, l) in edges {
        by_label.entry(*l).or_default().push((*a, *b));
    }
    let mut out = Vec::new();
    let mut covered = HashSet::new();
    for (label, es) in &by_label {
        for pair in closure(es) {
            covered.insert(pair);
            out.push((pair.0, pair.1, *label));
        }
    }
    let all: Vec<(NodeId, NodeId)> = edges.iter().map(|(a, b, _)| (*a, *b)).collect();
    for pair in closure(&all) {
        if !covered.contains(&pair) {
            out.push((pair.0, pair.1, Label::Cross));
        }
    }
    out
}

impl Schema {
    fn load(
        store: &TripleStore,
        snap: &Snapshot,
        graphs: &InferredGraphs,
    ) -> Result<Self, StorageError> {
        let names = Names::load(store);
        // Base schema facts only (inferred schema facts are recomputed).
        let edges = |p: &str| -> Result<Vec<(NodeId, NodeId, Label)>, StorageError> {
            Ok(snap
                .scan_scoped(None, Some(p), None, &GraphScope::Union)?
                .into_iter()
                .filter(|(g, _)| !graphs.is_inferred(*g) && graphs.is_schema_graph(*g))
                .filter_map(|(g, t)| match (t, graphs.label(g)) {
                    (
                        Triple::Relation {
                            subject, object, ..
                        },
                        Some(l),
                    ) => Some((subject, object, l)),
                    _ => None,
                })
                .collect())
        };
        let mut schema = Schema::default();
        let prop = |n: NodeId| names.property(store, n);

        let sco = edges(RDFS_SUBCLASS_OF)?;
        for (c, d, l) in labelled_closure(&sco) {
            if c == d {
                continue;
            }
            schema.super_classes.entry(c).or_default().push(d);
            schema.sub_classes.entry(d).or_default().push(c);
            if !sco.contains(&(c, d, l)) {
                schema.closure.push(Fact::new(c, RDFS_SUBCLASS_OF, d, l));
            }
        }
        let spo = edges(RDFS_SUBPROP_OF)?;
        for (p, q, l) in labelled_closure(&spo) {
            if p == q {
                continue;
            }
            if !spo.contains(&(p, q, l)) {
                schema.closure.push(Fact::new(p, RDFS_SUBPROP_OF, q, l));
            }
            if let (Some(p), Some(q)) = (prop(p), prop(q)) {
                schema
                    .super_props
                    .entry(p.clone())
                    .or_default()
                    .push(q.clone());
                schema.sub_props.entry(q).or_default().push(p);
            }
        }
        for (p, c, _) in edges(RDFS_DOMAIN)? {
            if let Some(p) = prop(p) {
                schema.domain.entry(p.clone()).or_default().push(c);
                schema.domain_inv.entry(c).or_default().push(p);
            }
        }
        for (p, c, _) in edges(RDFS_RANGE)? {
            if let Some(p) = prop(p) {
                schema.range.entry(p.clone()).or_default().push(c);
                schema.range_inv.entry(c).or_default().push(p);
            }
        }
        for (p, q, _) in edges(OWL_INVERSE_OF)? {
            if let (Some(p), Some(q)) = (prop(p), prop(q)) {
                // inv1: (s p o) ⇒ (o q s); inv2: (s q o) ⇒ (o p s).
                schema.inverse.entry(p.clone()).or_default().push(q.clone());
                schema.inverse.entry(q.clone()).or_default().push(p.clone());
                schema
                    .inverse_of
                    .entry(q.clone())
                    .or_default()
                    .push(p.clone());
                schema.inverse_of.entry(p).or_default().push(q);
            }
        }
        let typed = |class: &str| -> Result<HashSet<String>, StorageError> {
            Ok(snap
                .scan_scoped(
                    None,
                    Some(RDF_TYPE),
                    Some(&term::iri_to_node_id(class)),
                    &GraphScope::Union,
                )?
                .into_iter()
                .filter(|(g, _)| !graphs.is_inferred(*g) && graphs.is_schema_graph(*g))
                .filter_map(|(_, t)| prop(t.subject()))
                .collect())
        };
        schema.symmetric = typed(OWL_SYMMETRIC_PROP)?;
        schema.transitive = typed(OWL_TRANSITIVE_PROP)?;
        Ok(schema)
    }

    /// Facts one rule step derives using the instance fact `f`.
    fn consequences(&self, f: &Fact, src: &impl FactSource) -> Result<Vec<Fact>, StorageError> {
        let mut out = Vec::new();
        if f.is_schema() {
            return Ok(out);
        }
        let l = f.label;
        for c in self.domain.get(&f.p).into_iter().flatten() {
            out.push(Fact::new(f.s, RDF_TYPE, *c, l)); // rdfs2
        }
        for c in self.range.get(&f.p).into_iter().flatten() {
            out.push(Fact::new(f.o, RDF_TYPE, *c, l)); // rdfs3
        }
        for q in self.super_props.get(&f.p).into_iter().flatten() {
            out.push(Fact::new(f.s, q, f.o, l)); // rdfs7
        }
        if f.p == RDF_TYPE {
            for d in self.super_classes.get(&f.o).into_iter().flatten() {
                out.push(Fact::new(f.s, RDF_TYPE, *d, l)); // rdfs9
            }
        }
        if self.symmetric.contains(&f.p) || f.p == OWL_SAME_AS {
            out.push(Fact::new(f.o, &f.p, f.s, l)); // prp-symp, eq-sym
        }
        for q in self.inverse.get(&f.p).into_iter().flatten() {
            out.push(Fact::new(f.o, q, f.s, l)); // prp-inv1/2
        }
        if self.transitive.contains(&f.p) || f.p == OWL_SAME_AS {
            // prp-trp, eq-trans — with f as either premise.
            for (o2, l2) in src.objects(&f.p, f.o)? {
                out.push(Fact::new(f.s, &f.p, o2, l.combine(l2)));
            }
            for (s0, l0) in src.subjects(&f.p, f.s)? {
                out.push(Fact::new(s0, &f.p, f.o, l0.combine(l)));
            }
        }
        Ok(out)
    }

    /// Whether some rule derives `x` (with its label) in one step from `src`.
    fn supported(&self, x: &Fact, src: &impl FactSource) -> Result<bool, StorageError> {
        let has = |p: &str, s: NodeId, o: NodeId| -> Result<bool, StorageError> {
            Ok(src.objects(p, s)?.contains(&(o, x.label)))
        };
        if x.p == RDF_TYPE {
            for p in self.domain_inv.get(&x.o).into_iter().flatten() {
                if src.objects(p, x.s)?.iter().any(|(_, l)| *l == x.label) {
                    return Ok(true); // rdfs2
                }
            }
            for p in self.range_inv.get(&x.o).into_iter().flatten() {
                if src.subjects(p, x.s)?.iter().any(|(_, l)| *l == x.label) {
                    return Ok(true); // rdfs3
                }
            }
            for c in self.sub_classes.get(&x.o).into_iter().flatten() {
                if has(RDF_TYPE, x.s, *c)? {
                    return Ok(true); // rdfs9
                }
            }
        }
        for p in self.sub_props.get(&x.p).into_iter().flatten() {
            if has(p, x.s, x.o)? {
                return Ok(true); // rdfs7
            }
        }
        if (self.symmetric.contains(&x.p) || x.p == OWL_SAME_AS) && has(&x.p, x.o, x.s)? {
            return Ok(true); // prp-symp, eq-sym
        }
        for q in self.inverse_of.get(&x.p).into_iter().flatten() {
            if has(q, x.o, x.s)? {
                return Ok(true); // prp-inv1/2
            }
        }
        if self.transitive.contains(&x.p) || x.p == OWL_SAME_AS {
            for (m, l1) in src.objects(&x.p, x.s)? {
                if src
                    .objects(&x.p, m)?
                    .iter()
                    .any(|(o, l2)| *o == x.o && l1.combine(*l2) == x.label)
                {
                    return Ok(true); // prp-trp, eq-trans
                }
            }
        }
        Ok(false)
    }
}

// ── Writing ───────────────────────────────────────────────────────────────────

/// What a run changed.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MaterializationStats {
    /// Inferred facts asserted.
    pub asserted: u64,
    /// Inferred facts closed (no longer derivable).
    pub closed: u64,
    /// Live inferred facts after the run.
    pub derived_triples: u64,
    /// Whether this was a full recompute (vs. incremental DRed).
    pub full: bool,
}

fn relation(f: &Fact) -> Triple {
    Triple::Relation {
        subject: f.s,
        predicate: Predicate::new(&f.p),
        object: f.o,
        edge_id: term::edge_id_for(&f.s.to_string(), &f.p, &f.o.to_string()),
        temporal: BiTemporalRange::assert_now(Timestamp::now()),
    }
}

/// Assert `assert` and close the stored quads `close` in chunked commits.
fn write(
    store: &TripleStore,
    graphs: &mut InferredGraphs,
    assert: &[Fact],
    close: &[(GraphId, Triple)],
) -> Result<(), StorageError> {
    let now = Timestamp::now();
    let mut ops: Vec<(GraphId, Triple)> = Vec::with_capacity(assert.len() + close.len());
    for f in assert {
        ops.push((graphs.ensure(store, f.label)?, relation(f)));
    }
    for (g, t) in close {
        ops.push((*g, close_at(t.clone(), now)));
    }
    for chunk in ops.chunks(CHUNK) {
        let mut tx = store.begin();
        tx.set_author(INFERENCE_AUTHOR);
        for (g, t) in chunk {
            tx.insert_in(t.clone(), *g, WriteMode::Add);
        }
        tx.commit()?;
    }
    Ok(())
}

/// Live inferred quads: `(fact, graph, stored triple)`.
fn live_inferred(
    snap: &Snapshot,
    graphs: &InferredGraphs,
) -> Result<HashMap<Fact, (GraphId, Triple)>, StorageError> {
    let mut out = HashMap::new();
    let ids: Vec<GraphId> = graphs.label_of.keys().copied().collect();
    if ids.is_empty() {
        return Ok(out);
    }
    for (g, t) in snap.scan_scoped(None, None, None, &GraphScope::set(ids))? {
        if let (
            Triple::Relation {
                subject,
                predicate,
                object,
                ..
            },
            Some(l),
        ) = (&t, graphs.label_of.get(&g))
        {
            out.insert(Fact::new(*subject, &predicate.0, *object, *l), (g, t));
        }
    }
    Ok(out)
}

/// The last commit inference has covered (`None` before the first run).
pub fn inference_applied(store: &TripleStore) -> Result<Option<Timestamp>, StorageError> {
    get_applied(store)
}

/// The last commit incremental inference covered.
fn get_applied(store: &TripleStore) -> Result<Option<Timestamp>, StorageError> {
    let meta = store.cf_handle(crate::cf::META)?;
    Ok(store
        .db_ref()
        .get_cf(&meta, META_INFERENCE_APPLIED)?
        .and_then(|v| <[u8; 8]>::try_from(v.as_slice()).ok())
        .map(|b| Timestamp(i64::from_be_bytes(b))))
}

fn put_applied(store: &TripleStore, ts: Timestamp) -> Result<(), StorageError> {
    let meta = store.cf_handle(crate::cf::META)?;
    store
        .db_ref()
        .put_cf(&meta, META_INFERENCE_APPLIED, ts.0.to_be_bytes())?;
    Ok(())
}

// ── Schema graphs ─────────────────────────────────────────────────────────────

/// The graphs schema axioms are read from (IRIs, "" = the default graph);
/// `None` = every graph (the default).
pub fn schema_graphs(store: &TripleStore) -> Result<Option<Vec<String>>, StorageError> {
    let meta = store.cf_handle(crate::cf::META)?;
    Ok(store
        .db_ref()
        .get_cf(&meta, META_SCHEMA_GRAPHS)?
        .and_then(|v| serde_json::from_slice(&v).ok()))
}

/// Read schema axioms only from `graphs` (IRIs, "" = the default graph;
/// `None` = every graph), then recompute the inferred graphs. Schema
/// axioms in other graphs drive no rules; they stay ordinary data.
pub fn set_schema_graphs(
    store: &TripleStore,
    graphs: Option<&[String]>,
) -> Result<MaterializationStats, StorageError> {
    if store.is_replica() {
        return Err(StorageError::ReadOnly(
            "inference runs on the primary".into(),
        ));
    }
    let _run = run_lock();
    let meta = store.cf_handle(crate::cf::META)?;
    match graphs {
        Some(iris) => {
            let mut iris = iris.to_vec();
            iris.sort();
            iris.dedup();
            let json = serde_json::to_vec(&iris)?;
            store.db_ref().put_cf(&meta, META_SCHEMA_GRAPHS, json)?;
        }
        None => store.db_ref().delete_cf(&meta, META_SCHEMA_GRAPHS)?,
    }
    materialize_locked(store)
}

// ── Full materialization ──────────────────────────────────────────────────────

/// Recompute the closure of the base data and bring the inferred graphs in
/// line: assert new facts, close facts that no longer follow. Idempotent.
///
/// `_clear_first` is kept for API compatibility; the diff makes clearing
/// unnecessary.
pub fn materialize(
    store: &TripleStore,
    _clear_first: bool,
) -> Result<MaterializationStats, StorageError> {
    let _run = run_lock();
    materialize_locked(store)
}

/// One inference run at a time (a manual run, a settings change and the
/// background task would otherwise assert the same facts twice).
fn run_lock() -> std::sync::MutexGuard<'static, ()> {
    static RUN: std::sync::Mutex<()> = std::sync::Mutex::new(());
    RUN.lock().unwrap_or_else(|e| e.into_inner())
}

fn materialize_locked(store: &TripleStore) -> Result<MaterializationStats, StorageError> {
    if store.is_replica() {
        return Err(StorageError::ReadOnly(
            "inference runs on the primary".into(),
        ));
    }
    let at = Timestamp(store.oracle_ts());
    let stats = materialize_at(store, at)?;
    put_applied(store, at)?;
    Ok(stats)
}

fn materialize_at(
    store: &TripleStore,
    at: Timestamp,
) -> Result<MaterializationStats, StorageError> {
    let snap = store.snapshot(at);
    let mut graphs = InferredGraphs::load(store);
    let schema = Schema::load(store, &snap, &graphs)?;

    // Base facts: every relation outside the system and inferred graphs.
    let mut mem = MemFacts::default();
    let mut queue: VecDeque<Fact> = VecDeque::new();
    for (g, t) in snap.scan_scoped(None, None, None, &GraphScope::Union)? {
        if graphs.is_inferred(g) {
            continue;
        }
        let (
            Triple::Relation {
                subject,
                predicate,
                object,
                ..
            },
            Some(l),
        ) = (t, graphs.label(g))
        else {
            continue;
        };
        let f = Fact::new(subject, &predicate.0, object, l);
        if mem.insert(f.clone()) {
            queue.push_back(f);
        }
    }

    // Semi-naive closure: each derivation is found when its last premise
    // arrives.
    let mut derived: HashSet<Fact> = HashSet::new();
    for f in &schema.closure {
        if !mem.present(f)? {
            mem.insert(f.clone());
            derived.insert(f.clone());
        }
    }
    while let Some(f) = queue.pop_front() {
        for c in schema.consequences(&f, &mem)? {
            if !mem.present(&c)? {
                mem.insert(c.clone());
                derived.insert(c.clone());
                queue.push_back(c);
            }
        }
    }

    let live = live_inferred(&snap, &graphs)?;
    let assert: Vec<Fact> = derived
        .iter()
        .filter(|f| !live.contains_key(*f))
        .cloned()
        .collect();
    let close: Vec<(GraphId, Triple)> = live
        .iter()
        .filter(|(f, _)| !derived.contains(*f))
        .map(|(_, v)| v.clone())
        .collect();
    write(store, &mut graphs, &assert, &close)?;
    Ok(MaterializationStats {
        asserted: assert.len() as u64,
        closed: close.len() as u64,
        derived_triples: derived.len() as u64,
        full: true,
    })
}

// ── Incremental maintenance (DRed) ────────────────────────────────────────────

/// Bring the inferred graphs up to date with the commits logged since the
/// last run (DRed: over-delete, re-derive, insert). Falls back to a full
/// [`materialize`] when there is no resume point, the log was pruned past
/// it, or a schema fact changed. Returns `None` when nothing was pending.
pub fn infer_changes(store: &TripleStore) -> Result<Option<MaterializationStats>, StorageError> {
    if store.is_replica() {
        return Err(StorageError::ReadOnly(
            "inference runs on the primary".into(),
        ));
    }
    let _run = run_lock();
    let resume = get_applied(store)?;
    let Some(from) = resume.filter(|ts| *ts >= store.changes_floor().unwrap_or(Timestamp(0)))
    else {
        return materialize_locked(store).map(Some);
    };

    let mut records = Vec::new();
    let mut cursor = from;
    loop {
        let batch = store.changes_after(cursor, 1_000)?;
        let Some(last) = batch.last() else { break };
        cursor = last.commit_ts;
        let more = batch.len() == 1_000;
        records.extend(batch);
        if !more {
            break;
        }
    }
    let to = cursor;
    // Only inference's own commits since the last run: nothing to do.
    if records.iter().all(|r| r.author == INFERENCE_AUTHOR) {
        if !records.is_empty() {
            put_applied(store, to)?;
        }
        return Ok(None);
    }

    let mut graphs = InferredGraphs::load(store);
    let mut asserted: Vec<Fact> = Vec::new();
    let mut deleted: Vec<Fact> = Vec::new();
    let mut schema_changed = false;
    for record in records.iter().filter(|r| r.author != INFERENCE_AUTHOR) {
        for (g, t) in &record.quads {
            if graphs.is_inferred(*g) {
                continue;
            }
            let (
                Triple::Relation {
                    subject,
                    predicate,
                    object,
                    temporal,
                    ..
                },
                Some(l),
            ) = (t, graphs.label(*g))
            else {
                continue;
            };
            let f = Fact::new(*subject, &predicate.0, *object, l);
            schema_changed |= f.is_schema() && graphs.is_schema_graph(*g);
            if temporal.vt_end == Timestamp::END_OF_TIME {
                asserted.push(f);
            } else {
                deleted.push(f);
            }
        }
    }
    if schema_changed {
        let stats = materialize_at(store, to)?;
        put_applied(store, to)?;
        return Ok(Some(stats));
    }

    let old_snap = store.snapshot(from);
    let schema = Schema::load(store, &old_snap, &graphs)?;
    let old = StoreFacts {
        snap: old_snap,
        graphs: &graphs,
        removed: HashSet::new(),
        added: MemFacts::default(),
    };
    let live = live_inferred(&store.snapshot(to), &graphs)?;

    // 1. Over-delete: inferred facts with a derivation through a deleted fact.
    let mut over: HashSet<Fact> = HashSet::new();
    let mut queue: VecDeque<Fact> = deleted.iter().cloned().collect();
    while let Some(f) = queue.pop_front() {
        for c in schema.consequences(&f, &old)? {
            if live.contains_key(&c) && over.insert(c.clone()) {
                queue.push_back(c);
            }
        }
    }

    // 2. Re-derive: over-deleted facts still supported by what remains.
    let mut new = StoreFacts {
        snap: store.snapshot(to),
        graphs: &graphs,
        removed: over.clone(),
        added: MemFacts::default(),
    };
    let mut rederived: Vec<Fact> = Vec::new();
    loop {
        let mut found = Vec::new();
        for x in &new.removed {
            if schema.supported(x, &new)? {
                found.push(x.clone());
            }
        }
        if found.is_empty() {
            break;
        }
        for x in found {
            new.removed.remove(&x);
            rederived.push(x);
        }
    }

    // 3. Insert: forward from asserted base facts and re-derived facts.
    let mut added: Vec<Fact> = Vec::new();
    let mut queue: VecDeque<Fact> = asserted.into_iter().chain(rederived).collect();
    while let Some(f) = queue.pop_front() {
        for c in schema.consequences(&f, &new)? {
            if !new.present(&c)? {
                new.added.insert(c.clone());
                if new.removed.remove(&c) {
                    // Over-deleted, then derived again: still stored.
                } else {
                    added.push(c.clone());
                }
                queue.push_back(c);
            }
        }
    }

    // What stays over-deleted is closed.
    let close: Vec<(GraphId, Triple)> = new
        .removed
        .iter()
        .filter_map(|f| live.get(f).cloned())
        .collect();
    drop(new);
    drop(old);
    write(store, &mut graphs, &added, &close)?;
    put_applied(store, to)?;
    Ok(Some(MaterializationStats {
        asserted: added.len() as u64,
        closed: close.len() as u64,
        derived_triples: (live.len() + added.len()).saturating_sub(close.len()) as u64,
        full: false,
    }))
}

/// The node a predicate or class IRI names (`term::iri_to_node_id`) — for
/// schema triples such as `<p> rdfs:subPropertyOf <q>`.
pub fn predicate_node(pred: &str) -> NodeId {
    term::iri_to_node_id(pred)
}

/// Deprecated alias of [`predicate_node`]; now the canonical IRI mapping
/// (it used to hash with a different byte order, so schema loaded as RDF
/// never matched).
pub fn uri_to_node_id(uri: &str) -> NodeId {
    term::iri_to_node_id(uri)
}
