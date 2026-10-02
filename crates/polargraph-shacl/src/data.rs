//! The data a validation reads: a dataset of graphs at a snapshot, optionally
//! with an uncommitted overlay.

use std::collections::{HashMap, HashSet};

use polargraph_core::{
    id::{GraphId, NodeId},
    skolem, term,
    triple::Triple,
    value::Value,
};
use polargraph_storage::{keys, GraphScope, Snapshot, StorageError};

use crate::vocab::{RDFS_SUBCLASS_OF, RDF_TYPE};

/// An RDF object: a node or a literal value.
#[derive(Debug, Clone, PartialEq)]
pub enum Obj {
    Node(NodeId),
    Lit(Value),
}

impl Obj {
    /// The object slot it occupies in the quad index (node id or value hash).
    pub fn key(&self) -> NodeId {
        match self {
            Obj::Node(n) => *n,
            Obj::Lit(v) => keys::value_object(v),
        }
    }

    pub fn as_node(&self) -> Option<NodeId> {
        match self {
            Obj::Node(n) => Some(*n),
            Obj::Lit(_) => None,
        }
    }

    fn of(triple: &Triple) -> Option<(NodeId, String, Obj)> {
        match triple {
            Triple::Relation {
                subject,
                predicate,
                object,
                ..
            } => Some((*subject, predicate.0.clone(), Obj::Node(*object))),
            Triple::Property {
                subject,
                predicate,
                value,
                ..
            } => Some((*subject, predicate.0.clone(), Obj::Lit(value.clone()))),
            _ => None,
        }
    }
}

/// Uncommitted changes to validate as if applied: adds count in the view
/// whatever their graph; a retraction removes that quad's contribution from
/// the graph it names (the same `(s, p, o)` in another dataset graph stays).
#[derive(Debug, Clone, Default)]
pub struct Overlay {
    pub adds: Vec<Triple>,
    pub retractions: Vec<(GraphId, NodeId, String, Obj)>,
}

/// Read access to a dataset (graphs in `scope` at a snapshot) plus an
/// optional overlay.
pub struct DataView<'a> {
    snap: &'a Snapshot,
    scope: GraphScope,
    adds: Vec<(NodeId, String, Obj)>,
    retracted: HashSet<(GraphId, NodeId, String, NodeId)>,
}

type Spo = (NodeId, String, Obj);

impl<'a> DataView<'a> {
    pub fn new(snap: &'a Snapshot, scope: GraphScope, overlay: &Overlay) -> Self {
        Self {
            snap,
            scope,
            adds: overlay.adds.iter().filter_map(Obj::of).collect(),
            retracted: overlay
                .retractions
                .iter()
                .map(|(g, s, p, o)| (*g, *s, p.clone(), o.key()))
                .collect(),
        }
    }

    /// Every node the overlay mentions (subjects and node objects).
    pub fn touched(&self) -> HashSet<NodeId> {
        let mut out = HashSet::new();
        for (s, _, o) in &self.adds {
            out.insert(*s);
            out.extend(o.as_node());
        }
        for (_, s, _, o) in &self.retracted {
            out.insert(*s);
            out.insert(*o);
        }
        out
    }

    /// Matching `(s, p, o)` triples: stored ones (minus retractions) plus
    /// overlay adds, de-duplicated.
    fn matching(
        &self,
        s: Option<&NodeId>,
        p: Option<&str>,
        o: Option<&Obj>,
    ) -> Result<Vec<Spo>, StorageError> {
        let o_key = o.map(Obj::key);
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for (g, t) in self.snap.scan_scoped(s, p, o_key.as_ref(), &self.scope)? {
            let Some((ts, tp, to)) = Obj::of(&t) else {
                continue;
            };
            if self.retracted.contains(&(g, ts, tp.clone(), to.key())) {
                continue;
            }
            if seen.insert((ts, tp.clone(), to.key())) {
                out.push((ts, tp, to));
            }
        }
        for (as_, ap, ao) in &self.adds {
            if s.is_some_and(|s| s != as_)
                || p.is_some_and(|p| p != ap)
                || o_key.is_some_and(|k| k != ao.key())
            {
                continue;
            }
            if seen.insert((*as_, ap.clone(), ao.key())) {
                out.push((*as_, ap.clone(), ao.clone()));
            }
        }
        Ok(out)
    }

    /// Values of `p` on `s`.
    pub fn objects(&self, s: &NodeId, p: &str) -> Result<Vec<Obj>, StorageError> {
        Ok(self
            .matching(Some(s), Some(p), None)?
            .into_iter()
            .map(|(_, _, o)| o)
            .collect())
    }

    /// Subjects with `p` = `o`.
    pub fn subjects(&self, p: &str, o: &Obj) -> Result<Vec<NodeId>, StorageError> {
        Ok(self
            .matching(None, Some(p), Some(o))?
            .into_iter()
            .map(|(s, _, _)| s)
            .collect())
    }

    /// Every subject of `p`.
    pub fn subjects_of(&self, p: &str) -> Result<HashSet<NodeId>, StorageError> {
        Ok(self
            .matching(None, Some(p), None)?
            .into_iter()
            .map(|(s, _, _)| s)
            .collect())
    }

    /// Every node object of `p`.
    pub fn objects_of(&self, p: &str) -> Result<Vec<Obj>, StorageError> {
        Ok(self
            .matching(None, Some(p), None)?
            .into_iter()
            .map(|(_, _, o)| o)
            .collect())
    }

    /// Predicates used on `s`.
    pub fn predicates_of(&self, s: &NodeId) -> Result<HashSet<String>, StorageError> {
        Ok(self
            .matching(Some(s), None, None)?
            .into_iter()
            .map(|(_, p, _)| p)
            .collect())
    }

    /// Classes of `n` (`rdf:type`), closed over `rdfs:subClassOf`.
    pub fn types(&self, n: &NodeId) -> Result<HashSet<NodeId>, StorageError> {
        let mut out = HashSet::new();
        let mut todo: Vec<NodeId> = self
            .objects(n, RDF_TYPE)?
            .iter()
            .filter_map(Obj::as_node)
            .collect();
        while let Some(c) = todo.pop() {
            if out.insert(c) {
                todo.extend(
                    self.objects(&c, RDFS_SUBCLASS_OF)?
                        .iter()
                        .filter_map(Obj::as_node),
                );
            }
        }
        Ok(out)
    }

    /// Instances of `class` or any of its subclasses.
    pub fn instances(&self, class: &NodeId) -> Result<HashSet<NodeId>, StorageError> {
        let mut classes = HashSet::new();
        let mut todo = vec![*class];
        while let Some(c) = todo.pop() {
            if classes.insert(c) {
                todo.extend(self.subjects(RDFS_SUBCLASS_OF, &Obj::Node(c))?);
            }
        }
        let mut out = HashSet::new();
        for c in classes {
            out.extend(self.subjects(RDF_TYPE, &Obj::Node(c))?);
        }
        Ok(out)
    }

    /// The IRI of `n` (dictionary, else `urn:uuid:`).
    pub fn iri(&self, n: &NodeId) -> Result<String, StorageError> {
        Ok(self
            .snap
            .store()
            .iri_of(n)?
            .unwrap_or_else(|| term::fallback_iri(n)))
    }

    /// Whether `n` is a (skolemized) blank node.
    pub fn is_blank(&self, n: &NodeId) -> Result<bool, StorageError> {
        Ok(skolem::is_skolem_iri(&self.iri(n)?))
    }
}

/// Group helper used by the shapes loader: `p` → values of `s`.
pub(crate) fn properties(
    view: &DataView<'_>,
    s: &NodeId,
) -> Result<HashMap<String, Vec<Obj>>, StorageError> {
    let mut out: HashMap<String, Vec<Obj>> = HashMap::new();
    for (_, p, o) in view.matching(Some(s), None, None)? {
        out.entry(p).or_default().push(o);
    }
    Ok(out)
}
