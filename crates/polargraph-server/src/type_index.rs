//! `rdf:type` membership index, driven by the change log
//! (`docs/design/cypher-rdf.md` §2.5).
//!
//! Keyed by class node (`iri_to_node_id(class IRI)`), so a type name resolves
//! through the vocabulary: `iri_to_node_id(&vocab.expand(name))`.
//!
//! Every read first catches up with the change log ([`TypeIndex::sync`]), so
//! a class introduced by any write path — `Insert`, `ApplyChanges`, Cypher,
//! SPARQL Update, imports, or a replicated batch — is visible at once, with
//! no per-RPC hooks. Membership of each subject a commit touches is
//! recomputed from the store (all graphs, valid now), which handles closes
//! and the same type asserted in several graphs. When the index has fallen
//! behind the change log's floor (pruned), it is rebuilt from a scan.

use std::{
    collections::{HashMap, HashSet},
    sync::{Mutex, RwLock},
};

use polargraph_core::{
    id::NodeId,
    schema::{
        BUILTIN_HAS_ACCESS_PRED, BUILTIN_HAS_ACCESS_TYPE_PRED, BUILTIN_HAS_GRAPH_ACCESS_PRED,
        BUILTIN_MEMBER_OF_PRED,
    },
    temporal::Timestamp,
    triple::Triple,
};
use polargraph_storage::{legacy::RDF_TYPE, StorageError, TripleStore};
use tracing::info;

/// Change-log records read per batch while catching up.
const SYNC_BATCH: usize = 1_000;

struct State {
    /// Class → current instances.
    by_class: HashMap<NodeId, HashSet<NodeId>>,
    /// Subject → current classes (to update `by_class` on a change).
    of_subject: HashMap<NodeId, HashSet<NodeId>>,
    /// Commits up to here are reflected.
    applied: Timestamp,
}

/// What a catch-up saw, for dependent caches.
#[derive(Debug, Default, Clone, Copy)]
pub struct SyncOutcome {
    /// Some `rdf:type` membership changed (or the index was rebuilt).
    pub types: bool,
    /// Node-level access-control triples changed.
    pub node_access: bool,
    /// Group memberships or graph grants changed.
    pub graph_access: bool,
}

impl SyncOutcome {
    fn all() -> Self {
        Self {
            types: true,
            node_access: true,
            graph_access: true,
        }
    }
}

pub struct TypeIndex {
    store: TripleStore,
    state: RwLock<State>,
    /// Serialises catch-ups (readers don't wait on it).
    sync_lock: Mutex<()>,
}

impl TypeIndex {
    pub fn build(store: TripleStore) -> Result<Self, StorageError> {
        let index = Self {
            store,
            state: RwLock::new(State {
                by_class: HashMap::new(),
                of_subject: HashMap::new(),
                applied: Timestamp(0),
            }),
            sync_lock: Mutex::new(()),
        };
        index.rebuild()?;
        Ok(index)
    }

    fn rebuild(&self) -> Result<(), StorageError> {
        let at = Timestamp(self.store.oracle_ts());
        let mut state = State {
            by_class: HashMap::new(),
            of_subject: HashMap::new(),
            applied: at,
        };
        for t in self.store.snapshot(at).scan_by_predicate(RDF_TYPE)? {
            if let Triple::Relation {
                subject, object, ..
            } = t
            {
                state.by_class.entry(object).or_default().insert(subject);
                state.of_subject.entry(subject).or_default().insert(object);
            }
        }
        info!(classes = state.by_class.len(), "type index built");
        *self.state.write().unwrap() = state;
        Ok(())
    }

    /// Catch up with the change log.
    pub fn sync(&self) -> Result<SyncOutcome, StorageError> {
        let _guard = self.sync_lock.lock().unwrap();
        let mut applied = self.state.read().unwrap().applied;
        if applied < self.store.changes_floor()? {
            self.rebuild()?;
            return Ok(SyncOutcome::all());
        }
        let mut outcome = SyncOutcome::default();
        let mut touched: HashSet<NodeId> = HashSet::new();
        loop {
            let records = self.store.changes_after(applied, SYNC_BATCH)?;
            let Some(last) = records.last() else { break };
            applied = last.commit_ts;
            for record in &records {
                for (_, t) in &record.quads {
                    let p = t.predicate().0.as_str();
                    if p == RDF_TYPE {
                        if let Triple::Relation { subject, .. } = t {
                            touched.insert(*subject);
                        }
                    } else if p == BUILTIN_MEMBER_OF_PRED || p == BUILTIN_HAS_GRAPH_ACCESS_PRED {
                        outcome.graph_access = true;
                        outcome.node_access |= p == BUILTIN_MEMBER_OF_PRED;
                    } else if p == BUILTIN_HAS_ACCESS_PRED || p == BUILTIN_HAS_ACCESS_TYPE_PRED {
                        outcome.node_access = true;
                    }
                }
            }
            if records.len() < SYNC_BATCH {
                break;
            }
        }

        let mut current: Vec<(NodeId, HashSet<NodeId>)> = Vec::with_capacity(touched.len());
        if !touched.is_empty() {
            let snap = self.store.snapshot(applied);
            for subject in touched {
                let classes = snap
                    .scan_by_subject_predicate(&subject, RDF_TYPE)?
                    .into_iter()
                    .filter_map(|t| match t {
                        Triple::Relation { object, .. } => Some(object),
                        _ => None,
                    })
                    .collect();
                current.push((subject, classes));
            }
        }

        let mut state = self.state.write().unwrap();
        state.applied = applied;
        for (subject, classes) in current {
            let old = state.of_subject.remove(&subject).unwrap_or_default();
            if old == classes {
                if !classes.is_empty() {
                    state.of_subject.insert(subject, classes);
                }
                continue;
            }
            outcome.types = true;
            for c in old.difference(&classes) {
                if let Some(members) = state.by_class.get_mut(c) {
                    members.remove(&subject);
                    if members.is_empty() {
                        state.by_class.remove(c);
                    }
                }
            }
            for c in classes.difference(&old) {
                state.by_class.entry(*c).or_default().insert(subject);
            }
            if !classes.is_empty() {
                state.of_subject.insert(subject, classes);
            }
        }
        Ok(outcome)
    }

    /// Current instances of any of `classes` (as of the last [`Self::sync`]).
    pub fn instances_of(&self, classes: &[NodeId]) -> HashSet<NodeId> {
        let state = self.state.read().unwrap();
        classes
            .iter()
            .filter_map(|c| state.by_class.get(c))
            .flatten()
            .copied()
            .collect()
    }
}
