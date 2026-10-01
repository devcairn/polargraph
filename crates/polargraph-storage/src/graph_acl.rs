//! Graph-level access control (`docs/design/graph-acl.md`, plan §2.8).
//!
//! A grant is a `(principal) -[HAS_GRAPH_ACCESS]-> (graph IRI node)` relation
//! in the system graph, annotated (RDF-star) with its
//! [`GraphAccessLevel`] under `GRAPH_ACCESS_LEVEL`. A principal is a user or
//! a group; users inherit the grants of the groups they are `MEMBER_OF`.
//!
//! [`GraphAccessIndex`] turns the grants into per-user bitmaps of graph ids.
//! The default graph is readable and writable by everyone; every other graph
//! needs a grant (deny by default).

use std::{collections::HashMap, sync::Arc};

use polargraph_core::{
    id::{EdgeId, GraphId, NodeId},
    schema::{
        GraphAccessLevel, BUILTIN_GRAPH_ACCESS_LEVEL_PRED, BUILTIN_HAS_GRAPH_ACCESS_PRED,
        BUILTIN_MEMBER_OF_PRED,
    },
    temporal::{BiTemporalRange, Timestamp},
    term,
    triple::{Predicate, Triple},
    value::Value,
};
use roaring::RoaringBitmap;

use crate::{
    error::StorageError,
    graphs::close_at,
    mvcc::WriteMode,
    store::{EdgeAnnotationValue, GraphScope, TripleStore},
};

/// One live graph grant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphGrant {
    pub principal: NodeId,
    pub graph: GraphId,
    pub level: GraphAccessLevel,
}

impl TripleStore {
    /// Grant `principal` (a user or group) `level` on named graph `g`,
    /// replacing any earlier level. The default graph needs no grant.
    pub fn grant_graph_access(
        &self,
        principal: NodeId,
        g: GraphId,
        level: GraphAccessLevel,
    ) -> Result<(), StorageError> {
        let graph_node = self.graph_node(g).ok_or_else(|| {
            StorageError::Validation(format!(
                "{g} is not a named graph (the default graph needs no grant)"
            ))
        })?;
        let meta = self.system_graph()?;
        let edge_id = grant_edge(&principal, &graph_node);
        let temporal = BiTemporalRange::assert_now(Timestamp::now());
        let mut tx = self.begin();
        tx.insert_in(
            Triple::Relation {
                subject: principal,
                predicate: Predicate::new(BUILTIN_HAS_GRAPH_ACCESS_PRED),
                object: graph_node,
                edge_id,
                temporal,
            },
            meta,
            WriteMode::Add,
        );
        tx.insert_in(
            Triple::EdgeProperty {
                edge: edge_id,
                predicate: Predicate::new(BUILTIN_GRAPH_ACCESS_LEVEL_PRED),
                value: Value::Text(level.as_str().to_string()),
                temporal,
            },
            meta,
            WriteMode::Add,
        );
        tx.commit()?;
        Ok(())
    }

    /// Revoke `principal`'s grant on `g` (bitemporal: the grant's history
    /// stays queryable). Returns whether a live grant existed.
    pub fn revoke_graph_access(&self, principal: NodeId, g: GraphId) -> Result<bool, StorageError> {
        let (Some(graph_node), Some(meta)) =
            (self.graph_node(g), self.graph_id(crate::SYSTEM_GRAPH_IRI))
        else {
            return Ok(false);
        };
        let snap = self.snapshot(Timestamp(self.oracle_ts()));
        let live = snap.scan_scoped(
            Some(&principal),
            Some(BUILTIN_HAS_GRAPH_ACCESS_PRED),
            Some(&graph_node),
            &GraphScope::One(meta),
        )?;
        if live.is_empty() {
            return Ok(false);
        }
        let now = Timestamp::now();
        let mut tx = self.begin();
        for (_, t) in live {
            tx.insert_in(close_at(t, now), meta, WriteMode::Add);
        }
        tx.commit()?;
        Ok(true)
    }

    /// Every live graph grant.
    pub fn graph_grants(&self) -> Result<Vec<GraphGrant>, StorageError> {
        let Some(meta) = self.graph_id(crate::SYSTEM_GRAPH_IRI) else {
            return Ok(vec![]);
        };
        let ts = Timestamp(self.oracle_ts());
        let snap = self.snapshot(ts);
        let mut grants = Vec::new();
        for (_, t) in snap.scan_scoped(
            None,
            Some(BUILTIN_HAS_GRAPH_ACCESS_PRED),
            None,
            &GraphScope::One(meta),
        )? {
            let Triple::Relation {
                subject,
                object,
                edge_id,
                ..
            } = t
            else {
                continue;
            };
            let Some(graph) = self.graph_for_node(&object) else {
                continue;
            };
            let level = self
                .scan_edge_annotations(edge_id, ts)?
                .into_iter()
                .find(|a| a.predicate.0 == BUILTIN_GRAPH_ACCESS_LEVEL_PRED)
                .and_then(|a| match a.value {
                    EdgeAnnotationValue::Scalar(Value::Text(s)) => GraphAccessLevel::parse(&s),
                    _ => None,
                })
                .unwrap_or(GraphAccessLevel::Read);
            grants.push(GraphGrant {
                principal: subject,
                graph,
                level,
            });
        }
        Ok(grants)
    }
}

/// The deterministic edge id of `principal`'s grant on a graph node, so a
/// re-grant updates the same edge.
fn grant_edge(principal: &NodeId, graph_node: &NodeId) -> EdgeId {
    term::edge_id_for(
        &principal.to_string(),
        BUILTIN_HAS_GRAPH_ACCESS_PRED,
        &graph_node.to_string(),
    )
}

/// One user's effective graph access.
#[derive(Debug, Clone)]
pub struct UserGraphAccess {
    /// `at_least[l]`: graphs where the user's level is `l` or higher.
    at_least: [RoaringBitmap; 4],
    /// Readable graphs, the default graph included.
    readable: Arc<RoaringBitmap>,
}

impl UserGraphAccess {
    fn from_levels(levels: &HashMap<GraphId, GraphAccessLevel>) -> Self {
        let mut at_least: [RoaringBitmap; 4] = Default::default();
        for (g, level) in levels {
            for l in GraphAccessLevel::ALL.into_iter().filter(|l| l <= level) {
                at_least[l as usize].insert(g.0);
            }
        }
        let mut readable = at_least[GraphAccessLevel::Read as usize].clone();
        readable.insert(GraphId::DEFAULT.0);
        Self {
            at_least,
            readable: Arc::new(readable),
        }
    }

    /// Readable graph ids (for `Snapshot::with_readable_graphs`).
    pub fn readable(&self) -> Arc<RoaringBitmap> {
        Arc::clone(&self.readable)
    }

    /// The user's level on `g`. The default graph is `Write` for everyone.
    pub fn level(&self, g: GraphId) -> Option<GraphAccessLevel> {
        if g == GraphId::DEFAULT {
            return Some(GraphAccessLevel::Write);
        }
        GraphAccessLevel::ALL
            .into_iter()
            .rev()
            .find(|l| self.at_least[*l as usize].contains(g.0))
    }

    /// Graphs where the user has at least `level`, the default graph
    /// included up to `Write`.
    pub fn graphs_at_least(&self, level: GraphAccessLevel) -> RoaringBitmap {
        let mut graphs = self.at_least[level as usize].clone();
        if level <= GraphAccessLevel::Write {
            graphs.insert(GraphId::DEFAULT.0);
        }
        graphs
    }

    /// Whether the user has at least `level` on `g`.
    pub fn allows(&self, g: GraphId, level: GraphAccessLevel) -> bool {
        self.level(g).is_some_and(|l| l >= level)
    }
}

/// Effective graph access of every user, built from grants and group
/// membership.
#[derive(Debug, Clone)]
pub struct GraphAccessIndex {
    users: HashMap<NodeId, Arc<UserGraphAccess>>,
    /// Users with no grants: the default graph only.
    no_grants: Arc<UserGraphAccess>,
}

impl Default for GraphAccessIndex {
    fn default() -> Self {
        Self {
            users: HashMap::new(),
            no_grants: Arc::new(UserGraphAccess::from_levels(&HashMap::new())),
        }
    }
}

impl GraphAccessIndex {
    /// Build from the live grants and `MEMBER_OF` relations. A user's level on
    /// a graph is the highest of its own grant and its groups' grants.
    pub fn build(store: &TripleStore) -> Result<Self, StorageError> {
        let mut direct: HashMap<NodeId, HashMap<GraphId, GraphAccessLevel>> = HashMap::new();
        for grant in store.graph_grants()? {
            let slot = direct
                .entry(grant.principal)
                .or_default()
                .entry(grant.graph)
                .or_insert(grant.level);
            *slot = (*slot).max(grant.level);
        }
        let mut groups_of: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
        for t in store.scan_by_predicate(BUILTIN_MEMBER_OF_PRED)? {
            if let Triple::Relation {
                subject, object, ..
            } = t
            {
                groups_of.entry(subject).or_default().push(object);
            }
        }

        let mut users = HashMap::new();
        let principals = direct.keys().chain(groups_of.keys()).copied();
        for principal in principals.collect::<std::collections::HashSet<_>>() {
            let mut levels = direct.get(&principal).cloned().unwrap_or_default();
            for group in groups_of.get(&principal).into_iter().flatten() {
                for (g, level) in direct.get(group).into_iter().flatten() {
                    let slot = levels.entry(*g).or_insert(*level);
                    *slot = (*slot).max(*level);
                }
            }
            users.insert(principal, Arc::new(UserGraphAccess::from_levels(&levels)));
        }
        Ok(Self {
            users,
            ..Self::default()
        })
    }

    /// Effective access of `user` (the default graph only when it has no
    /// grants).
    pub fn for_user(&self, user: &NodeId) -> Arc<UserGraphAccess> {
        self.users
            .get(user)
            .cloned()
            .unwrap_or_else(|| Arc::clone(&self.no_grants))
    }
}
