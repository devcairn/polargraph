//! Which graphs each vector node lives in, for graph-ACL checks on vector
//! candidates (`docs/design/hybrid-search.md`, decision A).
//!
//! A vector hit is visible to a caller iff the node has a live quad (as
//! subject) in a graph the caller can read. Checking that with a store scan
//! per candidate cost ~95 % of `SearchVectorInSet` on large candidate sets
//! (cb-bench, team scale). This index keeps `node → graphs` for the nodes
//! that have vectors, so a check is a map lookup plus a bitmap test.
//!
//! Like [`crate::type_index::TypeIndex`] it is driven by the change log:
//! every check first catches up, recomputing the graphs of each touched
//! subject it holds, so all write paths (and replicas) are covered. The
//! initial build is one pass over `spog` keys (no value decoding), run in
//! the background; until it is ready, and for nodes it doesn't hold yet
//! (vectors added later), callers fall back to a per-node lookup. Graph
//! operations (drop / copy / move) and a change log pruned past the index
//! trigger a rebuild. Reads are at the latest state only: time-travel reads
//! use the per-node check.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, RwLock},
};

use polargraph_core::{
    id::{GraphId, NodeId},
    temporal::Timestamp,
};
use polargraph_storage::{GraphScope, RoaringBitmap, StorageError, TripleStore};
use tracing::{info, warn};

/// Change-log records read per batch while catching up.
const SYNC_BATCH: usize = 1_000;

struct State {
    /// The initial build has completed and `applied` is meaningful.
    ready: bool,
    /// A build is running (in the background).
    building: bool,
    graphs: HashMap<NodeId, Vec<GraphId>>,
    /// Commits up to here are reflected.
    applied: Timestamp,
}

pub struct VectorVisibility {
    store: TripleStore,
    state: RwLock<State>,
    /// Serialises catch-ups.
    sync_lock: Mutex<()>,
}

impl VectorVisibility {
    pub fn new(store: TripleStore) -> Arc<Self> {
        Arc::new(Self {
            store,
            state: RwLock::new(State {
                ready: false,
                building: false,
                graphs: HashMap::new(),
                applied: Timestamp(0),
            }),
            sync_lock: Mutex::new(()),
        })
    }

    /// Build in a background thread (no-op if a build is running).
    pub fn spawn_build(self: &Arc<Self>) {
        {
            let mut state = self.state.write().unwrap();
            if state.building {
                return;
            }
            state.building = true;
            state.ready = false;
        }
        let this = Arc::clone(self);
        std::thread::spawn(move || {
            if let Err(e) = this.build() {
                warn!("vector visibility index build failed: {e}");
                this.state.write().unwrap().building = false;
            }
        });
    }

    /// One pass over `spog` for every node that has a vector.
    fn build(&self) -> Result<(), StorageError> {
        let at = Timestamp(self.store.oracle_ts());
        let nodes: HashSet<NodeId> = self.store.vector_node_ids();
        let graphs = if nodes.is_empty() {
            HashMap::new()
        } else {
            self.store
                .snapshot(at)
                .all_subject_graphs(|s| nodes.contains(s))?
        };
        info!(nodes = graphs.len(), "vector visibility index built");
        let mut state = self.state.write().unwrap();
        *state = State {
            ready: true,
            building: false,
            graphs,
            applied: at,
        };
        Ok(())
    }

    /// Catch up with the change log. `false` when the index isn't usable
    /// (building, or rebuilding after a graph operation / pruned log).
    fn sync(self: &Arc<Self>) -> Result<bool, StorageError> {
        let _guard = self.sync_lock.lock().unwrap();
        let mut applied = {
            let state = self.state.read().unwrap();
            if !state.ready {
                return Ok(false);
            }
            state.applied
        };
        if applied < self.store.changes_floor()? {
            self.spawn_build();
            return Ok(false);
        }
        let mut touched: HashSet<NodeId> = HashSet::new();
        loop {
            let records = self.store.changes_after(applied, SYNC_BATCH)?;
            let Some(last) = records.last() else { break };
            applied = last.commit_ts;
            for record in &records {
                if !record.graph_ops.is_empty() {
                    // Drops, copies and moves don't list their quads.
                    self.spawn_build();
                    return Ok(false);
                }
                touched.extend(record.quads.iter().map(|(_, t)| t.subject()));
            }
            if records.len() < SYNC_BATCH {
                break;
            }
        }
        // Recompute only the subjects the index holds.
        let held: Vec<NodeId> = {
            let state = self.state.read().unwrap();
            touched
                .into_iter()
                .filter(|s| state.graphs.contains_key(s))
                .collect()
        };
        let snap = self.store.snapshot(applied);
        let mut current = Vec::with_capacity(held.len());
        for s in held {
            current.push((s, snap.subject_graphs(&s)?));
        }
        let mut state = self.state.write().unwrap();
        for (s, graphs) in current {
            state.graphs.insert(s, graphs);
        }
        state.applied = applied;
        Ok(true)
    }

    /// The nodes of `candidates` visible to a reader of `readable` (`None` =
    /// every graph) within `scope` (`None` = every graph), in order.
    /// `None` when the index isn't ready (use the per-node check).
    pub fn filter(
        self: &Arc<Self>,
        candidates: &[NodeId],
        readable: Option<&RoaringBitmap>,
        scope: Option<&GraphScope>,
    ) -> Result<Option<Vec<bool>>, StorageError> {
        if !self.sync()? {
            return Ok(None);
        }
        let admits = |g: &GraphId| {
            readable.map_or(true, |r| r.contains(g.0)) && scope.map_or(true, |s| s.admits(*g))
        };
        let mut out = Vec::with_capacity(candidates.len());
        let mut missing: Vec<usize> = Vec::new();
        {
            let state = self.state.read().unwrap();
            for (i, n) in candidates.iter().enumerate() {
                match state.graphs.get(n) {
                    Some(graphs) => out.push(graphs.iter().any(admits)),
                    None => {
                        out.push(false);
                        missing.push(i);
                    }
                }
            }
        }
        if !missing.is_empty() {
            // Nodes the index doesn't hold yet (e.g. vectors added since the
            // build): look them up once and keep them.
            let (snap, applied) = {
                let state = self.state.read().unwrap();
                (self.store.snapshot(state.applied), state.applied)
            };
            let mut found = Vec::with_capacity(missing.len());
            for i in missing {
                let graphs = snap.subject_graphs(&candidates[i])?;
                out[i] = graphs.iter().any(admits);
                found.push((candidates[i], graphs));
            }
            let mut state = self.state.write().unwrap();
            if state.ready && state.applied == applied {
                state.graphs.extend(found);
            }
        }
        Ok(Some(out))
    }

    #[cfg(test)]
    fn wait_ready(&self) {
        for _ in 0..500 {
            if self.state.read().unwrap().ready {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("vector visibility index not ready");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polargraph_core::{
        schema::StorageMode, temporal::BiTemporalRange, triple::Predicate, triple::Triple,
        value::Value,
    };
    use polargraph_storage::WriteMode;

    fn put(store: &TripleStore, n: NodeId, g: GraphId) {
        let mut tx = store.begin();
        tx.insert_in(
            Triple::Property {
                subject: n,
                predicate: Predicate::new("urn:p"),
                value: Value::Text("x".into()),
                temporal: BiTemporalRange::assert_now(Timestamp::now()),
            },
            g,
            WriteMode::Add,
        );
        tx.commit().unwrap();
    }

    #[test]
    fn follows_writes_graph_ops_and_readers() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = TripleStore::open(dir.path()).unwrap();
        let g1 = store.create_graph("urn:g:1", &[]).unwrap();
        let (a, b) = (
            NodeId(uuid::Uuid::from_u128(1)),
            NodeId(uuid::Uuid::from_u128(2)),
        );
        put(&store, a, g1);
        for n in [a, b] {
            store
                .insert_vector("s", n, vec![1.0, 0.0], StorageMode::Memory)
                .unwrap();
        }
        let vis = VectorVisibility::new(store.clone());
        vis.spawn_build();
        vis.wait_ready();

        let only_default: RoaringBitmap = [GraphId::DEFAULT.0].into_iter().collect();
        let with_g1: RoaringBitmap = [GraphId::DEFAULT.0, g1.0].into_iter().collect();
        let check = |r: &RoaringBitmap| vis.filter(&[a, b], Some(r), None).unwrap().unwrap();
        assert_eq!(check(&with_g1), vec![true, false], "b has no quads");
        assert_eq!(check(&only_default), vec![false, false]);

        // A later write is seen on the next check.
        put(&store, b, GraphId::DEFAULT);
        assert_eq!(check(&only_default), vec![false, true]);

        // A graph operation triggers a rebuild; meanwhile the caller falls
        // back, and afterwards the result reflects the drop.
        store.drop_graph(g1).unwrap();
        let during = vis.filter(&[a, b], Some(&with_g1), None).unwrap();
        if during.is_none() {
            vis.wait_ready();
        }
        assert_eq!(check(&with_g1), vec![false, true]);
    }
}
