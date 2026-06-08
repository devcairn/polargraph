//! gRPC service implementation.

use crate::{
    convert,
    proto::{
        polar_graph_service_server::PolarGraphService,
        search_vector_filtered_request::Filter,
        vector_seed_query_request::Filter as SeedFilter,
        BackupInfo as ProtoBackupInfo,
        BatchInsertError, BatchInsertVectorsRequest, BatchInsertVectorsResponse,
        CreateBackupRequest, CreateBackupResponse,
        GetEdgeTypeRequest, GetEdgeTypeResponse,
        GetNodeTypeRequest, GetNodeTypeResponse,
        InsertRequest, InsertResponse, InsertVectorRequest, InsertVectorResponse,
        ListBackupsRequest, ListBackupsResponse,
        ListEdgeTypesRequest, ListEdgeTypesResponse,
        ListNodeTypesRequest, ListNodeTypesResponse,
        ListPredicatesBetweenRequest, ListPredicatesBetweenResponse,
        PurgeOldBackupsRequest, PurgeOldBackupsResponse,
        QueryRequest, QueryResponse, ReachableRequest, ReachableResponse,
        RegisterEdgeTypeRequest, RegisterEdgeTypeResponse,
        RegisterNodeTypeRequest, RegisterNodeTypeResponse,
        ReplicaStatusRequest, ReplicaStatusResponse,
        RunRetentionRequest, RunRetentionResponse,
        ScoredBinding,
        SearchVectorFilteredRequest, SearchVectorFilteredResponse,
        SearchVectorInSetRequest, SearchVectorInSetResponse,
        SearchVectorRequest, SearchVectorResponse,
        StreamWalRequest, WalEntry,
        ValidateEdgeRequest, ValidateEdgeResponse,
        ValidateNodeRequest, ValidateNodeResponse,
        VectorSearchResult, VectorSeedQueryRequest, VectorSeedQueryResponse,
    },
};
use polargraph_core::{id::NodeId, schema::{RetentionPolicy, StorageMode}, triple::Triple, value::Value};
use polargraph_query::datalog::{
    execute_query, execute_query_seeded, reachable_from, reachable_from_hops, Bindings, Query,
    QueryError,
};
use polargraph_storage::{BackupManager, CompactionManager, EdgeTypeRegistry, NodeTypeRegistry, StorageError, TripleStore, WalStreamer};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

// ── Replica state ─────────────────────────────────────────────────────────────

/// Tracks WAL replication statistics for a replica instance.
pub struct ReplicaState {
    pub primary_address: String,
    pub last_catchup_at: AtomicI64,
    pub catchup_count: AtomicU64,
    pub last_applied_seq: AtomicU64,
}

impl ReplicaState {
    pub fn new(primary_address: String) -> Arc<Self> {
        Arc::new(Self {
            primary_address,
            last_catchup_at: AtomicI64::new(0),
            catchup_count: AtomicU64::new(0),
            last_applied_seq: AtomicU64::new(0),
        })
    }

    /// Record a successfully applied WAL batch at sequence number `seq`.
    pub fn record_catchup(&self, seq: u64) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as i64;
        self.last_catchup_at.store(now, Ordering::Relaxed);
        self.catchup_count.fetch_add(1, Ordering::Relaxed);
        self.last_applied_seq.store(seq, Ordering::Relaxed);
    }
}

// ── Server struct ─────────────────────────────────────────────────────────────

/// Per-type node ID cache.
///
/// Key: `__type` value string (e.g. `"Person"`).
/// Value: set of all NodeIds whose `__type` property equals that string.
///
/// Populated at startup from existing triples and updated incrementally on every
/// `Insert` commit that contains a `__type` property triple. All clones of
/// `PolarGraphServer` share the same underlying map via `Arc`.
type TypeCache = Arc<RwLock<HashMap<String, HashSet<NodeId>>>>;

#[derive(Clone)]
pub struct PolarGraphServer {
    store: TripleStore,
    registry: NodeTypeRegistry,
    edge_registry: EdgeTypeRegistry,
    type_cache: TypeCache,
    backup_manager: Option<Arc<BackupManager>>,
    /// Non-None when this server is a read replica.
    replica_state: Option<Arc<ReplicaState>>,
    /// Maximum time in milliseconds for a single query. 0 = no limit.
    query_timeout_ms: u64,
}

impl PolarGraphServer {
    /// Create a server without backup support.
    ///
    /// The `CreateBackup`, `ListBackups`, and `PurgeOldBackups` RPCs will
    /// return `FAILED_PRECONDITION`. Use [`Self::new_with_backup_dir`] to
    /// enable backup.
    pub fn new(store: TripleStore) -> Result<Self, StorageError> {
        Self::new_with_backup_dir(store, None)
    }

    /// Create a server with optional backup support.
    ///
    /// When `backup_dir` is `Some`, a `BackupManager` is opened on that
    /// directory (creating it if necessary) and all backup RPCs become
    /// available. When `None`, backup RPCs return `FAILED_PRECONDITION`.
    pub fn new_with_backup_dir(
        store: TripleStore,
        backup_dir: Option<&Path>,
    ) -> Result<Self, StorageError> {
        let registry = NodeTypeRegistry::new(store.clone())?;
        let edge_registry = EdgeTypeRegistry::new(store.clone())?;
        let type_cache = Arc::new(RwLock::new(Self::build_type_cache(&store)?));
        let backup_manager = backup_dir
            .map(|dir| BackupManager::open(dir, &store).map(Arc::new))
            .transpose()?;
        Ok(Self { store, registry, edge_registry, type_cache, backup_manager, replica_state: None, query_timeout_ms: 30_000 })
    }

    /// Set the maximum query execution time in milliseconds. 0 disables the timeout.
    pub fn with_query_timeout_ms(mut self, ms: u64) -> Self {
        self.query_timeout_ms = ms;
        self
    }

    /// Create a replica (read-only) server.
    ///
    /// Write RPCs will return `FAILED_PRECONDITION`. Returns an
    /// `Arc<ReplicaState>` that the caller should pass to
    /// `wal_client::run_replication`.
    pub fn new_replica(
        store: TripleStore,
        primary_address: &str,
    ) -> Result<(Self, Arc<ReplicaState>), StorageError> {
        let registry = NodeTypeRegistry::new(store.clone())?;
        let edge_registry = EdgeTypeRegistry::new(store.clone())?;
        let type_cache = Arc::new(RwLock::new(Self::build_type_cache(&store)?));
        let replica_state = ReplicaState::new(primary_address.to_owned());
        let server = Self {
            store,
            registry,
            edge_registry,
            type_cache,
            backup_manager: None,
            replica_state: Some(replica_state.clone()),
            query_timeout_ms: 30_000,
        };
        Ok((server, replica_state))
    }

    /// Expose the underlying store. Used in integration tests to plant
    /// triples with explicit timestamps without going through gRPC.
    pub fn store(&self) -> &TripleStore {
        &self.store
    }

    /// Scan existing `__type` triples and build the initial cache.
    /// Called once at startup; O(N) in the number of typed nodes.
    fn build_type_cache(store: &TripleStore) -> Result<HashMap<String, HashSet<NodeId>>, StorageError> {
        let snapshot = store.snapshot(store.begin().read_ts);
        let triples = snapshot.scan_by_predicate("__type")?;
        let mut cache: HashMap<String, HashSet<NodeId>> = HashMap::new();
        for triple in triples {
            if let Triple::Property { subject, value: Value::Text(type_name), .. } = triple {
                cache.entry(type_name).or_default().insert(subject);
            }
        }
        info!(types = cache.len(), "type cache built");
        Ok(cache)
    }

    /// Compute a query deadline from `query_timeout_ms`. Returns `None` when
    /// timeout is disabled (value is 0).
    fn make_deadline(&self) -> Option<Instant> {
        if self.query_timeout_ms == 0 {
            None
        } else {
            Some(Instant::now() + Duration::from_millis(self.query_timeout_ms))
        }
    }

    /// Returns a `FailedPrecondition` status if this is a read replica.
    fn check_not_replica(&self) -> Result<(), Status> {
        if self.store.is_replica() {
            Err(replica_not_writable())
        } else {
            Ok(())
        }
    }

    /// Update the cache for any `__type` property triples in a just-committed batch.
    /// Called after every successful `Insert` commit; acquires the write lock only
    /// when the batch actually contains a `__type` triple.
    fn update_type_cache(&self, triples: &[Triple]) {
        let updates: Vec<(NodeId, String)> = triples
            .iter()
            .filter_map(|t| match t {
                Triple::Property { subject, predicate, value: Value::Text(type_name), .. }
                    if predicate.0 == "__type" =>
                {
                    Some((*subject, type_name.clone()))
                }
                _ => None,
            })
            .collect();

        if updates.is_empty() {
            return;
        }

        let mut cache = self.type_cache.write().unwrap();
        for (subject, type_name) in updates {
            cache.entry(type_name).or_default().insert(subject);
        }
    }
}

// ── Service impl ──────────────────────────────────────────────────────────────

#[tonic::async_trait]
impl PolarGraphService for PolarGraphServer {
    type StreamWalStream = ReceiverStream<Result<WalEntry, Status>>;

    /// Insert one or more triples atomically.
    async fn insert(
        &self,
        request: Request<InsertRequest>,
    ) -> Result<Response<InsertResponse>, Status> {
        self.check_not_replica()?;
        let req = request.into_inner();

        if req.triples.is_empty() {
            return Err(Status::invalid_argument("insert request must contain at least one triple"));
        }

        // Convert proto triples → core triples.
        let triples: Vec<Triple> = req
            .triples
            .iter()
            .map(convert::triple_from_proto)
            .collect::<Result<_, _>>()?;

        debug!("insert: {} triple(s)", triples.len());

        // Extract any __type updates before consuming the vec.
        // This avoids a second allocation pass after the transaction.

        // Begin transaction, insert all, commit.
        let mut tx = self.store.begin();
        for triple in &triples {
            tx.insert(triple.clone());
        }
        let commit_ts = tx.commit().map_err(storage_err_to_status)?;

        // Incrementally update the type cache for any __type triples.
        self.update_type_cache(&triples);

        metrics::gauge!("polargraph_triples_total").increment(triples.len() as f64);

        Ok(Response::new(InsertResponse { commit_ts: commit_ts.0 }))
    }

    /// Execute a conjunctive query and return all satisfying bindings.
    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let req = request.into_inner();

        if req.patterns.is_empty() {
            return Err(Status::invalid_argument("query must contain at least one pattern"));
        }

        // Convert proto VarPatterns → datalog VarPatterns.
        let patterns: Vec<_> = req
            .patterns
            .iter()
            .map(convert::var_pattern_from_proto)
            .collect::<Result<_, _>>()?;

        // Build query.
        let mut query = Query::new();
        for p in patterns {
            query.patterns.push(p);
        }

        // Resolve snapshot timestamp:
        // as_of_tx_time takes priority over snapshot_ts; 0 on either = latest.
        let tx_ts = if req.as_of_tx_time != 0 { req.as_of_tx_time } else { req.snapshot_ts };
        let mut snapshot = if tx_ts == 0 {
            self.store.snapshot(self.store.begin().read_ts)
        } else {
            self.store.snapshot(polargraph_core::temporal::Timestamp(tx_ts))
        };

        // Apply valid-time filter when requested.
        if req.as_of_valid_time != 0 {
            snapshot = snapshot.with_vt_as_of(req.as_of_valid_time);
        }

        debug!(
            "query: {} pattern(s) at tx_ts={} vt_as_of={:?}",
            query.patterns.len(), snapshot.ts.0, snapshot.vt_as_of
        );

        let results = execute_query(&query, &snapshot, self.make_deadline())
            .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?;

        let bindings = results.iter().map(convert::binding_to_proto).collect();

        Ok(Response::new(QueryResponse { bindings }))
    }

    /// Insert or update a node's embedding vector in the named HNSW space.
    async fn insert_vector(
        &self,
        request: Request<InsertVectorRequest>,
    ) -> Result<Response<InsertVectorResponse>, Status> {
        self.check_not_replica()?;
        let req = request.into_inner();

        let node_id = convert::node_id_from_proto(
            req.node_id.as_ref().ok_or_else(|| Status::invalid_argument("node_id is required"))?,
        )?;

        if req.vector.is_empty() {
            return Err(Status::invalid_argument("vector must not be empty"));
        }

        let space = if req.space.is_empty() { "default" } else { &req.space };

        // Dimension validation against registered space def.
        if let Some(vs) = self.registry.get_space_def(space) {
            if req.vector.len() != vs.dimensions as usize {
                return Err(Status::invalid_argument(format!(
                    "space '{}' expects {} dimensions, got {}",
                    space, vs.dimensions, req.vector.len()
                )));
            }
        }

        let mode = self.registry
            .get_space_def(space)
            .map(|vs| vs.storage_mode)
            .unwrap_or(StorageMode::Memory);

        debug!("insert_vector: space={} node={} dim={} mode={:?}", space, node_id, req.vector.len(), mode);

        self.store
            .insert_vector(space, node_id, req.vector, mode)
            .map_err(storage_err_to_status)?;

        metrics::gauge!("polargraph_vector_spaces_total")
            .set(self.store.hnsw_space_count() as f64);

        Ok(Response::new(InsertVectorResponse {}))
    }

    /// Search for the k nearest neighbors of a query vector in a named space.
    async fn search_vector(
        &self,
        request: Request<SearchVectorRequest>,
    ) -> Result<Response<SearchVectorResponse>, Status> {
        let req = request.into_inner();

        if req.query.is_empty() {
            return Err(Status::invalid_argument("query vector must not be empty"));
        }
        let k = if req.k == 0 { 10 } else { req.k as usize };
        let space = if req.space.is_empty() { "default" } else { &req.space };

        debug!("search_vector: space={} dim={} k={k}", space, req.query.len());

        let hits = self.store.search_vector(space, req.query, k);
        let results = hits
            .into_iter()
            .map(|(id, score)| VectorSearchResult {
                node_id: Some(convert::node_id_to_proto(id)),
                similarity: score,
            })
            .collect();

        Ok(Response::new(SearchVectorResponse { results }))
    }

    /// Vector search with a node-type or reachability post-filter.
    async fn search_vector_filtered(
        &self,
        request: Request<SearchVectorFilteredRequest>,
    ) -> Result<Response<SearchVectorFilteredResponse>, Status> {
        let req = request.into_inner();

        if req.query.is_empty() {
            return Err(Status::invalid_argument("query vector must not be empty"));
        }
        let k = if req.k == 0 { 10 } else { req.k as usize };
        let space = if req.space.is_empty() { "default" } else { &req.space };
        let ef = k * 10;

        match req.filter {
            // ── NodeTypeFilter: O(1) cache read, no triple scan ───────────────
            Some(Filter::NodeTypeFilter(f)) => {
                // Clone the allowed set out from under the read lock so we don't
                // hold it across the (potentially slow) HNSW search.
                let allowed: HashSet<NodeId> = {
                    let cache = self.type_cache.read().unwrap();
                    cache.get(&f.type_name).cloned().unwrap_or_default()
                };

                debug!(
                    "search_vector_filtered(NodeType={}): {} candidates in cache",
                    f.type_name,
                    allowed.len()
                );

                // HNSW with large ef, then O(1)-per-candidate HashSet filter.
                let candidates = self.store.search_vector_ef(space, &req.query, ef, ef);
                let results: Vec<_> = candidates
                    .into_iter()
                    .filter(|(id, _)| allowed.contains(id))
                    .take(k)
                    .map(|(id, score)| VectorSearchResult {
                        node_id: Some(convert::node_id_to_proto(id)),
                        similarity: score,
                    })
                    .collect();

                Ok(Response::new(SearchVectorFilteredResponse { results }))
            }

            // ── ReachabilityFilter: graph traversal, unchanged ────────────────
            Some(Filter::ReachabilityFilter(f)) => {
                let from = convert::node_id_from_proto(
                    f.from_node.as_ref().ok_or_else(|| Status::invalid_argument("from_node is required"))?,
                )?;

                let snapshot = self.store.snapshot(self.store.begin().read_ts);
                let deadline = self.make_deadline();
                let allowed: HashSet<NodeId> = if f.max_hops == 0 {
                    reachable_from(from, &f.predicate, &snapshot, deadline)
                } else {
                    reachable_from_hops(from, &f.predicate, &snapshot, f.max_hops as usize, deadline)
                }
                .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?;

                let candidates = self.store.search_vector_ef(space, &req.query, ef, ef);
                let results: Vec<_> = candidates
                    .into_iter()
                    .filter(|(id, _)| allowed.contains(id))
                    .take(k)
                    .map(|(id, score)| VectorSearchResult {
                        node_id: Some(convert::node_id_to_proto(id)),
                        similarity: score,
                    })
                    .collect();

                Ok(Response::new(SearchVectorFilteredResponse { results }))
            }

            None => Err(Status::invalid_argument("filter must be set")),
        }
    }

    /// Score an explicit set of node IDs against a query; return top-k.
    async fn search_vector_in_set(
        &self,
        request: Request<SearchVectorInSetRequest>,
    ) -> Result<Response<SearchVectorInSetResponse>, Status> {
        let req = request.into_inner();

        if req.query.is_empty() {
            return Err(Status::invalid_argument("query vector must not be empty"));
        }
        let k = if req.k == 0 { 10 } else { req.k as usize };
        let space = if req.space.is_empty() { "default" } else { &req.space };

        let allowed: Vec<polargraph_core::NodeId> = req.node_ids
            .iter()
            .map(convert::node_id_from_proto)
            .collect::<Result<_, _>>()?;

        debug!("search_vector_in_set: space={} set_size={} k={k}", space, allowed.len());

        let hits = self.store.search_vector_in_set(space, &req.query, k, &allowed);
        let results = hits
            .into_iter()
            .map(|(id, score)| VectorSearchResult {
                node_id: Some(convert::node_id_to_proto(id)),
                similarity: score,
            })
            .collect();

        Ok(Response::new(SearchVectorInSetResponse { results }))
    }

    /// Insert multiple vectors into a named space atomically.
    async fn batch_insert_vectors(
        &self,
        request: Request<BatchInsertVectorsRequest>,
    ) -> Result<Response<BatchInsertVectorsResponse>, Status> {
        self.check_not_replica()?;
        let req = request.into_inner();

        let space = if req.space.is_empty() { "default".to_string() } else { req.space.clone() };

        // Dimension check against registered space def.
        let expected_dims = self.registry.get_space_def(&space).map(|vs| vs.dimensions as usize);

        let mut items: Vec<(polargraph_core::NodeId, Vec<f32>)> = Vec::with_capacity(req.items.len());
        let mut pre_errors: Vec<BatchInsertError> = Vec::new();

        for (i, item) in req.items.iter().enumerate() {
            let node_id = match item.node_id.as_ref() {
                Some(id) => convert::node_id_from_proto(id)?,
                None => {
                    pre_errors.push(BatchInsertError {
                        index: i as u32,
                        message: "node_id is required".into(),
                    });
                    continue;
                }
            };
            if item.vector.is_empty() {
                pre_errors.push(BatchInsertError {
                    index: i as u32,
                    message: "vector must not be empty".into(),
                });
                continue;
            }
            if let Some(dims) = expected_dims {
                if item.vector.len() != dims {
                    pre_errors.push(BatchInsertError {
                        index: i as u32,
                        message: format!(
                            "space '{}' expects {} dimensions, got {}",
                            space, dims, item.vector.len()
                        ),
                    });
                    continue;
                }
            }
            items.push((node_id, item.vector.clone()));
        }

        if !pre_errors.is_empty() {
            return Ok(Response::new(BatchInsertVectorsResponse {
                count_inserted: 0,
                errors: pre_errors,
            }));
        }

        let mode = self.registry
            .get_space_def(&space)
            .map(|vs| vs.storage_mode)
            .unwrap_or(StorageMode::Memory);

        debug!("batch_insert_vectors: space={} count={} mode={:?}", space, items.len(), mode);

        let (count, errs) = self.store.batch_insert_vectors(&space, &items, mode);
        let errors = errs
            .into_iter()
            .map(|(i, e)| BatchInsertError { index: i as u32, message: e.to_string() })
            .collect();

        Ok(Response::new(BatchInsertVectorsResponse {
            count_inserted: count as u32,
            errors,
        }))
    }

    /// Transitive reachability from a start node along a single predicate.
    async fn reachable(
        &self,
        request: Request<ReachableRequest>,
    ) -> Result<Response<ReachableResponse>, Status> {
        let req = request.into_inner();

        let start = convert::node_id_from_proto(
            req.start.as_ref().ok_or_else(|| Status::invalid_argument("start node_id is required"))?,
        )?;

        if req.predicate.is_empty() {
            return Err(Status::invalid_argument("predicate must not be empty"));
        }

        let snapshot = self.store.snapshot(self.store.begin().read_ts);

        debug!(
            "reachable: start={} predicate={} max_hops={}",
            start, req.predicate, req.max_hops
        );

        let deadline = self.make_deadline();
        let reachable_set = if req.max_hops == 0 {
            reachable_from(start, &req.predicate, &snapshot, deadline)
        } else {
            reachable_from_hops(start, &req.predicate, &snapshot, req.max_hops as usize, deadline)
        }
        .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?;

        let node_ids = reachable_set
            .into_iter()
            .map(convert::node_id_to_proto)
            .collect();

        Ok(Response::new(ReachableResponse { node_ids }))
    }

    /// Register (or overwrite) a node type schema.
    async fn register_node_type(
        &self,
        request: Request<RegisterNodeTypeRequest>,
    ) -> Result<Response<RegisterNodeTypeResponse>, Status> {
        self.check_not_replica()?;
        let req = request.into_inner();
        let def_proto = req.definition.ok_or_else(|| Status::invalid_argument("definition is required"))?;
        let def = convert::node_type_def_from_proto(&def_proto)?;

        debug!("register_node_type: type={}", def.type_name);

        self.registry.register_type(def).map_err(storage_err_to_status)?;
        Ok(Response::new(RegisterNodeTypeResponse {}))
    }

    /// Look up a registered node type by name.
    async fn get_node_type(
        &self,
        request: Request<GetNodeTypeRequest>,
    ) -> Result<Response<GetNodeTypeResponse>, Status> {
        let req = request.into_inner();
        if req.type_name.is_empty() {
            return Err(Status::invalid_argument("type_name must not be empty"));
        }

        let definition = self.registry.get_type(&req.type_name).as_ref().map(convert::node_type_def_to_proto);
        Ok(Response::new(GetNodeTypeResponse { definition }))
    }

    /// Return all registered node type schemas.
    async fn list_node_types(
        &self,
        _request: Request<ListNodeTypesRequest>,
    ) -> Result<Response<ListNodeTypesResponse>, Status> {
        let definitions = self.registry.list_types().iter().map(convert::node_type_def_to_proto).collect();
        Ok(Response::new(ListNodeTypesResponse { definitions }))
    }

    /// Validate a property map against a registered schema.
    async fn validate_node(
        &self,
        request: Request<ValidateNodeRequest>,
    ) -> Result<Response<ValidateNodeResponse>, Status> {
        let req = request.into_inner();
        if req.type_name.is_empty() {
            return Err(Status::invalid_argument("type_name must not be empty"));
        }

        let props = req.properties
            .iter()
            .map(|(k, v)| convert::value_from_proto(v).map(|val| (k.clone(), val)))
            .collect::<Result<std::collections::HashMap<_, _>, _>>()?;

        match self.registry.validate_properties(&req.type_name, &props) {
            Ok(()) => Ok(Response::new(ValidateNodeResponse { valid: true, errors: vec![] })),
            Err(errs) => Ok(Response::new(ValidateNodeResponse {
                valid: false,
                errors: errs.iter().map(|e| e.message.clone()).collect(),
            })),
        }
    }

    /// Register (or overwrite) an edge type schema.
    async fn register_edge_type(
        &self,
        request: Request<RegisterEdgeTypeRequest>,
    ) -> Result<Response<RegisterEdgeTypeResponse>, Status> {
        self.check_not_replica()?;
        let req = request.into_inner();
        let def_proto = req.definition.ok_or_else(|| Status::invalid_argument("definition is required"))?;
        let def = convert::edge_type_def_from_proto(&def_proto)?;

        debug!("register_edge_type: predicate={}", def.predicate);

        self.edge_registry.register_edge_type(def).map_err(storage_err_to_status)?;
        Ok(Response::new(RegisterEdgeTypeResponse {}))
    }

    /// Look up a registered edge type by predicate name.
    async fn get_edge_type(
        &self,
        request: Request<GetEdgeTypeRequest>,
    ) -> Result<Response<GetEdgeTypeResponse>, Status> {
        let req = request.into_inner();
        if req.predicate.is_empty() {
            return Err(Status::invalid_argument("predicate must not be empty"));
        }

        let definition = self.edge_registry.get_edge_type(&req.predicate).as_ref().map(convert::edge_type_def_to_proto);
        Ok(Response::new(GetEdgeTypeResponse { definition }))
    }

    /// Return all registered edge type schemas.
    async fn list_edge_types(
        &self,
        _request: Request<ListEdgeTypesRequest>,
    ) -> Result<Response<ListEdgeTypesResponse>, Status> {
        let definitions = self.edge_registry.list_edge_types().iter().map(convert::edge_type_def_to_proto).collect();
        Ok(Response::new(ListEdgeTypesResponse { definitions }))
    }

    /// Validate an edge's endpoint types and property map against a schema.
    async fn validate_edge(
        &self,
        request: Request<ValidateEdgeRequest>,
    ) -> Result<Response<ValidateEdgeResponse>, Status> {
        let req = request.into_inner();
        if req.predicate.is_empty() {
            return Err(Status::invalid_argument("predicate must not be empty"));
        }

        let props = req.properties
            .iter()
            .map(|(k, v)| convert::value_from_proto(v).map(|val| (k.clone(), val)))
            .collect::<Result<std::collections::HashMap<_, _>, _>>()?;

        let subject_type = if req.subject_type.is_empty() { None } else { Some(req.subject_type.as_str()) };
        let object_type  = if req.object_type.is_empty()  { None } else { Some(req.object_type.as_str()) };

        match self.edge_registry.validate_edge(&req.predicate, subject_type, object_type, &props) {
            Ok(()) => Ok(Response::new(ValidateEdgeResponse { valid: true, errors: vec![] })),
            Err(errs) => Ok(Response::new(ValidateEdgeResponse {
                valid: false,
                errors: errs.iter().map(|e| e.message.clone()).collect(),
            })),
        }
    }

    /// Return all registered predicate names whose domain and range match.
    async fn list_predicates_between(
        &self,
        request: Request<ListPredicatesBetweenRequest>,
    ) -> Result<Response<ListPredicatesBetweenResponse>, Status> {
        let req = request.into_inner();
        if req.domain_type.is_empty() || req.range_type.is_empty() {
            return Err(Status::invalid_argument("domain_type and range_type must not be empty"));
        }

        let predicates = self.edge_registry.list_predicates_between(&req.domain_type, &req.range_type);
        Ok(Response::new(ListPredicatesBetweenResponse { predicates }))
    }

    /// ANN vector search seeded into a conjunctive Datalog graph query.
    async fn vector_seed_query(
        &self,
        request: Request<VectorSeedQueryRequest>,
    ) -> Result<Response<VectorSeedQueryResponse>, Status> {
        let req = request.into_inner();

        if req.query_vector.is_empty() {
            return Err(Status::invalid_argument("query_vector must not be empty"));
        }
        if req.seed_variable.is_empty() {
            return Err(Status::invalid_argument("seed_variable must not be empty"));
        }

        let k = if req.k == 0 { 10 } else { req.k as usize };
        let space = if req.space.is_empty() { "default" } else { &req.space };
        let ef = k * 10;
        let seed_var = req.seed_variable.clone();

        debug!(
            "vector_seed_query: space={} dim={} k={k} seed_var={} patterns={}",
            space,
            req.query_vector.len(),
            seed_var,
            req.patterns.len()
        );

        // Step 1: ANN search with optional pre-filter.
        let ann_hits: Vec<(NodeId, f32)> = match &req.filter {
            Some(SeedFilter::NodeTypeFilter(f)) => {
                let allowed: HashSet<NodeId> = {
                    let cache = self.type_cache.read().unwrap();
                    cache.get(&f.type_name).cloned().unwrap_or_default()
                };
                self.store
                    .search_vector_ef(space, &req.query_vector, ef, ef)
                    .into_iter()
                    .filter(|(id, _)| allowed.contains(id))
                    .take(k)
                    .collect()
            }
            Some(SeedFilter::ReachabilityFilter(f)) => {
                let from = convert::node_id_from_proto(
                    f.from_node
                        .as_ref()
                        .ok_or_else(|| Status::invalid_argument("from_node is required"))?,
                )?;
                let snap = self.store.snapshot(self.store.begin().read_ts);
                let deadline = self.make_deadline();
                let allowed: HashSet<NodeId> = if f.max_hops == 0 {
                    reachable_from(from, &f.predicate, &snap, deadline)
                } else {
                    reachable_from_hops(from, &f.predicate, &snap, f.max_hops as usize, deadline)
                }
                .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?;
                self.store
                    .search_vector_ef(space, &req.query_vector, ef, ef)
                    .into_iter()
                    .filter(|(id, _)| allowed.contains(id))
                    .take(k)
                    .collect()
            }
            None => self.store.search_vector(space, req.query_vector.clone(), k),
        };

        if ann_hits.is_empty() {
            return Ok(Response::new(VectorSeedQueryResponse { bindings: vec![] }));
        }

        // Step 2: build score map and seed bindings.
        let score_map: HashMap<NodeId, f32> =
            ann_hits.iter().map(|&(id, s)| (id, s)).collect();

        let initial: Vec<Bindings> = ann_hits
            .iter()
            .map(|(id, _)| {
                let mut b = Bindings::new();
                b.insert(seed_var.clone(), *id);
                b
            })
            .collect();

        // Step 3: if no patterns, return seed bindings directly; otherwise join.
        let snapshot = if req.snapshot_ts == 0 {
            self.store.snapshot(self.store.begin().read_ts)
        } else {
            self.store
                .snapshot(polargraph_core::temporal::Timestamp(req.snapshot_ts))
        };

        let patterns: Vec<_> = req
            .patterns
            .iter()
            .map(convert::var_pattern_from_proto)
            .collect::<Result<_, _>>()?;

        let mut query = Query::new();
        for p in patterns {
            query.patterns.push(p);
        }

        let results = execute_query_seeded(&query, &snapshot, initial, self.make_deadline())
            .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?;

        // Step 4: attach scores by looking up the seed variable in each result.
        let bindings = results
            .into_iter()
            .map(|binding| {
                let score = binding
                    .get(&seed_var)
                    .and_then(|id| score_map.get(id))
                    .copied()
                    .unwrap_or(0.0);
                ScoredBinding {
                    vars: binding
                        .iter()
                        .map(|(k, &v)| (k.clone(), convert::node_id_to_proto(v)))
                        .collect(),
                    score,
                }
            })
            .collect();

        Ok(Response::new(VectorSeedQueryResponse { bindings }))
    }

    // ── Backup ────────────────────────────────────────────────────────────────

    /// Create a new incremental RocksDB backup.
    async fn create_backup(
        &self,
        _request: Request<CreateBackupRequest>,
    ) -> Result<Response<CreateBackupResponse>, Status> {
        self.check_not_replica()?;
        let mgr = self.backup_manager.as_ref().ok_or_else(backup_not_configured)?;
        let info = mgr.create_backup().map_err(storage_err_to_status)?;
        info!(backup_id = info.backup_id, size_bytes = info.size_bytes, "backup created");
        metrics::gauge!("polargraph_backup_last_size_bytes").set(info.size_bytes as f64);
        Ok(Response::new(CreateBackupResponse {
            backup_id: info.backup_id,
            size_bytes: info.size_bytes,
            created_at: info.timestamp,
        }))
    }

    /// List all available backups.
    async fn list_backups(
        &self,
        _request: Request<ListBackupsRequest>,
    ) -> Result<Response<ListBackupsResponse>, Status> {
        let mgr = self.backup_manager.as_ref().ok_or_else(backup_not_configured)?;
        let backups = mgr
            .list_backups()
            .map_err(storage_err_to_status)?
            .into_iter()
            .map(|i| ProtoBackupInfo {
                backup_id: i.backup_id,
                timestamp: i.timestamp,
                size_bytes: i.size_bytes,
                num_files: i.num_files,
            })
            .collect();
        Ok(Response::new(ListBackupsResponse { backups }))
    }

    /// Delete all but the `keep_n` most recent backups.
    async fn purge_old_backups(
        &self,
        request: Request<PurgeOldBackupsRequest>,
    ) -> Result<Response<PurgeOldBackupsResponse>, Status> {
        let mgr = self.backup_manager.as_ref().ok_or_else(backup_not_configured)?;
        let keep_n = request.into_inner().keep_n;
        let deleted_count = mgr.purge_old_backups(keep_n).map_err(storage_err_to_status)?;
        info!(keep_n, deleted_count, "old backups purged");
        Ok(Response::new(PurgeOldBackupsResponse { deleted_count }))
    }

    /// Scan hexastore CFs and delete expired triples per the supplied policy.
    async fn run_retention(
        &self,
        request: Request<RunRetentionRequest>,
    ) -> Result<Response<RunRetentionResponse>, Status> {
        self.check_not_replica()?;
        let req = request.into_inner();
        let policy = RetentionPolicy {
            tx_age_secs: req.tx_age_secs,
            vt_lookback_secs: if req.vt_lookback_secs == 0 {
                None
            } else {
                Some(req.vt_lookback_secs)
            },
        };
        let mgr = CompactionManager::new(self.store.clone());
        let stats = mgr.run_retention(&policy).map_err(storage_err_to_status)?;
        info!(
            triples_scanned = stats.triples_scanned,
            triples_deleted = stats.triples_deleted,
            duration_ms = stats.duration_ms,
            "retention run complete"
        );
        metrics::counter!("polargraph_compaction_deleted_total")
            .increment(stats.triples_deleted as u64);
        Ok(Response::new(RunRetentionResponse {
            triples_scanned: stats.triples_scanned as u64,
            triples_deleted: stats.triples_deleted as u64,
            duration_ms: stats.duration_ms,
        }))
    }

    /// Return replication status for this server instance.
    async fn replica_status(
        &self,
        _request: Request<ReplicaStatusRequest>,
    ) -> Result<Response<ReplicaStatusResponse>, Status> {
        let resp = match &self.replica_state {
            Some(state) => {
                let last_applied_seq = state.last_applied_seq.load(Ordering::Relaxed);
                let primary_latest = self.store.latest_sequence_number();
                let replication_lag_entries = primary_latest.saturating_sub(last_applied_seq);
                metrics::gauge!("polargraph_wal_applied_seq").set(last_applied_seq as f64);
                metrics::gauge!("polargraph_wal_lag_entries").set(replication_lag_entries as f64);
                ReplicaStatusResponse {
                    is_replica: true,
                    primary_address: state.primary_address.clone(),
                    last_catchup_at: state.last_catchup_at.load(Ordering::Relaxed),
                    catchup_count: state.catchup_count.load(Ordering::Relaxed),
                    last_applied_seq,
                    replication_lag_entries,
                }
            }
            None => ReplicaStatusResponse {
                is_replica: false,
                primary_address: String::new(),
                last_catchup_at: 0,
                catchup_count: 0,
                last_applied_seq: 0,
                replication_lag_entries: 0,
            },
        };
        Ok(Response::new(resp))
    }

    /// Stream WAL entries to a replica. Primary-only.
    async fn stream_wal(
        &self,
        request: Request<StreamWalRequest>,
    ) -> Result<Response<Self::StreamWalStream>, Status> {
        self.check_not_replica()?;

        let since_seq = request.into_inner().since_seq;
        let store = self.store.clone();

        // Channel carrying raw storage WalEntry items from the blocking streamer.
        let (raw_tx, mut raw_rx) = mpsc::channel::<polargraph_storage::WalEntry>(128);
        // Channel carrying proto WalEntry items for the gRPC response stream.
        let (proto_tx, proto_rx) = mpsc::channel::<Result<WalEntry, Status>>(128);

        // Blocking task: tails the RocksDB WAL and sends raw entries.
        tokio::task::spawn_blocking(move || {
            WalStreamer::new(store).run(since_seq, raw_tx);
        });

        // Async task: converts raw entries to proto and forwards to the client.
        tokio::spawn(async move {
            while let Some(entry) = raw_rx.recv().await {
                let proto_entry = WalEntry {
                    sequence_number: entry.sequence_number,
                    write_batch: entry.write_batch,
                };
                if proto_tx.send(Ok(proto_entry)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(proto_rx)))
    }
}

// ── Error mapping ─────────────────────────────────────────────────────────────

fn replica_not_writable() -> Status {
    Status::failed_precondition(
        "write operations are not supported on a read replica",
    )
}

fn backup_not_configured() -> Status {
    Status::failed_precondition(
        "backup not configured: start polargraphd with --backup-dir <PATH>",
    )
}

fn query_err_to_status(err: QueryError, timeout_ms: u64) -> Status {
    match err {
        QueryError::Timeout => Status::deadline_exceeded(format!(
            "query exceeded timeout of {}ms",
            timeout_ms
        )),
        QueryError::Storage(se) => storage_err_to_status(se),
    }
}

fn storage_err_to_status(err: StorageError) -> Status {
    match err {
        StorageError::WriteConflict(ref e) => {
            warn!("write conflict: {e}");
            Status::aborted(err.to_string())
        }
        StorageError::Rocks(_) => {
            warn!("rocksdb error: {err}");
            Status::internal(err.to_string())
        }
        StorageError::Serde(_) => Status::internal(err.to_string()),
        StorageError::MissingCf(_) => Status::internal(err.to_string()),
        StorageError::KeyDecode(_) => Status::internal(err.to_string()),
        StorageError::Io(_) => Status::internal(err.to_string()),
        StorageError::ReadOnly(_) => Status::failed_precondition(err.to_string()),
    }
}
