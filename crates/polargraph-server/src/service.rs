//! gRPC service implementation.
#![allow(clippy::result_large_err)]

const STREAM_CHUNK_SIZE: usize = 500;

use crate::{
    convert,
    proto::{
        polar_graph_service_server::PolarGraphService,
        search_vector_filtered_request::Filter,
        vector_seed_query_request::Filter as SeedFilter,
        BackupInfo as ProtoBackupInfo,
        BatchInsertError, BatchInsertVectorsRequest, BatchInsertVectorsResponse,
        BeginTransactionRequest, BeginTransactionResponse,
        CommitTransactionRequest, CommitTransactionResponse,
        CreateBackupRequest, CreateBackupResponse,
        AppliedMigrationInfo,
        CypherBinding, CypherQueryRequest, CypherQueryResponse, CypherWriteRequest, CypherWriteResponse,
        ExplainResponse, PlanNode,
        GetEdgeTypeRequest, GetEdgeTypeResponse,
        GetNodeTypeRequest, GetNodeTypeResponse,
        InsertRequest, InsertResponse, InsertVectorRequest, InsertVectorResponse,
        ListBackupsRequest, ListBackupsResponse,
        ListEdgeTypesRequest, ListEdgeTypesResponse,
        ListNodeTypesRequest, ListNodeTypesResponse,
        ListPredicatesBetweenRequest, ListPredicatesBetweenResponse,
        MigrateRequest, MigrateResponse, MigrationStatusRequest, MigrationStatusResponse,
        PurgeOldBackupsRequest, PurgeOldBackupsResponse,
        QueryRequest, QueryResponse, QueryStreamChunk, ReachableRequest, ReachableResponse,
        RegisterEdgeTypeRequest, RegisterEdgeTypeResponse,
        RegisterNodeTypeRequest, RegisterNodeTypeResponse,
        ReplicaStatusRequest, ReplicaStatusResponse,
        RollbackTransactionRequest, RollbackTransactionResponse,
        RunRetentionRequest, RunRetentionResponse,
        ScoredBinding,
        SearchVectorFilteredRequest, SearchVectorFilteredResponse,
        SearchVectorInSetRequest, SearchVectorInSetResponse,
        SearchVectorRequest, SearchVectorResponse,
        ShowIndexesRequest, ShowIndexesResponse,
        ShowStatsRequest, ShowStatsResponse,
        ColumnFamilyInfo, VectorSpaceInfo,
        StreamWalRequest, WalEntry,
        ValidateEdgeRequest, ValidateEdgeResponse,
        ValidateNodeRequest, ValidateNodeResponse,
        VectorSearchResult, VectorSeedQueryRequest, VectorSeedQueryResponse,
    },
};
use dashmap::DashMap;
use polargraph_core::{id::NodeId, schema::{RetentionPolicy, StorageMode}, triple::Triple, value::Value};
use polargraph_query::datalog::{
    execute_query, execute_query_hybrid, execute_query_seeded, execute_query_with_pending,
    execute_recursive, reachable_from, reachable_from_hops, Bindings, DerivedFacts, Query,
    QueryError,
};
use polargraph_query::explain::explain_query;
use polargraph_storage::{BackupManager, CompactionManager, EdgeTypeRegistry, MigrationRunner, NodeTypeRegistry, StorageError, Transaction, TripleStore, WalStreamer, MIGRATIONS};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, Mutex as AsyncMutex};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

// ── Open-transaction state ────────────────────────────────────────────────────

/// Server-side state for one open wire transaction.
struct OpenTransaction {
    tx: Transaction,
    last_used: Instant,
}

type TxMap = DashMap<String, Arc<AsyncMutex<OpenTransaction>>>;

// ── Replica state ─────────────────────────────────────────────────────────────

/// Tracks WAL replication statistics for a replica instance.
pub struct ReplicaState {
    pub primary_address: String,
    pub last_catchup_at: AtomicI64,
    pub catchup_count: AtomicU64,
    pub last_applied_seq: AtomicU64,
    /// True while the WAL streaming connection to the primary is active.
    pub connected: std::sync::atomic::AtomicBool,
}

impl ReplicaState {
    pub fn new(primary_address: String) -> Arc<Self> {
        Arc::new(Self {
            primary_address,
            last_catchup_at: AtomicI64::new(0),
            catchup_count: AtomicU64::new(0),
            last_applied_seq: AtomicU64::new(0),
            connected: std::sync::atomic::AtomicBool::new(false),
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
    /// Emit a warn! when a query RPC takes longer than this many ms. 0 = disabled.
    slow_query_ms: u64,
    /// Default HNSW exploration factor for all vector search RPCs.
    default_vector_ef: u32,
    /// Server start time, used to compute uptime in ShowStats.
    start_time: Instant,
    /// Live wire transactions. Shared across all clones via Arc.
    tx_map: Arc<TxMap>,
    /// Milliseconds of idle time after which an open transaction is rolled back. 0 = disabled.
    tx_idle_timeout_ms: u64,
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
        Ok(Self {
            store,
            registry,
            edge_registry,
            type_cache,
            backup_manager,
            replica_state: None,
            query_timeout_ms: 30_000,
            slow_query_ms: 1_000,
            default_vector_ef: 50,
            start_time: Instant::now(),
            tx_map: Arc::new(DashMap::new()),
            tx_idle_timeout_ms: 300_000,
        })
    }

    /// Set the maximum query execution time in milliseconds. 0 disables the timeout.
    pub fn with_query_timeout_ms(mut self, ms: u64) -> Self {
        self.query_timeout_ms = ms;
        self
    }

    /// Set the slow-query threshold in milliseconds. Queries exceeding this
    /// duration emit a `warn!` and increment `polargraph_slow_queries_total`.
    /// 0 disables slow-query logging.
    pub fn with_slow_query_ms(mut self, ms: u64) -> Self {
        self.slow_query_ms = ms;
        self
    }

    /// Set the default HNSW exploration factor for all vector search RPCs.
    /// Per-request `ef` fields override this value when non-zero.
    pub fn with_default_vector_ef(mut self, ef: u32) -> Self {
        self.default_vector_ef = ef;
        self
    }

    /// Set the idle TTL for open transactions in milliseconds. 0 disables idle expiry.
    pub fn with_tx_idle_timeout_ms(mut self, ms: u64) -> Self {
        self.tx_idle_timeout_ms = ms;
        self
    }

    /// Check whether `elapsed` crosses the slow-query threshold and, if so,
    /// log a warning and increment the Prometheus counter.
    fn check_slow_query(&self, method: &'static str, elapsed: Duration, extra: &str) {
        if self.slow_query_ms == 0 {
            return;
        }
        let duration_ms = elapsed.as_millis() as u64;
        if duration_ms >= self.slow_query_ms {
            warn!(
                method,
                duration_ms,
                threshold_ms = self.slow_query_ms,
                extra,
                "slow query detected"
            );
            metrics::counter!("polargraph_slow_queries_total", "method" => method).increment(1);
        }
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
            slow_query_ms: 1_000,
            default_vector_ef: 50,
            start_time: Instant::now(),
            tx_map: Arc::new(DashMap::new()),
            tx_idle_timeout_ms: 300_000,
        };
        Ok((server, replica_state))
    }

    /// Spawn a background task that periodically evicts idle open transactions.
    ///
    /// Any transaction whose `last_used` is older than `idle_timeout_ms` is
    /// removed from the map (effectively rolling it back). Stops when `token`
    /// is cancelled.
    pub fn spawn_tx_ttl_task(&self, token: CancellationToken, idle_timeout_ms: u64) {
        if idle_timeout_ms == 0 {
            return;
        }
        let tx_map = Arc::clone(&self.tx_map);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let threshold = Duration::from_millis(idle_timeout_ms);
                        let now = Instant::now();
                        let expired: Vec<String> = tx_map
                            .iter()
                            .filter_map(|entry| {
                                // Try a non-blocking lock; skip if the tx is being used.
                                if let Ok(guard) = entry.value().try_lock() {
                                    if now.duration_since(guard.last_used) > threshold {
                                        return Some(entry.key().clone());
                                    }
                                }
                                None
                            })
                            .collect();
                        for tx_id in &expired {
                            tx_map.remove(tx_id);
                        }
                        if !expired.is_empty() {
                            warn!(
                                count = expired.len(),
                                idle_timeout_ms,
                                "idle transactions expired and rolled back"
                            );
                            metrics::gauge!("polargraph_open_transactions")
                                .set(tx_map.len() as f64);
                        }
                    }
                    _ = token.cancelled() => {
                        let remaining = tx_map.len();
                        if remaining > 0 {
                            warn!(
                                count = remaining,
                                "rolling back open transactions on shutdown"
                            );
                        }
                        tx_map.clear();
                        break;
                    }
                }
            }
        });
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
    type QueryStreamStream = ReceiverStream<Result<QueryStreamChunk, Status>>;
    type CypherQueryStreamStream = ReceiverStream<Result<QueryStreamChunk, Status>>;

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

        // Convert proto triples → core triples, collecting EdgeIds for relations.
        let mut all_triples: Vec<Triple> = Vec::new();
        let mut edge_ids: Vec<Vec<u8>> = Vec::new();
        for proto_triple in &req.triples {
            let (triples, edge_id) = convert::triples_from_proto(proto_triple)?;
            all_triples.extend(triples);
            if let Some(eid) = edge_id {
                edge_ids.push(eid.0.as_bytes().to_vec());
            }
        }

        debug!("insert: {} triple(s) ({} relation(s))", all_triples.len(), edge_ids.len());

        // If a tx_id is provided, buffer into the open transaction without committing.
        if !req.tx_id.is_empty() {
            let entry = self
                .tx_map
                .get(&req.tx_id)
                .ok_or_else(|| Status::not_found(format!("unknown or expired transaction: {}", req.tx_id)))?;
            let mut guard = entry.lock().await;
            for triple in &all_triples {
                guard.tx.insert(triple.clone());
            }
            guard.last_used = Instant::now();
            debug!(tx_id = %req.tx_id, "buffered {} triple(s) into open transaction", all_triples.len());
            return Ok(Response::new(InsertResponse { commit_ts: 0, edge_ids }));
        }

        // Auto-commit path: begin a new transaction, insert all, commit.
        let mut tx = self.store.begin();
        for triple in &all_triples {
            tx.insert(triple.clone());
        }
        let commit_ts = tx.commit().map_err(storage_err_to_status)?;

        // Incrementally update the type cache for any __type triples.
        self.update_type_cache(&all_triples);

        metrics::gauge!("polargraph_triples_total").increment(all_triples.len() as f64);

        Ok(Response::new(InsertResponse { commit_ts: commit_ts.0, edge_ids }))
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

        let pattern_count = patterns.len();

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

        // Convert optional Datalog rules.
        let rules: Vec<_> = req.rules.iter()
            .map(convert::rule_from_proto)
            .collect::<Result<_, _>>()?;

        let t0 = Instant::now();

        // When tx_id is set, use the transaction's snapshot (read_ts) and overlay
        // pending write-buffer triples for write-your-own-reads.
        let results = if !req.tx_id.is_empty() {
            let entry = self
                .tx_map
                .get(&req.tx_id)
                .ok_or_else(|| Status::not_found(format!("unknown or expired transaction: {}", req.tx_id)))?;
            let mut guard = entry.lock().await;
            guard.last_used = Instant::now();
            let tx_snapshot = self.store.snapshot(guard.tx.read_ts);
            // Collect pending triples while we hold the lock.
            let pending: Vec<Triple> = guard.tx.pending_triples().to_vec();
            drop(guard);
            drop(entry);
            execute_query_with_pending(&query, &tx_snapshot, &pending, self.make_deadline(), Some(&self.edge_registry))
                .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?
        } else if rules.is_empty() {
            // Fast path: pure conjunctive query against base facts only.
            execute_query(&query, &snapshot, self.make_deadline(), Some(&self.edge_registry))
                .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?
        } else {
            // Recursive path: run rules to fixed point, then evaluate patterns
            // against the combined base + derived fact set.
            let derived: DerivedFacts = execute_recursive(&[], &rules, &snapshot, self.make_deadline())
                .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?;
            execute_query_hybrid(&query, &snapshot, &derived, self.make_deadline())
                .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?
        };
        self.check_slow_query("Query", t0.elapsed(), &format!("patterns={pattern_count} rules={}", rules.len()));

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
        let ef = if req.ef > 0 { req.ef as usize } else { self.default_vector_ef as usize };

        debug!("search_vector: space={} dim={} k={k} ef={ef}", space, req.query.len());

        let hits = self.store.search_vector_ef(space, &req.query, k, ef);
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
        let ef = if req.ef > 0 { req.ef as usize } else { self.default_vector_ef as usize };

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
        let t0 = Instant::now();
        let reachable_set = if req.max_hops == 0 {
            reachable_from(start, &req.predicate, &snapshot, deadline)
        } else {
            reachable_from_hops(start, &req.predicate, &snapshot, req.max_hops as usize, deadline)
        }
        .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?;
        self.check_slow_query(
            "Reachable",
            t0.elapsed(),
            &format!("predicate={} max_hops={}", req.predicate, req.max_hops),
        );

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
        let ef = if req.ef > 0 { req.ef as usize } else { self.default_vector_ef as usize };
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

        let t0 = Instant::now();
        let results = execute_query_seeded(&query, &snapshot, initial, self.make_deadline(), Some(&self.edge_registry))
            .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?;
        self.check_slow_query(
            "VectorSeedQuery",
            t0.elapsed(),
            &format!("space={space} k={k} patterns={}", query.patterns.len()),
        );

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

    /// Return the static execution plan for a query without running it.
    async fn explain_query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<ExplainResponse>, Status> {
        let req = request.into_inner();

        if req.patterns.is_empty() {
            return Err(Status::invalid_argument("query must contain at least one pattern"));
        }

        let patterns: Vec<_> = req
            .patterns
            .iter()
            .map(convert::var_pattern_from_proto)
            .collect::<Result<_, _>>()?;

        let mut query = Query::new();
        for p in patterns {
            query.patterns.push(p);
        }

        let plan = explain_query(&query, &[]);

        let nodes: Vec<PlanNode> = plan
            .steps
            .iter()
            .map(|step| PlanNode {
                node_type: step.node_type.clone(),
                description: step.description.clone(),
                index_used: step.index_used.clone(),
                children: vec![],
            })
            .collect();

        Ok(Response::new(ExplainResponse {
            plan_text: plan.plan_text,
            nodes,
        }))
    }

    /// Apply pending schema migrations (or dry-run). Primary-only.
    async fn migrate_schema(
        &self,
        request: Request<MigrateRequest>,
    ) -> Result<Response<MigrateResponse>, Status> {
        self.check_not_replica()?;
        let dry_run = request.into_inner().dry_run;
        let runner = MigrationRunner::new(self.store.clone());

        if dry_run {
            let current = runner.current_version().map_err(storage_err_to_status)?;
            let pending: Vec<u32> = MIGRATIONS
                .iter()
                .filter(|m| m.version > current)
                .map(|m| m.version)
                .collect();
            let skipped: Vec<u32> = MIGRATIONS
                .iter()
                .filter(|m| m.version <= current)
                .map(|m| m.version)
                .collect();
            let summary = if pending.is_empty() {
                "database is up to date (dry run)".to_string()
            } else {
                format!("would apply {} migration(s): {:?} (dry run)", pending.len(), pending)
            };
            return Ok(Response::new(MigrateResponse {
                applied_versions: pending,
                skipped_versions: skipped,
                summary,
            }));
        }

        let stats = runner.run_pending().map_err(storage_err_to_status)?;
        info!(
            applied = ?stats.applied,
            skipped = ?stats.skipped,
            "schema migration run complete"
        );
        let summary = if stats.applied.is_empty() {
            "database is already up to date".to_string()
        } else {
            format!("applied {} migration(s): {:?}", stats.applied.len(), stats.applied)
        };
        Ok(Response::new(MigrateResponse {
            applied_versions: stats.applied,
            skipped_versions: stats.skipped,
            summary,
        }))
    }

    /// Return the current migration version and history.
    async fn migration_status(
        &self,
        _request: Request<MigrationStatusRequest>,
    ) -> Result<Response<MigrationStatusResponse>, Status> {
        let runner = MigrationRunner::new(self.store.clone());
        let current_version = runner.current_version().map_err(storage_err_to_status)?;
        let latest_version = MigrationRunner::latest_version();
        let applied = runner.list_applied().map_err(storage_err_to_status)?;
        let applied_info: Vec<AppliedMigrationInfo> = applied
            .into_iter()
            .map(|m| AppliedMigrationInfo {
                version: m.version,
                description: m.description,
                applied_at_tx_time: m.applied_at_tx_time,
            })
            .collect();
        Ok(Response::new(MigrationStatusResponse {
            current_version,
            latest_version,
            applied: applied_info,
        }))
    }

    /// Parse and execute a Cypher query string.
    async fn cypher_query(
        &self,
        request: Request<CypherQueryRequest>,
    ) -> Result<Response<CypherQueryResponse>, Status> {
        let req = request.into_inner();

        if req.cypher.is_empty() {
            return Err(Status::invalid_argument("cypher query string must not be empty"));
        }

        // Reject write statements early — CypherWrite is the right RPC for those.
        if polargraph_query::cypher::is_write_statement(&req.cypher) {
            return Err(Status::invalid_argument(
                "CypherQuery does not accept write statements (CREATE/MERGE/SET/DELETE); use CypherWrite instead",
            ));
        }

        // Parse the Cypher string into an AST.
        let parsed = polargraph_query::cypher::parse(&req.cypher)
            .map_err(|e| Status::invalid_argument(format!("cypher parse error: {e}")))?;

        // Compile the AST into the Datalog IR.
        let compiled = polargraph_query::cypher::compile(parsed);

        // Resolve snapshot. If tx_id is set, read from the transaction's snapshot.
        let mut snapshot = if !req.tx_id.is_empty() {
            let open_arc = {
                let entry = self.tx_map.get(&req.tx_id).ok_or_else(|| {
                    Status::not_found(format!("transaction '{}' not found or expired", req.tx_id))
                })?;
                Arc::clone(entry.value())
            };
            let mut guard = open_arc.lock().await;
            guard.last_used = Instant::now();
            self.store.snapshot(guard.tx.read_ts)
        } else {
            let tx_ts = if req.as_of_tx_time != 0 { req.as_of_tx_time } else { 0 };
            if tx_ts == 0 {
                self.store.snapshot(self.store.begin().read_ts)
            } else {
                self.store.snapshot(polargraph_core::temporal::Timestamp(tx_ts))
            }
        };
        if req.as_of_valid_time != 0 {
            snapshot = snapshot.with_vt_as_of(req.as_of_valid_time);
        }

        let deadline = self.make_deadline();
        let t0 = Instant::now();

        // Execute: VECTOR_NEAR path (ANN → seed bindings → query) or regular path.
        let raw_results = if let Some(vn) = compiled.vector_near {
            if req.vector.is_empty() {
                return Err(Status::invalid_argument(
                    "VECTOR_NEAR requires a query vector in the request (field 'vector')",
                ));
            }
            let space = &vn.space;
            let k = vn.k as usize;
            let ef = vn.ef.map(|e| e as usize)
                .filter(|&e| e > 0)
                .unwrap_or_else(|| if req.ef > 0 { req.ef as usize } else { self.default_vector_ef as usize });
            let seed_var = vn.seed_variable.clone();

            let ann_hits = self
                .store
                .search_vector_ef(space, &req.vector, ef, ef)
                .into_iter()
                .take(k)
                .collect::<Vec<_>>();

            if ann_hits.is_empty() {
                vec![]
            } else {
                let initial: Vec<Bindings> = ann_hits
                    .iter()
                    .map(|(id, _)| {
                        let mut b = Bindings::new();
                        b.insert(seed_var.clone(), *id);
                        b
                    })
                    .collect();
                execute_query_seeded(&compiled.query, &snapshot, initial, deadline, Some(&self.edge_registry))
                    .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?
            }
        } else if compiled.rules.is_empty() {
            execute_query(&compiled.query, &snapshot, deadline, Some(&self.edge_registry))
                .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?
        } else {
            let derived: DerivedFacts =
                execute_recursive(&[], &compiled.rules, &snapshot, deadline)
                    .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?;
            execute_query_hybrid(&compiled.query, &snapshot, &derived, deadline)
                .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?
        };

        self.check_slow_query(
            "CypherQuery",
            t0.elapsed(),
            &format!("patterns={} rules={}", compiled.query.patterns.len(), compiled.rules.len()),
        );

        // Apply value filters (type constraints and WHERE clause checks).
        let filtered =
            polargraph_query::cypher::apply_value_filters(raw_results, &compiled.value_filters, &snapshot)
                .map_err(storage_err_to_status)?;

        // Apply text filters (CONTAINS / STARTS WITH / =~).
        let filtered =
            polargraph_query::cypher::apply_text_filters(filtered, &compiled.text_filters, &snapshot)
                .map_err(storage_err_to_status)?;

        // Apply aggregations, ORDER BY, SKIP, and LIMIT.
        let agg_rows = polargraph_query::aggregation::apply_aggregations(
            filtered,
            &compiled.group_keys,
            &compiled.aggregations,
            &compiled.order_by,
            compiled.skip,
            compiled.limit,
        );

        // Build CypherQueryResponse rows.
        let rows: Vec<CypherBinding> = agg_rows
            .iter()
            .map(|row| {
                // Project group-key nodes to the declared return_vars (or all if unspecified).
                let nodes = if compiled.return_vars.is_empty() {
                    row.group_keys
                        .iter()
                        .map(|(k, &id)| (k.clone(), convert::node_id_to_proto(id)))
                        .collect()
                } else {
                    compiled.return_vars
                        .iter()
                        .filter_map(|v| {
                            row.group_keys.get(v).map(|&id| (v.clone(), convert::node_id_to_proto(id)))
                        })
                        .collect()
                };
                let values = row.agg_values
                    .iter()
                    .map(|(k, v)| (k.clone(), convert::value_to_proto(v)))
                    .collect();
                CypherBinding { nodes, values }
            })
            .collect();

        Ok(Response::new(CypherQueryResponse { rows }))
    }

    /// Parse and execute a Cypher write statement (CREATE, MERGE, SET, DELETE),
    /// optionally preceded by a MATCH clause.
    async fn cypher_write(
        &self,
        request: Request<CypherWriteRequest>,
    ) -> Result<Response<CypherWriteResponse>, Status> {
        self.check_not_replica()?;
        let req = request.into_inner();

        if req.cypher.is_empty() {
            return Err(Status::invalid_argument("cypher write string must not be empty"));
        }

        let compiled = polargraph_query::cypher::parse_write(&req.cypher)
            .map_err(|e| Status::invalid_argument(format!("cypher parse error: {e}")))?;

        let map_write_err = |e: polargraph_query::cypher::CypherWriteError| match e {
            polargraph_query::cypher::CypherWriteError::UnboundVariable(v) => {
                Status::invalid_argument(format!("unbound variable '{v}'"))
            }
            polargraph_query::cypher::CypherWriteError::Storage(se) => storage_err_to_status(se),
            polargraph_query::cypher::CypherWriteError::Parse(pe) => {
                Status::invalid_argument(format!("cypher parse error: {pe}"))
            }
        };

        // Helper closure to run the write ops given a mutable Transaction reference.
        // Returns the WriteResult without committing.
        let execute_writes = |tx: &mut Transaction| -> Result<polargraph_query::cypher::WriteResult, Status> {
            let snapshot = self.store.snapshot(tx.read_ts);
            if let Some(ref mq) = compiled.match_query {
                let raw = execute_query(&mq.query, &snapshot, None, None)
                    .map_err(|e| Status::internal(format!("match query error: {e}")))?;
                let rows = polargraph_query::cypher::apply_value_filters(raw, &mq.value_filters, &snapshot)
                    .map_err(storage_err_to_status)?;
                let rows = polargraph_query::cypher::apply_text_filters(rows, &mq.text_filters, &snapshot)
                    .map_err(storage_err_to_status)?;
                let mut all_ids: Vec<NodeId> = Vec::new();
                let mut all_written: u64 = 0;
                let mut all_deleted: u64 = 0;
                for row in rows {
                    let mut row_bindings = row;
                    let r = polargraph_query::cypher::execute_write_ops(
                        &compiled.writes, tx, &snapshot, &mut row_bindings,
                    )
                    .map_err(&map_write_err)?;
                    all_ids.extend(r.created_ids);
                    all_written += r.triples_written;
                    all_deleted += r.triples_deleted;
                }
                Ok(polargraph_query::cypher::WriteResult {
                    created_ids: all_ids,
                    triples_written: all_written,
                    triples_deleted: all_deleted,
                })
            } else {
                let mut bindings = HashMap::new();
                polargraph_query::cypher::execute_write_ops(
                    &compiled.writes, tx, &snapshot, &mut bindings,
                )
                .map_err(map_write_err)
            }
        };

        // If tx_id is set, buffer writes into the open transaction without committing.
        if !req.tx_id.is_empty() {
            // Clone the Arc before awaiting to avoid holding a DashMap shard lock.
            let open_arc = {
                let entry = self
                    .tx_map
                    .get(&req.tx_id)
                    .ok_or_else(|| Status::not_found(format!("unknown or expired transaction: {}", req.tx_id)))?;
                Arc::clone(entry.value())
            };
            let mut guard = open_arc.lock().await;
            let result = execute_writes(&mut guard.tx)?;
            guard.last_used = Instant::now();
            debug!(
                tx_id = %req.tx_id,
                "cypher_write buffered: created={} written={} deleted={}",
                result.created_ids.len(), result.triples_written, result.triples_deleted
            );
            return Ok(Response::new(CypherWriteResponse {
                created_node_ids: result.created_ids.iter().map(|id| id.as_bytes().to_vec()).collect(),
                triples_written: result.triples_written,
                triples_deleted: result.triples_deleted,
            }));
        }

        // Auto-commit path.
        let mut tx = self.store.begin();
        let result = execute_writes(&mut tx)?;

        let commit_ts = tx.commit().map_err(storage_err_to_status)?;
        debug!(
            "cypher_write: created={} written={} deleted={} commit_ts={}",
            result.created_ids.len(), result.triples_written, result.triples_deleted, commit_ts.0
        );

        // Update the type cache for any newly created typed nodes.
        // We re-scan because the write result doesn't carry Triple objects.
        if !result.created_ids.is_empty() {
            let post_snap = self.store.snapshot(commit_ts);
            let mut updates: Vec<(NodeId, String)> = Vec::new();
            for node_id in &result.created_ids {
                if let Ok(triples) = post_snap.scan_by_subject_predicate(node_id, "__type") {
                    for t in triples {
                        if let Triple::Property { subject, value: Value::Text(type_name), .. } = t {
                            updates.push((subject, type_name));
                        }
                    }
                }
            }
            if !updates.is_empty() {
                let mut cache = self.type_cache.write().unwrap();
                for (subject, type_name) in updates {
                    cache.entry(type_name).or_default().insert(subject);
                }
            }
        }

        metrics::gauge!("polargraph_triples_total")
            .increment(result.triples_written as f64);

        Ok(Response::new(CypherWriteResponse {
            created_node_ids: result.created_ids.iter().map(|id| id.as_bytes().to_vec()).collect(),
            triples_written: result.triples_written,
            triples_deleted: result.triples_deleted,
        }))
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

    /// Stream a conjunctive query in chunks of `STREAM_CHUNK_SIZE` bindings.
    async fn query_stream(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<Self::QueryStreamStream>, Status> {
        let req = request.into_inner();

        if req.patterns.is_empty() {
            return Err(Status::invalid_argument("query must contain at least one pattern"));
        }

        let patterns: Vec<_> = req
            .patterns
            .iter()
            .map(convert::var_pattern_from_proto)
            .collect::<Result<_, _>>()?;

        let pattern_count = patterns.len();
        let mut query = Query::new();
        for p in patterns {
            query.patterns.push(p);
        }

        let tx_ts = if req.as_of_tx_time != 0 { req.as_of_tx_time } else { req.snapshot_ts };
        let mut snapshot = if tx_ts == 0 {
            self.store.snapshot(self.store.begin().read_ts)
        } else {
            self.store.snapshot(polargraph_core::temporal::Timestamp(tx_ts))
        };
        if req.as_of_valid_time != 0 {
            snapshot = snapshot.with_vt_as_of(req.as_of_valid_time);
        }

        let rules: Vec<_> = req.rules.iter()
            .map(convert::rule_from_proto)
            .collect::<Result<_, _>>()?;

        let t0 = Instant::now();
        let results = if rules.is_empty() {
            execute_query(&query, &snapshot, self.make_deadline(), Some(&self.edge_registry))
                .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?
        } else {
            let derived: DerivedFacts = execute_recursive(&[], &rules, &snapshot, self.make_deadline())
                .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?;
            execute_query_hybrid(&query, &snapshot, &derived, self.make_deadline())
                .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?
        };
        self.check_slow_query("QueryStream", t0.elapsed(), &format!("patterns={pattern_count} rules={}", rules.len()));

        let (tx, rx) = mpsc::channel::<Result<QueryStreamChunk, Status>>(4);
        tokio::spawn(async move {
            send_result_chunks(results, tx).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    /// Stream a Cypher query in chunks of `STREAM_CHUNK_SIZE` bindings.
    /// Respects the LIMIT clause in the Cypher string.
    async fn cypher_query_stream(
        &self,
        request: Request<CypherQueryRequest>,
    ) -> Result<Response<Self::CypherQueryStreamStream>, Status> {
        let req = request.into_inner();

        if req.cypher.is_empty() {
            return Err(Status::invalid_argument("cypher query string must not be empty"));
        }

        let parsed = polargraph_query::cypher::parse(&req.cypher)
            .map_err(|e| Status::invalid_argument(format!("cypher parse error: {e}")))?;
        let compiled = polargraph_query::cypher::compile(parsed);

        let tx_ts = if req.as_of_tx_time != 0 { req.as_of_tx_time } else { 0 };
        let mut snapshot = if tx_ts == 0 {
            self.store.snapshot(self.store.begin().read_ts)
        } else {
            self.store.snapshot(polargraph_core::temporal::Timestamp(tx_ts))
        };
        if req.as_of_valid_time != 0 {
            snapshot = snapshot.with_vt_as_of(req.as_of_valid_time);
        }

        let deadline = self.make_deadline();
        let t0 = Instant::now();

        let raw_results = if let Some(vn) = compiled.vector_near {
            if req.vector.is_empty() {
                return Err(Status::invalid_argument(
                    "VECTOR_NEAR requires a query vector in the request (field 'vector')",
                ));
            }
            let space = &vn.space;
            let k = vn.k as usize;
            let ef = vn.ef.map(|e| e as usize)
                .filter(|&e| e > 0)
                .unwrap_or_else(|| if req.ef > 0 { req.ef as usize } else { self.default_vector_ef as usize });
            let seed_var = vn.seed_variable.clone();
            let ann_hits = self
                .store
                .search_vector_ef(space, &req.vector, ef, ef)
                .into_iter()
                .take(k)
                .collect::<Vec<_>>();
            if ann_hits.is_empty() {
                vec![]
            } else {
                let initial: Vec<Bindings> = ann_hits
                    .iter()
                    .map(|(id, _)| {
                        let mut b = Bindings::new();
                        b.insert(seed_var.clone(), *id);
                        b
                    })
                    .collect();
                execute_query_seeded(&compiled.query, &snapshot, initial, deadline, Some(&self.edge_registry))
                    .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?
            }
        } else if compiled.rules.is_empty() {
            execute_query(&compiled.query, &snapshot, deadline, Some(&self.edge_registry))
                .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?
        } else {
            let derived: DerivedFacts =
                execute_recursive(&[], &compiled.rules, &snapshot, deadline)
                    .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?;
            execute_query_hybrid(&compiled.query, &snapshot, &derived, deadline)
                .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?
        };

        self.check_slow_query(
            "CypherQueryStream",
            t0.elapsed(),
            &format!("patterns={} rules={}", compiled.query.patterns.len(), compiled.rules.len()),
        );

        let filtered =
            polargraph_query::cypher::apply_value_filters(raw_results, &compiled.value_filters, &snapshot)
                .map_err(storage_err_to_status)?;

        let filtered =
            polargraph_query::cypher::apply_text_filters(filtered, &compiled.text_filters, &snapshot)
                .map_err(storage_err_to_status)?;

        let limited: Vec<_> = match compiled.limit {
            Some(n) => filtered.into_iter().take(n).collect(),
            None => filtered,
        };

        // Project to RETURN variables (same as cypher_query).
        let projected: Vec<_> = if compiled.return_vars.is_empty() {
            limited
        } else {
            limited.into_iter().map(|b| {
                compiled.return_vars.iter()
                    .filter_map(|v| b.get(v).map(|&id| (v.clone(), id)))
                    .collect()
            }).collect()
        };

        let (tx, rx) = mpsc::channel::<Result<QueryStreamChunk, Status>>(4);
        tokio::spawn(async move {
            send_result_chunks(projected, tx).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn show_indexes(
        &self,
        _req: Request<ShowIndexesRequest>,
    ) -> Result<Response<ShowIndexesResponse>, Status> {
        let column_families: Vec<ColumnFamilyInfo> = polargraph_storage::cf::ALL
            .iter()
            .map(|&name| ColumnFamilyInfo {
                name: name.to_owned(),
                approx_key_count: self.store.cf_approx_key_count(name),
                approx_size_bytes: self.store.cf_approx_size_bytes(name),
            })
            .collect();

        let vector_spaces: Vec<VectorSpaceInfo> = self
            .store
            .hnsw_spaces_info()
            .into_iter()
            .map(|(name, node_count, is_mmap)| {
                let dimensions = self
                    .registry
                    .get_space_def(&name)
                    .map(|vs| vs.dimensions)
                    .unwrap_or(0);
                VectorSpaceInfo {
                    name,
                    dimensions,
                    node_count: node_count as u64,
                    storage_mode: if is_mmap { "mmap".to_owned() } else { "memory".to_owned() },
                }
            })
            .collect();

        let predicate_count = self.store.predicate_count();

        Ok(Response::new(ShowIndexesResponse { column_families, vector_spaces, predicate_count }))
    }

    async fn show_stats(
        &self,
        _req: Request<ShowStatsRequest>,
    ) -> Result<Response<ShowStatsResponse>, Status> {
        let mode = if self.store.is_replica() { "replica" } else { "primary" };
        Ok(Response::new(ShowStatsResponse {
            live_sst_files: self.store.db_live_sst_files(),
            total_sst_size_bytes: self.store.db_total_sst_size_bytes(),
            memtable_size_bytes: self.store.db_memtable_size_bytes(),
            mvcc_oracle_ts: self.store.oracle_ts() as u64,
            predicate_intern_count: self.store.predicate_count(),
            open_transaction_count: self.tx_map.len() as u32,
            mode: mode.to_owned(),
        }))
    }

    // ── Wire transactions ─────────────────────────────────────────────────────

    /// Open a new server-side MVCC transaction and return its opaque ID.
    async fn begin_transaction(
        &self,
        _request: Request<BeginTransactionRequest>,
    ) -> Result<Response<BeginTransactionResponse>, Status> {
        self.check_not_replica()?;

        let tx = self.store.begin();
        let tx_id = uuid::Uuid::now_v7().to_string();

        let open = Arc::new(AsyncMutex::new(OpenTransaction {
            tx,
            last_used: Instant::now(),
        }));
        self.tx_map.insert(tx_id.clone(), open);

        metrics::gauge!("polargraph_open_transactions").set(self.tx_map.len() as f64);
        debug!(tx_id, "transaction opened");

        Ok(Response::new(BeginTransactionResponse { tx_id }))
    }

    /// Commit an open transaction.
    async fn commit_transaction(
        &self,
        request: Request<CommitTransactionRequest>,
    ) -> Result<Response<CommitTransactionResponse>, Status> {
        self.check_not_replica()?;
        let tx_id = request.into_inner().tx_id;

        let (_key, open) = self
            .tx_map
            .remove(&tx_id)
            .ok_or_else(|| Status::not_found(format!("unknown or expired transaction: {tx_id}")))?;

        // Unwrap the Arc — succeeds because we removed it from the map and no
        // new reference can be obtained.
        let inner = Arc::try_unwrap(open).map_err(|_| {
            Status::internal("transaction is still in use by a concurrent RPC")
        })?;
        let open_tx = inner.into_inner();
        let triples_written = open_tx.tx.pending_triples().len() as u64;

        debug!(tx_id, triples = triples_written, "committing transaction");
        open_tx.tx.commit().map_err(storage_err_to_status)?;

        metrics::gauge!("polargraph_open_transactions").set(self.tx_map.len() as f64);
        info!(tx_id, triples_written, "transaction committed");

        Ok(Response::new(CommitTransactionResponse { triples_written }))
    }

    /// Roll back an open transaction, discarding all buffered writes.
    async fn rollback_transaction(
        &self,
        request: Request<RollbackTransactionRequest>,
    ) -> Result<Response<RollbackTransactionResponse>, Status> {
        self.check_not_replica()?;
        let tx_id = request.into_inner().tx_id;

        self.tx_map
            .remove(&tx_id)
            .ok_or_else(|| Status::not_found(format!("unknown or expired transaction: {tx_id}")))?;

        metrics::gauge!("polargraph_open_transactions").set(self.tx_map.len() as f64);
        debug!(tx_id, "transaction rolled back");

        Ok(Response::new(RollbackTransactionResponse {}))
    }
}

// ── Streaming helpers ─────────────────────────────────────────────────────────

/// Split `results` into chunks of `STREAM_CHUNK_SIZE` and send each as a
/// `QueryStreamChunk` on `tx`. The last chunk has `done = true`.
async fn send_result_chunks(
    results: Vec<polargraph_query::datalog::Bindings>,
    tx: mpsc::Sender<Result<QueryStreamChunk, Status>>,
) {
    if results.is_empty() {
        let _ = tx.send(Ok(QueryStreamChunk { results: vec![], chunk_index: 0, done: true })).await;
        return;
    }
    let total = results.len();
    let num_chunks = total.div_ceil(STREAM_CHUNK_SIZE);
    for (chunk_index, chunk) in results.chunks(STREAM_CHUNK_SIZE).enumerate() {
        let is_done = chunk_index + 1 == num_chunks;
        let proto_results = chunk.iter().map(convert::binding_to_query_result).collect();
        let msg = QueryStreamChunk {
            results: proto_results,
            chunk_index: chunk_index as u64,
            done: is_done,
        };
        if tx.send(Ok(msg)).await.is_err() {
            return;
        }
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
