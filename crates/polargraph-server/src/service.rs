//! gRPC service implementation.
#![allow(clippy::result_large_err)]

const STREAM_CHUNK_SIZE: usize = 500;

/// Most adds + retractions one `ApplyChanges` call may carry (one bounded
/// transaction).
const MAX_CHANGESET: usize = 100_000;

use crate::proto::{ValidateShapesRequest, ValidateShapesResponse, ValidationResult};
use crate::{
    auth::KeyStore,
    convert,
    proto::{
        grant_access_request::Target as GrantTarget, polar_graph_service_server::PolarGraphService,
        revoke_access_request::Target as RevokeTarget, search_vector_filtered_request::Filter,
        vector_seed_query_request::Filter as SeedFilter, AddApiKeyRequest, AddApiKeyResponse,
        AddUserToGroupRequest, AddUserToGroupResponse, AppliedMigrationInfo, ApplyChangesRequest,
        ApplyChangesResponse, BackupInfo as ProtoBackupInfo, BatchInsertError,
        BatchInsertVectorsRequest, BatchInsertVectorsResponse, BeginTransactionRequest,
        BeginTransactionResponse, ChangeEvent, ChangeKind, ColumnFamilyInfo,
        CommitTransactionRequest, CommitTransactionResponse, ConvertLegacyDataRequest,
        ConvertLegacyDataResponse, CopyGraphRequest, CopyGraphResponse, CreateBackupRequest,
        CreateBackupResponse, CreateGraphRequest, CreateGraphResponse, CypherBinding,
        CypherQueryRequest, CypherQueryResponse, CypherWriteRequest, CypherWriteResponse,
        DeleteTriplesRequest, DeleteTriplesResponse, DropGraphRequest, DropGraphResponse,
        ExplainResponse, ExportGraphChunk, ExportGraphRequest, ExportedQuad,
        GetEdgeAnnotationsRequest, GetEdgeAnnotationsResponse, GetEdgeIdsByTripleRequest,
        GetEdgeIdsByTripleResponse, GetEdgeTypeRequest, GetEdgeTypeResponse, GetGraphAccessRequest,
        GetGraphAccessResponse, GetNodeTypeRequest, GetNodeTypeResponse, GetPropertyHistoryRequest,
        GetPropertyHistoryResponse, GetUserAccessRequest, GetUserAccessResponse,
        GetVocabularyRequest, GrantAccessRequest, GrantAccessResponse, GrantGraphAccessRequest,
        GrantGraphAccessResponse, GraphAccessEntry, GraphInfo, GraphMetadata, GraphStatsRequest,
        GraphStatsResponse, InsertRequest, InsertResponse, InsertVectorRequest,
        InsertVectorResponse, ListApiKeysRequest, ListApiKeysResponse, ListBackupsRequest,
        ListBackupsResponse, ListEdgeTypesRequest, ListEdgeTypesResponse, ListGraphsRequest,
        ListGraphsResponse, ListNodeTypesRequest, ListNodeTypesResponse,
        ListPredicatesBetweenRequest, ListPredicatesBetweenResponse, MigrateRequest,
        MigrateResponse, MigrationStatusRequest, MigrationStatusResponse, MoveGraphRequest,
        OntologyViolation, PlanNode, PredicateConversion, PropertyVersion, PurgeOldBackupsRequest,
        PurgeOldBackupsResponse, PutPrefixRequest, QueryRequest, QueryResponse, QueryStreamChunk,
        ReachableRequest, ReachableResponse, RegisterEdgeTypeRequest, RegisterEdgeTypeResponse,
        RegisterNodeTypeRequest, RegisterNodeTypeResponse, RemovePrefixRequest,
        ReplicaStatusRequest, ReplicaStatusResponse, ResolveIrisRequest, ResolveIrisResponse,
        RevokeAccessRequest, RevokeAccessResponse, RevokeApiKeyRequest, RevokeApiKeyResponse,
        RevokeGraphAccessRequest, RevokeGraphAccessResponse, RollbackTransactionRequest,
        RollbackTransactionResponse, RunMaterializationRequest, RunMaterializationResponse,
        RunRetentionRequest, RunRetentionResponse, ScoredBinding, SearchVectorFilteredRequest,
        SearchVectorFilteredResponse, SearchVectorInSetRequest, SearchVectorInSetResponse,
        SearchVectorRequest, SearchVectorResponse, SetVocabularyBaseRequest, ShowIndexesRequest,
        ShowIndexesResponse, ShowStatsRequest, ShowStatsResponse, StreamWalRequest,
        SubscribeRequest, ValidateEdgeRequest, ValidateEdgeResponse, ValidateNodeRequest,
        ValidateNodeResponse, ValidateOntologyRequest, ValidateOntologyResponse,
        VectorSearchResult, VectorSeedQueryRequest, VectorSeedQueryResponse, VectorSpaceInfo,
        WalEntry,
    },
};
use dashmap::DashMap;
use polargraph_core::{
    id::{EdgeId, NodeId},
    schema::{
        GraphAccessLevel, RetentionPolicy, StorageMode, BUILTIN_HAS_ACCESS_PRED,
        BUILTIN_HAS_ACCESS_TYPE_PRED, BUILTIN_HAS_GRAPH_ACCESS_PRED, BUILTIN_MEMBER_OF_PRED,
    },
    triple::Triple,
    value::Value,
};
use polargraph_query::datalog::{
    execute_query, execute_query_full, execute_query_hybrid, execute_query_hybrid_full,
    execute_query_seeded, execute_query_with_pending_full, execute_recursive, reachable_from,
    reachable_from_hops, Bindings, DerivedFacts, Query, QueryError, Solution,
};
use polargraph_query::explain::explain_query;
use polargraph_storage::owl_rl;
use polargraph_storage::{
    BackupManager, CompactionManager, EdgeTypeRegistry, MigrationRunner, NodeTypeRegistry,
    StorageError, Transaction, TripleStore, WalStreamer, MIGRATIONS,
};
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
use uuid;

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

/// `rdf:type` membership by class node, kept current from the change log
/// (shared by all clones).
type TypeCache = Arc<crate::type_index::TypeIndex>;

/// Access cache for graph-native access control.
///
/// Key: user node UUID as a hex string (the NodeId UUID).
/// Value: set of all NodeIds this user is allowed to see, derived from
/// their group memberships (`MEMBER_OF`) and the groups' access grants
/// (`HAS_ACCESS` for direct node grants, `HAS_ACCESS_TYPE` for type-level
/// grants expanded via the type cache).
///
/// An absent entry means the user has no recorded access grants.
/// An *empty* entry means the user has no access to any node.
///
/// Filtering is only applied when `user_id` is set on a request.
type AccessCache = Arc<RwLock<HashMap<String, HashSet<NodeId>>>>;

/// The graph access index and when it was built.
type GraphAccessState = Arc<RwLock<(Arc<polargraph_storage::GraphAccessIndex>, Instant)>>;

/// How stale a replica's graph access index may get (grants arrive by WAL).
const GRAPH_ACCESS_REFRESH: Duration = Duration::from_secs(5);

/// Logged once per process on the first `CypherWrite`.
const CYPHER_WRITE_DEPRECATION: &str = "CypherWrite (Cypher CREATE / MERGE / SET / DELETE) is \
    deprecated and will be removed in the next release; write with ApplyChanges or SPARQL \
    Update instead — see docs/upgrade-cypher-rdf.md";
/// `warning` header on every `CypherWrite` response (RFC 7234 form).
const CYPHER_WRITE_WARNING: &str =
    "299 polargraph \"CypherWrite is deprecated; use ApplyChanges or SPARQL Update\"";

/// How stale a replica's legacy-conversion status may get (the conversion
/// runs on the primary).
const LEGACY_STATUS_REFRESH: Duration = Duration::from_secs(60);

/// Pre-vocabulary data still to convert (`ConvertLegacyData`), with when it
/// was counted. Counting scans every `__type` label, so it is cached.
type LegacyState = Arc<RwLock<(Arc<polargraph_storage::LegacyStatus>, Instant)>>;

#[derive(Clone)]
pub struct PolarGraphServer {
    store: TripleStore,
    registry: NodeTypeRegistry,
    edge_registry: EdgeTypeRegistry,
    type_cache: TypeCache,
    /// Graph-native access control cache. Populated at startup and updated
    /// incrementally after every Insert that touches access-control triples.
    access_cache: AccessCache,
    /// Graph-level access (`docs/design/graph-acl.md`): per-user readable /
    /// writable graph bitmaps, always enforced for requests carrying a user
    /// id. Rebuilt when grants or memberships change (on replicas, at most
    /// every [`GRAPH_ACCESS_REFRESH`]).
    graph_access: GraphAccessState,
    backup_manager: Option<Arc<BackupManager>>,
    /// Non-None when this server is a read replica.
    replica_state: Option<Arc<ReplicaState>>,
    /// Maximum time in milliseconds for a single query. 0 = no limit.
    query_timeout_ms: u64,
    /// Emit a warn! when a query RPC takes longer than this many ms. 0 = disabled.
    slow_query_ms: u64,
    /// Default HNSW exploration factor for all vector search RPCs.
    default_vector_ef: u32,
    #[allow(dead_code)]
    start_time: Instant,
    /// Live wire transactions. Shared across all clones via Arc.
    tx_map: Arc<TxMap>,
    /// Milliseconds of idle time after which an open transaction is rolled back. 0 = disabled.
    tx_idle_timeout_ms: u64,
    /// Shared API key store — same `Arc<RwLock<...>>` used by `ApiKeyLayer`.
    /// `None` when the server was started without auth (e.g. in tests that
    /// don't pass a key store).
    key_store: Option<KeyStore>,
    /// Compiled Cypher query plan cache. Key = raw Cypher string, value = compiled plan.
    /// Shared across all clones. Plans are stored pre-substitution (with Param placeholders).
    query_plan_cache: Arc<DashMap<String, Arc<polargraph_query::cypher::CompiledQuery>>>,
    /// Maximum number of entries in `query_plan_cache`. 0 = disabled.
    query_cache_size: usize,
    /// Number of cache hits since startup.
    query_cache_hits: Arc<std::sync::atomic::AtomicU64>,
    /// Number of cache misses since startup.
    query_cache_misses: Arc<std::sync::atomic::AtomicU64>,
    /// Legacy-conversion status (`docs/upgrade-cypher-rdf.md`).
    legacy: LegacyState,
    /// Set while a `ConvertLegacyData` runs.
    converting: Arc<std::sync::atomic::AtomicBool>,
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
        let type_cache = Arc::new(crate::type_index::TypeIndex::build(store.clone())?);
        let store_legacy = store.legacy_status()?;
        let access_cache_map = Self::build_access_cache(&store, &type_cache)?;
        let access_cache = Arc::new(RwLock::new(access_cache_map));
        let graph_access = Arc::new(RwLock::new((
            Arc::new(polargraph_storage::GraphAccessIndex::build(&store)?),
            Instant::now(),
        )));
        let backup_manager = backup_dir
            .map(|dir| BackupManager::open(dir, &store).map(Arc::new))
            .transpose()?;
        Ok(Self {
            store,
            registry,
            edge_registry,
            type_cache,
            access_cache,
            graph_access,
            backup_manager,
            replica_state: None,
            query_timeout_ms: 30_000,
            slow_query_ms: 1_000,
            default_vector_ef: 50,
            start_time: Instant::now(),
            tx_map: Arc::new(DashMap::new()),
            tx_idle_timeout_ms: 300_000,
            key_store: None,
            query_plan_cache: Arc::new(DashMap::new()),
            query_cache_size: 1000,
            query_cache_hits: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            query_cache_misses: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            legacy: Arc::new(RwLock::new((Arc::new(store_legacy), Instant::now()))),
            converting: Arc::new(std::sync::atomic::AtomicBool::new(false)),
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

    /// Set the maximum number of compiled Cypher query plans to cache. 0 disables caching.
    pub fn with_query_cache_size(mut self, size: usize) -> Self {
        self.query_cache_size = size;
        self
    }

    /// Deserialize a `map<string, string>` params field (values are JSON-encoded Values).
    fn deserialize_params(
        raw: &std::collections::HashMap<String, String>,
    ) -> Result<std::collections::HashMap<String, polargraph_core::value::Value>, Status> {
        raw.iter()
            .map(|(k, v)| {
                let val: polargraph_core::value::Value = serde_json::from_str(v).map_err(|e| {
                    Status::invalid_argument(format!("invalid param '{}': {}", k, e))
                })?;
                Ok((k.clone(), val))
            })
            .collect()
    }

    /// Attach the shared API key store so the `AddApiKey`, `RevokeApiKey`,
    /// and `ListApiKeys` RPCs can mutate the live key list.
    pub fn with_key_store(mut self, store: KeyStore) -> Self {
        self.key_store = Some(store);
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
        let type_cache = Arc::new(crate::type_index::TypeIndex::build(store.clone())?);
        let store_legacy = store.legacy_status()?;
        let access_cache_map = Self::build_access_cache(&store, &type_cache)?;
        let access_cache = Arc::new(RwLock::new(access_cache_map));
        let graph_access = Arc::new(RwLock::new((
            Arc::new(polargraph_storage::GraphAccessIndex::build(&store)?),
            Instant::now(),
        )));
        let replica_state = ReplicaState::new(primary_address.to_owned());
        let server = Self {
            store,
            registry,
            edge_registry,
            type_cache,
            access_cache,
            graph_access,
            backup_manager: None,
            replica_state: Some(replica_state.clone()),
            query_timeout_ms: 30_000,
            slow_query_ms: 1_000,
            default_vector_ef: 50,
            start_time: Instant::now(),
            tx_map: Arc::new(DashMap::new()),
            tx_idle_timeout_ms: 300_000,
            key_store: None,
            query_plan_cache: Arc::new(DashMap::new()),
            query_cache_size: 1000,
            query_cache_hits: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            query_cache_misses: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            legacy: Arc::new(RwLock::new((Arc::new(store_legacy), Instant::now()))),
            converting: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        Ok((server, replica_state))
    }

    /// Spawn a background task that periodically evicts idle open transactions.
    ///
    /// Any transaction whose `last_used` is older than `idle_timeout_ms` is
    /// removed from the map (effectively rolling it back). Stops when `token`
    /// is cancelled.
    /// Keep the inferred graphs current (step 9b): every second, apply the
    /// commits logged since the last run (DRed; a full recompute after a
    /// schema change or when the log was pruned past the resume point).
    /// Primary only.
    pub fn spawn_inference_task(&self, token: CancellationToken) {
        if self.store.is_replica() {
            return;
        }
        let server = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = interval.tick() => {}
                }
                let store = server.store.clone();
                match tokio::task::spawn_blocking(move || owl_rl::infer_changes(&store)).await {
                    Ok(Ok(Some(stats))) => {
                        metrics::counter!("polargraph_inference_batches_total").increment(1);
                        metrics::counter!("polargraph_inference_asserted_total")
                            .increment(stats.asserted);
                        metrics::counter!("polargraph_inference_closed_total")
                            .increment(stats.closed);
                        if stats.asserted + stats.closed > 0 {
                            server.after_inference(&stats);
                        }
                    }
                    Ok(Ok(None)) => {}
                    Ok(Err(e)) => warn!("incremental inference failed: {e}"),
                    Err(e) => warn!("incremental inference task panicked: {e}"),
                }
                if let Ok(Some(applied)) = owl_rl::inference_applied(&server.store) {
                    let lag_us = server.store.oracle_ts().saturating_sub(applied.0).max(0);
                    metrics::gauge!("polargraph_inference_lag_seconds").set(lag_us as f64 / 1e6);
                }
            }
            info!("inference task stopped");
        });
    }

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

    /// Pre-vocabulary data still awaiting `ConvertLegacyData`. Counted at
    /// startup and after each conversion (on a replica, at most every
    /// [`LEGACY_STATUS_REFRESH`]).
    pub fn legacy_status(&self) -> Arc<polargraph_storage::LegacyStatus> {
        if self.store.is_replica()
            && self.legacy.read().unwrap().1.elapsed() > LEGACY_STATUS_REFRESH
        {
            self.refresh_legacy_status();
        }
        Arc::clone(&self.legacy.read().unwrap().0)
    }

    fn refresh_legacy_status(&self) {
        match self.store.legacy_status() {
            Ok(status) => *self.legacy.write().unwrap() = (Arc::new(status), Instant::now()),
            Err(e) => warn!("failed to count legacy data: {e}"),
        }
    }

    /// Log a warning while pre-vocabulary data is unconverted (startup).
    pub fn warn_if_legacy_pending(&self) {
        let status = self.legacy_status();
        if status.pending() {
            warn!(
                bare_predicates = status.bare_predicates.len(),
                type_labels = status.type_labels,
                pending_merges = status.pending_merges.len(),
                "legacy data awaits conversion: bare predicate names and __type labels are \
                 not reachable by bare name until it runs. Set the vocabulary base \
                 (SetVocabularyBase), then run ConvertLegacyData (REST POST \
                 /vocabulary/convert) — see docs/upgrade-cypher-rdf.md"
            );
        }
    }

    fn vocabulary_proto(&self) -> crate::proto::Vocabulary {
        let vocab = self.store.vocabulary();
        crate::proto::Vocabulary {
            base: vocab.base.clone(),
            prefixes: vocab
                .prefixes
                .iter()
                .map(|(name, namespace)| crate::proto::VocabularyPrefix {
                    name: name.clone(),
                    namespace: namespace.clone(),
                })
                .collect(),
            legacy: Some(legacy_to_proto(&self.legacy_status())),
        }
    }

    /// A compiled Cypher plan (pre-parameter-substitution), from the plan
    /// cache when enabled. Plans resolve names through the vocabulary, so the
    /// cache key includes its fingerprint: a vocabulary change (local or
    /// replicated) never serves a stale plan.
    #[allow(clippy::result_large_err)]
    fn compiled_cypher(
        &self,
        cypher: &str,
    ) -> Result<polargraph_query::cypher::CompiledQuery, Status> {
        let vocab = self.store.vocabulary();
        let compile = || {
            let parsed = polargraph_query::cypher::parse(cypher)
                .map_err(|e| Status::invalid_argument(format!("cypher parse error: {e}")))?;
            Ok::<_, Status>(polargraph_query::cypher::compile_with_vocabulary(
                parsed, &vocab,
            ))
        };
        if self.query_cache_size == 0 {
            self.query_cache_misses.fetch_add(1, Ordering::Relaxed);
            metrics::counter!("polargraph_query_cache_misses_total").increment(1);
            return compile();
        }
        let key = format!("{:016x}\u{0}{cypher}", vocab.fingerprint());
        if let Some(cached) = self.query_plan_cache.get(&key) {
            self.query_cache_hits.fetch_add(1, Ordering::Relaxed);
            metrics::counter!("polargraph_query_cache_hits_total").increment(1);
            return Ok(cached.as_ref().clone());
        }
        self.query_cache_misses.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("polargraph_query_cache_misses_total").increment(1);
        let plan = compile()?;
        // Evict an arbitrary entry when full (simple LRU-like drop).
        if self.query_plan_cache.len() >= self.query_cache_size {
            if let Some(entry) = self.query_plan_cache.iter().next().map(|e| e.key().clone()) {
                self.query_plan_cache.remove(&entry);
            }
        }
        self.query_plan_cache.insert(key, Arc::new(plan.clone()));
        Ok(plan)
    }

    async fn cypher_write_impl(
        &self,
        request: Request<CypherWriteRequest>,
    ) -> Result<Response<CypherWriteResponse>, Status> {
        self.check_not_replica()?;
        let meta_uid = meta_user_id(request.metadata());
        let req = request.into_inner();
        let author = resolve_user_id(&req.user_id, &meta_uid);
        let access = self.caller_access(&author);

        if req.cypher.is_empty() {
            return Err(Status::invalid_argument(
                "cypher write string must not be empty",
            ));
        }

        let mut compiled = polargraph_query::cypher::parse_write(&req.cypher)
            .map_err(|e| Status::invalid_argument(format!("cypher parse error: {e}")))?;
        // `graph` on the request acts like `USE GRAPH`; both must agree.
        let graph_iri = match (compiled.graph.clone(), req.graph.as_str()) {
            (Some(a), b) if !b.is_empty() && a != b => {
                return Err(Status::invalid_argument(format!(
                    "USE GRAPH <{a}> conflicts with request graph <{b}>"
                )))
            }
            (Some(a), _) => Some(a),
            (None, "") => None,
            (None, b) => {
                if let Some(mq) = &mut compiled.match_query {
                    mq.scope_to(&polargraph_query::GraphTerm::Iri(b.to_string()));
                }
                Some(b.to_string())
            }
        };
        // A user writes into a graph it can write — the default graph when
        // none is named, in which case MERGE / DELETE also stay there.
        let write_graph = match (&access, graph_iri.as_deref()) {
            (None, iri) => iri.map(|iri| self.target_graph(iri)).transpose()?,
            (Some(_), iri) => {
                Some(self.graph_for(&access, iri.unwrap_or(""), GraphAccessLevel::Write, false)?)
            }
        };
        reject_user_acl_writes(&access, write_predicates(&compiled.writes))?;

        let map_write_err = |e: polargraph_query::cypher::CypherWriteError| match e {
            polargraph_query::cypher::CypherWriteError::UnboundVariable(v) => {
                Status::invalid_argument(format!("unbound variable '{v}'"))
            }
            polargraph_query::cypher::CypherWriteError::Storage(se) => storage_err_to_status(se),
            polargraph_query::cypher::CypherWriteError::Parse(pe) => {
                Status::invalid_argument(format!("cypher parse error: {pe}"))
            }
            polargraph_query::cypher::CypherWriteError::InvalidNodeId(v) => {
                Status::invalid_argument(format!("'id' property must be a valid UUID string: {v}"))
            }
        };

        // Helper closure to run the write ops given a mutable Transaction reference.
        // Returns the WriteResult without committing.
        let execute_writes =
            |tx: &mut Transaction| -> Result<polargraph_query::cypher::WriteResult, Status> {
                let snapshot = self.snapshot_for(tx.read_ts, &access);
                if let Some(ref mq) = compiled.match_query {
                    let raw = execute_query(&mq.query, &snapshot, None, None)
                        .map_err(|e| Status::internal(format!("match query error: {e}")))?;
                    let rows = polargraph_query::cypher::apply_value_filters(
                        raw,
                        &mq.value_filters,
                        &snapshot,
                    )
                    .map_err(storage_err_to_status)?;
                    let rows = polargraph_query::cypher::apply_text_filters(
                        rows,
                        &mq.text_filters,
                        &snapshot,
                    )
                    .map_err(storage_err_to_status)?;
                    let mut all_ids: Vec<NodeId> = Vec::new();
                    let mut all_written: u64 = 0;
                    let mut all_deleted: u64 = 0;
                    for row in rows {
                        let mut row_bindings = row;
                        let r = polargraph_query::cypher::execute_write_ops_in(
                            &compiled.writes,
                            tx,
                            &snapshot,
                            &mut row_bindings,
                            write_graph,
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
                    polargraph_query::cypher::execute_write_ops_in(
                        &compiled.writes,
                        tx,
                        &snapshot,
                        &mut bindings,
                        write_graph,
                    )
                    .map_err(map_write_err)
                }
            };

        // If tx_id is set, buffer writes into the open transaction without committing.
        if !req.tx_id.is_empty() {
            // Clone the Arc before awaiting to avoid holding a DashMap shard lock.
            let open_arc = {
                let entry = self.tx_map.get(&req.tx_id).ok_or_else(|| {
                    Status::not_found(format!("unknown or expired transaction: {}", req.tx_id))
                })?;
                Arc::clone(entry.value())
            };
            let mut guard = open_arc.lock().await;
            guard.tx.set_author(author.clone());
            let result = execute_writes(&mut guard.tx)?;
            guard.last_used = Instant::now();
            debug!(
                tx_id = %req.tx_id,
                "cypher_write buffered: created={} written={} deleted={}",
                result.created_ids.len(), result.triples_written, result.triples_deleted
            );
            return Ok(Response::new(CypherWriteResponse {
                created_node_ids: result
                    .created_ids
                    .iter()
                    .map(|id| id.as_bytes().to_vec())
                    .collect(),
                triples_written: result.triples_written,
                triples_deleted: result.triples_deleted,
            }));
        }

        // Auto-commit path.
        let mut tx = self.store.begin();
        tx.set_author(author.clone());
        let result = execute_writes(&mut tx)?;

        let commit_ts = tx.commit().map_err(storage_err_to_status)?;
        debug!(
            "cypher_write: created={} written={} deleted={} commit_ts={}",
            result.created_ids.len(),
            result.triples_written,
            result.triples_deleted,
            commit_ts.0
        );

        metrics::gauge!("polargraph_triples_total").increment(result.triples_written as f64);

        Ok(Response::new(CypherWriteResponse {
            created_node_ids: result
                .created_ids
                .iter()
                .map(|id| id.as_bytes().to_vec())
                .collect(),
            triples_written: result.triples_written,
            triples_deleted: result.triples_deleted,
        }))
    }

    /// Resolve `RelationTriple.object_iri` (an IRI, `prefix:local` or a bare
    /// vocabulary name) to the object node, returning the IRIs to record in
    /// the IRI dictionary.
    #[allow(clippy::result_large_err)]
    fn resolve_object_iris<'a>(
        &self,
        triples: impl Iterator<Item = &'a mut crate::proto::Triple>,
    ) -> Result<Vec<String>, Status> {
        let vocab = self.store.vocabulary();
        let mut iris = Vec::new();
        for t in triples {
            let Some(crate::proto::triple::Kind::Relation(r)) = &mut t.kind else {
                continue;
            };
            if r.object_iri.is_empty() {
                continue;
            }
            let iri = vocab.expand(&r.object_iri);
            let node = convert::node_id_to_proto(polargraph_core::term::iri_to_node_id(&iri));
            match &r.object {
                Some(o) if *o != node => {
                    return Err(Status::invalid_argument(format!(
                        "relation object and object_iri <{iri}> name different nodes"
                    )))
                }
                _ => r.object = Some(node),
            }
            if polargraph_core::term::needs_dictionary(&iri) {
                iris.push(iri);
            }
        }
        Ok(iris)
    }

    /// After an inference run: log, update metrics, and pick up new inferred
    /// graphs in the graph-access index (they inherit their source's
    /// readers).
    pub fn after_inference(&self, stats: &owl_rl::MaterializationStats) {
        info!(
            asserted = stats.asserted,
            closed = stats.closed,
            derived = stats.derived_triples,
            full = stats.full,
            "OWL 2 RL inference applied"
        );
        metrics::gauge!("polargraph_materialization_derived_total")
            .set(stats.derived_triples as f64);
        if stats.asserted > 0 {
            self.rebuild_graph_access();
        }
    }

    /// `snapshot` without the inferred graphs when the request opts out.
    fn inferred_scope(
        &self,
        snapshot: polargraph_storage::Snapshot,
        exclude_inferred: bool,
    ) -> polargraph_storage::Snapshot {
        if exclude_inferred {
            snapshot.without_graphs(&owl_rl::InferredGraphs::load(&self.store).ids())
        } else {
            snapshot
        }
    }

    /// How a vector space stores vectors, from its registered definition.
    fn space_options(&self, space: &str) -> polargraph_storage::hnsw::SpaceOptions {
        match self.registry.get_space_def(space) {
            Some(vs) => polargraph_storage::hnsw::SpaceOptions {
                mode: vs.storage_mode,
                int8: vs.is_int8(),
            },
            None => StorageMode::Memory.into(),
        }
    }

    /// Checks shared by the vocabulary mutations.
    #[allow(clippy::result_large_err)]
    fn vocabulary_change_allowed(
        &self,
        user_id: &str,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<(), Status> {
        self.check_not_replica()?;
        require_service(&self.caller_access(&resolve_user_id(user_id, &meta_user_id(metadata))))
    }

    /// Build the access cache from scratch by scanning MEMBER_OF and HAS_ACCESS
    /// triples. Called once at startup.
    ///
    /// Algorithm:
    /// 1. Scan `MEMBER_OF` → build `group_id → Vec<user_id>` mapping.
    /// 2. Scan `HAS_ACCESS` (Relation) → build `group_id → Vec<NodeId>` direct grants.
    /// 3. Scan `HAS_ACCESS_TYPE` (Property) → build `group_id → Vec<type_name>` type grants.
    /// 4. For each user, union direct grants and type-expanded grants across all their groups.
    fn build_access_cache(
        store: &TripleStore,
        type_cache: &crate::type_index::TypeIndex,
    ) -> Result<HashMap<String, HashSet<NodeId>>, StorageError> {
        let end_of_time = polargraph_core::temporal::Timestamp::END_OF_TIME;

        // 1. group_id → list of user node IDs (only currently-valid MEMBER_OF triples).
        let mut group_to_users: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
        for triple in store.scan_by_predicate(BUILTIN_MEMBER_OF_PRED)? {
            if let Triple::Relation {
                subject: user_id,
                object: group_id,
                temporal,
                ..
            } = triple
            {
                if temporal.vt_end == end_of_time {
                    group_to_users.entry(group_id).or_default().push(user_id);
                }
            }
        }

        // 2. group_id → set of directly-granted node IDs (only currently-valid HAS_ACCESS triples).
        let mut group_to_nodes: HashMap<NodeId, HashSet<NodeId>> = HashMap::new();
        for triple in store.scan_by_predicate(BUILTIN_HAS_ACCESS_PRED)? {
            if let Triple::Relation {
                subject: group_id,
                object: node_id,
                temporal,
                ..
            } = triple
            {
                if temporal.vt_end == end_of_time {
                    group_to_nodes.entry(group_id).or_default().insert(node_id);
                }
            }
        }

        // 3. group_id → set of type-level grants (only currently-valid HAS_ACCESS_TYPE triples).
        let mut group_to_types: HashMap<NodeId, Vec<String>> = HashMap::new();
        for triple in store.scan_by_predicate(BUILTIN_HAS_ACCESS_TYPE_PRED)? {
            if let Triple::Property {
                subject: group_id,
                value: Value::Text(type_name),
                temporal,
                ..
            } = triple
            {
                if temporal.vt_end == end_of_time {
                    group_to_types.entry(group_id).or_default().push(type_name);
                }
            }
        }

        // 4. For each user, accumulate accessible node IDs from all their groups.
        let mut cache: HashMap<String, HashSet<NodeId>> = HashMap::new();
        for (group_id, users) in &group_to_users {
            // Direct node grants for this group.
            let direct_nodes: HashSet<NodeId> =
                group_to_nodes.get(group_id).cloned().unwrap_or_default();

            // Type-expanded nodes for this group.
            let mut type_nodes: HashSet<NodeId> = HashSet::new();
            if let Some(type_names) = group_to_types.get(group_id) {
                type_nodes.extend(type_cache.instances_of(&class_nodes(store, type_names)));
            }

            // Merge into each user's access set.
            for user_id in users {
                let user_key = user_id.to_string();
                let entry = cache.entry(user_key).or_default();
                entry.extend(direct_nodes.iter().copied());
                entry.extend(type_nodes.iter().copied());
            }
        }

        info!(users = cache.len(), "access cache built");
        Ok(cache)
    }

    /// Rebuild the access cache if the inserted triples contain any access-control
    /// predicates (`MEMBER_OF`, `HAS_ACCESS`, `HAS_ACCESS_TYPE`) or `__type`
    /// triples that could expand type-level grants.
    ///
    /// Called after every successful `Insert` commit.
    /// Rebuild the graph access index (after grants or memberships change).
    fn rebuild_graph_access(&self) {
        match polargraph_storage::GraphAccessIndex::build(&self.store) {
            Ok(index) => *self.graph_access.write().unwrap() = (Arc::new(index), Instant::now()),
            Err(e) => warn!("failed to rebuild graph access index: {e}"),
        }
    }

    /// The caller's graph access: `None` for a request without a user id (a
    /// trusted service call, full access); otherwise the user's grants, the
    /// default graph only when it has none. A user id is a node UUID or an
    /// IRI (mapped like any other IRI).
    fn caller_access(&self, user_id: &str) -> Option<Arc<polargraph_storage::UserGraphAccess>> {
        if user_id.is_empty() {
            return None;
        }
        let user = principal_node(user_id).ok()?;
        if self.store.is_replica()
            && self.graph_access.read().unwrap().1.elapsed() > GRAPH_ACCESS_REFRESH
        {
            self.rebuild_graph_access();
        }
        let index = Arc::clone(&self.graph_access.read().unwrap().0);
        Some(index.for_user(&user))
    }

    /// Whether a vector hit is visible to `access`: the node must have a live
    /// quad in a readable graph (HNSW itself isn't graph-aware).
    fn node_visible(
        &self,
        node: &NodeId,
        access: &Option<Arc<polargraph_storage::UserGraphAccess>>,
    ) -> bool {
        access.is_none()
            || self
                .snapshot_for(self.store.begin().read_ts, access)
                .scan_by_subject(node)
                .is_ok_and(|t| !t.is_empty())
    }

    /// A vector request's `graphs` filter ("" = default graph) as a scope;
    /// `None` when empty. Unknown graphs match nothing.
    fn vector_graph_scope(&self, graphs: &[String]) -> Option<polargraph_storage::GraphScope> {
        (!graphs.is_empty()).then(|| {
            polargraph_storage::GraphScope::set(
                graphs
                    .iter()
                    .filter_map(|iri| match iri.as_str() {
                        "" => Some(polargraph_core::id::GraphId::DEFAULT),
                        iri => self.store.graph_id(iri),
                    })
                    .collect(),
            )
        })
    }

    /// [`Self::node_visible`], and with `scope`, the live quad must be in
    /// one of its graphs.
    fn node_visible_in(
        &self,
        node: &NodeId,
        access: &Option<Arc<polargraph_storage::UserGraphAccess>>,
        scope: &Option<polargraph_storage::GraphScope>,
    ) -> bool {
        match scope {
            None => self.node_visible(node, access),
            Some(scope) => self
                .snapshot_for(self.store.begin().read_ts, access)
                .scan_scoped(Some(node), None, None, scope)
                .is_ok_and(|t| !t.is_empty()),
        }
    }

    /// The graph `iri` names, checked for `level` when the caller is a user.
    /// A service call may create the graph (`create`); a user's graph must
    /// already exist (it would otherwise have no grants), so unknown graphs
    /// are `PERMISSION_DENIED` rather than created.
    #[allow(clippy::result_large_err)]
    fn graph_for(
        &self,
        access: &Option<Arc<polargraph_storage::UserGraphAccess>>,
        iri: &str,
        level: GraphAccessLevel,
        create: bool,
    ) -> Result<polargraph_core::id::GraphId, Status> {
        if level >= GraphAccessLevel::Propose {
            reject_inferred_graph(iri)?;
        }
        if access.is_none() {
            return if create {
                self.target_graph(iri)
            } else {
                self.existing_graph(iri)
            };
        }
        let g = match self.existing_graph(iri) {
            Ok(g) => g,
            Err(e) if e.code() == tonic::Code::NotFound => {
                return Err(Status::permission_denied(format!(
                    "{} access to graph <{iri}> required (create it with CreateGraph first)",
                    level.as_str()
                )))
            }
            Err(e) => return Err(e),
        };
        require_level(access, g, level, iri)?;
        Ok(g)
    }

    /// A snapshot at `ts` restricted to what `access` may read.
    fn snapshot_for(
        &self,
        ts: polargraph_core::temporal::Timestamp,
        access: &Option<Arc<polargraph_storage::UserGraphAccess>>,
    ) -> polargraph_storage::Snapshot {
        let snapshot = self.store.snapshot(ts);
        match access {
            Some(a) => snapshot.with_readable_graphs(a.readable()),
            None => snapshot,
        }
    }

    fn update_access_cache_if_needed(&self, triples: &[Triple]) {
        if triples.iter().any(|t| {
            let p = &t.predicate().0;
            p == BUILTIN_MEMBER_OF_PRED || p == BUILTIN_HAS_GRAPH_ACCESS_PRED
        }) {
            self.rebuild_graph_access();
        }
        let needs_rebuild = triples.iter().any(|t| match t {
            Triple::Relation { predicate, .. }
                if predicate.0 == BUILTIN_MEMBER_OF_PRED
                    || predicate.0 == BUILTIN_HAS_ACCESS_PRED =>
            {
                true
            }
            Triple::Property { predicate, .. } if predicate.0 == BUILTIN_HAS_ACCESS_TYPE_PRED => {
                true
            }
            _ => false,
        });

        if !needs_rebuild {
            return;
        }
        self.rebuild_access_cache();
    }

    /// Rebuild the node-level access cache from the store and type index.
    fn rebuild_access_cache(&self) {
        match Self::build_access_cache(&self.store, &self.type_cache) {
            Ok(new_cache) => {
                *self.access_cache.write().unwrap() = new_cache;
                debug!("access cache rebuilt");
            }
            Err(e) => {
                warn!("failed to rebuild access cache: {e}");
            }
        }
    }

    /// Return the set of allowed NodeIds for `user_id`, or `None` when no
    /// filtering should be applied (empty user_id or no entry in the cache).
    fn get_access_filter(&self, user_id: &str) -> Option<HashSet<NodeId>> {
        if user_id.is_empty() {
            return None;
        }
        self.sync_types();
        self.access_cache.read().unwrap().get(user_id).cloned()
    }

    /// Catch the type index up with the change log, rebuilding the access
    /// caches that depend on what changed.
    fn sync_types(&self) {
        match self.type_cache.sync() {
            Ok(outcome) => {
                if outcome.graph_access {
                    self.rebuild_graph_access();
                }
                if outcome.types || outcome.node_access {
                    self.rebuild_access_cache();
                }
            }
            Err(e) => warn!("failed to sync the type index: {e}"),
        }
    }

    /// Current instances of the named types (names resolve through the
    /// vocabulary: bare → base, `prefix:local`, or a full IRI).
    pub fn instances_of_types(&self, names: &[String]) -> HashSet<NodeId> {
        self.sync_types();
        self.type_cache
            .instances_of(&class_nodes(&self.store, names))
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
    /// A graph that must already exist: empty IRI = default graph.
    #[allow(clippy::result_large_err)]
    fn existing_graph(&self, iri: &str) -> Result<polargraph_core::id::GraphId, Status> {
        if iri.is_empty() {
            return Ok(polargraph_core::id::GraphId::DEFAULT);
        }
        self.store
            .graph_id(iri)
            .ok_or_else(|| Status::not_found(format!("unknown graph: {iri}")))
    }

    /// A write target: empty IRI = default graph; interned on first use.
    #[allow(clippy::result_large_err)]
    /// Graph scope for `DeleteTriples`: unset = every graph (quads are not
    /// de-duplicated across graphs, so each copy is closed).
    fn delete_scope(
        &self,
        graph: Option<&crate::proto::GraphTerm>,
    ) -> Result<polargraph_storage::GraphScope, Status> {
        use crate::proto::graph_term::Kind;
        use polargraph_storage::GraphScope;
        Ok(match graph.and_then(|g| g.kind.as_ref()) {
            None | Some(Kind::DefaultGraph(false)) => GraphScope::Union,
            Some(Kind::DefaultGraph(true)) => {
                GraphScope::One(polargraph_core::id::GraphId::DEFAULT)
            }
            Some(Kind::Iri(iri)) => GraphScope::One(self.existing_graph(iri)?),
            Some(Kind::Set(set)) => GraphScope::set(
                set.iris
                    .iter()
                    .filter_map(|iri| self.store.graph_id(iri))
                    .collect(),
            ),
            Some(Kind::Var(_)) => {
                return Err(Status::invalid_argument(
                    "DeleteTriples.graph cannot be a variable",
                ))
            }
        })
    }

    /// Apply a `CypherQueryRequest.graphs` dataset to the MATCH patterns not
    /// already scoped by `USE GRAPH`.
    fn cypher_dataset(
        &self,
        mut compiled: polargraph_query::cypher::CompiledQuery,
        graphs: &[String],
    ) -> polargraph_query::cypher::CompiledQuery {
        if !graphs.is_empty() {
            let ids = graphs
                .iter()
                .filter_map(|iri| match iri.as_str() {
                    "" => Some(polargraph_core::id::GraphId::DEFAULT),
                    iri => self.store.graph_id(iri),
                })
                .collect();
            compiled.scope_to(&polargraph_query::GraphTerm::Set(ids));
        }
        compiled
    }

    /// Confine a Cypher read to its dataset (`graphs`, "" = default graph):
    /// filters, projections and aggregates read the snapshot directly, so
    /// scoping the patterns alone isn't enough.
    fn cypher_snapshot_scope(
        &self,
        snapshot: polargraph_storage::Snapshot,
        graphs: &[String],
    ) -> polargraph_storage::Snapshot {
        if graphs.is_empty() {
            return snapshot;
        }
        snapshot.within_graphs(graphs.iter().filter_map(|iri| match iri.as_str() {
            "" => Some(polargraph_core::id::GraphId::DEFAULT),
            iri => self.store.graph_id(iri),
        }))
    }

    fn target_graph(&self, iri: &str) -> Result<polargraph_core::id::GraphId, Status> {
        if iri.is_empty() {
            return Ok(polargraph_core::id::GraphId::DEFAULT);
        }
        reject_inferred_graph(iri)?;
        // create_graph logs GRAPH_CREATED for a new graph (change feed).
        self.store
            .create_graph(iri, &[])
            .map_err(storage_err_to_status)
    }

    fn check_not_replica(&self) -> Result<(), Status> {
        if self.store.is_replica() {
            Err(replica_not_writable())
        } else {
            Ok(())
        }
    }
}

/// Class nodes for type names, resolved through the current vocabulary.
fn class_nodes(store: &TripleStore, names: &[String]) -> Vec<NodeId> {
    let vocab = store.vocabulary();
    names
        .iter()
        .map(|n| polargraph_core::term::iri_to_node_id(&vocab.expand(n)))
        .collect()
}

// ── Access-control helpers ────────────────────────────────────────────────────

/// Extract the `x-polargraph-user-id` gRPC metadata header value, returning
/// an empty string when the header is absent or not valid ASCII.
fn meta_user_id(metadata: &tonic::metadata::MetadataMap) -> String {
    metadata
        .get("x-polargraph-user-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string()
}

/// Resolve the effective user identity: prefer the explicit proto field over
/// the metadata header.  Returns an empty string when neither is set.
fn resolve_user_id(field: &str, from_meta: &str) -> String {
    if !field.is_empty() {
        field.to_string()
    } else {
        from_meta.to_string()
    }
}

/// Proto form of a SHACL result, nodes rendered as IRIs.
fn validation_result_to_proto(
    r: &polargraph_shacl::ValidationResult,
    view: &polargraph_shacl::DataView<'_>,
) -> Result<ValidationResult, StorageError> {
    use polargraph_shacl::{shapes::Path, vocab::SH, Obj};
    fn path_str(p: &Path) -> String {
        match p {
            Path::Predicate(iri) => format!("<{iri}>"),
            Path::Inverse(inner) => format!("^{}", path_str(inner)),
            Path::Sequence(steps) => steps.iter().map(path_str).collect::<Vec<_>>().join("/"),
        }
    }
    let split = |o: &Obj| -> Result<(String, Option<crate::proto::Value>), StorageError> {
        Ok(match o {
            Obj::Node(n) => (view.iri(n)?, None),
            Obj::Lit(v) => (String::new(), Some(convert::value_to_proto(v))),
        })
    };
    let (focus_node, focus_literal) = split(&r.focus_node)?;
    let (value_node, value_literal) = match &r.value {
        Some(v) => split(v)?,
        None => (String::new(), None),
    };
    Ok(ValidationResult {
        focus_node,
        focus_literal,
        path: r.path.as_ref().map(path_str).unwrap_or_default(),
        value_node,
        value_literal,
        source_shape: view.iri(&r.source_shape)?,
        constraint_component: format!("{SH}{}", r.component),
        severity: format!("{SH}{}", r.severity.local_name()),
        message: r.message.clone(),
    })
}

/// What a `Subscribe` stream delivers.
struct ChangeFilter {
    graphs: Option<HashSet<polargraph_core::id::GraphId>>,
    predicates: Option<HashSet<String>>,
    types: Vec<String>,
    include_values: bool,
}

impl PolarGraphServer {
    /// The events a logged commit produces for one subscriber.
    fn change_events(
        &self,
        record: &polargraph_storage::ChangeRecord,
        filter: &ChangeFilter,
        access: &Option<Arc<polargraph_storage::UserGraphAccess>>,
    ) -> Vec<ChangeEvent> {
        use polargraph_storage::GraphOp;
        let visible = |g: polargraph_core::id::GraphId| {
            filter.graphs.as_ref().map_or(true, |gs| gs.contains(&g))
                && access
                    .as_ref()
                    .map_or(true, |a| a.allows(g, GraphAccessLevel::Read))
        };
        let iri = |g: polargraph_core::id::GraphId| self.store.graph_iri(g).unwrap_or_default();
        let event = |g, kind: ChangeKind| ChangeEvent {
            commit_ts: record.commit_ts.0,
            graph: iri(g),
            kind: kind as i32,
            author: record.author.clone(),
            ..Default::default()
        };

        let types: Option<HashSet<NodeId>> =
            (!filter.types.is_empty()).then(|| self.instances_of_types(&filter.types));

        let mut out = Vec::new();
        for (g, triple) in &record.quads {
            if !visible(*g)
                || filter
                    .predicates
                    .as_ref()
                    .is_some_and(|ps| !ps.contains(&triple.predicate().0))
                || types
                    .as_ref()
                    .is_some_and(|ts| !ts.contains(&triple.subject()))
            {
                continue;
            }
            let Some(quad) = convert::triple_to_proto(triple, filter.include_values) else {
                continue;
            };
            let kind =
                if triple.temporal().vt_end == polargraph_core::temporal::Timestamp::END_OF_TIME {
                    ChangeKind::Assert
                } else {
                    ChangeKind::Close
                };
            let edge_id = match triple {
                Triple::Relation { edge_id, .. } => edge_id.as_bytes().to_vec(),
                _ => vec![],
            };
            out.push(ChangeEvent {
                quad: Some(quad),
                edge_id,
                ..event(*g, kind)
            });
        }
        for op in &record.graph_ops {
            match *op {
                GraphOp::Created(g) if visible(g) => out.push(event(g, ChangeKind::GraphCreated)),
                GraphOp::Dropped(g) if visible(g) => out.push(event(g, ChangeKind::GraphDropped)),
                GraphOp::Copied { source, target } if visible(target) => out.push(ChangeEvent {
                    // Don't name a source graph the subscriber can't read.
                    source_graph: if access
                        .as_ref()
                        .map_or(true, |a| a.allows(source, GraphAccessLevel::Read))
                    {
                        iri(source)
                    } else {
                        String::new()
                    },
                    ..event(target, ChangeKind::GraphCopied)
                }),
                _ => {}
            }
        }
        out
    }
}

/// A user or group named by node UUID or IRI.
#[allow(clippy::result_large_err)]
fn principal_node(s: &str) -> Result<NodeId, Status> {
    if s.is_empty() {
        return Err(Status::invalid_argument("principal must not be empty"));
    }
    Ok(uuid::Uuid::parse_str(s)
        .map(NodeId)
        .unwrap_or_else(|_| polargraph_core::term::iri_to_node_id(s)))
}

/// Predicates that define access control. Only service calls may write them:
/// a user adding `MEMBER_OF` to a privileged group would escalate itself.
const ACCESS_CONTROL_PREDS: [&str; 5] = [
    BUILTIN_MEMBER_OF_PRED,
    BUILTIN_HAS_ACCESS_PRED,
    BUILTIN_HAS_ACCESS_TYPE_PRED,
    BUILTIN_HAS_GRAPH_ACCESS_PRED,
    polargraph_core::schema::BUILTIN_GRAPH_ACCESS_LEVEL_PRED,
];

/// `PERMISSION_DENIED` when a user's write contains access-control triples.
#[allow(clippy::result_large_err)]
fn reject_user_acl_writes<'a>(
    access: &Option<Arc<polargraph_storage::UserGraphAccess>>,
    predicates: impl IntoIterator<Item = &'a str>,
) -> Result<(), Status> {
    if access.is_none() {
        return Ok(());
    }
    match predicates
        .into_iter()
        .find(|p| ACCESS_CONTROL_PREDS.contains(p))
    {
        Some(p) => Err(Status::permission_denied(format!(
            "{p} triples are access control and can only be written by a service call"
        ))),
        None => Ok(()),
    }
}

/// Predicates a Cypher write would store (relation types, property keys).
fn write_predicates(ops: &[polargraph_query::cypher::WriteOp]) -> Vec<&str> {
    use polargraph_query::cypher::WriteOp;
    let mut preds = Vec::new();
    for op in ops {
        match op {
            WriteOp::CreateNode { props, .. } | WriteOp::Merge { props, .. } => {
                preds.extend(props.iter().map(|(k, _)| k.as_str()))
            }
            WriteOp::CreateRelation { predicate, .. } => preds.push(predicate.as_str()),
            WriteOp::Set(clauses) => preds.extend(clauses.iter().map(|c| c.key.as_str())),
            WriteOp::Delete(_) => {}
        }
    }
    preds
}

/// Inferred graphs are written by inference only (step 9): any other write,
/// grant or graph admin operation on one is `PERMISSION_DENIED`.
#[allow(clippy::result_large_err)]
fn reject_inferred_graph(iri: &str) -> Result<(), Status> {
    if owl_rl::is_inferred_graph_iri(iri) {
        return Err(Status::permission_denied(format!(
            "<{iri}> is an inferred graph; it is written by OWL RL inference only"
        )));
    }
    Ok(())
}

/// `PERMISSION_DENIED` when the caller is a user (service-only RPCs).
#[allow(clippy::result_large_err)]
fn require_service(
    access: &Option<Arc<polargraph_storage::UserGraphAccess>>,
) -> Result<(), Status> {
    match access {
        Some(_) => Err(Status::permission_denied(
            "this RPC is limited to service calls (no user id)",
        )),
        None => Ok(()),
    }
}

/// `PERMISSION_DENIED` unless the caller (if any) has `level` on `g`.
#[allow(clippy::result_large_err)]
fn require_level(
    access: &Option<Arc<polargraph_storage::UserGraphAccess>>,
    g: polargraph_core::id::GraphId,
    level: GraphAccessLevel,
    graph_name: &str,
) -> Result<(), Status> {
    match access {
        Some(a) if !a.allows(g, level) => Err(Status::permission_denied(format!(
            "{} access to graph <{graph_name}> required",
            level.as_str()
        ))),
        _ => Ok(()),
    }
}

/// Filter a set of Datalog `Bindings` rows to only those where every bound
/// NodeId is present in `allowed`. When `allowed` is `None`, returns all rows.
fn filter_bindings(results: Vec<Bindings>, allowed: Option<&HashSet<NodeId>>) -> Vec<Bindings> {
    match allowed {
        None => results,
        Some(set) => results
            .into_iter()
            .filter(|b| b.values().all(|id| set.contains(id)))
            .collect(),
    }
}

/// Like [`filter_bindings`] but for full solutions, filtering on the node
/// bindings (a value's subject was already checked by the scan).
fn filter_bindings_full(
    results: Vec<Solution>,
    allowed: Option<&HashSet<NodeId>>,
) -> Vec<Solution> {
    match allowed {
        None => results,
        Some(set) => results
            .into_iter()
            .filter(|s| s.nodes.values().all(|id| set.contains(id)))
            .collect(),
    }
}

// ── Service impl ──────────────────────────────────────────────────────────────

#[tonic::async_trait]
impl PolarGraphService for PolarGraphServer {
    // ── Counters (step 9d) ────────────────────────────────────────────────────

    async fn increment_counters(
        &self,
        request: Request<crate::proto::IncrementCountersRequest>,
    ) -> Result<Response<crate::proto::IncrementCountersResponse>, Status> {
        self.check_not_replica()?;
        let meta_uid = meta_user_id(request.metadata());
        let req = request.into_inner();
        require_service(&self.caller_access(&resolve_user_id(&req.user_id, &meta_uid)))?;
        let increments = req
            .increments
            .iter()
            .map(|i| {
                let node = i
                    .node
                    .as_ref()
                    .ok_or_else(|| Status::invalid_argument("increment needs a node"))?;
                Ok((convert::node_id_from_proto(node)?, i.delta))
            })
            .collect::<Result<Vec<_>, Status>>()?;
        self.store
            .increment_counters(&req.namespace, &increments)
            .map_err(vocab_err_to_status)?;
        Ok(Response::new(crate::proto::IncrementCountersResponse {}))
    }

    async fn get_counters(
        &self,
        request: Request<crate::proto::GetCountersRequest>,
    ) -> Result<Response<crate::proto::GetCountersResponse>, Status> {
        let meta_uid = meta_user_id(request.metadata());
        let req = request.into_inner();
        require_service(&self.caller_access(&resolve_user_id(&req.user_id, &meta_uid)))?;
        let nodes = req
            .nodes
            .iter()
            .map(convert::node_id_from_proto)
            .collect::<Result<Vec<_>, Status>>()?;
        let values = self
            .store
            .get_counters(&req.namespace, &nodes)
            .map_err(vocab_err_to_status)?;
        Ok(Response::new(crate::proto::GetCountersResponse { values }))
    }

    // ── Vocabulary (docs/design/cypher-rdf.md) ────────────────────────────────

    async fn get_vocabulary(
        &self,
        _request: Request<GetVocabularyRequest>,
    ) -> Result<Response<crate::proto::Vocabulary>, Status> {
        Ok(Response::new(self.vocabulary_proto()))
    }

    async fn set_vocabulary_base(
        &self,
        request: Request<SetVocabularyBaseRequest>,
    ) -> Result<Response<crate::proto::Vocabulary>, Status> {
        let (metadata, _, req) = request.into_parts();
        self.vocabulary_change_allowed(&req.user_id, &metadata)?;
        self.store
            .set_vocabulary_base(&req.base)
            .map_err(vocab_err_to_status)?;
        info!(base = %req.base, "vocabulary base set");
        Ok(Response::new(self.vocabulary_proto()))
    }

    async fn put_prefix(
        &self,
        request: Request<PutPrefixRequest>,
    ) -> Result<Response<crate::proto::Vocabulary>, Status> {
        let (metadata, _, req) = request.into_parts();
        self.vocabulary_change_allowed(&req.user_id, &metadata)?;
        self.store
            .put_prefix(&req.name, &req.namespace)
            .map_err(vocab_err_to_status)?;
        info!(prefix = %req.name, namespace = %req.namespace, "vocabulary prefix set");
        Ok(Response::new(self.vocabulary_proto()))
    }

    async fn remove_prefix(
        &self,
        request: Request<RemovePrefixRequest>,
    ) -> Result<Response<crate::proto::Vocabulary>, Status> {
        let (metadata, _, req) = request.into_parts();
        self.vocabulary_change_allowed(&req.user_id, &metadata)?;
        if self
            .store
            .remove_prefix(&req.name)
            .map_err(vocab_err_to_status)?
        {
            info!(prefix = %req.name, "vocabulary prefix removed");
        }
        Ok(Response::new(self.vocabulary_proto()))
    }

    async fn convert_legacy_data(
        &self,
        request: Request<ConvertLegacyDataRequest>,
    ) -> Result<Response<ConvertLegacyDataResponse>, Status> {
        let (metadata, _, req) = request.into_parts();
        self.vocabulary_change_allowed(&req.user_id, &metadata)?;
        if self.converting.swap(true, Ordering::SeqCst) {
            return Err(Status::aborted("a legacy conversion is already running"));
        }
        let store = self.store.clone();
        let dry_run = req.dry_run;
        let result = tokio::task::spawn_blocking(move || store.convert_legacy(dry_run)).await;
        self.converting.store(false, Ordering::SeqCst);
        let report = result
            .map_err(|e| Status::internal(format!("conversion task failed: {e}")))?
            .map_err(storage_err_to_status)?;
        if !dry_run {
            self.refresh_legacy_status();
            self.sync_types();
            self.rebuild_access_cache();
            info!(
                renamed = report.renamed.len(),
                merged = report.merged.len(),
                labels = report.labels_converted,
                "legacy data converted"
            );
        }
        let predicates = report
            .renamed
            .iter()
            .map(|(from, to)| PredicateConversion {
                from: from.clone(),
                to: to.clone(),
                merged: false,
                quads_moved: 0,
            })
            .chain(
                report
                    .merged
                    .iter()
                    .map(|(from, to, moved)| PredicateConversion {
                        from: from.clone(),
                        to: to.clone(),
                        merged: true,
                        quads_moved: *moved,
                    }),
            )
            .collect();
        let legacy = if dry_run {
            self.legacy_status()
        } else {
            Arc::clone(&self.legacy.read().unwrap().0)
        };
        Ok(Response::new(ConvertLegacyDataResponse {
            dry_run,
            predicates,
            labels_converted: report.labels_converted,
            legacy: Some(legacy_to_proto(&legacy)),
        }))
    }

    type StreamWalStream = ReceiverStream<Result<WalEntry, Status>>;
    type QueryStreamStream = ReceiverStream<Result<QueryStreamChunk, Status>>;
    type CypherQueryStreamStream = ReceiverStream<Result<QueryStreamChunk, Status>>;
    type ExportGraphStream = ReceiverStream<Result<ExportGraphChunk, Status>>;
    type SubscribeStream = ReceiverStream<Result<ChangeEvent, Status>>;

    /// Insert one or more triples atomically.
    async fn create_graph(
        &self,
        request: Request<CreateGraphRequest>,
    ) -> Result<Response<CreateGraphResponse>, Status> {
        self.check_not_replica()?;
        let meta_uid = meta_user_id(request.metadata());
        let req = request.into_inner();
        let author = resolve_user_id(&req.user_id, &meta_uid);
        let access = self.caller_access(&author);
        if req.iri.is_empty() {
            return Err(Status::invalid_argument("graph iri must not be empty"));
        }
        reject_inferred_graph(&req.iri)?;
        // A user may create a new graph (and becomes its admin); changing an
        // existing graph's metadata needs admin.
        let creator = match (&access, self.store.graph_id(&req.iri)) {
            (Some(_), Some(g)) => {
                require_level(&access, g, GraphAccessLevel::Admin, &req.iri)?;
                None
            }
            (Some(_), None) => Some(principal_node(&resolve_user_id(&req.user_id, &meta_uid))?),
            (None, _) => None,
        };
        let metadata = metadata_from_proto(&req.metadata)?;
        let store = self.store.clone();
        let graph = tokio::task::spawn_blocking(move || -> Result<GraphInfo, StorageError> {
            let g = store.create_graph_by(&req.iri, &metadata, &author)?;
            if let Some(user) = creator {
                store.grant_graph_access(user, g, GraphAccessLevel::Admin)?;
            }
            graph_info(&store, g, req.iri)
        })
        .await
        .map_err(|e| Status::internal(format!("create_graph task failed: {e}")))?
        .map_err(storage_err_to_status)?;
        if access.is_some() {
            self.rebuild_graph_access();
        }
        Ok(Response::new(CreateGraphResponse { graph: Some(graph) }))
    }

    async fn list_graphs(
        &self,
        request: Request<ListGraphsRequest>,
    ) -> Result<Response<ListGraphsResponse>, Status> {
        let user_id = meta_user_id(request.metadata());
        let req = request.into_inner();
        let access = self.caller_access(&user_id);
        let filter = metadata_from_proto(&req.filter)?;
        let mut graphs = Vec::new();
        for (g, iri) in self.store.list_graphs() {
            if iri == polargraph_storage::SYSTEM_GRAPH_IRI && !req.include_system {
                continue;
            }
            // Users only see graphs they can read (the system graph is never
            // readable through a user's grants).
            if access
                .as_ref()
                .is_some_and(|a| !a.allows(g, GraphAccessLevel::Read))
            {
                continue;
            }
            let info = graph_info(&self.store, g, iri).map_err(storage_err_to_status)?;
            let vocab = self.store.vocabulary();
            let has = |(p, v): &(String, Value)| {
                info.metadata.iter().any(|m| {
                    m.predicate == vocab.canonical_predicate(p).as_ref()
                        && m.value
                            .as_ref()
                            .and_then(|pv| convert::value_from_proto(pv).ok())
                            .as_ref()
                            == Some(v)
                })
            };
            if filter.iter().all(has) {
                graphs.push(info);
            }
        }
        Ok(Response::new(ListGraphsResponse { graphs }))
    }

    async fn graph_stats(
        &self,
        request: Request<GraphStatsRequest>,
    ) -> Result<Response<GraphStatsResponse>, Status> {
        let access = self.caller_access(&meta_user_id(request.metadata()));
        let iri = request.into_inner().iri;
        let g = self.existing_graph(&iri)?;
        require_level(&access, g, GraphAccessLevel::Read, &iri)?;
        let store = self.store.clone();
        let stats = tokio::task::spawn_blocking(move || store.graph_stats(g))
            .await
            .map_err(|e| Status::internal(format!("graph_stats task failed: {e}")))?
            .map_err(storage_err_to_status)?;
        Ok(Response::new(GraphStatsResponse {
            iri,
            live_quads: stats.live_quads,
            last_write_tt: stats.last_write_tt,
        }))
    }

    async fn copy_graph(
        &self,
        request: Request<CopyGraphRequest>,
    ) -> Result<Response<CopyGraphResponse>, Status> {
        self.check_not_replica()?;
        let meta_uid = meta_user_id(request.metadata());
        let req = request.into_inner();
        let author = resolve_user_id(&req.user_id, &meta_uid);
        let access = self.caller_access(&author);
        let source = self.graph_for(&access, &req.source, GraphAccessLevel::Read, false)?;
        let target = self.graph_for(&access, &req.target, GraphAccessLevel::Admin, true)?;
        let store = self.store.clone();
        let quads = tokio::task::spawn_blocking(move || {
            store.copy_graph_by(source, target, req.clear_target, &author)
        })
        .await
        .map_err(|e| Status::internal(format!("copy_graph task failed: {e}")))?
        .map_err(storage_err_to_status)?;
        Ok(Response::new(CopyGraphResponse {
            quads: quads as u64,
        }))
    }

    async fn move_graph(
        &self,
        request: Request<MoveGraphRequest>,
    ) -> Result<Response<CopyGraphResponse>, Status> {
        self.check_not_replica()?;
        let meta_uid = meta_user_id(request.metadata());
        let req = request.into_inner();
        let author = resolve_user_id(&req.user_id, &meta_uid);
        let access = self.caller_access(&author);
        let source = self.graph_for(&access, &req.source, GraphAccessLevel::Admin, false)?;
        let target = self.graph_for(&access, &req.target, GraphAccessLevel::Admin, true)?;
        let store = self.store.clone();
        let quads =
            tokio::task::spawn_blocking(move || store.move_graph_by(source, target, &author))
                .await
                .map_err(|e| Status::internal(format!("move_graph task failed: {e}")))?
                .map_err(storage_err_to_status)?;
        Ok(Response::new(CopyGraphResponse {
            quads: quads as u64,
        }))
    }

    async fn drop_graph(
        &self,
        request: Request<DropGraphRequest>,
    ) -> Result<Response<DropGraphResponse>, Status> {
        self.check_not_replica()?;
        let meta_uid = meta_user_id(request.metadata());
        let req = request.into_inner();
        let author = resolve_user_id(&req.user_id, &meta_uid);
        let access = self.caller_access(&author);
        let g = self.graph_for(&access, &req.iri, GraphAccessLevel::Admin, false)?;
        let store = self.store.clone();
        let closed = tokio::task::spawn_blocking(move || store.drop_graph_by(g, &author))
            .await
            .map_err(|e| Status::internal(format!("drop_graph task failed: {e}")))?
            .map_err(storage_err_to_status)?;
        Ok(Response::new(DropGraphResponse {
            quads_closed: closed as u64,
        }))
    }

    async fn export_graph(
        &self,
        request: Request<ExportGraphRequest>,
    ) -> Result<Response<Self::ExportGraphStream>, Status> {
        let user_id = meta_user_id(request.metadata());
        let req = request.into_inner();
        let access = self.caller_access(&user_id);
        let graphs: Vec<(polargraph_core::id::GraphId, String)> = if req.all_graphs {
            std::iter::once((polargraph_core::id::GraphId::DEFAULT, String::new()))
                .chain(
                    self.store
                        .list_graphs()
                        .into_iter()
                        .filter(|(_, iri)| iri != polargraph_storage::SYSTEM_GRAPH_IRI),
                )
                // A user's dataset export covers the graphs it can read.
                .filter(|(g, _)| {
                    access
                        .as_ref()
                        .map_or(true, |a| a.allows(*g, GraphAccessLevel::Read))
                })
                .collect()
        } else {
            let g = self.existing_graph(&req.iri)?;
            require_level(&access, g, GraphAccessLevel::Read, &req.iri)?;
            vec![(g, req.iri)]
        };
        let snapshot = self.snapshot_for(self.store.begin().read_ts, &access);
        let (tx, rx) = mpsc::channel::<Result<ExportGraphChunk, Status>>(4);
        tokio::task::spawn_blocking(move || {
            for (g, iri) in graphs {
                let triples = match snapshot.scan_graph(g) {
                    Ok(t) => t,
                    Err(e) => {
                        let _ = tx.blocking_send(Err(storage_err_to_status(e)));
                        return;
                    }
                };
                for chunk in triples.chunks(STREAM_CHUNK_SIZE) {
                    let quads = chunk
                        .iter()
                        .filter_map(|t| exported_quad(t, &iri))
                        .collect();
                    if tx.blocking_send(Ok(ExportGraphChunk { quads })).is_err() {
                        return;
                    }
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn apply_changes(
        &self,
        request: Request<ApplyChangesRequest>,
    ) -> Result<Response<ApplyChangesResponse>, Status> {
        self.check_not_replica()?;
        let meta_uid = meta_user_id(request.metadata());
        let mut req = request.into_inner();
        let resolved =
            self.resolve_object_iris(req.adds.iter_mut().flat_map(|g| g.triples.iter_mut()))?;
        req.iris.extend(resolved);
        let author = resolve_user_id(&req.user_id, &meta_uid);
        let access = self.caller_access(&author);

        let n_adds: usize = req.adds.iter().map(|g| g.triples.len()).sum();
        let n_changes = n_adds + req.retractions.len() + req.edge_annotations.len();
        if n_changes == 0 && req.iris.is_empty() {
            return Err(Status::invalid_argument(
                "changeset must contain adds, retractions, annotations or IRIs",
            ));
        }
        if n_changes > MAX_CHANGESET {
            return Err(Status::resource_exhausted(format!(
                "changeset has {n_changes} adds + retractions; the limit is {MAX_CHANGESET} — split it"
            )));
        }
        if req.iris.iter().any(|iri| iri.is_empty()) {
            return Err(Status::invalid_argument(
                "iris must not contain empty strings",
            ));
        }
        if req.read_ts < 0 {
            return Err(Status::invalid_argument("read_ts must not be negative"));
        }

        // ── adds ──────────────────────────────────────────────────────────────
        type Add = (
            Triple,
            polargraph_core::id::GraphId,
            polargraph_storage::WriteMode,
        );
        let mut adds: Vec<Add> = Vec::with_capacity(n_adds);
        let mut edge_ids: Vec<Vec<u8>> = Vec::new();
        for group in &req.adds {
            let g = self.graph_for(&access, &group.graph, GraphAccessLevel::Write, true)?;
            for proto_triple in &group.triples {
                let (triples, edge_id) = convert::triples_from_proto(proto_triple)?;
                let mode = convert::write_mode_from_proto(proto_triple)?;
                adds.extend(triples.into_iter().map(|t| (t, g, mode)));
                if let Some(eid) = edge_id {
                    edge_ids.push(eid.0.as_bytes().to_vec());
                }
            }
        }
        if !req.edge_annotations.is_empty() {
            let g = self.graph_for(&access, "", GraphAccessLevel::Write, true)?;
            for ann in &req.edge_annotations {
                adds.push((
                    convert::edge_annotation_from_proto(ann)?,
                    g,
                    polargraph_storage::WriteMode::Auto,
                ));
            }
        }
        for (triple, _, _) in &adds {
            if let Triple::Relation {
                subject,
                predicate,
                object,
                ..
            } = triple
            {
                self.edge_registry
                    .validate_cardinality(predicate.0.as_str(), *subject, *object, &self.store)
                    .map_err(|e| Status::failed_precondition(e.message))?;
            }
        }

        // ── retractions: resolve each to the live quad at the read point ──────
        let read_at = if req.read_ts > 0 {
            polargraph_core::temporal::Timestamp(req.read_ts)
        } else {
            self.store.begin().read_ts
        };
        let snapshot = self.store.snapshot(read_at);
        let mut closes: Vec<(Triple, polargraph_core::id::GraphId)> = Vec::new();
        let mut not_found: u64 = 0;
        for r in &req.retractions {
            // `all_graphs`: every graph holding the quad that the caller may
            // write (inferred graphs never).
            let scope = if r.all_graphs {
                polargraph_storage::GraphScope::Union
            } else if access.is_some() {
                polargraph_storage::GraphScope::One(self.graph_for(
                    &access,
                    &r.graph,
                    GraphAccessLevel::Write,
                    false,
                )?)
            } else {
                match self.existing_graph(&r.graph) {
                    Ok(g) => polargraph_storage::GraphScope::One(g),
                    Err(_) => {
                        not_found += 1;
                        continue;
                    }
                }
            };
            let subject = convert::node_id_from_proto(
                r.subject
                    .as_ref()
                    .ok_or_else(|| Status::invalid_argument("retraction subject is required"))?,
            )?;
            let (object, value) = match &r.object {
                Some(crate::proto::quad_ref::Object::Node(n)) => {
                    (convert::node_id_from_proto(n)?, None)
                }
                Some(crate::proto::quad_ref::Object::Value(v)) => {
                    let v = convert::value_from_proto(v)?;
                    (polargraph_storage::keys::value_object(&v), Some(v))
                }
                None => {
                    return Err(Status::invalid_argument(
                        "retraction object (node or value) is required",
                    ))
                }
            };
            let writable = |g: polargraph_core::id::GraphId| {
                let iri = self.store.graph_iri(g).unwrap_or_default();
                !owl_rl::is_inferred_graph_iri(&iri)
                    && iri != polargraph_storage::SYSTEM_GRAPH_IRI
                    && access
                        .as_ref()
                        .map_or(true, |a| a.allows(g, GraphAccessLevel::Write))
            };
            let matched: Vec<(polargraph_core::id::GraphId, Triple)> = snapshot
                .scan_scoped(
                    Some(&subject),
                    Some(r.predicate.as_str()),
                    Some(&object),
                    &scope,
                )
                .map_err(storage_err_to_status)?
                .into_iter()
                .filter(|(g, t)| {
                    (!r.all_graphs || writable(*g))
                        && match (t, &value) {
                            (Triple::Property { value: tv, .. }, Some(v)) => tv == v,
                            (Triple::Relation { .. }, None) => true,
                            _ => false,
                        }
                })
                .collect();
            if matched.is_empty() {
                not_found += 1;
            }
            closes.extend(matched.into_iter().map(|(g, t)| (t, g)));
        }
        if req.strict && not_found > 0 {
            return Err(Status::failed_precondition(format!(
                "{not_found} retraction(s) match no live quad; nothing was applied"
            )));
        }
        reject_user_acl_writes(
            &access,
            adds.iter()
                .map(|(t, _, _)| t.predicate().0.as_str())
                .chain(closes.iter().map(|(t, _)| t.predicate().0.as_str())),
        )?;

        // ── one transaction ───────────────────────────────────────────────────
        let mut tx = if req.read_ts > 0 {
            self.store
                .begin_at(polargraph_core::temporal::Timestamp(req.read_ts))
                .map_err(|e| Status::invalid_argument(e.to_string()))?
        } else {
            self.store.begin()
        };
        tx.set_author(author);
        let now = polargraph_core::temporal::Timestamp::now();
        for (t, g) in &closes {
            tx.insert_in(
                polargraph_storage::close_at(t.clone(), now),
                *g,
                polargraph_storage::WriteMode::Add,
            );
        }
        for (t, g, mode) in &adds {
            tx.insert_in(t.clone(), *g, *mode);
        }
        for iri in &req.iris {
            tx.bind_iri(iri.as_str());
        }
        let commit_ts = tx.commit().map_err(storage_err_to_status)?;

        let added: Vec<Triple> = adds.into_iter().map(|(t, _, _)| t).collect();
        self.update_access_cache_if_needed(&added);
        metrics::gauge!("polargraph_triples_total").increment(added.len() as f64);

        Ok(Response::new(ApplyChangesResponse {
            commit_ts: commit_ts.0,
            added: added.len() as u64,
            retracted: closes.len() as u64,
            retractions_not_found: not_found,
            edge_ids,
        }))
    }

    async fn validate_shapes(
        &self,
        request: Request<ValidateShapesRequest>,
    ) -> Result<Response<ValidateShapesResponse>, Status> {
        use polargraph_shacl::{DataView, Obj, Overlay, Shapes};
        let meta_uid = meta_user_id(request.metadata());
        let mut req = request.into_inner();
        self.resolve_object_iris(
            req.overlay_adds
                .iter_mut()
                .flat_map(|g| g.triples.iter_mut()),
        )?;
        let access = self.caller_access(&resolve_user_id(&req.user_id, &meta_uid));
        if req.shapes_graphs.is_empty() {
            return Err(Status::invalid_argument(
                "shapes_graphs must name at least one graph",
            ));
        }
        if req.read_ts < 0 {
            return Err(Status::invalid_argument("read_ts must not be negative"));
        }
        let readable = |iri: &str| -> Result<polargraph_core::id::GraphId, Status> {
            let g = self.existing_graph(iri)?;
            require_level(&access, g, GraphAccessLevel::Read, iri)?;
            Ok(g)
        };
        let shapes_scope = polargraph_storage::GraphScope::set(
            req.shapes_graphs
                .iter()
                .map(|iri| readable(iri))
                .collect::<Result<_, _>>()?,
        );
        let data_scope = if req.data_graphs.is_empty() {
            polargraph_storage::GraphScope::Union
        } else {
            polargraph_storage::GraphScope::set(
                req.data_graphs
                    .iter()
                    .map(|iri| readable(iri))
                    .collect::<Result<_, _>>()?,
            )
        };

        let mut overlay = Overlay::default();
        for group in &req.overlay_adds {
            for t in &group.triples {
                overlay.adds.extend(convert::triples_from_proto(t)?.0);
            }
        }
        for r in &req.overlay_retractions {
            let g = if r.all_graphs {
                None
            } else {
                let Ok(g) = self.existing_graph(&r.graph) else {
                    continue;
                };
                Some(g)
            };
            let subject = convert::node_id_from_proto(
                r.subject
                    .as_ref()
                    .ok_or_else(|| Status::invalid_argument("retraction subject is required"))?,
            )?;
            let object = match &r.object {
                Some(crate::proto::quad_ref::Object::Node(n)) => {
                    Obj::Node(convert::node_id_from_proto(n)?)
                }
                Some(crate::proto::quad_ref::Object::Value(v)) => {
                    Obj::Lit(convert::value_from_proto(v)?)
                }
                None => {
                    return Err(Status::invalid_argument(
                        "retraction object (node or value) is required",
                    ))
                }
            };
            let graphs: Vec<polargraph_core::id::GraphId> = match g {
                Some(g) => vec![g],
                // `all_graphs`: every graph where the quad is live now.
                None => {
                    let key = match &object {
                        Obj::Node(n) => *n,
                        Obj::Lit(v) => polargraph_storage::keys::value_object(v),
                    };
                    self.store
                        .snapshot(self.store.begin().read_ts)
                        .scan_scoped(
                            Some(&subject),
                            Some(r.predicate.as_str()),
                            Some(&key),
                            &polargraph_storage::GraphScope::Union,
                        )
                        .map_err(storage_err_to_status)?
                        .into_iter()
                        .map(|(g, _)| g)
                        .collect()
                }
            };
            for g in graphs {
                overlay
                    .retractions
                    .push((g, subject, r.predicate.clone(), object.clone()));
            }
        }
        let has_overlay = !overlay.adds.is_empty() || !overlay.retractions.is_empty();

        let ts = if req.read_ts > 0 {
            polargraph_core::temporal::Timestamp(req.read_ts)
        } else {
            self.store.begin().read_ts
        };
        let snapshot = self.inferred_scope(self.snapshot_for(ts, &access), req.exclude_inferred);
        let all_focus = req.all_focus_nodes;
        let response = tokio::task::spawn_blocking(move || -> Result<_, Status> {
            let to_status = |e: polargraph_shacl::ShapeError| match e {
                polargraph_shacl::ShapeError::Storage(e) => storage_err_to_status(e),
                e => Status::invalid_argument(e.to_string()),
            };
            let shapes_view = DataView::new(&snapshot, shapes_scope, &Overlay::default());
            let shapes = Shapes::load(&shapes_view).map_err(to_status)?;
            let view = DataView::new(&snapshot, data_scope, &overlay);
            let touched = view.touched();
            let focus = (has_overlay && !all_focus).then_some(&touched);
            let report = polargraph_shacl::validate(&shapes, &view, focus).map_err(to_status)?;
            let results = report
                .results
                .iter()
                .map(|r| validation_result_to_proto(r, &view))
                .collect::<Result<Vec<_>, _>>()
                .map_err(storage_err_to_status)?;
            Ok(ValidateShapesResponse {
                conforms: report.conforms(),
                no_violations: !report.has_violations(),
                results,
            })
        })
        .await
        .map_err(|e| Status::internal(format!("validation task failed: {e}")))??;
        Ok(Response::new(response))
    }

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let meta_uid = meta_user_id(request.metadata());
        let req = request.into_inner();
        let user_id = resolve_user_id(&req.user_id, &meta_uid);

        let floor = self.store.changes_floor().map_err(storage_err_to_status)?;
        let start = if req.resume_after_ts == 0 {
            polargraph_core::temporal::Timestamp(self.store.oracle_ts())
        } else if req.resume_after_ts < floor.0 {
            return Err(Status::out_of_range(format!(
                "resume_after_ts {} is older than the retained change log (floor {}); \
                 re-sync and subscribe from now",
                req.resume_after_ts, floor.0
            )));
        } else {
            polargraph_core::temporal::Timestamp(req.resume_after_ts)
        };
        let filter = ChangeFilter {
            graphs: (!req.graphs.is_empty()).then(|| {
                req.graphs
                    .iter()
                    .filter_map(|iri| {
                        if iri.is_empty() {
                            Some(polargraph_core::id::GraphId::DEFAULT)
                        } else {
                            self.store.graph_id(iri)
                        }
                    })
                    .collect()
            }),
            predicates: (!req.predicates.is_empty()).then(|| {
                let vocab = self.store.vocabulary();
                req.predicates
                    .iter()
                    .map(|p| vocab.canonical_predicate(p).into_owned())
                    .collect()
            }),
            types: req.types.clone(),
            include_values: req.include_values,
        };

        let (tx, rx) = mpsc::channel::<Result<ChangeEvent, Status>>(64);
        let server = self.clone();
        tokio::spawn(async move {
            let mut commits = server.store.commit_watch();
            let mut cursor = start;
            loop {
                // Access is re-evaluated per batch, so a revoked grant stops
                // the flow mid-stream.
                let access = server.caller_access(&user_id);
                let records = match server.store.changes_after(cursor, 256) {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = tx.send(Err(storage_err_to_status(e))).await;
                        return;
                    }
                };
                let caught_up = records.len() < 256;
                for record in records {
                    cursor = record.commit_ts;
                    for event in server.change_events(&record, &filter, &access) {
                        if tx.send(Ok(event)).await.is_err() {
                            return;
                        }
                    }
                }
                if caught_up {
                    tokio::select! {
                        _ = commits.changed() => {}
                        _ = tx.closed() => return,
                        // Replicas apply batches without a local commit; poll.
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    }
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn grant_graph_access(
        &self,
        request: Request<GrantGraphAccessRequest>,
    ) -> Result<Response<GrantGraphAccessResponse>, Status> {
        self.check_not_replica()?;
        let meta_uid = meta_user_id(request.metadata());
        let req = request.into_inner();
        let access = self.caller_access(&resolve_user_id(&req.user_id, &meta_uid));
        let level = GraphAccessLevel::parse(&req.level).ok_or_else(|| {
            Status::invalid_argument(format!(
                "level must be read, propose, write or admin, got {:?}",
                req.level
            ))
        })?;
        if req.graph.is_empty() {
            return Err(Status::invalid_argument(
                "the default graph is open to everyone and takes no grants",
            ));
        }
        reject_inferred_graph(&req.graph)?;
        let g = self.existing_graph(&req.graph)?;
        require_level(&access, g, GraphAccessLevel::Admin, &req.graph)?;
        self.store
            .grant_graph_access(principal_node(&req.principal)?, g, level)
            .map_err(storage_err_to_status)?;
        self.rebuild_graph_access();
        Ok(Response::new(GrantGraphAccessResponse {}))
    }

    async fn revoke_graph_access(
        &self,
        request: Request<RevokeGraphAccessRequest>,
    ) -> Result<Response<RevokeGraphAccessResponse>, Status> {
        self.check_not_replica()?;
        let meta_uid = meta_user_id(request.metadata());
        let req = request.into_inner();
        let access = self.caller_access(&resolve_user_id(&req.user_id, &meta_uid));
        let g = self.existing_graph(&req.graph)?;
        require_level(&access, g, GraphAccessLevel::Admin, &req.graph)?;
        let revoked = self
            .store
            .revoke_graph_access(principal_node(&req.principal)?, g)
            .map_err(storage_err_to_status)?;
        self.rebuild_graph_access();
        Ok(Response::new(RevokeGraphAccessResponse { revoked }))
    }

    async fn get_graph_access(
        &self,
        request: Request<GetGraphAccessRequest>,
    ) -> Result<Response<GetGraphAccessResponse>, Status> {
        let meta_uid = meta_user_id(request.metadata());
        let req = request.into_inner();
        let caller_id = resolve_user_id(&req.user_id, &meta_uid);
        let caller = self.caller_access(&caller_id);
        let principal = principal_node(&req.principal)?;
        let target = self
            .caller_access(&principal.to_string())
            .expect("a non-empty principal always has access");
        let is_self = caller_id.is_empty() || principal_node(&caller_id)? == principal;
        let graphs = self
            .store
            .list_graphs()
            .into_iter()
            .filter(|(_, iri)| iri != polargraph_storage::SYSTEM_GRAPH_IRI)
            .filter(|(g, _)| {
                is_self
                    || caller
                        .as_ref()
                        .is_some_and(|c| c.allows(*g, GraphAccessLevel::Admin))
            })
            .filter_map(|(g, iri)| {
                target.level(g).map(|level| GraphAccessEntry {
                    graph: iri,
                    level: level.as_str().to_string(),
                })
            })
            .collect();
        Ok(Response::new(GetGraphAccessResponse { graphs }))
    }

    async fn resolve_iris(
        &self,
        request: Request<ResolveIrisRequest>,
    ) -> Result<Response<ResolveIrisResponse>, Status> {
        const MAX_NODES: usize = 10_000;
        let req = request.into_inner();
        if req.nodes.len() > MAX_NODES {
            return Err(Status::invalid_argument(format!(
                "at most {MAX_NODES} nodes per ResolveIris request"
            )));
        }
        let nodes = req
            .nodes
            .iter()
            .map(convert::node_id_from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        let stored = self.store.iris_of(&nodes).map_err(storage_err_to_status)?;
        let iris = nodes
            .iter()
            .map(|n| {
                stored
                    .get(n)
                    .cloned()
                    .unwrap_or_else(|| polargraph_core::term::fallback_iri(n))
            })
            .collect();
        Ok(Response::new(ResolveIrisResponse { iris }))
    }

    async fn insert(
        &self,
        request: Request<InsertRequest>,
    ) -> Result<Response<InsertResponse>, Status> {
        self.check_not_replica()?;
        let meta_uid = meta_user_id(request.metadata());
        let mut req = request.into_inner();
        let resolved = self.resolve_object_iris(req.triples.iter_mut())?;
        req.iris.extend(resolved);
        let author = resolve_user_id(&req.user_id, &meta_uid);
        let access = self.caller_access(&author);

        if req.triples.is_empty() && req.edge_annotations.is_empty() && req.iris.is_empty() {
            return Err(Status::invalid_argument(
                "insert request must contain at least one triple, edge annotation or IRI",
            ));
        }
        if req.iris.iter().any(|iri| iri.is_empty()) {
            return Err(Status::invalid_argument(
                "iris must not contain empty strings",
            ));
        }

        // Target graph (empty = default graph). Service calls intern it on
        // first use; users need write access to an existing graph.
        let graph = self.graph_for(&access, &req.graph, GraphAccessLevel::Write, true)?;

        // Convert proto triples → core triples (with each one's write mode),
        // collecting EdgeIds for relations.
        let mut all_triples: Vec<Triple> = Vec::new();
        let mut modes: Vec<polargraph_storage::WriteMode> = Vec::new();
        let mut edge_ids: Vec<Vec<u8>> = Vec::new();
        for proto_triple in &req.triples {
            let (triples, edge_id) = convert::triples_from_proto(proto_triple)?;
            let mode = convert::write_mode_from_proto(proto_triple)?;
            modes.extend(std::iter::repeat(mode).take(triples.len()));
            all_triples.extend(triples);
            if let Some(eid) = edge_id {
                edge_ids.push(eid.0.as_bytes().to_vec());
            }
        }

        // Convert edge annotations (RDF-star).
        for ann in &req.edge_annotations {
            let triple = convert::edge_annotation_from_proto(ann)?;
            all_triples.push(triple);
            modes.push(polargraph_storage::WriteMode::Auto);
        }
        reject_user_acl_writes(
            &access,
            all_triples.iter().map(|t| t.predicate().0.as_str()),
        )?;

        debug!(
            "insert: {} triple(s) ({} relation(s), {} annotation(s))",
            all_triples.len(),
            edge_ids.len(),
            req.edge_annotations.len()
        );

        // If a tx_id is provided, buffer into the open transaction without committing.
        if !req.tx_id.is_empty() {
            let entry = self.tx_map.get(&req.tx_id).ok_or_else(|| {
                Status::not_found(format!("unknown or expired transaction: {}", req.tx_id))
            })?;
            let mut guard = entry.lock().await;
            for (triple, mode) in all_triples.iter().zip(&modes) {
                guard.tx.set_author(author.clone());
                guard.tx.insert_in(triple.clone(), graph, *mode);
            }
            for iri in &req.iris {
                guard.tx.bind_iri(iri.as_str());
            }
            guard.last_used = Instant::now();
            debug!(tx_id = %req.tx_id, "buffered {} triple(s) into open transaction", all_triples.len());
            return Ok(Response::new(InsertResponse {
                commit_ts: 0,
                edge_ids,
            }));
        }

        // Cardinality pre-check: scan committed state before inserting.
        for triple in &all_triples {
            if let Triple::Relation {
                subject,
                predicate,
                object,
                ..
            } = triple
            {
                self.edge_registry
                    .validate_cardinality(predicate.0.as_str(), *subject, *object, &self.store)
                    .map_err(|e| Status::failed_precondition(e.message))?;
            }
        }

        // Auto-commit path: begin a new transaction, insert all, commit.
        let mut tx = self.store.begin();
        tx.set_author(author.clone());
        for (triple, mode) in all_triples.iter().zip(&modes) {
            tx.insert_in(triple.clone(), graph, *mode);
        }
        for iri in &req.iris {
            tx.bind_iri(iri.as_str());
        }
        let commit_ts = tx.commit().map_err(storage_err_to_status)?;

        // Rebuild access cache if any access-control triples were inserted.
        self.update_access_cache_if_needed(&all_triples);

        metrics::gauge!("polargraph_triples_total").increment(all_triples.len() as f64);

        Ok(Response::new(InsertResponse {
            commit_ts: commit_ts.0,
            edge_ids,
        }))
    }

    /// Execute a conjunctive query and return all satisfying bindings.
    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let meta_uid = meta_user_id(request.metadata());
        let req = request.into_inner();
        let user_id = resolve_user_id(&req.user_id, &meta_uid);
        let access = self.caller_access(&user_id);

        if req.patterns.is_empty() {
            return Err(Status::invalid_argument(
                "query must contain at least one pattern",
            ));
        }

        // Convert proto VarPatterns → datalog VarPatterns.
        let patterns = convert::var_patterns_from_proto(&req.patterns, &req.graphs, &|iri| {
            self.store.graph_id(iri)
        })?;

        let pattern_count = patterns.len();

        // Build query.
        let mut query = Query::new();
        for p in patterns {
            query.patterns.push(p);
        }

        // Resolve snapshot timestamp:
        // as_of_tx_time takes priority over snapshot_ts; 0 on either = latest.
        let tx_ts = if req.as_of_tx_time != 0 {
            req.as_of_tx_time
        } else {
            req.snapshot_ts
        };
        let mut snapshot = if tx_ts == 0 {
            self.snapshot_for(self.store.begin().read_ts, &access)
        } else {
            self.snapshot_for(polargraph_core::temporal::Timestamp(tx_ts), &access)
        };

        // Apply valid-time filter when requested.
        if req.as_of_valid_time != 0 {
            snapshot = snapshot.with_vt_as_of(req.as_of_valid_time);
        }
        let snapshot = self.inferred_scope(snapshot, req.exclude_inferred);

        debug!(
            "query: {} pattern(s) at tx_ts={} vt_as_of={:?}",
            query.patterns.len(),
            snapshot.ts.0,
            snapshot.vt_as_of
        );

        // Convert optional Datalog rules.
        let rules: Vec<_> = req
            .rules
            .iter()
            .map(|r| convert::rule_from_proto(r, &|iri| self.store.graph_id(iri)))
            .collect::<Result<_, _>>()?;

        let t0 = Instant::now();

        // When tx_id is set, use the transaction's snapshot (read_ts) and overlay
        // pending write-buffer triples for write-your-own-reads.
        let results = if !req.tx_id.is_empty() {
            let entry = self.tx_map.get(&req.tx_id).ok_or_else(|| {
                Status::not_found(format!("unknown or expired transaction: {}", req.tx_id))
            })?;
            let mut guard = entry.lock().await;
            guard.last_used = Instant::now();
            let tx_snapshot = self.inferred_scope(
                self.snapshot_for(guard.tx.read_ts, &access),
                req.exclude_inferred,
            );
            // Collect pending triples while we hold the lock.
            let pending: Vec<Triple> = guard.tx.pending_triples().to_vec();
            drop(guard);
            drop(entry);
            execute_query_with_pending_full(
                &query,
                &tx_snapshot,
                &pending,
                self.make_deadline(),
                Some(&self.edge_registry),
            )
            .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?
        } else if rules.is_empty() {
            // Fast path: pure conjunctive query against base facts only.
            execute_query_full(
                &query,
                &snapshot,
                self.make_deadline(),
                Some(&self.edge_registry),
            )
            .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?
        } else {
            // Recursive path: run rules to fixed point, then evaluate patterns
            // against the combined base + derived fact set.
            let derived: DerivedFacts =
                execute_recursive(&[], &rules, &snapshot, self.make_deadline())
                    .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?;
            execute_query_hybrid_full(&query, &snapshot, &derived, self.make_deadline())
                .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?
        };
        self.check_slow_query(
            "Query",
            t0.elapsed(),
            &format!("patterns={pattern_count} rules={}", rules.len()),
        );

        // Apply access-control filter when a user_id is set.
        let allowed = self.get_access_filter(&user_id);
        let results = filter_bindings_full(results, allowed.as_ref());
        let bindings = results.iter().map(convert::binding_to_proto_full).collect();

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
            req.node_id
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("node_id is required"))?,
        )?;

        if req.vector.is_empty() {
            return Err(Status::invalid_argument("vector must not be empty"));
        }

        let space = if req.space.is_empty() {
            "default"
        } else {
            &req.space
        };

        // Dimension validation against registered space def.
        if let Some(vs) = self.registry.get_space_def(space) {
            if req.vector.len() != vs.dimensions as usize {
                return Err(Status::invalid_argument(format!(
                    "space '{}' expects {} dimensions, got {}",
                    space,
                    vs.dimensions,
                    req.vector.len()
                )));
            }
        }

        let mode = self.space_options(space);

        debug!(
            "insert_vector: space={} node={} dim={} mode={:?}",
            space,
            node_id,
            req.vector.len(),
            mode
        );

        self.store
            .insert_vector(space, node_id, req.vector, mode)
            .map_err(storage_err_to_status)?;

        metrics::gauge!("polargraph_vector_spaces_total").set(self.store.hnsw_space_count() as f64);

        Ok(Response::new(InsertVectorResponse {}))
    }

    /// Search for the k nearest neighbors of a query vector in a named space.
    async fn search_vector(
        &self,
        request: Request<SearchVectorRequest>,
    ) -> Result<Response<SearchVectorResponse>, Status> {
        let user_id = meta_user_id(request.metadata());
        let req = request.into_inner();
        let access = self.caller_access(&user_id);

        if req.query.is_empty() {
            return Err(Status::invalid_argument("query vector must not be empty"));
        }
        let k = if req.k == 0 { 10 } else { req.k as usize };
        let space = if req.space.is_empty() {
            "default"
        } else {
            &req.space
        };
        let ef = if req.ef > 0 {
            req.ef as usize
        } else {
            self.default_vector_ef as usize
        };

        debug!(
            "search_vector: space={} dim={} k={k} ef={ef}",
            space,
            req.query.len()
        );

        // A restricted or graph-scoped search over-fetches, then drops hits
        // it can't see.
        let scope = self.vector_graph_scope(&req.graphs);
        let fetch = if access.is_some() || scope.is_some() {
            k.max(ef)
        } else {
            k
        };
        let hits = self.store.search_vector_ef(space, &req.query, fetch, ef);
        let results = hits
            .into_iter()
            .filter(|(id, _)| self.node_visible_in(id, &access, &scope))
            .take(k)
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
        let meta_uid = meta_user_id(request.metadata());
        let req = request.into_inner();
        let user_id = resolve_user_id(&req.user_id, &meta_uid);
        let access = self.caller_access(&user_id);

        if req.query.is_empty() {
            return Err(Status::invalid_argument("query vector must not be empty"));
        }
        let k = if req.k == 0 { 10 } else { req.k as usize };
        let space = if req.space.is_empty() {
            "default"
        } else {
            &req.space
        };
        let ef = if req.ef > 0 {
            req.ef as usize
        } else {
            self.default_vector_ef as usize
        };

        // Access filter (may be None when user_id is not set).
        let access_allowed = self.get_access_filter(&user_id);
        let scope = self.vector_graph_scope(&req.graphs);

        match req.filter {
            // ── NodeTypeFilter: O(1) cache read, no triple scan ───────────────
            Some(Filter::NodeTypeFilter(f)) => {
                // Clone the allowed set out from under the read lock so we don't
                // hold it across the (potentially slow) HNSW search.
                let type_allowed = self.instances_of_types(std::slice::from_ref(&f.type_name));

                debug!(
                    "search_vector_filtered(NodeType={}): {} candidates in cache",
                    f.type_name,
                    type_allowed.len()
                );

                // HNSW with large ef, then O(1)-per-candidate HashSet filter.
                let candidates = self.store.search_vector_ef(space, &req.query, ef, ef);
                let results: Vec<_> = candidates
                    .into_iter()
                    .filter(|(id, _)| type_allowed.contains(id))
                    .filter(|(id, _)| access_allowed.as_ref().map_or(true, |s| s.contains(id)))
                    .filter(|(id, _)| self.node_visible_in(id, &access, &scope))
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
                    f.from_node
                        .as_ref()
                        .ok_or_else(|| Status::invalid_argument("from_node is required"))?,
                )?;

                let snapshot = self.snapshot_for(self.store.begin().read_ts, &access);
                let deadline = self.make_deadline();
                let reach_allowed: HashSet<NodeId> = if f.max_hops == 0 {
                    reachable_from(from, &f.predicate, &snapshot, deadline)
                } else {
                    reachable_from_hops(
                        from,
                        &f.predicate,
                        &snapshot,
                        f.max_hops as usize,
                        deadline,
                    )
                }
                .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?;

                let candidates = self.store.search_vector_ef(space, &req.query, ef, ef);
                let results: Vec<_> = candidates
                    .into_iter()
                    .filter(|(id, _)| reach_allowed.contains(id))
                    .filter(|(id, _)| access_allowed.as_ref().map_or(true, |s| s.contains(id)))
                    .filter(|(id, _)| self.node_visible_in(id, &access, &scope))
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
        let user_id = meta_user_id(request.metadata());
        let req = request.into_inner();
        let access = self.caller_access(&user_id);

        if req.query.is_empty() {
            return Err(Status::invalid_argument("query vector must not be empty"));
        }
        let k = if req.k == 0 { 10 } else { req.k as usize };
        let space = if req.space.is_empty() {
            "default"
        } else {
            &req.space
        };

        let allowed: Vec<polargraph_core::NodeId> = req
            .node_ids
            .iter()
            .map(convert::node_id_from_proto)
            .collect::<Result<_, _>>()?;

        debug!(
            "search_vector_in_set: space={} set_size={} k={k}",
            space,
            allowed.len()
        );

        // Drop nodes the caller can't see (or outside `graphs`) before
        // ranking, so up to k visible nodes come back.
        let scope = self.vector_graph_scope(&req.graphs);
        let allowed: Vec<polargraph_core::NodeId> = allowed
            .into_iter()
            .filter(|id| self.node_visible_in(id, &access, &scope))
            .collect();
        let hits = self
            .store
            .search_vector_in_set(space, &req.query, k, &allowed);
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

        let space = if req.space.is_empty() {
            "default".to_string()
        } else {
            req.space.clone()
        };

        // Dimension check against registered space def.
        let expected_dims = self
            .registry
            .get_space_def(&space)
            .map(|vs| vs.dimensions as usize);

        let mut items: Vec<(polargraph_core::NodeId, Vec<f32>)> =
            Vec::with_capacity(req.items.len());
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
                            space,
                            dims,
                            item.vector.len()
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

        let mode = self.space_options(&space);

        debug!(
            "batch_insert_vectors: space={} count={} mode={:?}",
            space,
            items.len(),
            mode
        );

        let (count, errs) = self.store.batch_insert_vectors(&space, &items, mode);
        let errors = errs
            .into_iter()
            .map(|(i, e)| BatchInsertError {
                index: i as u32,
                message: e.to_string(),
            })
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
        let user_id = meta_user_id(request.metadata());
        let req = request.into_inner();
        let access = self.caller_access(&user_id);

        let start = convert::node_id_from_proto(
            req.start
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("start node_id is required"))?,
        )?;

        if req.predicate.is_empty() {
            return Err(Status::invalid_argument("predicate must not be empty"));
        }

        let snapshot = self.snapshot_for(self.store.begin().read_ts, &access);

        debug!(
            "reachable: start={} predicate={} max_hops={}",
            start, req.predicate, req.max_hops
        );

        let deadline = self.make_deadline();
        let t0 = Instant::now();
        let reachable_set = if req.max_hops == 0 {
            reachable_from(start, &req.predicate, &snapshot, deadline)
        } else {
            reachable_from_hops(
                start,
                &req.predicate,
                &snapshot,
                req.max_hops as usize,
                deadline,
            )
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
        let def_proto = req
            .definition
            .ok_or_else(|| Status::invalid_argument("definition is required"))?;
        let def = convert::node_type_def_from_proto(&def_proto)?;

        debug!("register_node_type: type={}", def.type_name);

        self.registry
            .register_type(def)
            .map_err(storage_err_to_status)?;
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

        let definition = self
            .registry
            .get_type(&req.type_name)
            .as_ref()
            .map(convert::node_type_def_to_proto);
        Ok(Response::new(GetNodeTypeResponse { definition }))
    }

    /// Return all registered node type schemas.
    async fn list_node_types(
        &self,
        _request: Request<ListNodeTypesRequest>,
    ) -> Result<Response<ListNodeTypesResponse>, Status> {
        let definitions = self
            .registry
            .list_types()
            .iter()
            .map(convert::node_type_def_to_proto)
            .collect();
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

        let props = req
            .properties
            .iter()
            .map(|(k, v)| convert::value_from_proto(v).map(|val| (k.clone(), val)))
            .collect::<Result<std::collections::HashMap<_, _>, _>>()?;

        match self.registry.validate_properties(&req.type_name, &props) {
            Ok(()) => Ok(Response::new(ValidateNodeResponse {
                valid: true,
                errors: vec![],
            })),
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
        let def_proto = req
            .definition
            .ok_or_else(|| Status::invalid_argument("definition is required"))?;
        let def = convert::edge_type_def_from_proto(&def_proto)?;

        debug!("register_edge_type: predicate={}", def.predicate);

        self.edge_registry
            .register_edge_type(def)
            .map_err(storage_err_to_status)?;
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

        let definition = self
            .edge_registry
            .get_edge_type(&req.predicate)
            .as_ref()
            .map(convert::edge_type_def_to_proto);
        Ok(Response::new(GetEdgeTypeResponse { definition }))
    }

    /// Return all registered edge type schemas.
    async fn list_edge_types(
        &self,
        _request: Request<ListEdgeTypesRequest>,
    ) -> Result<Response<ListEdgeTypesResponse>, Status> {
        let definitions = self
            .edge_registry
            .list_edge_types()
            .iter()
            .map(convert::edge_type_def_to_proto)
            .collect();
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

        let props = req
            .properties
            .iter()
            .map(|(k, v)| convert::value_from_proto(v).map(|val| (k.clone(), val)))
            .collect::<Result<std::collections::HashMap<_, _>, _>>()?;

        let subject_type = if req.subject_type.is_empty() {
            None
        } else {
            Some(req.subject_type.as_str())
        };
        let object_type = if req.object_type.is_empty() {
            None
        } else {
            Some(req.object_type.as_str())
        };

        match self
            .edge_registry
            .validate_edge(&req.predicate, subject_type, object_type, &props)
        {
            Ok(()) => Ok(Response::new(ValidateEdgeResponse {
                valid: true,
                errors: vec![],
            })),
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
            return Err(Status::invalid_argument(
                "domain_type and range_type must not be empty",
            ));
        }

        let predicates = self
            .edge_registry
            .list_predicates_between(&req.domain_type, &req.range_type);
        Ok(Response::new(ListPredicatesBetweenResponse { predicates }))
    }

    /// Check the full ontology for consistency.
    async fn validate_ontology(
        &self,
        _request: Request<ValidateOntologyRequest>,
    ) -> Result<Response<ValidateOntologyResponse>, Status> {
        let mut violations: Vec<OntologyViolation> = Vec::new();

        let edge_types = self.edge_registry.list_edge_types();
        let node_types = self.registry.list_types();

        // 1. Cardinality violations: for each constrained predicate, scan the store.
        for def in &edge_types {
            use polargraph_core::schema::Cardinality;
            if def.cardinality == Cardinality::Many {
                continue;
            }
            let triples = match self.store.scan_by_predicate(&def.predicate) {
                Ok(t) => t,
                Err(_) => continue,
            };
            // Collect relation triples only.
            let relations: Vec<_> = triples
                .iter()
                .filter_map(|t| {
                    if let Triple::Relation {
                        subject, object, ..
                    } = t
                    {
                        Some((*subject, *object))
                    } else {
                        None
                    }
                })
                .collect();

            // For OneToMany/OneToOne: each subject appears at most once.
            if matches!(
                def.cardinality,
                Cardinality::OneToMany | Cardinality::OneToOne
            ) {
                let mut subjects: HashMap<NodeId, usize> = HashMap::new();
                for (subj, _) in &relations {
                    *subjects.entry(*subj).or_insert(0) += 1;
                }
                for (subj, count) in &subjects {
                    if *count > 1 {
                        violations.push(OntologyViolation {
                            violation_type: "cardinality".into(),
                            name: def.predicate.clone(),
                            message: format!(
                                "predicate '{}' (one_to_many): subject {:?} has {} objects",
                                def.predicate, subj, count
                            ),
                        });
                    }
                }
            }

            // For ManyToOne/OneToOne: each object appears at most once.
            if matches!(
                def.cardinality,
                Cardinality::ManyToOne | Cardinality::OneToOne
            ) {
                let mut objects: HashMap<NodeId, usize> = HashMap::new();
                for (_, obj) in &relations {
                    *objects.entry(*obj).or_insert(0) += 1;
                }
                for (obj, count) in &objects {
                    if *count > 1 {
                        violations.push(OntologyViolation {
                            violation_type: "cardinality".into(),
                            name: def.predicate.clone(),
                            message: format!(
                                "predicate '{}' (many_to_one): object {:?} has {} subjects",
                                def.predicate, obj, count
                            ),
                        });
                    }
                }
            }
        }

        // 2. Inverse-predicate pair check.
        for def in &edge_types {
            let Some(inv) = &def.inverse_of else { continue };
            // Scan all (A, predicate, B) and check that (B, inv, A) exists.
            let triples = match self.store.scan_by_predicate(&def.predicate) {
                Ok(t) => t,
                Err(_) => continue,
            };
            for triple in &triples {
                let Triple::Relation {
                    subject, object, ..
                } = triple
                else {
                    continue;
                };
                // Check if (object, inv, subject) exists.
                let inv_triples = self
                    .store
                    .scan_by_subject_predicate(object, inv)
                    .unwrap_or_default();
                let found = inv_triples
                    .iter()
                    .any(|t| matches!(t, Triple::Relation { object: o, .. } if *o == *subject));
                if !found {
                    violations.push(OntologyViolation {
                        violation_type: "inverse".into(),
                        name: def.predicate.clone(),
                        message: format!(
                            "predicate '{}' inverse '{}': no ({:?} → {} → {:?}) counterpart",
                            def.predicate, inv, object, inv, subject
                        ),
                    });
                }
            }
        }

        // 3. Cycle detection in type hierarchy.
        for def in &node_types {
            for parent in &def.parent_types {
                if self.registry.is_subtype_of(parent, &def.type_name) {
                    violations.push(OntologyViolation {
                        violation_type: "cycle".into(),
                        name: def.type_name.clone(),
                        message: format!(
                            "type hierarchy cycle detected: '{}' inherits from '{}' which inherits from '{}'",
                            def.type_name, parent, def.type_name
                        ),
                    });
                }
            }
        }

        let valid = violations.is_empty();
        Ok(Response::new(ValidateOntologyResponse {
            valid,
            violations,
        }))
    }

    /// ANN vector search seeded into a conjunctive Datalog graph query.
    async fn vector_seed_query(
        &self,
        request: Request<VectorSeedQueryRequest>,
    ) -> Result<Response<VectorSeedQueryResponse>, Status> {
        let meta_uid = meta_user_id(request.metadata());
        let req = request.into_inner();
        let user_id = resolve_user_id(&req.user_id, &meta_uid);
        let access = self.caller_access(&user_id);

        if req.query_vector.is_empty() {
            return Err(Status::invalid_argument("query_vector must not be empty"));
        }
        if req.seed_variable.is_empty() {
            return Err(Status::invalid_argument("seed_variable must not be empty"));
        }

        let k = if req.k == 0 { 10 } else { req.k as usize };
        let space = if req.space.is_empty() {
            "default"
        } else {
            &req.space
        };
        let ef = if req.ef > 0 {
            req.ef as usize
        } else {
            self.default_vector_ef as usize
        };
        let seed_var = req.seed_variable.clone();

        debug!(
            "vector_seed_query: space={} dim={} k={k} seed_var={} patterns={}",
            space,
            req.query_vector.len(),
            seed_var,
            req.patterns.len()
        );

        let scope = self.vector_graph_scope(&req.graphs);

        // Step 1: ANN search with optional pre-filter.
        let ann_hits: Vec<(NodeId, f32)> = match &req.filter {
            Some(SeedFilter::NodeTypeFilter(f)) => {
                let allowed = self.instances_of_types(std::slice::from_ref(&f.type_name));
                self.store
                    .search_vector_ef(space, &req.query_vector, ef, ef)
                    .into_iter()
                    .filter(|(id, _)| allowed.contains(id))
                    .filter(|(id, _)| self.node_visible_in(id, &access, &scope))
                    .take(k)
                    .collect()
            }
            Some(SeedFilter::ReachabilityFilter(f)) => {
                let from = convert::node_id_from_proto(
                    f.from_node
                        .as_ref()
                        .ok_or_else(|| Status::invalid_argument("from_node is required"))?,
                )?;
                let snap = self.snapshot_for(self.store.begin().read_ts, &access);
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
                    .filter(|(id, _)| self.node_visible_in(id, &access, &scope))
                    .take(k)
                    .collect()
            }
            None if access.is_some() || scope.is_some() => self
                .store
                .search_vector_ef(space, &req.query_vector, k.max(ef), ef)
                .into_iter()
                .filter(|(id, _)| self.node_visible_in(id, &access, &scope))
                .take(k)
                .collect(),
            None => self.store.search_vector(space, req.query_vector.clone(), k),
        };

        if ann_hits.is_empty() {
            return Ok(Response::new(VectorSeedQueryResponse { bindings: vec![] }));
        }

        // Step 2: build score map and seed bindings.
        let score_map: HashMap<NodeId, f32> = ann_hits.iter().map(|&(id, s)| (id, s)).collect();

        let initial: Vec<Bindings> = ann_hits
            .iter()
            .map(|(id, _)| {
                let mut b = Bindings::new();
                b.insert(seed_var.clone(), *id);
                b
            })
            .collect();

        // Step 3: if no patterns, return seed bindings directly; otherwise join.
        let read_ts = if req.snapshot_ts == 0 {
            self.store.begin().read_ts
        } else {
            polargraph_core::temporal::Timestamp(req.snapshot_ts)
        };
        let snapshot = self.snapshot_for(read_ts, &access);

        let patterns = convert::var_patterns_from_proto(&req.patterns, &req.graphs, &|iri| {
            self.store.graph_id(iri)
        })?;

        let mut query = Query::new();
        for p in patterns {
            query.patterns.push(p);
        }

        let t0 = Instant::now();
        let results = execute_query_seeded(
            &query,
            &snapshot,
            initial,
            self.make_deadline(),
            Some(&self.edge_registry),
        )
        .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?;
        self.check_slow_query(
            "VectorSeedQuery",
            t0.elapsed(),
            &format!("space={space} k={k} patterns={}", query.patterns.len()),
        );

        // Apply access-control filter before building scored bindings.
        let access_allowed = self.get_access_filter(&user_id);
        let results = filter_bindings(results, access_allowed.as_ref());

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
        let mgr = self
            .backup_manager
            .as_ref()
            .ok_or_else(backup_not_configured)?;
        let info = mgr.create_backup().map_err(storage_err_to_status)?;
        info!(
            backup_id = info.backup_id,
            size_bytes = info.size_bytes,
            "backup created"
        );
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
        let mgr = self
            .backup_manager
            .as_ref()
            .ok_or_else(backup_not_configured)?;
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
        let mgr = self
            .backup_manager
            .as_ref()
            .ok_or_else(backup_not_configured)?;
        let keep_n = request.into_inner().keep_n;
        let deleted_count = mgr
            .purge_old_backups(keep_n)
            .map_err(storage_err_to_status)?;
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
            return Err(Status::invalid_argument(
                "query must contain at least one pattern",
            ));
        }

        let patterns = convert::var_patterns_from_proto(&req.patterns, &req.graphs, &|iri| {
            self.store.graph_id(iri)
        })?;

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
                format!(
                    "would apply {} migration(s): {:?} (dry run)",
                    pending.len(),
                    pending
                )
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
            format!(
                "applied {} migration(s): {:?}",
                stats.applied.len(),
                stats.applied
            )
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
        let meta_uid = meta_user_id(request.metadata());
        let req = request.into_inner();
        let user_id = resolve_user_id(&req.user_id, &meta_uid);
        let access = self.caller_access(&user_id);

        if req.cypher.is_empty() {
            return Err(Status::invalid_argument(
                "cypher query string must not be empty",
            ));
        }

        // Reject write statements early — CypherWrite is the right RPC for those.
        if polargraph_query::cypher::is_write_statement(&req.cypher) {
            return Err(Status::invalid_argument(
                "CypherQuery does not accept write statements (CREATE/MERGE/SET/DELETE); use CypherWrite instead",
            ));
        }

        // Deserialize named parameters from the request.
        let params = Self::deserialize_params(&req.params)?;

        // Look up or compile the query plan (with caching when enabled).
        let compiled = self.compiled_cypher(&req.cypher)?;

        // Substitute named parameters into the plan.
        let compiled = compiled
            .substitute_params(&params)
            .map_err(|e| Status::invalid_argument(format!("parameter error: {e}")))?;
        let compiled = self.cypher_dataset(compiled, &req.graphs);

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
            self.snapshot_for(guard.tx.read_ts, &access)
        } else {
            let tx_ts = if req.as_of_tx_time != 0 {
                req.as_of_tx_time
            } else {
                0
            };
            if tx_ts == 0 {
                self.snapshot_for(self.store.begin().read_ts, &access)
            } else {
                self.snapshot_for(polargraph_core::temporal::Timestamp(tx_ts), &access)
            }
        };
        if req.as_of_valid_time != 0 {
            snapshot = snapshot.with_vt_as_of(req.as_of_valid_time);
        }
        let snapshot = self.inferred_scope(snapshot, req.exclude_inferred);
        let snapshot = self.cypher_snapshot_scope(snapshot, &req.graphs);

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
            let ef = vn
                .ef
                .map(|e| e as usize)
                .filter(|&e| e > 0)
                .unwrap_or(if req.ef > 0 {
                    req.ef as usize
                } else {
                    self.default_vector_ef as usize
                });
            let seed_var = vn.seed_variable.clone();

            let ann_hits = self
                .store
                .search_vector_ef(space, &req.vector, ef, ef)
                .into_iter()
                .filter(|(id, _)| self.node_visible(id, &access))
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
                execute_query_seeded(
                    &compiled.query,
                    &snapshot,
                    initial,
                    deadline,
                    Some(&self.edge_registry),
                )
                .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?
            }
        } else if compiled.rules.is_empty() {
            execute_query(
                &compiled.query,
                &snapshot,
                deadline,
                Some(&self.edge_registry),
            )
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
            &format!(
                "patterns={} rules={}",
                compiled.query.patterns.len(),
                compiled.rules.len()
            ),
        );

        // Apply access-control filter before other Cypher filters.
        let access_allowed = self.get_access_filter(&user_id);
        let raw_results = filter_bindings(raw_results, access_allowed.as_ref());

        // Apply value filters (type constraints and WHERE clause checks).
        let filtered = polargraph_query::cypher::apply_value_filters(
            raw_results,
            &compiled.value_filters,
            &snapshot,
        )
        .map_err(storage_err_to_status)?;

        // Apply text filters (CONTAINS / STARTS WITH / =~).
        let filtered = polargraph_query::cypher::apply_text_filters(
            filtered,
            &compiled.text_filters,
            &snapshot,
        )
        .map_err(storage_err_to_status)?;

        // Apply edge annotation filters (WHERE r.prop <op> val).
        let filtered = polargraph_query::cypher::apply_edge_annotation_filters(
            filtered,
            &compiled.edge_annotation_filters,
            &snapshot,
        )
        .map_err(storage_err_to_status)?;

        // Apply aggregations, ORDER BY, SKIP, and LIMIT.
        let agg_rows = polargraph_query::aggregation::apply_aggregations(
            filtered,
            &compiled.group_keys,
            &compiled.aggregations,
            &compiled.order_by,
            compiled.skip,
            compiled.limit,
            Some(&snapshot),
        );

        // Build CypherQueryResponse rows.
        let rows: Vec<CypherBinding> = agg_rows
            .iter()
            .map(|row| {
                // Project group-key nodes to the declared return_vars (or all if unspecified).
                let nodes =
                    if compiled.return_vars.is_empty() && compiled.prop_projections.is_empty() {
                        row.group_keys
                            .iter()
                            .map(|(k, &id)| (k.clone(), convert::node_id_to_proto(id)))
                            .collect()
                    } else {
                        compiled
                            .return_vars
                            .iter()
                            .filter_map(|v| {
                                row.group_keys
                                    .get(v)
                                    .map(|&id| (v.clone(), convert::node_id_to_proto(id)))
                            })
                            .collect()
                    };
                let mut values: HashMap<String, _> = row
                    .agg_values
                    .iter()
                    .map(|(k, v)| (k.clone(), convert::value_to_proto(v)))
                    .collect();
                // Resolve property projections (RETURN n.prop) via snapshot lookup.
                let vocab = self.store.vocabulary();
                for (var, prop) in &compiled.prop_projections {
                    if let Some(&node_id) = row.group_keys.get(var.as_str()) {
                        let key = format!("{}.{}", var, prop);
                        if let Ok(triples) =
                            snapshot.scan_by_subject_predicate(&node_id, &vocab.expand(prop))
                        {
                            if let Some(value) = triples.into_iter().find_map(|t| match t {
                                Triple::Property { value, .. } => Some(value),
                                _ => None,
                            }) {
                                values.insert(key, convert::value_to_proto(&value));
                            }
                        }
                    }
                }
                // Resolve edge property projections (RETURN r.prop) via annotation lookup.
                for (var, prop) in &compiled.edge_prop_projections {
                    if let Some(&node_id) = row.group_keys.get(var.as_str()) {
                        let edge_id = polargraph_core::id::EdgeId(node_id.0);
                        let key = format!("{}.{}", var, prop);
                        if let Ok(Some(ann)) = self.store.get_edge_annotation(
                            edge_id,
                            &vocab.expand(prop),
                            snapshot.ts,
                        ) {
                            if let polargraph_storage::EdgeAnnotationValue::Scalar(v) = ann.value {
                                values.insert(key, convert::value_to_proto(&v));
                            }
                        }
                    }
                }
                // Resolve edge ID projections (RETURN id(r) / elementId(r)).
                // The rel var is stored in group_keys as NodeId bytes reinterpreting the EdgeId UUID.
                for proj in &compiled.edge_id_projections {
                    if let Some(&node_id) = row.group_keys.get(proj.rel_var.as_str()) {
                        let edge_id = polargraph_core::id::EdgeId(node_id.0);
                        values.insert(
                            proj.output_key.clone(),
                            convert::value_to_proto(&polargraph_core::value::Value::Text(
                                edge_id.to_string(),
                            )),
                        );
                    }
                }
                // Resolve node ID projections (RETURN id(n) / elementId(n)).
                for proj in &compiled.node_id_projections {
                    if let Some(&node_id) = row.group_keys.get(proj.node_var.as_str()) {
                        values.insert(
                            proj.output_key.clone(),
                            convert::value_to_proto(&polargraph_core::value::Value::Text(
                                node_id.to_string(),
                            )),
                        );
                    }
                }
                CypherBinding { nodes, values }
            })
            .collect();

        Ok(Response::new(CypherQueryResponse { rows }))
    }

    /// Parse and execute a Cypher write statement (CREATE, MERGE, SET, DELETE),
    /// optionally preceded by a MATCH clause.
    /// Deprecated (docs/upgrade-cypher-rdf.md): still executed for one
    /// release, with a warning, a `warning` response header and the
    /// `polargraph_deprecated_rpc_total{rpc="CypherWrite"}` counter. Use
    /// `ApplyChanges` or SPARQL Update.
    async fn cypher_write(
        &self,
        request: Request<CypherWriteRequest>,
    ) -> Result<Response<CypherWriteResponse>, Status> {
        static WARNED: std::sync::Once = std::sync::Once::new();
        WARNED.call_once(|| {
            warn!("{CYPHER_WRITE_DEPRECATION}");
        });
        metrics::counter!("polargraph_deprecated_rpc_total", "rpc" => "CypherWrite").increment(1);
        let mut response = self.cypher_write_impl(request).await?;
        response.metadata_mut().insert(
            "warning",
            tonic::metadata::MetadataValue::from_static(CYPHER_WRITE_WARNING),
        );
        Ok(response)
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
        let meta_uid = meta_user_id(request.metadata());
        let req = request.into_inner();
        let user_id = resolve_user_id(&req.user_id, &meta_uid);
        let access = self.caller_access(&user_id);

        if req.patterns.is_empty() {
            return Err(Status::invalid_argument(
                "query must contain at least one pattern",
            ));
        }

        let patterns = convert::var_patterns_from_proto(&req.patterns, &req.graphs, &|iri| {
            self.store.graph_id(iri)
        })?;

        let pattern_count = patterns.len();
        let mut query = Query::new();
        for p in patterns {
            query.patterns.push(p);
        }

        let tx_ts = if req.as_of_tx_time != 0 {
            req.as_of_tx_time
        } else {
            req.snapshot_ts
        };
        let mut snapshot = if tx_ts == 0 {
            self.snapshot_for(self.store.begin().read_ts, &access)
        } else {
            self.snapshot_for(polargraph_core::temporal::Timestamp(tx_ts), &access)
        };
        if req.as_of_valid_time != 0 {
            snapshot = snapshot.with_vt_as_of(req.as_of_valid_time);
        }
        let snapshot = self.inferred_scope(snapshot, req.exclude_inferred);

        let rules: Vec<_> = req
            .rules
            .iter()
            .map(|r| convert::rule_from_proto(r, &|iri| self.store.graph_id(iri)))
            .collect::<Result<_, _>>()?;

        let t0 = Instant::now();
        let results = if rules.is_empty() {
            execute_query_full(
                &query,
                &snapshot,
                self.make_deadline(),
                Some(&self.edge_registry),
            )
            .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?
        } else {
            let derived: DerivedFacts =
                execute_recursive(&[], &rules, &snapshot, self.make_deadline())
                    .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?;
            execute_query_hybrid_full(&query, &snapshot, &derived, self.make_deadline())
                .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?
        };
        self.check_slow_query(
            "QueryStream",
            t0.elapsed(),
            &format!("patterns={pattern_count} rules={}", rules.len()),
        );

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
        let meta_uid = meta_user_id(request.metadata());
        let req = request.into_inner();
        let user_id = resolve_user_id(&req.user_id, &meta_uid);
        let access = self.caller_access(&user_id);

        if req.cypher.is_empty() {
            return Err(Status::invalid_argument(
                "cypher query string must not be empty",
            ));
        }

        let params = Self::deserialize_params(&req.params)?;

        let compiled = self.compiled_cypher(&req.cypher)?;

        let compiled = compiled
            .substitute_params(&params)
            .map_err(|e| Status::invalid_argument(format!("parameter error: {e}")))?;
        let compiled = self.cypher_dataset(compiled, &req.graphs);

        let tx_ts = if req.as_of_tx_time != 0 {
            req.as_of_tx_time
        } else {
            0
        };
        let mut snapshot = if tx_ts == 0 {
            self.snapshot_for(self.store.begin().read_ts, &access)
        } else {
            self.snapshot_for(polargraph_core::temporal::Timestamp(tx_ts), &access)
        };
        if req.as_of_valid_time != 0 {
            snapshot = snapshot.with_vt_as_of(req.as_of_valid_time);
        }
        let snapshot = self.inferred_scope(snapshot, req.exclude_inferred);
        let snapshot = self.cypher_snapshot_scope(snapshot, &req.graphs);

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
            let ef = vn
                .ef
                .map(|e| e as usize)
                .filter(|&e| e > 0)
                .unwrap_or(if req.ef > 0 {
                    req.ef as usize
                } else {
                    self.default_vector_ef as usize
                });
            let seed_var = vn.seed_variable.clone();
            let ann_hits = self
                .store
                .search_vector_ef(space, &req.vector, ef, ef)
                .into_iter()
                .filter(|(id, _)| self.node_visible(id, &access))
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
                execute_query_seeded(
                    &compiled.query,
                    &snapshot,
                    initial,
                    deadline,
                    Some(&self.edge_registry),
                )
                .map_err(|e| query_err_to_status(e, self.query_timeout_ms))?
            }
        } else if compiled.rules.is_empty() {
            execute_query(
                &compiled.query,
                &snapshot,
                deadline,
                Some(&self.edge_registry),
            )
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
            &format!(
                "patterns={} rules={}",
                compiled.query.patterns.len(),
                compiled.rules.len()
            ),
        );

        let filtered = polargraph_query::cypher::apply_value_filters(
            raw_results,
            &compiled.value_filters,
            &snapshot,
        )
        .map_err(storage_err_to_status)?;

        let filtered = polargraph_query::cypher::apply_text_filters(
            filtered,
            &compiled.text_filters,
            &snapshot,
        )
        .map_err(storage_err_to_status)?;

        // Apply edge annotation filters (WHERE r.prop <op> val).
        let filtered = polargraph_query::cypher::apply_edge_annotation_filters(
            filtered,
            &compiled.edge_annotation_filters,
            &snapshot,
        )
        .map_err(storage_err_to_status)?;

        let limited: Vec<_> = match compiled.limit {
            Some(n) => filtered.into_iter().take(n).collect(),
            None => filtered,
        };

        // Project to RETURN variables; also inject ID projections as NodeId bytes.
        let projected: Vec<_> = if compiled.return_vars.is_empty()
            && compiled.edge_id_projections.is_empty()
            && compiled.node_id_projections.is_empty()
        {
            limited
        } else {
            limited
                .into_iter()
                .map(|b| {
                    let mut out: polargraph_query::datalog::Bindings = compiled
                        .return_vars
                        .iter()
                        .filter_map(|v| b.get(v).map(|&id| (v.clone(), id)))
                        .collect();
                    // Edge ID projections: reuse the NodeId-aliased EdgeId bytes already in bindings.
                    for proj in &compiled.edge_id_projections {
                        if let Some(&id) = b.get(proj.rel_var.as_str()) {
                            out.insert(proj.output_key.clone(), id);
                        }
                    }
                    // Node ID projections: NodeId bytes are stored directly in bindings.
                    for proj in &compiled.node_id_projections {
                        if let Some(&id) = b.get(proj.node_var.as_str()) {
                            out.insert(proj.output_key.clone(), id);
                        }
                    }
                    out
                })
                .collect()
        };

        let (tx, rx) = mpsc::channel::<Result<QueryStreamChunk, Status>>(4);
        tokio::spawn(async move {
            let projected = projected
                .into_iter()
                .map(|nodes| Solution {
                    nodes,
                    ..Default::default()
                })
                .collect();
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
                    storage_mode: if is_mmap {
                        "mmap".to_owned()
                    } else {
                        "memory".to_owned()
                    },
                }
            })
            .collect();

        let predicate_count = self.store.predicate_count();

        Ok(Response::new(ShowIndexesResponse {
            column_families,
            vector_spaces,
            predicate_count,
        }))
    }

    async fn show_stats(
        &self,
        _req: Request<ShowStatsRequest>,
    ) -> Result<Response<ShowStatsResponse>, Status> {
        let mode = if self.store.is_replica() {
            "replica"
        } else {
            "primary"
        };
        let legacy = self.legacy_status();
        Ok(Response::new(ShowStatsResponse {
            live_sst_files: self.store.db_live_sst_files(),
            total_sst_size_bytes: self.store.db_total_sst_size_bytes(),
            memtable_size_bytes: self.store.db_memtable_size_bytes(),
            mvcc_oracle_ts: self.store.oracle_ts() as u64,
            predicate_intern_count: self.store.predicate_count(),
            open_transaction_count: self.tx_map.len() as u32,
            mode: mode.to_owned(),
            query_cache_hits: self.query_cache_hits.load(Ordering::Relaxed),
            query_cache_misses: self.query_cache_misses.load(Ordering::Relaxed),
            query_cache_size: self.query_plan_cache.len() as u32,
            legacy_conversion_pending: legacy.pending(),
            legacy_bare_predicates: legacy.bare_predicates.len() as u32,
            legacy_type_labels: legacy.type_labels,
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
        let inner = Arc::try_unwrap(open)
            .map_err(|_| Status::internal("transaction is still in use by a concurrent RPC"))?;
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

    // ── RDF-star edge annotations ─────────────────────────────────────────────

    async fn get_edge_annotations(
        &self,
        request: Request<GetEdgeAnnotationsRequest>,
    ) -> Result<Response<GetEdgeAnnotationsResponse>, Status> {
        let user_id = meta_user_id(request.metadata());
        let req = request.into_inner();
        let access = self.caller_access(&user_id);
        let edge_bytes: [u8; 16] = req
            .edge_id
            .as_slice()
            .try_into()
            .map_err(|_| Status::invalid_argument("edge_id must be exactly 16 bytes"))?;
        let edge = polargraph_core::id::EdgeId(uuid::Uuid::from_bytes(edge_bytes));

        let annotations = self
            .snapshot_for(self.store.begin().read_ts, &access)
            .scan_edge_annotations(edge)
            .map_err(storage_err_to_status)?;

        let proto_annotations = annotations
            .iter()
            .map(|ann| convert::edge_annotation_to_proto(ann, req.edge_id.clone()))
            .collect();

        Ok(Response::new(GetEdgeAnnotationsResponse {
            annotations: proto_annotations,
        }))
    }

    async fn get_edge_ids_by_triple(
        &self,
        request: Request<GetEdgeIdsByTripleRequest>,
    ) -> Result<Response<GetEdgeIdsByTripleResponse>, Status> {
        let user_id = meta_user_id(request.metadata());
        let req = request.into_inner();
        let access = self.caller_access(&user_id);

        let subj_bytes: [u8; 16] = req
            .subject_id
            .as_slice()
            .try_into()
            .map_err(|_| Status::invalid_argument("subject_id must be exactly 16 bytes"))?;
        let obj_bytes: [u8; 16] = req
            .object_id
            .as_slice()
            .try_into()
            .map_err(|_| Status::invalid_argument("object_id must be exactly 16 bytes"))?;

        let subject = NodeId(uuid::Uuid::from_bytes(subj_bytes));
        let object = NodeId(uuid::Uuid::from_bytes(obj_bytes));
        let predicate = &req.predicate;

        let snapshot_ts = self.store.begin().read_ts;

        // Look up the predicate ID. If it doesn't exist, the triple can't exist.
        let pred_id = match self.store.lookup_predicate(predicate) {
            Some(id) => id,
            None => {
                return Ok(Response::new(GetEdgeIdsByTripleResponse {
                    edge_ids: vec![],
                }))
            }
        };

        // Scan the SPO CF with prefix [subject:16][pred_id:4][object:16] to find
        // all MVCC versions. The CF value bytes contain the edge_id.
        let edge_ids = self
            .store
            .scan_spo_for_edge_ids_in(
                subject,
                pred_id,
                object,
                snapshot_ts,
                access.as_ref().map(|a| a.readable()).as_deref(),
            )
            .map_err(storage_err_to_status)?
            .into_iter()
            .map(|eid: EdgeId| eid.as_bytes().to_vec())
            .collect();

        Ok(Response::new(GetEdgeIdsByTripleResponse { edge_ids }))
    }

    // ── API key management ────────────────────────────────────────────────────

    async fn add_api_key(
        &self,
        request: Request<AddApiKeyRequest>,
    ) -> Result<Response<AddApiKeyResponse>, Status> {
        self.check_not_replica()?;
        let key = request.into_inner().key;
        if key.is_empty() {
            return Err(Status::invalid_argument("key must not be empty"));
        }
        let store = self.key_store.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "auth is disabled; start the server with --api-key to enable key management",
            )
        })?;
        // Reject management when auth is currently disabled (empty key list).
        // An unauthenticated caller must not be able to silently enable auth.
        {
            let keys = store.read().unwrap();
            if keys.is_empty() {
                return Err(Status::failed_precondition(
                    "auth is disabled; start the server with --api-key to enable key management",
                ));
            }
        }
        let mut keys = store.write().unwrap();
        keys.push(key.clone());
        let total_keys = keys.len() as u32;
        drop(keys);
        info!(total_keys, "api key added");
        Ok(Response::new(AddApiKeyResponse { total_keys }))
    }

    async fn revoke_api_key(
        &self,
        request: Request<RevokeApiKeyRequest>,
    ) -> Result<Response<RevokeApiKeyResponse>, Status> {
        self.check_not_replica()?;
        let key = request.into_inner().key;
        let store = self.key_store.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "auth is disabled; start the server with --api-key to enable key management",
            )
        })?;
        {
            let keys = store.read().unwrap();
            if keys.is_empty() {
                return Err(Status::failed_precondition(
                    "auth is disabled; start the server with --api-key to enable key management",
                ));
            }
        }
        let mut keys = store.write().unwrap();
        let before = keys.len();
        keys.retain(|k| k != &key);
        let found = keys.len() < before;
        let total_keys = keys.len() as u32;
        drop(keys);
        info!(found, total_keys, "api key revoked");
        Ok(Response::new(RevokeApiKeyResponse { found, total_keys }))
    }

    async fn list_api_keys(
        &self,
        _request: Request<ListApiKeysRequest>,
    ) -> Result<Response<ListApiKeysResponse>, Status> {
        self.check_not_replica()?;
        let Some(store) = &self.key_store else {
            return Ok(Response::new(ListApiKeysResponse {
                key_prefixes: vec![],
                total_keys: 0,
            }));
        };
        let keys = store.read().unwrap();
        let total_keys = keys.len() as u32;
        let key_prefixes = keys
            .iter()
            .map(|k| {
                let prefix: String = k.chars().take(4).collect();
                format!("{prefix}****")
            })
            .collect();
        drop(keys);
        Ok(Response::new(ListApiKeysResponse {
            key_prefixes,
            total_keys,
        }))
    }

    // ── Graph-native access control ───────────────────────────────────────────

    /// Grant a group access to a node (direct) or all nodes of a type.
    async fn grant_access(
        &self,
        request: Request<GrantAccessRequest>,
    ) -> Result<Response<GrantAccessResponse>, Status> {
        self.check_not_replica()?;
        require_service(&self.caller_access(&meta_user_id(request.metadata())))?;
        let req = request.into_inner();

        let group_id = NodeId(
            uuid::Uuid::from_slice(&req.group_id)
                .map_err(|_| Status::invalid_argument("group_id must be a 16-byte UUID"))?,
        );

        let triple = match req.target {
            Some(GrantTarget::NodeId(bytes)) => {
                let node_id = NodeId(
                    uuid::Uuid::from_slice(&bytes)
                        .map_err(|_| Status::invalid_argument("node_id must be a 16-byte UUID"))?,
                );
                Triple::Relation {
                    subject: group_id,
                    predicate: polargraph_core::triple::Predicate(
                        BUILTIN_HAS_ACCESS_PRED.to_string(),
                    ),
                    object: node_id,
                    edge_id: EdgeId::new(),
                    temporal: polargraph_core::temporal::BiTemporalRange {
                        vt_start: polargraph_core::temporal::Timestamp(0),
                        vt_end: polargraph_core::temporal::Timestamp::END_OF_TIME,
                        tt: polargraph_core::temporal::Timestamp::now(),
                    },
                }
            }
            Some(GrantTarget::TypeName(type_name)) => Triple::Property {
                subject: group_id,
                predicate: polargraph_core::triple::Predicate(
                    BUILTIN_HAS_ACCESS_TYPE_PRED.to_string(),
                ),
                value: Value::Text(type_name),
                temporal: polargraph_core::temporal::BiTemporalRange {
                    vt_start: polargraph_core::temporal::Timestamp(0),
                    vt_end: polargraph_core::temporal::Timestamp::END_OF_TIME,
                    tt: polargraph_core::temporal::Timestamp::now(),
                },
            },
            None => {
                return Err(Status::invalid_argument(
                    "target (node_id or type_name) must be set",
                ))
            }
        };

        let all_triples = vec![triple];
        let mut tx = self.store.begin();
        for t in &all_triples {
            tx.insert(t.clone());
        }
        tx.commit().map_err(storage_err_to_status)?;

        self.update_access_cache_if_needed(&all_triples);
        info!("GrantAccess: group={:?}", group_id);
        Ok(Response::new(GrantAccessResponse {}))
    }

    /// Revoke a group's access grant by closing its valid time.
    async fn revoke_access(
        &self,
        request: Request<RevokeAccessRequest>,
    ) -> Result<Response<RevokeAccessResponse>, Status> {
        self.check_not_replica()?;
        require_service(&self.caller_access(&meta_user_id(request.metadata())))?;
        let req = request.into_inner();

        let group_id = NodeId(
            uuid::Uuid::from_slice(&req.group_id)
                .map_err(|_| Status::invalid_argument("group_id must be a 16-byte UUID"))?,
        );

        let now_ts = polargraph_core::temporal::Timestamp::now();

        match req.target {
            Some(RevokeTarget::NodeId(bytes)) => {
                let node_id = NodeId(
                    uuid::Uuid::from_slice(&bytes)
                        .map_err(|_| Status::invalid_argument("node_id must be a 16-byte UUID"))?,
                );
                // Find existing HAS_ACCESS triple and close its valid time.
                let existing = self
                    .store
                    .scan_by_subject_predicate(&group_id, BUILTIN_HAS_ACCESS_PRED)
                    .map_err(storage_err_to_status)?;
                let mut tx = self.store.begin();
                for t in existing {
                    if let Triple::Relation {
                        object, temporal, ..
                    } = &t
                    {
                        if *object == node_id
                            && temporal.vt_end == polargraph_core::temporal::Timestamp::END_OF_TIME
                        {
                            // Insert a superseding triple that closes valid time.
                            tx.insert(Triple::Relation {
                                subject: group_id,
                                predicate: polargraph_core::triple::Predicate(
                                    BUILTIN_HAS_ACCESS_PRED.to_string(),
                                ),
                                object: node_id,
                                edge_id: EdgeId::new(),
                                temporal: polargraph_core::temporal::BiTemporalRange {
                                    vt_start: temporal.vt_start,
                                    vt_end: now_ts,
                                    tt: polargraph_core::temporal::Timestamp::now(),
                                },
                            });
                        }
                    }
                }
                tx.commit().map_err(storage_err_to_status)?;
            }
            Some(RevokeTarget::TypeName(type_name)) => {
                let existing = self
                    .store
                    .scan_by_subject_predicate(&group_id, BUILTIN_HAS_ACCESS_TYPE_PRED)
                    .map_err(storage_err_to_status)?;
                let mut tx = self.store.begin();
                for t in existing {
                    if let Triple::Property {
                        value: Value::Text(ref tn),
                        temporal,
                        ..
                    } = t
                    {
                        if tn == &type_name
                            && temporal.vt_end == polargraph_core::temporal::Timestamp::END_OF_TIME
                        {
                            tx.insert(Triple::Property {
                                subject: group_id,
                                predicate: polargraph_core::triple::Predicate(
                                    BUILTIN_HAS_ACCESS_TYPE_PRED.to_string(),
                                ),
                                value: Value::Text(type_name.clone()),
                                temporal: polargraph_core::temporal::BiTemporalRange {
                                    vt_start: temporal.vt_start,
                                    vt_end: now_ts,
                                    tt: polargraph_core::temporal::Timestamp::now(),
                                },
                            });
                        }
                    }
                }
                tx.commit().map_err(storage_err_to_status)?;
            }
            None => {
                return Err(Status::invalid_argument(
                    "target (node_id or type_name) must be set",
                ))
            }
        }

        // Rebuild access cache since an access triple changed.
        match Self::build_access_cache(&self.store, &self.type_cache) {
            Ok(new_cache) => {
                *self.access_cache.write().unwrap() = new_cache;
            }
            Err(e) => {
                warn!("failed to rebuild access cache after revoke: {e}");
            }
        }
        info!("RevokeAccess: group={:?}", group_id);
        Ok(Response::new(RevokeAccessResponse {}))
    }

    /// Add a user to a group by writing a MEMBER_OF triple.
    async fn add_user_to_group(
        &self,
        request: Request<AddUserToGroupRequest>,
    ) -> Result<Response<AddUserToGroupResponse>, Status> {
        self.check_not_replica()?;
        require_service(&self.caller_access(&meta_user_id(request.metadata())))?;
        let req = request.into_inner();

        let user_id = NodeId(
            uuid::Uuid::from_slice(&req.user_id)
                .map_err(|_| Status::invalid_argument("user_id must be a 16-byte UUID"))?,
        );
        let group_id = NodeId(
            uuid::Uuid::from_slice(&req.group_id)
                .map_err(|_| Status::invalid_argument("group_id must be a 16-byte UUID"))?,
        );

        let triple = Triple::Relation {
            subject: user_id,
            predicate: polargraph_core::triple::Predicate(BUILTIN_MEMBER_OF_PRED.to_string()),
            object: group_id,
            edge_id: EdgeId::new(),
            temporal: polargraph_core::temporal::BiTemporalRange {
                vt_start: polargraph_core::temporal::Timestamp(0),
                vt_end: polargraph_core::temporal::Timestamp::END_OF_TIME,
                tt: polargraph_core::temporal::Timestamp::now(),
            },
        };

        let all_triples = vec![triple];
        let mut tx = self.store.begin();
        for t in &all_triples {
            tx.insert(t.clone());
        }
        tx.commit().map_err(storage_err_to_status)?;

        self.update_access_cache_if_needed(&all_triples);
        info!("AddUserToGroup: user={:?} group={:?}", user_id, group_id);
        Ok(Response::new(AddUserToGroupResponse {}))
    }

    /// Return the accessible nodes and type grants for a user.
    async fn get_user_access(
        &self,
        request: Request<GetUserAccessRequest>,
    ) -> Result<Response<GetUserAccessResponse>, Status> {
        let req = request.into_inner();

        let user_id = NodeId(
            uuid::Uuid::from_slice(&req.user_id)
                .map_err(|_| Status::invalid_argument("user_id must be a 16-byte UUID"))?,
        );
        let user_key = user_id.to_string();

        // Get expanded node set from the cache.
        let node_ids: Vec<Vec<u8>> = {
            let cache = self.access_cache.read().unwrap();
            cache
                .get(&user_key)
                .map(|s| s.iter().map(|id| id.as_bytes().to_vec()).collect())
                .unwrap_or_default()
        };

        // Compute type grants by live scan (group memberships × HAS_ACCESS_TYPE).
        let mut type_grants: Vec<String> = Vec::new();
        let member_of = self
            .store
            .scan_by_subject_predicate(&user_id, BUILTIN_MEMBER_OF_PRED)
            .map_err(storage_err_to_status)?;
        for t in &member_of {
            if let Triple::Relation {
                object: group_id, ..
            } = t
            {
                let type_triples = self
                    .store
                    .scan_by_subject_predicate(group_id, BUILTIN_HAS_ACCESS_TYPE_PRED)
                    .map_err(storage_err_to_status)?;
                for tt in type_triples {
                    if let Triple::Property {
                        value: Value::Text(type_name),
                        ..
                    } = tt
                    {
                        if !type_grants.contains(&type_name) {
                            type_grants.push(type_name);
                        }
                    }
                }
            }
        }

        Ok(Response::new(GetUserAccessResponse {
            node_ids,
            type_grants,
        }))
    }

    // ── Property history ──────────────────────────────────────────────────────

    async fn get_property_history(
        &self,
        request: Request<GetPropertyHistoryRequest>,
    ) -> Result<Response<GetPropertyHistoryResponse>, Status> {
        let user_id = meta_user_id(request.metadata());
        let req = request.into_inner();
        let access = self.caller_access(&user_id);
        let subject_bytes: [u8; 16] = req
            .subject_id
            .as_slice()
            .try_into()
            .map_err(|_| Status::invalid_argument("subject_id must be exactly 16 bytes"))?;
        let subject = NodeId(uuid::Uuid::from_bytes(subject_bytes));

        let versions = self
            .store
            .scan_property_history_in(
                subject,
                &req.predicate,
                req.limit,
                access.as_ref().map(|a| a.readable()).as_deref(),
            )
            .map_err(storage_err_to_status)?;

        let proto_versions = versions
            .into_iter()
            .map(|(value, tt)| {
                let value_json =
                    serde_json::to_string(&value).unwrap_or_else(|_| "null".to_owned());
                PropertyVersion {
                    value_json,
                    transaction_time: tt,
                }
            })
            .collect();

        Ok(Response::new(GetPropertyHistoryResponse {
            versions: proto_versions,
        }))
    }

    async fn delete_triples(
        &self,
        request: Request<DeleteTriplesRequest>,
    ) -> Result<Response<DeleteTriplesResponse>, Status> {
        self.check_not_replica()?;
        let meta_uid = meta_user_id(request.metadata());
        let req = request.into_inner();
        let author = resolve_user_id(&req.user_id, &meta_uid);
        let access = self.caller_access(&author);
        reject_user_acl_writes(&access, [req.predicate.as_str()])?;

        let vt_end_ts = if req.vt_end != 0 {
            polargraph_core::temporal::Timestamp(req.vt_end)
        } else {
            polargraph_core::temporal::Timestamp::now()
        };
        let pred_filter: Option<String> = if req.predicate.is_empty() {
            None
        } else {
            Some(req.predicate.clone())
        };
        let object_filter: Option<NodeId> = if req.object_id.is_empty() {
            None
        } else {
            Some(NodeId(uuid::Uuid::from_slice(&req.object_id).map_err(
                |_| Status::invalid_argument("object_id must be a 16-byte UUID"),
            )?))
        };
        let value_filter: Option<Value> = req
            .value
            .as_ref()
            .map(crate::convert::value_from_proto)
            .transpose()?;
        if object_filter.is_some() && value_filter.is_some() {
            return Err(Status::invalid_argument(
                "object_id and value are mutually exclusive",
            ));
        }
        let mut scope = self.delete_scope(req.graph.as_ref())?;
        // A user only closes triples in graphs it can write.
        if let Some(a) = &access {
            let writable = a.graphs_at_least(GraphAccessLevel::Write);
            scope = match scope {
                polargraph_storage::GraphScope::One(g) => {
                    require_level(&access, g, GraphAccessLevel::Write, &g.to_string())?;
                    polargraph_storage::GraphScope::One(g)
                }
                other => polargraph_storage::GraphScope::set(
                    writable
                        .iter()
                        .map(polargraph_core::id::GraphId)
                        .filter(|g| other.admits(*g))
                        .collect(),
                ),
            };
        }
        let snapshot = self.store.snapshot(self.store.begin().read_ts);
        let mut deleted_count: u64 = 0;

        for id_bytes in &req.subject_ids {
            let subject = NodeId(
                uuid::Uuid::from_slice(id_bytes)
                    .map_err(|_| Status::invalid_argument("subject_ids must be 16-byte UUIDs"))?,
            );

            // Every live quad of the subject in scope, each closed in its own
            // graph.
            let quads = snapshot
                .scan_scoped(Some(&subject), pred_filter.as_deref(), None, &scope)
                .map_err(storage_err_to_status)?;

            let mut tx = self.store.begin();
            tx.set_author(author.clone());
            for (g, triple) in quads {
                let selected = match &triple {
                    Triple::Relation { object, .. } => {
                        value_filter.is_none() && object_filter.map_or(true, |o| o == *object)
                    }
                    Triple::Property { value, .. } => {
                        object_filter.is_none()
                            && value_filter.as_ref().map_or(true, |v| v == value)
                    }
                    _ => false,
                };
                if !selected {
                    continue;
                }
                match &triple {
                    Triple::Relation {
                        subject: s,
                        predicate: p,
                        object: o,
                        edge_id,
                        temporal,
                    } => {
                        if temporal.vt_end == polargraph_core::temporal::Timestamp::END_OF_TIME {
                            tx.insert_in(
                                Triple::Relation {
                                    subject: *s,
                                    predicate: p.clone(),
                                    object: *o,
                                    edge_id: *edge_id,
                                    temporal: polargraph_core::temporal::BiTemporalRange {
                                        vt_start: temporal.vt_start,
                                        vt_end: vt_end_ts,
                                        tt: polargraph_core::temporal::Timestamp::now(),
                                    },
                                },
                                g,
                                polargraph_storage::WriteMode::Add,
                            );
                            deleted_count += 1;
                        }
                    }
                    Triple::Property {
                        subject: s,
                        predicate: p,
                        value: v,
                        temporal,
                    } if temporal.vt_end == polargraph_core::temporal::Timestamp::END_OF_TIME => {
                        tx.insert_in(
                            Triple::Property {
                                subject: *s,
                                predicate: p.clone(),
                                value: v.clone(),
                                temporal: polargraph_core::temporal::BiTemporalRange {
                                    vt_start: temporal.vt_start,
                                    vt_end: vt_end_ts,
                                    tt: polargraph_core::temporal::Timestamp::now(),
                                },
                            },
                            g,
                            polargraph_storage::WriteMode::Add,
                        );
                        deleted_count += 1;
                    }
                    // EdgeProperty / EdgeRelation — not soft-deleted here.
                    _ => {}
                }
            }
            tx.commit().map_err(storage_err_to_status)?;
        }

        info!("DeleteTriples: deleted_count={}", deleted_count);
        Ok(Response::new(DeleteTriplesResponse { deleted_count }))
    }

    /// Recompute OWL 2 RL inference and bring the inferred graphs in line
    /// (assert new facts, close facts that no longer follow).
    async fn run_materialization(
        &self,
        _request: Request<RunMaterializationRequest>,
    ) -> Result<Response<RunMaterializationResponse>, Status> {
        self.check_not_replica()?;
        let stats = tokio::task::spawn_blocking({
            let store = self.store.clone();
            move || owl_rl::materialize(&store, true)
        })
        .await
        .map_err(|e| Status::internal(format!("materialize task panicked: {e}")))?
        .map_err(storage_err_to_status)?;
        self.after_inference(&stats);

        Ok(Response::new(RunMaterializationResponse {
            rules_fired: stats.asserted,
            derived_triples: stats.derived_triples,
            iterations: u32::from(stats.asserted + stats.closed > 0),
            asserted: stats.asserted,
            closed: stats.closed,
        }))
    }

    async fn get_inference_settings(
        &self,
        _request: Request<crate::proto::GetInferenceSettingsRequest>,
    ) -> Result<Response<crate::proto::InferenceSettings>, Status> {
        let graphs = owl_rl::schema_graphs(&self.store).map_err(storage_err_to_status)?;
        Ok(Response::new(crate::proto::InferenceSettings {
            all_graphs: graphs.is_none(),
            schema_graphs: graphs.unwrap_or_default(),
        }))
    }

    async fn set_inference_settings(
        &self,
        request: Request<crate::proto::SetInferenceSettingsRequest>,
    ) -> Result<Response<RunMaterializationResponse>, Status> {
        self.check_not_replica()?;
        let meta_uid = meta_user_id(request.metadata());
        let req = request.into_inner();
        require_service(&self.caller_access(&resolve_user_id(&req.user_id, &meta_uid)))?;
        if req.all_graphs && !req.schema_graphs.is_empty() {
            return Err(Status::invalid_argument(
                "all_graphs and schema_graphs are exclusive",
            ));
        }
        if let Some(iri) = req
            .schema_graphs
            .iter()
            .find(|g| owl_rl::is_inferred_graph_iri(g))
        {
            return Err(Status::invalid_argument(format!(
                "<{iri}> is an inferred graph; schema graphs hold base data"
            )));
        }
        let stats = tokio::task::spawn_blocking({
            let store = self.store.clone();
            move || {
                let graphs = (!req.all_graphs).then_some(req.schema_graphs);
                owl_rl::set_schema_graphs(&store, graphs.as_deref())
            }
        })
        .await
        .map_err(|e| Status::internal(format!("materialize task panicked: {e}")))?
        .map_err(storage_err_to_status)?;
        self.after_inference(&stats);
        Ok(Response::new(RunMaterializationResponse {
            rules_fired: stats.asserted,
            derived_triples: stats.derived_triples,
            iterations: u32::from(stats.asserted + stats.closed > 0),
            asserted: stats.asserted,
            closed: stats.closed,
        }))
    }
}

// ── Streaming helpers ─────────────────────────────────────────────────────────

/// Split `results` into chunks of `STREAM_CHUNK_SIZE` and send each as a
/// `QueryStreamChunk` on `tx`. The last chunk has `done = true`.
async fn send_result_chunks(
    results: Vec<Solution>,
    tx: mpsc::Sender<Result<QueryStreamChunk, Status>>,
) {
    if results.is_empty() {
        let _ = tx
            .send(Ok(QueryStreamChunk {
                results: vec![],
                chunk_index: 0,
                done: true,
            }))
            .await;
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
    Status::failed_precondition("write operations are not supported on a read replica")
}

fn backup_not_configured() -> Status {
    Status::failed_precondition("backup not configured: start polargraphd with --backup-dir <PATH>")
}

fn query_err_to_status(err: QueryError, timeout_ms: u64) -> Status {
    match err {
        QueryError::Timeout => {
            Status::deadline_exceeded(format!("query exceeded timeout of {}ms", timeout_ms))
        }
        QueryError::Storage(se) => storage_err_to_status(se),
    }
}

#[allow(clippy::result_large_err)]
fn metadata_from_proto(meta: &[GraphMetadata]) -> Result<Vec<(String, Value)>, Status> {
    meta.iter()
        .map(|m| {
            if m.predicate.is_empty() {
                return Err(Status::invalid_argument(
                    "metadata predicate must not be empty",
                ));
            }
            let value = m
                .value
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("metadata value is required"))?;
            Ok((m.predicate.clone(), convert::value_from_proto(value)?))
        })
        .collect()
}

/// Proto form of a relation or property quad in graph `graph` (other triple
/// kinds are not exported).
fn exported_quad(t: &Triple, graph: &str) -> Option<ExportedQuad> {
    use crate::proto::exported_quad::Object;
    let (subject, predicate, object) = match t {
        Triple::Relation {
            subject,
            predicate,
            object,
            ..
        } => (
            subject,
            predicate,
            Object::Node(convert::node_id_to_proto(*object)),
        ),
        Triple::Property {
            subject,
            predicate,
            value,
            ..
        } => (
            subject,
            predicate,
            Object::Value(convert::value_to_proto(value)),
        ),
        _ => return None,
    };
    Some(ExportedQuad {
        subject: Some(convert::node_id_to_proto(*subject)),
        predicate: predicate.0.clone(),
        object: Some(object),
        graph: graph.to_string(),
    })
}

fn graph_info(
    store: &TripleStore,
    g: polargraph_core::id::GraphId,
    iri: String,
) -> Result<GraphInfo, StorageError> {
    Ok(GraphInfo {
        iri,
        id: g.0,
        metadata: store
            .graph_metadata(g)?
            .into_iter()
            .map(|(predicate, value)| GraphMetadata {
                predicate,
                value: Some(convert::value_to_proto(&value)),
            })
            .collect(),
    })
}

fn legacy_to_proto(status: &polargraph_storage::LegacyStatus) -> crate::proto::LegacyStatus {
    crate::proto::LegacyStatus {
        conversion_pending: status.pending(),
        bare_predicates: status.bare_predicates.clone(),
        type_labels: status.type_labels,
        pending_merges: status.pending_merges.clone(),
    }
}

/// Vocabulary input errors are the caller's (`INVALID_ARGUMENT`).
fn vocab_err_to_status(err: StorageError) -> Status {
    match err {
        StorageError::Validation(msg) => Status::invalid_argument(msg),
        other => storage_err_to_status(other),
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
        StorageError::Validation(_) => Status::failed_precondition(err.to_string()),
        StorageError::NeedsMigration | StorageError::UnsupportedFormat(_) => {
            Status::failed_precondition(err.to_string())
        }
        StorageError::IriCollision { .. } => {
            warn!("{err}");
            Status::already_exists(err.to_string())
        }
    }
}
