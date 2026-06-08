//! gRPC service implementation.

use crate::{
    convert,
    proto::{
        polar_graph_service_server::PolarGraphService,
        search_vector_filtered_request::Filter,
        vector_seed_query_request::Filter as SeedFilter,
        BatchInsertError, BatchInsertVectorsRequest, BatchInsertVectorsResponse,
        GetEdgeTypeRequest, GetEdgeTypeResponse,
        GetNodeTypeRequest, GetNodeTypeResponse,
        InsertRequest, InsertResponse, InsertVectorRequest, InsertVectorResponse,
        ListEdgeTypesRequest, ListEdgeTypesResponse,
        ListNodeTypesRequest, ListNodeTypesResponse,
        ListPredicatesBetweenRequest, ListPredicatesBetweenResponse,
        QueryRequest, QueryResponse, ReachableRequest, ReachableResponse,
        RegisterEdgeTypeRequest, RegisterEdgeTypeResponse,
        RegisterNodeTypeRequest, RegisterNodeTypeResponse,
        ScoredBinding,
        SearchVectorFilteredRequest, SearchVectorFilteredResponse,
        SearchVectorInSetRequest, SearchVectorInSetResponse,
        SearchVectorRequest, SearchVectorResponse,
        ValidateEdgeRequest, ValidateEdgeResponse,
        ValidateNodeRequest, ValidateNodeResponse,
        VectorSearchResult, VectorSeedQueryRequest, VectorSeedQueryResponse,
    },
};
use polargraph_core::{id::NodeId, schema::StorageMode, triple::Triple, value::Value};
use polargraph_query::datalog::{
    execute_query, execute_query_seeded, reachable_from, reachable_from_hops, Bindings, Query,
};
use polargraph_storage::{EdgeTypeRegistry, NodeTypeRegistry, StorageError, TripleStore};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

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
}

impl PolarGraphServer {
    pub fn new(store: TripleStore) -> Result<Self, StorageError> {
        let registry = NodeTypeRegistry::new(store.clone())?;
        let edge_registry = EdgeTypeRegistry::new(store.clone())?;
        let type_cache = Arc::new(RwLock::new(Self::build_type_cache(&store)?));
        Ok(Self { store, registry, edge_registry, type_cache })
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
    /// Insert one or more triples atomically.
    async fn insert(
        &self,
        request: Request<InsertRequest>,
    ) -> Result<Response<InsertResponse>, Status> {
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

        // Resolve snapshot: 0 or omitted → current read_ts; otherwise use given ts.
        let snapshot = if req.snapshot_ts == 0 {
            self.store.snapshot(self.store.begin().read_ts)
        } else {
            self.store.snapshot(polargraph_core::temporal::Timestamp(req.snapshot_ts))
        };

        debug!("query: {} pattern(s) at ts={}", query.patterns.len(), snapshot.ts.0);

        let results = execute_query(&query, &snapshot).map_err(storage_err_to_status)?;

        let bindings = results.iter().map(convert::binding_to_proto).collect();

        Ok(Response::new(QueryResponse { bindings }))
    }

    /// Insert or update a node's embedding vector in the named HNSW space.
    async fn insert_vector(
        &self,
        request: Request<InsertVectorRequest>,
    ) -> Result<Response<InsertVectorResponse>, Status> {
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
                let allowed: HashSet<NodeId> = if f.max_hops == 0 {
                    reachable_from(from, &f.predicate, &snapshot)
                } else {
                    reachable_from_hops(from, &f.predicate, &snapshot, f.max_hops as usize)
                }
                .map_err(storage_err_to_status)?;

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

        let reachable_set = if req.max_hops == 0 {
            reachable_from(start, &req.predicate, &snapshot)
        } else {
            reachable_from_hops(start, &req.predicate, &snapshot, req.max_hops as usize)
        }
        .map_err(storage_err_to_status)?;

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
                let allowed: HashSet<NodeId> = if f.max_hops == 0 {
                    reachable_from(from, &f.predicate, &snap)
                } else {
                    reachable_from_hops(from, &f.predicate, &snap, f.max_hops as usize)
                }
                .map_err(storage_err_to_status)?;
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

        let results = execute_query_seeded(&query, &snapshot, initial)
            .map_err(storage_err_to_status)?;

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
}

// ── Error mapping ─────────────────────────────────────────────────────────────

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
    }
}
