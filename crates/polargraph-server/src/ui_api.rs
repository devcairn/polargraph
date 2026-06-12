//! REST API and HTML UI for the PolarGraph management web interface.
//!
//! Served on a dedicated port (default 8080, separate from gRPC and metrics).
//! Auth mirrors the gRPC layer: if API keys are configured, all `/api/*`
//! routes require `Authorization: Bearer <key>`. The root `GET /` always
//! serves the HTML regardless of auth state.

use axum::{
    extract::{Query as QueryParams, State},
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use polargraph_core::{id::NodeId, triple::Triple, value::Value};
use serde::{Deserialize, Serialize};
use std::{sync::{atomic::Ordering, Arc}, time::Instant};
use tonic::Request;
use uuid::Uuid;

use crate::{
    auth::{check_bearer_auth, KeyStore},
    proto::{
        self, term::Kind as TermKind, triple::Kind as TripleKind, value::Kind as ValueKind,
        polar_graph_service_server::PolarGraphService,
        CreateBackupRequest, InsertRequest, ListBackupsRequest, ListEdgeTypesRequest,
        ListNodeTypesRequest, PropertyTriple, PurgeOldBackupsRequest, QueryRequest,
        RelationTriple, ReplicaStatusRequest, ShowIndexesRequest, ShowStatsRequest,
        Triple as ProtoTriple, Value as ProtoValue, VarPattern,
    },
    service::PolarGraphServer,
};

static UI_HTML: &str = include_str!("ui.html");

// ── State ─────────────────────────────────────────────────────────────────────

pub struct UiState {
    pub service: PolarGraphServer,
    /// Shared API key store — same `Arc<RwLock<...>>` used by `ApiKeyLayer`.
    pub api_keys: KeyStore,
    pub start_time: Instant,
    pub data_dir: String,
    pub grpc_addr: String,
    /// Set only on replicas — exposes WAL connectivity for the `/health` endpoint.
    pub replica_state: Option<Arc<crate::service::ReplicaState>>,
}

// ── Router ────────────────────────────────────────────────────────────────────

pub fn build_ui_router(state: Arc<UiState>) -> Router {
    Router::new()
        .route("/", get(serve_ui))
        .route("/health", get(health_check))
        .route("/api/status", get(api_status))
        .route("/api/node-types", get(api_node_types))
        .route("/api/edge-types", get(api_edge_types))
        .route("/api/metrics", get(api_metrics))
        .route("/api/query", post(api_query))
        .route("/api/insert", post(api_insert))
        .route("/api/search", get(api_search))
        .route("/api/indexes", get(api_indexes))
        .route("/api/stats", get(api_stats))
        .route("/api/backups", get(api_backups_list))
        .route("/api/backups/create", post(api_backups_create))
        .route("/api/backups/purge", post(api_backups_purge))
        .route("/api/keys", get(api_keys_list))
        .route("/api/keys/add", post(api_keys_add))
        .route("/api/keys/revoke", post(api_keys_revoke))
        .with_state(state)
}

// ── Auth helper ───────────────────────────────────────────────────────────────

fn require_auth(headers: &HeaderMap, keys: &[String]) -> Option<Response> {
    if keys.is_empty() || check_bearer_auth(headers, keys) {
        None
    } else {
        Some(
            (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"error": "unauthorized: include Authorization: Bearer <key>"})),
            )
                .into_response(),
        )
    }
}

/// Auth check that reads from the shared `KeyStore`. The read lock is held only
/// for the duration of the comparison and is dropped before returning.
fn require_auth_ks(headers: &HeaderMap, store: &KeyStore) -> Option<Response> {
    let keys = store.read().unwrap();
    require_auth(headers, keys.as_slice())
}

// ── Handlers ──────────────────────────────────────────────────────────────────

/// Serve the management SPA (no auth required so the UI can load and prompt).
async fn serve_ui() -> Html<&'static str> {
    Html(UI_HTML)
}

// ── GET /health ───────────────────────────────────────────────────────────────

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    mode: &'static str,
    triples: u64,
}

#[derive(Serialize)]
struct DegradedResponse {
    status: &'static str,
}

async fn health_check(State(state): State<Arc<UiState>>) -> Response {
    let is_replica = state.service.store().is_replica();

    // A replica is degraded when its WAL connection to the primary is down.
    if is_replica {
        let connected = state
            .replica_state
            .as_ref()
            .map(|rs| rs.connected.load(Ordering::Relaxed))
            .unwrap_or(false);

        if !connected {
            return (StatusCode::SERVICE_UNAVAILABLE, Json(DegradedResponse { status: "degraded" }))
                .into_response();
        }
    }

    let triples = state.service.store().estimate_triple_count();
    let mode = if is_replica { "replica" } else { "primary" };

    (StatusCode::OK, Json(HealthResponse { status: "ok", mode, triples })).into_response()
}

// ── /api/status ───────────────────────────────────────────────────────────────

#[derive(Serialize)]
struct StatusResponse {
    version: &'static str,
    uptime_secs: u64,
    data_dir: String,
    grpc_addr: String,
    is_replica: bool,
    primary_address: Option<String>,
    last_applied_seq: u64,
    wal_lag_entries: u64,
    auth_enabled: bool,
}

async fn api_status(
    State(state): State<Arc<UiState>>,
    headers: HeaderMap,
) -> Response {
    if let Some(err) = require_auth_ks(&headers, &state.api_keys) {
        return err;
    }

    let replica = state
        .service
        .replica_status(Request::new(ReplicaStatusRequest {}))
        .await
        .map(|r: tonic::Response<_>| r.into_inner())
        .ok();

    let (is_replica, primary_address, last_applied_seq, wal_lag_entries) = match &replica {
        Some(r) => (
            r.is_replica,
            if r.primary_address.is_empty() { None } else { Some(r.primary_address.clone()) },
            r.last_applied_seq,
            r.replication_lag_entries,
        ),
        None => (false, None, 0, 0),
    };

    let uptime_secs = state.start_time.elapsed().as_secs();

    Json(StatusResponse {
        version: env!("CARGO_PKG_VERSION"),
        uptime_secs,
        data_dir: state.data_dir.clone(),
        grpc_addr: state.grpc_addr.clone(),
        is_replica,
        primary_address,
        last_applied_seq,
        wal_lag_entries,
        auth_enabled: !state.api_keys.read().unwrap().is_empty(),
    })
    .into_response()
}

// ── /api/node-types ───────────────────────────────────────────────────────────

#[derive(Serialize)]
struct FieldDefJson {
    name: String,
    kind: String,
    required: bool,
}

#[derive(Serialize)]
struct VectorSpaceJson {
    space_name: String,
    dimensions: u32,
    embedding_model: Option<String>,
    storage_mode: String,
}

#[derive(Serialize)]
struct NodeTypeJson {
    type_name: String,
    fields: Vec<FieldDefJson>,
    vector_space: Option<VectorSpaceJson>,
}

async fn api_node_types(
    State(state): State<Arc<UiState>>,
    headers: HeaderMap,
) -> Response {
    if let Some(err) = require_auth_ks(&headers, &state.api_keys) {
        return err;
    }

    let resp = match state
        .service
        .list_node_types(Request::new(ListNodeTypesRequest {}))
        .await
    {
        Ok(r) => r.into_inner(),
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.message()})),
            )
                .into_response()
        }
    };

    let types: Vec<NodeTypeJson> = resp
        .definitions
        .into_iter()
        .map(|d| NodeTypeJson {
            type_name: d.type_name,
            fields: d
                .fields
                .into_iter()
                .map(|f| FieldDefJson { name: f.field_name, kind: f.kind, required: f.required })
                .collect(),
            vector_space: d.vector_space.map(|vs| VectorSpaceJson {
                space_name: vs.space_name,
                dimensions: vs.dimensions,
                embedding_model: if vs.embedding_model.is_empty() {
                    None
                } else {
                    Some(vs.embedding_model)
                },
                storage_mode: vs.storage_mode,
            }),
        })
        .collect();

    Json(types).into_response()
}

// ── /api/edge-types ───────────────────────────────────────────────────────────

#[derive(Serialize)]
struct EdgeTypeJson {
    predicate: String,
    domain: Option<String>,
    range: Option<String>,
    fields: Vec<FieldDefJson>,
}

async fn api_edge_types(
    State(state): State<Arc<UiState>>,
    headers: HeaderMap,
) -> Response {
    if let Some(err) = require_auth_ks(&headers, &state.api_keys) {
        return err;
    }

    let resp = match state
        .service
        .list_edge_types(Request::new(ListEdgeTypesRequest {}))
        .await
    {
        Ok(r) => r.into_inner(),
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.message()})),
            )
                .into_response()
        }
    };

    let types: Vec<EdgeTypeJson> = resp
        .definitions
        .into_iter()
        .map(|d| EdgeTypeJson {
            predicate: d.predicate,
            domain: if d.domain.is_empty() { None } else { Some(d.domain) },
            range: if d.range.is_empty() { None } else { Some(d.range) },
            fields: d
                .fields
                .into_iter()
                .map(|f| FieldDefJson { name: f.field_name, kind: f.kind, required: f.required })
                .collect(),
        })
        .collect();

    Json(types).into_response()
}

// ── /api/metrics ──────────────────────────────────────────────────────────────

#[derive(Serialize)]
struct MetricsResponse {
    vector_spaces: usize,
    is_replica: bool,
    wal_applied_seq: u64,
    wal_lag_entries: u64,
    uptime_secs: u64,
}

async fn api_metrics(
    State(state): State<Arc<UiState>>,
    headers: HeaderMap,
) -> Response {
    if let Some(err) = require_auth_ks(&headers, &state.api_keys) {
        return err;
    }

    let replica = state
        .service
        .replica_status(Request::new(ReplicaStatusRequest {}))
        .await
        .map(|r: tonic::Response<_>| r.into_inner())
        .ok();

    let (is_replica, wal_applied_seq, wal_lag_entries) = match &replica {
        Some(r) => (r.is_replica, r.last_applied_seq, r.replication_lag_entries),
        None => (false, 0, 0),
    };

    Json(MetricsResponse {
        vector_spaces: state.service.store().hnsw_space_count(),
        is_replica,
        wal_applied_seq,
        wal_lag_entries,
        uptime_secs: state.start_time.elapsed().as_secs(),
    })
    .into_response()
}

// ── /api/query ────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct QueryApiRequest {
    patterns: Vec<PatternJson>,
    #[serde(default)]
    as_of_valid_time: i64,
    #[serde(default)]
    as_of_tx_time: i64,
}

#[derive(Deserialize)]
struct PatternJson {
    s: String,
    p: String,
    o: String,
}

fn parse_term(s: &str) -> Result<proto::Term, String> {
    if s.is_empty() {
        return Ok(proto::Term { kind: None });
    }
    if let Some(var) = s.strip_prefix('?') {
        if var.is_empty() {
            return Err("variable name must not be empty after '?'".into());
        }
        return Ok(proto::Term { kind: Some(TermKind::Var(var.to_owned())) });
    }
    let uuid = Uuid::parse_str(s).map_err(|_| format!("'{s}' is not a valid UUID or ?variable"))?;
    let node_id = proto::NodeId { bytes: uuid.as_bytes().to_vec() };
    Ok(proto::Term { kind: Some(TermKind::Bound(node_id)) })
}

async fn api_query(
    State(state): State<Arc<UiState>>,
    headers: HeaderMap,
    Json(body): Json<QueryApiRequest>,
) -> Response {
    if let Some(err) = require_auth_ks(&headers, &state.api_keys) {
        return err;
    }

    let mut patterns = Vec::with_capacity(body.patterns.len());
    for p in &body.patterns {
        let subject = match parse_term(&p.s) {
            Ok(t) => Some(t),
            Err(e) => {
                return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": e})))
                    .into_response()
            }
        };
        let object = match parse_term(&p.o) {
            Ok(t) => Some(t),
            Err(e) => {
                return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": e})))
                    .into_response()
            }
        };
        patterns.push(VarPattern { subject, predicate: p.p.clone(), object });
    }

    let req = QueryRequest {
        patterns,
        snapshot_ts: 0,
        as_of_valid_time: body.as_of_valid_time,
        as_of_tx_time: body.as_of_tx_time,
        rules: vec![],
        ..Default::default()
    };

    match state.service.query(Request::new(req)).await {
        Ok(resp) => {
            let bindings: Vec<serde_json::Value> = resp
                .into_inner()
                .bindings
                .into_iter()
                .map(|b| {
                    let map: serde_json::Map<String, serde_json::Value> = b
                        .vars
                        .into_iter()
                        .map(|(k, v)| {
                            let uuid = Uuid::from_bytes(
                                v.bytes
                                    .as_slice()
                                    .try_into()
                                    .unwrap_or([0u8; 16]),
                            );
                            (k, serde_json::Value::String(uuid.to_string()))
                        })
                        .collect();
                    serde_json::Value::Object(map)
                })
                .collect();
            Json(serde_json::json!({"bindings": bindings})).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e.message()})),
        )
            .into_response(),
    }
}

// ── /api/insert ───────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct InsertApiRequest {
    subject: String,
    predicate: String,
    object: String,
}

async fn api_insert(
    State(state): State<Arc<UiState>>,
    headers: HeaderMap,
    Json(body): Json<InsertApiRequest>,
) -> Response {
    if let Some(err) = require_auth_ks(&headers, &state.api_keys) {
        return err;
    }

    let subject_uuid = match Uuid::parse_str(&body.subject) {
        Ok(u) => u,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "subject must be a valid UUID"})),
            )
                .into_response()
        }
    };
    let subject_id = proto::NodeId { bytes: subject_uuid.as_bytes().to_vec() };

    // Auto-detect: if object is a UUID → RelationTriple; otherwise → PropertyTriple(text).
    let triple = if let Ok(obj_uuid) = Uuid::parse_str(&body.object) {
        let object_id = proto::NodeId { bytes: obj_uuid.as_bytes().to_vec() };
        ProtoTriple {
            kind: Some(TripleKind::Relation(RelationTriple {
                subject: Some(subject_id),
                predicate: body.predicate.clone(),
                object: Some(object_id),
                vt_start: 0,
                vt_end: 0,
                properties: vec![],
            })),
        }
    } else {
        ProtoTriple {
            kind: Some(TripleKind::Property(PropertyTriple {
                subject: Some(subject_id),
                predicate: body.predicate.clone(),
                value: Some(ProtoValue { kind: Some(ValueKind::TextVal(body.object.clone())) }),
                vt_start: 0,
                vt_end: 0,
            })),
        }
    };

    match state
        .service
        .insert(Request::new(InsertRequest { triples: vec![triple], ..Default::default() }))
        .await
    {
        Ok(resp) => {
            Json(serde_json::json!({"commit_ts": resp.into_inner().commit_ts})).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e.message()})),
        )
            .into_response(),
    }
}

// ── /api/search ───────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct SearchParams {
    q: String,
    #[serde(rename = "type", default)]
    type_filter: String,
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    20
}

#[derive(Serialize)]
struct SearchHit {
    subject: String,
    predicate: String,
    object: String,
}

async fn api_search(
    State(state): State<Arc<UiState>>,
    headers: HeaderMap,
    QueryParams(params): QueryParams<SearchParams>,
) -> Response {
    if let Some(err) = require_auth_ks(&headers, &state.api_keys) {
        return err;
    }

    let q = params.q.to_lowercase();
    let limit = params.limit.min(200);

    // Get the set of subjects of the requested type (if type filter is set).
    let type_subjects: Option<std::collections::HashSet<NodeId>> = if params.type_filter.is_empty() {
        None
    } else {
        let store = state.service.store();
        let triples = match store.scan_by_predicate("__type") {
            Ok(t) => t,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": e.to_string()})),
                )
                    .into_response()
            }
        };
        Some(
            triples
                .into_iter()
                .filter_map(|t| match t {
                    Triple::Property { subject, value: Value::Text(ref tn), .. }
                        if tn == &params.type_filter =>
                    {
                        Some(subject)
                    }
                    _ => None,
                })
                .collect(),
        )
    };

    // Scan all triples and filter.
    let store = state.service.store();
    let all = match store.scan_all() {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response()
        }
    };

    let hits: Vec<SearchHit> = all
        .into_iter()
        .filter(|t| {
            // Apply type filter if set.
            if let Some(ref allowed) = type_subjects {
                let subj = match t {
                    Triple::Relation { subject, .. } | Triple::Property { subject, .. } => subject,
                    Triple::EdgeProperty { .. } | Triple::EdgeRelation { .. } => return false,
                };
                if !allowed.contains(subj) {
                    return false;
                }
            }
            // Match query against object (text values and relation target IDs).
            match t {
                Triple::Property { value: Value::Text(ref s), .. } => {
                    s.to_lowercase().contains(&q)
                }
                Triple::Relation { object, predicate, .. } => {
                    object.to_string().to_lowercase().contains(&q)
                        || predicate.0.to_lowercase().contains(&q)
                }
                Triple::Property { predicate, .. } => predicate.0.to_lowercase().contains(&q),
                Triple::EdgeProperty { .. } | Triple::EdgeRelation { .. } => false,
            }
        })
        .take(limit)
        .map(|t| match t {
            Triple::Relation { subject, predicate, object, .. } => SearchHit {
                subject: subject.to_string(),
                predicate: predicate.0.clone(),
                object: object.to_string(),
            },
            Triple::Property { subject, predicate, value, .. } => SearchHit {
                subject: subject.to_string(),
                predicate: predicate.0.clone(),
                object: format_value(&value),
            },
            Triple::EdgeProperty { edge, predicate, value, .. } => SearchHit {
                subject: edge.0.to_string(),
                predicate: predicate.0.clone(),
                object: format_value(&value),
            },
            Triple::EdgeRelation { edge, predicate, object, .. } => SearchHit {
                subject: edge.0.to_string(),
                predicate: predicate.0.clone(),
                object: object.to_string(),
            },
        })
        .collect();

    Json(hits).into_response()
}

fn format_value(v: &Value) -> String {
    match v {
        Value::Null => "null".into(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Text(s) => s.clone(),
        Value::Blob(b) => format!("<blob {} bytes>", b.len()),
        Value::Vector(fs) => format!("<vec dim={}>", fs.len()),
    }
}

// ── /api/indexes ──────────────────────────────────────────────────────────────

async fn api_indexes(
    State(state): State<Arc<UiState>>,
    headers: HeaderMap,
) -> Response {
    if let Some(err) = require_auth_ks(&headers, &state.api_keys) {
        return err;
    }

    match state.service.show_indexes(Request::new(ShowIndexesRequest {})).await {
        Ok(r) => {
            let resp = r.into_inner();
            let cfs: Vec<serde_json::Value> = resp.column_families.into_iter().map(|cf| {
                serde_json::json!({
                    "name": cf.name,
                    "approx_key_count": cf.approx_key_count,
                    "approx_size_bytes": cf.approx_size_bytes,
                })
            }).collect();
            let spaces: Vec<serde_json::Value> = resp.vector_spaces.into_iter().map(|vs| {
                serde_json::json!({
                    "name": vs.name,
                    "dimensions": vs.dimensions,
                    "node_count": vs.node_count,
                    "storage_mode": vs.storage_mode,
                })
            }).collect();
            Json(serde_json::json!({
                "column_families": cfs,
                "vector_spaces": spaces,
                "predicate_count": resp.predicate_count,
            })).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.message()})),
        ).into_response(),
    }
}

// ── /api/stats ────────────────────────────────────────────────────────────────

async fn api_stats(
    State(state): State<Arc<UiState>>,
    headers: HeaderMap,
) -> Response {
    if let Some(err) = require_auth_ks(&headers, &state.api_keys) {
        return err;
    }

    match state.service.show_stats(Request::new(ShowStatsRequest {})).await {
        Ok(r) => {
            let s = r.into_inner();
            Json(serde_json::json!({
                "live_sst_files": s.live_sst_files,
                "total_sst_size_bytes": s.total_sst_size_bytes,
                "memtable_size_bytes": s.memtable_size_bytes,
                "mvcc_oracle_ts": s.mvcc_oracle_ts,
                "predicate_intern_count": s.predicate_intern_count,
                "open_transaction_count": s.open_transaction_count,
                "mode": s.mode,
            })).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.message()})),
        ).into_response(),
    }
}

// ── /api/backups ──────────────────────────────────────────────────────────────

#[derive(Serialize)]
struct BackupInfoJson {
    id: u32,
    timestamp: i64,
    size_bytes: u64,
}

#[derive(Serialize)]
struct ListBackupsApiResponse {
    backups: Vec<BackupInfoJson>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

async fn api_backups_list(
    State(state): State<Arc<UiState>>,
    headers: HeaderMap,
) -> Response {
    if let Some(err) = require_auth_ks(&headers, &state.api_keys) {
        return err;
    }

    match state.service.list_backups(Request::new(ListBackupsRequest {})).await {
        Ok(r) => {
            let backups = r.into_inner().backups.into_iter().map(|b| BackupInfoJson {
                id: b.backup_id,
                timestamp: b.timestamp,
                size_bytes: b.size_bytes,
            }).collect();
            Json(ListBackupsApiResponse { backups, error: None }).into_response()
        }
        Err(e) if e.code() == tonic::Code::FailedPrecondition => {
            Json(ListBackupsApiResponse {
                backups: vec![],
                error: Some("backup directory not configured".into()),
            }).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.message()})),
        ).into_response(),
    }
}

async fn api_backups_create(
    State(state): State<Arc<UiState>>,
    headers: HeaderMap,
) -> Response {
    if let Some(err) = require_auth_ks(&headers, &state.api_keys) {
        return err;
    }

    match state.service.create_backup(Request::new(CreateBackupRequest {})).await {
        Ok(r) => {
            let inner = r.into_inner();
            Json(serde_json::json!({"backup_id": inner.backup_id})).into_response()
        }
        Err(e) if e.code() == tonic::Code::FailedPrecondition => {
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "backup directory not configured"})),
            ).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.message()})),
        ).into_response(),
    }
}

#[derive(Deserialize)]
struct PurgeApiBody {
    keep: u32,
}

async fn api_backups_purge(
    State(state): State<Arc<UiState>>,
    headers: HeaderMap,
    Json(body): Json<PurgeApiBody>,
) -> Response {
    if let Some(err) = require_auth_ks(&headers, &state.api_keys) {
        return err;
    }

    match state
        .service
        .purge_old_backups(Request::new(PurgeOldBackupsRequest { keep_n: body.keep }))
        .await
    {
        Ok(r) => {
            let inner = r.into_inner();
            Json(serde_json::json!({"purged": inner.deleted_count})).into_response()
        }
        Err(e) if e.code() == tonic::Code::FailedPrecondition => {
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "backup directory not configured"})),
            ).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.message()})),
        ).into_response(),
    }
}

// ── /api/keys ─────────────────────────────────────────────────────────────────

async fn api_keys_list(
    State(state): State<Arc<UiState>>,
    headers: HeaderMap,
) -> Response {
    if let Some(err) = require_auth_ks(&headers, &state.api_keys) {
        return err;
    }

    let keys = state.api_keys.read().unwrap();
    let auth_disabled = keys.is_empty();
    let total = keys.len() as u32;
    let prefixes: Vec<String> = keys
        .iter()
        .map(|k| {
            let prefix: String = k.chars().take(4).collect();
            format!("{prefix}****")
        })
        .collect();
    drop(keys);

    Json(serde_json::json!({
        "keys": prefixes,
        "total": total,
        "auth_disabled": auth_disabled,
    }))
    .into_response()
}

#[derive(Deserialize)]
struct KeyBody {
    key: String,
}

async fn api_keys_add(
    State(state): State<Arc<UiState>>,
    headers: HeaderMap,
    Json(body): Json<KeyBody>,
) -> Response {
    if let Some(err) = require_auth_ks(&headers, &state.api_keys) {
        return err;
    }

    if body.key.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "key must not be empty"})),
        )
            .into_response();
    }

    // Auth disabled when no keys are configured — block management operations.
    {
        let keys = state.api_keys.read().unwrap();
        if keys.is_empty() {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "auth is disabled; restart with --api-key to enable key management"})),
            )
                .into_response();
        }
    }

    let mut keys = state.api_keys.write().unwrap();
    keys.push(body.key);
    let total = keys.len() as u32;
    drop(keys);

    Json(serde_json::json!({"total": total})).into_response()
}

async fn api_keys_revoke(
    State(state): State<Arc<UiState>>,
    headers: HeaderMap,
    Json(body): Json<KeyBody>,
) -> Response {
    if let Some(err) = require_auth_ks(&headers, &state.api_keys) {
        return err;
    }

    // Auth disabled — block management operations.
    {
        let keys = state.api_keys.read().unwrap();
        if keys.is_empty() {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "auth is disabled; restart with --api-key to enable key management"})),
            )
                .into_response();
        }
    }

    let mut keys = state.api_keys.write().unwrap();
    let before = keys.len();
    keys.retain(|k| k != &body.key);
    let found = keys.len() < before;
    let total = keys.len() as u32;
    drop(keys);

    Json(serde_json::json!({"found": found, "total": total})).into_response()
}
