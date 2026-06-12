//! HTTP/JSON REST gateway for PolarGraph.
//!
//! Translates HTTP requests into gRPC calls to a running polargraphd instance
//! and returns JSON responses. Useful for clients that cannot use a gRPC stub.

use axum::{
    extract::{Query as QueryParams, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc};
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tracing::info;
use uuid::Uuid;

mod proto {
    tonic::include_proto!("polargraph.v1");
}

use proto::polar_graph_service_client::PolarGraphServiceClient;

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(name = "polargraph-rest", about = "HTTP/JSON REST gateway for PolarGraph")]
struct Args {
    /// gRPC address of the upstream polargraphd server.
    #[arg(long, env = "POLARGRAPH_UPSTREAM", default_value = "http://localhost:50051")]
    upstream: String,

    /// Address to bind the REST HTTP server.
    #[arg(long, env = "POLARGRAPH_REST_LISTEN", default_value = "0.0.0.0:8000")]
    listen: SocketAddr,

    /// API key forwarded as `Authorization: Bearer <key>` to the upstream gRPC server.
    #[arg(long, env = "POLARGRAPH_REST_API_KEY")]
    api_key: Option<String>,

    /// Path to a PEM CA certificate for verifying the upstream TLS connection.
    #[arg(long, env = "POLARGRAPH_REST_TLS_CA")]
    tls_ca: Option<std::path::PathBuf>,
}

// ── Auth interceptor ──────────────────────────────────────────────────────────

#[derive(Clone)]
struct AuthInterceptor {
    token: Option<String>,
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(
        &mut self,
        mut req: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(t) = &self.token {
            let val: MetadataValue<tonic::metadata::Ascii> =
                format!("Bearer {}", t).parse().map_err(|_| {
                    tonic::Status::internal("could not encode api key as gRPC metadata")
                })?;
            req.metadata_mut().insert("authorization", val);
        }
        Ok(req)
    }
}

/// Attach an `x-polargraph-user-id` metadata header to a gRPC request when
/// `user_id` is non-empty.
fn attach_user_id<T>(mut req: tonic::Request<T>, user_id: &str) -> tonic::Request<T> {
    if !user_id.is_empty() {
        if let Ok(val) = user_id.parse::<MetadataValue<tonic::metadata::Ascii>>() {
            req.metadata_mut().insert("x-polargraph-user-id", val);
        }
    }
    req
}

type GrpcClient = PolarGraphServiceClient<
    tonic::service::interceptor::InterceptedService<Channel, AuthInterceptor>,
>;

// ── App state ─────────────────────────────────────────────────────────────────

struct AppState {
    /// gRPC client; cheap to clone (backed by a pooled Channel).
    client: GrpcClient,
}

// ── JSON request/response types ───────────────────────────────────────────────

/// A Datalog rule in JSON form.
///
/// Example:
/// ```json
/// {
///   "head_predicate": "reachable",
///   "head_subject_var": "x",
///   "head_object_var": "z",
///   "body": ["?x :edge ?y", "?y :edge ?z"]
/// }
/// ```
#[derive(Deserialize)]
struct RuleJson {
    head_predicate:   String,
    head_subject_var: String,
    head_object_var:  String,
    /// Body patterns in the same `"?s :pred ?o"` format as query patterns.
    body: Vec<String>,
}

#[derive(Deserialize)]
struct QueryBody {
    patterns: Vec<String>,
    /// Datalog rules for recursive / derived-predicate queries.
    #[serde(default)]
    rules: Vec<RuleJson>,
    #[serde(default)]
    as_of_valid_time: Option<i64>,
    #[serde(default)]
    as_of_tx_time: Option<i64>,
    /// Open transaction ID to read from (write-your-own-reads overlay).
    #[serde(default)]
    tx_id: Option<String>,
    /// Optional user identity for access-control filtering.
    /// Also forwarded from the `X-User-Id` HTTP header when this field is absent.
    #[serde(default)]
    user_id: Option<String>,
}

/// A scalar property to attach to an edge at insert time.
/// `value` follows the same JSON-encoding as `PropertyTriple.value`:
/// `{"text_val":"hello"}`, `{"int_val":42}`, `{"float_val":3.14}`, `{"bool_val":true}`,
/// `{"blob_val":"<base64>"}`, or `{"vec_val":{"values":[...]}}`.
#[derive(Deserialize)]
struct EdgePropertyJson {
    name:  String,
    value: serde_json::Value,
}

#[derive(Deserialize)]
struct InsertBody {
    subject:   String,
    predicate: String,
    object:    String,
    /// Optional properties stored on the edge (accessible via the returned edge_id).
    #[serde(default)]
    properties: Vec<EdgePropertyJson>,
    /// Open transaction ID to buffer this insert into instead of auto-committing.
    #[serde(default)]
    tx_id: Option<String>,
}

#[derive(Deserialize)]
struct TripleQueryParams {
    subject: Option<String>,
    predicate: Option<String>,
    object: Option<String>,
}

#[derive(Deserialize)]
struct VectorSearchBody {
    vector: Vec<f32>,
    top_k: u32,
    #[serde(default = "default_namespace")]
    namespace: String,
    /// HNSW exploration factor. 0 or absent = use server default.
    #[serde(default)]
    ef: u32,
}

fn default_namespace() -> String {
    "default".to_string()
}

#[derive(Serialize)]
struct TripleJson {
    subject: String,
    predicate: String,
    object: String,
}

// ── Pattern parsing ───────────────────────────────────────────────────────────

/// Parse a 3-token string pattern into a proto `VarPattern`.
///
/// Format: `<subject> <predicate> <object>`
///
/// - `?varname`  → variable term
/// - `_`         → wildcard term (matches anything, not bound)
/// - UUID string → bound NodeId term
/// - `:predicate` or `predicate` → predicate string (leading `:` stripped)
pub fn parse_pattern(s: &str) -> Result<proto::VarPattern, String> {
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() != 3 {
        return Err(format!(
            "pattern must have exactly 3 whitespace-separated tokens (got {}): {:?}",
            parts.len(),
            s
        ));
    }
    let subject = parse_term(parts[0]).map_err(|e| format!("subject in {:?}: {}", s, e))?;
    let predicate = parts[1].trim_start_matches(':').to_string();
    let object = parse_term(parts[2]).map_err(|e| format!("object in {:?}: {}", s, e))?;
    Ok(proto::VarPattern {
        subject: Some(subject),
        predicate,
        object: Some(object),
    })
}

fn parse_term(s: &str) -> Result<proto::Term, String> {
    if let Some(var_name) = s.strip_prefix('?') {
        return Ok(proto::Term {
            kind: Some(proto::term::Kind::Var(var_name.to_string())),
        });
    }
    if s == "_" {
        return Ok(proto::Term { kind: None });
    }
    let uuid =
        Uuid::parse_str(s).map_err(|_| format!("expected ?variable, _, or UUID; got {:?}", s))?;
    Ok(proto::Term {
        kind: Some(proto::term::Kind::Bound(proto::NodeId {
            bytes: uuid.as_bytes().to_vec(),
        })),
    })
}

// ── Rule / EdgeProperty conversion ───────────────────────────────────────────

fn rule_to_proto(rule: &RuleJson) -> Result<proto::DatalogRule, String> {
    let body = rule
        .body
        .iter()
        .map(|p| parse_pattern(p))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(proto::DatalogRule {
        head_predicate:   rule.head_predicate.clone(),
        head_subject_var: rule.head_subject_var.clone(),
        head_object_var:  rule.head_object_var.clone(),
        body,
    })
}

fn edge_property_to_proto(ep: &EdgePropertyJson) -> Result<proto::EdgeProperty, String> {
    // Accept any JSON value and map it to the proto Value encoding.
    let kind = if let Some(v) = ep.value.get("bool_val").and_then(|v| v.as_bool()) {
        proto::value::Kind::BoolVal(v)
    } else if let Some(v) = ep.value.get("int_val").and_then(|v| v.as_i64()) {
        proto::value::Kind::IntVal(v)
    } else if let Some(v) = ep.value.get("float_val").and_then(|v| v.as_f64()) {
        proto::value::Kind::FloatVal(v)
    } else if let Some(v) = ep.value.get("text_val").and_then(|v| v.as_str()) {
        proto::value::Kind::TextVal(v.to_string())
    } else if let Some(arr) = ep.value.get("blob_val").and_then(|v| v.as_array()) {
        // blob_val is a JSON array of integers 0–255.
        let bytes = arr
            .iter()
            .map(|b| b.as_u64().and_then(|n| u8::try_from(n).ok())
                .ok_or_else(|| format!("blob_val elements must be integers 0–255 for property {:?}", ep.name)))
            .collect::<Result<Vec<_>, _>>()?;
        proto::value::Kind::BlobVal(bytes)
    } else if let Some(arr) = ep.value.get("vec_val")
        .and_then(|v| v.get("values"))
        .and_then(|v| v.as_array())
    {
        let values = arr
            .iter()
            .map(|f| f.as_f64().map(|x| x as f32).ok_or_else(|| "vec_val elements must be numbers".to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        proto::value::Kind::VecVal(proto::FloatArray { values })
    } else if ep.value.get("null_val").is_some() || ep.value.is_null() {
        proto::value::Kind::NullVal(true)
    } else {
        return Err(format!(
            "unrecognised value encoding for edge property {:?}: use {{\"text_val\":\"...\"}}, {{\"int_val\":N}}, etc.",
            ep.name
        ));
    };
    Ok(proto::EdgeProperty {
        name:  ep.name.clone(),
        value: Some(proto::Value { kind: Some(kind) }),
    })
}

// ── gRPC status → HTTP status ─────────────────────────────────────────────────

pub fn grpc_to_http_status(code: tonic::Code) -> StatusCode {
    use tonic::Code;
    match code {
        Code::NotFound => StatusCode::NOT_FOUND,
        Code::Unauthenticated => StatusCode::UNAUTHORIZED,
        Code::PermissionDenied => StatusCode::FORBIDDEN,
        Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        Code::DeadlineExceeded => StatusCode::REQUEST_TIMEOUT,
        Code::InvalidArgument => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn grpc_error(status: tonic::Status) -> Response {
    let http = grpc_to_http_status(status.code());
    (http, Json(serde_json::json!({ "error": status.message() }))).into_response()
}

// ── NodeId helpers ────────────────────────────────────────────────────────────

fn node_id_to_uuid_string(nid: &proto::NodeId) -> String {
    if nid.bytes.len() == 16 {
        let arr: [u8; 16] = nid.bytes[..16].try_into().unwrap_or([0u8; 16]);
        Uuid::from_bytes(arr).to_string()
    } else {
        nid.bytes.iter().fold(String::from("0x"), |mut a, b| {
            use std::fmt::Write;
            let _ = write!(a, "{:02x}", b);
            a
        })
    }
}

fn proto_value_to_json(v: &proto::Value) -> serde_json::Value {
    use proto::value::Kind;
    match &v.kind {
        Some(Kind::NullVal(_)) | None => serde_json::Value::Null,
        Some(Kind::BoolVal(b)) => serde_json::Value::Bool(*b),
        Some(Kind::IntVal(i)) => serde_json::json!(i),
        Some(Kind::FloatVal(f)) => serde_json::json!(f),
        Some(Kind::TextVal(s)) => serde_json::Value::String(s.clone()),
        Some(Kind::BlobVal(b)) => {
            let hex: String = b.iter().map(|x| format!("{:02x}", x)).collect();
            serde_json::Value::String(hex)
        }
        Some(Kind::VecVal(fa)) => serde_json::json!(fa.values),
    }
}

fn json_to_proto_value(v: &serde_json::Value) -> proto::Value {
    use proto::value::Kind;
    let kind = match v {
        serde_json::Value::Null => Kind::NullVal(true),
        serde_json::Value::Bool(b) => Kind::BoolVal(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Kind::IntVal(i)
            } else {
                Kind::FloatVal(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => Kind::TextVal(s.clone()),
        other => Kind::TextVal(other.to_string()),
    };
    proto::Value { kind: Some(kind) }
}

fn uuid_string_to_node_id(s: &str) -> Result<proto::NodeId, String> {
    Uuid::parse_str(s)
        .map(|u| proto::NodeId { bytes: u.as_bytes().to_vec() })
        .map_err(|_| format!("invalid UUID: {:?}", s))
}

fn parse_patterns(raw: &[String]) -> Result<Vec<proto::VarPattern>, Response> {
    raw.iter()
        .map(|p| parse_pattern(p))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": e }))).into_response()
        })
}

// ── POST /query ───────────────────────────────────────────────────────────────

async fn handle_query(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Json(body): Json<QueryBody>,
) -> Response {
    let patterns = match parse_patterns(&body.patterns) {
        Ok(p) => p,
        Err(e) => return e,
    };

    let rules: Vec<proto::DatalogRule> = match body.rules.iter()
        .map(rule_to_proto)
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(r) => r,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": e }))).into_response(),
    };

    // Resolve user_id: prefer JSON body field, fall back to X-User-Id header.
    let user_id = body.user_id.clone()
        .or_else(|| headers.get("x-user-id").and_then(|v| v.to_str().ok()).map(str::to_string))
        .unwrap_or_default();

    let req = proto::QueryRequest {
        patterns,
        rules,
        snapshot_ts: 0,
        as_of_valid_time: body.as_of_valid_time.unwrap_or(0),
        as_of_tx_time: body.as_of_tx_time.unwrap_or(0),
        tx_id: body.tx_id.unwrap_or_default(),
        user_id: user_id.clone(),
        ..Default::default()
    };

    let mut client = state.client.clone();
    let grpc_req = attach_user_id(tonic::Request::new(req), &user_id);
    let resp = match client.query(grpc_req).await {
        Ok(r) => r.into_inner(),
        Err(e) => return grpc_error(e),
    };

    let results: Vec<serde_json::Value> = resp
        .bindings
        .into_iter()
        .map(|b| {
            let mut obj = serde_json::Map::new();
            for (k, v) in b.vars {
                obj.insert(k, serde_json::Value::String(node_id_to_uuid_string(&v)));
            }
            serde_json::Value::Object(obj)
        })
        .collect();

    Json(serde_json::json!({ "results": results })).into_response()
}

// ── POST /insert ──────────────────────────────────────────────────────────────

async fn handle_insert(
    State(state): State<Arc<AppState>>,
    Json(body): Json<InsertBody>,
) -> Response {
    let subject = match uuid_string_to_node_id(&body.subject) {
        Ok(n) => n,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": e })))
                .into_response()
        }
    };
    let object = match uuid_string_to_node_id(&body.object) {
        Ok(n) => n,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": e })))
                .into_response()
        }
    };

    let properties: Vec<proto::EdgeProperty> = match body.properties.iter()
        .map(edge_property_to_proto)
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(p) => p,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": e }))).into_response(),
    };

    let triple = proto::Triple {
        kind: Some(proto::triple::Kind::Relation(proto::RelationTriple {
            subject: Some(subject),
            predicate: body.predicate,
            object: Some(object),
            vt_start: 0,
            vt_end: i64::MAX,
            properties,
        })),
    };

    let mut client = state.client.clone();
    match client
        .insert(tonic::Request::new(proto::InsertRequest {
            triples: vec![triple],
            tx_id: body.tx_id.unwrap_or_default(),
            ..Default::default()
        }))
        .await
    {
        Ok(r) => {
            let inner = r.into_inner();
            // Return the edge_id UUID so clients can query edge properties later.
            let edge_id = inner.edge_ids.first()
                .filter(|b| b.len() == 16)
                .map(|b| {
                    let arr: [u8; 16] = b[..16].try_into().unwrap_or([0u8; 16]);
                    Uuid::from_bytes(arr).to_string()
                });
            Json(serde_json::json!({ "ok": true, "tx_time": inner.commit_ts, "edge_id": edge_id }))
                .into_response()
        }
        Err(e) => grpc_error(e),
    }
}

// ── GET /triples ──────────────────────────────────────────────────────────────
//
// Builds a single VarPattern from query params and runs a Query RPC.
// Because VarPattern.predicate is a filter string (not a variable), the predicate
// is not included in bindings — the response echoes back the provided predicate
// or an empty string when none was supplied.

async fn handle_triples(
    State(state): State<Arc<AppState>>,
    QueryParams(params): QueryParams<TripleQueryParams>,
) -> Response {
    let subject_term = match &params.subject {
        Some(s) => match uuid_string_to_node_id(s) {
            Ok(nid) => proto::Term { kind: Some(proto::term::Kind::Bound(nid)) },
            Err(e) => {
                return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": e })))
                    .into_response()
            }
        },
        None => proto::Term { kind: Some(proto::term::Kind::Var("__s".to_string())) },
    };

    let object_term = match &params.object {
        Some(o) => match uuid_string_to_node_id(o) {
            Ok(nid) => proto::Term { kind: Some(proto::term::Kind::Bound(nid)) },
            Err(e) => {
                return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": e })))
                    .into_response()
            }
        },
        None => proto::Term { kind: Some(proto::term::Kind::Var("__o".to_string())) },
    };

    let predicate_filter = params.predicate.clone().unwrap_or_default();

    let req = proto::QueryRequest {
        patterns: vec![proto::VarPattern {
            subject: Some(subject_term),
            predicate: predicate_filter.clone(),
            object: Some(object_term),
        }],
        snapshot_ts: 0,
        as_of_valid_time: 0,
        as_of_tx_time: 0,
        rules: vec![],
        ..Default::default()
    };

    let mut client = state.client.clone();
    let resp = match client.query(tonic::Request::new(req)).await {
        Ok(r) => r.into_inner(),
        Err(e) => return grpc_error(e),
    };

    let triples: Vec<TripleJson> = resp
        .bindings
        .into_iter()
        .map(|b| TripleJson {
            subject: params
                .subject
                .clone()
                .unwrap_or_else(|| b.vars.get("__s").map(node_id_to_uuid_string).unwrap_or_default()),
            predicate: predicate_filter.clone(),
            object: params
                .object
                .clone()
                .unwrap_or_else(|| b.vars.get("__o").map(node_id_to_uuid_string).unwrap_or_default()),
        })
        .collect();

    Json(serde_json::json!({ "triples": triples })).into_response()
}

// ── POST /vector/search ───────────────────────────────────────────────────────

async fn handle_vector_search(
    State(state): State<Arc<AppState>>,
    Json(body): Json<VectorSearchBody>,
) -> Response {
    let req = proto::SearchVectorRequest {
        query: body.vector,
        k: body.top_k,
        space: body.namespace,
        ef: body.ef,
    };

    let mut client = state.client.clone();
    match client.search_vector(tonic::Request::new(req)).await {
        Ok(r) => {
            let results: Vec<serde_json::Value> = r
                .into_inner()
                .results
                .into_iter()
                .map(|vr| serde_json::json!({
                    "id": vr.node_id.as_ref().map(node_id_to_uuid_string).unwrap_or_default(),
                    "score": vr.similarity,
                }))
                .collect();
            Json(serde_json::json!({ "results": results })).into_response()
        }
        Err(e) => grpc_error(e),
    }
}

// ── GET /health ───────────────────────────────────────────────────────────────

async fn handle_health(State(state): State<Arc<AppState>>) -> Response {
    let mut client = state.client.clone();
    match client
        .replica_status(tonic::Request::new(proto::ReplicaStatusRequest {}))
        .await
    {
        Ok(r) => {
            let inner = r.into_inner();
            let mode = if inner.is_replica { "replica" } else { "primary" };
            Json(serde_json::json!({ "status": "ok", "mode": mode })).into_response()
        }
        Err(e) => grpc_error(e),
    }
}

// ── POST /cypher ──────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct CypherBody {
    cypher: String,
    #[serde(default)]
    as_of_valid_time: Option<i64>,
    #[serde(default)]
    as_of_tx_time: Option<i64>,
    /// Query embedding vector for VECTOR_NEAR. Required when the Cypher string
    /// contains VECTOR_NEAR(var, "space", k); omit or leave empty otherwise.
    #[serde(default)]
    vector: Vec<f32>,
    /// HNSW exploration factor override. 0 or absent = use server default.
    #[serde(default)]
    ef: u32,
    /// Open transaction ID to read from.
    #[serde(default)]
    tx_id: Option<String>,
    /// Optional user identity for access-control filtering.
    #[serde(default)]
    user_id: Option<String>,
}

#[derive(Deserialize)]
struct CypherWriteBody {
    cypher: String,
    /// Open transaction ID to buffer writes into instead of auto-committing.
    #[serde(default)]
    tx_id: Option<String>,
}

async fn handle_cypher(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Json(body): Json<CypherBody>,
) -> Response {
    let user_id = body.user_id.clone()
        .or_else(|| headers.get("x-user-id").and_then(|v| v.to_str().ok()).map(str::to_string))
        .unwrap_or_default();

    let req = proto::CypherQueryRequest {
        cypher: body.cypher,
        as_of_valid_time: body.as_of_valid_time.unwrap_or(0),
        as_of_tx_time: body.as_of_tx_time.unwrap_or(0),
        vector: body.vector,
        ef: body.ef,
        tx_id: body.tx_id.unwrap_or_default(),
        user_id: user_id.clone(),
        ..Default::default()
    };

    let mut client = state.client.clone();
    let grpc_req = attach_user_id(tonic::Request::new(req), &user_id);
    let resp = match client.cypher_query(grpc_req).await {
        Ok(r) => r.into_inner(),
        Err(e) => return grpc_error(e),
    };

    let results: Vec<serde_json::Value> = resp
        .rows
        .into_iter()
        .map(|b| {
            let mut obj = serde_json::Map::new();
            for (k, v) in b.nodes {
                obj.insert(k, serde_json::Value::String(node_id_to_uuid_string(&v)));
            }
            for (k, v) in b.values {
                obj.insert(k, proto_value_to_json(&v));
            }
            serde_json::Value::Object(obj)
        })
        .collect();

    Json(serde_json::json!({ "results": results })).into_response()
}

// ── POST /cypher/write ────────────────────────────────────────────────────────

async fn handle_cypher_write(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CypherWriteBody>,
) -> Response {
    let req = proto::CypherWriteRequest {
        cypher: body.cypher,
        tx_id: body.tx_id.unwrap_or_default(),
        ..Default::default()
    };

    let mut client = state.client.clone();
    match client.cypher_write(tonic::Request::new(req)).await {
        Ok(r) => {
            let inner = r.into_inner();
            let created_node_ids: Vec<String> = inner
                .created_node_ids
                .iter()
                .filter(|b| b.len() == 16)
                .map(|b| {
                    let arr: [u8; 16] = b[..16].try_into().unwrap_or([0u8; 16]);
                    Uuid::from_bytes(arr).to_string()
                })
                .collect();
            Json(serde_json::json!({
                "ok": true,
                "created_node_ids": created_node_ids,
                "triples_written": inner.triples_written,
                "triples_deleted": inner.triples_deleted,
            }))
            .into_response()
        }
        Err(e) => grpc_error(e),
    }
}

// ── POST /query/stream ────────────────────────────────────────────────────────

async fn handle_query_stream(
    State(state): State<Arc<AppState>>,
    Json(body): Json<QueryBody>,
) -> axum::response::Response {
    let patterns = match parse_patterns(&body.patterns) {
        Ok(p) => p,
        Err(e) => return e,
    };

    let rules: Vec<proto::DatalogRule> = match body.rules.iter()
        .map(rule_to_proto)
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(r) => r,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": e }))).into_response(),
    };

    let req = proto::QueryRequest {
        patterns,
        rules,
        snapshot_ts: 0,
        as_of_valid_time: body.as_of_valid_time.unwrap_or(0),
        as_of_tx_time: body.as_of_tx_time.unwrap_or(0),
        ..Default::default()
    };

    let mut client = state.client.clone();
    let grpc_stream = match client.query_stream(tonic::Request::new(req)).await {
        Ok(r) => r.into_inner(),
        Err(e) => return grpc_error(e),
    };

    ndjson_streaming_response(grpc_stream).await
}

// ── POST /cypher/stream ───────────────────────────────────────────────────────

async fn handle_cypher_stream(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CypherBody>,
) -> axum::response::Response {
    let req = proto::CypherQueryRequest {
        cypher: body.cypher,
        as_of_valid_time: body.as_of_valid_time.unwrap_or(0),
        as_of_tx_time: body.as_of_tx_time.unwrap_or(0),
        vector: body.vector,
        ef: body.ef,
        ..Default::default()
    };

    let mut client = state.client.clone();
    let grpc_stream = match client.cypher_query_stream(tonic::Request::new(req)).await {
        Ok(r) => r.into_inner(),
        Err(e) => return grpc_error(e),
    };

    ndjson_streaming_response(grpc_stream).await
}

/// Consume a `QueryStreamChunk` gRPC stream and emit NDJSON via a streaming
/// HTTP response body. Each `QueryResult` is written as one JSON object + "\n".
async fn ndjson_streaming_response(
    mut grpc_stream: tonic::codec::Streaming<proto::QueryStreamChunk>,
) -> axum::response::Response {
    use axum::http::header;

    let (mut body_tx, body) = hyper::Body::channel();

    tokio::spawn(async move {
        loop {
            match grpc_stream.message().await {
                Ok(Some(chunk)) => {
                    for result in chunk.results {
                        let mut obj = serde_json::Map::new();
                        for (k, v) in result.vars {
                            obj.insert(k, serde_json::Value::String(node_id_to_uuid_string(&v)));
                        }
                        let mut line = serde_json::to_string(&serde_json::Value::Object(obj))
                            .unwrap_or_else(|_| "{}".to_string());
                        line.push('\n');
                        if body_tx.send_data(bytes::Bytes::from(line)).await.is_err() {
                            return;
                        }
                    }
                    if chunk.done {
                        break;
                    }
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }
        // body_tx is dropped here, signalling EOF to the HTTP client
    });

    axum::response::Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .body(axum::body::boxed(body))
        .unwrap()
}

// ── POST /explain ─────────────────────────────────────────────────────────────

async fn handle_explain(
    State(state): State<Arc<AppState>>,
    Json(body): Json<QueryBody>,
) -> Response {
    let patterns = match parse_patterns(&body.patterns) {
        Ok(p) => p,
        Err(e) => return e,
    };

    // Rules are forwarded so the explain output reflects the full query shape.
    let rules: Vec<proto::DatalogRule> = match body.rules.iter()
        .map(rule_to_proto)
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(r) => r,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": e }))).into_response(),
    };

    let req = proto::QueryRequest {
        patterns,
        rules,
        snapshot_ts: 0,
        as_of_valid_time: body.as_of_valid_time.unwrap_or(0),
        as_of_tx_time: body.as_of_tx_time.unwrap_or(0),
        ..Default::default()
    };

    let mut client = state.client.clone();
    match client.explain_query(tonic::Request::new(req)).await {
        Ok(r) => {
            let er = r.into_inner();
            let nodes: Vec<serde_json::Value> = er
                .nodes
                .into_iter()
                .map(|n| serde_json::json!({
                    "node_type": n.node_type,
                    "description": n.description,
                    "index_used": n.index_used,
                }))
                .collect();
            Json(serde_json::json!({ "plan_text": er.plan_text, "nodes": nodes }))
                .into_response()
        }
        Err(e) => grpc_error(e),
    }
}

// ── GET /indexes ──────────────────────────────────────────────────────────────

async fn handle_indexes(State(state): State<Arc<AppState>>) -> Response {
    let mut client = state.client.clone();
    match client.show_indexes(tonic::Request::new(proto::ShowIndexesRequest {})).await {
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
        Err(e) => grpc_error(e),
    }
}

// ── POST /tx/begin ────────────────────────────────────────────────────────────

async fn handle_tx_begin(State(state): State<Arc<AppState>>) -> Response {
    let mut client = state.client.clone();
    match client.begin_transaction(tonic::Request::new(proto::BeginTransactionRequest {})).await {
        Ok(r) => Json(serde_json::json!({ "tx_id": r.into_inner().tx_id })).into_response(),
        Err(e) => grpc_error(e),
    }
}

// ── POST /tx/commit ───────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct TxIdBody {
    tx_id: String,
}

async fn handle_tx_commit(
    State(state): State<Arc<AppState>>,
    Json(body): Json<TxIdBody>,
) -> Response {
    let req = proto::CommitTransactionRequest { tx_id: body.tx_id };
    let mut client = state.client.clone();
    match client.commit_transaction(tonic::Request::new(req)).await {
        Ok(r) => {
            let inner = r.into_inner();
            Json(serde_json::json!({ "ok": true, "triples_written": inner.triples_written }))
                .into_response()
        }
        Err(e) => grpc_error(e),
    }
}

// ── POST /tx/rollback ─────────────────────────────────────────────────────────

async fn handle_tx_rollback(
    State(state): State<Arc<AppState>>,
    Json(body): Json<TxIdBody>,
) -> Response {
    let req = proto::RollbackTransactionRequest { tx_id: body.tx_id };
    let mut client = state.client.clone();
    match client.rollback_transaction(tonic::Request::new(req)).await {
        Ok(_) => Json(serde_json::json!({ "ok": true })).into_response(),
        Err(e) => grpc_error(e),
    }
}

// ── POST /edge-annotations ────────────────────────────────────────────────────

/// JSON body for inserting edge annotations (RDF-star / statement metadata).
///
/// `edge_id` must be the UUID of an edge that already exists (or will be
/// inserted in the same call via the normal `/insert` endpoint).
/// Either `node_id` (a UUID string) or `scalar` (any JSON value) must be
/// present.
#[derive(Deserialize)]
struct EdgeAnnotationBody {
    edge_id:   String,
    predicate: String,
    node_id:   Option<String>,
    scalar:    Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct InsertEdgeAnnotationsBody {
    annotations: Vec<EdgeAnnotationBody>,
}

async fn handle_insert_edge_annotations(
    State(state): State<Arc<AppState>>,
    Json(body): Json<InsertEdgeAnnotationsBody>,
) -> Response {
    let mut annotations = Vec::new();
    for a in body.annotations {
        // Parse edge_id
        let edge_uuid = match Uuid::parse_str(&a.edge_id) {
            Ok(u) => u,
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": format!("invalid edge_id UUID: {}", a.edge_id) })),
                )
                    .into_response()
            }
        };
        let edge_bytes = edge_uuid.as_bytes().to_vec();

        let value = if let Some(node_id_str) = a.node_id {
            let node_uuid = match Uuid::parse_str(&node_id_str) {
                Ok(u) => u,
                Err(_) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(serde_json::json!({ "error": format!("invalid node_id UUID: {}", node_id_str) })),
                    )
                        .into_response()
                }
            };
            Some(proto::edge_annotation::Value::NodeId(node_uuid.as_bytes().to_vec()))
        } else if let Some(scalar_val) = a.scalar {
            let v = json_to_proto_value(&scalar_val);
            Some(proto::edge_annotation::Value::Scalar(v))
        } else {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "each annotation must have either node_id or scalar" })),
            )
                .into_response();
        };

        annotations.push(proto::EdgeAnnotation {
            edge_id: edge_bytes,
            predicate: a.predicate,
            value,
        });
    }

    let req = proto::InsertRequest {
        edge_annotations: annotations,
        ..Default::default()
    };

    let mut client = state.client.clone();
    match client.insert(tonic::Request::new(req)).await {
        Ok(r) => {
            let inner = r.into_inner();
            Json(serde_json::json!({ "ok": true, "commit_ts": inner.commit_ts }))
                .into_response()
        }
        Err(e) => grpc_error(e),
    }
}

// ── GET /edge-annotations/:edge_id ───────────────────────────────────────────

async fn handle_get_edge_annotations(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(edge_id): axum::extract::Path<String>,
) -> Response {
    let edge_uuid = match Uuid::parse_str(&edge_id) {
        Ok(u) => u,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": format!("invalid edge_id UUID: {}", edge_id) })),
            )
                .into_response()
        }
    };

    let req = proto::GetEdgeAnnotationsRequest {
        edge_id: edge_uuid.as_bytes().to_vec(),
    };

    let mut client = state.client.clone();
    match client.get_edge_annotations(tonic::Request::new(req)).await {
        Ok(r) => {
            let annotations: Vec<serde_json::Value> = r
                .into_inner()
                .annotations
                .into_iter()
                .map(|a| {
                    let val = match a.value {
                        Some(proto::edge_annotation::Value::NodeId(bytes)) if bytes.len() == 16 => {
                            let arr: [u8; 16] = bytes[..16].try_into().unwrap_or([0u8; 16]);
                            serde_json::json!({ "node_id": Uuid::from_bytes(arr).to_string() })
                        }
                        Some(proto::edge_annotation::Value::Scalar(v)) => {
                            serde_json::json!({ "scalar": proto_value_to_json(&v) })
                        }
                        _ => serde_json::json!(null),
                    };
                    serde_json::json!({
                        "predicate": a.predicate,
                        "value": val,
                    })
                })
                .collect();
            Json(serde_json::json!({ "annotations": annotations })).into_response()
        }
        Err(e) => grpc_error(e),
    }
}

// ── POST /access/grant ────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct GrantAccessBody {
    /// UUID string of the Group node.
    group_id: String,
    /// UUID string of the target node (for a direct grant).
    node_id: Option<String>,
    /// Type name string (for a type-level grant).
    type_name: Option<String>,
}

async fn handle_grant_access(
    State(state): State<Arc<AppState>>,
    Json(body): Json<GrantAccessBody>,
) -> Response {
    let group_id = match uuid_string_to_node_id(&body.group_id) {
        Ok(n) => n.bytes,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": e }))).into_response(),
    };

    let target = if let Some(ref nid_str) = body.node_id {
        match uuid_string_to_node_id(nid_str) {
            Ok(n) => proto::grant_access_request::Target::NodeId(n.bytes),
            Err(e) => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": e }))).into_response(),
        }
    } else if let Some(ref tn) = body.type_name {
        proto::grant_access_request::Target::TypeName(tn.clone())
    } else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "node_id or type_name must be set" }))).into_response();
    };

    let req = proto::GrantAccessRequest { group_id, target: Some(target) };
    let mut client = state.client.clone();
    match client.grant_access(tonic::Request::new(req)).await {
        Ok(_) => Json(serde_json::json!({ "ok": true })).into_response(),
        Err(e) => grpc_error(e),
    }
}

// ── POST /access/revoke ───────────────────────────────────────────────────────

#[derive(Deserialize)]
struct RevokeAccessBody {
    group_id: String,
    node_id: Option<String>,
    type_name: Option<String>,
}

async fn handle_revoke_access(
    State(state): State<Arc<AppState>>,
    Json(body): Json<RevokeAccessBody>,
) -> Response {
    let group_id = match uuid_string_to_node_id(&body.group_id) {
        Ok(n) => n.bytes,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": e }))).into_response(),
    };

    let target = if let Some(ref nid_str) = body.node_id {
        match uuid_string_to_node_id(nid_str) {
            Ok(n) => proto::revoke_access_request::Target::NodeId(n.bytes),
            Err(e) => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": e }))).into_response(),
        }
    } else if let Some(ref tn) = body.type_name {
        proto::revoke_access_request::Target::TypeName(tn.clone())
    } else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "node_id or type_name must be set" }))).into_response();
    };

    let req = proto::RevokeAccessRequest { group_id, target: Some(target) };
    let mut client = state.client.clone();
    match client.revoke_access(tonic::Request::new(req)).await {
        Ok(_) => Json(serde_json::json!({ "ok": true })).into_response(),
        Err(e) => grpc_error(e),
    }
}

// ── POST /access/add-user ─────────────────────────────────────────────────────

#[derive(Deserialize)]
struct AddUserToGroupBody {
    user_id: String,
    group_id: String,
}

async fn handle_add_user_to_group(
    State(state): State<Arc<AppState>>,
    Json(body): Json<AddUserToGroupBody>,
) -> Response {
    let user_id = match uuid_string_to_node_id(&body.user_id) {
        Ok(n) => n.bytes,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": e }))).into_response(),
    };
    let group_id = match uuid_string_to_node_id(&body.group_id) {
        Ok(n) => n.bytes,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": e }))).into_response(),
    };

    let req = proto::AddUserToGroupRequest { user_id, group_id };
    let mut client = state.client.clone();
    match client.add_user_to_group(tonic::Request::new(req)).await {
        Ok(_) => Json(serde_json::json!({ "ok": true })).into_response(),
        Err(e) => grpc_error(e),
    }
}

// ── GET /access/user/:user_id ─────────────────────────────────────────────────

async fn handle_get_user_access(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(user_id_str): axum::extract::Path<String>,
) -> Response {
    let user_id = match uuid_string_to_node_id(&user_id_str) {
        Ok(n) => n.bytes,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": e }))).into_response(),
    };

    let req = proto::GetUserAccessRequest { user_id };
    let mut client = state.client.clone();
    match client.get_user_access(tonic::Request::new(req)).await {
        Ok(r) => {
            let inner = r.into_inner();
            let node_ids: Vec<String> = inner.node_ids
                .iter()
                .filter(|b| b.len() == 16)
                .map(|b| {
                    let arr: [u8; 16] = b[..16].try_into().unwrap_or([0u8; 16]);
                    Uuid::from_bytes(arr).to_string()
                })
                .collect();
            Json(serde_json::json!({
                "node_ids": node_ids,
                "type_grants": inner.type_grants,
            })).into_response()
        }
        Err(e) => grpc_error(e),
    }
}

// ── GET /stats ────────────────────────────────────────────────────────────────

async fn handle_stats(State(state): State<Arc<AppState>>) -> Response {
    let mut client = state.client.clone();
    match client.show_stats(tonic::Request::new(proto::ShowStatsRequest {})).await {
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
        Err(e) => grpc_error(e),
    }
}

// ── main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let endpoint = tonic::transport::Endpoint::from_shared(args.upstream.clone())?;
    let endpoint = if let Some(ca_path) = &args.tls_ca {
        let pem = tokio::fs::read(ca_path).await?;
        let cert = tonic::transport::Certificate::from_pem(pem);
        let tls = tonic::transport::ClientTlsConfig::new().ca_certificate(cert);
        endpoint.tls_config(tls)?
    } else {
        endpoint
    };
    let channel = endpoint.connect_lazy();

    let client =
        PolarGraphServiceClient::with_interceptor(channel, AuthInterceptor { token: args.api_key });

    let state = Arc::new(AppState { client });

    let app = Router::new()
        .route("/query", post(handle_query))
        .route("/query/stream", post(handle_query_stream))
        .route("/cypher", post(handle_cypher))
        .route("/cypher/write", post(handle_cypher_write))
        .route("/cypher/stream", post(handle_cypher_stream))
        .route("/insert", post(handle_insert))
        .route("/triples", get(handle_triples))
        .route("/vector/search", post(handle_vector_search))
        .route("/health", get(handle_health))
        .route("/explain", post(handle_explain))
        .route("/indexes", get(handle_indexes))
        .route("/stats", get(handle_stats))
        .route("/tx/begin", post(handle_tx_begin))
        .route("/tx/commit", post(handle_tx_commit))
        .route("/tx/rollback", post(handle_tx_rollback))
        .route("/edge-annotations", post(handle_insert_edge_annotations))
        .route("/edge-annotations/:edge_id", get(handle_get_edge_annotations))
        .route("/access/grant", post(handle_grant_access))
        .route("/access/revoke", post(handle_revoke_access))
        .route("/access/add-user", post(handle_add_user_to_group))
        .route("/access/user/:user_id", get(handle_get_user_access))
        .with_state(state);

    info!(addr = %args.listen, upstream = %args.upstream, "polargraph-rest listening");
    axum::Server::bind(&args.listen).serve(app.into_make_service()).await?;

    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_pattern_variable_predicate_variable() {
        let p = parse_pattern("?s :knows ?o").unwrap();
        assert_eq!(p.predicate, "knows");

        let subj = p.subject.unwrap();
        match subj.kind {
            Some(proto::term::Kind::Var(v)) => assert_eq!(v, "s"),
            other => panic!("expected Var(s), got {:?}", other),
        }

        let obj = p.object.unwrap();
        match obj.kind {
            Some(proto::term::Kind::Var(v)) => assert_eq!(v, "o"),
            other => panic!("expected Var(o), got {:?}", other),
        }
    }

    #[test]
    fn parse_pattern_no_colon_prefix() {
        let p = parse_pattern("?s knows ?o").unwrap();
        assert_eq!(p.predicate, "knows");
    }

    #[test]
    fn parse_pattern_wildcard_term() {
        let p = parse_pattern("_ :knows ?o").unwrap();
        let subj = p.subject.unwrap();
        assert!(subj.kind.is_none(), "wildcard should have no kind");
    }

    #[test]
    fn parse_pattern_bound_uuid() {
        let uuid_str = "018e8c1e-1234-7000-8000-000000000001";
        let p = parse_pattern(&format!("{} :knows ?o", uuid_str)).unwrap();
        let subj = p.subject.unwrap();
        match subj.kind {
            Some(proto::term::Kind::Bound(nid)) => {
                let bytes: [u8; 16] = nid.bytes[..16].try_into().unwrap();
                assert_eq!(Uuid::from_bytes(bytes).to_string(), uuid_str);
            }
            other => panic!("expected Bound, got {:?}", other),
        }
    }

    #[test]
    fn parse_pattern_wrong_token_count() {
        assert!(parse_pattern("?s :knows").is_err());
        assert!(parse_pattern("?s :knows ?o extra").is_err());
    }

    #[test]
    fn grpc_resource_exhausted_maps_to_429() {
        let code = tonic::Status::resource_exhausted("rate limit").code();
        assert_eq!(grpc_to_http_status(code), StatusCode::TOO_MANY_REQUESTS);
    }

    #[test]
    fn grpc_status_code_mappings() {
        use tonic::Code;
        assert_eq!(grpc_to_http_status(Code::NotFound), StatusCode::NOT_FOUND);
        assert_eq!(grpc_to_http_status(Code::Unauthenticated), StatusCode::UNAUTHORIZED);
        assert_eq!(grpc_to_http_status(Code::PermissionDenied), StatusCode::FORBIDDEN);
        assert_eq!(grpc_to_http_status(Code::DeadlineExceeded), StatusCode::REQUEST_TIMEOUT);
        assert_eq!(grpc_to_http_status(Code::InvalidArgument), StatusCode::BAD_REQUEST);
        assert_eq!(grpc_to_http_status(Code::Internal), StatusCode::INTERNAL_SERVER_ERROR);
    }

    /// attach_user_id sets the x-polargraph-user-id metadata header for a
    /// non-empty user_id and leaves the request unmodified for an empty one.
    #[test]
    fn attach_user_id_sets_metadata_when_non_empty() {
        let req = tonic::Request::new(());
        let req = attach_user_id(req, "alice-uuid-123");
        assert_eq!(
            req.metadata().get("x-polargraph-user-id")
                .map(|v| v.to_str().unwrap()),
            Some("alice-uuid-123"),
            "metadata header should be set for non-empty user_id"
        );
    }

    #[test]
    fn attach_user_id_is_noop_for_empty_string() {
        let req = tonic::Request::new(());
        let req = attach_user_id(req, "");
        assert!(
            req.metadata().get("x-polargraph-user-id").is_none(),
            "no metadata header should be set for empty user_id"
        );
    }
}
