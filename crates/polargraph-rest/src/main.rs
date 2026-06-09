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

type GrpcClient = PolarGraphServiceClient<
    tonic::service::interceptor::InterceptedService<Channel, AuthInterceptor>,
>;

// ── App state ─────────────────────────────────────────────────────────────────

struct AppState {
    /// gRPC client; cheap to clone (backed by a pooled Channel).
    client: GrpcClient,
}

// ── JSON request/response types ───────────────────────────────────────────────

#[derive(Deserialize)]
struct QueryBody {
    patterns: Vec<String>,
    /// Reserved for future Datalog rule support; accepted but not yet forwarded.
    #[serde(default)]
    #[allow(dead_code)]
    rules: Vec<serde_json::Value>,
    #[serde(default)]
    as_of_valid_time: Option<i64>,
    #[serde(default)]
    as_of_tx_time: Option<i64>,
}

#[derive(Deserialize)]
struct InsertBody {
    subject: String,
    predicate: String,
    object: String,
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
    Json(body): Json<QueryBody>,
) -> Response {
    let patterns = match parse_patterns(&body.patterns) {
        Ok(p) => p,
        Err(e) => return e,
    };

    let req = proto::QueryRequest {
        patterns,
        snapshot_ts: 0,
        as_of_valid_time: body.as_of_valid_time.unwrap_or(0),
        as_of_tx_time: body.as_of_tx_time.unwrap_or(0),
    };

    let mut client = state.client.clone();
    let resp = match client.query(tonic::Request::new(req)).await {
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

    let triple = proto::Triple {
        kind: Some(proto::triple::Kind::Relation(proto::RelationTriple {
            subject: Some(subject),
            predicate: body.predicate,
            object: Some(object),
            vt_start: 0,
            vt_end: i64::MAX,
        })),
    };

    let mut client = state.client.clone();
    match client
        .insert(tonic::Request::new(proto::InsertRequest { triples: vec![triple] }))
        .await
    {
        Ok(r) => {
            Json(serde_json::json!({ "ok": true, "tx_time": r.into_inner().commit_ts }))
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

// ── POST /explain ─────────────────────────────────────────────────────────────

async fn handle_explain(
    State(state): State<Arc<AppState>>,
    Json(body): Json<QueryBody>,
) -> Response {
    let patterns = match parse_patterns(&body.patterns) {
        Ok(p) => p,
        Err(e) => return e,
    };

    let req = proto::QueryRequest {
        patterns,
        snapshot_ts: 0,
        as_of_valid_time: body.as_of_valid_time.unwrap_or(0),
        as_of_tx_time: body.as_of_tx_time.unwrap_or(0),
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
        .route("/insert", post(handle_insert))
        .route("/triples", get(handle_triples))
        .route("/vector/search", post(handle_vector_search))
        .route("/health", get(handle_health))
        .route("/explain", post(handle_explain))
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
}
