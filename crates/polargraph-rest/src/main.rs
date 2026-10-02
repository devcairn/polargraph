//! HTTP/JSON REST gateway for PolarGraph.
//!
//! Translates HTTP requests into gRPC calls to a running polargraphd instance
//! and returns JSON responses. Useful for clients that cannot use a gRPC stub.

use axum::{
    extract::{Query as QueryParams, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use clap::Parser;
use polargraph_core::{id::NodeId, term::iri_to_node_id};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc};
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tracing::info;
use uuid::Uuid;

#[allow(clippy::enum_variant_names)]
mod proto {
    tonic::include_proto!("polargraph.v1");
}

use proto::polar_graph_service_client::PolarGraphServiceClient;

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "polargraph-rest",
    about = "HTTP/JSON REST gateway for PolarGraph"
)]
struct Args {
    /// gRPC address of the upstream polargraphd server.
    #[arg(
        long,
        env = "POLARGRAPH_UPSTREAM",
        default_value = "http://localhost:50051"
    )]
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

    /// Base IRI for skolemized blank nodes on RDF import
    /// (`{base}/.well-known/genid/{import_id}/{label}`). Set this to the
    /// instance's public origin, e.g. `https://kb.example.com`.
    #[arg(
        long,
        env = "POLARGRAPH_REST_SKOLEM_BASE",
        default_value = polargraph_sparql::DEFAULT_SKOLEM_BASE
    )]
    skolem_base: String,
}

// ── Auth interceptor ──────────────────────────────────────────────────────────

#[derive(Clone)]
struct AuthInterceptor {
    token: Option<String>,
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(t) = &self.token {
            let val: MetadataValue<tonic::metadata::Ascii> =
                format!("Bearer {}", t).parse().map_err(|_| {
                    tonic::Status::internal("could not encode api key as gRPC metadata")
                })?;
            req.metadata_mut().insert("authorization", val);
        }
        // Forward the HTTP caller's identity (see `forward_user_id`) unless a
        // handler already set one.
        if !req.metadata().contains_key("x-polargraph-user-id") {
            if let Ok(Some(val)) = REQUEST_USER.try_with(|u| {
                (!u.is_empty())
                    .then(|| u.parse::<MetadataValue<tonic::metadata::Ascii>>().ok())
                    .flatten()
            }) {
                req.metadata_mut().insert("x-polargraph-user-id", val);
            }
        }
        Ok(req)
    }
}

tokio::task_local! {
    /// The `X-User-Id` of the HTTP request being handled.
    static REQUEST_USER: String;
}

/// Middleware: make the request's `X-User-Id` header the identity of every
/// gRPC call the handler makes, so the server's graph access control applies
/// to all endpoints (SPARQL, imports, exports, `/graphs`, …).
async fn forward_user_id<B>(
    req: axum::http::Request<B>,
    next: axum::middleware::Next<B>,
) -> Response {
    let user = req
        .headers()
        .get("x-user-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    REQUEST_USER.scope(user, next.run(req)).await
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
    /// Base IRI for blank-node skolem IRIs on RDF import.
    skolem_base: String,
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
    head_predicate: String,
    head_subject_var: String,
    head_object_var: String,
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
    /// Dataset: graph IRIs for patterns without an `@graph` suffix.
    #[serde(default)]
    graphs: Vec<String>,
}

/// A scalar property to attach to an edge at insert time.
/// `value` follows the same JSON-encoding as `PropertyTriple.value`:
/// `{"text_val":"hello"}`, `{"int_val":42}`, `{"float_val":3.14}`, `{"bool_val":true}`,
/// `{"blob_val":"<base64>"}`, or `{"vec_val":{"values":[...]}}`.
#[derive(Deserialize)]
struct EdgePropertyJson {
    name: String,
    value: serde_json::Value,
}

#[derive(Deserialize)]
struct InsertBody {
    subject: String,
    predicate: String,
    object: String,
    /// Optional properties stored on the edge (accessible via the returned edge_id).
    #[serde(default)]
    properties: Vec<EdgePropertyJson>,
    /// Open transaction ID to buffer this insert into instead of auto-committing.
    #[serde(default)]
    tx_id: Option<String>,
    /// Named graph (IRI) to write into; omitted = the default graph.
    #[serde(default)]
    graph: Option<String>,
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
    if !(parts.len() == 3 || (parts.len() == 4 && parts[3].starts_with('@'))) {
        return Err(format!(
            "pattern must be `subject predicate object [@graph]` (got {} tokens): {:?}",
            parts.len(),
            s
        ));
    }
    let graph = match parts.get(3) {
        Some(g) => {
            Some(parse_graph_token(&g[1..]).map_err(|e| format!("graph in {:?}: {}", s, e))?)
        }
        None => None,
    };
    let subject = parse_term(parts[0]).map_err(|e| format!("subject in {:?}: {}", s, e))?;
    let predicate = parts[1].trim_start_matches(':').to_string();
    let object = parse_term(parts[2]).map_err(|e| format!("object in {:?}: {}", s, e))?;
    Ok(proto::VarPattern {
        subject: Some(subject),
        predicate,
        object: Some(object),
        predicate_var: String::new(),
        graph,
    })
}

/// The `@graph` suffix of a pattern string: `@default`, `@?g` (graph
/// variable), or `@<iri>` / `@iri` (one named graph).
fn parse_graph_token(s: &str) -> Result<proto::GraphTerm, String> {
    use proto::graph_term::Kind;
    let kind = if s == "default" {
        Kind::DefaultGraph(true)
    } else if let Some(var) = s.strip_prefix('?') {
        if var.is_empty() {
            return Err("empty graph variable".into());
        }
        Kind::Var(var.to_string())
    } else {
        let iri = s
            .strip_prefix('<')
            .and_then(|t| t.strip_suffix('>'))
            .unwrap_or(s);
        if iri.is_empty() {
            return Err("empty graph IRI".into());
        }
        Kind::Iri(iri.to_string())
    };
    Ok(proto::GraphTerm { kind: Some(kind) })
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
        head_predicate: rule.head_predicate.clone(),
        head_subject_var: rule.head_subject_var.clone(),
        head_object_var: rule.head_object_var.clone(),
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
            .map(|b| {
                b.as_u64()
                    .and_then(|n| u8::try_from(n).ok())
                    .ok_or_else(|| {
                        format!(
                            "blob_val elements must be integers 0–255 for property {:?}",
                            ep.name
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        proto::value::Kind::BlobVal(bytes)
    } else if let Some(arr) = ep
        .value
        .get("vec_val")
        .and_then(|v| v.get("values"))
        .and_then(|v| v.as_array())
    {
        let values = arr
            .iter()
            .map(|f| {
                f.as_f64()
                    .map(|x| x as f32)
                    .ok_or_else(|| "vec_val elements must be numbers".to_string())
            })
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
        name: ep.name.clone(),
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
        // Resume point older than the retained change log.
        Code::OutOfRange => StatusCode::GONE,
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

/// A query row as SPARQL bindings: node variables as IRIs (by node), value
/// variables as literals (`docs/design/value-bindings.md`).
fn sparql_row(pb: proto::Binding) -> polargraph_sparql::SparqlBindings {
    use polargraph_sparql::SparqlValue;
    let mut b: polargraph_sparql::SparqlBindings = std::collections::HashMap::new();
    for (k, v) in pb.vars {
        if let Some(id) = proto_node_id(&v) {
            b.insert(k, SparqlValue::Uri(id));
        }
    }
    for (k, v) in pb.values {
        if let Some(value) = proto_value_to_pg(&v)
            .as_ref()
            .and_then(SparqlValue::from_value)
        {
            b.insert(k, value);
        }
    }
    // Predicate variables (`?p` in `?s ?p ?o`) bind the predicate IRI.
    for (k, p) in pb.predicates {
        b.insert(k, SparqlValue::Iri(p));
    }
    b
}

/// Query rows whose variables are all nodes. Paths that don't read value
/// bindings yet use this to keep their results as before value bindings
/// (docs/design/value-bindings.md); a row with a value binding is one the
/// engine used to drop.
fn node_rows(bindings: Vec<proto::Binding>) -> impl Iterator<Item = proto::Binding> {
    bindings.into_iter().filter(|b| b.values.is_empty())
}

/// One `/query` row: node variables as UUID strings, plus — when any
/// variable is bound to a property value — `"@values": {var: value}` (the
/// JSON value encoding of `/insert`; `@` can't occur in a variable name).
fn query_row_json(
    vars: std::collections::HashMap<String, proto::NodeId>,
    values: std::collections::HashMap<String, proto::Value>,
) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    for (k, v) in vars {
        obj.insert(k, serde_json::Value::String(node_id_to_uuid_string(&v)));
    }
    if !values.is_empty() {
        let values: serde_json::Map<String, serde_json::Value> = values
            .iter()
            .map(|(k, v)| (k.clone(), proto_value_to_json(v)))
            .collect();
        obj.insert("@values".into(), serde_json::Value::Object(values));
    }
    serde_json::Value::Object(obj)
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
        // JSON-LD value objects, so the tag / datatype survives a round trip.
        Some(Kind::LangText(l)) => serde_json::json!({ "@value": l.text, "@language": l.lang }),
        Some(Kind::Typed(t)) => serde_json::json!({ "@value": t.lexical, "@type": t.datatype }),
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
        // JSON-LD value objects: {"@value", "@language"} / {"@value", "@type"}.
        serde_json::Value::Object(o) => match (
            o.get("@value").and_then(|v| v.as_str()),
            o.get("@language").and_then(|v| v.as_str()),
            o.get("@type").and_then(|v| v.as_str()),
        ) {
            (Some(text), Some(lang), _) if !lang.is_empty() => Kind::LangText(proto::LangText {
                text: text.to_string(),
                lang: lang.to_string(),
            }),
            (Some(lexical), None, Some(datatype)) if !datatype.is_empty() => {
                Kind::Typed(proto::TypedLiteral {
                    lexical: lexical.to_string(),
                    datatype: datatype.to_string(),
                })
            }
            _ => Kind::TextVal(v.to_string()),
        },
        other => Kind::TextVal(other.to_string()),
    };
    proto::Value { kind: Some(kind) }
}

fn uuid_string_to_node_id(s: &str) -> Result<proto::NodeId, String> {
    Uuid::parse_str(s)
        .map(|u| proto::NodeId {
            bytes: u.as_bytes().to_vec(),
        })
        .map_err(|_| format!("invalid UUID: {:?}", s))
}

#[allow(clippy::result_large_err)]
fn parse_patterns(raw: &[String]) -> Result<Vec<proto::VarPattern>, Response> {
    raw.iter()
        .map(|p| parse_pattern(p))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response()
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

    let rules: Vec<proto::DatalogRule> = match body
        .rules
        .iter()
        .map(rule_to_proto)
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response()
        }
    };

    // Resolve user_id: prefer JSON body field, fall back to X-User-Id header.
    let user_id = body
        .user_id
        .clone()
        .or_else(|| {
            headers
                .get("x-user-id")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
        })
        .unwrap_or_default();

    let req = proto::QueryRequest {
        patterns,
        rules,
        snapshot_ts: 0,
        as_of_valid_time: body.as_of_valid_time.unwrap_or(0),
        as_of_tx_time: body.as_of_tx_time.unwrap_or(0),
        tx_id: body.tx_id.unwrap_or_default(),
        user_id: user_id.clone(),
        params: std::collections::HashMap::new(),
        graphs: body.graphs.clone(),
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
        .map(|b| query_row_json(b.vars, b.values))
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
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response()
        }
    };
    let object = match uuid_string_to_node_id(&body.object) {
        Ok(n) => n,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response()
        }
    };

    let properties: Vec<proto::EdgeProperty> = match body
        .properties
        .iter()
        .map(edge_property_to_proto)
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(p) => p,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e })),
            )
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
            properties,
            object_iri: String::new(),
        })),
    };

    let mut client = state.client.clone();
    match client
        .insert(tonic::Request::new(proto::InsertRequest {
            triples: vec![triple],
            tx_id: body.tx_id.unwrap_or_default(),
            graph: body.graph.unwrap_or_default(),
            ..Default::default()
        }))
        .await
    {
        Ok(r) => {
            let inner = r.into_inner();
            // Return the edge_id UUID so clients can query edge properties later.
            let edge_id = inner.edge_ids.first().filter(|b| b.len() == 16).map(|b| {
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
// The pattern's predicate_var is set to "__p" so the actual matched predicate
// string comes back on each binding, regardless of whether a predicate filter
// was supplied.

async fn handle_triples(
    State(state): State<Arc<AppState>>,
    QueryParams(params): QueryParams<TripleQueryParams>,
) -> Response {
    let subject_term = match &params.subject {
        Some(s) => match uuid_string_to_node_id(s) {
            Ok(nid) => proto::Term {
                kind: Some(proto::term::Kind::Bound(nid)),
            },
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": e })),
                )
                    .into_response()
            }
        },
        None => proto::Term {
            kind: Some(proto::term::Kind::Var("__s".to_string())),
        },
    };

    let object_term = match &params.object {
        Some(o) => match uuid_string_to_node_id(o) {
            Ok(nid) => proto::Term {
                kind: Some(proto::term::Kind::Bound(nid)),
            },
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": e })),
                )
                    .into_response()
            }
        },
        None => proto::Term {
            kind: Some(proto::term::Kind::Var("__o".to_string())),
        },
    };

    let predicate_filter = params.predicate.clone().unwrap_or_default();

    let req = proto::QueryRequest {
        patterns: vec![proto::VarPattern {
            subject: Some(subject_term),
            predicate: predicate_filter.clone(),
            object: Some(object_term),
            predicate_var: "__p".to_string(),
            graph: None,
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

    let triples: Vec<TripleJson> = node_rows(resp.bindings)
        .map(|b| TripleJson {
            subject: params.subject.clone().unwrap_or_else(|| {
                b.vars
                    .get("__s")
                    .map(node_id_to_uuid_string)
                    .unwrap_or_default()
            }),
            predicate: b
                .predicates
                .get("__p")
                .cloned()
                .unwrap_or_else(|| predicate_filter.clone()),
            object: params.object.clone().unwrap_or_else(|| {
                b.vars
                    .get("__o")
                    .map(node_id_to_uuid_string)
                    .unwrap_or_default()
            }),
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
                .map(|vr| {
                    serde_json::json!({
                        "id": vr.node_id.as_ref().map(node_id_to_uuid_string).unwrap_or_default(),
                        "score": vr.similarity,
                    })
                })
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
            let mode = if inner.is_replica {
                "replica"
            } else {
                "primary"
            };
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
    /// Named query parameters for `$param` substitution.
    /// Values must be JSON-encoded `Value` objects (e.g. `"\"Alice\""` for a string).
    #[serde(default)]
    params: std::collections::HashMap<String, String>,
    /// Dataset: graph IRIs the MATCH reads (empty = every graph).
    #[serde(default)]
    graphs: Vec<String>,
}

#[derive(Deserialize)]
struct CypherWriteBody {
    cypher: String,
    /// Open transaction ID to buffer writes into instead of auto-committing.
    #[serde(default)]
    tx_id: Option<String>,
    /// Graph to write to and MATCH in (as `USE GRAPH`).
    #[serde(default)]
    graph: String,
}

async fn handle_cypher(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Json(body): Json<CypherBody>,
) -> Response {
    let user_id = body
        .user_id
        .clone()
        .or_else(|| {
            headers
                .get("x-user-id")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
        })
        .unwrap_or_default();

    let req = proto::CypherQueryRequest {
        cypher: body.cypher,
        as_of_valid_time: body.as_of_valid_time.unwrap_or(0),
        as_of_tx_time: body.as_of_tx_time.unwrap_or(0),
        vector: body.vector,
        ef: body.ef,
        tx_id: body.tx_id.unwrap_or_default(),
        user_id: user_id.clone(),
        params: body.params,
        graphs: body.graphs,
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

/// `POST /cypher/write` — **deprecated** (removed next release): responses
/// carry `Deprecation: true` and a `Warning` header. Write with
/// `POST /changes` or `POST /sparql/update` instead.
async fn handle_cypher_write(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CypherWriteBody>,
) -> Response {
    let mut response = cypher_write(state, body).await;
    let headers = response.headers_mut();
    headers.insert("deprecation", axum::http::HeaderValue::from_static("true"));
    headers.insert(
        axum::http::header::WARNING,
        axum::http::HeaderValue::from_static(
            "299 polargraph \"POST /cypher/write is deprecated; use POST /changes or POST /sparql/update\"",
        ),
    );
    response
}

async fn cypher_write(state: Arc<AppState>, body: CypherWriteBody) -> Response {
    let req = proto::CypherWriteRequest {
        user_id: String::new(),
        cypher: body.cypher,
        tx_id: body.tx_id.unwrap_or_default(),
        graph: body.graph,
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

    let rules: Vec<proto::DatalogRule> = match body
        .rules
        .iter()
        .map(rule_to_proto)
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response()
        }
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
        params: body.params,
        graphs: body.graphs,
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
                        let row = query_row_json(result.vars, result.values);
                        let mut line =
                            serde_json::to_string(&row).unwrap_or_else(|_| "{}".to_string());
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

// ── POST /changes (atomic changeset) ──────────────────────────────────────────

/// A quad in a changeset: `object` (node) or `value` (literal, the usual JSON
/// value encoding). Nodes are UUIDs or IRIs.
#[derive(Deserialize)]
struct ChangeQuadJson {
    subject: String,
    predicate: String,
    #[serde(default)]
    object: Option<String>,
    #[serde(default)]
    value: Option<serde_json::Value>,
    /// Graph IRI (retractions; omitted = default graph).
    #[serde(default)]
    graph: String,
}

#[derive(Deserialize)]
struct ChangeGroupJson {
    #[serde(default)]
    graph: String,
    triples: Vec<ChangeQuadJson>,
}

#[derive(Deserialize)]
struct ChangesBody {
    #[serde(default)]
    adds: Vec<ChangeGroupJson>,
    #[serde(default)]
    retractions: Vec<ChangeQuadJson>,
    #[serde(default)]
    read_ts: i64,
    #[serde(default)]
    strict: bool,
}

/// A node given as a UUID or an IRI (recorded for the IRI dictionary).
fn change_node(s: &str, iris: &mut Vec<String>) -> proto::NodeId {
    match Uuid::parse_str(s) {
        Ok(u) => proto::NodeId {
            bytes: u.as_bytes().to_vec(),
        },
        Err(_) => {
            iris.push(s.to_string());
            pg_node_id_to_proto(iri_to_node_id(s))
        }
    }
}

/// `POST /changes` — apply adds and retractions across graphs atomically
/// (`ApplyChanges`). Body: `{adds: [{graph, triples: [{subject, predicate,
/// object | value}]}], retractions: [{subject, predicate, object | value,
/// graph}], read_ts, strict}`. A precondition failure is HTTP 409. An add's
/// `object` may name a node by IRI, `prefix:local` or bare vocabulary name
/// (`{"predicate": "rdf-type-iri", "object": "Person"}`); subjects and
/// retraction objects are UUIDs or full IRIs.
async fn handle_apply_changes(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ChangesBody>,
) -> Response {
    let mut iris = Vec::new();
    let bad = |msg: &str| {
        (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": msg })),
        )
            .into_response()
    };
    let mut adds = Vec::new();
    for group in body.adds {
        let mut triples = Vec::new();
        for t in group.triples {
            match change_triple(t, &mut iris) {
                Some(t) => triples.push(t),
                None => return bad("each triple needs exactly one of object or value"),
            }
        }
        adds.push(proto::GraphTriples {
            graph: group.graph,
            triples,
        });
    }
    let mut retractions = Vec::new();
    for r in body.retractions {
        match change_quad_ref(r, &mut iris) {
            Some(q) => retractions.push(q),
            None => return bad("each retraction needs exactly one of object or value"),
        }
    }
    iris.sort();
    iris.dedup();
    let req = proto::ApplyChangesRequest {
        adds,
        retractions,
        read_ts: body.read_ts,
        strict: body.strict,
        iris,
        user_id: String::new(),
    };
    match state
        .client
        .clone()
        .apply_changes(tonic::Request::new(req))
        .await
    {
        Ok(r) => {
            let r = r.into_inner();
            let edge_ids: Vec<String> = r
                .edge_ids
                .iter()
                .filter_map(|b| <[u8; 16]>::try_from(b.as_slice()).ok())
                .map(|a| Uuid::from_bytes(a).to_string())
                .collect();
            Json(serde_json::json!({
                "commit_ts": r.commit_ts,
                "added": r.added,
                "retracted": r.retracted,
                "retractions_not_found": r.retractions_not_found,
                "edge_ids": edge_ids,
            }))
            .into_response()
        }
        // Something changed since read_ts: a conflict.
        Err(e) if e.code() == tonic::Code::Aborted => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": e.message() })),
        )
            .into_response(),
        // strict: a retraction matched nothing.
        Err(e) if e.code() == tonic::Code::FailedPrecondition => (
            StatusCode::PRECONDITION_FAILED,
            Json(serde_json::json!({ "error": e.message() })),
        )
            .into_response(),
        Err(e) => grpc_error(e),
    }
}

// ── POST /validate (SHACL) ────────────────────────────────────────────────────

#[derive(Deserialize, Default)]
struct OverlayJson {
    #[serde(default)]
    adds: Vec<ChangeGroupJson>,
    #[serde(default)]
    retractions: Vec<ChangeQuadJson>,
}

#[derive(Deserialize)]
struct ValidateBody {
    shapes_graphs: Vec<String>,
    #[serde(default)]
    data_graphs: Vec<String>,
    #[serde(default)]
    overlay: OverlayJson,
    #[serde(default)]
    read_ts: i64,
    #[serde(default)]
    all_focus_nodes: bool,
}

/// `POST /validate` — SHACL validation (`ValidateShapes`). Body:
/// `{shapes_graphs, data_graphs, overlay: {adds, retractions}, read_ts,
/// all_focus_nodes}` (overlay entries as in `POST /changes`). JSON report by
/// default; `Accept: text/turtle` returns an `sh:ValidationReport`.
async fn handle_validate(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Json(body): Json<ValidateBody>,
) -> Response {
    let mut iris = Vec::new();
    let mut overlay_adds = Vec::new();
    for group in body.overlay.adds {
        let mut triples = Vec::new();
        for t in group.triples {
            match change_triple(t, &mut iris) {
                Some(t) => triples.push(t),
                None => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(serde_json::json!({
                            "error": "each triple needs exactly one of object or value"
                        })),
                    )
                        .into_response()
                }
            }
        }
        overlay_adds.push(proto::GraphTriples {
            graph: group.graph,
            triples,
        });
    }
    let mut overlay_retractions = Vec::new();
    for r in body.overlay.retractions {
        match change_quad_ref(r, &mut iris) {
            Some(q) => overlay_retractions.push(q),
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": "each retraction needs exactly one of object or value"
                    })),
                )
                    .into_response()
            }
        }
    }
    let req = proto::ValidateShapesRequest {
        shapes_graphs: body.shapes_graphs,
        data_graphs: body.data_graphs,
        overlay_adds,
        overlay_retractions,
        read_ts: body.read_ts,
        user_id: String::new(),
        all_focus_nodes: body.all_focus_nodes,
    };
    let resp = match state
        .client
        .clone()
        .validate_shapes(tonic::Request::new(req))
        .await
    {
        Ok(r) => r.into_inner(),
        Err(e) => return grpc_error(e),
    };
    let wants_turtle = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|a| a.contains("text/turtle"));
    if wants_turtle {
        return axum::response::Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/turtle")
            .body(axum::body::boxed(axum::body::Full::from(
                validation_report_turtle(&resp),
            )))
            .unwrap();
    }
    let results: Vec<serde_json::Value> = resp
        .results
        .iter()
        .map(|r| {
            let mut o = serde_json::json!({
                "path": r.path,
                "source_shape": r.source_shape,
                "constraint_component": r.constraint_component,
                "severity": r.severity,
                "message": r.message,
            });
            match &r.focus_literal {
                Some(v) => o["focus_literal"] = proto_value_to_json(v),
                None => o["focus_node"] = serde_json::json!(r.focus_node),
            }
            if let Some(v) = &r.value_literal {
                o["value_literal"] = proto_value_to_json(v);
            } else if !r.value_node.is_empty() {
                o["value_node"] = serde_json::json!(r.value_node);
            }
            o
        })
        .collect();
    Json(serde_json::json!({
        "conforms": resp.conforms,
        "no_violations": resp.no_violations,
        "results": results,
    }))
    .into_response()
}

/// A changeset triple (`object` or `value`) as a proto triple.
fn change_triple(t: ChangeQuadJson, iris: &mut Vec<String>) -> Option<proto::Triple> {
    let subject = Some(change_node(&t.subject, iris));
    let kind = match (t.object, t.value) {
        // A non-UUID object is named by IRI, `prefix:local` or a bare
        // vocabulary name; the server resolves it and records the IRI.
        (Some(o), None) => {
            let (object, object_iri) = match Uuid::parse_str(&o) {
                Ok(u) => (
                    Some(proto::NodeId {
                        bytes: u.as_bytes().to_vec(),
                    }),
                    String::new(),
                ),
                Err(_) => (None, o),
            };
            proto::triple::Kind::Relation(proto::RelationTriple {
                subject,
                predicate: t.predicate,
                object,
                vt_start: 0,
                vt_end: i64::MAX,
                object_iri,
                properties: vec![],
            })
        }
        (None, Some(v)) => proto::triple::Kind::Property(proto::PropertyTriple {
            subject,
            predicate: t.predicate,
            value: Some(json_to_proto_value(&v)),
            vt_start: 0,
            vt_end: i64::MAX,
            mode: proto::PropertyWriteMode::Auto as i32,
        }),
        _ => return None,
    };
    Some(proto::Triple { kind: Some(kind) })
}

/// A changeset retraction as a `QuadRef`.
fn change_quad_ref(r: ChangeQuadJson, iris: &mut Vec<String>) -> Option<proto::QuadRef> {
    let object = match (r.object, r.value) {
        (Some(o), None) => proto::quad_ref::Object::Node(change_node(&o, iris)),
        (None, Some(v)) => proto::quad_ref::Object::Value(json_to_proto_value(&v)),
        _ => return None,
    };
    Some(proto::QuadRef {
        subject: Some(change_node(&r.subject, iris)),
        predicate: r.predicate,
        object: Some(object),
        graph: r.graph,
    })
}

/// A SHACL path string (`<p>`, `^<p>`, `<a>/<b>`) as a Turtle path term.
fn path_turtle(path: &str) -> String {
    let mut steps = Vec::new();
    let mut rest = path;
    while let Some(start) = rest.find('<') {
        let inverse = rest[..start].contains('^');
        let Some(end) = rest[start..].find('>') else {
            break;
        };
        let iri = &rest[start..start + end + 1];
        steps.push(if inverse {
            format!("[ sh:inversePath {iri} ]")
        } else {
            iri.to_string()
        });
        rest = &rest[start + end + 1..];
    }
    match steps.len() {
        0 => String::new(),
        1 => steps.remove(0),
        _ => format!("( {} )", steps.join(" ")),
    }
}

/// The report as an `sh:ValidationReport` in Turtle.
fn validation_report_turtle(resp: &proto::ValidateShapesResponse) -> String {
    let term = |node: &str, lit: &Option<proto::Value>| match lit {
        Some(v) => proto_value_to_pg(v)
            .map(|v| polargraph_sparql::value_to_nt_literal(&v))
            .unwrap_or_else(|| "\"\"".into()),
        None => format!("<{node}>"),
    };
    let mut out = String::from("@prefix sh: <http://www.w3.org/ns/shacl#> .\n\n");
    out.push_str(&format!(
        "[] a sh:ValidationReport ;\n    sh:conforms {}",
        resp.conforms
    ));
    for r in &resp.results {
        out.push_str(" ;\n    sh:result [\n        a sh:ValidationResult ;\n");
        out.push_str(&format!(
            "        sh:focusNode {} ;\n",
            term(&r.focus_node, &r.focus_literal)
        ));
        if !r.path.is_empty() {
            out.push_str(&format!(
                "        sh:resultPath {} ;\n",
                path_turtle(&r.path)
            ));
        }
        if r.value_literal.is_some() || !r.value_node.is_empty() {
            out.push_str(&format!(
                "        sh:value {} ;\n",
                term(&r.value_node, &r.value_literal)
            ));
        }
        out.push_str(&format!(
            "        sh:sourceShape <{}> ;\n        sh:sourceConstraintComponent <{}> ;\n        sh:resultSeverity <{}> ;\n        sh:resultMessage {}\n    ]",
            r.source_shape,
            r.constraint_component,
            r.severity,
            serde_json::to_string(&r.message).unwrap_or_else(|_| "\"\"".into())
        ));
    }
    out.push_str(" .\n");
    out
}

// ── GET /subscribe (Server-Sent Events) ───────────────────────────────────────

#[derive(Deserialize)]
struct SubscribeParams {
    /// Comma-separated graph IRIs ("default" = the default graph).
    #[serde(default)]
    graphs: String,
    /// Comma-separated predicates.
    #[serde(default)]
    predicates: String,
    /// Comma-separated `__type` names (matched against current types).
    #[serde(default)]
    types: String,
    /// Resume after this commit timestamp (also `Last-Event-ID`).
    resume_after: Option<i64>,
    #[serde(default)]
    include_values: bool,
}

fn split_list(s: &str) -> Vec<String> {
    s.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

/// `GET /subscribe` — the change feed as Server-Sent Events. Each event has
/// `id:` = commit timestamp (so `Last-Event-ID` resumes), `event:` = the
/// change kind, and a JSON `data:` line. A resume point older than the
/// retained log is HTTP 410. Comment lines keep idle connections alive.
async fn handle_subscribe(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    QueryParams(params): QueryParams<SubscribeParams>,
) -> Response {
    let resume_after_ts = params.resume_after.unwrap_or_else(|| {
        headers
            .get("last-event-id")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0)
    });
    let req = proto::SubscribeRequest {
        graphs: split_list(&params.graphs)
            .into_iter()
            .map(|g| if g == "default" { String::new() } else { g })
            .collect(),
        predicates: split_list(&params.predicates),
        types: split_list(&params.types),
        resume_after_ts,
        include_values: params.include_values,
        user_id: String::new(),
    };
    let mut client = state.client.clone();
    let mut stream = match client.subscribe(tonic::Request::new(req)).await {
        Ok(r) => r.into_inner(),
        Err(e) => return grpc_error(e),
    };

    let (mut body_tx, body) = hyper::Body::channel();
    tokio::spawn(async move {
        let mut keepalive = tokio::time::interval(std::time::Duration::from_secs(15));
        keepalive.tick().await;
        loop {
            let event = tokio::select! {
                msg = stream.message() => match msg {
                    Ok(Some(event)) => event,
                    Ok(None) => return,
                    Err(e) => {
                        let frame = format!(
                            "event: error\ndata: {}\n\n",
                            serde_json::json!({ "error": e.message() })
                        );
                        let _ = body_tx.send_data(bytes::Bytes::from(frame)).await;
                        return;
                    }
                },
                _ = keepalive.tick() => {
                    if body_tx.send_data(bytes::Bytes::from_static(b": keep-alive\n\n")).await.is_err() {
                        return;
                    }
                    continue;
                }
            };
            let names = resolve_names(&mut client, change_event_nodes(&event), false).await;
            let frame = format!(
                "id: {}\nevent: {}\ndata: {}\n\n",
                event.commit_ts,
                change_kind_name(event.kind),
                change_event_json(&event, &names)
            );
            if body_tx.send_data(bytes::Bytes::from(frame)).await.is_err() {
                return;
            }
        }
    });

    axum::response::Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .body(axum::body::boxed(body))
        .unwrap()
}

fn change_kind_name(kind: i32) -> &'static str {
    match proto::ChangeKind::try_from(kind).unwrap_or(proto::ChangeKind::Unspecified) {
        proto::ChangeKind::Assert => "assert",
        proto::ChangeKind::Close => "close",
        proto::ChangeKind::GraphCreated => "graph_created",
        proto::ChangeKind::GraphDropped => "graph_dropped",
        proto::ChangeKind::GraphCopied => "graph_copied",
        proto::ChangeKind::Unspecified => "unknown",
    }
}

/// Nodes named by an event's quad (for IRI resolution).
fn change_event_nodes(event: &proto::ChangeEvent) -> Vec<NodeId> {
    use proto::triple::Kind;
    let mut ids = Vec::new();
    match event.quad.as_ref().and_then(|q| q.kind.as_ref()) {
        Some(Kind::Relation(r)) => {
            ids.extend(r.subject.as_ref().and_then(proto_node_id));
            ids.extend(r.object.as_ref().and_then(proto_node_id));
        }
        Some(Kind::Property(p)) => ids.extend(p.subject.as_ref().and_then(proto_node_id)),
        None => {}
    }
    ids
}

/// The JSON `data:` payload of a change event; nodes rendered as IRIs.
fn change_event_json(
    event: &proto::ChangeEvent,
    names: &polargraph_sparql::IriNames,
) -> serde_json::Value {
    use proto::triple::Kind;
    let node = |n: &Option<proto::NodeId>| {
        n.as_ref()
            .and_then(proto_node_id)
            .map(|id| serde_json::Value::String(names.iri(&id)))
            .unwrap_or(serde_json::Value::Null)
    };
    let mut obj = serde_json::json!({
        "commit_ts": event.commit_ts,
        "kind": change_kind_name(event.kind),
        "graph": event.graph,
        "author": event.author,
    });
    if !event.source_graph.is_empty() {
        obj["source_graph"] = serde_json::json!(event.source_graph);
    }
    match event.quad.as_ref().and_then(|q| q.kind.as_ref()) {
        Some(Kind::Relation(r)) => {
            obj["subject"] = node(&r.subject);
            obj["predicate"] = serde_json::json!(r.predicate);
            obj["object"] = node(&r.object);
            obj["vt_start"] = serde_json::json!(r.vt_start);
            obj["vt_end"] = serde_json::json!(r.vt_end);
            if let Ok(arr) = <[u8; 16]>::try_from(event.edge_id.as_slice()) {
                obj["edge_id"] = serde_json::json!(Uuid::from_bytes(arr).to_string());
            }
        }
        Some(Kind::Property(p)) => {
            obj["subject"] = node(&p.subject);
            obj["predicate"] = serde_json::json!(p.predicate);
            if let Some(v) = &p.value {
                obj["value"] = proto_value_to_json(v);
            }
            obj["vt_start"] = serde_json::json!(p.vt_start);
            obj["vt_end"] = serde_json::json!(p.vt_end);
        }
        None => {}
    }
    obj
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
    let rules: Vec<proto::DatalogRule> = match body
        .rules
        .iter()
        .map(rule_to_proto)
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response()
        }
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
                .map(|n| {
                    serde_json::json!({
                        "node_type": n.node_type,
                        "description": n.description,
                        "index_used": n.index_used,
                    })
                })
                .collect();
            Json(serde_json::json!({ "plan_text": er.plan_text, "nodes": nodes })).into_response()
        }
        Err(e) => grpc_error(e),
    }
}

// ── GET /indexes ──────────────────────────────────────────────────────────────

async fn handle_indexes(State(state): State<Arc<AppState>>) -> Response {
    let mut client = state.client.clone();
    match client
        .show_indexes(tonic::Request::new(proto::ShowIndexesRequest {}))
        .await
    {
        Ok(r) => {
            let resp = r.into_inner();
            let cfs: Vec<serde_json::Value> = resp
                .column_families
                .into_iter()
                .map(|cf| {
                    serde_json::json!({
                        "name": cf.name,
                        "approx_key_count": cf.approx_key_count,
                        "approx_size_bytes": cf.approx_size_bytes,
                    })
                })
                .collect();
            let spaces: Vec<serde_json::Value> = resp
                .vector_spaces
                .into_iter()
                .map(|vs| {
                    serde_json::json!({
                        "name": vs.name,
                        "dimensions": vs.dimensions,
                        "node_count": vs.node_count,
                        "storage_mode": vs.storage_mode,
                    })
                })
                .collect();
            Json(serde_json::json!({
                "column_families": cfs,
                "vector_spaces": spaces,
                "predicate_count": resp.predicate_count,
            }))
            .into_response()
        }
        Err(e) => grpc_error(e),
    }
}

// ── POST /tx/begin ────────────────────────────────────────────────────────────

async fn handle_tx_begin(State(state): State<Arc<AppState>>) -> Response {
    let mut client = state.client.clone();
    match client
        .begin_transaction(tonic::Request::new(proto::BeginTransactionRequest {}))
        .await
    {
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
    edge_id: String,
    predicate: String,
    node_id: Option<String>,
    scalar: Option<serde_json::Value>,
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
            Err(_) => return (
                StatusCode::BAD_REQUEST,
                Json(
                    serde_json::json!({ "error": format!("invalid edge_id UUID: {}", a.edge_id) }),
                ),
            )
                .into_response(),
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
            Some(proto::edge_annotation::Value::NodeId(
                node_uuid.as_bytes().to_vec(),
            ))
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
            Json(serde_json::json!({ "ok": true, "commit_ts": inner.commit_ts })).into_response()
        }
        Err(e) => grpc_error(e),
    }
}

// ── GET /edge-annotations/:edge_id ───────────────────────────────────────────

async fn handle_get_edge_annotations(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(edge_id): axum::extract::Path<String>,
) -> Response {
    let edge_uuid =
        match Uuid::parse_str(&edge_id) {
            Ok(u) => u,
            Err(_) => return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": format!("invalid edge_id UUID: {}", edge_id) })),
            )
                .into_response(),
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

// ── GET /property-history ─────────────────────────────────────────────────────

#[derive(Deserialize)]
struct PropertyHistoryParams {
    subject: String,
    predicate: String,
    limit: Option<u32>,
}

async fn handle_property_history(
    State(state): State<Arc<AppState>>,
    QueryParams(params): QueryParams<PropertyHistoryParams>,
) -> Response {
    let subject_uuid = match Uuid::parse_str(&params.subject) {
        Ok(u) => u,
        Err(_) => return (
            StatusCode::BAD_REQUEST,
            Json(
                serde_json::json!({ "error": format!("invalid subject UUID: {}", params.subject) }),
            ),
        )
            .into_response(),
    };

    let req = proto::GetPropertyHistoryRequest {
        subject_id: subject_uuid.as_bytes().to_vec(),
        predicate: params.predicate,
        limit: params.limit.unwrap_or(0),
    };

    let mut client = state.client.clone();
    match client.get_property_history(tonic::Request::new(req)).await {
        Ok(r) => {
            let versions: Vec<serde_json::Value> = r
                .into_inner()
                .versions
                .into_iter()
                .map(|v| {
                    let value: serde_json::Value =
                        serde_json::from_str(&v.value_json).unwrap_or(serde_json::Value::Null);
                    serde_json::json!({
                        "value": value,
                        "transaction_time": v.transaction_time,
                    })
                })
                .collect();
            Json(serde_json::json!({ "versions": versions })).into_response()
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
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response()
        }
    };

    let target = if let Some(ref nid_str) = body.node_id {
        match uuid_string_to_node_id(nid_str) {
            Ok(n) => proto::grant_access_request::Target::NodeId(n.bytes),
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": e })),
                )
                    .into_response()
            }
        }
    } else if let Some(ref tn) = body.type_name {
        proto::grant_access_request::Target::TypeName(tn.clone())
    } else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "node_id or type_name must be set" })),
        )
            .into_response();
    };

    let req = proto::GrantAccessRequest {
        group_id,
        target: Some(target),
    };
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
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response()
        }
    };

    let target = if let Some(ref nid_str) = body.node_id {
        match uuid_string_to_node_id(nid_str) {
            Ok(n) => proto::revoke_access_request::Target::NodeId(n.bytes),
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": e })),
                )
                    .into_response()
            }
        }
    } else if let Some(ref tn) = body.type_name {
        proto::revoke_access_request::Target::TypeName(tn.clone())
    } else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "node_id or type_name must be set" })),
        )
            .into_response();
    };

    let req = proto::RevokeAccessRequest {
        group_id,
        target: Some(target),
    };
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
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response()
        }
    };
    let group_id = match uuid_string_to_node_id(&body.group_id) {
        Ok(n) => n.bytes,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response()
        }
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
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response()
        }
    };

    let req = proto::GetUserAccessRequest { user_id };
    let mut client = state.client.clone();
    match client.get_user_access(tonic::Request::new(req)).await {
        Ok(r) => {
            let inner = r.into_inner();
            let node_ids: Vec<String> = inner
                .node_ids
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
            }))
            .into_response()
        }
        Err(e) => grpc_error(e),
    }
}

// ── GET /sparql and POST /sparql ──────────────────────────────────────────────

#[derive(Deserialize)]
struct SparqlGetParams {
    query: String,
}

async fn handle_sparql_get(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    axum::extract::RawQuery(raw): axum::extract::RawQuery,
    QueryParams(params): QueryParams<SparqlGetParams>,
) -> Response {
    let dataset = polargraph_sparql::protocol::dataset_from_params(&[raw.as_deref().unwrap_or("")]);
    execute_sparql_query(state, headers, params.query, dataset).await
}

async fn handle_sparql_post(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    axum::extract::RawQuery(raw): axum::extract::RawQuery,
    body: axum::body::Bytes,
) -> Response {
    let form_body = if headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.contains("application/x-www-form-urlencoded"))
    {
        String::from_utf8_lossy(&body).into_owned()
    } else {
        String::new()
    };
    let dataset = polargraph_sparql::protocol::dataset_from_params(&[
        raw.as_deref().unwrap_or(""),
        &form_body,
    ]);
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let query_string = if content_type.contains("application/sparql-query") {
        match String::from_utf8(body.to_vec()) {
            Ok(s) => s,
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": "invalid UTF-8 body" })),
                )
                    .into_response()
            }
        }
    } else if content_type.contains("application/x-www-form-urlencoded") {
        let form_str = match String::from_utf8(body.to_vec()) {
            Ok(s) => s,
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": "invalid UTF-8 body" })),
                )
                    .into_response()
            }
        };
        match polargraph_sparql::protocol::extract_query_from_form(&form_str) {
            Some(q) => q,
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": "missing query= field in form body" })),
                )
                    .into_response()
            }
        }
    } else {
        // Default: treat body as raw SPARQL
        match String::from_utf8(body.to_vec()) {
            Ok(s) => s,
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": "invalid UTF-8 body" })),
                )
                    .into_response()
            }
        }
    };

    execute_sparql_query(state, headers, query_string, dataset).await
}

// ── SPARQL-star runtime execution helpers ────────────────────────────────────

fn sparql_annotation_to_value(
    ann_value: &Option<proto::edge_annotation::Value>,
) -> Option<polargraph_sparql::SparqlValue> {
    use polargraph_sparql::SparqlValue;
    match ann_value {
        Some(proto::edge_annotation::Value::NodeId(bytes)) if bytes.len() == 16 => {
            let arr: [u8; 16] = bytes[..16].try_into().ok()?;
            Some(SparqlValue::Uri(NodeId(uuid::Uuid::from_bytes(arr))))
        }
        Some(proto::edge_annotation::Value::Scalar(v)) => match &v.kind {
            Some(proto::value::Kind::TextVal(s)) => Some(SparqlValue::Literal(s.clone())),
            Some(proto::value::Kind::IntVal(n)) => Some(SparqlValue::LiteralInt(*n)),
            Some(proto::value::Kind::FloatVal(f)) => Some(SparqlValue::LiteralFloat(*f)),
            Some(proto::value::Kind::BoolVal(b)) => Some(SparqlValue::LiteralBool(*b)),
            _ => None,
        },
        _ => None,
    }
}

/// Look up the edge UUID(s) for a specific (S, P, O) triple via the gRPC service.
async fn resolve_edge_ids(
    client: &mut GrpcClient,
    subject_bytes: Vec<u8>,
    predicate: &str,
    object_bytes: Vec<u8>,
) -> Vec<Vec<u8>> {
    let req = proto::GetEdgeIdsByTripleRequest {
        subject_id: subject_bytes,
        predicate: predicate.to_string(),
        object_id: object_bytes,
    };
    match client
        .get_edge_ids_by_triple(tonic::Request::new(req))
        .await
    {
        Ok(r) => r.into_inner().edge_ids,
        Err(_) => vec![],
    }
}

/// Execute subject-position SPARQL-star annotation steps.
///
/// For each binding in `bindings` and each `EdgeAnnotationStep`, resolves the embedded
/// triple to an edge ID and fetches annotations, extending the binding with the annotation value.
async fn execute_annotation_steps(
    client: &mut GrpcClient,
    bindings: Vec<polargraph_sparql::SparqlBindings>,
    steps: &[polargraph_sparql::EdgeAnnotationStep],
) -> Vec<polargraph_sparql::SparqlBindings> {
    use polargraph_sparql::SparqlValue;

    let mut result = Vec::new();

    // When there are no input bindings but we have bound-subject steps, seed with one empty binding.
    let seed: Vec<polargraph_sparql::SparqlBindings> = if bindings.is_empty() {
        vec![std::collections::HashMap::new()]
    } else {
        bindings
    };

    for binding in seed {
        let mut extended = vec![binding.clone()];

        for step in steps {
            let mut next = Vec::new();
            for b in &extended {
                // Resolve subject and object terms (may be variable or bound).
                let subj_bytes = match &step.edge_subject {
                    polargraph_query::Term::Bound(id) => id.0.as_bytes().to_vec(),
                    polargraph_query::Term::Var(v) => {
                        if let Some(SparqlValue::Uri(id)) = b.get(v.as_str()) {
                            id.0.as_bytes().to_vec()
                        } else {
                            continue;
                        }
                    }
                    _ => continue,
                };
                let obj_bytes = match &step.edge_object {
                    polargraph_query::Term::Bound(id) => id.0.as_bytes().to_vec(),
                    polargraph_query::Term::Var(v) => {
                        if let Some(SparqlValue::Uri(id)) = b.get(v.as_str()) {
                            id.0.as_bytes().to_vec()
                        } else {
                            continue;
                        }
                    }
                    _ => continue,
                };

                let edge_ids =
                    resolve_edge_ids(client, subj_bytes, &step.edge_predicate, obj_bytes).await;

                for edge_id_bytes in &edge_ids {
                    let ann_req = proto::GetEdgeAnnotationsRequest {
                        edge_id: edge_id_bytes.clone(),
                    };
                    let annotations = match client
                        .get_edge_annotations(tonic::Request::new(ann_req))
                        .await
                    {
                        Ok(r) => r.into_inner().annotations,
                        Err(_) => continue,
                    };

                    for ann in &annotations {
                        // Filter by annotation predicate (unless variable).
                        let pred_matches = step.annotation_predicate_var.is_some()
                            || ann.predicate == step.annot_predicate;
                        if !pred_matches {
                            continue;
                        }

                        // Extract annotation value as SparqlValue.
                        let val_opt = sparql_annotation_to_value(&ann.value);

                        if let Some(val) = val_opt {
                            let mut new_b = b.clone();
                            new_b.insert(step.value_var.clone(), val);
                            if let Some(pred_var) = &step.annotation_predicate_var {
                                new_b.insert(
                                    pred_var.clone(),
                                    SparqlValue::Literal(ann.predicate.clone()),
                                );
                            }
                            next.push(new_b);
                        }
                    }
                }
            }
            extended = next;
        }
        result.extend(extended);
    }
    result
}

/// Execute object-position SPARQL-star annotation steps.
///
/// Pattern: `?x :annot << S P O >>` — looks up edge by (S, P, O) then returns
/// annotation values for `:annot` as bindings for `?x`.
async fn execute_annotation_object_steps(
    client: &mut GrpcClient,
    bindings: Vec<polargraph_sparql::SparqlBindings>,
    steps: &[polargraph_sparql::EdgeAnnotationObjectStep],
) -> Vec<polargraph_sparql::SparqlBindings> {
    use polargraph_sparql::SparqlValue;

    let seed: Vec<polargraph_sparql::SparqlBindings> = if bindings.is_empty() {
        vec![std::collections::HashMap::new()]
    } else {
        bindings
    };

    let mut result = Vec::new();

    for binding in seed {
        let mut extended = vec![binding.clone()];

        for step in steps {
            let mut next = Vec::new();
            for b in &extended {
                let subj_bytes = match &step.edge_subject {
                    polargraph_query::Term::Bound(id) => id.0.as_bytes().to_vec(),
                    polargraph_query::Term::Var(v) => {
                        if let Some(SparqlValue::Uri(id)) = b.get(v.as_str()) {
                            id.0.as_bytes().to_vec()
                        } else {
                            continue;
                        }
                    }
                    _ => continue,
                };
                let obj_bytes = match &step.edge_object {
                    polargraph_query::Term::Bound(id) => id.0.as_bytes().to_vec(),
                    polargraph_query::Term::Var(v) => {
                        if let Some(SparqlValue::Uri(id)) = b.get(v.as_str()) {
                            id.0.as_bytes().to_vec()
                        } else {
                            continue;
                        }
                    }
                    _ => continue,
                };

                let edge_ids =
                    resolve_edge_ids(client, subj_bytes, &step.edge_predicate, obj_bytes).await;

                for edge_id_bytes in &edge_ids {
                    let ann_req = proto::GetEdgeAnnotationsRequest {
                        edge_id: edge_id_bytes.clone(),
                    };
                    let annotations = match client
                        .get_edge_annotations(tonic::Request::new(ann_req))
                        .await
                    {
                        Ok(r) => r.into_inner().annotations,
                        Err(_) => continue,
                    };

                    for ann in &annotations {
                        if ann.predicate != step.annotation_predicate {
                            continue;
                        }
                        // The annotation VALUE is what we bind to `result_var`.
                        let val_opt: Option<SparqlValue> = sparql_annotation_to_value(&ann.value);

                        if let Some(val) = val_opt {
                            let mut new_b = b.clone();
                            new_b.insert(step.result_var.clone(), val);
                            next.push(new_b);
                        }
                    }
                }
            }
            extended = next;
        }
        result.extend(extended);
    }
    result
}

/// Run a SPARQL query; `protocol_dataset` (`default-graph-uri` /
/// `named-graph-uri`) replaces the query's own `FROM` / `FROM NAMED`.
async fn execute_sparql_query(
    state: Arc<AppState>,
    headers: axum::http::HeaderMap,
    query_string: String,
    protocol_dataset: Option<polargraph_sparql::SparqlDataset>,
) -> Response {
    use polargraph_sparql::response::ResponseFormat;
    use polargraph_sparql::{translate_query, SparqlBindings, SparqlError, SparqlValue};

    // 1. Parse
    let mut parsed = match spargebra::Query::parse(&query_string, None) {
        Ok(q) => q,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": format!("SPARQL parse error: {}", e) })),
            )
                .into_response()
        }
    };
    if let Some(ds) = &protocol_dataset {
        if let Err(e) = polargraph_sparql::protocol::apply_protocol_dataset(&mut parsed, ds) {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response();
        }
    }

    // Dispatch CONSTRUCT / DESCRIBE to the dedicated handler.
    if matches!(
        &parsed,
        spargebra::Query::Construct { .. } | spargebra::Query::Describe { .. }
    ) {
        return execute_sparql_construct(state, headers, parsed).await;
    }

    // 2. Translate to PolarGraph query
    let translation = match translate_query(&parsed) {
        Ok(t) => t,
        Err(SparqlError::Unsupported(msg)) => {
            return (
                StatusCode::NOT_IMPLEMENTED,
                Json(serde_json::json!({ "error": msg })),
            )
                .into_response()
        }
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    };

    // 3. Execute each branch against gRPC; collect as SparqlBindings.
    let mut all_bindings: Vec<SparqlBindings> = Vec::new();

    // Dataset: FROM graphs scope the patterns outside GRAPH; FROM NAMED
    // limits what GRAPH ?g may bind (a graph variable binds the graph IRI's
    // node, so the check is on NodeIds).
    let dataset_graphs: Vec<String> = translation
        .dataset
        .as_ref()
        .map(|d| d.default.clone())
        .unwrap_or_default();
    let named_nodes: Option<std::collections::HashSet<NodeId>> = translation
        .dataset
        .as_ref()
        .and_then(|d| d.named.as_ref())
        .map(|named| named.iter().map(|iri| iri_to_node_id(iri)).collect());
    let graph_vars_ok = |b: &SparqlBindings| match &named_nodes {
        None => true,
        Some(named) => translation.graph_vars.iter().all(|gv| match b.get(gv) {
            Some(SparqlValue::Uri(id)) => named.contains(id),
            _ => true,
        }),
    };

    for branch in &translation.branches {
        let patterns: Vec<proto::VarPattern> =
            branch.patterns.iter().map(sparql_varpat_to_proto).collect();
        let rules: Vec<proto::DatalogRule> =
            branch.rules.iter().map(sparql_rule_to_proto).collect();

        let req = proto::QueryRequest {
            patterns,
            rules,
            graphs: dataset_graphs.clone(),
            ..Default::default()
        };

        let mut client = state.client.clone();
        let resp = match client.query(tonic::Request::new(req)).await {
            Ok(r) => r.into_inner(),
            Err(e) => return grpc_error(e),
        };

        // Convert proto bindings → SparqlBindings.
        let mut branch_bindings: Vec<SparqlBindings> = resp
            .bindings
            .into_iter()
            .filter_map(|pb| {
                let b = sparql_row(pb);
                if graph_vars_ok(&b) && sparql_filter_bindings(&b, &branch.filters) {
                    Some(b)
                } else {
                    None
                }
            })
            .collect();

        // 3a. Handle OPTIONAL branches (left join).
        for opt in &branch.optional_branches {
            let opt_patterns = opt.patterns.iter().map(sparql_varpat_to_proto).collect();
            let opt_rules = opt.rules.iter().map(sparql_rule_to_proto).collect();
            let opt_req = proto::QueryRequest {
                patterns: opt_patterns,
                rules: opt_rules,
                graphs: dataset_graphs.clone(),
                ..Default::default()
            };
            let opt_resp = match client.query(tonic::Request::new(opt_req)).await {
                Ok(r) => r.into_inner(),
                Err(_) => {
                    // Optional branch failed — keep left bindings as-is.
                    continue;
                }
            };
            let right: Vec<SparqlBindings> = opt_resp
                .bindings
                .into_iter()
                .filter_map(|pb| {
                    let b = sparql_row(pb);
                    if graph_vars_ok(&b) && sparql_filter_bindings(&b, &opt.filters) {
                        Some(b)
                    } else {
                        None
                    }
                })
                .collect();
            branch_bindings = polargraph_sparql::execute::left_join(branch_bindings, right, None);
        }

        // 3b. Execute SPARQL-star subject-position annotation steps.
        // Each step: resolve the quoted triple to an edge ID then fetch annotations.
        if !branch.edge_annotation_steps.is_empty() {
            branch_bindings = execute_annotation_steps(
                &mut client,
                branch_bindings,
                &branch.edge_annotation_steps,
            )
            .await;
        }

        // 3c. Execute SPARQL-star object-position annotation steps.
        // Pattern: `?x :annot << S P O >>` — find edge then bind annotation value to ?x.
        if !branch.edge_annotation_object_steps.is_empty() {
            branch_bindings = execute_annotation_object_steps(
                &mut client,
                branch_bindings,
                &branch.edge_annotation_object_steps,
            )
            .await;
        }

        all_bindings.extend(branch_bindings);
    }

    // Names for every node in the result, resolved once (before aggregation,
    // which may fold URIs into GROUP_CONCAT strings).
    let names = resolve_names(
        &mut state.client.clone(),
        polargraph_sparql::node_ids_in_bindings(&all_bindings),
        false,
    )
    .await;

    // 4. GROUP BY / aggregation.
    if !translation.aggregates.is_empty() || !translation.group_by.is_empty() {
        all_bindings = polargraph_sparql::execute::execute_sparql_aggregations(
            all_bindings,
            &translation.group_by,
            &translation.aggregates,
            translation.having_filter.as_ref(),
            &names,
        );
    }

    // 4a. ORDER BY (after aggregation, before projection, so it can sort on
    // variables that aren't projected).
    polargraph_sparql::execute::order_bindings(&mut all_bindings, &translation.order_by, &names);

    // 5. Determine projected variables.
    let all_var_names: Vec<String> = if let Some(proj) = &translation.projection {
        proj.clone()
    } else {
        let mut seen = std::collections::HashSet::new();
        for b in &all_bindings {
            for k in b.keys() {
                seen.insert(k.clone());
            }
        }
        let mut names: Vec<String> = seen.into_iter().collect();
        names.sort();
        names
    };

    // 6. Project.
    let projected: Vec<SparqlBindings> = all_bindings
        .iter()
        .map(|b| {
            all_var_names
                .iter()
                .filter_map(|v| b.get(v).map(|val| (v.clone(), val.clone())))
                .collect()
        })
        .collect();

    // 7. DISTINCT.
    let projected = if translation.distinct {
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        projected
            .into_iter()
            .filter(|b| {
                // Stable key from sorted (var, value) pairs.
                let mut pairs: Vec<_> = b.iter().collect();
                pairs.sort_by_key(|(k, _)| k.as_str());
                let key = pairs
                    .into_iter()
                    .map(|(k, v)| format!("{}={:?}", k, v))
                    .collect::<Vec<_>>()
                    .join("\0");
                seen.insert(key)
            })
            .collect()
    } else {
        projected
    };

    // 8. OFFSET + LIMIT.
    let projected: Vec<SparqlBindings> = projected
        .into_iter()
        .skip(translation.offset)
        .take(translation.limit.unwrap_or(usize::MAX))
        .collect();

    // 9. ASK queries return a boolean.
    if translation.is_ask {
        let result = !projected.is_empty();
        return Json(serde_json::json!({ "head": {}, "boolean": result })).into_response();
    }

    // 10. Serialize.
    let http_headers: http::HeaderMap = headers
        .iter()
        .filter_map(|(k, v)| {
            let name = http::header::HeaderName::from_bytes(k.as_str().as_bytes()).ok()?;
            let val = http::header::HeaderValue::from_bytes(v.as_bytes()).ok()?;
            Some((name, val))
        })
        .collect();
    let format = polargraph_sparql::negotiate_format(&http_headers);
    match format {
        ResponseFormat::Json => {
            let body = polargraph_sparql::serialize_json(&all_var_names, &projected, &names);
            axum::response::Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/sparql-results+json")
                .body(axum::body::boxed(axum::body::Full::from(body)))
                .unwrap()
        }
        ResponseFormat::Csv => {
            let body = polargraph_sparql::serialize_csv(&all_var_names, &projected, &names);
            axum::response::Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/csv")
                .body(axum::body::boxed(axum::body::Full::from(body)))
                .unwrap()
        }
    }
}

fn sparql_varpat_to_proto(vp: &polargraph_query::VarPattern) -> proto::VarPattern {
    proto::VarPattern {
        subject: Some(sparql_term_to_proto(&vp.subject)),
        predicate: vp.predicate.clone().unwrap_or_default(),
        object: Some(sparql_term_to_proto(&vp.object)),
        predicate_var: vp.predicate_var.clone().unwrap_or_default(),
        graph: sparql_graph_to_proto(&vp.graph),
    }
}

/// Proto form of a translated pattern's graph term (`None` = union).
fn sparql_graph_to_proto(graph: &polargraph_query::GraphTerm) -> Option<proto::GraphTerm> {
    use polargraph_query::GraphTerm;
    use proto::graph_term::Kind;
    let kind = match graph {
        GraphTerm::Union => return None,
        GraphTerm::Default => Kind::DefaultGraph(true),
        GraphTerm::Iri(iri) => Kind::Iri(iri.clone()),
        GraphTerm::Var(v) => Kind::Var(v.clone()),
        // The translator only produces the empty set (a graph outside the
        // dataset); GraphIds have no meaning outside the server, so any
        // other id-based term also matches nothing.
        GraphTerm::Bound(_) | GraphTerm::Set(_) => Kind::Set(proto::GraphSet { iris: vec![] }),
    };
    Some(proto::GraphTerm { kind: Some(kind) })
}

fn sparql_term_to_proto(term: &polargraph_query::Term) -> proto::Term {
    use polargraph_query::Term;
    match term {
        Term::Var(v) => proto::Term {
            kind: Some(proto::term::Kind::Var(v.clone())),
        },
        Term::Bound(id) => proto::Term {
            kind: Some(proto::term::Kind::Bound(proto::NodeId {
                bytes: id.0.as_bytes().to_vec(),
            })),
        },
        Term::Literal(v) => proto::Term {
            kind: Some(proto::term::Kind::Literal(pg_value_to_proto(v))),
        },
        Term::Any | Term::Param(_) => proto::Term { kind: None },
    }
}

fn sparql_rule_to_proto(rule: &polargraph_query::Rule) -> proto::DatalogRule {
    proto::DatalogRule {
        head_predicate: rule.head_predicate.clone(),
        head_subject_var: rule.head_subject_var.clone(),
        head_object_var: rule.head_object_var.clone(),
        body: rule.body.iter().map(sparql_varpat_to_proto).collect(),
    }
}

/// Apply all SPARQL post-filters to a single SparqlBindings row.
fn sparql_filter_bindings(
    bindings: &polargraph_sparql::SparqlBindings,
    filters: &[polargraph_sparql::SparqlFilter],
) -> bool {
    filters
        .iter()
        .all(|f| polargraph_sparql::execute::apply_sparql_filter(bindings, f))
}

// ── SPARQL CONSTRUCT / DESCRIBE ───────────────────────────────────────────────

async fn execute_sparql_construct(
    state: Arc<AppState>,
    headers: axum::http::HeaderMap,
    query: spargebra::Query,
) -> Response {
    use polargraph_sparql::{
        node_id_to_iri, serialize_ntriples_star, serialize_turtle_star, translate_construct,
        RdfStarSubject, RdfStarTriple, SparqlValue,
    };
    use std::collections::HashSet;

    let is_describe = matches!(query, spargebra::Query::Describe { .. });

    let ct = match translate_construct(&query) {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    };

    // Dataset handling as in `execute_sparql_query`.
    let dataset_graphs: Vec<String> = ct
        .dataset
        .as_ref()
        .map(|d| d.default.clone())
        .unwrap_or_default();
    let named_nodes: Option<HashSet<NodeId>> = ct
        .dataset
        .as_ref()
        .and_then(|d| d.named.as_ref())
        .map(|named| named.iter().map(|iri| iri_to_node_id(iri)).collect());

    // Execute WHERE clause branches and collect SparqlBindings.
    let mut all_bindings: Vec<polargraph_sparql::SparqlBindings> = Vec::new();
    for branch in &ct.branches {
        let patterns: Vec<proto::VarPattern> =
            branch.patterns.iter().map(sparql_varpat_to_proto).collect();
        let rules: Vec<proto::DatalogRule> =
            branch.rules.iter().map(sparql_rule_to_proto).collect();

        if patterns.is_empty() && rules.is_empty() {
            continue;
        }

        let req = proto::QueryRequest {
            patterns,
            rules,
            graphs: dataset_graphs.clone(),
            ..Default::default()
        };
        let mut client = state.client.clone();
        let resp = match client.query(tonic::Request::new(req)).await {
            Ok(r) => r.into_inner(),
            Err(e) => return grpc_error(e),
        };

        let branch_bindings: Vec<polargraph_sparql::SparqlBindings> = resp
            .bindings
            .into_iter()
            .filter(|pb| {
                // FROM NAMED: graph variables must name one of the graphs.
                let Some(named) = &named_nodes else {
                    return true;
                };
                ct.graph_vars
                    .iter()
                    .all(|gv| match pb.vars.get(gv).and_then(proto_node_id) {
                        Some(id) => named.contains(&id),
                        None => true,
                    })
            })
            .map(sparql_row)
            .collect();
        all_bindings.extend(branch_bindings);
    }

    // Build RDF triples.
    let mut rdf_triples: Vec<RdfStarTriple> = if is_describe {
        // Collect unique NodeIds from all bound values, plus any bare DESCRIBE <iri>.
        let mut node_ids: HashSet<NodeId> = HashSet::new();
        for b in &all_bindings {
            for v in b.values() {
                if let SparqlValue::Uri(id) = v {
                    node_ids.insert(*id);
                }
            }
        }
        // Bare DESCRIBE <urn:uuid:…> with no WHERE bindings.
        if node_ids.is_empty() {
            if let Some(ref iri) = ct.describe_iri {
                node_ids.insert(iri_to_node_id(iri));
            }
        }

        let mut result = Vec::new();
        // For each NodeId, scan its triples (relations and property values)
        // via the Query RPC.
        // We use a wildcard predicate with a bound subject and variable object.
        for id in &node_ids {
            let subj_bytes: Vec<u8> = id.0.as_bytes().to_vec();
            let req = proto::QueryRequest {
                patterns: vec![proto::VarPattern {
                    subject: Some(proto::Term {
                        kind: Some(proto::term::Kind::Bound(proto::NodeId {
                            bytes: subj_bytes,
                        })),
                    }),
                    predicate: String::new(), // wildcard
                    object: Some(proto::Term {
                        kind: Some(proto::term::Kind::Var("_o".to_string())),
                    }),
                    predicate_var: "_p".to_string(),
                    graph: None,
                }],
                ..Default::default()
            };
            let mut client = state.client.clone();
            if let Ok(resp) = client.query(tonic::Request::new(req)).await {
                for pb in resp.into_inner().bindings {
                    let predicate = pb
                        .predicates
                        .get("_p")
                        .map(|p| format!("<{}>", p))
                        .unwrap_or_else(|| "<urn:polargraph:unknownPredicate>".to_string());
                    // The object: a node, or a property value (a literal).
                    let object = match (
                        pb.vars.get("_o").and_then(proto_node_id),
                        pb.values.get("_o"),
                    ) {
                        (Some(node), _) => node_id_to_iri(&node),
                        (None, Some(v)) => match proto_value_to_pg(v) {
                            Some(value) => polargraph_sparql::value_to_nt_literal(&value),
                            None => continue,
                        },
                        (None, None) => continue,
                    };
                    result.push(RdfStarTriple {
                        subject: RdfStarSubject::Iri(node_id_to_iri(id)),
                        predicate,
                        object,
                    });
                }
            }
        }
        result
    } else {
        // CONSTRUCT: substitute WHERE bindings into the template triples.
        let mut result = Vec::new();
        for binding in &all_bindings {
            for tmpl in &ct.templates {
                if let Some(triple) = substitute_construct_template(tmpl, binding) {
                    result.push(triple);
                }
            }
        }
        result
    };

    let names = resolve_names(
        &mut state.client.clone(),
        polargraph_sparql::node_ids_in_star_triples(&rdf_triples),
        false,
    )
    .await;
    names.rewrite_star_triples(&mut rdf_triples);

    // Serialize based on Accept header.
    let accept = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let (content_type, body) = if accept.contains("application/n-triples") {
        (
            "application/n-triples",
            serialize_ntriples_star(&rdf_triples),
        )
    } else {
        ("text/turtle", serialize_turtle_star(&rdf_triples))
    };

    axum::response::Response::builder()
        .status(StatusCode::OK)
        .header("content-type", content_type)
        .body(axum::body::boxed(axum::body::Full::from(body)))
        .unwrap()
}

fn substitute_construct_template(
    tmpl: &polargraph_sparql::ConstructTemplate,
    binding: &polargraph_sparql::SparqlBindings,
) -> Option<polargraph_sparql::RdfStarTriple> {
    use polargraph_sparql::{node_id_to_iri, RdfStarSubject, RdfStarTriple, SparqlValue};

    // Gap 3: subject may be a quoted triple.
    let star_subject = if let Some(ref inner) = tmpl.subject_quoted {
        // Resolve the inner triple's three components.
        let inner_triple = substitute_construct_template(inner, binding)?;
        let (s, p, o) = match inner_triple.subject {
            RdfStarSubject::Iri(iri) => (iri, inner_triple.predicate, inner_triple.object),
            RdfStarSubject::QuotedTriple { .. } => return None, // deeply nested — not supported
        };
        RdfStarSubject::QuotedTriple { s, p, o }
    } else if let Some(ref var) = tmpl.subject_var {
        match binding.get(var)? {
            SparqlValue::Uri(id) => RdfStarSubject::Iri(node_id_to_iri(id)),
            _ => return None,
        }
    } else {
        RdfStarSubject::Iri(format!("<{}>", tmpl.subject_iri.as_deref()?))
    };

    let predicate = format!("<{}>", &tmpl.predicate);

    let object = if let Some(ref var) = tmpl.object_var {
        match binding.get(var)? {
            SparqlValue::Uri(id) => node_id_to_iri(id),
            literal => polargraph_sparql::value_to_nt_literal(&literal.to_value()?),
        }
    } else if let Some(ref iri) = tmpl.object_iri {
        format!("<{}>", iri)
    } else if let Some(ref lit) = tmpl.object_literal {
        polargraph_sparql::value_to_nt_literal(&lit.to_value().to_value()?)
    } else {
        return None;
    };

    Some(RdfStarTriple {
        subject: star_subject,
        predicate,
        object,
    })
}

// ── POST /sparql/update ───────────────────────────────────────────────────────

async fn handle_sparql_update(
    State(state): State<Arc<AppState>>,
    body: axum::body::Bytes,
) -> Response {
    let body_str = match String::from_utf8(body.to_vec()) {
        Ok(s) => s,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "invalid UTF-8 body" })),
            )
                .into_response()
        }
    };

    let update = match spargebra::Update::parse(&body_str, None) {
        Ok(u) => u,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": format!("SPARQL Update parse error: {}", e) })),
            )
                .into_response()
        }
    };

    let mut inserted: u64 = 0;
    let mut deleted: u64 = 0;
    // Quads that couldn't be applied (unsupported terms or RPC errors) — reported
    // rather than silently dropped.
    let mut failed: u64 = 0;
    // Messages for operations that failed as a whole (unsupported, or a
    // graph operation the server rejected without SILENT).
    let mut errors: Vec<String> = Vec::new();

    for operation in update.operations {
        match operation {
            spargebra::GraphUpdateOperation::InsertData { data } => {
                for quad in &data {
                    // Gap 4: handle quoted-triple subjects `<< S P O >> :annot :val`.
                    if let spargebra::term::Subject::Triple(inner) = &quad.subject {
                        // Resolve (S, P, O) to an edge_id, then insert annotation.
                        if let Some(ann) = sparql_star_quad_to_annotation(
                            inner,
                            quad.predicate.as_str(),
                            &quad.object,
                            &mut state.client.clone(),
                        )
                        .await
                        {
                            let mut client = state.client.clone();
                            let req = proto::InsertRequest {
                                edge_annotations: vec![ann],
                                ..Default::default()
                            };
                            if client.insert(tonic::Request::new(req)).await.is_ok() {
                                inserted += 1;
                            }
                        }
                        continue;
                    }
                    if let Some(triple) = sparql_quad_to_proto_triple(quad) {
                        let mut client = state.client.clone();
                        let req = proto::InsertRequest {
                            triples: vec![triple],
                            iris: sparql_quad_iris(quad),
                            graph: graph_name_iri(&quad.graph_name),
                            ..Default::default()
                        };
                        if client.insert(tonic::Request::new(req)).await.is_ok() {
                            inserted += 1;
                        } else {
                            failed += 1;
                        }
                    } else {
                        failed += 1;
                    }
                }
            }
            spargebra::GraphUpdateOperation::DeleteData { data } => {
                // Each plain quad closes exactly that (S, P, O) — never other
                // triples of the subject.
                for gq in &data {
                    match &gq.subject {
                        spargebra::term::GroundSubject::Triple(inner) => {
                            // Gap 4: DELETE DATA { << S P O >> :annot :val }
                            // Resolve inner triple to edge_id and delete the annotation.
                            if let Some(s_iri) = ground_triple_subject_iri(inner) {
                                let s_id = iri_to_node_id(&s_iri);
                                // Use the inner triple's subject as a proxy to soft-delete
                                // the annotation predicate on all edges from that subject.
                                let mut client = state.client.clone();
                                let req = proto::DeleteTriplesRequest {
                                    subject_ids: vec![s_id.0.as_bytes().to_vec()],
                                    predicate: gq.predicate.as_str().to_string(),
                                    ..Default::default()
                                };
                                if let Ok(r) = client.delete_triples(tonic::Request::new(req)).await
                                {
                                    deleted += r.into_inner().deleted_count;
                                }
                            }
                        }
                        spargebra::term::GroundSubject::NamedNode(n) => {
                            let Some(target) = ground_term_delete_target(&gq.object) else {
                                failed += 1;
                                continue;
                            };
                            let mut req = exact_delete_request(
                                iri_to_node_id(n.as_str()),
                                gq.predicate.as_str(),
                                target,
                            );
                            req.graph = delete_graph_term(&gq.graph_name);
                            let mut client = state.client.clone();
                            match client.delete_triples(tonic::Request::new(req)).await {
                                Ok(r) => deleted += r.into_inner().deleted_count,
                                // A graph that doesn't exist holds nothing to delete.
                                Err(e) if e.code() == tonic::Code::NotFound => {}
                                Err(_) => failed += 1,
                            }
                        }
                        #[allow(unreachable_patterns)]
                        _ => {}
                    }
                }
            }
            spargebra::GraphUpdateOperation::DeleteInsert {
                delete,
                insert,
                using,
                pattern,
            } => {
                // ADD / COPY / MOVE arrive rewritten as `INSERT { GRAPH dst
                // { ?s ?p ?o } } WHERE { GRAPH src { ?s ?p ?o } }` (after a
                // DROP for COPY / MOVE). Run that as a server-side graph copy,
                // which keeps literal values and edge ids.
                if let Some((source, target)) =
                    graph_copy_shape(&delete, &insert, using.as_ref(), &pattern)
                {
                    let req = proto::CopyGraphRequest {
                        user_id: String::new(),
                        source,
                        target,
                        clear_target: false,
                    };
                    match state
                        .client
                        .clone()
                        .copy_graph(tonic::Request::new(req))
                        .await
                    {
                        Ok(r) => inserted += r.into_inner().quads,
                        // An absent source graph is empty.
                        Err(e) if e.code() == tonic::Code::NotFound => {}
                        Err(e) => errors.push(format!("copy: {}", e.message())),
                    }
                    continue;
                }

                // INSERT/DELETE WHERE: evaluate WHERE clause, then apply templates.
                // USING / USING NAMED set the WHERE clause's dataset.
                let using_dataset = using.as_ref().map(|ds| polargraph_sparql::SparqlDataset {
                    default: ds.default.iter().map(|n| n.as_str().to_string()).collect(),
                    named: ds
                        .named
                        .as_ref()
                        .map(|ns| ns.iter().map(|n| n.as_str().to_string()).collect()),
                });
                let mut dummy = polargraph_sparql::SparqlTranslation {
                    dataset: using_dataset.clone(),
                    ..Default::default()
                };
                let mut counter = 0usize;
                let branches = match polargraph_sparql::translate_pattern_pub(
                    &pattern,
                    &mut counter,
                    &mut dummy,
                ) {
                    Ok(b) => b,
                    Err(_) => continue,
                };

                // Collect bindings from WHERE clause.
                let mut where_bindings: Vec<polargraph_sparql::SparqlBindings> = Vec::new();
                for branch in &branches {
                    let patterns: Vec<proto::VarPattern> =
                        branch.patterns.iter().map(sparql_varpat_to_proto).collect();
                    let rules: Vec<proto::DatalogRule> =
                        branch.rules.iter().map(sparql_rule_to_proto).collect();
                    if patterns.is_empty() && rules.is_empty() {
                        continue;
                    }
                    let req = proto::QueryRequest {
                        patterns,
                        rules,
                        graphs: using_dataset
                            .as_ref()
                            .map(|d| d.default.clone())
                            .unwrap_or_default(),
                        ..Default::default()
                    };
                    let mut client = state.client.clone();
                    if let Ok(resp) = client.query(tonic::Request::new(req)).await {
                        for pb in resp.into_inner().bindings {
                            where_bindings.push(sparql_row(pb));
                        }
                    }
                }

                // IRIs of graph nodes bound by `GRAPH ?g`, for templates
                // whose graph is a variable.
                let graph_var_templates = delete
                    .iter()
                    .map(|q| &q.graph_name)
                    .chain(insert.iter().map(|q| &q.graph_name))
                    .any(|g| matches!(g, spargebra::term::GraphNamePattern::Variable(_)));
                let graph_names = if graph_var_templates {
                    resolve_names(
                        &mut state.client.clone(),
                        polargraph_sparql::node_ids_in_bindings(&where_bindings),
                        false,
                    )
                    .await
                } else {
                    polargraph_sparql::IriNames::new(Default::default(), false)
                };

                // Apply DELETE templates: each GroundQuadPattern has subject/object as
                // GroundTermPattern (variable or bound IRI) and predicate as NamedNodePattern.
                for gqp in &delete {
                    for binding in &where_bindings {
                        if let Some(subj_id) = resolve_ground_term_subject(&gqp.subject, binding) {
                            let Some(pred) = template_predicate(&gqp.predicate, binding) else {
                                continue;
                            };
                            let Some(target) = resolve_ground_term_object(&gqp.object, binding)
                            else {
                                failed += 1;
                                continue;
                            };
                            let mut req = exact_delete_request(subj_id, &pred, target);
                            req.graph = match template_graph(&gqp.graph_name, binding, &graph_names)
                            {
                                Some(TemplateGraph::Default) => {
                                    delete_graph_term(&spargebra::term::GraphName::DefaultGraph)
                                }
                                Some(TemplateGraph::Iri(iri)) => Some(proto::GraphTerm {
                                    kind: Some(proto::graph_term::Kind::Iri(iri)),
                                }),
                                None => {
                                    failed += 1;
                                    continue;
                                }
                            };
                            let mut client = state.client.clone();
                            match client.delete_triples(tonic::Request::new(req)).await {
                                Ok(r) => deleted += r.into_inner().deleted_count,
                                Err(e) if e.code() == tonic::Code::NotFound => {}
                                Err(_) => failed += 1,
                            }
                        }
                    }
                }

                // Apply INSERT templates.
                for qp in &insert {
                    for binding in &where_bindings {
                        if let Some(triple) = resolve_quad_pattern_to_proto(qp, binding) {
                            let graph = match template_graph(&qp.graph_name, binding, &graph_names)
                            {
                                Some(TemplateGraph::Default) => String::new(),
                                Some(TemplateGraph::Iri(iri)) => iri,
                                None => {
                                    failed += 1;
                                    continue;
                                }
                            };
                            let mut client = state.client.clone();
                            let req = proto::InsertRequest {
                                triples: vec![triple],
                                iris: quad_pattern_iris(qp),
                                graph,
                                ..Default::default()
                            };
                            if client.insert(tonic::Request::new(req)).await.is_ok() {
                                inserted += 1;
                            } else {
                                failed += 1;
                            }
                        }
                    }
                }
            }
            // CLEAR and DROP both close every live quad of the target
            // graphs (bitemporal: history stays queryable).
            spargebra::GraphUpdateOperation::Clear { silent, graph }
            | spargebra::GraphUpdateOperation::Drop { silent, graph } => {
                let mut client = state.client.clone();
                let iris = match update_graph_targets(&mut client, &graph).await {
                    Ok(iris) => iris,
                    Err(e) => {
                        errors.push(format!("list graphs: {}", e.message()));
                        continue;
                    }
                };
                for iri in iris {
                    let req = proto::DropGraphRequest {
                        iri: iri.clone(),
                        user_id: String::new(),
                    };
                    match client.drop_graph(tonic::Request::new(req)).await {
                        Ok(r) => deleted += r.into_inner().quads_closed,
                        Err(_) if silent => {}
                        Err(e) => errors.push(format!("drop <{iri}>: {}", e.message())),
                    }
                }
            }
            spargebra::GraphUpdateOperation::Create { graph, .. } => {
                // Idempotent: creating an existing graph is not an error.
                let req = proto::CreateGraphRequest {
                    user_id: String::new(),
                    iri: graph.as_str().to_string(),
                    metadata: vec![],
                };
                if let Err(e) = state
                    .client
                    .clone()
                    .create_graph(tonic::Request::new(req))
                    .await
                {
                    errors.push(format!("create <{}>: {}", graph.as_str(), e.message()));
                }
            }
            spargebra::GraphUpdateOperation::Load { silent, source, .. } => {
                if !silent {
                    errors.push(format!(
                        "LOAD <{}> is not supported; use POST /import/rdf",
                        source.as_str()
                    ));
                }
            }
        }
    }

    Json(serde_json::json!({
        "ok": failed == 0 && errors.is_empty(),
        "inserted": inserted,
        "deleted": deleted,
        "failed": failed,
        "errors": errors,
    }))
    .into_response()
}

/// A SPARQL graph name as an `InsertRequest.graph` IRI ("" = default graph).
fn graph_name_iri(graph: &spargebra::term::GraphName) -> String {
    match graph {
        spargebra::term::GraphName::NamedNode(n) => n.as_str().to_string(),
        spargebra::term::GraphName::DefaultGraph => String::new(),
    }
}

/// The `DeleteTriplesRequest.graph` for a quad's graph name. The SPARQL
/// default graph is the union of all graphs, so a delete without `GRAPH`
/// closes the triple wherever it lives.
fn delete_graph_term(graph: &spargebra::term::GraphName) -> Option<proto::GraphTerm> {
    match graph {
        spargebra::term::GraphName::NamedNode(n) => Some(proto::GraphTerm {
            kind: Some(proto::graph_term::Kind::Iri(n.as_str().to_string())),
        }),
        spargebra::term::GraphName::DefaultGraph => None,
    }
}

/// The graph of a template quad for one WHERE solution.
enum TemplateGraph {
    Default,
    Iri(String),
}

/// Resolve a template's graph: a fixed IRI, the default graph, or a variable
/// bound (by `GRAPH ?g` in the WHERE clause) to a graph IRI node. `None` when
/// the variable is unbound.
fn template_graph(
    graph: &spargebra::term::GraphNamePattern,
    binding: &polargraph_sparql::SparqlBindings,
    names: &polargraph_sparql::IriNames,
) -> Option<TemplateGraph> {
    use spargebra::term::GraphNamePattern;
    match graph {
        GraphNamePattern::NamedNode(n) => Some(TemplateGraph::Iri(n.as_str().to_string())),
        GraphNamePattern::DefaultGraph => Some(TemplateGraph::Default),
        GraphNamePattern::Variable(v) => match binding.get(v.as_str()) {
            Some(polargraph_sparql::SparqlValue::Uri(id)) => {
                Some(TemplateGraph::Iri(names.iri(id)))
            }
            _ => None,
        },
    }
}

/// `(source, target)` graph IRIs ("" = default graph) when a DeleteInsert is
/// spargebra's rewriting of ADD / COPY / MOVE: no DELETE or USING, one
/// `?s ?p ?o` INSERT template into a fixed graph, and a WHERE clause that is
/// exactly `?s ?p ?o` (default graph) or `GRAPH <src> { ?s ?p ?o }`.
fn graph_copy_shape(
    delete: &[spargebra::term::GroundQuadPattern],
    insert: &[spargebra::term::QuadPattern],
    using: Option<&spargebra::algebra::QueryDataset>,
    pattern: &spargebra::algebra::GraphPattern,
) -> Option<(String, String)> {
    use spargebra::algebra::GraphPattern;
    use spargebra::term::{GraphNamePattern, NamedNodePattern, TermPattern};

    if !delete.is_empty() || using.is_some() {
        return None;
    }
    let [template] = insert else { return None };
    let (TermPattern::Variable(s), NamedNodePattern::Variable(p), TermPattern::Variable(o)) =
        (&template.subject, &template.predicate, &template.object)
    else {
        return None;
    };
    let target = match &template.graph_name {
        GraphNamePattern::NamedNode(n) => n.as_str().to_string(),
        GraphNamePattern::DefaultGraph => String::new(),
        GraphNamePattern::Variable(_) => return None,
    };
    let (source, bgp) = match pattern {
        GraphPattern::Graph {
            name: NamedNodePattern::NamedNode(n),
            inner,
        } => (n.as_str().to_string(), inner.as_ref()),
        other => (String::new(), other),
    };
    let GraphPattern::Bgp { patterns } = bgp else {
        return None;
    };
    let [tp] = patterns.as_slice() else {
        return None;
    };
    let same = matches!(
        (&tp.subject, &tp.predicate, &tp.object),
        (TermPattern::Variable(ws), NamedNodePattern::Variable(wp), TermPattern::Variable(wo))
            if ws == s && wp == p && wo == o
    );
    same.then_some((source, target))
}

/// Graph IRIs ("" = default graph) that a CLEAR / DROP target names.
async fn update_graph_targets(
    client: &mut GrpcClient,
    target: &spargebra::algebra::GraphTarget,
) -> Result<Vec<String>, tonic::Status> {
    use spargebra::algebra::GraphTarget;
    let named = |client: &mut GrpcClient| {
        let mut client = client.clone();
        async move {
            let req = proto::ListGraphsRequest {
                filter: vec![],
                include_system: false,
            };
            let graphs = client.list_graphs(tonic::Request::new(req)).await?;
            Ok::<_, tonic::Status>(
                graphs
                    .into_inner()
                    .graphs
                    .into_iter()
                    .map(|g| g.iri)
                    .collect::<Vec<_>>(),
            )
        }
    };
    Ok(match target {
        GraphTarget::NamedNode(n) => vec![n.as_str().to_string()],
        GraphTarget::DefaultGraph => vec![String::new()],
        GraphTarget::NamedGraphs => named(client).await?,
        GraphTarget::AllGraphs => {
            let mut all = vec![String::new()];
            all.extend(named(client).await?);
            all
        }
    })
}

// ── POST /delete ──────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct DeleteTriplesBody {
    /// List of subject UUIDs (string form) to soft-delete.
    subject_ids: Vec<String>,
    /// Optional predicate filter; if absent all predicates are deleted.
    #[serde(default)]
    predicate: String,
    /// Optional explicit vt_end timestamp in microseconds; 0 = server clock.
    #[serde(default)]
    vt_end: i64,
}

async fn handle_delete_triples(
    State(state): State<Arc<AppState>>,
    Json(body): Json<DeleteTriplesBody>,
) -> Response {
    let subject_ids: Result<Vec<Vec<u8>>, String> = body
        .subject_ids
        .iter()
        .map(|s| {
            uuid::Uuid::parse_str(s)
                .map(|u| u.as_bytes().to_vec())
                .map_err(|_| format!("invalid UUID: {:?}", s))
        })
        .collect();
    let subject_ids = match subject_ids {
        Ok(ids) => ids,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response()
        }
    };

    let req = proto::DeleteTriplesRequest {
        subject_ids,
        predicate: body.predicate,
        vt_end: body.vt_end,
        ..Default::default()
    };

    let mut client = state.client.clone();
    match client.delete_triples(tonic::Request::new(req)).await {
        Ok(r) => {
            Json(serde_json::json!({ "ok": true, "deleted_count": r.into_inner().deleted_count }))
                .into_response()
        }
        Err(e) => grpc_error(e),
    }
}

// ── OWL 2 RL materialization endpoint ────────────────────────────────────────

#[derive(Deserialize)]
struct MaterializeBody {
    /// When true, clear the DRV column family before re-materializing.
    /// When absent/false, performs an incremental run.
    #[serde(default)]
    clear_first: bool,
}

async fn handle_materialize(
    State(state): State<Arc<AppState>>,
    body: Option<Json<MaterializeBody>>,
) -> Response {
    let clear_first = body.map(|b| b.clear_first).unwrap_or(false);
    let req = proto::RunMaterializationRequest { clear_first };
    let mut client = state.client.clone();
    match client.run_materialization(tonic::Request::new(req)).await {
        Ok(r) => {
            let inner = r.into_inner();
            Json(serde_json::json!({
                "ok": true,
                "rules_fired": inner.rules_fired,
                "derived_triples": inner.derived_triples,
                "iterations": inner.iterations,
            }))
            .into_response()
        }
        Err(e) => grpc_error(e),
    }
}

// ── SPARQL-star Update helpers (Gap 4) ───────────────────────────────────────

/// Extract the subject IRI (as a bare urn:uuid:... string) from a `GroundTriple` subject.
/// Returns `None` when the subject is not a UUID IRI.
fn ground_triple_subject_iri(gt: &spargebra::term::GroundTriple) -> Option<String> {
    use spargebra::term::GroundSubject;
    match &gt.subject {
        GroundSubject::NamedNode(n) => Some(n.as_str().to_string()),
        GroundSubject::Triple(inner) => ground_triple_subject_iri(inner),
    }
}

/// For a SPARQL-star INSERT DATA statement with a quoted-triple subject,
/// resolve the inner triple to an edge ID and return the corresponding `EdgeAnnotation` proto.
async fn sparql_star_quad_to_annotation(
    inner: &spargebra::term::Triple,
    annot_pred: &str,
    object: &spargebra::term::Term,
    client: &mut GrpcClient,
) -> Option<proto::EdgeAnnotation> {
    use spargebra::term::{Subject, Term};

    // Resolve inner triple subject to UUID.
    let s_iri = match &inner.subject {
        Subject::NamedNode(n) => n.as_str().to_string(),
        _ => return None,
    };
    let s_uuid = iri_to_node_id(&s_iri).0;
    let s_bytes = s_uuid.as_bytes().to_vec();

    let pred = inner.predicate.as_str().to_string();

    let o_iri = match &inner.object {
        Term::NamedNode(n) => n.as_str().to_string(),
        _ => return None,
    };
    let o_uuid = iri_to_node_id(&o_iri).0;
    let o_bytes = o_uuid.as_bytes().to_vec();

    // Look up the edge ID for (S, P, O).
    let edge_ids = resolve_edge_ids(client, s_bytes, &pred, o_bytes).await;
    let edge_id_bytes = edge_ids.into_iter().next()?;

    // Map the annotation object to an EdgeAnnotation value.
    let ann_value = match object {
        Term::NamedNode(n) => {
            let obj_uuid = iri_to_node_id(n.as_str()).0;
            proto::edge_annotation::Value::NodeId(obj_uuid.as_bytes().to_vec())
        }
        Term::Literal(lit) => {
            let pv = sparql_literal_to_proto_value(lit)?;
            proto::edge_annotation::Value::Scalar(pv)
        }
        _ => return None,
    };

    Some(proto::EdgeAnnotation {
        edge_id: edge_id_bytes,
        predicate: annot_pred.to_string(),
        value: Some(ann_value),
    })
}

// ── SPARQL Update helpers ─────────────────────────────────────────────────────

/// Convert a spargebra [`Quad`] (INSERT DATA) to a proto [`Triple`] or annotation.
///
/// Returns `None` if the quad cannot be mapped (e.g. blank-node subjects).
/// Quoted-triple subjects `<< S P O >>` are converted to edge annotation inserts via the
/// separate `sparql_quad_to_edge_annotation` path (handled by the caller).
/// IRIs a ground quad names nodes by (subject and IRI object), for the IRI
/// dictionary.
fn sparql_quad_iris(quad: &spargebra::term::Quad) -> Vec<String> {
    use spargebra::term::{Subject, Term};
    let mut iris = Vec::new();
    if let Subject::NamedNode(n) = &quad.subject {
        iris.push(n.as_str().to_string());
    }
    if let Term::NamedNode(n) = &quad.object {
        iris.push(n.as_str().to_string());
    }
    iris.retain(|i| polargraph_core::term::needs_dictionary(i));
    iris
}

/// IRIs named directly (not via variables) by an INSERT template, for the
/// IRI dictionary.
fn quad_pattern_iris(qp: &spargebra::term::QuadPattern) -> Vec<String> {
    use spargebra::term::TermPattern;
    let mut iris: Vec<String> = [&qp.subject, &qp.object]
        .into_iter()
        .filter_map(|t| match t {
            TermPattern::NamedNode(n) => Some(n.as_str().to_string()),
            _ => None,
        })
        .collect();
    iris.retain(|i| polargraph_core::term::needs_dictionary(i));
    iris
}

fn sparql_quad_to_proto_triple(quad: &spargebra::term::Quad) -> Option<proto::Triple> {
    use spargebra::term::{Subject, Term};

    // Quoted-triple subjects are handled by a dedicated annotation path.
    if matches!(&quad.subject, Subject::Triple(_)) {
        return None;
    }

    let subj_iri = match &quad.subject {
        Subject::NamedNode(n) => n.as_str().to_string(),
        _ => return None, // blank nodes not supported
    };
    let subj_id = iri_to_node_id(&subj_iri).0;
    let predicate = quad.predicate.as_str().to_string();

    match &quad.object {
        Term::NamedNode(n) => {
            let obj_iri = n.as_str();
            let obj_id = iri_to_node_id(obj_iri).0;
            Some(proto::Triple {
                kind: Some(proto::triple::Kind::Relation(proto::RelationTriple {
                    subject: Some(proto::NodeId {
                        bytes: subj_id.as_bytes().to_vec(),
                    }),
                    predicate,
                    object: Some(proto::NodeId {
                        bytes: obj_id.as_bytes().to_vec(),
                    }),
                    vt_start: 0,
                    vt_end: i64::MAX,
                    object_iri: String::new(),
                    properties: vec![],
                })),
            })
        }
        Term::Literal(lit) => {
            let val = sparql_literal_to_proto_value(lit)?;
            Some(proto::Triple {
                kind: Some(proto::triple::Kind::Property(proto::PropertyTriple {
                    subject: Some(proto::NodeId {
                        bytes: subj_id.as_bytes().to_vec(),
                    }),
                    predicate,
                    value: Some(val),
                    vt_start: 0,
                    vt_end: i64::MAX,
                    mode: proto::PropertyWriteMode::Add as i32,
                })),
            })
        }
        _ => None,
    }
}

/// A SPARQL literal as a proto value (see `polargraph_core::term::literal_to_value`).
fn sparql_literal_to_proto_value(lit: &spargebra::term::Literal) -> Option<proto::Value> {
    Some(pg_value_to_proto(&polargraph_core::term::literal_to_value(
        lit.value(),
        Some(lit.datatype().as_str()),
        lit.language(),
    )))
}

/// What a SPARQL DELETE quad's object pins down: a node or a literal value.
enum DeleteTarget {
    Node(NodeId),
    Value(proto::Value),
}

/// A `DeleteTriples` request that closes exactly `(subject, predicate, target)`.
fn exact_delete_request(
    subject: NodeId,
    predicate: &str,
    target: DeleteTarget,
) -> proto::DeleteTriplesRequest {
    let mut req = proto::DeleteTriplesRequest {
        subject_ids: vec![subject.0.as_bytes().to_vec()],
        predicate: predicate.to_string(),
        ..Default::default()
    };
    match target {
        DeleteTarget::Node(o) => req.object_id = o.0.as_bytes().to_vec(),
        DeleteTarget::Value(v) => req.value = Some(v),
    }
    req
}

/// The object of a DELETE DATA quad. `None` for terms we can't delete by
/// (quoted triples, unsupported literal types).
fn ground_term_delete_target(term: &spargebra::term::GroundTerm) -> Option<DeleteTarget> {
    use spargebra::term::GroundTerm;
    match term {
        GroundTerm::NamedNode(n) => Some(DeleteTarget::Node(iri_to_node_id(n.as_str()))),
        GroundTerm::Literal(l) => sparql_literal_to_proto_value(l).map(DeleteTarget::Value),
        #[allow(unreachable_patterns)]
        _ => None,
    }
}

/// The object of a DELETE template quad under `binding`.
fn resolve_ground_term_object(
    gtp: &spargebra::term::GroundTermPattern,
    binding: &polargraph_sparql::SparqlBindings,
) -> Option<DeleteTarget> {
    use polargraph_sparql::SparqlValue;
    use spargebra::term::GroundTermPattern;
    match gtp {
        GroundTermPattern::NamedNode(n) => Some(DeleteTarget::Node(iri_to_node_id(n.as_str()))),
        GroundTermPattern::Literal(l) => sparql_literal_to_proto_value(l).map(DeleteTarget::Value),
        GroundTermPattern::Variable(v) => match binding.get(v.as_str())? {
            SparqlValue::Uri(id) => Some(DeleteTarget::Node(*id)),
            SparqlValue::Iri(iri) => Some(DeleteTarget::Node(iri_to_node_id(iri))),
            other => sparql_value_to_proto(other).map(DeleteTarget::Value),
        },
        _ => None,
    }
}

/// Convert a literal SPARQL binding to a proto value; `None` for URIs.
fn sparql_value_to_proto(v: &polargraph_sparql::SparqlValue) -> Option<proto::Value> {
    v.to_value().as_ref().map(pg_value_to_proto)
}

/// Resolve a [`GroundTermPattern`] subject to a [`NodeId`] using current bindings.
fn resolve_ground_term_subject(
    gtp: &spargebra::term::GroundTermPattern,
    binding: &polargraph_sparql::SparqlBindings,
) -> Option<NodeId> {
    use polargraph_sparql::SparqlValue;
    use spargebra::term::GroundTermPattern;
    match gtp {
        GroundTermPattern::NamedNode(n) => Some(iri_to_node_id(n.as_str())),
        GroundTermPattern::Variable(v) => {
            if let Some(SparqlValue::Uri(id)) = binding.get(v.as_str()) {
                Some(*id)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Resolve a [`QuadPattern`] to a proto [`Triple`] using current variable bindings.
fn resolve_quad_pattern_to_proto(
    qp: &spargebra::term::QuadPattern,
    binding: &polargraph_sparql::SparqlBindings,
) -> Option<proto::Triple> {
    use polargraph_sparql::SparqlValue;
    use spargebra::term::TermPattern;

    // Resolve subject (a literal can't be a subject: the triple is skipped).
    let subj_id = match &qp.subject {
        TermPattern::NamedNode(n) => iri_to_node_id(n.as_str()),
        TermPattern::Variable(v) => match binding.get(v.as_str())? {
            SparqlValue::Uri(id) => *id,
            SparqlValue::Iri(iri) => iri_to_node_id(iri),
            _ => return None,
        },
        _ => return None,
    };
    let subject = Some(pg_node_id_to_proto(subj_id));

    let predicate = template_predicate(&qp.predicate, binding)?;
    let relation = |object: Option<proto::NodeId>, object_iri: String| proto::Triple {
        kind: Some(proto::triple::Kind::Relation(proto::RelationTriple {
            subject: subject.clone(),
            predicate: predicate.clone(),
            object,
            vt_start: 0,
            vt_end: i64::MAX,
            object_iri,
            properties: vec![],
        })),
    };
    let property = |value: proto::Value| proto::Triple {
        kind: Some(proto::triple::Kind::Property(proto::PropertyTriple {
            subject: subject.clone(),
            predicate: predicate.clone(),
            value: Some(value),
            vt_start: 0,
            vt_end: i64::MAX,
            mode: proto::PropertyWriteMode::Add as i32,
        })),
    };

    // Resolve object.
    match &qp.object {
        TermPattern::NamedNode(n) => Some(relation(
            Some(pg_node_id_to_proto(iri_to_node_id(n.as_str()))),
            String::new(),
        )),
        TermPattern::Variable(v) => match binding.get(v.as_str())? {
            SparqlValue::Uri(obj_id) => {
                Some(relation(Some(pg_node_id_to_proto(*obj_id)), String::new()))
            }
            SparqlValue::Iri(iri) => Some(relation(None, iri.clone())),
            // A value variable inserts the bound literal.
            literal => sparql_value_to_proto(literal).map(property),
        },
        TermPattern::Literal(lit) => sparql_literal_to_proto_value(lit).map(property),
        _ => None,
    }
}

/// A template's predicate: an IRI, or a predicate variable's binding.
/// `None` (skip the triple) when the variable is unbound or not an IRI.
fn template_predicate(
    p: &spargebra::term::NamedNodePattern,
    binding: &polargraph_sparql::SparqlBindings,
) -> Option<String> {
    use polargraph_sparql::SparqlValue;
    match p {
        spargebra::term::NamedNodePattern::NamedNode(n) => Some(n.as_str().to_string()),
        spargebra::term::NamedNodePattern::Variable(v) => match binding.get(v.as_str())? {
            SparqlValue::Iri(iri) => Some(iri.clone()),
            _ => None,
        },
    }
}

// ── GET /stats ────────────────────────────────────────────────────────────────

// ── RDF interoperability helpers ──────────────────────────────────────────────

/// Convert a PolarGraph [`polargraph_core::id::NodeId`] to proto bytes.
fn pg_node_id_to_proto(id: polargraph_core::id::NodeId) -> proto::NodeId {
    proto::NodeId {
        bytes: id.0.as_bytes().to_vec(),
    }
}

/// Convert a polargraph_core Value to a proto Value.
fn pg_value_to_proto(v: &polargraph_core::value::Value) -> proto::Value {
    use polargraph_core::value::Value as V;
    proto::Value {
        kind: Some(match v {
            V::Text(s) => proto::value::Kind::TextVal(s.clone()),
            V::Int(n) => proto::value::Kind::IntVal(*n),
            V::Float(f) => proto::value::Kind::FloatVal(*f),
            V::Bool(b) => proto::value::Kind::BoolVal(*b),
            V::Blob(b) => proto::value::Kind::BlobVal(b.clone()),
            V::Vector(vs) => proto::value::Kind::VecVal(proto::FloatArray { values: vs.clone() }),
            V::LangText { text, lang } => proto::value::Kind::LangText(proto::LangText {
                text: text.clone(),
                lang: lang.clone(),
            }),
            V::Typed { lexical, datatype } => proto::value::Kind::Typed(proto::TypedLiteral {
                lexical: lexical.clone(),
                datatype: datatype.clone(),
            }),
            V::Null => return proto::Value { kind: None },
        }),
    }
}

/// Convert a batch of [`polargraph_sparql::ImportedTriple`] objects to `proto::Triple` objects.
///
/// Each Relation becomes one `proto::Triple::Relation`; each Literal becomes a Property.
/// Blank nodes are skolemized within `scope`, so the same label in two
/// imports names two different nodes.
fn imported_triples_to_proto(
    triples: &[polargraph_sparql::ImportedTriple],
    scope: &polargraph_sparql::ImportScope,
) -> Vec<proto::Triple> {
    use polargraph_sparql::ImportedObject;

    triples
        .iter()
        .map(|t| {
            let subject_proto = pg_node_id_to_proto(t.subject_node_id(scope));

            match &t.object {
                ImportedObject::Iri(_) | ImportedObject::BlankNode(_) => {
                    let obj_node_id = t
                        .object
                        .node_id(scope)
                        .expect("IRI and blank-node objects always have a NodeId");
                    proto::Triple {
                        kind: Some(proto::triple::Kind::Relation(proto::RelationTriple {
                            subject: Some(subject_proto),
                            predicate: t.predicate.clone(),
                            object: Some(pg_node_id_to_proto(obj_node_id)),
                            vt_start: 0,
                            vt_end: i64::MAX,
                            object_iri: String::new(),
                            properties: vec![],
                        })),
                    }
                }
                ImportedObject::Literal { value, .. } => {
                    let proto_val = pg_value_to_proto(value);
                    proto::Triple {
                        kind: Some(proto::triple::Kind::Property(proto::PropertyTriple {
                            subject: Some(subject_proto),
                            predicate: t.predicate.clone(),
                            value: Some(proto_val),
                            vt_start: 0,
                            vt_end: i64::MAX,
                            mode: proto::PropertyWriteMode::Add as i32,
                        })),
                    }
                }
            }
        })
        .collect()
}

/// Names for `ids` from the IRI dictionary (`ResolveIris`, batched). Display
/// only: if the lookup fails (e.g. an older server), nodes fall back to
/// `urn:uuid:` and a warning is logged rather than failing the response.
async fn resolve_names(
    client: &mut GrpcClient,
    ids: Vec<NodeId>,
    deskolemize: bool,
) -> polargraph_sparql::IriNames {
    const BATCH: usize = 10_000;
    let mut pairs = Vec::with_capacity(ids.len());
    for chunk in ids.chunks(BATCH) {
        let req = proto::ResolveIrisRequest {
            nodes: chunk.iter().map(|id| pg_node_id_to_proto(*id)).collect(),
        };
        match client.resolve_iris(tonic::Request::new(req)).await {
            Ok(resp) => pairs.extend(chunk.iter().copied().zip(resp.into_inner().iris)),
            Err(e) => {
                tracing::warn!("ResolveIris failed; exporting urn:uuid IRIs: {e}");
                break;
            }
        }
    }
    polargraph_sparql::IriNames::from_pairs(pairs, deskolemize)
}

/// Insert a batch of proto triples via the gRPC Insert RPC, recording `iris`
/// in the IRI dictionary in the same commit.
async fn insert_proto_triples(
    client: &mut GrpcClient,
    triples: Vec<proto::Triple>,
    iris: Vec<String>,
    graph: String,
) -> Result<usize, tonic::Status> {
    let n = triples.len();
    client
        .insert(tonic::Request::new(proto::InsertRequest {
            triples,
            iris,
            graph,
            ..Default::default()
        }))
        .await?;
    Ok(n)
}

/// Distinct IRIs (including skolem IRIs) named by `triples`, for the IRI
/// dictionary. `urn:uuid:` IRIs are left out — they carry their ID.
fn imported_iris(
    triples: &[polargraph_sparql::ImportedTriple],
    scope: &polargraph_sparql::ImportScope,
) -> Vec<String> {
    let mut seen = std::collections::BTreeSet::new();
    for t in triples {
        for iri in t.node_iris(scope) {
            if polargraph_core::term::needs_dictionary(&iri) {
                seen.insert(iri);
            }
        }
    }
    seen.into_iter().collect()
}

// ── POST /import/rdf ──────────────────────────────────────────────────────────
//
// Accept RDF data in multiple serialization formats and batch-insert via gRPC.
// Format is determined by the Content-Type header:
//   application/n-triples  → N-Triples
//   text/turtle             → Turtle
//   application/ld+json    → JSON-LD
//
// Blank nodes are skolemized per import. Pass `?import_id=<id>` to make a
// re-import idempotent (same id → same blank-node NodeIds); otherwise a fresh
// UUIDv7 is generated. The id used is returned as `import_id`.

#[derive(Deserialize, Default)]
struct ImportRdfParams {
    import_id: Option<String>,
    /// Target graph (IRI) for triples that don't name one — all triples of
    /// a triple format. Omitted = the default graph.
    graph: Option<String>,
}

/// An `import_id` becomes a path segment of every skolem IRI, so keep it to
/// unreserved IRI characters.
fn valid_import_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~'))
}

async fn handle_import_rdf(
    State(state): State<Arc<AppState>>,
    QueryParams(params): QueryParams<ImportRdfParams>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    use polargraph_sparql::{parse_jsonld, parse_ntriples, parse_turtle, ImportScope};
    use std::time::Instant;

    let scope = match params.import_id {
        Some(id) if !valid_import_id(&id) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "import_id must be 1-128 characters of [A-Za-z0-9._~-]"
                })),
            )
                .into_response()
        }
        Some(id) => ImportScope::new(&state.skolem_base, id),
        None => ImportScope::fresh(&state.skolem_base),
    };

    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_lowercase();

    let start = Instant::now();

    let imported_triples = if content_type.contains("application/n-quads") {
        match polargraph_sparql::parse_nquads(&body) {
            Ok(t) => t,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": e })),
                )
                    .into_response()
            }
        }
    } else if content_type.contains("application/trig") {
        match polargraph_sparql::parse_trig(&body) {
            Ok(t) => t,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": e })),
                )
                    .into_response()
            }
        }
    } else if content_type.contains("application/n-triples") {
        match parse_ntriples(&body) {
            Ok(t) => t,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": e })),
                )
                    .into_response()
            }
        }
    } else if content_type.contains("text/turtle") {
        match parse_turtle(&body) {
            Ok(t) => t,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": e })),
                )
                    .into_response()
            }
        }
    } else if content_type.contains("application/ld+json") {
        let text = match std::str::from_utf8(&body) {
            Ok(s) => s,
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": "body is not valid UTF-8" })),
                )
                    .into_response()
            }
        };
        match parse_jsonld(text) {
            Ok(t) => t,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": e })),
                )
                    .into_response()
            }
        }
    } else if content_type.contains("application/rdf+xml")
        || content_type.contains("application/owl+xml")
    {
        return (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Json(serde_json::json!({ "error": "RDF/XML and OWL/XML formats are not supported; use application/n-triples, text/turtle, or application/ld+json" })),
        )
            .into_response();
    } else {
        return (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Json(serde_json::json!({ "error": format!("unsupported Content-Type: {}; supported: application/n-triples, text/turtle, application/ld+json, application/n-quads, application/trig", content_type) })),
        )
            .into_response();
    };

    let total = imported_triples.len();

    // Group by target graph (quad formats name it per triple; `?graph=` is
    // the fallback), then insert in batches of 1 000, each carrying the IRIs
    // it names.
    let mut by_graph: std::collections::BTreeMap<String, Vec<polargraph_sparql::ImportedTriple>> =
        std::collections::BTreeMap::new();
    for t in imported_triples {
        let graph = t
            .graph_iri(&scope)
            .or_else(|| params.graph.clone())
            .unwrap_or_default();
        by_graph.entry(graph).or_default().push(t);
    }
    const BATCH: usize = 1_000;
    let mut imported = 0usize;
    let mut client = state.client.clone();
    for (graph, triples_in_graph) in &by_graph {
        for chunk in triples_in_graph.chunks(BATCH) {
            let triples = imported_triples_to_proto(chunk, &scope);
            let iris = imported_iris(chunk, &scope);
            match insert_proto_triples(&mut client, triples, iris, graph.clone()).await {
                Ok(n) => imported += n,
                Err(e) => return grpc_error(e),
            }
        }
    }

    let duration_ms = start.elapsed().as_millis() as u64;
    Json(serde_json::json!({
        "imported": imported,
        "total_parsed": total,
        "duration_ms": duration_ms,
        "import_id": scope.import_id(),
    }))
    .into_response()
}

// ── POST /export/jsonld ───────────────────────────────────────────────────────
//
// Export triples for a list of subjects as JSON-LD.
// For each subject × predicate combination, queries the gRPC service.
// Subjects can be IRI strings (mapped via uri_to_node_id) or UUID strings.

#[derive(Deserialize)]
struct ExportJsonLdBody {
    /// Subject IRIs or UUID strings to export.
    #[serde(default)]
    subjects: Vec<String>,
    /// Predicate IRIs to include. If empty, returns an empty @graph (predicates
    /// must be known; variable-predicate scan is not yet supported via gRPC).
    #[serde(default)]
    predicates: Vec<String>,
    /// Optional view/namespace label (informational, not yet enforced).
    #[serde(default)]
    #[allow(dead_code)]
    view_id: Option<String>,
    /// Render skolem IRIs as blank nodes (`_:label`).
    #[serde(default)]
    deskolemize: bool,
}

#[derive(Deserialize)]
struct ExportJsonLdParams {
    /// Single subject IRI or UUID (GET shorthand).
    subject: Option<String>,
    /// Comma-separated predicate IRIs (GET shorthand).
    predicates: Option<String>,
    /// Render skolem IRIs as blank nodes (`_:label`).
    #[serde(default)]
    deskolemize: bool,
}

async fn export_jsonld_for(
    state: Arc<AppState>,
    subjects: Vec<String>,
    predicates: Vec<String>,
    deskolemize: bool,
) -> Response {
    use polargraph_sparql::{node_id_to_iri, serialize_jsonld, uri_to_node_id, RdfTriple};

    if subjects.is_empty() {
        let empty = serialize_jsonld(&[]);
        return axum::response::Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/ld+json")
            .body(axum::body::boxed(axum::body::Full::from(empty)))
            .unwrap();
    }

    let mut all_rdf: Vec<RdfTriple> = Vec::new();
    let mut client = state.client.clone();

    for subject_iri in &subjects {
        // Resolve the subject to a NodeId: try UUID first, then IRI hash.
        let subj_node_id = if let Ok(u) = uuid::Uuid::parse_str(subject_iri) {
            polargraph_core::id::NodeId(u)
        } else {
            uri_to_node_id(subject_iri)
        };
        let subj_bytes = subj_node_id.0.as_bytes().to_vec();
        let subject_iri_str = format!("<urn:uuid:{}>", subj_node_id.0);

        if predicates.is_empty() {
            // Query all relation triples with this subject (predicates unknown).
            let req = proto::QueryRequest {
                patterns: vec![proto::VarPattern {
                    subject: Some(proto::Term {
                        kind: Some(proto::term::Kind::Bound(proto::NodeId {
                            bytes: subj_bytes.clone(),
                        })),
                    }),
                    predicate: String::new(),
                    object: Some(proto::Term {
                        kind: Some(proto::term::Kind::Var("_o".to_string())),
                    }),
                    predicate_var: "_p".to_string(),
                    graph: None,
                }],
                ..Default::default()
            };
            if let Ok(resp) = client.query(tonic::Request::new(req)).await {
                for pb in node_rows(resp.into_inner().bindings) {
                    if let Some(obj_val) = pb.vars.get("_o") {
                        if obj_val.bytes.len() == 16 {
                            if let Ok(arr) = obj_val.bytes[..16].try_into() {
                                let obj_id =
                                    polargraph_core::id::NodeId(uuid::Uuid::from_bytes(arr));
                                let predicate = pb
                                    .predicates
                                    .get("_p")
                                    .map(|p| format!("<{}>", p))
                                    .unwrap_or_else(|| {
                                        "<urn:polargraph:unknownPredicate>".to_string()
                                    });
                                all_rdf.push(RdfTriple {
                                    subject: subject_iri_str.clone(),
                                    predicate,
                                    object: node_id_to_iri(&obj_id),
                                });
                            }
                        }
                    }
                }
            }
        } else {
            for pred in &predicates {
                // Query for relation triples.
                let rel_req = proto::QueryRequest {
                    patterns: vec![proto::VarPattern {
                        subject: Some(proto::Term {
                            kind: Some(proto::term::Kind::Bound(proto::NodeId {
                                bytes: subj_bytes.clone(),
                            })),
                        }),
                        predicate: pred.clone(),
                        object: Some(proto::Term {
                            kind: Some(proto::term::Kind::Var("_o".to_string())),
                        }),
                        predicate_var: String::new(),
                        graph: None,
                    }],
                    ..Default::default()
                };
                if let Ok(resp) = client.query(tonic::Request::new(rel_req)).await {
                    for pb in node_rows(resp.into_inner().bindings) {
                        if let Some(obj_val) = pb.vars.get("_o") {
                            if obj_val.bytes.len() == 16 {
                                if let Ok(arr) = obj_val.bytes[..16].try_into() {
                                    let obj_id =
                                        polargraph_core::id::NodeId(uuid::Uuid::from_bytes(arr));
                                    all_rdf.push(RdfTriple {
                                        subject: subject_iri_str.clone(),
                                        predicate: format!("<{}>", pred),
                                        object: node_id_to_iri(&obj_id),
                                    });
                                }
                            }
                        }
                    }
                }

                // Query for property triples via SPARQL (handles scalar values).
                let sparql = format!(
                    "SELECT ?v WHERE {{ <urn:uuid:{}> <{}> ?v }}",
                    subj_node_id.0, pred
                );
                if let Ok(sparql_parsed) = spargebra::Query::parse(&sparql, None) {
                    if let Ok(translation) = polargraph_sparql::translate_query(&sparql_parsed) {
                        for branch in &translation.branches {
                            let patterns: Vec<proto::VarPattern> =
                                branch.patterns.iter().map(sparql_varpat_to_proto).collect();
                            let prop_req = proto::QueryRequest {
                                patterns,
                                ..Default::default()
                            };
                            if let Ok(resp) = client.query(tonic::Request::new(prop_req)).await {
                                for pb in node_rows(resp.into_inner().bindings) {
                                    // Node bindings only — property values come via
                                    // annotation steps (not in scope here).
                                    if let Some(obj_val) = pb.vars.get("v") {
                                        if obj_val.bytes.len() == 16 {
                                            if let Ok(arr) = obj_val.bytes[..16].try_into() {
                                                let obj_id = polargraph_core::id::NodeId(
                                                    uuid::Uuid::from_bytes(arr),
                                                );
                                                all_rdf.push(RdfTriple {
                                                    subject: subject_iri_str.clone(),
                                                    predicate: format!("<{}>", pred),
                                                    object: node_id_to_iri(&obj_id),
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let names = resolve_names(
        &mut state.client.clone(),
        polargraph_sparql::node_ids_in_triples(&all_rdf),
        deskolemize,
    )
    .await;
    names.rewrite_triples(&mut all_rdf);

    let body = serialize_jsonld(&all_rdf);
    axum::response::Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/ld+json")
        .body(axum::body::boxed(axum::body::Full::from(body)))
        .unwrap()
}

async fn handle_export_jsonld_get(
    State(state): State<Arc<AppState>>,
    QueryParams(params): QueryParams<ExportJsonLdParams>,
) -> Response {
    let subjects: Vec<String> = params.subject.into_iter().collect();
    let predicates: Vec<String> = params
        .predicates
        .map(|s| s.split(',').map(str::trim).map(str::to_string).collect())
        .unwrap_or_default();
    export_jsonld_for(state, subjects, predicates, params.deskolemize).await
}

async fn handle_export_jsonld_post(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ExportJsonLdBody>,
) -> Response {
    export_jsonld_for(state, body.subjects, body.predicates, body.deskolemize).await
}

// ── GET /export/subgraph ──────────────────────────────────────────────────────
//
// Export a set of nodes and their outgoing Relation edges as a self-contained
// RDF document (N-Triples, Turtle, or JSON-LD based on Accept header).
//
// Also exports edge annotations (EPA/EPO) as additional triples where the edge
// node id is the subject.

#[derive(Deserialize)]
struct ExportSubgraphParams {
    /// Comma-separated subjects to export: UUIDs or IRIs.
    subjects: Option<String>,
    /// Comma-separated predicate IRIs to include (if omitted: all, unknown).
    predicates: Option<String>,
    /// Render skolem IRIs as blank nodes (`_:label`).
    #[serde(default)]
    deskolemize: bool,
}

async fn handle_export_subgraph(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    QueryParams(params): QueryParams<ExportSubgraphParams>,
) -> Response {
    use polargraph_sparql::{node_id_to_iri, serialize_ntriples, serialize_turtle, RdfTriple};

    let subject_uuids: Vec<uuid::Uuid> = params
        .subjects
        .as_deref()
        .unwrap_or("")
        .split(',')
        .filter(|s| !s.trim().is_empty())
        .map(|s| {
            let s = s.trim();
            uuid::Uuid::parse_str(s).unwrap_or_else(|_| iri_to_node_id(s).0)
        })
        .collect();

    let predicates: Vec<String> = params
        .predicates
        .as_deref()
        .unwrap_or("")
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();

    let accept = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    // Quad formats query the default graph and each named graph (`?_g`)
    // separately so every triple keeps its graph; otherwise one union query.
    let quads = accept.contains("application/n-quads") || accept.contains("application/trig");
    let graph_terms: Vec<Option<proto::GraphTerm>> = if quads {
        vec![
            Some(proto::GraphTerm {
                kind: Some(proto::graph_term::Kind::DefaultGraph(true)),
            }),
            Some(proto::GraphTerm {
                kind: Some(proto::graph_term::Kind::Var("_g".to_string())),
            }),
        ]
    } else {
        vec![None]
    };

    let mut all_rdf: Vec<RdfTriple> = Vec::new();
    // Graph of each `all_rdf` entry as a `<urn:uuid:…>` term (None = default).
    let mut rdf_graphs: Vec<Option<String>> = Vec::new();
    let mut client = state.client.clone();

    for subj_uuid in &subject_uuids {
        let subj_node_id = polargraph_core::id::NodeId(*subj_uuid);
        let subj_bytes = subj_uuid.as_bytes().to_vec();
        let subject_iri = node_id_to_iri(&subj_node_id);

        let query_predicates = if predicates.is_empty() {
            vec![String::new()] // empty = wildcard
        } else {
            predicates.clone()
        };

        for pred in &query_predicates {
            for graph in &graph_terms {
                let req = proto::QueryRequest {
                    patterns: vec![proto::VarPattern {
                        subject: Some(proto::Term {
                            kind: Some(proto::term::Kind::Bound(proto::NodeId {
                                bytes: subj_bytes.clone(),
                            })),
                        }),
                        predicate: pred.clone(),
                        object: Some(proto::Term {
                            kind: Some(proto::term::Kind::Var("_o".to_string())),
                        }),
                        predicate_var: "_p".to_string(),
                        graph: graph.clone(),
                    }],
                    ..Default::default()
                };
                if let Ok(resp) = client.query(tonic::Request::new(req)).await {
                    for pb in node_rows(resp.into_inner().bindings) {
                        if let Some(obj_val) = pb.vars.get("_o") {
                            if obj_val.bytes.len() == 16 {
                                if let Ok(arr) = obj_val.bytes[..16].try_into() {
                                    let obj_id =
                                        polargraph_core::id::NodeId(uuid::Uuid::from_bytes(arr));
                                    let pred_iri = if !pred.is_empty() {
                                        format!("<{}>", pred)
                                    } else if let Some(p) = pb.predicates.get("_p") {
                                        format!("<{}>", p)
                                    } else {
                                        "<urn:polargraph:unknownPredicate>".to_string()
                                    };
                                    all_rdf.push(RdfTriple {
                                        subject: subject_iri.clone(),
                                        predicate: pred_iri,
                                        object: node_id_to_iri(&obj_id),
                                    });
                                    rdf_graphs.push(
                                        pb.vars
                                            .get("_g")
                                            .and_then(proto_node_id)
                                            .map(|g| node_id_to_iri(&g)),
                                    );
                                }
                            }
                        }
                    }
                }
            }

            // Fetch edge annotations: for each relation triple, also export annotations.
            // We derive the edge_id from the GetEdgeIdsByTriple RPC.
            if !pred.is_empty() {
                let req2 = proto::GetEdgeIdsByTripleRequest {
                    subject_id: subj_bytes.clone(),
                    predicate: pred.clone(),
                    object_id: vec![],
                };
                if let Ok(resp) = client
                    .get_edge_ids_by_triple(tonic::Request::new(req2))
                    .await
                {
                    for edge_id_bytes in resp.into_inner().edge_ids {
                        if edge_id_bytes.len() == 16 {
                            if let Ok(arr) = edge_id_bytes[..16].try_into() {
                                let edge_uuid: uuid::Uuid = uuid::Uuid::from_bytes(arr);
                                let edge_node_id = polargraph_core::id::NodeId(edge_uuid);
                                let edge_subject_iri = node_id_to_iri(&edge_node_id);
                                // Fetch annotations for this edge.
                                let ann_req = proto::GetEdgeAnnotationsRequest {
                                    edge_id: edge_id_bytes.clone(),
                                };
                                if let Ok(ann_resp) = client
                                    .get_edge_annotations(tonic::Request::new(ann_req))
                                    .await
                                {
                                    for ann in ann_resp.into_inner().annotations {
                                        let pred_iri = format!("<{}>", ann.predicate);
                                        let obj_str = match ann.value {
                                            Some(proto::edge_annotation::Value::NodeId(bytes))
                                                if bytes.len() == 16 =>
                                            {
                                                if let Ok(a) = bytes[..16].try_into() {
                                                    let nid = polargraph_core::id::NodeId(
                                                        uuid::Uuid::from_bytes(a),
                                                    );
                                                    node_id_to_iri(&nid)
                                                } else {
                                                    continue;
                                                }
                                            }
                                            Some(proto::edge_annotation::Value::Scalar(v)) => {
                                                use polargraph_core::value::Value as V;
                                                let pg_val = match v.kind {
                                                    Some(proto::value::Kind::TextVal(s)) => {
                                                        V::Text(s)
                                                    }
                                                    Some(proto::value::Kind::IntVal(n)) => {
                                                        V::Int(n)
                                                    }
                                                    Some(proto::value::Kind::FloatVal(f)) => {
                                                        V::Float(f)
                                                    }
                                                    Some(proto::value::Kind::BoolVal(b)) => {
                                                        V::Bool(b)
                                                    }
                                                    _ => continue,
                                                };
                                                polargraph_sparql::value_to_nt_literal(&pg_val)
                                            }
                                            _ => continue,
                                        };
                                        all_rdf.push(RdfTriple {
                                            subject: edge_subject_iri.clone(),
                                            predicate: pred_iri,
                                            object: obj_str,
                                        });
                                        rdf_graphs.push(None);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let mut ids = polargraph_sparql::node_ids_in_triples(&all_rdf);
    ids.extend(
        rdf_graphs
            .iter()
            .flatten()
            .filter_map(|g| {
                g.strip_prefix("<urn:uuid:")?
                    .strip_suffix('>')?
                    .parse()
                    .ok()
            })
            .map(NodeId),
    );
    let names = resolve_names(&mut state.client.clone(), ids, params.deskolemize).await;
    names.rewrite_triples(&mut all_rdf);

    if quads {
        let quads: Vec<polargraph_sparql::RdfQuad> = all_rdf
            .into_iter()
            .zip(rdf_graphs)
            .map(|(triple, graph)| polargraph_sparql::RdfQuad {
                triple,
                graph: graph.map(|g| names.rewrite_term(&g)),
            })
            .collect();
        let (content_type, body) = if accept.contains("application/trig") {
            (
                "application/trig",
                polargraph_sparql::serialize_trig(&quads),
            )
        } else {
            (
                "application/n-quads",
                polargraph_sparql::serialize_nquads(&quads),
            )
        };
        return axum::response::Response::builder()
            .status(StatusCode::OK)
            .header("content-type", content_type)
            .body(axum::body::boxed(axum::body::Full::from(body)))
            .unwrap();
    }

    if accept.contains("application/ld+json") {
        let body = polargraph_sparql::serialize_jsonld(&all_rdf);
        return axum::response::Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/ld+json")
            .body(axum::body::boxed(axum::body::Full::from(body)))
            .unwrap();
    }

    if accept.contains("text/turtle") {
        let body = serialize_turtle(&all_rdf);
        return axum::response::Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/turtle")
            .body(axum::body::boxed(axum::body::Full::from(body)))
            .unwrap();
    }

    // Default: N-Triples.
    let body = serialize_ntriples(&all_rdf);
    axum::response::Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/n-triples")
        .body(axum::body::boxed(axum::body::Full::from(body)))
        .unwrap()
}

// ── POST /import/subgraph ─────────────────────────────────────────────────────
//
// Accept the same formats as /import/rdf. Functionally identical — the
// separate endpoint exists to allow point-to-point PolarGraph transfer without
// callers needing to know the Content-Type routing logic.

async fn handle_import_subgraph(
    state: State<Arc<AppState>>,
    params: QueryParams<ImportRdfParams>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    handle_import_rdf(state, params, headers, body).await
}

// ── GET /schema/rdf ───────────────────────────────────────────────────────────
//
// Export registered node types and edge types as OWL/RDFS Turtle.

async fn handle_schema_rdf_get(State(state): State<Arc<AppState>>) -> Response {
    use polargraph_sparql::{serialize_schema_rdf, SchemaEdgeType, SchemaField, SchemaNodeType};

    let mut client = state.client.clone();

    let node_types_resp = match client
        .list_node_types(tonic::Request::new(proto::ListNodeTypesRequest {}))
        .await
    {
        Ok(r) => r.into_inner(),
        Err(e) => return grpc_error(e),
    };

    let edge_types_resp = match client
        .list_edge_types(tonic::Request::new(proto::ListEdgeTypesRequest {}))
        .await
    {
        Ok(r) => r.into_inner(),
        Err(e) => return grpc_error(e),
    };

    let schema_nodes: Vec<SchemaNodeType> = node_types_resp
        .definitions
        .into_iter()
        .map(|nt| SchemaNodeType {
            type_name: nt.type_name,
            fields: nt
                .fields
                .into_iter()
                .map(|f| SchemaField {
                    name: f.field_name,
                    kind: f.kind,
                    required: f.required,
                })
                .collect(),
            parent_types: nt.parent_types,
        })
        .collect();

    let schema_edges: Vec<SchemaEdgeType> = edge_types_resp
        .definitions
        .into_iter()
        .map(|et| SchemaEdgeType {
            predicate: et.predicate,
            domain: et.domain,
            range: et.range,
            fields: et
                .fields
                .into_iter()
                .map(|f| SchemaField {
                    name: f.field_name,
                    kind: f.kind,
                    required: f.required,
                })
                .collect(),
        })
        .collect();

    let turtle = serialize_schema_rdf(&schema_nodes, &schema_edges);

    axum::response::Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/turtle")
        .body(axum::body::boxed(axum::body::Full::from(turtle)))
        .unwrap()
}

// ── POST /schema/rdf ──────────────────────────────────────────────────────────
//
// Accept OWL/RDFS Turtle and register node/edge types.

async fn handle_schema_rdf_post(
    State(state): State<Arc<AppState>>,
    body: axum::body::Bytes,
) -> Response {
    use polargraph_sparql::parse_schema_rdf;

    let (schema_nodes, schema_edges) = match parse_schema_rdf(&body) {
        Ok(pair) => pair,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response()
        }
    };

    let mut client = state.client.clone();
    let mut registered_nodes = 0usize;
    let mut registered_edges = 0usize;

    for sn in &schema_nodes {
        let req = proto::RegisterNodeTypeRequest {
            definition: Some(proto::NodeTypeDef {
                type_name: sn.type_name.clone(),
                fields: sn
                    .fields
                    .iter()
                    .map(|f| proto::FieldDef {
                        field_name: f.name.clone(),
                        kind: f.kind.clone(),
                        required: f.required,
                    })
                    .collect(),
                parent_types: sn.parent_types.clone(),
                vector_space: None,
            }),
        };
        match client.register_node_type(tonic::Request::new(req)).await {
            Ok(_) => registered_nodes += 1,
            Err(e) => return grpc_error(e),
        }
    }

    for se in &schema_edges {
        let req = proto::RegisterEdgeTypeRequest {
            definition: Some(proto::EdgeTypeDef {
                predicate: se.predicate.clone(),
                domain: se.domain.clone(),
                range: se.range.clone(),
                fields: se
                    .fields
                    .iter()
                    .map(|f| proto::FieldDef {
                        field_name: f.name.clone(),
                        kind: f.kind.clone(),
                        required: f.required,
                    })
                    .collect(),
                cardinality: String::new(),
                inverse_of: String::new(),
            }),
        };
        match client.register_edge_type(tonic::Request::new(req)).await {
            Ok(_) => registered_edges += 1,
            Err(e) => return grpc_error(e),
        }
    }

    Json(serde_json::json!({
        "ok": true,
        "registered_node_types": registered_nodes,
        "registered_edge_types": registered_edges,
    }))
    .into_response()
}

// ── Named graphs ──────────────────────────────────────────────────────────────
//
// Graph IRIs travel in the body or the `iri` query parameter rather than the
// path (IRIs contain slashes). An empty / missing IRI means the default graph
// where that makes sense (stats, copy/move source or target, drop).

#[derive(Deserialize)]
struct CreateGraphBody {
    iri: String,
    /// Metadata properties: predicate → JSON value (same encoding as other
    /// property values, incl. `{"@value", "@language"}` objects).
    #[serde(default)]
    metadata: std::collections::BTreeMap<String, serde_json::Value>,
}

#[derive(Deserialize)]
struct GraphIriParams {
    #[serde(default)]
    iri: String,
    #[serde(default)]
    include_system: bool,
}

#[derive(Deserialize)]
struct CopyGraphBody {
    #[serde(default)]
    source: String,
    #[serde(default)]
    target: String,
    /// true = COPY (replace the target), false = ADD.
    #[serde(default = "default_true")]
    clear_target: bool,
}

fn default_true() -> bool {
    true
}

fn graph_info_json(g: &proto::GraphInfo) -> serde_json::Value {
    let metadata: serde_json::Map<String, serde_json::Value> = g
        .metadata
        .iter()
        .map(|m| {
            (
                m.predicate.clone(),
                m.value
                    .as_ref()
                    .map(proto_value_to_json)
                    .unwrap_or(serde_json::Value::Null),
            )
        })
        .collect();
    serde_json::json!({ "iri": g.iri, "id": g.id, "metadata": metadata })
}

async fn handle_create_graph(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateGraphBody>,
) -> Response {
    let req = proto::CreateGraphRequest {
        user_id: String::new(),
        iri: body.iri,
        metadata: body
            .metadata
            .iter()
            .map(|(predicate, v)| proto::GraphMetadata {
                predicate: predicate.clone(),
                value: Some(json_to_proto_value(v)),
            })
            .collect(),
    };
    match state
        .client
        .clone()
        .create_graph(tonic::Request::new(req))
        .await
    {
        Ok(r) => match r.into_inner().graph {
            Some(g) => Json(graph_info_json(&g)).into_response(),
            None => Json(serde_json::json!({})).into_response(),
        },
        Err(e) => grpc_error(e),
    }
}

async fn handle_list_graphs(
    State(state): State<Arc<AppState>>,
    QueryParams(params): QueryParams<GraphIriParams>,
) -> Response {
    let req = proto::ListGraphsRequest {
        filter: vec![],
        include_system: params.include_system,
    };
    match state
        .client
        .clone()
        .list_graphs(tonic::Request::new(req))
        .await
    {
        Ok(r) => {
            let graphs: Vec<_> = r.into_inner().graphs.iter().map(graph_info_json).collect();
            Json(serde_json::json!({ "graphs": graphs })).into_response()
        }
        Err(e) => grpc_error(e),
    }
}

async fn handle_graph_stats(
    State(state): State<Arc<AppState>>,
    QueryParams(params): QueryParams<GraphIriParams>,
) -> Response {
    let req = proto::GraphStatsRequest { iri: params.iri };
    match state
        .client
        .clone()
        .graph_stats(tonic::Request::new(req))
        .await
    {
        Ok(r) => {
            let s = r.into_inner();
            Json(serde_json::json!({
                "iri": s.iri,
                "live_quads": s.live_quads,
                "last_write_tt": s.last_write_tt,
            }))
            .into_response()
        }
        Err(e) => grpc_error(e),
    }
}

#[derive(Deserialize)]
struct GraphAccessBody {
    /// User or group: node UUID or IRI.
    principal: String,
    graph: String,
    /// `read`, `propose`, `write` or `admin`.
    level: String,
}

#[derive(Deserialize)]
struct GraphAccessParams {
    principal: String,
    #[serde(default)]
    graph: String,
}

// ── Vocabulary (docs/design/cypher-rdf.md) ───────────────────────────────────

fn vocabulary_json(v: proto::Vocabulary) -> serde_json::Value {
    let prefixes: serde_json::Map<String, serde_json::Value> = v
        .prefixes
        .into_iter()
        .map(|p| (p.name, serde_json::Value::String(p.namespace)))
        .collect();
    serde_json::json!({
        "base": v.base,
        "prefixes": prefixes,
        "legacy": v.legacy.map(legacy_json),
    })
}

fn legacy_json(l: proto::LegacyStatus) -> serde_json::Value {
    serde_json::json!({
        "conversion_pending": l.conversion_pending,
        "bare_predicates": l.bare_predicates,
        "type_labels": l.type_labels,
        "pending_merges": l.pending_merges,
    })
}

fn vocabulary_reply(r: Result<tonic::Response<proto::Vocabulary>, tonic::Status>) -> Response {
    match r {
        Ok(r) => Json(vocabulary_json(r.into_inner())).into_response(),
        Err(e) => grpc_error(e),
    }
}

/// `GET /vocabulary` — base, prefixes and legacy-conversion status.
async fn handle_get_vocabulary(State(state): State<Arc<AppState>>) -> Response {
    vocabulary_reply(
        state
            .client
            .clone()
            .get_vocabulary(tonic::Request::new(proto::GetVocabularyRequest {}))
            .await,
    )
}

#[derive(Deserialize)]
struct VocabularyBaseBody {
    base: String,
}

/// `PUT /vocabulary/base {base}` — set the base IRI for bare names.
async fn handle_set_vocabulary_base(
    State(state): State<Arc<AppState>>,
    Json(body): Json<VocabularyBaseBody>,
) -> Response {
    let req = proto::SetVocabularyBaseRequest {
        base: body.base,
        user_id: String::new(),
    };
    vocabulary_reply(
        state
            .client
            .clone()
            .set_vocabulary_base(tonic::Request::new(req))
            .await,
    )
}

#[derive(Deserialize)]
struct PrefixBody {
    name: String,
    namespace: String,
}

/// `POST /vocabulary/prefixes {name, namespace}` — declare or re-point a prefix.
async fn handle_put_prefix(
    State(state): State<Arc<AppState>>,
    Json(body): Json<PrefixBody>,
) -> Response {
    let req = proto::PutPrefixRequest {
        name: body.name,
        namespace: body.namespace,
        user_id: String::new(),
    };
    vocabulary_reply(
        state
            .client
            .clone()
            .put_prefix(tonic::Request::new(req))
            .await,
    )
}

#[derive(Deserialize)]
struct PrefixParams {
    name: String,
}

/// `DELETE /vocabulary/prefixes?name=` — remove a prefix.
async fn handle_remove_prefix(
    State(state): State<Arc<AppState>>,
    QueryParams(params): QueryParams<PrefixParams>,
) -> Response {
    let req = proto::RemovePrefixRequest {
        name: params.name,
        user_id: String::new(),
    };
    vocabulary_reply(
        state
            .client
            .clone()
            .remove_prefix(tonic::Request::new(req))
            .await,
    )
}

#[derive(Deserialize, Default)]
struct ConvertBody {
    #[serde(default)]
    dry_run: bool,
}

/// `POST /vocabulary/convert {dry_run?}` — one-time conversion of
/// pre-vocabulary data (idempotent, resumable).
async fn handle_convert_legacy(
    State(state): State<Arc<AppState>>,
    body: Option<Json<ConvertBody>>,
) -> Response {
    let req = proto::ConvertLegacyDataRequest {
        dry_run: body.map(|Json(b)| b.dry_run).unwrap_or_default(),
        user_id: String::new(),
    };
    match state
        .client
        .clone()
        .convert_legacy_data(tonic::Request::new(req))
        .await
    {
        Ok(r) => {
            let r = r.into_inner();
            let predicates: Vec<_> = r
                .predicates
                .into_iter()
                .map(|p| {
                    serde_json::json!({
                        "from": p.from, "to": p.to,
                        "merged": p.merged, "quads_moved": p.quads_moved,
                    })
                })
                .collect();
            Json(serde_json::json!({
                "dry_run": r.dry_run,
                "predicates": predicates,
                "labels_converted": r.labels_converted,
                "legacy": r.legacy.map(legacy_json),
            }))
            .into_response()
        }
        Err(e) => grpc_error(e),
    }
}

/// `POST /graphs/access {principal, graph, level}` — grant (caller from
/// `X-User-Id` must be admin of the graph, unless it is a service call).
async fn handle_grant_graph_access(
    State(state): State<Arc<AppState>>,
    Json(body): Json<GraphAccessBody>,
) -> Response {
    let req = proto::GrantGraphAccessRequest {
        principal: body.principal,
        graph: body.graph,
        level: body.level,
        user_id: String::new(),
    };
    match state
        .client
        .clone()
        .grant_graph_access(tonic::Request::new(req))
        .await
    {
        Ok(_) => Json(serde_json::json!({ "ok": true })).into_response(),
        Err(e) => grpc_error(e),
    }
}

/// `DELETE /graphs/access?principal=&graph=` — revoke.
async fn handle_revoke_graph_access(
    State(state): State<Arc<AppState>>,
    QueryParams(params): QueryParams<GraphAccessParams>,
) -> Response {
    let req = proto::RevokeGraphAccessRequest {
        principal: params.principal,
        graph: params.graph,
        user_id: String::new(),
    };
    match state
        .client
        .clone()
        .revoke_graph_access(tonic::Request::new(req))
        .await
    {
        Ok(r) => Json(serde_json::json!({ "revoked": r.into_inner().revoked })).into_response(),
        Err(e) => grpc_error(e),
    }
}

/// `GET /graphs/access?principal=` — a user's effective graph access.
async fn handle_get_graph_access(
    State(state): State<Arc<AppState>>,
    QueryParams(params): QueryParams<GraphAccessParams>,
) -> Response {
    let req = proto::GetGraphAccessRequest {
        principal: params.principal,
        user_id: String::new(),
    };
    match state
        .client
        .clone()
        .get_graph_access(tonic::Request::new(req))
        .await
    {
        Ok(r) => {
            let graphs: Vec<_> = r
                .into_inner()
                .graphs
                .into_iter()
                .map(|g| serde_json::json!({ "graph": g.graph, "level": g.level }))
                .collect();
            Json(serde_json::json!({ "graphs": graphs })).into_response()
        }
        Err(e) => grpc_error(e),
    }
}

#[derive(Deserialize)]
struct ExportGraphParams {
    /// Graph IRI; empty = the default graph.
    #[serde(default)]
    iri: String,
    /// Export every graph (default + named) instead of `iri`.
    #[serde(default)]
    all: bool,
    /// Render skolem IRIs as blank nodes (`_:label`).
    #[serde(default)]
    deskolemize: bool,
}

/// `GET /graphs/export?iri=<graph>|all=true` — live quads of one graph or of
/// the whole dataset. N-Quads by default; `Accept: application/trig` for
/// TriG. A single graph can also be fetched as N-Triples, Turtle or JSON-LD
/// (graph label dropped); `all=true` requires a quad format (406 otherwise).
async fn handle_export_graph(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    QueryParams(params): QueryParams<ExportGraphParams>,
) -> Response {
    use polargraph_sparql::{node_id_to_iri, value_to_nt_literal, RdfQuad, RdfTriple};

    let accept = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let triple_format = [
        "application/n-triples",
        "text/turtle",
        "application/ld+json",
    ]
    .into_iter()
    .find(|f| accept.contains(f));
    if params.all && triple_format.is_some() && !accept.contains("application/trig") {
        return (
            StatusCode::NOT_ACCEPTABLE,
            Json(serde_json::json!({
                "error": "all=true needs a quad format: application/n-quads or application/trig"
            })),
        )
            .into_response();
    }

    let mut client = state.client.clone();
    let req = proto::ExportGraphRequest {
        iri: params.iri,
        all_graphs: params.all,
    };
    let mut stream = match client.export_graph(tonic::Request::new(req)).await {
        Ok(r) => r.into_inner(),
        Err(e) => return grpc_error(e),
    };
    let mut triples: Vec<RdfTriple> = Vec::new();
    let mut graphs: Vec<Option<String>> = Vec::new();
    loop {
        let chunk = match stream.message().await {
            Ok(Some(chunk)) => chunk,
            Ok(None) => break,
            Err(e) => return grpc_error(e),
        };
        for q in chunk.quads {
            let Some(subject) = q.subject.as_ref().and_then(proto_node_id) else {
                continue;
            };
            let object = match q.object {
                Some(proto::exported_quad::Object::Node(n)) => match proto_node_id(&n) {
                    Some(id) => node_id_to_iri(&id),
                    None => continue,
                },
                Some(proto::exported_quad::Object::Value(v)) => match proto_value_to_pg(&v) {
                    Some(v) => value_to_nt_literal(&v),
                    None => continue,
                },
                None => continue,
            };
            triples.push(RdfTriple {
                subject: node_id_to_iri(&subject),
                predicate: format!("<{}>", q.predicate),
                object,
            });
            graphs.push((!q.graph.is_empty()).then(|| format!("<{}>", q.graph)));
        }
    }

    let names = resolve_names(
        &mut client,
        polargraph_sparql::node_ids_in_triples(&triples),
        params.deskolemize,
    )
    .await;
    names.rewrite_triples(&mut triples);

    let (content_type, body) = if accept.contains("application/trig") {
        let quads: Vec<RdfQuad> = triples
            .into_iter()
            .zip(graphs)
            .map(|(triple, graph)| RdfQuad { triple, graph })
            .collect();
        (
            "application/trig",
            polargraph_sparql::serialize_trig(&quads),
        )
    } else {
        match triple_format {
            Some("text/turtle") => ("text/turtle", polargraph_sparql::serialize_turtle(&triples)),
            Some("application/ld+json") => (
                "application/ld+json",
                polargraph_sparql::serialize_jsonld(&triples),
            ),
            Some(_) => (
                "application/n-triples",
                polargraph_sparql::serialize_ntriples(&triples),
            ),
            None => {
                let quads: Vec<RdfQuad> = triples
                    .into_iter()
                    .zip(graphs)
                    .map(|(triple, graph)| RdfQuad { triple, graph })
                    .collect();
                (
                    "application/n-quads",
                    polargraph_sparql::serialize_nquads(&quads),
                )
            }
        }
    };
    axum::response::Response::builder()
        .status(StatusCode::OK)
        .header("content-type", content_type)
        .body(axum::body::boxed(axum::body::Full::from(body)))
        .unwrap()
}

/// A 16-byte proto NodeId as a [`NodeId`].
fn proto_node_id(n: &proto::NodeId) -> Option<NodeId> {
    let arr: [u8; 16] = n.bytes.as_slice().try_into().ok()?;
    Some(NodeId(uuid::Uuid::from_bytes(arr)))
}

/// Inverse of [`pg_value_to_proto`]; `None` for an empty value.
fn proto_value_to_pg(v: &proto::Value) -> Option<polargraph_core::value::Value> {
    use polargraph_core::value::Value as V;
    use proto::value::Kind;
    Some(match v.kind.as_ref()? {
        Kind::NullVal(_) => V::Null,
        Kind::BoolVal(b) => V::Bool(*b),
        Kind::IntVal(n) => V::Int(*n),
        Kind::FloatVal(f) => V::Float(*f),
        Kind::TextVal(s) => V::Text(s.clone()),
        Kind::BlobVal(b) => V::Blob(b.clone()),
        Kind::VecVal(a) => V::Vector(a.values.clone()),
        Kind::LangText(l) => V::LangText {
            text: l.text.clone(),
            lang: l.lang.clone(),
        },
        Kind::Typed(t) => V::Typed {
            lexical: t.lexical.clone(),
            datatype: t.datatype.clone(),
        },
    })
}

async fn handle_copy_graph(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CopyGraphBody>,
) -> Response {
    let req = proto::CopyGraphRequest {
        user_id: String::new(),
        source: body.source,
        target: body.target,
        clear_target: body.clear_target,
    };
    match state
        .client
        .clone()
        .copy_graph(tonic::Request::new(req))
        .await
    {
        Ok(r) => Json(serde_json::json!({ "quads": r.into_inner().quads })).into_response(),
        Err(e) => grpc_error(e),
    }
}

async fn handle_move_graph(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CopyGraphBody>,
) -> Response {
    let req = proto::MoveGraphRequest {
        user_id: String::new(),
        source: body.source,
        target: body.target,
    };
    match state
        .client
        .clone()
        .move_graph(tonic::Request::new(req))
        .await
    {
        Ok(r) => Json(serde_json::json!({ "quads": r.into_inner().quads })).into_response(),
        Err(e) => grpc_error(e),
    }
}

async fn handle_drop_graph(
    State(state): State<Arc<AppState>>,
    QueryParams(params): QueryParams<GraphIriParams>,
) -> Response {
    let req = proto::DropGraphRequest {
        iri: params.iri,
        user_id: String::new(),
    };
    match state
        .client
        .clone()
        .drop_graph(tonic::Request::new(req))
        .await
    {
        Ok(r) => {
            Json(serde_json::json!({ "quads_closed": r.into_inner().quads_closed })).into_response()
        }
        Err(e) => grpc_error(e),
    }
}

// ── GET /stats ────────────────────────────────────────────────────────────────

async fn handle_stats(State(state): State<Arc<AppState>>) -> Response {
    let mut client = state.client.clone();
    match client
        .show_stats(tonic::Request::new(proto::ShowStatsRequest {}))
        .await
    {
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
            }))
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
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
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

    let client = PolarGraphServiceClient::with_interceptor(
        channel,
        AuthInterceptor {
            token: args.api_key,
        },
    );

    let state = Arc::new(AppState {
        client,
        skolem_base: args.skolem_base.clone(),
    });

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
        .route(
            "/edge-annotations/:edge_id",
            get(handle_get_edge_annotations),
        )
        .route("/property-history", get(handle_property_history))
        .route("/access/grant", post(handle_grant_access))
        .route("/access/revoke", post(handle_revoke_access))
        .route("/access/add-user", post(handle_add_user_to_group))
        .route("/access/user/:user_id", get(handle_get_user_access))
        .route("/sparql", get(handle_sparql_get).post(handle_sparql_post))
        .route("/sparql/update", post(handle_sparql_update))
        .route("/delete", post(handle_delete_triples))
        .route("/materialize", post(handle_materialize))
        .route("/import/rdf", post(handle_import_rdf))
        .route("/import/subgraph", post(handle_import_subgraph))
        .route(
            "/export/jsonld",
            get(handle_export_jsonld_get).post(handle_export_jsonld_post),
        )
        .route("/export/subgraph", get(handle_export_subgraph))
        .route(
            "/schema/rdf",
            get(handle_schema_rdf_get).post(handle_schema_rdf_post),
        )
        .route(
            "/graphs",
            get(handle_list_graphs)
                .post(handle_create_graph)
                .delete(handle_drop_graph),
        )
        .route("/graphs/stats", get(handle_graph_stats))
        .route("/graphs/export", get(handle_export_graph))
        .route("/graphs/copy", post(handle_copy_graph))
        .route("/graphs/move", post(handle_move_graph))
        .route("/subscribe", get(handle_subscribe))
        .route("/changes", post(handle_apply_changes))
        .route("/validate", post(handle_validate))
        .route("/vocabulary", get(handle_get_vocabulary))
        .route("/vocabulary/base", put(handle_set_vocabulary_base))
        .route(
            "/vocabulary/prefixes",
            post(handle_put_prefix).delete(handle_remove_prefix),
        )
        .route("/vocabulary/convert", post(handle_convert_legacy))
        .route(
            "/graphs/access",
            get(handle_get_graph_access)
                .post(handle_grant_graph_access)
                .delete(handle_revoke_graph_access),
        )
        .layer(axum::middleware::from_fn(forward_user_id))
        .with_state(state);

    info!(addr = %args.listen, upstream = %args.upstream, "polargraph-rest listening");
    axum::Server::bind(&args.listen)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {

    #[test]
    fn query_rows_carry_value_bindings_under_at_values() {
        let node = proto::NodeId {
            bytes: Uuid::nil().as_bytes().to_vec(),
        };
        let vars = std::collections::HashMap::from([("b".to_string(), node)]);
        let values = std::collections::HashMap::from([(
            "t".to_string(),
            proto::Value {
                kind: Some(proto::value::Kind::TextVal("Dune".into())),
            },
        )]);
        let row = query_row_json(vars.clone(), values);
        assert_eq!(row["b"], Uuid::nil().to_string());
        assert_eq!(row["@values"]["t"], "Dune");
        // No value bindings: the row shape is unchanged.
        let row = query_row_json(vars, Default::default());
        assert!(row.get("@values").is_none());
    }

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
        assert_eq!(
            grpc_to_http_status(Code::Unauthenticated),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            grpc_to_http_status(Code::PermissionDenied),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            grpc_to_http_status(Code::DeadlineExceeded),
            StatusCode::REQUEST_TIMEOUT
        );
        assert_eq!(
            grpc_to_http_status(Code::InvalidArgument),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            grpc_to_http_status(Code::Internal),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    /// attach_user_id sets the x-polargraph-user-id metadata header for a
    /// non-empty user_id and leaves the request unmodified for an empty one.
    #[test]
    fn validation_report_turtle_renders_results_and_paths() {
        assert_eq!(path_turtle("<http://ex/p>"), "<http://ex/p>");
        assert_eq!(
            path_turtle("^<http://ex/p>"),
            "[ sh:inversePath <http://ex/p> ]"
        );
        assert_eq!(
            path_turtle("<http://ex/a>/^<http://ex/b>"),
            "( <http://ex/a> [ sh:inversePath <http://ex/b> ] )"
        );
        let resp = proto::ValidateShapesResponse {
            conforms: false,
            no_violations: false,
            results: vec![proto::ValidationResult {
                focus_node: "http://ex/api".into(),
                path: "<http://ex/owner>".into(),
                value_literal: Some(proto::Value {
                    kind: Some(proto::value::Kind::TextVal("x".into())),
                }),
                source_shape: "http://ex/S".into(),
                constraint_component: "http://www.w3.org/ns/shacl#PatternConstraintComponent"
                    .into(),
                severity: "http://www.w3.org/ns/shacl#Violation".into(),
                message: "no \"match\"".into(),
                ..Default::default()
            }],
        };
        let ttl = validation_report_turtle(&resp);
        assert!(ttl.contains("sh:conforms false"));
        assert!(ttl.contains("sh:focusNode <http://ex/api>"));
        assert!(ttl.contains("sh:resultPath <http://ex/owner>"));
        assert!(ttl.contains("sh:resultMessage \"no \\\"match\\\"\""));
    }

    #[test]
    fn change_event_json_renders_nodes_and_kind() {
        let id = uuid::Uuid::now_v7();
        let event = proto::ChangeEvent {
            commit_ts: 42,
            graph: "urn:g:1".into(),
            kind: proto::ChangeKind::Close as i32,
            quad: Some(proto::Triple {
                kind: Some(proto::triple::Kind::Property(proto::PropertyTriple {
                    subject: Some(proto::NodeId {
                        bytes: id.as_bytes().to_vec(),
                    }),
                    predicate: "name".into(),
                    value: None,
                    vt_start: 1,
                    vt_end: 2,
                    mode: 0,
                })),
            }),
            author: "urn:user:alice".into(),
            ..Default::default()
        };
        let names = polargraph_sparql::IriNames::new(
            std::collections::HashMap::from([(NodeId(id), "http://ex/a".to_string())]),
            false,
        );
        let json = change_event_json(&event, &names);
        assert_eq!(json["kind"], "close");
        assert_eq!(json["subject"], "http://ex/a");
        assert_eq!(json["author"], "urn:user:alice");
        assert!(json.get("value").is_none());
        assert_eq!(change_event_nodes(&event), vec![NodeId(id)]);
    }

    #[tokio::test]
    async fn interceptor_forwards_the_request_user() {
        use tonic::service::Interceptor;
        let mut interceptor = AuthInterceptor { token: None };
        let req = REQUEST_USER
            .scope("urn:user:alice".to_string(), async {
                interceptor.call(tonic::Request::new(())).unwrap()
            })
            .await;
        assert_eq!(
            req.metadata().get("x-polargraph-user-id").unwrap(),
            "urn:user:alice"
        );
        // Outside a request scope, and with an explicit header, nothing changes.
        let req = interceptor.call(tonic::Request::new(())).unwrap();
        assert!(req.metadata().get("x-polargraph-user-id").is_none());
        let req = REQUEST_USER
            .scope("urn:user:alice".to_string(), async {
                interceptor
                    .call(attach_user_id(tonic::Request::new(()), "urn:user:bob"))
                    .unwrap()
            })
            .await;
        assert_eq!(
            req.metadata().get("x-polargraph-user-id").unwrap(),
            "urn:user:bob"
        );
    }

    #[test]
    fn attach_user_id_sets_metadata_when_non_empty() {
        let req = tonic::Request::new(());
        let req = attach_user_id(req, "alice-uuid-123");
        assert_eq!(
            req.metadata()
                .get("x-polargraph-user-id")
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

    #[test]
    fn json_ld_value_objects_round_trip_through_proto() {
        let lang = serde_json::json!({ "@value": "Acme", "@language": "en" });
        let typed = serde_json::json!({
            "@value": "2026-09-29",
            "@type": "http://www.w3.org/2001/XMLSchema#date"
        });
        for v in [lang, typed] {
            assert_eq!(proto_value_to_json(&json_to_proto_value(&v)), v);
        }
    }

    #[test]
    fn graph_copy_shape_recognises_add_copy_move() {
        let shapes = |update: &str| -> Vec<Option<(String, String)>> {
            spargebra::Update::parse(update, None)
                .unwrap()
                .operations
                .iter()
                .filter_map(|op| match op {
                    spargebra::GraphUpdateOperation::DeleteInsert {
                        delete,
                        insert,
                        using,
                        pattern,
                    } => Some(graph_copy_shape(delete, insert, using.as_ref(), pattern)),
                    _ => None,
                })
                .collect()
        };
        let pair = |a: &str, b: &str| Some((a.to_string(), b.to_string()));
        assert_eq!(
            shapes("ADD <urn:a> TO <urn:b>"),
            vec![pair("urn:a", "urn:b")]
        );
        assert_eq!(shapes("COPY DEFAULT TO <urn:b>"), vec![pair("", "urn:b")]);
        assert_eq!(shapes("MOVE <urn:a> TO DEFAULT"), vec![pair("urn:a", "")]);
        // A general INSERT … WHERE is not a copy.
        assert_eq!(
            shapes("INSERT { GRAPH <urn:b> { ?s ?p ?x } } WHERE { ?s ?p ?o }"),
            vec![None]
        );
        assert_eq!(
            shapes("INSERT { GRAPH <urn:b> { ?s ?p ?o } } WHERE { ?s ?p ?o . ?o ?q ?r }"),
            vec![None]
        );
    }

    #[test]
    fn parse_pattern_graph_suffix() {
        use proto::graph_term::Kind;
        let kind = |s: &str| parse_pattern(s).unwrap().graph.and_then(|g| g.kind);
        assert_eq!(kind("?s :knows ?o"), None);
        assert_eq!(
            kind("?s :knows ?o @default"),
            Some(Kind::DefaultGraph(true))
        );
        assert_eq!(kind("?s :knows ?o @?g"), Some(Kind::Var("g".into())));
        assert_eq!(
            kind("?s :knows ?o @<https://kb.example/g/eng>"),
            Some(Kind::Iri("https://kb.example/g/eng".into()))
        );
        assert!(parse_pattern("?s :knows ?o extra").is_err());
        assert!(parse_pattern("?s :knows ?o @?").is_err());
    }

    #[test]
    fn import_id_validation() {
        assert!(valid_import_id("crm-2026-09-29.v1"));
        assert!(valid_import_id(&Uuid::now_v7().to_string()));
        assert!(!valid_import_id(""));
        assert!(!valid_import_id("a/b"));
        assert!(!valid_import_id("has space"));
        assert!(!valid_import_id(&"x".repeat(129)));
    }

    #[test]
    fn imported_iris_are_distinct_hashed_iris_including_skolems() {
        use polargraph_sparql::{parse_ntriples, ImportScope};

        let doc = concat!(
            "<http://ex/a> <http://ex/p> _:b0 .\n",
            "<http://ex/a> <http://ex/name> \"A\" .\n",
            "<http://ex/a> <http://ex/p> <urn:uuid:0191c1f6-2b1e-7c3a-9f00-000000000001> .\n",
        );
        let parsed = parse_ntriples(doc.as_bytes()).unwrap();
        let iris = imported_iris(&parsed, &ImportScope::new("https://kb.example.com", "i1"));
        assert_eq!(
            iris,
            vec![
                "http://ex/a".to_string(),
                "https://kb.example.com/.well-known/genid/i1/b0".to_string(),
            ]
        );
    }

    #[test]
    fn rdf_import_skolemizes_bnodes_per_scope() {
        use polargraph_sparql::{parse_ntriples, ImportScope};

        let doc = b"_:b0 <http://schema.org/knows> _:b1 .\n";
        let parsed = parse_ntriples(doc).unwrap();
        let rel = |scope: &ImportScope| match &imported_triples_to_proto(&parsed, scope)[0].kind {
            Some(proto::triple::Kind::Relation(r)) => (r.subject.clone(), r.object.clone()),
            other => panic!("expected relation, got {other:?}"),
        };

        let a = rel(&ImportScope::new("https://kb.example.com", "one"));
        let b = rel(&ImportScope::new("https://kb.example.com", "two"));
        let a_again = rel(&ImportScope::new("https://kb.example.com", "one"));

        assert_ne!(a.0, b.0, "_:b0 must differ across imports");
        assert_ne!(a.1, b.1, "_:b1 must differ across imports");
        assert_eq!(a, a_again, "same import_id is idempotent");
    }
}
