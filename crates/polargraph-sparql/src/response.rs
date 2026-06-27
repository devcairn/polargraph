//! SPARQL query result serialization.
//!
//! Implements [SPARQL 1.1 Query Results JSON Format](https://www.w3.org/TR/sparql11-results-json/)
//! and CSV format.

use std::collections::HashMap;

use polargraph_core::id::NodeId;
use serde_json::{json, Map, Value};

// ── Value types ───────────────────────────────────────────────────────────────

/// A typed value in a SPARQL binding row.
///
/// Extends the core `NodeId` type to cover literal values returned by
/// SPARQL-star annotation lookups and aggregation functions.
#[derive(Debug, Clone, PartialEq)]
pub enum SparqlValue {
    /// A graph node referenced by its UUID (serialized as `urn:uuid:<uuid>`).
    Uri(NodeId),
    /// A plain string literal.
    Literal(String),
    /// An integer literal (xsd:integer).
    LiteralInt(i64),
    /// A double-precision float literal (xsd:double).
    LiteralFloat(f64),
    /// A boolean literal (xsd:boolean).
    LiteralBool(bool),
}

/// A single row of SPARQL result bindings.
///
/// Keys are SPARQL variable names (without the `?` prefix).
pub type SparqlBindings = HashMap<String, SparqlValue>;

/// Convert a `polargraph_query::Bindings` (NodeId map) to [`SparqlBindings`].
pub fn node_bindings_to_sparql(bindings: &HashMap<String, NodeId>) -> SparqlBindings {
    bindings
        .iter()
        .map(|(k, v)| (k.clone(), SparqlValue::Uri(*v)))
        .collect()
}

// ── Supported formats ─────────────────────────────────────────────────────────

/// Supported SPARQL response serialization formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseFormat {
    Json,
    Csv,
}

// ── Serializers ───────────────────────────────────────────────────────────────

/// Serialize bindings to [SPARQL 1.1 Query Results JSON Format](https://www.w3.org/TR/sparql11-results-json/).
///
/// - `vars`: projected variable names in order
/// - `bindings`: list of binding rows
pub fn serialize_json(vars: &[String], bindings: &[SparqlBindings]) -> String {
    let head = json!({ "vars": vars });

    let result_bindings: Vec<Value> = bindings
        .iter()
        .map(|b| {
            let mut obj = Map::new();
            for var in vars {
                if let Some(val) = b.get(var) {
                    obj.insert(var.clone(), sparql_value_to_json(val));
                }
            }
            Value::Object(obj)
        })
        .collect();

    json!({
        "head": head,
        "results": { "bindings": result_bindings }
    })
    .to_string()
}

/// Serialize bindings to SPARQL 1.1 CSV format.
///
/// - `vars`: projected variable names in order
/// - `bindings`: list of binding rows
pub fn serialize_csv(vars: &[String], bindings: &[SparqlBindings]) -> String {
    let mut out = String::new();
    out.push_str(&vars.join(","));
    out.push('\n');
    for b in bindings {
        let row: Vec<String> = vars
            .iter()
            .map(|v| match b.get(v) {
                Some(SparqlValue::Uri(id)) => format!("urn:uuid:{}", id.0),
                Some(SparqlValue::Literal(s)) => s.clone(),
                Some(SparqlValue::LiteralInt(n)) => n.to_string(),
                Some(SparqlValue::LiteralFloat(f)) => f.to_string(),
                Some(SparqlValue::LiteralBool(b)) => b.to_string(),
                None => String::new(),
            })
            .collect();
        out.push_str(&row.join(","));
        out.push('\n');
    }
    out
}

// ── Internal helpers ──────────────────────────────────────────────────────────

fn sparql_value_to_json(val: &SparqlValue) -> Value {
    match val {
        SparqlValue::Uri(id) => json!({
            "type": "uri",
            "value": format!("urn:uuid:{}", id.0)
        }),
        SparqlValue::Literal(s) => json!({
            "type": "literal",
            "value": s
        }),
        SparqlValue::LiteralInt(n) => json!({
            "type": "literal",
            "value": n.to_string(),
            "datatype": "http://www.w3.org/2001/XMLSchema#integer"
        }),
        SparqlValue::LiteralFloat(f) => json!({
            "type": "literal",
            "value": f.to_string(),
            "datatype": "http://www.w3.org/2001/XMLSchema#double"
        }),
        SparqlValue::LiteralBool(b) => json!({
            "type": "literal",
            "value": b.to_string(),
            "datatype": "http://www.w3.org/2001/XMLSchema#boolean"
        }),
    }
}
