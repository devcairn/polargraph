//! SPARQL query result serialization.
//!
//! Implements [SPARQL 1.1 Query Results JSON Format](https://www.w3.org/TR/sparql11-results-json/)
//! and CSV format.

use std::collections::HashMap;

use polargraph_core::id::NodeId;

use crate::names::IriNames;
use serde_json::{json, Map, Value};

// ── Value types ───────────────────────────────────────────────────────────────

/// A typed value in a SPARQL binding row.
///
/// A node, or a literal: a property value bound to a variable
/// (`docs/design/value-bindings.md`), an annotation value or an aggregate.
#[derive(Debug, Clone, PartialEq)]
pub enum SparqlValue {
    /// A graph node referenced by its UUID (serialized as its IRI from the
    /// dictionary, else `urn:uuid:<uuid>`).
    Uri(NodeId),
    /// An IRI known by its string — a bound predicate variable. The same
    /// term as `Uri(iri_to_node_id(iri))`.
    Iri(String),
    /// A plain string literal.
    Literal(String),
    /// An integer literal (xsd:integer).
    LiteralInt(i64),
    /// A double-precision float literal (xsd:double).
    LiteralFloat(f64),
    /// A boolean literal (xsd:boolean).
    LiteralBool(bool),
    /// A language-tagged string (rdf:langString).
    LangLiteral { text: String, lang: String },
    /// Any other typed literal: lexical form + datatype IRI.
    TypedLiteral { lexical: String, datatype: String },
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
/// - `names`: IRIs for URI values (see [`IriNames`])
pub fn serialize_json(vars: &[String], bindings: &[SparqlBindings], names: &IriNames) -> String {
    let head = json!({ "vars": vars });

    let result_bindings: Vec<Value> = bindings
        .iter()
        .map(|b| {
            let mut obj = Map::new();
            for var in vars {
                if let Some(val) = b.get(var) {
                    obj.insert(var.clone(), sparql_value_to_json(val, names));
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
/// - `names`: IRIs for URI values (see [`IriNames`])
pub fn serialize_csv(vars: &[String], bindings: &[SparqlBindings], names: &IriNames) -> String {
    let mut out = String::new();
    out.push_str(&vars.join(","));
    out.push('\n');
    for b in bindings {
        let row: Vec<String> = vars
            .iter()
            .map(|v| match b.get(v) {
                Some(SparqlValue::Uri(id)) => match names.bnode_label(id) {
                    Some(label) => format!("_:{label}"),
                    None => names.iri(id),
                },
                Some(SparqlValue::Iri(iri)) => iri.clone(),
                Some(literal) => csv_field(&literal.lexical().unwrap_or_default()),
                None => String::new(),
            })
            .collect();
        out.push_str(&row.join(","));
        out.push('\n');
    }
    out
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/// A CSV field, quoted when it contains a comma, quote or line break (RFC 4180).
fn csv_field(s: &str) -> String {
    if s.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn sparql_value_to_json(val: &SparqlValue, names: &IriNames) -> Value {
    match val {
        SparqlValue::Uri(id) => match names.bnode_label(id) {
            Some(label) => json!({ "type": "bnode", "value": label }),
            None => json!({ "type": "uri", "value": names.iri(id) }),
        },
        SparqlValue::Iri(iri) => json!({ "type": "uri", "value": iri }),
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
        SparqlValue::LangLiteral { text, lang } => json!({
            "type": "literal",
            "value": text,
            "xml:lang": lang
        }),
        SparqlValue::TypedLiteral { lexical, datatype } => json!({
            "type": "literal",
            "value": lexical,
            "datatype": datatype
        }),
    }
}
