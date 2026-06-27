//! SPARQL query result serialization.
//!
//! Implements [SPARQL 1.1 Query Results JSON Format](https://www.w3.org/TR/sparql11-results-json/)
//! and CSV format.

use polargraph_query::Bindings;
use serde_json::{json, Map, Value};

/// Supported SPARQL response serialization formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseFormat {
    Json,
    Csv,
}

/// Serialize bindings to [SPARQL 1.1 Query Results JSON Format](https://www.w3.org/TR/sparql11-results-json/).
///
/// - `vars`: projected variable names in order
/// - `bindings`: list of binding maps (variable name → NodeId)
pub fn serialize_json(vars: &[String], bindings: &[Bindings]) -> String {
    let head = json!({ "vars": vars });

    let result_bindings: Vec<Value> = bindings
        .iter()
        .map(|b| {
            let mut obj = Map::new();
            for var in vars {
                if let Some(node_id) = b.get(var) {
                    obj.insert(var.clone(), node_id_to_sparql_json(node_id));
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

/// Represent a `NodeId` as a SPARQL JSON URI term.
fn node_id_to_sparql_json(id: &polargraph_core::id::NodeId) -> Value {
    let uuid = id.0;
    json!({
        "type": "uri",
        "value": format!("urn:uuid:{}", uuid)
    })
}

/// Serialize bindings to SPARQL 1.1 CSV format.
///
/// - `vars`: projected variable names in order
/// - `bindings`: list of binding maps
pub fn serialize_csv(vars: &[String], bindings: &[Bindings]) -> String {
    let mut out = String::new();
    // Header row
    out.push_str(&vars.join(","));
    out.push('\n');
    for b in bindings {
        let row: Vec<String> = vars
            .iter()
            .map(|v| {
                if let Some(id) = b.get(v) {
                    format!("urn:uuid:{}", id.0)
                } else {
                    String::new()
                }
            })
            .collect();
        out.push_str(&row.join(","));
        out.push('\n');
    }
    out
}
