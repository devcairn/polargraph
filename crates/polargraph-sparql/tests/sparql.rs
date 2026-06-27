//! Unit tests for SPARQL translation and serialization.
//! No gRPC required — tests the library in isolation.

use polargraph_core::id::NodeId;
use polargraph_query::Term;
use polargraph_sparql::{serialize_csv, serialize_json, translate_query};
use std::collections::HashMap;
use uuid::Uuid;

#[test]
fn translate_simple_bgp() {
    let q = spargebra::Query::parse(
        "SELECT ?s ?o WHERE { ?s <http://example.org/knows> ?o }",
        None,
    )
    .unwrap();
    let t = translate_query(&q).unwrap();
    assert_eq!(t.branches.len(), 1);
    let branch = &t.branches[0];
    assert_eq!(branch.patterns.len(), 1);
    assert_eq!(
        branch.patterns[0].predicate.as_deref(),
        Some("http://example.org/knows")
    );
    // Subject and object are variables
    assert!(
        matches!(&branch.patterns[0].subject, Term::Var(v) if v == "s"),
        "expected subject Var(s), got {:?}",
        branch.patterns[0].subject
    );
    assert!(
        matches!(&branch.patterns[0].object, Term::Var(v) if v == "o"),
        "expected object Var(o), got {:?}",
        branch.patterns[0].object
    );
    // Projection
    assert_eq!(
        t.projection.as_ref().unwrap(),
        &vec!["s".to_string(), "o".to_string()]
    );
}

#[test]
fn translate_multi_pattern_bgp() {
    let q = spargebra::Query::parse(
        "SELECT ?s ?o WHERE { ?s <http://example.org/knows> ?o . ?o <http://example.org/age> ?age }",
        None,
    )
    .unwrap();
    let t = translate_query(&q).unwrap();
    // A BGP with two patterns compiles to one branch with two patterns
    assert!(!t.branches.is_empty());
    let total_patterns: usize = t.branches.iter().map(|b| b.patterns.len()).sum();
    assert_eq!(total_patterns, 2);
}

#[test]
fn translate_union() {
    let q = spargebra::Query::parse(
        "SELECT ?s WHERE { { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Person> } UNION { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Agent> } }",
        None,
    )
    .unwrap();
    let t = translate_query(&q).unwrap();
    // UNION should produce 2 branches
    assert_eq!(t.branches.len(), 2);
}

#[test]
fn translate_path_plus() {
    let q = spargebra::Query::parse(
        "SELECT ?dest WHERE { <urn:uuid:018e8c1e-1234-7000-8000-000000000001> <http://example.org/knows>+ ?dest }",
        None,
    )
    .unwrap();
    let t = translate_query(&q).unwrap();
    // Should have rules for transitive closure
    let total_rules: usize = t.branches.iter().map(|b| b.rules.len()).sum();
    assert!(
        total_rules >= 2,
        "expected at least 2 TC rules, got {}",
        total_rules
    );
}

#[test]
fn translate_limit_offset() {
    let q = spargebra::Query::parse(
        "SELECT ?s WHERE { ?s <http://example.org/a> ?o } LIMIT 10 OFFSET 5",
        None,
    )
    .unwrap();
    let t = translate_query(&q).unwrap();
    assert_eq!(t.limit, Some(10));
    assert_eq!(t.offset, 5);
}

#[test]
fn translate_distinct() {
    let q = spargebra::Query::parse(
        "SELECT DISTINCT ?s WHERE { ?s <http://example.org/a> ?o }",
        None,
    )
    .unwrap();
    let t = translate_query(&q).unwrap();
    assert!(t.distinct);
}

#[test]
fn translate_optional_returns_unsupported() {
    let q = spargebra::Query::parse(
        "SELECT ?s ?o WHERE { ?s <http://example.org/knows> ?o . OPTIONAL { ?o <http://example.org/age> ?age } }",
        None,
    )
    .unwrap();
    let result = translate_query(&q);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("not supported") || err_msg.contains("unsupported"),
        "got: {}",
        err_msg
    );
}

#[test]
fn serialize_json_format() {
    let uuid = Uuid::parse_str("018e8c1e-1234-7000-8000-000000000001").unwrap();
    let id = NodeId(uuid);

    let mut b: HashMap<String, NodeId> = HashMap::new();
    b.insert("s".to_string(), id);

    let json_str = serialize_json(&["s".to_string()], &[b]);
    let v: serde_json::Value = serde_json::from_str(&json_str).unwrap();

    // Check structure
    assert!(v["head"]["vars"]
        .as_array()
        .unwrap()
        .contains(&serde_json::json!("s")));
    let bindings = v["results"]["bindings"].as_array().unwrap();
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0]["s"]["type"], "uri");
    assert!(bindings[0]["s"]["value"]
        .as_str()
        .unwrap()
        .starts_with("urn:uuid:"));
}

#[test]
fn serialize_csv_format() {
    let uuid = Uuid::parse_str("018e8c1e-1234-7000-8000-000000000001").unwrap();
    let id = NodeId(uuid);

    let mut b: HashMap<String, NodeId> = HashMap::new();
    b.insert("s".to_string(), id);

    let csv = serialize_csv(&["s".to_string()], &[b]);
    let lines: Vec<&str> = csv.lines().collect();
    assert_eq!(lines[0], "s");
    assert!(lines[1].starts_with("urn:uuid:"));
}

#[test]
fn translate_ask_query() {
    let q =
        spargebra::Query::parse("ASK { ?s <http://example.org/knows> ?o }", None).unwrap();
    let t = translate_query(&q).unwrap();
    assert!(t.is_ask);
}
