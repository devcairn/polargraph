//! Unit tests for SPARQL translation, serialization, and in-process execution.
//! No gRPC required — tests the library in isolation.

use polargraph_core::id::NodeId;
use polargraph_query::Term;
use polargraph_sparql::{
    execute::{apply_sparql_filter, execute_sparql_aggregations, left_join},
    serialize_csv, serialize_json, translate_query, SparqlAggFunc, SparqlAggregateSpec,
    SparqlBindings, SparqlFilter, SparqlLiteral, SparqlValue,
};
use std::collections::HashMap;
use uuid::Uuid;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn node(uuid_str: &str) -> NodeId {
    NodeId(Uuid::parse_str(uuid_str).unwrap())
}

fn bind_map(pairs: &[(&str, SparqlValue)]) -> SparqlBindings {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

// ── Phase 1: translation tests ────────────────────────────────────────────────

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
fn translate_ask_query() {
    let q =
        spargebra::Query::parse("ASK { ?s <http://example.org/knows> ?o }", None).unwrap();
    let t = translate_query(&q).unwrap();
    assert!(t.is_ask);
}

// ── Phase 1: serialization ────────────────────────────────────────────────────

#[test]
fn serialize_json_format() {
    let id = node("018e8c1e-1234-7000-8000-000000000001");
    let b: SparqlBindings = bind_map(&[("s", SparqlValue::Uri(id))]);
    let json_str = serialize_json(&["s".to_string()], &[b]);
    let v: serde_json::Value = serde_json::from_str(&json_str).unwrap();
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
    let id = node("018e8c1e-1234-7000-8000-000000000001");
    let b: SparqlBindings = bind_map(&[("s", SparqlValue::Uri(id))]);
    let csv = serialize_csv(&["s".to_string()], &[b]);
    let lines: Vec<&str> = csv.lines().collect();
    assert_eq!(lines[0], "s");
    assert!(lines[1].starts_with("urn:uuid:"));
}

// ── Phase 2: OPTIONAL / left join ────────────────────────────────────────────

/// Translation: OPTIONAL should succeed and produce `optional_branches`.
#[test]
fn translate_optional_produces_optional_branch() {
    let q = spargebra::Query::parse(
        "SELECT ?s ?o ?age WHERE { ?s <http://example.org/knows> ?o . OPTIONAL { ?o <http://example.org/age> ?age } }",
        None,
    )
    .unwrap();
    let t = translate_query(&q).unwrap();
    assert!(!t.branches.is_empty(), "should produce at least one branch");
    let main_branch = &t.branches[0];
    assert!(
        !main_branch.optional_branches.is_empty(),
        "expected optional_branches from OPTIONAL clause"
    );
    let opt = &main_branch.optional_branches[0];
    assert_eq!(opt.patterns.len(), 1, "optional branch should have one pattern");
    assert_eq!(
        opt.patterns[0].predicate.as_deref(),
        Some("http://example.org/age")
    );
}

/// Execution: when left and right bindings are compatible, both vars are bound.
#[test]
fn optional_left_join_both_match() {
    let alice = node("018e8c1e-0000-7000-8000-000000000001");
    let left = vec![bind_map(&[("s", SparqlValue::Uri(alice))])];
    let right = vec![bind_map(&[
        ("s", SparqlValue::Uri(alice)),
        ("age", SparqlValue::LiteralInt(30)),
    ])];

    let result = left_join(left, right, None);

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get("s"), Some(&SparqlValue::Uri(alice)));
    assert_eq!(result[0].get("age"), Some(&SparqlValue::LiteralInt(30)));
}

/// Execution: when right side has no compatible binding, left binding is kept
/// unchanged with right-side variables absent.
#[test]
fn optional_left_join_right_no_match() {
    let alice = node("018e8c1e-0000-7000-8000-000000000001");
    let bob = node("018e8c1e-0000-7000-8000-000000000002");
    // Only Bob has an age in the right side; Alice has none.
    let left = vec![bind_map(&[("s", SparqlValue::Uri(alice))])];
    let right = vec![bind_map(&[
        ("s", SparqlValue::Uri(bob)),
        ("age", SparqlValue::LiteralInt(25)),
    ])];

    let result = left_join(left, right, None);

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get("s"), Some(&SparqlValue::Uri(alice)));
    assert!(
        !result[0].contains_key("age"),
        "age should be absent when right side has no match for alice"
    );
}

// ── Phase 2: SPARQL-star ──────────────────────────────────────────────────────

/// Translation: a quoted triple in subject position produces an EdgeAnnotationStep.
#[test]
fn sparql_star_translation_produces_annotation_step() {
    let q = spargebra::Query::parse(
        "SELECT ?date WHERE { \
            <<<urn:uuid:018e8c1e-0000-7000-8000-000000000001> \
              <http://example.org/knows> \
              <urn:uuid:018e8c1e-0000-7000-8000-000000000002>>> \
            <http://example.org/since> ?date \
        }",
        None,
    )
    .unwrap();
    let t = translate_query(&q).unwrap();
    assert!(!t.branches.is_empty());
    let branch = &t.branches[0];
    assert_eq!(
        branch.edge_annotation_steps.len(),
        1,
        "expected one EdgeAnnotationStep from quoted triple"
    );
    let step = &branch.edge_annotation_steps[0];
    assert_eq!(step.annot_predicate, "http://example.org/since");
    assert_eq!(step.value_var, "date");
    assert_eq!(step.edge_predicate, "http://example.org/knows");
    // No regular patterns — the whole BGP becomes the annotation step.
    assert_eq!(
        branch.patterns.len(),
        0,
        "no regular VarPatterns expected when subject is quoted triple"
    );
}

// ── Phase 2: GROUP BY / aggregations ─────────────────────────────────────────

/// Translation: GROUP BY clause populates `group_by` and `aggregates` fields.
#[test]
fn translate_group_by_populates_fields() {
    let q = spargebra::Query::parse(
        "SELECT ?type (COUNT(*) AS ?n) WHERE { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type } GROUP BY ?type",
        None,
    )
    .unwrap();
    let t = translate_query(&q).unwrap();
    assert_eq!(t.group_by, vec!["type".to_string()]);
    assert_eq!(t.aggregates.len(), 1);
    assert_eq!(t.aggregates[0].alias, "n");
    assert!(matches!(t.aggregates[0].func, SparqlAggFunc::CountStar));
}

/// Execution: COUNT(*) produces correct group counts.
#[test]
fn group_by_count_execution() {
    let type_a = node("018e8c1e-0000-7000-8000-000000000001");
    let type_b = node("018e8c1e-0000-7000-8000-000000000002");

    let bindings: Vec<SparqlBindings> = vec![
        bind_map(&[("type", SparqlValue::Uri(type_a))]),
        bind_map(&[("type", SparqlValue::Uri(type_a))]),
        bind_map(&[("type", SparqlValue::Uri(type_b))]),
    ];
    let specs = vec![SparqlAggregateSpec {
        alias: "n".to_string(),
        func: SparqlAggFunc::CountStar,
    }];

    let result = execute_sparql_aggregations(bindings, &["type".to_string()], &specs, None);

    assert_eq!(result.len(), 2);
    let group_a = result
        .iter()
        .find(|r| r.get("type") == Some(&SparqlValue::Uri(type_a)))
        .expect("group for type_a not found");
    assert_eq!(group_a.get("n"), Some(&SparqlValue::LiteralInt(2)));
    let group_b = result
        .iter()
        .find(|r| r.get("type") == Some(&SparqlValue::Uri(type_b)))
        .expect("group for type_b not found");
    assert_eq!(group_b.get("n"), Some(&SparqlValue::LiteralInt(1)));
}

/// Execution: AVG produces the correct arithmetic mean.
#[test]
fn group_by_avg_execution() {
    let g_a = node("018e8c1e-0000-7000-8000-000000000001");
    let g_b = node("018e8c1e-0000-7000-8000-000000000002");

    let bindings: Vec<SparqlBindings> = vec![
        bind_map(&[("g", SparqlValue::Uri(g_a)), ("val", SparqlValue::LiteralInt(10))]),
        bind_map(&[("g", SparqlValue::Uri(g_a)), ("val", SparqlValue::LiteralInt(20))]),
        bind_map(&[("g", SparqlValue::Uri(g_b)), ("val", SparqlValue::LiteralInt(30))]),
    ];
    let specs = vec![SparqlAggregateSpec {
        alias: "avg_val".to_string(),
        func: SparqlAggFunc::Avg("val".to_string()),
    }];

    let result = execute_sparql_aggregations(bindings, &["g".to_string()], &specs, None);

    assert_eq!(result.len(), 2);
    let ga = result
        .iter()
        .find(|r| r.get("g") == Some(&SparqlValue::Uri(g_a)))
        .expect("group_a not found");
    // avg(10, 20) = 15.0
    assert_eq!(ga.get("avg_val"), Some(&SparqlValue::LiteralFloat(15.0)));
    let gb = result
        .iter()
        .find(|r| r.get("g") == Some(&SparqlValue::Uri(g_b)))
        .expect("group_b not found");
    assert_eq!(gb.get("avg_val"), Some(&SparqlValue::LiteralFloat(30.0)));
}

/// Execution: HAVING filter removes groups that don't satisfy the condition.
#[test]
fn having_filter_execution() {
    let type_a = node("018e8c1e-0000-7000-8000-000000000001");
    let type_b = node("018e8c1e-0000-7000-8000-000000000002");

    let bindings: Vec<SparqlBindings> = vec![
        bind_map(&[("type", SparqlValue::Uri(type_a))]),
        bind_map(&[("type", SparqlValue::Uri(type_a))]),
        bind_map(&[("type", SparqlValue::Uri(type_b))]),
    ];
    let specs = vec![SparqlAggregateSpec {
        alias: "n".to_string(),
        func: SparqlAggFunc::CountStar,
    }];
    // HAVING ?n > 1 — only type_a (count=2) passes
    let having = SparqlFilter::GreaterThan("n".to_string(), SparqlLiteral::Int(1));

    let result =
        execute_sparql_aggregations(bindings, &["type".to_string()], &specs, Some(&having));

    assert_eq!(result.len(), 1, "only one group should pass HAVING count > 1");
    assert_eq!(result[0].get("type"), Some(&SparqlValue::Uri(type_a)));
    assert_eq!(result[0].get("n"), Some(&SparqlValue::LiteralInt(2)));
}

// ── Phase 2: named graphs ─────────────────────────────────────────────────────

/// Translation: GRAPH <iri> { ... } populates `graph_iri` on the branch and
/// preserves the inner patterns.
#[test]
fn named_graph_scoping_translation() {
    let q = spargebra::Query::parse(
        "SELECT ?s ?o WHERE { GRAPH <urn:view:my-view> { ?s <http://example.org/knows> ?o } }",
        None,
    )
    .unwrap();
    let t = translate_query(&q).unwrap();
    assert_eq!(t.branches.len(), 1);
    assert_eq!(
        t.branches[0].graph_iri.as_deref(),
        Some("urn:view:my-view"),
        "graph_iri should be populated from GRAPH clause"
    );
    assert_eq!(t.branches[0].patterns.len(), 1);
    assert_eq!(
        t.branches[0].patterns[0].predicate.as_deref(),
        Some("http://example.org/knows")
    );
}

// ── Phase 2: filter evaluation ────────────────────────────────────────────────

#[test]
fn filter_greater_than_int() {
    let b = bind_map(&[("n", SparqlValue::LiteralInt(5))]);
    assert!(apply_sparql_filter(
        &b,
        &SparqlFilter::GreaterThan("n".into(), SparqlLiteral::Int(3))
    ));
    assert!(!apply_sparql_filter(
        &b,
        &SparqlFilter::GreaterThan("n".into(), SparqlLiteral::Int(5))
    ));
    assert!(!apply_sparql_filter(
        &b,
        &SparqlFilter::GreaterThan("n".into(), SparqlLiteral::Int(7))
    ));
}

#[test]
fn filter_less_than_int() {
    let b = bind_map(&[("n", SparqlValue::LiteralInt(3))]);
    assert!(apply_sparql_filter(
        &b,
        &SparqlFilter::LessThan("n".into(), SparqlLiteral::Int(5))
    ));
    assert!(!apply_sparql_filter(
        &b,
        &SparqlFilter::LessThan("n".into(), SparqlLiteral::Int(3))
    ));
}

#[test]
fn filter_bound_and_unbound() {
    let bound_b = bind_map(&[("x", SparqlValue::LiteralInt(1))]);
    let unbound_b: SparqlBindings = HashMap::new();
    assert!(apply_sparql_filter(&bound_b, &SparqlFilter::Bound("x".into())));
    assert!(!apply_sparql_filter(&unbound_b, &SparqlFilter::Bound("x".into())));
}

// ── Phase 2: additional aggregation edge cases ────────────────────────────────

#[test]
fn sum_integers() {
    let bindings = vec![
        bind_map(&[("v", SparqlValue::LiteralInt(3))]),
        bind_map(&[("v", SparqlValue::LiteralInt(7))]),
    ];
    let specs = vec![SparqlAggregateSpec {
        alias: "total".into(),
        func: SparqlAggFunc::Sum("v".into()),
    }];
    let result = execute_sparql_aggregations(bindings, &[], &specs, None);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get("total"), Some(&SparqlValue::LiteralInt(10)));
}

#[test]
fn min_and_max_integers() {
    let bindings = vec![
        bind_map(&[("v", SparqlValue::LiteralInt(1))]),
        bind_map(&[("v", SparqlValue::LiteralInt(5))]),
        bind_map(&[("v", SparqlValue::LiteralInt(3))]),
    ];
    let min_spec = vec![SparqlAggregateSpec {
        alias: "mn".into(),
        func: SparqlAggFunc::Min("v".into()),
    }];
    let max_spec = vec![SparqlAggregateSpec {
        alias: "mx".into(),
        func: SparqlAggFunc::Max("v".into()),
    }];

    let min_r = execute_sparql_aggregations(bindings.clone(), &[], &min_spec, None);
    assert_eq!(min_r[0].get("mn"), Some(&SparqlValue::LiteralInt(1)));

    let max_r = execute_sparql_aggregations(bindings, &[], &max_spec, None);
    assert_eq!(max_r[0].get("mx"), Some(&SparqlValue::LiteralInt(5)));
}

// ── malformed SPARQL ──────────────────────────────────────────────────────────

#[test]
fn malformed_sparql_fails_to_parse() {
    let result = spargebra::Query::parse("SELECT ?s WHERE { NOT VALID SPARQL }", None);
    assert!(result.is_err(), "malformed SPARQL should fail at the parse level");
}

#[test]
fn malformed_sparql_missing_where_fails() {
    let result = spargebra::Query::parse("SELECT ?s", None);
    assert!(result.is_err(), "SPARQL without WHERE clause should fail to parse");
}

// ── empty result set serialization ───────────────────────────────────────────

#[test]
fn serialize_json_empty_bindings_is_valid_sparql_json() {
    let json_str = serialize_json(&["s".to_string(), "o".to_string()], &[]);
    let v: serde_json::Value = serde_json::from_str(&json_str).unwrap();
    let vars = v["head"]["vars"].as_array().expect("head.vars should be an array");
    assert_eq!(vars.len(), 2);
    let bindings = v["results"]["bindings"].as_array().expect("results.bindings should be an array");
    assert!(bindings.is_empty(), "empty input should produce empty bindings array");
}

#[test]
fn serialize_csv_empty_bindings_has_header_only() {
    let empty: Vec<SparqlBindings> = vec![];
    let csv = serialize_csv(&["s".to_string()], &empty);
    let lines: Vec<&str> = csv.lines().collect();
    assert_eq!(lines.len(), 1, "CSV with no results should have only the header row");
    assert_eq!(lines[0], "s");
}

// ── large result set serialization ───────────────────────────────────────────

#[test]
fn serialize_json_large_bindings_stays_bounded() {
    let n = 1_000usize;
    let bindings: Vec<SparqlBindings> = (0..n as u128)
        .map(|i| bind_map(&[("s", SparqlValue::Uri(NodeId(Uuid::from_u128(i + 1))))]))
        .collect();

    let json_str = serialize_json(&["s".to_string()], &bindings);
    let v: serde_json::Value = serde_json::from_str(&json_str).expect("output must be valid JSON");
    let result_bindings = v["results"]["bindings"].as_array().unwrap();
    assert_eq!(result_bindings.len(), n);
}

#[test]
fn translate_with_limit_propagates_to_translation() {
    let q = spargebra::Query::parse(
        "SELECT ?s WHERE { ?s <http://example.org/type> ?o } LIMIT 100",
        None,
    )
    .unwrap();
    let t = translate_query(&q).unwrap();
    assert_eq!(t.limit, Some(100));
}
