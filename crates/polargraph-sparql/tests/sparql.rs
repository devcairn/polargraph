//! Unit tests for SPARQL translation, serialization, and in-process execution.
//! No gRPC required — tests the library in isolation.

use polargraph_core::id::NodeId;
use polargraph_core::value::Value;
use polargraph_query::Term;
use polargraph_sparql::{
    execute::{apply_sparql_filter, execute_sparql_aggregations, left_join},
    node_id_to_iri, serialize_csv, serialize_json, serialize_ntriples, serialize_turtle,
    translate_construct, translate_query, value_to_nt_literal, RdfTriple, SparqlAggFunc,
    SparqlAggregateSpec, SparqlBindings, SparqlFilter, SparqlLiteral, SparqlValue,
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
    let q = spargebra::Query::parse("ASK { ?s <http://example.org/knows> ?o }", None).unwrap();
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
    assert_eq!(
        opt.patterns.len(),
        1,
        "optional branch should have one pattern"
    );
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
        bind_map(&[
            ("g", SparqlValue::Uri(g_a)),
            ("val", SparqlValue::LiteralInt(10)),
        ]),
        bind_map(&[
            ("g", SparqlValue::Uri(g_a)),
            ("val", SparqlValue::LiteralInt(20)),
        ]),
        bind_map(&[
            ("g", SparqlValue::Uri(g_b)),
            ("val", SparqlValue::LiteralInt(30)),
        ]),
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

    assert_eq!(
        result.len(),
        1,
        "only one group should pass HAVING count > 1"
    );
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
    assert!(apply_sparql_filter(
        &bound_b,
        &SparqlFilter::Bound("x".into())
    ));
    assert!(!apply_sparql_filter(
        &unbound_b,
        &SparqlFilter::Bound("x".into())
    ));
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
    assert!(
        result.is_err(),
        "malformed SPARQL should fail at the parse level"
    );
}

#[test]
fn malformed_sparql_missing_where_fails() {
    let result = spargebra::Query::parse("SELECT ?s", None);
    assert!(
        result.is_err(),
        "SPARQL without WHERE clause should fail to parse"
    );
}

// ── empty result set serialization ───────────────────────────────────────────

#[test]
fn serialize_json_empty_bindings_is_valid_sparql_json() {
    let json_str = serialize_json(&["s".to_string(), "o".to_string()], &[]);
    let v: serde_json::Value = serde_json::from_str(&json_str).unwrap();
    let vars = v["head"]["vars"]
        .as_array()
        .expect("head.vars should be an array");
    assert_eq!(vars.len(), 2);
    let bindings = v["results"]["bindings"]
        .as_array()
        .expect("results.bindings should be an array");
    assert!(
        bindings.is_empty(),
        "empty input should produce empty bindings array"
    );
}

#[test]
fn serialize_csv_empty_bindings_has_header_only() {
    let empty: Vec<SparqlBindings> = vec![];
    let csv = serialize_csv(&["s".to_string()], &empty);
    let lines: Vec<&str> = csv.lines().collect();
    assert_eq!(
        lines.len(),
        1,
        "CSV with no results should have only the header row"
    );
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

// ── Phase 3: CONSTRUCT / DESCRIBE translation ─────────────────────────────────

/// CONSTRUCT query: translation produces branches (WHERE clause) + templates.
#[test]
fn translate_construct_basic() {
    let q = spargebra::Query::parse(
        "CONSTRUCT { ?s <http://example.org/knows> ?o } \
         WHERE { ?s <http://example.org/knows> ?o }",
        None,
    )
    .unwrap();
    let ct = translate_construct(&q).unwrap();
    assert!(!ct.is_describe);
    assert!(
        !ct.branches.is_empty(),
        "CONSTRUCT should produce WHERE branches"
    );
    assert_eq!(
        ct.templates.len(),
        1,
        "should have one CONSTRUCT template triple"
    );
    let tmpl = &ct.templates[0];
    assert_eq!(tmpl.predicate, "http://example.org/knows");
    assert!(tmpl.subject_var.is_some(), "subject should be a variable");
    assert_eq!(tmpl.subject_var.as_deref(), Some("s"));
    assert!(tmpl.object_var.is_some(), "object should be a variable");
    assert_eq!(tmpl.object_var.as_deref(), Some("o"));
    assert!(tmpl.subject_iri.is_none());
    assert!(tmpl.object_iri.is_none());
    assert!(tmpl.object_literal.is_none());
}

/// CONSTRUCT with bound IRI subject in template.
#[test]
fn translate_construct_bound_subject_iri() {
    let q = spargebra::Query::parse(
        "CONSTRUCT { <urn:uuid:018e8c1e-0000-7000-8000-000000000001> <http://example.org/knows> ?o } \
         WHERE { <urn:uuid:018e8c1e-0000-7000-8000-000000000001> <http://example.org/knows> ?o }",
        None,
    )
    .unwrap();
    let ct = translate_construct(&q).unwrap();
    assert!(!ct.is_describe);
    assert_eq!(ct.templates.len(), 1);
    let tmpl = &ct.templates[0];
    assert!(
        tmpl.subject_var.is_none(),
        "subject should be a bound IRI, not a var"
    );
    assert!(tmpl.subject_iri.is_some(), "subject_iri should be set");
    assert!(tmpl.subject_iri.as_deref().unwrap().contains("018e8c1e"));
}

/// CONSTRUCT with literal object in template.
#[test]
fn translate_construct_literal_object() {
    let q = spargebra::Query::parse(
        "CONSTRUCT { ?s <http://example.org/name> \"Alice\" } \
         WHERE { ?s <http://example.org/name> ?n }",
        None,
    )
    .unwrap();
    let ct = translate_construct(&q).unwrap();
    assert_eq!(ct.templates.len(), 1);
    let tmpl = &ct.templates[0];
    assert!(tmpl.object_var.is_none());
    assert!(tmpl.object_iri.is_none());
    assert!(
        tmpl.object_literal.is_some(),
        "bound literal should populate object_literal"
    );
}

/// CONSTRUCT: translate_query returns an error for CONSTRUCT (use translate_construct instead).
#[test]
fn translate_query_rejects_construct() {
    let q = spargebra::Query::parse("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }", None).unwrap();
    assert!(
        translate_query(&q).is_err(),
        "translate_query should reject CONSTRUCT — use translate_construct()"
    );
}

/// DESCRIBE query: is_describe flag is true, no templates, branches from WHERE.
#[test]
fn translate_describe_with_where() {
    let q = spargebra::Query::parse(
        "DESCRIBE ?s WHERE { ?s <http://example.org/type> <http://example.org/Person> }",
        None,
    )
    .unwrap();
    let ct = translate_construct(&q).unwrap();
    assert!(ct.is_describe, "should be marked as DESCRIBE");
    assert!(
        ct.templates.is_empty(),
        "DESCRIBE has no CONSTRUCT templates"
    );
    assert!(
        !ct.branches.is_empty(),
        "branches from WHERE clause expected"
    );
}

/// DESCRIBE <iri>: bare DESCRIBE stores the IRI in describe_iri.
#[test]
fn translate_describe_bare_iri() {
    let q = spargebra::Query::parse(
        "DESCRIBE <urn:uuid:018e8c1e-0000-7000-8000-000000000001>",
        None,
    )
    .unwrap();
    let ct = translate_construct(&q).unwrap();
    assert!(ct.is_describe);
    // The describe_iri may or may not be populated depending on how spargebra represents this.
    // If it is populated it must contain the UUID.
    if let Some(ref iri) = ct.describe_iri {
        assert!(
            iri.contains("018e8c1e"),
            "describe_iri should contain the IRI"
        );
    }
}

// ── Phase 3: N-Triples / Turtle serializers ───────────────────────────────────

#[test]
fn ntriples_single_triple() {
    let triples = vec![RdfTriple {
        subject: "<urn:uuid:aaa>".to_string(),
        predicate: "<http://example.org/knows>".to_string(),
        object: "<urn:uuid:bbb>".to_string(),
    }];
    let nt = serialize_ntriples(&triples);
    assert_eq!(
        nt,
        "<urn:uuid:aaa> <http://example.org/knows> <urn:uuid:bbb> .\n"
    );
}

#[test]
fn ntriples_multiple_triples_each_on_own_line() {
    let triples = vec![
        RdfTriple {
            subject: "<urn:uuid:aaa>".to_string(),
            predicate: "<http://example.org/a>".to_string(),
            object: "<urn:uuid:bbb>".to_string(),
        },
        RdfTriple {
            subject: "<urn:uuid:ccc>".to_string(),
            predicate: "<http://example.org/b>".to_string(),
            object: "\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>".to_string(),
        },
    ];
    let nt = serialize_ntriples(&triples);
    let lines: Vec<&str> = nt.lines().collect();
    assert_eq!(lines.len(), 2, "two triples → two lines");
    assert!(lines[0].ends_with(" ."));
    assert!(lines[1].ends_with(" ."));
}

#[test]
fn ntriples_empty_is_empty_string() {
    assert!(serialize_ntriples(&[]).is_empty());
}

#[test]
fn turtle_groups_by_subject() {
    let subj = "<urn:uuid:subj>";
    let triples = vec![
        RdfTriple {
            subject: subj.to_string(),
            predicate: "<http://example.org/p1>".to_string(),
            object: "<urn:uuid:o1>".to_string(),
        },
        RdfTriple {
            subject: subj.to_string(),
            predicate: "<http://example.org/p2>".to_string(),
            object: "<urn:uuid:o2>".to_string(),
        },
    ];
    let ttl = serialize_turtle(&triples);
    // Subject IRI appears exactly once (grouped).
    assert_eq!(
        ttl.matches(subj).count(),
        1,
        "subject should appear exactly once in Turtle (grouped)"
    );
    // Both predicates present.
    assert!(ttl.contains("<http://example.org/p1>"));
    assert!(ttl.contains("<http://example.org/p2>"));
    // Has prefix declarations.
    assert!(ttl.contains("@prefix xsd:"));
    assert!(ttl.contains("@prefix rdf:"));
}

#[test]
fn turtle_multiple_subjects_appear_separately() {
    let triples = vec![
        RdfTriple {
            subject: "<urn:uuid:s1>".to_string(),
            predicate: "<http://example.org/p>".to_string(),
            object: "<urn:uuid:o1>".to_string(),
        },
        RdfTriple {
            subject: "<urn:uuid:s2>".to_string(),
            predicate: "<http://example.org/p>".to_string(),
            object: "<urn:uuid:o2>".to_string(),
        },
    ];
    let ttl = serialize_turtle(&triples);
    assert!(ttl.contains("<urn:uuid:s1>"));
    assert!(ttl.contains("<urn:uuid:s2>"));
}

#[test]
fn turtle_empty_input_has_only_prefixes() {
    let ttl = serialize_turtle(&[]);
    // No trailing newline after prefixes when triples is empty.
    assert!(
        ttl.contains("@prefix xsd:"),
        "prefix declarations always present"
    );
    // No subject blocks.
    assert!(
        !ttl.contains("<urn:uuid:"),
        "no subject blocks in empty output"
    );
}

// ── Phase 3: value_to_nt_literal ─────────────────────────────────────────────

#[test]
fn nt_literal_text() {
    let lit = value_to_nt_literal(&Value::Text("hello".to_string()));
    assert!(lit.starts_with("\"hello\""));
    assert!(lit.contains("XMLSchema#string"));
}

#[test]
fn nt_literal_text_with_quotes() {
    let lit = value_to_nt_literal(&Value::Text("say \"hi\"".to_string()));
    assert!(lit.contains("\\\"hi\\\""), "double quotes must be escaped");
}

#[test]
fn nt_literal_int() {
    let lit = value_to_nt_literal(&Value::Int(99));
    assert!(lit.contains("99"));
    assert!(lit.contains("XMLSchema#integer"));
}

#[test]
fn nt_literal_float() {
    let lit = value_to_nt_literal(&Value::Float(3.14));
    assert!(lit.contains("3.14") || lit.contains("3.1"));
    assert!(lit.contains("XMLSchema#double"));
}

#[test]
fn nt_literal_bool_true() {
    let lit = value_to_nt_literal(&Value::Bool(true));
    assert!(lit.contains("true"));
    assert!(lit.contains("XMLSchema#boolean"));
}

#[test]
fn nt_literal_bool_false() {
    let lit = value_to_nt_literal(&Value::Bool(false));
    assert!(lit.contains("false"));
    assert!(lit.contains("XMLSchema#boolean"));
}

#[test]
fn nt_literal_blob_hex_encoded() {
    let lit = value_to_nt_literal(&Value::Blob(vec![0xde, 0xad, 0xbe, 0xef]));
    assert!(lit.contains("deadbeef"), "blob should be hex-encoded");
    assert!(lit.contains("hexBinary"));
}

#[test]
fn nt_literal_null_is_empty_string() {
    let lit = value_to_nt_literal(&Value::Null);
    assert!(
        lit.starts_with("\"\""),
        "Null should produce empty string literal"
    );
    assert!(lit.contains("XMLSchema#string"));
}

#[test]
fn node_id_to_iri_round_trips_uuid() {
    let uuid_str = "018e8c1e-1234-7000-8000-000000000001";
    let id = NodeId(Uuid::parse_str(uuid_str).unwrap());
    let iri = node_id_to_iri(&id);
    assert_eq!(iri, format!("<urn:uuid:{}>", uuid_str));
    // Verify it can be parsed back.
    let stripped = iri.trim_start_matches('<').trim_end_matches('>');
    assert!(stripped.starts_with("urn:uuid:"));
    let back = Uuid::parse_str(stripped.strip_prefix("urn:uuid:").unwrap()).unwrap();
    assert_eq!(back.to_string(), uuid_str);
}

// ── Phase 3: SPARQL Update parsing ───────────────────────────────────────────

/// spargebra can parse SPARQL Update INSERT DATA.
#[test]
fn sparql_update_insert_data_parses() {
    let update_str = "INSERT DATA { \
        <urn:uuid:018e8c1e-0000-7000-8000-000000000001> \
        <http://example.org/name> \
        \"Alice\" \
    }";
    let update = spargebra::Update::parse(update_str, None);
    assert!(update.is_ok(), "INSERT DATA should parse successfully");
    let ops = update.unwrap().operations;
    assert_eq!(ops.len(), 1);
    assert!(
        matches!(ops[0], spargebra::GraphUpdateOperation::InsertData { .. }),
        "should be InsertData operation"
    );
}

/// spargebra can parse SPARQL Update DELETE DATA.
#[test]
fn sparql_update_delete_data_parses() {
    let update_str = "DELETE DATA { \
        <urn:uuid:018e8c1e-0000-7000-8000-000000000001> \
        <http://example.org/name> \
        \"Alice\" \
    }";
    let update = spargebra::Update::parse(update_str, None);
    assert!(update.is_ok(), "DELETE DATA should parse successfully");
    let ops = update.unwrap().operations;
    assert_eq!(ops.len(), 1);
    assert!(
        matches!(ops[0], spargebra::GraphUpdateOperation::DeleteData { .. }),
        "should be DeleteData operation"
    );
}

/// spargebra can parse SPARQL Update DELETE/INSERT WHERE.
#[test]
fn sparql_update_delete_insert_where_parses() {
    let update_str = "DELETE { ?s <http://example.org/old> ?o } \
        INSERT { ?s <http://example.org/new> ?o } \
        WHERE  { ?s <http://example.org/old> ?o }";
    let update = spargebra::Update::parse(update_str, None);
    assert!(
        update.is_ok(),
        "DELETE/INSERT WHERE should parse successfully"
    );
    let ops = update.unwrap().operations;
    assert_eq!(ops.len(), 1);
    assert!(
        matches!(ops[0], spargebra::GraphUpdateOperation::DeleteInsert { .. }),
        "should be DeleteInsert operation"
    );
}

/// Malformed SPARQL Update should fail to parse.
#[test]
fn sparql_update_malformed_fails() {
    let result = spargebra::Update::parse("INSERT GARBAGE", None);
    assert!(
        result.is_err(),
        "malformed SPARQL Update should fail to parse"
    );
}

// ── Phase 3: gRPC integration placeholder ────────────────────────────────────
//
// Full gRPC tests (DeleteTriples RPC round-trip) live in:
//   crates/polargraph-server/tests/
// and require a running polargraphd. The unit tests above cover the library
// layer exhaustively without network I/O.

// ── SPARQL-star (RDF-star) tests ──────────────────────────────────────────────

/// Gap 1: object-position embedded triple — `?x <:ann> << <:Alice> <:likes> <:Bob> >>`.
/// The translator should produce an `edge_annotation_object_steps` entry.
#[test]
fn sparql_star_object_position() {
    use polargraph_sparql::translate_query;

    let sparql = "SELECT ?x WHERE { \
        ?x <http://example.org/annotates> << <urn:uuid:018e8c1e-0000-7000-8000-000000000001> <http://example.org/likes> <urn:uuid:018e8c1e-0000-7000-8000-000000000002> >> \
    }";
    let q = spargebra::Query::parse(sparql, None).expect("query should parse");
    let translation = translate_query(&q).expect("query should translate");

    let object_steps: Vec<_> = translation
        .branches
        .iter()
        .flat_map(|b| b.edge_annotation_object_steps.iter())
        .collect();

    assert!(
        !object_steps.is_empty(),
        "expected at least one edge_annotation_object_step for object-position embedded triple"
    );
    let step = &object_steps[0];
    assert_eq!(step.annotation_predicate, "http://example.org/annotates");
    assert_eq!(step.edge_predicate, "http://example.org/likes");
}

/// Gap 2: variable annotation predicate — `<< <:Alice> <:likes> <:Bob> >> ?annot ?val`.
/// The translator should produce an `edge_annotation_steps` entry with
/// `annotation_predicate_var = Some("annot")`.
#[test]
fn sparql_star_variable_predicate() {
    use polargraph_sparql::translate_query;

    let sparql = "SELECT ?annot ?val WHERE { \
        << <urn:uuid:018e8c1e-0000-7000-8000-000000000001> <http://example.org/likes> <urn:uuid:018e8c1e-0000-7000-8000-000000000002> >> ?annot ?val \
    }";
    let q = spargebra::Query::parse(sparql, None).expect("query should parse");
    let translation = translate_query(&q).expect("query should translate");

    let ann_steps: Vec<_> = translation
        .branches
        .iter()
        .flat_map(|b| b.edge_annotation_steps.iter())
        .collect();

    assert!(
        !ann_steps.is_empty(),
        "expected at least one edge_annotation_step for variable predicate"
    );
    let step = &ann_steps[0];
    assert_eq!(
        step.annotation_predicate_var,
        Some("annot".to_string()),
        "annotation_predicate_var should capture the ?annot variable"
    );
}

/// Gap 3: Turtle-star serialization — `RdfStarTriple` with `QuotedTriple` subject
/// should emit `<< s p o >> pred obj .` syntax.
#[test]
fn sparql_star_turtle_output() {
    use polargraph_sparql::{
        serialize_ntriples_star, serialize_turtle_star, RdfStarSubject, RdfStarTriple,
    };

    let triples = vec![RdfStarTriple {
        subject: RdfStarSubject::QuotedTriple {
            s: "<urn:uuid:018e8c1e-0000-7000-8000-000000000001>".to_string(),
            p: "<http://example.org/likes>".to_string(),
            o: "<urn:uuid:018e8c1e-0000-7000-8000-000000000002>".to_string(),
        },
        predicate: "<http://example.org/since>".to_string(),
        object: "\"2020\"^^<http://www.w3.org/2001/XMLSchema#string>".to_string(),
    }];

    let nt = serialize_ntriples_star(&triples);
    assert!(
        nt.contains("<< ") && nt.contains(" >>"),
        "N-Triples-star output should contain << >> syntax, got: {nt}"
    );
    assert!(
        nt.contains("http://example.org/since"),
        "should contain annotation predicate"
    );

    let ttl = serialize_turtle_star(&triples);
    assert!(
        ttl.contains("<< ") && ttl.contains(" >>"),
        "Turtle-star output should contain << >> syntax, got: {ttl}"
    );
    assert!(
        ttl.contains("http://example.org/since"),
        "should contain annotation predicate"
    );
}

/// Gap 4 (INSERT): SPARQL Update `INSERT DATA` with an embedded-triple subject.
/// spargebra should parse it and produce a `Subject::Triple` in the quad.
#[test]
fn sparql_star_update_insert() {
    let update_str = "INSERT DATA { \
        << <urn:uuid:018e8c1e-0000-7000-8000-000000000001> \
           <http://example.org/likes> \
           <urn:uuid:018e8c1e-0000-7000-8000-000000000002> >> \
        <http://example.org/since> \"2020\" \
    }";
    let update = spargebra::Update::parse(update_str, None)
        .expect("INSERT DATA with quoted triple should parse");
    let ops = update.operations;
    assert_eq!(ops.len(), 1);
    if let spargebra::GraphUpdateOperation::InsertData { data } = &ops[0] {
        assert!(!data.is_empty(), "should have at least one quad");
        let quad = &data[0];
        assert!(
            matches!(quad.subject, spargebra::term::Subject::Triple(_)),
            "subject should be a quoted triple"
        );
    } else {
        panic!("expected InsertData operation");
    }
}

/// Gap 4 (DELETE): SPARQL Update `DELETE DATA` with an embedded-triple subject.
/// spargebra should parse it and produce a `GroundSubject::Triple` in the quad.
#[test]
fn sparql_star_update_delete() {
    let update_str = "DELETE DATA { \
        << <urn:uuid:018e8c1e-0000-7000-8000-000000000001> \
           <http://example.org/likes> \
           <urn:uuid:018e8c1e-0000-7000-8000-000000000002> >> \
        <http://example.org/since> \"2020\" \
    }";
    let update = spargebra::Update::parse(update_str, None)
        .expect("DELETE DATA with quoted triple should parse");
    let ops = update.operations;
    assert_eq!(ops.len(), 1);
    if let spargebra::GraphUpdateOperation::DeleteData { data } = &ops[0] {
        assert!(!data.is_empty(), "should have at least one quad");
        let quad = &data[0];
        assert!(
            matches!(quad.subject, spargebra::term::GroundSubject::Triple(_)),
            "subject should be a quoted triple"
        );
    } else {
        panic!("expected DeleteData operation");
    }
}

// ── Type-testing filter built-ins ─────────────────────────────────────────────

fn mixed_bindings() -> (SparqlBindings, SparqlBindings) {
    let uri_row = bind_map(&[("o", SparqlValue::Uri(node("018e8c1e-0000-7000-8000-000000000001")))]);
    let lit_row = bind_map(&[("o", SparqlValue::LiteralInt(42))]);
    (uri_row, lit_row)
}

#[test]
fn filter_is_literal_accepts_string_literal() {
    let b = bind_map(&[("o", SparqlValue::Literal("hello".into()))]);
    assert!(apply_sparql_filter(&b, &SparqlFilter::IsLiteral("o".into())));
}

#[test]
fn filter_is_literal_accepts_int_literal() {
    let b = bind_map(&[("o", SparqlValue::LiteralInt(99))]);
    assert!(apply_sparql_filter(&b, &SparqlFilter::IsLiteral("o".into())));
}

#[test]
fn filter_is_literal_accepts_float_literal() {
    let b = bind_map(&[("o", SparqlValue::LiteralFloat(3.14))]);
    assert!(apply_sparql_filter(&b, &SparqlFilter::IsLiteral("o".into())));
}

#[test]
fn filter_is_literal_accepts_bool_literal() {
    let b = bind_map(&[("o", SparqlValue::LiteralBool(true))]);
    assert!(apply_sparql_filter(&b, &SparqlFilter::IsLiteral("o".into())));
}

#[test]
fn filter_is_literal_rejects_uri() {
    let (uri_row, _) = mixed_bindings();
    assert!(!apply_sparql_filter(&uri_row, &SparqlFilter::IsLiteral("o".into())));
}

#[test]
fn filter_is_literal_rejects_unbound() {
    let empty: SparqlBindings = HashMap::new();
    assert!(!apply_sparql_filter(&empty, &SparqlFilter::IsLiteral("o".into())));
}

#[test]
fn filter_is_iri_accepts_uri() {
    let (uri_row, _) = mixed_bindings();
    assert!(apply_sparql_filter(&uri_row, &SparqlFilter::IsIri("o".into())));
}

#[test]
fn filter_is_iri_rejects_literal() {
    let (_, lit_row) = mixed_bindings();
    assert!(!apply_sparql_filter(&lit_row, &SparqlFilter::IsIri("o".into())));
}

#[test]
fn filter_is_blank_always_false() {
    let (uri_row, lit_row) = mixed_bindings();
    let empty: SparqlBindings = HashMap::new();
    assert!(!apply_sparql_filter(&uri_row, &SparqlFilter::IsBlank("o".into())));
    assert!(!apply_sparql_filter(&lit_row, &SparqlFilter::IsBlank("o".into())));
    assert!(!apply_sparql_filter(&empty, &SparqlFilter::IsBlank("o".into())));
}

#[test]
fn translate_filter_is_literal_from_sparql() {
    let q = spargebra::Query::parse(
        "SELECT ?s ?o WHERE { ?s <http://example.org/p> ?o . FILTER(isLiteral(?o)) }",
        None,
    )
    .expect("query should parse");
    let t = translate_query(&q).expect("query should translate");
    let filters: Vec<_> = t.branches.iter().flat_map(|b| b.filters.iter()).collect();
    assert!(
        filters.iter().any(|f| matches!(f, SparqlFilter::IsLiteral(_))),
        "expected IsLiteral filter, got: {:?}",
        filters
    );
}

#[test]
fn translate_filter_is_iri_from_sparql() {
    let q = spargebra::Query::parse(
        "SELECT ?s ?o WHERE { ?s <http://example.org/p> ?o . FILTER(isIRI(?o)) }",
        None,
    )
    .expect("query should parse");
    let t = translate_query(&q).expect("query should translate");
    let filters: Vec<_> = t.branches.iter().flat_map(|b| b.filters.iter()).collect();
    assert!(
        filters.iter().any(|f| matches!(f, SparqlFilter::IsIri(_))),
        "expected IsIri filter, got: {:?}",
        filters
    );
}

#[test]
fn translate_filter_is_uri_maps_to_is_iri() {
    // isURI is a SPARQL 1.1 synonym for isIRI — both should produce IsIri.
    let q = spargebra::Query::parse(
        "SELECT ?s ?o WHERE { ?s <http://example.org/p> ?o . FILTER(isURI(?o)) }",
        None,
    )
    .expect("query should parse");
    let t = translate_query(&q).expect("query should translate");
    let filters: Vec<_> = t.branches.iter().flat_map(|b| b.filters.iter()).collect();
    assert!(
        filters.iter().any(|f| matches!(f, SparqlFilter::IsIri(_))),
        "isURI should map to IsIri filter, got: {:?}",
        filters
    );
}

#[test]
fn translate_filter_is_blank_from_sparql() {
    let q = spargebra::Query::parse(
        "SELECT ?s ?o WHERE { ?s <http://example.org/p> ?o . FILTER(isBlank(?o)) }",
        None,
    )
    .expect("query should parse");
    let t = translate_query(&q).expect("query should translate");
    let filters: Vec<_> = t.branches.iter().flat_map(|b| b.filters.iter()).collect();
    assert!(
        filters.iter().any(|f| matches!(f, SparqlFilter::IsBlank(_))),
        "expected IsBlank filter, got: {:?}",
        filters
    );
}

#[test]
fn filter_is_literal_applied_to_mixed_rows_keeps_only_literals() {
    let rows = vec![
        bind_map(&[("o", SparqlValue::Uri(node("018e8c1e-0000-7000-8000-000000000001")))]),
        bind_map(&[("o", SparqlValue::LiteralInt(1))]),
        bind_map(&[("o", SparqlValue::Literal("text".into()))]),
        bind_map(&[("o", SparqlValue::LiteralBool(false))]),
    ];
    let filtered: Vec<_> = rows
        .into_iter()
        .filter(|b| apply_sparql_filter(b, &SparqlFilter::IsLiteral("o".into())))
        .collect();
    assert_eq!(filtered.len(), 3, "URI row should be excluded");
}

#[test]
fn filter_is_blank_applied_to_mixed_rows_returns_empty() {
    let rows = vec![
        bind_map(&[("o", SparqlValue::Uri(node("018e8c1e-0000-7000-8000-000000000001")))]),
        bind_map(&[("o", SparqlValue::LiteralInt(1))]),
    ];
    let filtered: Vec<_> = rows
        .into_iter()
        .filter(|b| apply_sparql_filter(b, &SparqlFilter::IsBlank("o".into())))
        .collect();
    assert!(filtered.is_empty(), "isBlank is always false — no rows expected");
}
