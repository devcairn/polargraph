//! Integration tests for the gRPC service handlers.
//!
//! Tests call service methods directly (no live server socket) so they are
//! fast and parallelisable. The full proto serialisation path is still
//! exercised because we construct real proto request messages and inspect
//! real proto response messages.

use polargraph_server::{
    proto::{
        polar_graph_service_server::PolarGraphService,
        term::Kind as TermKind,
        triple::Kind as TripleKind,
        value::Kind as ValueKind,
        search_vector_filtered_request::Filter,
        vector_seed_query_request::Filter as SeedFilter,
        BatchInsertVectorsRequest, EdgeTypeDef, FieldDef, GetEdgeTypeRequest,
        GetNodeTypeRequest, InsertRequest, InsertVectorRequest, ListEdgeTypesRequest,
        ListNodeTypesRequest, ListPredicatesBetweenRequest, NodeId, NodeTypeDef, NodeTypeFilter,
        PropertyTriple, QueryRequest, ReachableRequest, RegisterEdgeTypeRequest,
        RegisterNodeTypeRequest, RelationTriple, SearchVectorFilteredRequest,
        SearchVectorInSetRequest, SearchVectorRequest, Term, Triple, ValidateEdgeRequest,
        ValidateNodeRequest, Value, VarPattern, VectorItem, VectorSeedQueryRequest, VectorSpaceDef,
    },
    service::PolarGraphServer,
};
use polargraph_storage::TripleStore;
use tempfile::TempDir;
use tonic::Request;

// ── helpers ───────────────────────────────────────────────────────────────────

fn open() -> (PolarGraphServer, TempDir) {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    (PolarGraphServer::new(store).unwrap(), dir)
}

fn new_node() -> (polargraph_core::id::NodeId, NodeId) {
    let core = polargraph_core::id::NodeId::new();
    let proto = NodeId { bytes: core.as_bytes().to_vec() };
    (core, proto)
}

fn bound(id: &NodeId) -> Term {
    Term { kind: Some(TermKind::Bound(id.clone())) }
}
fn var(name: &str) -> Term {
    Term { kind: Some(TermKind::Var(name.into())) }
}
fn any() -> Term {
    Term { kind: None }
}

fn rel(subject: NodeId, predicate: &str, object: NodeId) -> Triple {
    Triple {
        kind: Some(TripleKind::Relation(RelationTriple {
            subject: Some(subject),
            predicate: predicate.into(),
            object: Some(object),
            vt_start: 0,
            vt_end: 0,
        })),
    }
}

fn text_prop(subject: NodeId, predicate: &str, text: &str) -> Triple {
    Triple {
        kind: Some(TripleKind::Property(PropertyTriple {
            subject: Some(subject),
            predicate: predicate.into(),
            value: Some(Value { kind: Some(ValueKind::TextVal(text.into())) }),
            vt_start: 0,
            vt_end: 0,
        })),
    }
}

fn pattern(sub: Term, pred: &str, obj: Term) -> VarPattern {
    VarPattern { subject: Some(sub), predicate: pred.into(), object: Some(obj) }
}

// ── Insert tests ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn insert_single_relation_returns_positive_commit_ts() {
    let (svc, _dir) = open();
    let (_, alice) = new_node();
    let (_, bob)   = new_node();

    let resp = svc
        .insert(Request::new(InsertRequest { triples: vec![rel(alice, "knows", bob)] }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.commit_ts > 0);
}

#[tokio::test]
async fn insert_batch_all_triples_committed() {
    let (svc, _dir) = open();
    let (_, alice) = new_node();
    let (_, bob)   = new_node();
    let (_, carol) = new_node();

    let ts = svc
        .insert(Request::new(InsertRequest {
            triples: vec![
                rel(alice.clone(), "knows", bob.clone()),
                rel(alice.clone(), "knows", carol.clone()),
                text_prop(alice.clone(), "name", "Alice"),
            ],
        }))
        .await
        .unwrap()
        .into_inner()
        .commit_ts;

    // Query alice's triples: wildcard predicate, wildcard object.
    let resp = svc
        .query(Request::new(QueryRequest {
            patterns: vec![pattern(bound(&alice), "", any())],
            snapshot_ts: ts,
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.bindings.len(), 3);
}

#[tokio::test]
async fn insert_empty_returns_invalid_argument() {
    let (svc, _dir) = open();
    let err = svc
        .insert(Request::new(InsertRequest { triples: vec![] }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn insert_bad_node_id_length_returns_invalid_argument() {
    let (svc, _dir) = open();
    let bad = Triple {
        kind: Some(TripleKind::Relation(RelationTriple {
            subject:   Some(NodeId { bytes: vec![0u8; 15] }), // wrong
            predicate: "knows".into(),
            object:    Some(NodeId { bytes: vec![0u8; 16] }),
            vt_start: 0, vt_end: 0,
        })),
    };
    let err = svc
        .insert(Request::new(InsertRequest { triples: vec![bad] }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn insert_empty_predicate_returns_invalid_argument() {
    let (svc, _dir) = open();
    let (_, a) = new_node();
    let (_, b) = new_node();
    let bad = rel(a, "", b); // empty predicate
    let err = svc
        .insert(Request::new(InsertRequest { triples: vec![bad] }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

// ── Query tests ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn query_empty_patterns_returns_invalid_argument() {
    let (svc, _dir) = open();
    let err = svc
        .query(Request::new(QueryRequest { patterns: vec![], snapshot_ts: 0 }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn query_with_object_variable_binds_objects() {
    let (svc, _dir) = open();
    let (_, alice)      = new_node();
    let (core_bob, bob) = new_node();
    let (core_carol, carol) = new_node();

    let ts = svc
        .insert(Request::new(InsertRequest {
            triples: vec![
                rel(alice.clone(), "knows", bob),
                rel(alice.clone(), "knows", carol),
            ],
        }))
        .await.unwrap().into_inner().commit_ts;

    let resp = svc
        .query(Request::new(QueryRequest {
            patterns: vec![pattern(bound(&alice), "knows", var("who"))],
            snapshot_ts: ts,
        }))
        .await.unwrap().into_inner();

    assert_eq!(resp.bindings.len(), 2);
    let bound_ids: Vec<Vec<u8>> = resp.bindings.iter()
        .filter_map(|b| b.vars.get("who").map(|n| n.bytes.clone()))
        .collect();
    assert!(bound_ids.contains(&core_bob.as_bytes().to_vec()));
    assert!(bound_ids.contains(&core_carol.as_bytes().to_vec()));
}

#[tokio::test]
async fn query_snapshot_at_old_ts_excludes_later_writes() {
    let (svc, _dir) = open();
    let (_, alice) = new_node();
    let (_, bob)   = new_node();
    let (_, carol) = new_node();

    let ts1 = svc
        .insert(Request::new(InsertRequest { triples: vec![rel(alice.clone(), "knows", bob)] }))
        .await.unwrap().into_inner().commit_ts;

    svc.insert(Request::new(InsertRequest { triples: vec![rel(alice.clone(), "knows", carol)] }))
        .await.unwrap();

    // Snapshot at ts1 should only see the first insert.
    let resp = svc
        .query(Request::new(QueryRequest {
            patterns: vec![pattern(bound(&alice), "knows", var("x"))],
            snapshot_ts: ts1,
        }))
        .await.unwrap().into_inner();

    assert_eq!(resp.bindings.len(), 1, "snapshot must not see writes after ts1");
}

#[tokio::test]
async fn query_at_ts_zero_sees_latest() {
    let (svc, _dir) = open();
    let (_, alice) = new_node();
    let (_, bob)   = new_node();

    svc.insert(Request::new(InsertRequest { triples: vec![rel(alice.clone(), "knows", bob)] }))
        .await.unwrap();

    // ts=0 means "latest"
    let resp = svc
        .query(Request::new(QueryRequest {
            patterns: vec![pattern(bound(&alice), "knows", var("x"))],
            snapshot_ts: 0,
        }))
        .await.unwrap().into_inner();

    assert_eq!(resp.bindings.len(), 1);
}

#[tokio::test]
async fn two_pattern_join_via_service() {
    let (svc, _dir) = open();
    let (_, alice) = new_node();
    let (_, bob)   = new_node();
    let (_, mgr)   = new_node();

    let ts = svc
        .insert(Request::new(InsertRequest {
            triples: vec![
                rel(alice.clone(), "reports-to", mgr.clone()),
                rel(bob.clone(),   "reports-to", mgr.clone()),
            ],
        }))
        .await.unwrap().into_inner().commit_ts;

    let resp = svc
        .query(Request::new(QueryRequest {
            patterns: vec![
                pattern(bound(&alice), "reports-to", var("mgr")),
                pattern(var("colleague"), "reports-to", var("mgr")),
            ],
            snapshot_ts: ts,
        }))
        .await.unwrap().into_inner();

    assert_eq!(resp.bindings.len(), 2); // alice + bob
}

#[tokio::test]
async fn query_no_match_returns_empty_bindings() {
    let (svc, _dir) = open();
    let (_, nobody) = new_node();

    let resp = svc
        .query(Request::new(QueryRequest {
            patterns: vec![pattern(bound(&nobody), "knows", var("x"))],
            snapshot_ts: 0,
        }))
        .await.unwrap().into_inner();

    assert!(resp.bindings.is_empty());
}

// ── InsertVector / SearchVector tests ────────────────────────────────────────

#[tokio::test]
async fn insert_vector_empty_node_id_returns_invalid_argument() {
    let (svc, _dir) = open();
    let err = svc
        .insert_vector(Request::new(InsertVectorRequest {
            node_id: None,
            vector: vec![1.0, 0.0],
            space: String::new(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn insert_vector_empty_vector_returns_invalid_argument() {
    let (svc, _dir) = open();
    let (_, id) = new_node();
    let err = svc
        .insert_vector(Request::new(InsertVectorRequest { node_id: Some(id), vector: vec![], space: String::new() }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn search_vector_empty_query_returns_invalid_argument() {
    let (svc, _dir) = open();
    let err = svc
        .search_vector(Request::new(SearchVectorRequest { query: vec![], k: 5, space: String::new() }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn insert_then_search_vector_finds_nearest() {
    let (svc, _dir) = open();
    let (_, id1) = new_node();
    let (_, id2) = new_node();
    let (_, id3) = new_node();

    svc.insert_vector(Request::new(InsertVectorRequest {
        node_id: Some(id1.clone()),
        vector: vec![1.0, 0.0, 0.0],
        space: String::new(),
    }))
    .await
    .unwrap();
    svc.insert_vector(Request::new(InsertVectorRequest {
        node_id: Some(id2),
        vector: vec![0.0, 1.0, 0.0],
        space: String::new(),
    }))
    .await
    .unwrap();
    svc.insert_vector(Request::new(InsertVectorRequest {
        node_id: Some(id3),
        vector: vec![0.0, 0.0, 1.0],
        space: String::new(),
    }))
    .await
    .unwrap();

    let resp = svc
        .search_vector(Request::new(SearchVectorRequest {
            query: vec![1.0, 0.0, 0.0],
            k: 1,
            space: String::new(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.results.len(), 1);
    assert_eq!(resp.results[0].node_id.as_ref().unwrap().bytes, id1.bytes);
    assert!((resp.results[0].similarity - 1.0).abs() < 1e-5);
}

#[tokio::test]
async fn search_vector_empty_index_returns_empty() {
    let (svc, _dir) = open();
    let resp = svc
        .search_vector(Request::new(SearchVectorRequest {
            query: vec![1.0, 0.0],
            k: 5,
            space: String::new(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.results.is_empty());
}

#[tokio::test]
async fn search_vector_default_k_when_zero() {
    let (svc, _dir) = open();
    for i in 0u8..5 {
        let (_, id) = new_node();
        svc.insert_vector(Request::new(InsertVectorRequest {
            node_id: Some(id),
            vector: vec![i as f32, 0.0],
            space: String::new(),
        }))
        .await
        .unwrap();
    }
    // k=0 should default to 10, returning all 5 results.
    let resp = svc
        .search_vector(Request::new(SearchVectorRequest {
            query: vec![1.0, 0.0],
            k: 0,
            space: String::new(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.results.len(), 5);
}

#[tokio::test]
async fn commit_ts_increments_across_inserts() {
    let (svc, _dir) = open();
    let (_, a) = new_node();
    let (_, b) = new_node();
    let (_, c) = new_node();

    let ts1 = svc
        .insert(Request::new(InsertRequest { triples: vec![rel(a.clone(), "k", b)] }))
        .await.unwrap().into_inner().commit_ts;

    let ts2 = svc
        .insert(Request::new(InsertRequest { triples: vec![rel(a, "k", c)] }))
        .await.unwrap().into_inner().commit_ts;

    assert!(ts2 > ts1, "commit timestamps must be monotonically increasing");
}

// ── Reachable tests ───────────────────────────────────────────────────────────

/// Build a chain A → B → C → D and verify all are reachable from A.
#[tokio::test]
async fn reachable_linear_chain() {
    let (svc, _dir) = open();
    let nodes: Vec<_> = (0..4).map(|_| new_node()).collect();
    let (core_a, proto_a) = &nodes[0];
    let (core_b, _)       = &nodes[1];
    let (core_c, _)       = &nodes[2];
    let (core_d, _)       = &nodes[3];

    svc.insert(Request::new(InsertRequest {
        triples: vec![
            rel(nodes[0].1.clone(), "follows", nodes[1].1.clone()),
            rel(nodes[1].1.clone(), "follows", nodes[2].1.clone()),
            rel(nodes[2].1.clone(), "follows", nodes[3].1.clone()),
        ],
    }))
    .await
    .unwrap();

    let resp = svc
        .reachable(Request::new(ReachableRequest {
            start: Some(proto_a.clone()),
            predicate: "follows".into(),
            max_hops: 0, // unlimited
        }))
        .await
        .unwrap()
        .into_inner();

    let ids: Vec<Vec<u8>> = resp.node_ids.iter().map(|n| n.bytes.clone()).collect();
    assert_eq!(ids.len(), 3, "expected B, C, D");
    assert!(ids.contains(&core_b.as_bytes().to_vec()));
    assert!(ids.contains(&core_c.as_bytes().to_vec()));
    assert!(ids.contains(&core_d.as_bytes().to_vec()));
    assert!(!ids.contains(&core_a.as_bytes().to_vec()), "start node must not appear");
}

/// Diamond: A → B, A → C, B → D, C → D. All three non-A nodes reachable, D not duplicated.
#[tokio::test]
async fn reachable_diamond_graph() {
    let (svc, _dir) = open();
    let nodes: Vec<_> = (0..4).map(|_| new_node()).collect();
    let (core_b, _) = &nodes[1];
    let (core_c, _) = &nodes[2];
    let (core_d, _) = &nodes[3];

    svc.insert(Request::new(InsertRequest {
        triples: vec![
            rel(nodes[0].1.clone(), "edge", nodes[1].1.clone()), // A→B
            rel(nodes[0].1.clone(), "edge", nodes[2].1.clone()), // A→C
            rel(nodes[1].1.clone(), "edge", nodes[3].1.clone()), // B→D
            rel(nodes[2].1.clone(), "edge", nodes[3].1.clone()), // C→D
        ],
    }))
    .await
    .unwrap();

    let resp = svc
        .reachable(Request::new(ReachableRequest {
            start: Some(nodes[0].1.clone()),
            predicate: "edge".into(),
            max_hops: 0,
        }))
        .await
        .unwrap()
        .into_inner();

    let ids: Vec<Vec<u8>> = resp.node_ids.iter().map(|n| n.bytes.clone()).collect();
    assert_eq!(ids.len(), 3, "B, C, D — no duplicates");
    assert!(ids.contains(&core_b.as_bytes().to_vec()));
    assert!(ids.contains(&core_c.as_bytes().to_vec()));
    assert!(ids.contains(&core_d.as_bytes().to_vec()));
}

/// Cycle A → B → C → A should terminate without infinite loop.
#[tokio::test]
async fn reachable_cycle_terminates() {
    let (svc, _dir) = open();
    let nodes: Vec<_> = (0..3).map(|_| new_node()).collect();
    let (core_b, _) = &nodes[1];
    let (core_c, _) = &nodes[2];

    svc.insert(Request::new(InsertRequest {
        triples: vec![
            rel(nodes[0].1.clone(), "link", nodes[1].1.clone()),
            rel(nodes[1].1.clone(), "link", nodes[2].1.clone()),
            rel(nodes[2].1.clone(), "link", nodes[0].1.clone()),
        ],
    }))
    .await
    .unwrap();

    let resp = svc
        .reachable(Request::new(ReachableRequest {
            start: Some(nodes[0].1.clone()),
            predicate: "link".into(),
            max_hops: 0,
        }))
        .await
        .unwrap()
        .into_inner();

    // B and C are reachable; A itself comes back through the cycle but
    // reachable_from only returns *other* nodes (start is filtered out).
    let ids: Vec<Vec<u8>> = resp.node_ids.iter().map(|n| n.bytes.clone()).collect();
    assert!(ids.contains(&core_b.as_bytes().to_vec()));
    assert!(ids.contains(&core_c.as_bytes().to_vec()));
}

/// max_hops=1 should return only direct neighbours, not two-hop nodes.
#[tokio::test]
async fn reachable_max_hops_limits_depth() {
    let (svc, _dir) = open();
    let nodes: Vec<_> = (0..3).map(|_| new_node()).collect();
    let (core_b, _) = &nodes[1];
    let (core_c, _) = &nodes[2];

    svc.insert(Request::new(InsertRequest {
        triples: vec![
            rel(nodes[0].1.clone(), "hop", nodes[1].1.clone()), // A→B (1 hop)
            rel(nodes[1].1.clone(), "hop", nodes[2].1.clone()), // B→C (2 hops)
        ],
    }))
    .await
    .unwrap();

    let resp = svc
        .reachable(Request::new(ReachableRequest {
            start: Some(nodes[0].1.clone()),
            predicate: "hop".into(),
            max_hops: 1,
        }))
        .await
        .unwrap()
        .into_inner();

    let ids: Vec<Vec<u8>> = resp.node_ids.iter().map(|n| n.bytes.clone()).collect();
    assert_eq!(ids.len(), 1, "only B within 1 hop");
    assert!(ids.contains(&core_b.as_bytes().to_vec()));
    assert!(!ids.contains(&core_c.as_bytes().to_vec()), "C is 2 hops away");
}

/// Isolated node has no reachable neighbours.
#[tokio::test]
async fn reachable_isolated_node_returns_empty() {
    let (svc, _dir) = open();
    let (_, id) = new_node();

    let resp = svc
        .reachable(Request::new(ReachableRequest {
            start: Some(id),
            predicate: "knows".into(),
            max_hops: 0,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.node_ids.is_empty());
}

/// Missing start node ID → InvalidArgument.
#[tokio::test]
async fn reachable_missing_start_returns_invalid_argument() {
    let (svc, _dir) = open();
    let err = svc
        .reachable(Request::new(ReachableRequest {
            start: None,
            predicate: "knows".into(),
            max_hops: 0,
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

/// Empty predicate → InvalidArgument.
#[tokio::test]
async fn reachable_empty_predicate_returns_invalid_argument() {
    let (svc, _dir) = open();
    let (_, id) = new_node();
    let err = svc
        .reachable(Request::new(ReachableRequest {
            start: Some(id),
            predicate: String::new(),
            max_hops: 0,
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

// ── Node type registry tests ──────────────────────────────────────────────────

fn person_type_def() -> NodeTypeDef {
    NodeTypeDef {
        type_name: "Person".into(),
        fields: vec![
            FieldDef { field_name: "name".into(), kind: "text".into(), required: true },
            FieldDef { field_name: "age".into(),  kind: "int".into(),  required: false },
        ],
        vector_space: None,
    }
}

#[tokio::test]
async fn register_and_get_node_type() {
    let (svc, _dir) = open();

    svc.register_node_type(Request::new(RegisterNodeTypeRequest {
        definition: Some(person_type_def()),
    }))
    .await
    .unwrap();

    let resp = svc
        .get_node_type(Request::new(GetNodeTypeRequest { type_name: "Person".into() }))
        .await
        .unwrap()
        .into_inner();

    let def = resp.definition.expect("definition should be present");
    assert_eq!(def.type_name, "Person");
    assert_eq!(def.fields.len(), 2);
}

#[tokio::test]
async fn get_unknown_type_returns_empty_definition() {
    let (svc, _dir) = open();

    let resp = svc
        .get_node_type(Request::new(GetNodeTypeRequest { type_name: "Ghost".into() }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.definition.is_none());
}

#[tokio::test]
async fn list_node_types_returns_all_registered() {
    let (svc, _dir) = open();

    svc.register_node_type(Request::new(RegisterNodeTypeRequest {
        definition: Some(person_type_def()),
    }))
    .await
    .unwrap();

    svc.register_node_type(Request::new(RegisterNodeTypeRequest {
        definition: Some(NodeTypeDef {
            type_name: "Project".into(),
            fields: vec![FieldDef { field_name: "title".into(), kind: "text".into(), required: true }],
            vector_space: None,
        }),
    }))
    .await
    .unwrap();

    let resp = svc
        .list_node_types(Request::new(ListNodeTypesRequest {}))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.definitions.len(), 2);
    let mut names: Vec<_> = resp.definitions.iter().map(|d| d.type_name.clone()).collect();
    names.sort();
    assert_eq!(names, vec!["Person", "Project"]);
}

#[tokio::test]
async fn validate_node_valid_properties() {
    let (svc, _dir) = open();

    svc.register_node_type(Request::new(RegisterNodeTypeRequest {
        definition: Some(person_type_def()),
    }))
    .await
    .unwrap();

    let resp = svc
        .validate_node(Request::new(ValidateNodeRequest {
            type_name: "Person".into(),
            properties: std::collections::HashMap::from([
                ("name".to_string(), Value { kind: Some(ValueKind::TextVal("Alice".into())) }),
                ("age".to_string(),  Value { kind: Some(ValueKind::IntVal(30)) }),
            ]),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.valid);
    assert!(resp.errors.is_empty());
}

#[tokio::test]
async fn validate_node_missing_required_field() {
    let (svc, _dir) = open();

    svc.register_node_type(Request::new(RegisterNodeTypeRequest {
        definition: Some(person_type_def()),
    }))
    .await
    .unwrap();

    // "name" is required but absent.
    let resp = svc
        .validate_node(Request::new(ValidateNodeRequest {
            type_name: "Person".into(),
            properties: std::collections::HashMap::from([
                ("age".to_string(), Value { kind: Some(ValueKind::IntVal(30)) }),
            ]),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(!resp.valid);
    assert_eq!(resp.errors.len(), 1);
    assert!(resp.errors[0].contains("name"));
}

#[tokio::test]
async fn validate_node_wrong_value_kind() {
    let (svc, _dir) = open();

    svc.register_node_type(Request::new(RegisterNodeTypeRequest {
        definition: Some(person_type_def()),
    }))
    .await
    .unwrap();

    // "name" is Text but we pass an Int.
    let resp = svc
        .validate_node(Request::new(ValidateNodeRequest {
            type_name: "Person".into(),
            properties: std::collections::HashMap::from([
                ("name".to_string(), Value { kind: Some(ValueKind::IntVal(99)) }),
            ]),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(!resp.valid);
    assert!(!resp.errors.is_empty());
}

#[tokio::test]
async fn validate_node_unknown_type_returns_invalid() {
    let (svc, _dir) = open();

    let resp = svc
        .validate_node(Request::new(ValidateNodeRequest {
            type_name: "Ghost".into(),
            properties: std::collections::HashMap::new(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(!resp.valid);
    assert!(!resp.errors.is_empty());
    assert!(resp.errors[0].contains("Ghost"));
}

#[tokio::test]
async fn register_node_type_missing_definition_returns_error() {
    let (svc, _dir) = open();
    let err = svc
        .register_node_type(Request::new(RegisterNodeTypeRequest { definition: None }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

// ── Edge type registry tests ──────────────────────────────────────────────────

fn works_at_edge_def() -> EdgeTypeDef {
    EdgeTypeDef {
        predicate: "works_at".into(),
        domain: "Person".into(),
        range: "Company".into(),
        fields: vec![
            FieldDef { field_name: "since".into(), kind: "int".into(), required: true },
        ],
    }
}

#[tokio::test]
async fn register_and_get_edge_type() {
    let (svc, _dir) = open();

    svc.register_edge_type(Request::new(RegisterEdgeTypeRequest {
        definition: Some(works_at_edge_def()),
    }))
    .await
    .unwrap();

    let resp = svc
        .get_edge_type(Request::new(GetEdgeTypeRequest { predicate: "works_at".into() }))
        .await
        .unwrap()
        .into_inner();

    let def = resp.definition.expect("definition should be present");
    assert_eq!(def.predicate, "works_at");
    assert_eq!(def.domain, "Person");
    assert_eq!(def.range, "Company");
    assert_eq!(def.fields.len(), 1);
}

#[tokio::test]
async fn get_unknown_edge_type_returns_empty() {
    let (svc, _dir) = open();

    let resp = svc
        .get_edge_type(Request::new(GetEdgeTypeRequest { predicate: "phantom".into() }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.definition.is_none());
}

#[tokio::test]
async fn list_edge_types_returns_all_registered() {
    let (svc, _dir) = open();

    svc.register_edge_type(Request::new(RegisterEdgeTypeRequest {
        definition: Some(works_at_edge_def()),
    }))
    .await
    .unwrap();

    svc.register_edge_type(Request::new(RegisterEdgeTypeRequest {
        definition: Some(EdgeTypeDef {
            predicate: "knows".into(),
            domain: "".into(),
            range: "".into(),
            fields: vec![],
        }),
    }))
    .await
    .unwrap();

    let resp = svc
        .list_edge_types(Request::new(ListEdgeTypesRequest {}))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.definitions.len(), 2);
    let mut names: Vec<_> = resp.definitions.iter().map(|d| d.predicate.clone()).collect();
    names.sort();
    assert_eq!(names, vec!["knows", "works_at"]);
}

#[tokio::test]
async fn validate_edge_valid() {
    let (svc, _dir) = open();

    svc.register_edge_type(Request::new(RegisterEdgeTypeRequest {
        definition: Some(works_at_edge_def()),
    }))
    .await
    .unwrap();

    let resp = svc
        .validate_edge(Request::new(ValidateEdgeRequest {
            predicate: "works_at".into(),
            subject_type: "Person".into(),
            object_type: "Company".into(),
            properties: std::collections::HashMap::from([
                ("since".to_string(), Value { kind: Some(ValueKind::IntVal(2020)) }),
            ]),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.valid, "errors: {:?}", resp.errors);
}

#[tokio::test]
async fn validate_edge_domain_mismatch() {
    let (svc, _dir) = open();

    svc.register_edge_type(Request::new(RegisterEdgeTypeRequest {
        definition: Some(works_at_edge_def()),
    }))
    .await
    .unwrap();

    let resp = svc
        .validate_edge(Request::new(ValidateEdgeRequest {
            predicate: "works_at".into(),
            subject_type: "Robot".into(),
            object_type: "Company".into(),
            properties: std::collections::HashMap::from([
                ("since".to_string(), Value { kind: Some(ValueKind::IntVal(2020)) }),
            ]),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(!resp.valid);
    assert!(!resp.errors.is_empty());
    assert!(resp.errors[0].contains("Robot") || resp.errors[0].contains("Person"));
}

#[tokio::test]
async fn validate_edge_range_mismatch() {
    let (svc, _dir) = open();

    svc.register_edge_type(Request::new(RegisterEdgeTypeRequest {
        definition: Some(works_at_edge_def()),
    }))
    .await
    .unwrap();

    let resp = svc
        .validate_edge(Request::new(ValidateEdgeRequest {
            predicate: "works_at".into(),
            subject_type: "Person".into(),
            object_type: "Project".into(),
            properties: std::collections::HashMap::from([
                ("since".to_string(), Value { kind: Some(ValueKind::IntVal(2020)) }),
            ]),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(!resp.valid);
    assert!(!resp.errors.is_empty());
    assert!(resp.errors[0].contains("Project") || resp.errors[0].contains("Company"));
}

#[tokio::test]
async fn validate_edge_missing_required_prop() {
    let (svc, _dir) = open();

    svc.register_edge_type(Request::new(RegisterEdgeTypeRequest {
        definition: Some(works_at_edge_def()),
    }))
    .await
    .unwrap();

    // "since" required but absent.
    let resp = svc
        .validate_edge(Request::new(ValidateEdgeRequest {
            predicate: "works_at".into(),
            subject_type: "Person".into(),
            object_type: "Company".into(),
            properties: std::collections::HashMap::new(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(!resp.valid);
    assert_eq!(resp.errors.len(), 1);
    assert!(resp.errors[0].contains("since"));
}

#[tokio::test]
async fn validate_edge_wrong_prop_type() {
    let (svc, _dir) = open();

    svc.register_edge_type(Request::new(RegisterEdgeTypeRequest {
        definition: Some(works_at_edge_def()),
    }))
    .await
    .unwrap();

    let resp = svc
        .validate_edge(Request::new(ValidateEdgeRequest {
            predicate: "works_at".into(),
            subject_type: "Person".into(),
            object_type: "Company".into(),
            properties: std::collections::HashMap::from([
                // "since" should be int, not text
                ("since".to_string(), Value { kind: Some(ValueKind::TextVal("2020".into())) }),
            ]),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(!resp.valid);
    assert!(!resp.errors.is_empty());
    assert!(resp.errors[0].contains("since"));
}

#[tokio::test]
async fn validate_edge_unknown_predicate_is_valid() {
    let (svc, _dir) = open();

    let resp = svc
        .validate_edge(Request::new(ValidateEdgeRequest {
            predicate: "unregistered".into(),
            subject_type: "A".into(),
            object_type: "B".into(),
            properties: std::collections::HashMap::new(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.valid, "unregistered predicate should be treated as valid (open-world)");
}

#[tokio::test]
async fn list_predicates_between_returns_correct_subset() {
    let (svc, _dir) = open();

    svc.register_edge_type(Request::new(RegisterEdgeTypeRequest {
        definition: Some(works_at_edge_def()), // Person → Company
    }))
    .await
    .unwrap();

    svc.register_edge_type(Request::new(RegisterEdgeTypeRequest {
        definition: Some(EdgeTypeDef {
            predicate: "manages".into(),
            domain: "Person".into(),
            range: "Person".into(),
            fields: vec![],
        }),
    }))
    .await
    .unwrap();

    // Person → Company should return only "works_at".
    let resp = svc
        .list_predicates_between(Request::new(ListPredicatesBetweenRequest {
            domain_type: "Person".into(),
            range_type: "Company".into(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.predicates, vec!["works_at"]);
}

#[tokio::test]
async fn list_predicates_between_empty_type_returns_error() {
    let (svc, _dir) = open();
    let err = svc
        .list_predicates_between(Request::new(ListPredicatesBetweenRequest {
            domain_type: String::new(),
            range_type: "Company".into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn register_edge_type_missing_definition_returns_error() {
    let (svc, _dir) = open();
    let err = svc
        .register_edge_type(Request::new(RegisterEdgeTypeRequest { definition: None }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

// ── Named spaces + dimension validation ──────────────────────────────────────

#[tokio::test]
async fn insert_vector_named_space_is_independent() {
    let (svc, _dir) = open();
    let (_, id_a) = new_node();
    let (_, id_b) = new_node();

    svc.insert_vector(Request::new(InsertVectorRequest {
        node_id: Some(id_a.clone()),
        vector: vec![1.0, 0.0],
        space: "space_a".into(),
    }))
    .await
    .unwrap();

    svc.insert_vector(Request::new(InsertVectorRequest {
        node_id: Some(id_b),
        vector: vec![0.0, 1.0],
        space: "space_b".into(),
    }))
    .await
    .unwrap();

    // space_a should return only id_a
    let resp = svc
        .search_vector(Request::new(SearchVectorRequest {
            query: vec![1.0, 0.0],
            k: 5,
            space: "space_a".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.results.len(), 1);
    assert_eq!(resp.results[0].node_id.as_ref().unwrap().bytes, id_a.bytes);
}

#[tokio::test]
async fn register_node_type_with_vector_space_round_trips() {
    let (svc, _dir) = open();
    let def = NodeTypeDef {
        type_name: "Article".into(),
        fields: vec![],
        vector_space: Some(VectorSpaceDef {
            space_name: "article_space".into(),
            dimensions: 4,
            embedding_model: "my-model".into(),
            storage_mode: String::new(),
        }),
    };

    svc.register_node_type(Request::new(polargraph_server::proto::RegisterNodeTypeRequest {
        definition: Some(def.clone()),
    }))
    .await
    .unwrap();

    let resp = svc
        .get_node_type(Request::new(GetNodeTypeRequest { type_name: "Article".into() }))
        .await
        .unwrap()
        .into_inner();

    let got = resp.definition.unwrap();
    let vs = got.vector_space.unwrap();
    assert_eq!(vs.space_name, "article_space");
    assert_eq!(vs.dimensions, 4);
    assert_eq!(vs.embedding_model, "my-model");
}

#[tokio::test]
async fn insert_vector_dimension_mismatch_rejected() {
    let (svc, _dir) = open();

    // Register a node type with a vector space expecting 3 dimensions.
    svc.register_node_type(Request::new(polargraph_server::proto::RegisterNodeTypeRequest {
        definition: Some(NodeTypeDef {
            type_name: "Thing".into(),
            fields: vec![],
            vector_space: Some(VectorSpaceDef {
                space_name: "thing_space".into(),
                dimensions: 3,
                embedding_model: String::new(),
                storage_mode: String::new(),
            }),
        }),
    }))
    .await
    .unwrap();

    let (_, id) = new_node();
    let err = svc
        .insert_vector(Request::new(InsertVectorRequest {
            node_id: Some(id),
            vector: vec![1.0, 0.0],  // 2 dims, not 3
            space: "thing_space".into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

// ── BatchInsertVectors ────────────────────────────────────────────────────────

#[tokio::test]
async fn batch_insert_vectors_inserts_all() {
    let (svc, _dir) = open();
    let (_, id1) = new_node();
    let (_, id2) = new_node();
    let (_, id3) = new_node();

    let resp = svc
        .batch_insert_vectors(Request::new(BatchInsertVectorsRequest {
            space: "default".into(),
            items: vec![
                VectorItem { node_id: Some(id1.clone()), vector: vec![1.0, 0.0, 0.0] },
                VectorItem { node_id: Some(id2), vector: vec![0.0, 1.0, 0.0] },
                VectorItem { node_id: Some(id3), vector: vec![0.0, 0.0, 1.0] },
            ],
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.count_inserted, 3);
    assert!(resp.errors.is_empty());

    let search = svc
        .search_vector(Request::new(SearchVectorRequest {
            query: vec![1.0, 0.0, 0.0],
            k: 1,
            space: "default".into(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(search.results[0].node_id.as_ref().unwrap().bytes, id1.bytes);
}

#[tokio::test]
async fn batch_insert_vectors_rejects_dimension_mismatch() {
    let (svc, _dir) = open();

    svc.register_node_type(Request::new(polargraph_server::proto::RegisterNodeTypeRequest {
        definition: Some(NodeTypeDef {
            type_name: "Doc".into(),
            fields: vec![],
            vector_space: Some(VectorSpaceDef {
                space_name: "doc_space".into(),
                dimensions: 3,
                embedding_model: String::new(),
                storage_mode: String::new(),
            }),
        }),
    }))
    .await
    .unwrap();

    let (_, id) = new_node();
    let resp = svc
        .batch_insert_vectors(Request::new(BatchInsertVectorsRequest {
            space: "doc_space".into(),
            items: vec![VectorItem { node_id: Some(id), vector: vec![1.0, 0.0] }], // wrong dim
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.count_inserted, 0);
    assert_eq!(resp.errors.len(), 1);
    assert_eq!(resp.errors[0].index, 0);
}

// ── SearchVectorInSet ─────────────────────────────────────────────────────────

#[tokio::test]
async fn search_vector_in_set_limits_to_allowed() {
    let (svc, _dir) = open();
    let (_, id1) = new_node();
    let (_, id2) = new_node();
    let (_, id3) = new_node();

    for (id, v) in [
        (id1.clone(), vec![1.0_f32, 0.0, 0.0]),
        (id2.clone(), vec![0.0, 1.0, 0.0]),
        (id3.clone(), vec![0.0, 0.0, 1.0]),
    ] {
        svc.insert_vector(Request::new(InsertVectorRequest {
            node_id: Some(id),
            vector: v,
            space: "default".into(),
        }))
        .await
        .unwrap();
    }

    // id1 is the best match but not in the allowed set (id2, id3).
    let resp = svc
        .search_vector_in_set(Request::new(SearchVectorInSetRequest {
            space: "default".into(),
            query: vec![1.0, 0.0, 0.0],
            k: 2,
            node_ids: vec![id2.clone(), id3.clone()],
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.results.len(), 2);
    assert!(resp.results.iter().all(|r| r.node_id.as_ref().unwrap().bytes != id1.bytes));
}

#[tokio::test]
async fn search_vector_in_set_empty_query_returns_invalid_argument() {
    let (svc, _dir) = open();
    let err = svc
        .search_vector_in_set(Request::new(SearchVectorInSetRequest {
            space: "default".into(),
            query: vec![],
            k: 5,
            node_ids: vec![],
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

// ── SearchVectorFiltered ──────────────────────────────────────────────────────

#[tokio::test]
async fn search_vector_filtered_missing_filter_returns_invalid_argument() {
    let (svc, _dir) = open();
    let err = svc
        .search_vector_filtered(Request::new(SearchVectorFilteredRequest {
            space: "default".into(),
            query: vec![1.0, 0.0],
            k: 5,
            filter: None,
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn search_vector_filtered_by_node_type_returns_only_matching_nodes() {
    let (svc, _dir) = open();
    let (core_a, proto_a) = new_node();
    let (core_b, proto_b) = new_node();
    let (_, proto_c)      = new_node();

    // Tag A and B as "Widget"; C gets no type.
    for (id, type_name) in [
        (proto_a.clone(), "Widget"),
        (proto_b.clone(), "Widget"),
        (proto_c.clone(), "Other"),
    ] {
        svc.insert(Request::new(InsertRequest {
            triples: vec![text_prop(id, "__type", type_name)],
        }))
        .await
        .unwrap();
    }

    // Insert vectors for all three.
    for (id, v) in [
        (proto_a.clone(), vec![1.0_f32, 0.0, 0.0]),
        (proto_b.clone(), vec![0.9, 0.1, 0.0]),
        (proto_c.clone(), vec![0.0, 0.0, 1.0]),
    ] {
        svc.insert_vector(Request::new(InsertVectorRequest {
            node_id: Some(id),
            vector: v,
            space: "default".into(),
        }))
        .await
        .unwrap();
    }

    let resp = svc
        .search_vector_filtered(Request::new(SearchVectorFilteredRequest {
            space: "default".into(),
            query: vec![1.0, 0.0, 0.0],
            k: 5,
            filter: Some(Filter::NodeTypeFilter(NodeTypeFilter { type_name: "Widget".into() })),
        }))
        .await
        .unwrap()
        .into_inner();

    // Only A and B should appear; C is "Other".
    let ids: Vec<Vec<u8>> = resp.results.iter().map(|r| r.node_id.as_ref().unwrap().bytes.clone()).collect();
    assert!(ids.contains(&core_a.as_bytes().to_vec()));
    assert!(ids.contains(&core_b.as_bytes().to_vec()));
    assert!(!ids.iter().any(|b| *b == proto_c.bytes));
}

// ── VectorSeedQuery tests ─────────────────────────────────────────────────────

// Helper: insert a vector for a node and return the proto NodeId.
async fn insert_vec(svc: &PolarGraphServer, node: NodeId, vector: Vec<f32>, space: &str) {
    svc.insert_vector(Request::new(InsertVectorRequest {
        node_id: Some(node),
        vector,
        space: space.into(),
    }))
    .await
    .unwrap();
}

#[tokio::test]
async fn vector_seed_query_binds_seeds_to_graph_patterns() {
    // Setup: three nodes A, B, C in a chain A -follows-> B -follows-> C.
    // A is near the query vector; C is far. With a "follows" pattern we expect
    // only B (the node A follows) to appear in the results.
    let (svc, _dir) = open();
    let (core_a, proto_a) = new_node();
    let (core_b, proto_b) = new_node();
    let (_, proto_c) = new_node();

    // A follows B; A follows C.
    svc.insert(Request::new(InsertRequest {
        triples: vec![
            rel(proto_a.clone(), "follows", proto_b.clone()),
            rel(proto_a.clone(), "follows", proto_c.clone()),
        ],
    }))
    .await
    .unwrap();

    // Vectors: A is very close to [1,0,0]; B is moderately close; C is far.
    insert_vec(&svc, proto_a.clone(), vec![1.0_f32, 0.0, 0.0], "default").await;
    insert_vec(&svc, proto_b.clone(), vec![0.9_f32, 0.1, 0.0], "default").await;
    insert_vec(&svc, proto_c.clone(), vec![0.0_f32, 0.0, 1.0], "default").await;

    // Query: find "n" (seed) that follows "friend".
    // Only A is close to the query vector, so "n" = A, "friend" = {B, C}.
    let resp = svc
        .vector_seed_query(Request::new(VectorSeedQueryRequest {
            space: "default".into(),
            query_vector: vec![1.0, 0.0, 0.0],
            k: 3,
            seed_variable: "n".into(),
            patterns: vec![pattern(var("n"), "follows", var("friend"))],
            snapshot_ts: 0,
            filter: None,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(!resp.bindings.is_empty(), "expected at least one result");

    // Every returned row must have "n" = A.
    for row in &resp.bindings {
        let n_id = row.vars.get("n").expect("missing 'n' var");
        assert_eq!(n_id.bytes, core_a.as_bytes().to_vec());
        assert!(row.score > 0.0, "score must be positive");
    }

    // The "friend" variables should include both B and C.
    let friend_ids: Vec<Vec<u8>> = resp.bindings.iter()
        .map(|row| row.vars["friend"].bytes.clone())
        .collect();
    assert!(friend_ids.contains(&core_b.as_bytes().to_vec()), "B should be a friend of A");
}

#[tokio::test]
async fn vector_seed_query_with_node_type_filter() {
    // Only "TypedNode" nodes should appear as seeds; untyped nodes are excluded.
    let (svc, _dir) = open();
    let (core_typed, proto_typed) = new_node();
    let (_, proto_untyped) = new_node();

    // Mark proto_typed as "TypedNode".
    svc.insert(Request::new(InsertRequest {
        triples: vec![text_prop(proto_typed.clone(), "__type", "TypedNode")],
    }))
    .await
    .unwrap();

    // Both nodes get similar vectors.
    insert_vec(&svc, proto_typed.clone(), vec![1.0_f32, 0.0], "default").await;
    insert_vec(&svc, proto_untyped.clone(), vec![0.98_f32, 0.02], "default").await;

    let resp = svc
        .vector_seed_query(Request::new(VectorSeedQueryRequest {
            space: "default".into(),
            query_vector: vec![1.0, 0.0],
            k: 5,
            seed_variable: "n".into(),
            patterns: vec![],
            snapshot_ts: 0,
            filter: Some(SeedFilter::NodeTypeFilter(NodeTypeFilter {
                type_name: "TypedNode".into(),
            })),
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.bindings.len(), 1, "only the typed node should be returned");
    assert_eq!(resp.bindings[0].vars["n"].bytes, core_typed.as_bytes().to_vec());
}

#[tokio::test]
async fn vector_seed_query_empty_patterns_returns_seed_bindings() {
    // With no graph patterns, results are exactly the k ANN hits with scores.
    let (svc, _dir) = open();
    let (core_a, proto_a) = new_node();
    let (core_b, proto_b) = new_node();

    insert_vec(&svc, proto_a.clone(), vec![1.0_f32, 0.0], "default").await;
    insert_vec(&svc, proto_b.clone(), vec![0.0_f32, 1.0], "default").await;

    let resp = svc
        .vector_seed_query(Request::new(VectorSeedQueryRequest {
            space: "default".into(),
            query_vector: vec![1.0, 0.0],
            k: 2,
            seed_variable: "n".into(),
            patterns: vec![],
            snapshot_ts: 0,
            filter: None,
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.bindings.len(), 2);

    let result_ids: Vec<Vec<u8>> = resp.bindings.iter()
        .map(|b| b.vars["n"].bytes.clone())
        .collect();
    assert!(result_ids.contains(&core_a.as_bytes().to_vec()));
    assert!(result_ids.contains(&core_b.as_bytes().to_vec()));

    // All scores must be in [-1, 1].
    for row in &resp.bindings {
        assert!(row.score >= -1.0 && row.score <= 1.0, "score out of range: {}", row.score);
    }
}

#[tokio::test]
async fn vector_seed_query_seeds_with_no_edges_return_no_rows() {
    // Seeds exist in the vector index but have no edges matching the pattern.
    // The join should produce zero results.
    let (svc, _dir) = open();
    let (_, proto_a) = new_node();

    insert_vec(&svc, proto_a.clone(), vec![1.0_f32, 0.0], "default").await;

    let resp = svc
        .vector_seed_query(Request::new(VectorSeedQueryRequest {
            space: "default".into(),
            query_vector: vec![1.0, 0.0],
            k: 5,
            seed_variable: "n".into(),
            // Pattern requires "n" to "knows" someone — but no such edge exists.
            patterns: vec![pattern(var("n"), "knows", var("friend"))],
            snapshot_ts: 0,
            filter: None,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.bindings.is_empty(), "no edges means no results");
}

// ── Memmap storage mode ───────────────────────────────────────────────────────

#[tokio::test]
async fn mmap_storage_mode_insert_and_search() {
    // Register a node type with storage_mode = "mmap", insert vectors, search.
    let (svc, _dir) = open();

    svc.register_node_type(Request::new(polargraph_server::proto::RegisterNodeTypeRequest {
        definition: Some(NodeTypeDef {
            type_name: "MmapDoc".into(),
            fields: vec![],
            vector_space: Some(VectorSpaceDef {
                space_name: "mmap_space".into(),
                dimensions: 3,
                embedding_model: String::new(),
                storage_mode: "mmap".into(),
            }),
        }),
    }))
    .await
    .unwrap();

    let (core_a, proto_a) = new_node();
    let (_, proto_b) = new_node();
    let (_, proto_c) = new_node();

    for (proto, vec) in [
        (&proto_a, vec![1.0_f32, 0.0, 0.0]),
        (&proto_b, vec![0.0_f32, 1.0, 0.0]),
        (&proto_c, vec![0.0_f32, 0.0, 1.0]),
    ] {
        svc.insert_vector(Request::new(InsertVectorRequest {
            node_id: Some(proto.clone()),
            vector: vec,
            space: "mmap_space".into(),
        }))
        .await
        .unwrap();
    }

    let resp = svc
        .search_vector(Request::new(SearchVectorRequest {
            query: vec![1.0, 0.0, 0.0],
            k: 1,
            space: "mmap_space".into(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.results.len(), 1);
    assert_eq!(
        resp.results[0].node_id.as_ref().unwrap().bytes,
        core_a.as_bytes().to_vec()
    );
}

#[tokio::test]
async fn mmap_storage_mode_round_trips_in_node_type() {
    // storage_mode field survives register → get round-trip.
    let (svc, _dir) = open();

    svc.register_node_type(Request::new(polargraph_server::proto::RegisterNodeTypeRequest {
        definition: Some(NodeTypeDef {
            type_name: "MmapNode".into(),
            fields: vec![],
            vector_space: Some(VectorSpaceDef {
                space_name: "mm_space".into(),
                dimensions: 8,
                embedding_model: String::new(),
                storage_mode: "mmap".into(),
            }),
        }),
    }))
    .await
    .unwrap();

    let resp = svc
        .get_node_type(Request::new(GetNodeTypeRequest { type_name: "MmapNode".into() }))
        .await
        .unwrap()
        .into_inner();

    let vs = resp.definition.unwrap().vector_space.unwrap();
    assert_eq!(vs.storage_mode, "mmap");
    assert_eq!(vs.dimensions, 8);
}
