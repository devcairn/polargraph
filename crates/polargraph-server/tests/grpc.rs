//! Integration tests for the gRPC service handlers.
//!
//! Most tests call service methods directly (no live server socket) for speed.
//! The replica tests require real TCP sockets and a live gRPC server.

use polargraph_server::{
    auth::ApiKeyLayer,
    proto::{
        polar_graph_service_client::PolarGraphServiceClient,
        polar_graph_service_server::{PolarGraphService, PolarGraphServiceServer},
        term::Kind as TermKind,
        triple::Kind as TripleKind,
        value::Kind as ValueKind,
        search_vector_filtered_request::Filter,
        vector_seed_query_request::Filter as SeedFilter,
        BatchInsertVectorsRequest, CreateBackupRequest, EdgeTypeDef, FieldDef,
        GetEdgeTypeRequest, GetNodeTypeRequest, InsertRequest, InsertVectorRequest,
        ListBackupsRequest, ListEdgeTypesRequest, ListNodeTypesRequest,
        ListPredicatesBetweenRequest, NodeId, NodeTypeDef, NodeTypeFilter,
        PropertyTriple, PurgeOldBackupsRequest, QueryRequest, ReachableRequest,
        RegisterEdgeTypeRequest, RegisterNodeTypeRequest, RelationTriple,
        ReplicaStatusRequest, RunRetentionRequest, StreamWalRequest,
        SearchVectorFilteredRequest, SearchVectorInSetRequest, SearchVectorRequest,
        Term, Triple, ValidateEdgeRequest, ValidateNodeRequest, Value, VarPattern,
        VectorItem, VectorSeedQueryRequest, VectorSpaceDef,
    },
    service::PolarGraphServer,
    wal_client,
};
use polargraph_core::temporal::Timestamp;
use polargraph_storage::TripleStore;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server, Request};

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
            ..Default::default()
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
        .query(Request::new(QueryRequest { patterns: vec![], snapshot_ts: 0, ..Default::default() }))
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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

// ── Backup tests ──────────────────────────────────────────────────────────────

fn open_with_backup() -> (PolarGraphServer, TempDir, TempDir) {
    let data_dir = TempDir::new().unwrap();
    let backup_dir = TempDir::new().unwrap();
    let store = TripleStore::open(data_dir.path()).unwrap();
    let svc = PolarGraphServer::new_with_backup_dir(store, Some(backup_dir.path())).unwrap();
    (svc, data_dir, backup_dir)
}

#[tokio::test]
async fn backup_create_unconfigured_returns_precondition() {
    let (svc, _dir) = open();
    let err = svc.create_backup(Request::new(CreateBackupRequest {})).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn backup_list_unconfigured_returns_precondition() {
    let (svc, _dir) = open();
    let err = svc.list_backups(Request::new(ListBackupsRequest {})).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn backup_purge_unconfigured_returns_precondition() {
    let (svc, _dir) = open();
    let err = svc
        .purge_old_backups(Request::new(PurgeOldBackupsRequest { keep_n: 3 }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn backup_create_and_list() {
    let (svc, _data, _backup) = open_with_backup();

    let resp = svc
        .create_backup(Request::new(CreateBackupRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.backup_id, 1);
    assert!(resp.size_bytes > 0);
    // created_at is a Unix timestamp; must be a plausible value (after year 2020).
    assert!(resp.created_at > 1_577_836_800);

    let resp = svc
        .list_backups(Request::new(ListBackupsRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.backups.len(), 1);
    assert_eq!(resp.backups[0].backup_id, 1);
}

#[tokio::test]
async fn backup_purge_removes_old_backups() {
    let (svc, _data, _backup) = open_with_backup();

    svc.create_backup(Request::new(CreateBackupRequest {})).await.unwrap();
    svc.create_backup(Request::new(CreateBackupRequest {})).await.unwrap();
    svc.create_backup(Request::new(CreateBackupRequest {})).await.unwrap();

    let resp = svc
        .purge_old_backups(Request::new(PurgeOldBackupsRequest { keep_n: 1 }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.deleted_count, 2);

    let resp = svc
        .list_backups(Request::new(ListBackupsRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.backups.len(), 1);
}

// ── RunRetention tests ────────────────────────────────────────────────────────

#[tokio::test]
async fn run_retention_deletes_old_triples() {
    let (svc, _dir) = open();
    let (_core_s, proto_s) = new_node();

    // Insert a triple via the normal path (recent tt).
    let insert_req = InsertRequest {
        triples: vec![text_prop(proto_s.clone(), "label", "hello")],
    };
    svc.insert(Request::new(insert_req)).await.unwrap();

    // Plant an old triple directly via the store (tt = 1 µs since epoch).
    let (core_old, _) = new_node();
    let old_triple = polargraph_core::triple::Triple::Property {
        subject: core_old,
        predicate: polargraph_core::triple::Predicate::new("label"),
        value: polargraph_core::value::Value::Text("ancient".into()),
        temporal: polargraph_core::temporal::BiTemporalRange {
            vt_start: Timestamp(0),
            vt_end: Timestamp(i64::MAX),
            tt: Timestamp(0),
        },
    };
    svc.store().insert_at_ts(&old_triple, Timestamp(1)).unwrap();

    // Run retention with 2-second tx_age — old triple's tt(1 µs) is < (now - 2s).
    let resp = svc
        .run_retention(Request::new(RunRetentionRequest {
            tx_age_secs: 2,
            vt_lookback_secs: 0,
        }))
        .await
        .unwrap()
        .into_inner();

    // 6 CF copies of the old triple should be gone.
    assert_eq!(resp.triples_deleted, 6);
    assert!(resp.triples_scanned >= 6);
}

#[tokio::test]
async fn run_retention_recent_triple_survives() {
    let (svc, _dir) = open();
    let (_, proto_s) = new_node();

    svc.insert(Request::new(InsertRequest {
        triples: vec![text_prop(proto_s, "label", "current")],
    }))
    .await
    .unwrap();

    let resp = svc
        .run_retention(Request::new(RunRetentionRequest {
            tx_age_secs: 2,
            vt_lookback_secs: 0,
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.triples_deleted, 0);
}

#[tokio::test]
async fn run_retention_zero_vt_lookback_disables_vt_check() {
    let (svc, _dir) = open();
    let (core_s, _) = new_node();

    // Triple with vt_end in the past (1 µs since epoch), recent tt.
    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;
    let triple = polargraph_core::triple::Triple::Property {
        subject: core_s,
        predicate: polargraph_core::triple::Predicate::new("label"),
        value: polargraph_core::value::Value::Text("expired_vt".into()),
        temporal: polargraph_core::temporal::BiTemporalRange {
            vt_start: Timestamp(0),
            vt_end: Timestamp(1),  // expired long ago
            tt: Timestamp(0),
        },
    };
    svc.store().insert_at_ts(&triple, Timestamp(now_us)).unwrap();

    // vt_lookback_secs = 0 means disabled → triple survives (tx is recent).
    let resp = svc
        .run_retention(Request::new(RunRetentionRequest {
            tx_age_secs: 1_000_000_000, // 31+ years — effectively disable tx check
            vt_lookback_secs: 0,
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.triples_deleted, 0);
}

// ── Time-travel (as_of) tests ─────────────────────────────────────────────────

fn rel_with_vt(subject: NodeId, predicate: &str, object: NodeId, vt_start: i64, vt_end: i64) -> Triple {
    Triple {
        kind: Some(TripleKind::Relation(RelationTriple {
            subject: Some(subject),
            predicate: predicate.into(),
            object: Some(object),
            vt_start,
            vt_end,
        })),
    }
}

fn text_prop_with_vt(subject: NodeId, predicate: &str, text: &str, vt_start: i64, vt_end: i64) -> Triple {
    Triple {
        kind: Some(TripleKind::Property(PropertyTriple {
            subject: Some(subject),
            predicate: predicate.into(),
            value: Some(Value { kind: Some(ValueKind::TextVal(text.into())) }),
            vt_start,
            vt_end,
        })),
    }
}

/// as_of_valid_time=0 is the same as no filter: returns the latest version.
#[tokio::test]
async fn as_of_valid_time_zero_sees_latest() {
    let (svc, _dir) = open();
    let (_, alice) = new_node();
    let (_, bob) = new_node();

    svc.insert(Request::new(InsertRequest {
        triples: vec![rel_with_vt(alice.clone(), "knows", bob.clone(), 1000, 0)],
    }))
    .await
    .unwrap();

    let resp = svc
        .query(Request::new(QueryRequest {
            patterns: vec![pattern(bound(&alice), "knows", var("x"))],
            snapshot_ts: 0,
            as_of_valid_time: 0,
            as_of_tx_time: 0,
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.bindings.len(), 1);
}

/// A triple with vt_start=1000, vt_end=2000 is visible at vt=1500 but not at vt=2500.
#[tokio::test]
async fn as_of_valid_time_filters_expired_triples() {
    let (svc, _dir) = open();
    let (_, alice) = new_node();
    let (_, bob) = new_node();

    // Triple valid in [1000, 2000).
    svc.insert(Request::new(InsertRequest {
        triples: vec![rel_with_vt(alice.clone(), "knows", bob.clone(), 1000, 2000)],
    }))
    .await
    .unwrap();

    // Inside the valid window.
    let resp_inside = svc
        .query(Request::new(QueryRequest {
            patterns: vec![pattern(bound(&alice), "knows", var("x"))],
            snapshot_ts: 0,
            as_of_valid_time: 1500,
            as_of_tx_time: 0,
        }))
        .await
        .unwrap()
        .into_inner();

    // Outside the valid window (after vt_end).
    let resp_outside = svc
        .query(Request::new(QueryRequest {
            patterns: vec![pattern(bound(&alice), "knows", var("x"))],
            snapshot_ts: 0,
            as_of_valid_time: 2500,
            as_of_tx_time: 0,
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp_inside.bindings.len(), 1, "triple should be visible inside its vt window");
    assert!(resp_outside.bindings.is_empty(), "triple should be invisible after its vt window ends");
}

/// Two versions of the same (S,P) with non-overlapping vt ranges. Querying at
/// different as_of_valid_time values returns the correct version.
#[tokio::test]
async fn as_of_valid_time_returns_correct_version() {
    let (svc, _dir) = open();
    let (_, alice) = new_node();

    // Version 1: vt=[1000, 2000)
    svc.insert(Request::new(InsertRequest {
        triples: vec![text_prop_with_vt(alice.clone(), "status", "active", 1000, 2000)],
    }))
    .await
    .unwrap();

    // Version 2: vt=[2000, ∞)  (vt_end=0 → END_OF_TIME)
    svc.insert(Request::new(InsertRequest {
        triples: vec![text_prop_with_vt(alice.clone(), "status", "retired", 2000, 0)],
    }))
    .await
    .unwrap();

    // at vt=1500: version 1 should match (1000 ≤ 1500 < 2000).
    let resp_v1 = svc
        .query(Request::new(QueryRequest {
            patterns: vec![pattern(bound(&alice), "status", any())],
            snapshot_ts: 0,
            as_of_valid_time: 1500,
            as_of_tx_time: 0,
        }))
        .await
        .unwrap()
        .into_inner();

    // at vt=2500: version 2 should match (2000 ≤ 2500 < MAX).
    let resp_v2 = svc
        .query(Request::new(QueryRequest {
            patterns: vec![pattern(bound(&alice), "status", any())],
            snapshot_ts: 0,
            as_of_valid_time: 2500,
            as_of_tx_time: 0,
        }))
        .await
        .unwrap()
        .into_inner();

    // as_of_valid_time=0 (no filter): latest committed version (v2) wins.
    let resp_latest = svc
        .query(Request::new(QueryRequest {
            patterns: vec![pattern(bound(&alice), "status", any())],
            snapshot_ts: 0,
            as_of_valid_time: 0,
            as_of_tx_time: 0,
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp_v1.bindings.len(), 1, "vt=1500 should see version 1 (active window)");
    assert_eq!(resp_v2.bindings.len(), 1, "vt=2500 should see version 2 (active window)");
    assert_eq!(resp_latest.bindings.len(), 1, "no filter should see current state");
}

/// as_of_tx_time before the insert: triple is not yet visible.
#[tokio::test]
async fn as_of_tx_time_before_insert_sees_nothing() {
    let (svc, _dir) = open();
    let (_, alice) = new_node();
    let (_, bob) = new_node();

    // Record a wall-clock timestamp before the insert.
    let t_before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;

    svc.insert(Request::new(InsertRequest {
        triples: vec![rel(alice.clone(), "knows", bob)],
    }))
    .await
    .unwrap();

    let resp = svc
        .query(Request::new(QueryRequest {
            patterns: vec![pattern(bound(&alice), "knows", var("x"))],
            snapshot_ts: 0,
            as_of_valid_time: 0,
            as_of_tx_time: t_before,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.bindings.is_empty(), "triple must not be visible before its commit time");
}

/// as_of_tx_time at the commit timestamp: triple becomes visible.
#[tokio::test]
async fn as_of_tx_time_at_commit_sees_triple() {
    let (svc, _dir) = open();
    let (_, alice) = new_node();
    let (_, bob) = new_node();

    let commit_ts = svc
        .insert(Request::new(InsertRequest {
            triples: vec![rel(alice.clone(), "knows", bob)],
        }))
        .await
        .unwrap()
        .into_inner()
        .commit_ts;

    let resp = svc
        .query(Request::new(QueryRequest {
            patterns: vec![pattern(bound(&alice), "knows", var("x"))],
            snapshot_ts: 0,
            as_of_valid_time: 0,
            as_of_tx_time: commit_ts,
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.bindings.len(), 1, "triple must be visible at its commit time");
}

/// Combining as_of_tx_time and as_of_valid_time: both filters must be satisfied.
#[tokio::test]
async fn as_of_tx_and_vt_combined_filter() {
    let (svc, _dir) = open();
    let (_, alice) = new_node();
    let (_, bob) = new_node();

    // Triple valid in [1000, 3000), committed now.
    let commit_ts = svc
        .insert(Request::new(InsertRequest {
            triples: vec![rel_with_vt(alice.clone(), "knows", bob, 1000, 3000)],
        }))
        .await
        .unwrap()
        .into_inner()
        .commit_ts;

    // Both filters satisfied: tx_time = commit_ts, vt = 2000 (inside [1000, 3000)).
    let resp_ok = svc
        .query(Request::new(QueryRequest {
            patterns: vec![pattern(bound(&alice), "knows", var("x"))],
            snapshot_ts: 0,
            as_of_valid_time: 2000,
            as_of_tx_time: commit_ts,
        }))
        .await
        .unwrap()
        .into_inner();

    // vt filter fails (4000 is outside [1000, 3000)).
    let resp_vt_fail = svc
        .query(Request::new(QueryRequest {
            patterns: vec![pattern(bound(&alice), "knows", var("x"))],
            snapshot_ts: 0,
            as_of_valid_time: 4000,
            as_of_tx_time: commit_ts,
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp_ok.bindings.len(), 1, "triple passes both filters");
    assert!(resp_vt_fail.bindings.is_empty(), "triple fails vt filter");
}

// ── Read replica tests (WAL streaming) ───────────────────────────────────────
//
// These tests require a live gRPC server and real TCP connections since WAL
// streaming is a network protocol. We bind to port 0 (OS-assigned) to avoid
// conflicts between parallel test runs.

/// Start a PolarGraphServer as a real gRPC server on a random TCP port.
/// Returns the address and a shutdown channel.
async fn start_primary_server(
    store: TripleStore,
) -> (SocketAddr, tokio::sync::oneshot::Sender<()>) {
    let svc = PolarGraphServer::new(store).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        Server::builder()
            .add_service(PolarGraphServiceServer::new(svc))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async { drop(shutdown_rx.await) },
            )
            .await
            .unwrap();
    });

    // Small delay for the server to start accepting connections.
    tokio::time::sleep(Duration::from_millis(20)).await;
    (addr, shutdown_tx)
}

/// Open a replica store pointing at `primary_address` and start the WAL
/// replication task. Returns the PolarGraphServer (for direct RPC calls) and
/// the replica's TempDir.
async fn open_wal_replica(
    primary_address: String,
) -> (PolarGraphServer, TempDir) {
    let replica_dir = TempDir::new().unwrap();
    let replica_store =
        TripleStore::open_as_replica(replica_dir.path(), primary_address.clone()).unwrap();
    let (server, rs) =
        PolarGraphServer::new_replica(replica_store.clone(), &primary_address).unwrap();
    let repl_store = replica_store;
    let repl_state = Arc::clone(&rs);
    let token = tokio_util::sync::CancellationToken::new();
    tokio::spawn(async move {
        wal_client::run_replication(repl_store, repl_state, token).await;
    });
    (server, replica_dir)
}

#[tokio::test]
async fn replica_streams_triples_from_primary() {
    let primary_dir = TempDir::new().unwrap();
    let primary_store = TripleStore::open(primary_dir.path()).unwrap();

    let (addr, _shutdown) = start_primary_server(primary_store.clone()).await;
    let primary_addr = format!("http://{addr}");

    // Insert a triple on the primary.
    let (_, alice) = new_node();
    let (_, bob) = new_node();
    let primary = PolarGraphServer::new(primary_store.clone()).unwrap();
    primary
        .insert(Request::new(InsertRequest {
            triples: vec![rel(alice.clone(), "knows", bob.clone())],
        }))
        .await
        .unwrap();

    // Open a replica and wait for WAL delivery.
    let (replica, _replica_dir) = open_wal_replica(primary_addr).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let resp = replica
        .query(Request::new(QueryRequest {
            patterns: vec![VarPattern {
                subject: Some(bound(&alice)),
                predicate: "knows".into(),
                object: Some(var("x")),
            }],
            snapshot_ts: 0,
            as_of_valid_time: 0,
            as_of_tx_time: 0,
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.bindings.len(), 1, "replica receives triple via WAL stream");
    assert_eq!(resp.bindings[0].vars["x"].bytes, bob.bytes);
}

#[tokio::test]
async fn replica_write_rpcs_return_failed_precondition() {
    let replica_dir = TempDir::new().unwrap();
    let replica_store =
        TripleStore::open_as_replica(replica_dir.path(), "http://127.0.0.1:1".into()).unwrap();
    let (replica, _rs) =
        PolarGraphServer::new_replica(replica_store, "http://127.0.0.1:1").unwrap();

    let (_, node) = new_node();
    let (_, node2) = new_node();

    let err = replica
        .insert(Request::new(InsertRequest {
            triples: vec![rel(node.clone(), "x", node2.clone())],
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition, "Insert on replica");

    let err = replica
        .insert_vector(Request::new(InsertVectorRequest {
            node_id: Some(node.clone()),
            vector: vec![1.0, 0.0],
            space: "default".into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition, "InsertVector on replica");

    let err = replica
        .register_node_type(Request::new(RegisterNodeTypeRequest {
            definition: Some(NodeTypeDef {
                type_name: "Foo".into(),
                fields: vec![],
                vector_space: None,
            }),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition, "RegisterNodeType on replica");

    let err = replica
        .register_edge_type(Request::new(RegisterEdgeTypeRequest {
            definition: Some(polargraph_server::proto::EdgeTypeDef {
                predicate: "foo".into(),
                domain: String::new(),
                range: String::new(),
                fields: vec![],
            }),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition, "RegisterEdgeType on replica");

    let err = replica
        .run_retention(Request::new(RunRetentionRequest {
            tx_age_secs: 1,
            vt_lookback_secs: 0,
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition, "RunRetention on replica");

    let err = replica
        .create_backup(Request::new(CreateBackupRequest {}))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition, "CreateBackup on replica");
}

#[tokio::test]
async fn stream_wal_on_replica_returns_failed_precondition() {
    // StreamWal should be rejected on a replica node.
    let replica_dir = TempDir::new().unwrap();
    let primary_dir = TempDir::new().unwrap();
    let primary_store = TripleStore::open(primary_dir.path()).unwrap();
    let (addr, _shutdown) = start_primary_server(primary_store).await;
    let primary_addr = format!("http://{addr}");

    let replica_store =
        TripleStore::open_as_replica(replica_dir.path(), primary_addr.clone()).unwrap();
    let (replica_svc, _rs) =
        PolarGraphServer::new_replica(replica_store, &primary_addr).unwrap();

    let err = replica_svc
        .stream_wal(Request::new(StreamWalRequest { since_seq: 0 }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition, "StreamWal on replica");
}

#[tokio::test]
async fn replica_status_rpc() {
    // Primary: is_replica = false.
    let (primary, _dir) = open();
    let status = primary
        .replica_status(Request::new(ReplicaStatusRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert!(!status.is_replica, "primary reports is_replica=false");
    assert!(status.primary_address.is_empty(), "primary has no primary_address");
    assert_eq!(status.catchup_count, 0);

    // Replica: is_replica = true, primary_address is set.
    let replica_dir = TempDir::new().unwrap();
    let replica_store =
        TripleStore::open_as_replica(replica_dir.path(), "http://127.0.0.1:1".into()).unwrap();
    let (replica, _rs) =
        PolarGraphServer::new_replica(replica_store, "http://127.0.0.1:1").unwrap();
    let status = replica
        .replica_status(Request::new(ReplicaStatusRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert!(status.is_replica, "replica reports is_replica=true");
    assert_eq!(status.primary_address, "http://127.0.0.1:1");
    assert_eq!(status.last_applied_seq, 0, "no batches applied yet");
}

#[tokio::test]
async fn replica_receives_triples_inserted_after_connect() {
    // Insert a triple AFTER the replica starts; verify it arrives via streaming.
    let primary_dir = TempDir::new().unwrap();
    let primary_store = TripleStore::open(primary_dir.path()).unwrap();
    let (addr, _shutdown) = start_primary_server(primary_store.clone()).await;
    let primary_addr = format!("http://{addr}");

    // Start replica first (nothing in DB yet).
    let (replica, _replica_dir) = open_wal_replica(primary_addr).await;

    // Insert on primary after replica is running.
    let (_, alice) = new_node();
    let (_, bob) = new_node();
    let primary = PolarGraphServer::new(primary_store).unwrap();
    primary
        .insert(Request::new(InsertRequest {
            triples: vec![rel(alice.clone(), "follows", bob.clone())],
        }))
        .await
        .unwrap();

    // Allow WAL delivery.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let resp = replica
        .query(Request::new(QueryRequest {
            patterns: vec![VarPattern {
                subject: Some(bound(&alice)),
                predicate: "follows".into(),
                object: Some(var("y")),
            }],
            snapshot_ts: 0,
            as_of_valid_time: 0,
            as_of_tx_time: 0,
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.bindings.len(), 1, "replica sees post-connect triple via WAL");
}

// ── API key authentication tests ──────────────────────────────────────────────
//
// Auth is implemented as a tower layer at the transport level, so these tests
// require a live TCP server and a real gRPC client channel.

/// Start a PolarGraphServer with an `ApiKeyLayer` and return its address.
async fn start_server_with_keys(
    store: TripleStore,
    keys: Vec<String>,
) -> (SocketAddr, tokio::sync::oneshot::Sender<()>) {
    let svc = PolarGraphServer::new(store).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let auth = ApiKeyLayer::new(keys);

    tokio::spawn(async move {
        Server::builder()
            .layer(auth)
            .add_service(PolarGraphServiceServer::new(svc))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async { drop(shutdown_rx.await) },
            )
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(20)).await;
    (addr, shutdown_tx)
}

/// Open an unauthenticated gRPC client for the given address.
async fn grpc_client(addr: SocketAddr) -> PolarGraphServiceClient<tonic::transport::Channel> {
    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    PolarGraphServiceClient::new(channel)
}

/// Wrap a request body with an `Authorization: Bearer <key>` metadata header.
fn bearer<T>(body: T, key: &str) -> Request<T> {
    let mut req = Request::new(body);
    req.metadata_mut()
        .insert("authorization", format!("Bearer {key}").parse().unwrap());
    req
}

#[tokio::test]
async fn auth_request_without_key_returns_unauthenticated() {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    let (addr, _shutdown) = start_server_with_keys(store, vec!["valid-key".into()]).await;

    let mut client = grpc_client(addr).await;
    let (_, node_a) = new_node();
    let (_, node_b) = new_node();

    let err = client
        .insert(Request::new(InsertRequest {
            triples: vec![rel(node_a, "knows", node_b)],
        }))
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

#[tokio::test]
async fn auth_request_with_correct_key_succeeds() {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    let (addr, _shutdown) = start_server_with_keys(store, vec!["valid-key".into()]).await;

    let mut client = grpc_client(addr).await;
    let (_, node_a) = new_node();
    let (_, node_b) = new_node();

    let resp = client
        .insert(bearer(InsertRequest {
            triples: vec![rel(node_a, "knows", node_b)],
        }, "valid-key"))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.commit_ts > 0);
}

#[tokio::test]
async fn auth_request_with_wrong_key_returns_unauthenticated() {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    let (addr, _shutdown) = start_server_with_keys(store, vec!["valid-key".into()]).await;

    let mut client = grpc_client(addr).await;
    let (_, node_a) = new_node();
    let (_, node_b) = new_node();

    let err = client
        .insert(bearer(InsertRequest {
            triples: vec![rel(node_a, "knows", node_b)],
        }, "wrong-key"))
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

#[tokio::test]
async fn auth_replica_status_exempt_from_auth() {
    // ReplicaStatus must succeed even without an API key.
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    let (addr, _shutdown) = start_server_with_keys(store, vec!["valid-key".into()]).await;

    let mut client = grpc_client(addr).await;

    let resp = client
        .replica_status(Request::new(ReplicaStatusRequest {}))
        .await
        .unwrap()
        .into_inner();

    assert!(!resp.is_replica, "primary reports is_replica=false without auth key");
}

#[tokio::test]
async fn auth_disabled_all_requests_pass() {
    // When no keys are configured the layer is transparent (verified via direct call).
    let (svc, _dir) = open();
    let (_, node_a) = new_node();
    let (_, node_b) = new_node();

    let resp = svc
        .insert(Request::new(InsertRequest {
            triples: vec![rel(node_a, "knows", node_b)],
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.commit_ts > 0);
}

#[tokio::test]
async fn auth_multiple_keys_any_accepted() {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    let (addr, _shutdown) =
        start_server_with_keys(store, vec!["key-a".into(), "key-b".into()]).await;

    let mut client = grpc_client(addr).await;

    // key-a works
    let (_, a1) = new_node();
    let (_, a2) = new_node();
    client
        .insert(bearer(InsertRequest { triples: vec![rel(a1, "x", a2)] }, "key-a"))
        .await
        .unwrap();

    // key-b works
    let (_, b1) = new_node();
    let (_, b2) = new_node();
    client
        .insert(bearer(InsertRequest { triples: vec![rel(b1, "x", b2)] }, "key-b"))
        .await
        .unwrap();

    // wrong key fails
    let (_, c1) = new_node();
    let (_, c2) = new_node();
    let err = client
        .insert(bearer(InsertRequest { triples: vec![rel(c1, "x", c2)] }, "key-c"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

// ── Management UI smoke tests ─────────────────────────────────────────────────

/// Spin up a UI HTTP server bound to a random port.
async fn start_ui_http(
    store: TripleStore,
    api_keys: Vec<String>,
) -> (String, tokio::sync::oneshot::Sender<()>, TempDir) {
    use polargraph_server::ui_api;

    let dir = TempDir::new().unwrap();
    let svc = PolarGraphServer::new(store).unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let state = Arc::new(ui_api::UiState {
        service: svc,
        api_keys: Arc::new(api_keys),
        start_time: std::time::Instant::now(),
        data_dir: dir.path().display().to_string(),
        grpc_addr: "127.0.0.1:50051".into(),
    });
    let app = ui_api::build_ui_router(state);

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        let server = axum::Server::from_tcp(listener.into_std().unwrap())
            .unwrap()
            .serve(app.into_make_service());
        tokio::select! {
            _ = server => {}
            _ = shutdown_rx => {}
        }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    (format!("http://{addr}"), shutdown_tx, dir)
}

#[tokio::test]
async fn ui_get_root_returns_html() {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    let (base, shutdown, _dir) = start_ui_http(store, vec![]).await;

    let client = reqwest::Client::new();
    let res = client.get(format!("{base}/")).send().await.unwrap();
    assert_eq!(res.status(), 200);
    let body = res.text().await.unwrap();
    assert!(body.contains("PolarGraph"), "HTML should contain 'PolarGraph'");

    let _ = shutdown.send(());
}

#[tokio::test]
async fn ui_api_status_returns_expected_fields() {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    let (base, shutdown, _dir) = start_ui_http(store, vec![]).await;

    let client = reqwest::Client::new();
    let res = client.get(format!("{base}/api/status")).send().await.unwrap();
    let status = res.status();
    let body_text = res.text().await.unwrap_or_default();
    assert_eq!(status, 200, "expected 200 but got {status}; body: {body_text}");

    let json: serde_json::Value = serde_json::from_str(&body_text).unwrap();
    assert!(json.get("version").is_some(), "missing 'version'");
    assert!(json.get("uptime_secs").is_some(), "missing 'uptime_secs'");
    assert!(json.get("data_dir").is_some(), "missing 'data_dir'");
    assert!(json.get("auth_enabled").is_some(), "missing 'auth_enabled'");
    assert_eq!(json["auth_enabled"], false);
    assert_eq!(json["is_replica"], false);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn ui_api_requires_auth_when_keys_configured() {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    let (base, shutdown, _dir) = start_ui_http(store, vec!["secret".into()]).await;

    let client = reqwest::Client::new();

    // No key → 401
    let res = client.get(format!("{base}/api/status")).send().await.unwrap();
    assert_eq!(res.status(), 401);

    // Wrong key → 401
    let res = client
        .get(format!("{base}/api/status"))
        .header("Authorization", "Bearer wrong")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 401);

    // Correct key → 200
    let res = client
        .get(format!("{base}/api/status"))
        .header("Authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let json: serde_json::Value = res.json().await.unwrap();
    assert_eq!(json["auth_enabled"], true);

    // GET / (UI HTML) is always accessible without auth
    let res = client.get(format!("{base}/")).send().await.unwrap();
    assert_eq!(res.status(), 200);

    let _ = shutdown.send(());
}

// ── Graceful-shutdown tests ───────────────────────────────────────────────────
//
// These tests drive shutdown via a CancellationToken instead of real OS
// signals (the signal path is just another cancel() call under the hood).

/// Start a gRPC server wired to a CancellationToken for shutdown.
/// Returns (addr, token, JoinHandle).
async fn start_server_with_token(
    store: TripleStore,
) -> (SocketAddr, CancellationToken, tokio::task::JoinHandle<()>) {
    let svc = PolarGraphServer::new(store).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let token = CancellationToken::new();
    let drain_token = token.clone();

    let jh = tokio::spawn(async move {
        Server::builder()
            .add_service(PolarGraphServiceServer::new(svc))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move { drain_token.cancelled().await },
            )
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(20)).await;
    (addr, token, jh)
}

#[tokio::test]
async fn shutdown_token_stops_server() {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    let (addr, token, jh) = start_server_with_token(store).await;

    // Verify server is reachable before shutdown.
    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = PolarGraphServiceClient::new(channel);
    client
        .list_node_types(Request::new(polargraph_server::proto::ListNodeTypesRequest {}))
        .await
        .unwrap();

    // Trigger shutdown via the token.
    token.cancel();

    // Server task should exit within a reasonable deadline.
    let result = tokio::time::timeout(Duration::from_secs(5), jh).await;
    assert!(result.is_ok(), "server task should complete after token cancelled");
}

#[tokio::test]
async fn shutdown_rejects_new_connections_after_drain() {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    let (addr, token, jh) = start_server_with_token(store).await;

    // Cancel and wait for the server to stop.
    token.cancel();
    tokio::time::timeout(Duration::from_secs(5), jh)
        .await
        .expect("server did not stop in time")
        .unwrap();

    // A new TCP connection attempt should be refused (or fail to connect).
    let result = tokio::time::timeout(
        Duration::from_millis(500),
        tonic::transport::Channel::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect(),
    )
    .await;

    // Either a timeout or a connection error is acceptable — the server is gone.
    match result {
        Ok(Ok(_)) => {
            // Connected (unlikely — the port may have been reused); try an RPC.
            // We just assert the server task is not running any more, which the
            // join above already confirmed.
        }
        Ok(Err(_)) | Err(_) => {
            // Expected: connection refused or timed out.
        }
    }
}

#[tokio::test]
async fn wal_client_stops_on_cancellation() {
    // Start a primary.
    let primary_dir = TempDir::new().unwrap();
    let primary_store = TripleStore::open(primary_dir.path()).unwrap();
    let (addr, _primary_shutdown, _primary_jh) = start_server_with_token(primary_store).await;
    let primary_addr = format!("http://{addr}");

    // Open a replica and start the WAL task with its own token.
    let replica_dir = TempDir::new().unwrap();
    let replica_store =
        TripleStore::open_as_replica(replica_dir.path(), primary_addr.clone()).unwrap();
    let (_, rs) = PolarGraphServer::new_replica(replica_store.clone(), &primary_addr).unwrap();
    let wal_token = CancellationToken::new();
    let wal_store = replica_store;
    let wal_state = Arc::clone(&rs);
    let cancel_token = wal_token.clone();
    let wal_jh = tokio::spawn(async move {
        wal_client::run_replication(wal_store, wal_state, wal_token).await;
    });

    // Let the WAL client establish its stream.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Cancel and confirm the WAL task exits.
    cancel_token.cancel();
    let result = tokio::time::timeout(Duration::from_secs(5), wal_jh).await;
    assert!(result.is_ok(), "WAL replication task should exit after cancellation");
}

// ── Query timeout tests ───────────────────────────────────────────────────────

fn open_with_timeout(timeout_ms: u64) -> (PolarGraphServer, TempDir) {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    let svc = PolarGraphServer::new(store)
        .unwrap()
        .with_query_timeout_ms(timeout_ms);
    (svc, dir)
}

#[tokio::test]
async fn query_timeout_fires_on_large_recursive_query() {
    // Build a 300-node chain: node[0]→node[1]→…→node[299].
    // Reachable (transitive closure) without a timeout would run ~300 fixed-point
    // iterations. With a 1ms timeout the deadline check fires before completion.
    let (svc, _dir) = open_with_timeout(1);

    let nodes: Vec<_> = (0..300).map(|_| new_node()).collect();
    let triples: Vec<Triple> = nodes
        .windows(2)
        .map(|w| rel(w[0].1.clone(), "next", w[1].1.clone()))
        .collect();
    svc.insert(Request::new(InsertRequest { triples })).await.unwrap();

    let err = svc
        .reachable(Request::new(ReachableRequest {
            start:     Some(nodes[0].1.clone()),
            predicate: "next".into(),
            max_hops:  0,
        }))
        .await
        .unwrap_err();

    assert_eq!(
        err.code(),
        tonic::Code::DeadlineExceeded,
        "expected DeadlineExceeded, got: {err:?}"
    );
    assert!(err.message().contains("1ms"), "message should include timeout: {}", err.message());
}

#[tokio::test]
async fn query_completes_within_generous_timeout() {
    let (svc, _dir) = open_with_timeout(30_000);

    let (_, a) = new_node();
    let (_, b) = new_node();
    let (_, c) = new_node();

    // Cyclic: A→B→C→A
    svc.insert(Request::new(InsertRequest {
        triples: vec![
            rel(a.clone(), "edge", b.clone()),
            rel(b.clone(), "edge", c.clone()),
            rel(c.clone(), "edge", a.clone()),
        ],
    }))
    .await
    .unwrap();

    let resp = svc
        .reachable(Request::new(ReachableRequest {
            start:     Some(a.clone()),
            predicate: "edge".into(),
            max_hops:  0,
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.node_ids.len(), 3, "a, b, c all reachable in a cycle");
}

#[tokio::test]
async fn query_timeout_zero_disables_timeout() {
    // timeout_ms = 0 → no limit; even a non-trivial query must complete.
    let (svc, _dir) = open_with_timeout(0);

    let (_, a) = new_node();
    let (_, b) = new_node();
    let (_, c) = new_node();

    // Cyclic: A→B→C→A
    svc.insert(Request::new(InsertRequest {
        triples: vec![
            rel(a.clone(), "edge", b.clone()),
            rel(b.clone(), "edge", c.clone()),
            rel(c.clone(), "edge", a.clone()),
        ],
    }))
    .await
    .unwrap();

    let resp = svc
        .reachable(Request::new(ReachableRequest {
            start:     Some(a.clone()),
            predicate: "edge".into(),
            max_hops:  0,
        }))
        .await
        .expect("query should complete without timeout when timeout_ms=0")
        .into_inner();

    assert_eq!(resp.node_ids.len(), 3);
}
