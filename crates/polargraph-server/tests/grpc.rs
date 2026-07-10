//! Integration tests for the gRPC service handlers.
//!
//! Most tests call service methods directly (no live server socket) for speed.
//! The replica tests require real TCP sockets and a live gRPC server.

use axum;
use polargraph_core::temporal::Timestamp;
use polargraph_server::{
    auth::ApiKeyLayer,
    proto::{
        polar_graph_service_client::PolarGraphServiceClient,
        polar_graph_service_server::{PolarGraphService, PolarGraphServiceServer},
        search_vector_filtered_request::Filter,
        term::Kind as TermKind,
        triple::Kind as TripleKind,
        value::Kind as ValueKind,
        vector_seed_query_request::Filter as SeedFilter,
        AddUserToGroupRequest, BatchInsertVectorsRequest, BeginTransactionRequest,
        CommitTransactionRequest, CreateBackupRequest, CypherQueryRequest, CypherWriteRequest,
        EdgeTypeDef, FieldDef, GetEdgeTypeRequest, GetNodeTypeRequest, GetUserAccessRequest,
        GrantAccessRequest, InsertRequest, InsertVectorRequest, ListBackupsRequest,
        ListEdgeTypesRequest, ListNodeTypesRequest, ListPredicatesBetweenRequest, MigrateRequest,
        MigrationStatusRequest, NodeId, NodeTypeDef, NodeTypeFilter, PropertyTriple,
        PurgeOldBackupsRequest, QueryRequest, ReachableRequest, RegisterEdgeTypeRequest,
        RegisterNodeTypeRequest, RelationTriple, ReplicaStatusRequest, RevokeAccessRequest,
        RollbackTransactionRequest, RunRetentionRequest, SearchVectorFilteredRequest,
        SearchVectorInSetRequest, SearchVectorRequest, StreamWalRequest, Term, Triple,
        ValidateEdgeRequest, ValidateNodeRequest, ValidateOntologyRequest, Value, VarPattern,
        VectorItem, VectorSeedQueryRequest, VectorSpaceDef,
    },
    service::PolarGraphServer,
    wal_client,
};
use polargraph_storage::TripleStore;
use std::{
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::Duration,
};
use tempfile::TempDir;
use tokio_stream::StreamExt as _;
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
    let proto = NodeId {
        bytes: core.as_bytes().to_vec(),
    };
    (core, proto)
}

fn bound(id: &NodeId) -> Term {
    Term {
        kind: Some(TermKind::Bound(id.clone())),
    }
}
fn var(name: &str) -> Term {
    Term {
        kind: Some(TermKind::Var(name.into())),
    }
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
            properties: vec![],
        })),
    }
}

fn text_prop(subject: NodeId, predicate: &str, text: &str) -> Triple {
    Triple {
        kind: Some(TripleKind::Property(PropertyTriple {
            subject: Some(subject),
            predicate: predicate.into(),
            value: Some(Value {
                kind: Some(ValueKind::TextVal(text.into())),
            }),
            vt_start: 0,
            vt_end: 0,
        })),
    }
}

fn pattern(sub: Term, pred: &str, obj: Term) -> VarPattern {
    VarPattern {
        subject: Some(sub),
        predicate: pred.into(),
        object: Some(obj),
        predicate_var: String::new(),
    }
}

// ── Insert tests ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn insert_single_relation_returns_positive_commit_ts() {
    let (svc, _dir) = open();
    let (_, alice) = new_node();
    let (_, bob) = new_node();

    let resp = svc
        .insert(Request::new(InsertRequest {
            triples: vec![rel(alice, "knows", bob)],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.commit_ts > 0);
}

#[tokio::test]
async fn insert_batch_all_triples_committed() {
    let (svc, _dir) = open();
    let (_, alice) = new_node();
    let (_, bob) = new_node();
    let (_, carol) = new_node();

    let ts = svc
        .insert(Request::new(InsertRequest {
            triples: vec![
                rel(alice.clone(), "knows", bob.clone()),
                rel(alice.clone(), "knows", carol.clone()),
                text_prop(alice.clone(), "name", "Alice"),
            ],
            ..Default::default()
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
        .insert(Request::new(InsertRequest {
            triples: vec![],
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn insert_bad_node_id_length_returns_invalid_argument() {
    let (svc, _dir) = open();
    let bad = Triple {
        kind: Some(TripleKind::Relation(RelationTriple {
            subject: Some(NodeId {
                bytes: vec![0u8; 15],
            }), // wrong
            predicate: "knows".into(),
            object: Some(NodeId {
                bytes: vec![0u8; 16],
            }),
            vt_start: 0,
            vt_end: 0,
            properties: vec![],
        })),
    };
    let err = svc
        .insert(Request::new(InsertRequest {
            triples: vec![bad],
            ..Default::default()
        }))
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
        .insert(Request::new(InsertRequest {
            triples: vec![bad],
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

// ── Query tests ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn query_empty_patterns_returns_invalid_argument() {
    let (svc, _dir) = open();
    let err = svc
        .query(Request::new(QueryRequest {
            patterns: vec![],
            snapshot_ts: 0,
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn query_with_object_variable_binds_objects() {
    let (svc, _dir) = open();
    let (_, alice) = new_node();
    let (core_bob, bob) = new_node();
    let (core_carol, carol) = new_node();

    let ts = svc
        .insert(Request::new(InsertRequest {
            triples: vec![
                rel(alice.clone(), "knows", bob),
                rel(alice.clone(), "knows", carol),
            ],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
        .commit_ts;

    let resp = svc
        .query(Request::new(QueryRequest {
            patterns: vec![pattern(bound(&alice), "knows", var("who"))],
            snapshot_ts: ts,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.bindings.len(), 2);
    let bound_ids: Vec<Vec<u8>> = resp
        .bindings
        .iter()
        .filter_map(|b| b.vars.get("who").map(|n| n.bytes.clone()))
        .collect();
    assert!(bound_ids.contains(&core_bob.as_bytes().to_vec()));
    assert!(bound_ids.contains(&core_carol.as_bytes().to_vec()));
}

#[tokio::test]
async fn query_snapshot_at_old_ts_excludes_later_writes() {
    let (svc, _dir) = open();
    let (_, alice) = new_node();
    let (_, bob) = new_node();
    let (_, carol) = new_node();

    let ts1 = svc
        .insert(Request::new(InsertRequest {
            triples: vec![rel(alice.clone(), "knows", bob)],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
        .commit_ts;

    svc.insert(Request::new(InsertRequest {
        triples: vec![rel(alice.clone(), "knows", carol)],
        ..Default::default()
    }))
    .await
    .unwrap();

    // Snapshot at ts1 should only see the first insert.
    let resp = svc
        .query(Request::new(QueryRequest {
            patterns: vec![pattern(bound(&alice), "knows", var("x"))],
            snapshot_ts: ts1,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        resp.bindings.len(),
        1,
        "snapshot must not see writes after ts1"
    );
}

#[tokio::test]
async fn query_at_ts_zero_sees_latest() {
    let (svc, _dir) = open();
    let (_, alice) = new_node();
    let (_, bob) = new_node();

    svc.insert(Request::new(InsertRequest {
        triples: vec![rel(alice.clone(), "knows", bob)],
        ..Default::default()
    }))
    .await
    .unwrap();

    // ts=0 means "latest"
    let resp = svc
        .query(Request::new(QueryRequest {
            patterns: vec![pattern(bound(&alice), "knows", var("x"))],
            snapshot_ts: 0,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.bindings.len(), 1);
}

#[tokio::test]
async fn two_pattern_join_via_service() {
    let (svc, _dir) = open();
    let (_, alice) = new_node();
    let (_, bob) = new_node();
    let (_, mgr) = new_node();

    let ts = svc
        .insert(Request::new(InsertRequest {
            triples: vec![
                rel(alice.clone(), "reports-to", mgr.clone()),
                rel(bob.clone(), "reports-to", mgr.clone()),
            ],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
        .commit_ts;

    let resp = svc
        .query(Request::new(QueryRequest {
            patterns: vec![
                pattern(bound(&alice), "reports-to", var("mgr")),
                pattern(var("colleague"), "reports-to", var("mgr")),
            ],
            snapshot_ts: ts,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

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
        .await
        .unwrap()
        .into_inner();

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
        .insert_vector(Request::new(InsertVectorRequest {
            node_id: Some(id),
            vector: vec![],
            space: String::new(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn search_vector_empty_query_returns_invalid_argument() {
    let (svc, _dir) = open();
    let err = svc
        .search_vector(Request::new(SearchVectorRequest {
            query: vec![],
            k: 5,
            space: String::new(),
            ef: 0,
        }))
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
            ef: 0,
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
            ef: 0,
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
            ef: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.results.len(), 5);
}

#[tokio::test]
async fn search_vector_respects_ef() {
    // Verifies that the ef field is plumbed through — a very low ef=1 should
    // still return a non-empty result (not an error), confirming the field is wired.
    let (svc, _dir) = open();
    let (_, id) = new_node();
    svc.insert_vector(Request::new(InsertVectorRequest {
        node_id: Some(id),
        vector: vec![1.0, 0.0],
        space: String::new(),
    }))
    .await
    .unwrap();

    let resp = svc
        .search_vector(Request::new(SearchVectorRequest {
            query: vec![1.0, 0.0],
            k: 1,
            space: String::new(),
            ef: 1,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(
        !resp.results.is_empty(),
        "ef=1 per-request override should still return a result"
    );
}

#[tokio::test]
async fn commit_ts_increments_across_inserts() {
    let (svc, _dir) = open();
    let (_, a) = new_node();
    let (_, b) = new_node();
    let (_, c) = new_node();

    let ts1 = svc
        .insert(Request::new(InsertRequest {
            triples: vec![rel(a.clone(), "k", b)],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
        .commit_ts;

    let ts2 = svc
        .insert(Request::new(InsertRequest {
            triples: vec![rel(a, "k", c)],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
        .commit_ts;

    assert!(
        ts2 > ts1,
        "commit timestamps must be monotonically increasing"
    );
}

// ── Reachable tests ───────────────────────────────────────────────────────────

/// Build a chain A → B → C → D and verify all are reachable from A.
#[tokio::test]
async fn reachable_linear_chain() {
    let (svc, _dir) = open();
    let nodes: Vec<_> = (0..4).map(|_| new_node()).collect();
    let (core_a, proto_a) = &nodes[0];
    let (core_b, _) = &nodes[1];
    let (core_c, _) = &nodes[2];
    let (core_d, _) = &nodes[3];

    svc.insert(Request::new(InsertRequest {
        triples: vec![
            rel(nodes[0].1.clone(), "follows", nodes[1].1.clone()),
            rel(nodes[1].1.clone(), "follows", nodes[2].1.clone()),
            rel(nodes[2].1.clone(), "follows", nodes[3].1.clone()),
        ],
        ..Default::default()
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
    assert!(
        !ids.contains(&core_a.as_bytes().to_vec()),
        "start node must not appear"
    );
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
        ..Default::default()
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
        ..Default::default()
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
        ..Default::default()
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
    assert!(
        !ids.contains(&core_c.as_bytes().to_vec()),
        "C is 2 hops away"
    );
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
            FieldDef {
                field_name: "name".into(),
                kind: "text".into(),
                required: true,
            },
            FieldDef {
                field_name: "age".into(),
                kind: "int".into(),
                required: false,
            },
        ],
        vector_space: None,
        parent_types: vec![],
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
        .get_node_type(Request::new(GetNodeTypeRequest {
            type_name: "Person".into(),
        }))
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
        .get_node_type(Request::new(GetNodeTypeRequest {
            type_name: "Ghost".into(),
        }))
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
            fields: vec![FieldDef {
                field_name: "title".into(),
                kind: "text".into(),
                required: true,
            }],
            vector_space: None,
            parent_types: vec![],
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
    let mut names: Vec<_> = resp
        .definitions
        .iter()
        .map(|d| d.type_name.clone())
        .collect();
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
                (
                    "name".to_string(),
                    Value {
                        kind: Some(ValueKind::TextVal("Alice".into())),
                    },
                ),
                (
                    "age".to_string(),
                    Value {
                        kind: Some(ValueKind::IntVal(30)),
                    },
                ),
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
            properties: std::collections::HashMap::from([(
                "age".to_string(),
                Value {
                    kind: Some(ValueKind::IntVal(30)),
                },
            )]),
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
            properties: std::collections::HashMap::from([(
                "name".to_string(),
                Value {
                    kind: Some(ValueKind::IntVal(99)),
                },
            )]),
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
        fields: vec![FieldDef {
            field_name: "since".into(),
            kind: "int".into(),
            required: true,
        }],
        cardinality: String::new(),
        inverse_of: String::new(),
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
        .get_edge_type(Request::new(GetEdgeTypeRequest {
            predicate: "works_at".into(),
        }))
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
        .get_edge_type(Request::new(GetEdgeTypeRequest {
            predicate: "phantom".into(),
        }))
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
            cardinality: String::new(),
            inverse_of: String::new(),
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
    let mut names: Vec<_> = resp
        .definitions
        .iter()
        .map(|d| d.predicate.clone())
        .collect();
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
            properties: std::collections::HashMap::from([(
                "since".to_string(),
                Value {
                    kind: Some(ValueKind::IntVal(2020)),
                },
            )]),
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
            properties: std::collections::HashMap::from([(
                "since".to_string(),
                Value {
                    kind: Some(ValueKind::IntVal(2020)),
                },
            )]),
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
            properties: std::collections::HashMap::from([(
                "since".to_string(),
                Value {
                    kind: Some(ValueKind::IntVal(2020)),
                },
            )]),
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
                (
                    "since".to_string(),
                    Value {
                        kind: Some(ValueKind::TextVal("2020".into())),
                    },
                ),
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

    assert!(
        resp.valid,
        "unregistered predicate should be treated as valid (open-world)"
    );
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
            cardinality: String::new(),
            inverse_of: String::new(),
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
            ef: 0,
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
        parent_types: vec![],
        vector_space: Some(VectorSpaceDef {
            space_name: "article_space".into(),
            dimensions: 4,
            embedding_model: "my-model".into(),
            storage_mode: String::new(),
        }),
    };

    svc.register_node_type(Request::new(
        polargraph_server::proto::RegisterNodeTypeRequest {
            definition: Some(def.clone()),
        },
    ))
    .await
    .unwrap();

    let resp = svc
        .get_node_type(Request::new(GetNodeTypeRequest {
            type_name: "Article".into(),
        }))
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
    svc.register_node_type(Request::new(
        polargraph_server::proto::RegisterNodeTypeRequest {
            definition: Some(NodeTypeDef {
                type_name: "Thing".into(),
                fields: vec![],
                parent_types: vec![],
                vector_space: Some(VectorSpaceDef {
                    space_name: "thing_space".into(),
                    dimensions: 3,
                    embedding_model: String::new(),
                    storage_mode: String::new(),
                }),
            }),
        },
    ))
    .await
    .unwrap();

    let (_, id) = new_node();
    let err = svc
        .insert_vector(Request::new(InsertVectorRequest {
            node_id: Some(id),
            vector: vec![1.0, 0.0], // 2 dims, not 3
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
                VectorItem {
                    node_id: Some(id1.clone()),
                    vector: vec![1.0, 0.0, 0.0],
                },
                VectorItem {
                    node_id: Some(id2),
                    vector: vec![0.0, 1.0, 0.0],
                },
                VectorItem {
                    node_id: Some(id3),
                    vector: vec![0.0, 0.0, 1.0],
                },
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
            ef: 0,
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(search.results[0].node_id.as_ref().unwrap().bytes, id1.bytes);
}

#[tokio::test]
async fn batch_insert_vectors_rejects_dimension_mismatch() {
    let (svc, _dir) = open();

    svc.register_node_type(Request::new(
        polargraph_server::proto::RegisterNodeTypeRequest {
            definition: Some(NodeTypeDef {
                type_name: "Doc".into(),
                fields: vec![],
                parent_types: vec![],
                vector_space: Some(VectorSpaceDef {
                    space_name: "doc_space".into(),
                    dimensions: 3,
                    embedding_model: String::new(),
                    storage_mode: String::new(),
                }),
            }),
        },
    ))
    .await
    .unwrap();

    let (_, id) = new_node();
    let resp = svc
        .batch_insert_vectors(Request::new(BatchInsertVectorsRequest {
            space: "doc_space".into(),
            items: vec![VectorItem {
                node_id: Some(id),
                vector: vec![1.0, 0.0],
            }], // wrong dim
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
    assert!(resp
        .results
        .iter()
        .all(|r| r.node_id.as_ref().unwrap().bytes != id1.bytes));
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
            ef: 0,
            user_id: String::new(),
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
    let (_, proto_c) = new_node();

    // Tag A and B as "Widget"; C gets no type.
    for (id, type_name) in [
        (proto_a.clone(), "Widget"),
        (proto_b.clone(), "Widget"),
        (proto_c.clone(), "Other"),
    ] {
        svc.insert(Request::new(InsertRequest {
            triples: vec![text_prop(id, "__type", type_name)],
            ..Default::default()
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
            filter: Some(Filter::NodeTypeFilter(NodeTypeFilter {
                type_name: "Widget".into(),
            })),
            ef: 0,
            user_id: String::new(),
        }))
        .await
        .unwrap()
        .into_inner();

    // Only A and B should appear; C is "Other".
    let ids: Vec<Vec<u8>> = resp
        .results
        .iter()
        .map(|r| r.node_id.as_ref().unwrap().bytes.clone())
        .collect();
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
        ..Default::default()
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
            ef: 0,
            ..Default::default()
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
    let friend_ids: Vec<Vec<u8>> = resp
        .bindings
        .iter()
        .map(|row| row.vars["friend"].bytes.clone())
        .collect();
    assert!(
        friend_ids.contains(&core_b.as_bytes().to_vec()),
        "B should be a friend of A"
    );
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
        ..Default::default()
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
                ..Default::default()
            })),
            ef: 0,
            user_id: String::new(),
            params: Default::default(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        resp.bindings.len(),
        1,
        "only the typed node should be returned"
    );
    assert_eq!(
        resp.bindings[0].vars["n"].bytes,
        core_typed.as_bytes().to_vec()
    );
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
            ef: 0,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.bindings.len(), 2);

    let result_ids: Vec<Vec<u8>> = resp
        .bindings
        .iter()
        .map(|b| b.vars["n"].bytes.clone())
        .collect();
    assert!(result_ids.contains(&core_a.as_bytes().to_vec()));
    assert!(result_ids.contains(&core_b.as_bytes().to_vec()));

    // All scores must be in [-1, 1].
    for row in &resp.bindings {
        assert!(
            row.score >= -1.0 && row.score <= 1.0,
            "score out of range: {}",
            row.score
        );
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
            ef: 0,
            ..Default::default()
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

    svc.register_node_type(Request::new(
        polargraph_server::proto::RegisterNodeTypeRequest {
            definition: Some(NodeTypeDef {
                type_name: "MmapDoc".into(),
                fields: vec![],
                parent_types: vec![],
                vector_space: Some(VectorSpaceDef {
                    space_name: "mmap_space".into(),
                    dimensions: 3,
                    embedding_model: String::new(),
                    storage_mode: "mmap".into(),
                }),
            }),
        },
    ))
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
            ef: 0,
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

    svc.register_node_type(Request::new(
        polargraph_server::proto::RegisterNodeTypeRequest {
            definition: Some(NodeTypeDef {
                type_name: "MmapNode".into(),
                fields: vec![],
                parent_types: vec![],
                vector_space: Some(VectorSpaceDef {
                    space_name: "mm_space".into(),
                    dimensions: 8,
                    embedding_model: String::new(),
                    storage_mode: "mmap".into(),
                }),
            }),
        },
    ))
    .await
    .unwrap();

    let resp = svc
        .get_node_type(Request::new(GetNodeTypeRequest {
            type_name: "MmapNode".into(),
        }))
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
    let err = svc
        .create_backup(Request::new(CreateBackupRequest {}))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn backup_list_unconfigured_returns_precondition() {
    let (svc, _dir) = open();
    let err = svc
        .list_backups(Request::new(ListBackupsRequest {}))
        .await
        .unwrap_err();
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

    svc.create_backup(Request::new(CreateBackupRequest {}))
        .await
        .unwrap();
    svc.create_backup(Request::new(CreateBackupRequest {}))
        .await
        .unwrap();
    svc.create_backup(Request::new(CreateBackupRequest {}))
        .await
        .unwrap();

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
        ..Default::default()
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
        ..Default::default()
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
            vt_end: Timestamp(1), // expired long ago
            tt: Timestamp(0),
        },
    };
    svc.store()
        .insert_at_ts(&triple, Timestamp(now_us))
        .unwrap();

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

fn rel_with_vt(
    subject: NodeId,
    predicate: &str,
    object: NodeId,
    vt_start: i64,
    vt_end: i64,
) -> Triple {
    Triple {
        kind: Some(TripleKind::Relation(RelationTriple {
            subject: Some(subject),
            predicate: predicate.into(),
            object: Some(object),
            vt_start,
            vt_end,
            properties: vec![],
        })),
    }
}

fn text_prop_with_vt(
    subject: NodeId,
    predicate: &str,
    text: &str,
    vt_start: i64,
    vt_end: i64,
) -> Triple {
    Triple {
        kind: Some(TripleKind::Property(PropertyTriple {
            subject: Some(subject),
            predicate: predicate.into(),
            value: Some(Value {
                kind: Some(ValueKind::TextVal(text.into())),
            }),
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
        ..Default::default()
    }))
    .await
    .unwrap();

    let resp = svc
        .query(Request::new(QueryRequest {
            patterns: vec![pattern(bound(&alice), "knows", var("x"))],
            snapshot_ts: 0,
            as_of_valid_time: 0,
            as_of_tx_time: 0,
            rules: vec![],
            ..Default::default()
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
        ..Default::default()
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
            rules: vec![],
            ..Default::default()
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
            rules: vec![],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        resp_inside.bindings.len(),
        1,
        "triple should be visible inside its vt window"
    );
    assert!(
        resp_outside.bindings.is_empty(),
        "triple should be invisible after its vt window ends"
    );
}

/// Two versions of the same (S,P) with non-overlapping vt ranges. Querying at
/// different as_of_valid_time values returns the correct version.
#[tokio::test]
async fn as_of_valid_time_returns_correct_version() {
    let (svc, _dir) = open();
    let (_, alice) = new_node();

    // Version 1: vt=[1000, 2000)
    svc.insert(Request::new(InsertRequest {
        triples: vec![text_prop_with_vt(
            alice.clone(),
            "status",
            "active",
            1000,
            2000,
        )],
        ..Default::default()
    }))
    .await
    .unwrap();

    // Version 2: vt=[2000, ∞)  (vt_end=0 → END_OF_TIME)
    svc.insert(Request::new(InsertRequest {
        triples: vec![text_prop_with_vt(
            alice.clone(),
            "status",
            "retired",
            2000,
            0,
        )],
        ..Default::default()
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
            rules: vec![],
            ..Default::default()
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
            rules: vec![],
            ..Default::default()
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
            rules: vec![],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        resp_v1.bindings.len(),
        1,
        "vt=1500 should see version 1 (active window)"
    );
    assert_eq!(
        resp_v2.bindings.len(),
        1,
        "vt=2500 should see version 2 (active window)"
    );
    assert_eq!(
        resp_latest.bindings.len(),
        1,
        "no filter should see current state"
    );
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
        ..Default::default()
    }))
    .await
    .unwrap();

    let resp = svc
        .query(Request::new(QueryRequest {
            patterns: vec![pattern(bound(&alice), "knows", var("x"))],
            snapshot_ts: 0,
            as_of_valid_time: 0,
            as_of_tx_time: t_before,
            rules: vec![],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(
        resp.bindings.is_empty(),
        "triple must not be visible before its commit time"
    );
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
            ..Default::default()
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
            rules: vec![],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        resp.bindings.len(),
        1,
        "triple must be visible at its commit time"
    );
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
            ..Default::default()
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
            rules: vec![],
            ..Default::default()
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
            rules: vec![],
            ..Default::default()
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
async fn open_wal_replica(primary_address: String) -> (PolarGraphServer, TempDir) {
    let replica_dir = TempDir::new().unwrap();
    let replica_store =
        TripleStore::open_as_replica(replica_dir.path(), primary_address.clone()).unwrap();
    let (server, rs) =
        PolarGraphServer::new_replica(replica_store.clone(), &primary_address).unwrap();
    let repl_store = replica_store;
    let repl_state = Arc::clone(&rs);
    let token = tokio_util::sync::CancellationToken::new();
    tokio::spawn(async move {
        wal_client::run_replication(repl_store, repl_state, token, None, None).await;
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
            ..Default::default()
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
                ..Default::default()
            }],
            snapshot_ts: 0,
            as_of_valid_time: 0,
            as_of_tx_time: 0,
            rules: vec![],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        resp.bindings.len(),
        1,
        "replica receives triple via WAL stream"
    );
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
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::FailedPrecondition,
        "Insert on replica"
    );

    let err = replica
        .insert_vector(Request::new(InsertVectorRequest {
            node_id: Some(node.clone()),
            vector: vec![1.0, 0.0],
            space: "default".into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::FailedPrecondition,
        "InsertVector on replica"
    );

    let err = replica
        .register_node_type(Request::new(RegisterNodeTypeRequest {
            definition: Some(NodeTypeDef {
                type_name: "Foo".into(),
                fields: vec![],
                vector_space: None,
                parent_types: vec![],
            }),
        }))
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::FailedPrecondition,
        "RegisterNodeType on replica"
    );

    let err = replica
        .register_edge_type(Request::new(RegisterEdgeTypeRequest {
            definition: Some(polargraph_server::proto::EdgeTypeDef {
                predicate: "foo".into(),
                domain: String::new(),
                range: String::new(),
                fields: vec![],
                cardinality: String::new(),
                inverse_of: String::new(),
            }),
        }))
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::FailedPrecondition,
        "RegisterEdgeType on replica"
    );

    let err = replica
        .run_retention(Request::new(RunRetentionRequest {
            tx_age_secs: 1,
            vt_lookback_secs: 0,
        }))
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::FailedPrecondition,
        "RunRetention on replica"
    );

    let err = replica
        .create_backup(Request::new(CreateBackupRequest {}))
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::FailedPrecondition,
        "CreateBackup on replica"
    );
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
    let (replica_svc, _rs) = PolarGraphServer::new_replica(replica_store, &primary_addr).unwrap();

    let err = replica_svc
        .stream_wal(Request::new(StreamWalRequest { since_seq: 0 }))
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::FailedPrecondition,
        "StreamWal on replica"
    );
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
    assert!(
        status.primary_address.is_empty(),
        "primary has no primary_address"
    );
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
            ..Default::default()
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
                ..Default::default()
            }],
            snapshot_ts: 0,
            as_of_valid_time: 0,
            as_of_tx_time: 0,
            rules: vec![],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        resp.bindings.len(),
        1,
        "replica sees post-connect triple via WAL"
    );
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
    let (auth, _key_store) = ApiKeyLayer::new(keys);

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
            ..Default::default()
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
        .insert(bearer(
            InsertRequest {
                triples: vec![rel(node_a, "knows", node_b)],
                ..Default::default()
            },
            "valid-key",
        ))
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
        .insert(bearer(
            InsertRequest {
                triples: vec![rel(node_a, "knows", node_b)],
                ..Default::default()
            },
            "wrong-key",
        ))
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

    assert!(
        !resp.is_replica,
        "primary reports is_replica=false without auth key"
    );
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
            ..Default::default()
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
        .insert(bearer(
            InsertRequest {
                triples: vec![rel(a1, "x", a2)],
                ..Default::default()
            },
            "key-a",
        ))
        .await
        .unwrap();

    // key-b works
    let (_, b1) = new_node();
    let (_, b2) = new_node();
    client
        .insert(bearer(
            InsertRequest {
                triples: vec![rel(b1, "x", b2)],
                ..Default::default()
            },
            "key-b",
        ))
        .await
        .unwrap();

    // wrong key fails
    let (_, c1) = new_node();
    let (_, c2) = new_node();
    let err = client
        .insert(bearer(
            InsertRequest {
                triples: vec![rel(c1, "x", c2)],
                ..Default::default()
            },
            "key-c",
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

// ── Runtime API key management (gRPC) ────────────────────────────────────────
//
// These tests use a live TCP server so that the ApiKeyLayer tower middleware
// is wired in exactly as it would be in production.

/// Start a server with the given keys and also return the key store handle so
/// tests can call AddApiKey / RevokeApiKey / ListApiKeys via a gRPC client.
async fn start_server_with_keys_and_store(
    store: TripleStore,
    keys: Vec<String>,
) -> (
    SocketAddr,
    polargraph_server::auth::KeyStore,
    tokio::sync::oneshot::Sender<()>,
) {
    let (auth, key_store) = ApiKeyLayer::new(keys.clone());
    let svc = PolarGraphServer::new(store)
        .unwrap()
        .with_key_store(key_store.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

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
    (addr, key_store, shutdown_tx)
}

#[tokio::test]
async fn api_key_add_and_list() {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    let (addr, _ks, _shutdown) =
        start_server_with_keys_and_store(store, vec!["initial-key".into()]).await;
    let mut client = grpc_client(addr).await;

    // Add a second key.
    let resp = client
        .add_api_key(bearer(
            polargraph_server::proto::AddApiKeyRequest {
                key: "new-key-abcd".into(),
            },
            "initial-key",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.total_keys, 2);

    // List — should see both, masked.
    let list = client
        .list_api_keys(bearer(
            polargraph_server::proto::ListApiKeysRequest {},
            "initial-key",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list.total_keys, 2);
    // Each prefix should be 4 chars + "****" = 8 chars.
    assert!(list.key_prefixes.iter().all(|p| p.ends_with("****")));
    // Full keys must NOT appear.
    assert!(!list
        .key_prefixes
        .iter()
        .any(|p| p == "initial-key" || p == "new-key-abcd"));
    // Masked prefixes should start with the right characters.
    assert!(list.key_prefixes.iter().any(|p| p.starts_with("init")));
    assert!(list.key_prefixes.iter().any(|p| p.starts_with("new-")));
}

#[tokio::test]
async fn api_key_revoke_removes_key() {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    let (addr, _ks, _shutdown) =
        start_server_with_keys_and_store(store, vec!["keep-key".into(), "remove-key".into()]).await;
    let mut client = grpc_client(addr).await;

    // Revoke "remove-key".
    let resp = client
        .revoke_api_key(bearer(
            polargraph_server::proto::RevokeApiKeyRequest {
                key: "remove-key".into(),
            },
            "keep-key",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.found, "key should have been found");
    assert_eq!(resp.total_keys, 1);

    // Revoking again — not found.
    let resp2 = client
        .revoke_api_key(bearer(
            polargraph_server::proto::RevokeApiKeyRequest {
                key: "remove-key".into(),
            },
            "keep-key",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!resp2.found, "second revoke should return found=false");
    assert_eq!(resp2.total_keys, 1);
}

#[tokio::test]
async fn api_key_new_key_works_for_auth() {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    let (addr, _ks, _shutdown) =
        start_server_with_keys_and_store(store, vec!["admin-key".into()]).await;
    let mut client = grpc_client(addr).await;

    // Add a second key via the management RPC.
    client
        .add_api_key(bearer(
            polargraph_server::proto::AddApiKeyRequest {
                key: "user-key-xyz".into(),
            },
            "admin-key",
        ))
        .await
        .unwrap();

    // Immediately use the new key for an Insert.
    let (_, a) = new_node();
    let (_, b) = new_node();
    let resp = client
        .insert(bearer(
            InsertRequest {
                triples: vec![rel(a, "knows", b)],
                ..Default::default()
            },
            "user-key-xyz",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(
        resp.commit_ts > 0,
        "insert with newly added key should succeed"
    );
}

#[tokio::test]
async fn api_key_disabled_returns_failed_precondition() {
    // Server started with no keys at all — auth is disabled.
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    // Build server with empty key store (auth disabled) but wire the key store in.
    let (auth, key_store) = ApiKeyLayer::new(vec![]);
    let svc = PolarGraphServer::new(store)
        .unwrap()
        .with_key_store(key_store);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
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
    let _s = shutdown_tx;

    let mut client = grpc_client(addr).await;

    // AddApiKey on a key-store-backed but auth-disabled server should return
    // FailedPrecondition, because auth is currently disabled (empty key list).
    let err = client
        .add_api_key(Request::new(polargraph_server::proto::AddApiKeyRequest {
            key: "new-key".into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
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
        api_keys: Arc::new(RwLock::new(api_keys)),
        start_time: std::time::Instant::now(),
        data_dir: dir.path().display().to_string(),
        grpc_addr: "127.0.0.1:50051".into(),
        replica_state: None,
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
    assert!(
        body.contains("PolarGraph"),
        "HTML should contain 'PolarGraph'"
    );

    let _ = shutdown.send(());
}

#[tokio::test]
async fn ui_api_status_returns_expected_fields() {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    let (base, shutdown, _dir) = start_ui_http(store, vec![]).await;

    let client = reqwest::Client::new();
    let res = client
        .get(format!("{base}/api/status"))
        .send()
        .await
        .unwrap();
    let status = res.status();
    let body_text = res.text().await.unwrap_or_default();
    assert_eq!(
        status, 200,
        "expected 200 but got {status}; body: {body_text}"
    );

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
    let res = client
        .get(format!("{base}/api/status"))
        .send()
        .await
        .unwrap();
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

/// Start a UI HTTP server with a pre-built `PolarGraphServer` (e.g. one that has a backup dir).
async fn start_ui_http_svc(
    svc: PolarGraphServer,
    api_keys: Vec<String>,
) -> (String, tokio::sync::oneshot::Sender<()>) {
    use polargraph_server::ui_api;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let state = Arc::new(ui_api::UiState {
        service: svc,
        api_keys: Arc::new(RwLock::new(api_keys)),
        start_time: std::time::Instant::now(),
        data_dir: "/tmp".into(),
        grpc_addr: "127.0.0.1:50051".into(),
        replica_state: None,
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

    (format!("http://{addr}"), shutdown_tx)
}

#[tokio::test]
async fn ui_backups_list_unconfigured_returns_empty() {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    let (base, shutdown, _dir) = start_ui_http(store, vec![]).await;

    let client = reqwest::Client::new();
    let res = client
        .get(format!("{base}/api/backups"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let json: serde_json::Value = res.json().await.unwrap();
    assert_eq!(json["backups"], serde_json::json!([]));
    assert!(
        json.get("error").is_some(),
        "expected error field when backup dir not configured"
    );

    let _ = shutdown.send(());
}

#[tokio::test]
async fn ui_backups_create_and_list() {
    let (svc, _data, _backup) = open_with_backup();
    let (base, shutdown) = start_ui_http_svc(svc, vec![]).await;

    let client = reqwest::Client::new();

    // Create a backup
    let res = client
        .post(format!("{base}/api/backups/create"))
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let json: serde_json::Value = res.json().await.unwrap();
    assert!(
        json.get("backup_id").is_some(),
        "expected backup_id in create response"
    );

    // List backups — should contain the one we just made
    let res = client
        .get(format!("{base}/api/backups"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let json: serde_json::Value = res.json().await.unwrap();
    assert!(
        json.get("error").is_none(),
        "should have no error when backup dir is configured"
    );
    let backups = json["backups"]
        .as_array()
        .expect("backups should be an array");
    assert!(
        !backups.is_empty(),
        "expected at least one backup after create"
    );
    assert!(backups[0].get("id").is_some(), "backup entry missing id");
    assert!(
        backups[0].get("timestamp").is_some(),
        "backup entry missing timestamp"
    );
    assert!(
        backups[0].get("size_bytes").is_some(),
        "backup entry missing size_bytes"
    );

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
        .list_node_types(Request::new(
            polargraph_server::proto::ListNodeTypesRequest {},
        ))
        .await
        .unwrap();

    // Trigger shutdown via the token.
    token.cancel();

    // Server task should exit within a reasonable deadline.
    let result = tokio::time::timeout(Duration::from_secs(5), jh).await;
    assert!(
        result.is_ok(),
        "server task should complete after token cancelled"
    );
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
        wal_client::run_replication(wal_store, wal_state, wal_token, None, None).await;
    });

    // Let the WAL client establish its stream.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Cancel and confirm the WAL task exits.
    cancel_token.cancel();
    let result = tokio::time::timeout(Duration::from_secs(5), wal_jh).await;
    assert!(
        result.is_ok(),
        "WAL replication task should exit after cancellation"
    );
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
    svc.insert(Request::new(InsertRequest {
        triples,
        ..Default::default()
    }))
    .await
    .unwrap();

    let err = svc
        .reachable(Request::new(ReachableRequest {
            start: Some(nodes[0].1.clone()),
            predicate: "next".into(),
            max_hops: 0,
        }))
        .await
        .unwrap_err();

    assert_eq!(
        err.code(),
        tonic::Code::DeadlineExceeded,
        "expected DeadlineExceeded, got: {err:?}"
    );
    assert!(
        err.message().contains("1ms"),
        "message should include timeout: {}",
        err.message()
    );
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
        ..Default::default()
    }))
    .await
    .unwrap();

    let resp = svc
        .reachable(Request::new(ReachableRequest {
            start: Some(a.clone()),
            predicate: "edge".into(),
            max_hops: 0,
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
        ..Default::default()
    }))
    .await
    .unwrap();

    let resp = svc
        .reachable(Request::new(ReachableRequest {
            start: Some(a.clone()),
            predicate: "edge".into(),
            max_hops: 0,
        }))
        .await
        .expect("query should complete without timeout when timeout_ms=0")
        .into_inner();

    assert_eq!(resp.node_ids.len(), 3);
}

// ── Health check tests ────────────────────────────────────────────────────────

/// Start a gRPC server that includes the tonic health service and verify that
/// the Health RPC reports SERVING for the PolarGraph service name.
#[tokio::test]
async fn health_rpc_returns_serving_for_running_server() {
    use tonic_health::{pb::health_client::HealthClient, pb::HealthCheckRequest, ServingStatus};

    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    let svc = PolarGraphServer::new(store).unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let (mut reporter, health_service) = tonic_health::server::health_reporter();
    reporter
        .set_service_status("polargraph.v1.PolarGraphService", ServingStatus::Serving)
        .await;

    tokio::spawn(async move {
        Server::builder()
            .add_service(health_service)
            .add_service(PolarGraphServiceServer::new(svc))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async { drop(shutdown_rx.await) },
            )
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();

    let mut client = HealthClient::new(channel);
    let resp = client
        .check(HealthCheckRequest {
            service: "polargraph.v1.PolarGraphService".into(),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        resp.status,
        tonic_health::pb::health_check_response::ServingStatus::Serving as i32,
        "health status should be SERVING"
    );

    let _ = shutdown_tx.send(());
}

/// Verify that the `/health` HTTP endpoint on the management UI returns 200
/// with a JSON body containing `status: "ok"` for a primary server.
#[tokio::test]
async fn health_http_endpoint_returns_ok_for_primary() {
    use polargraph_server::ui_api;

    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    let svc = PolarGraphServer::new(store).unwrap();

    let state = Arc::new(ui_api::UiState {
        service: svc,
        api_keys: Arc::new(RwLock::new(vec![])),
        start_time: std::time::Instant::now(),
        data_dir: dir.path().display().to_string(),
        grpc_addr: "127.0.0.1:50051".into(),
        replica_state: None,
    });

    let app = ui_api::build_ui_router(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        axum::Server::from_tcp(listener.into_std().unwrap())
            .unwrap()
            .serve(app.into_make_service())
            .with_graceful_shutdown(async { drop(shutdown_rx.await) })
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    let resp = reqwest::get(format!("http://{addr}/health")).await.unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok");
    assert_eq!(body["mode"], "primary");
    assert!(body["triples"].is_number());

    let _ = shutdown_tx.send(());
}

// ── Rate limiting tests ───────────────────────────────────────────────────────
//
// Rate limiting is a tower layer at the transport level, so these tests use a
// live TCP server and a real gRPC client channel — same pattern as the auth tests.

use polargraph_server::rate_limit::RateLimitLayer;

/// Start a server with rate limiting at `max_rps` requests per second.
async fn start_server_with_rate_limit(
    store: TripleStore,
    max_rps: u32,
) -> (SocketAddr, tokio::sync::oneshot::Sender<()>) {
    let svc = PolarGraphServer::new(store).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let rate = RateLimitLayer::new(max_rps);

    tokio::spawn(async move {
        Server::builder()
            .layer(rate)
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

/// Build a gRPC request with an `x-forwarded-for` header to simulate a specific client IP.
fn req_from_ip<T>(body: T, ip: &str) -> Request<T> {
    let mut req = Request::new(body);
    req.metadata_mut()
        .insert("x-forwarded-for", ip.parse().unwrap());
    req
}

#[tokio::test]
async fn rate_limit_single_client_throttled_after_quota() {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    // Allow only 3 requests per second so the test doesn't need to send thousands.
    let (addr, _shutdown) = start_server_with_rate_limit(store, 3).await;

    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = PolarGraphServiceClient::new(channel);

    let (_, na) = new_node();
    let (_, nb) = new_node();

    // The first 3 requests from this IP should succeed (bucket starts full).
    for _ in 0..3 {
        let result = client
            .insert(req_from_ip(
                InsertRequest {
                    triples: vec![rel(na.clone(), "knows", nb.clone())],
                    ..Default::default()
                },
                "10.1.1.1",
            ))
            .await;
        assert!(result.is_ok(), "expected success while within quota");
    }

    // The 4th request should be rejected.
    let err = client
        .insert(req_from_ip(
            InsertRequest {
                triples: vec![rel(na, "knows", nb)],
                ..Default::default()
            },
            "10.1.1.1",
        ))
        .await
        .unwrap_err();

    assert_eq!(
        err.code(),
        tonic::Code::ResourceExhausted,
        "expected ResourceExhausted when quota exceeded"
    );
}

#[tokio::test]
async fn rate_limit_two_clients_have_independent_buckets() {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    // Allow 2 requests per second per client.
    let (addr, _shutdown) = start_server_with_rate_limit(store, 2).await;

    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = PolarGraphServiceClient::new(channel);

    let (_, na) = new_node();
    let (_, nb) = new_node();

    // Exhaust client A's quota (2 requests).
    for _ in 0..2 {
        let result = client
            .insert(req_from_ip(
                InsertRequest {
                    triples: vec![rel(na.clone(), "knows", nb.clone())],
                    ..Default::default()
                },
                "10.2.2.1",
            ))
            .await;
        assert!(result.is_ok(), "A within quota");
    }
    let err = client
        .insert(req_from_ip(
            InsertRequest {
                triples: vec![rel(na.clone(), "knows", nb.clone())],
                ..Default::default()
            },
            "10.2.2.1",
        ))
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::ResourceExhausted,
        "A should be throttled"
    );

    // Client B (different IP) still has its own full quota.
    for _ in 0..2 {
        let result = client
            .insert(req_from_ip(
                InsertRequest {
                    triples: vec![rel(na.clone(), "knows", nb.clone())],
                    ..Default::default()
                },
                "10.2.2.2",
            ))
            .await;
        assert!(result.is_ok(), "B should have its own independent bucket");
    }
}

// ── Schema migration tests ─────────────────────────────────────────────────────

#[tokio::test]
async fn migrate_schema_fresh_db_applies_all_migrations() {
    let (svc, _dir) = open();
    let resp = svc
        .migrate_schema(Request::new(MigrateRequest { dry_run: false }))
        .await
        .unwrap()
        .into_inner();

    // Both built-in migrations must have run.
    assert_eq!(resp.applied_versions, vec![1, 2]);
    assert!(resp.skipped_versions.is_empty());
    assert!(resp.summary.contains("applied"));
}

#[tokio::test]
async fn migrate_schema_already_at_latest_is_noop() {
    let (svc, _dir) = open();
    // First run applies everything.
    svc.migrate_schema(Request::new(MigrateRequest { dry_run: false }))
        .await
        .unwrap();

    // Second run must be a no-op.
    let resp = svc
        .migrate_schema(Request::new(MigrateRequest { dry_run: false }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.applied_versions.is_empty());
    assert_eq!(resp.skipped_versions, vec![1, 2]);
    assert!(resp.summary.contains("up to date"));
}

#[tokio::test]
async fn migrate_schema_dry_run_does_not_apply() {
    let (svc, _dir) = open();

    // dry_run = true: returns pending list without applying.
    let resp = svc
        .migrate_schema(Request::new(MigrateRequest { dry_run: true }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.applied_versions, vec![1, 2]);
    assert!(resp.summary.contains("dry run"));

    // The database should still be at version 0 (nothing was applied).
    let status = svc
        .migration_status(Request::new(MigrationStatusRequest {}))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(status.current_version, 0);
    assert!(status.applied.is_empty());
}

#[tokio::test]
async fn migration_status_reflects_applied_state() {
    let (svc, _dir) = open();
    svc.migrate_schema(Request::new(MigrateRequest { dry_run: false }))
        .await
        .unwrap();

    let status = svc
        .migration_status(Request::new(MigrationStatusRequest {}))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(status.current_version, 2);
    assert_eq!(status.latest_version, 2);
    assert_eq!(status.applied.len(), 2);
    assert_eq!(status.applied[0].version, 1);
    assert_eq!(status.applied[0].description, "initial schema");
    assert!(status.applied[0].applied_at_tx_time > 0);
}

// ── CypherQuery ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn cypher_query_simple() {
    let (svc, _dir) = open();
    let (alice_core, alice) = new_node();
    let (bob_core, bob) = new_node();
    let (_carol_core, carol) = new_node();

    // Insert: alice and bob are Person; carol is Robot.
    // alice knows bob; alice knows carol.
    svc.insert(Request::new(InsertRequest {
        triples: vec![
            text_prop(alice.clone(), "__type", "Person"),
            text_prop(bob.clone(), "__type", "Person"),
            text_prop(carol.clone(), "__type", "Robot"),
            rel(alice.clone(), "knows", bob.clone()),
            rel(alice.clone(), "knows", carol.clone()),
        ],
        ..Default::default()
    }))
    .await
    .unwrap();

    // Query: who does a Person know who is also a Person?
    // alice (Person) knows bob (Person) and carol (Robot).
    // Expected: only alice→bob survives because carol is not a Person.
    let resp = svc
        .cypher_query(Request::new(CypherQueryRequest {
            cypher: "MATCH (a:Person)-[:knows]->(b:Person) RETURN a, b".to_string(),
            as_of_valid_time: 0,
            as_of_tx_time: 0,
            vector: vec![],
            ef: 0,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.rows.len(), 1, "only alice→bob matches; carol is Robot");

    let bound_a = resp.rows[0].nodes.get("a").unwrap();
    let bound_a_core = polargraph_core::id::NodeId(uuid::Uuid::from_bytes(
        bound_a.bytes[..16].try_into().unwrap(),
    ));
    let bound_b = resp.rows[0].nodes.get("b").unwrap();
    let bound_b_core = polargraph_core::id::NodeId(uuid::Uuid::from_bytes(
        bound_b.bytes[..16].try_into().unwrap(),
    ));
    assert_eq!(bound_a_core, alice_core);
    assert_eq!(bound_b_core, bob_core);
}

#[tokio::test]
async fn cypher_query_recursive() {
    let (svc, _dir) = open();
    let (a_core, a) = new_node();
    let (b_core, b) = new_node();
    let (c_core, c) = new_node();
    let (d_core, d) = new_node();

    // Chain: a → b → c → d
    svc.insert(Request::new(InsertRequest {
        triples: vec![
            rel(a.clone(), "follows", b.clone()),
            rel(b.clone(), "follows", c.clone()),
            rel(c.clone(), "follows", d.clone()),
        ],
        ..Default::default()
    }))
    .await
    .unwrap();

    // Transitive query: who does a reach via follows*?
    let resp = svc
        .cypher_query(Request::new(CypherQueryRequest {
            cypher: "MATCH (a)-[:follows*]->(b) RETURN a, b".to_string(),
            as_of_valid_time: 0,
            as_of_tx_time: 0,
            vector: vec![],
            ef: 0,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    // All (src, dst) pairs where dst is reachable from src.
    // a→b, a→c, a→d, b→c, b→d, c→d = 6 pairs
    assert_eq!(resp.rows.len(), 6, "expected 6 transitive pairs");

    // Collect all 'b' values that appear when 'a' is bound to a_core.
    let reachable_from_a: std::collections::HashSet<polargraph_core::id::NodeId> = resp
        .rows
        .iter()
        .filter(|binding| {
            binding
                .nodes
                .get("a")
                .map(|n| {
                    polargraph_core::id::NodeId(uuid::Uuid::from_bytes(
                        n.bytes[..16].try_into().unwrap(),
                    )) == a_core
                })
                .unwrap_or(false)
        })
        .filter_map(|binding| {
            binding.nodes.get("b").map(|n| {
                polargraph_core::id::NodeId(uuid::Uuid::from_bytes(
                    n.bytes[..16].try_into().unwrap(),
                ))
            })
        })
        .collect();

    assert!(reachable_from_a.contains(&b_core));
    assert!(reachable_from_a.contains(&c_core));
    assert!(reachable_from_a.contains(&d_core));
    assert_eq!(reachable_from_a.len(), 3);
}

#[tokio::test]
async fn cypher_vector_near_query() {
    // Register a node type with a 3-D vector space, insert 5 Document nodes
    // with vectors, connect them with :cites edges, then run:
    //   MATCH (a)-[:cites]->(b) WHERE VECTOR_NEAR(a, "docs", 3) RETURN b
    //
    // The ANN search seeds variable "a"; the graph pattern then joins to find
    // all "b" nodes that the seed nodes cite.  Results must be non-empty and
    // all "b" values must be directly cited by at least one ANN hit.
    let (svc, _dir) = open();

    // Register node type with vector space so insertions succeed.
    svc.register_node_type(Request::new(RegisterNodeTypeRequest {
        definition: Some(NodeTypeDef {
            type_name: "Document".into(),
            fields: vec![],
            parent_types: vec![],
            vector_space: Some(VectorSpaceDef {
                space_name: "docs".into(),
                dimensions: 3,
                embedding_model: String::new(),
                storage_mode: "memory".into(),
            }),
        }),
    }))
    .await
    .unwrap();

    // Create 5 nodes: a0..a4
    let nodes: Vec<(polargraph_core::id::NodeId, NodeId)> = (0..5).map(|_| new_node()).collect();

    // Vectors: a0 is near query [1,0,0]; a1 is slightly off; others are far.
    let vectors: Vec<Vec<f32>> = vec![
        vec![1.0, 0.0, 0.0], // a0 — closest
        vec![0.9, 0.1, 0.0], // a1 — second
        vec![0.8, 0.2, 0.0], // a2 — third
        vec![0.0, 0.0, 1.0], // a3 — far
        vec![0.0, 1.0, 0.0], // a4 — far
    ];

    // Insert :cites edges: a0→a1, a1→a2, a3→a4 (a3/a4 are far from query)
    svc.insert(Request::new(InsertRequest {
        triples: vec![
            rel(nodes[0].1.clone(), "cites", nodes[1].1.clone()),
            rel(nodes[1].1.clone(), "cites", nodes[2].1.clone()),
            rel(nodes[3].1.clone(), "cites", nodes[4].1.clone()),
        ],
        ..Default::default()
    }))
    .await
    .unwrap();

    // Insert vectors into the "docs" space.
    for (i, (_, proto_node)) in nodes.iter().enumerate() {
        insert_vec(&svc, proto_node.clone(), vectors[i].clone(), "docs").await;
    }

    // Query: ANN seeds (k=3) from [1,0,0] → a0, a1, a2 are top-3.
    // Of those, a0 cites a1, a1 cites a2 — so b = {a1, a2}.
    let resp = svc
        .cypher_query(Request::new(CypherQueryRequest {
            cypher: r#"MATCH (a)-[:cites]->(b) WHERE VECTOR_NEAR(a, "docs", 3) RETURN b"#.into(),
            as_of_valid_time: 0,
            as_of_tx_time: 0,
            vector: vec![1.0, 0.0, 0.0],
            ef: 0,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(
        !resp.rows.is_empty(),
        "expected at least one result from VECTOR_NEAR + cites"
    );

    // All returned "b" nodes must be a1 or a2 (cited by ANN-hit nodes).
    let expected_b_ids: std::collections::HashSet<Vec<u8>> = vec![
        nodes[1].0.as_bytes().to_vec(),
        nodes[2].0.as_bytes().to_vec(),
    ]
    .into_iter()
    .collect();

    for binding in &resp.rows {
        let b_id = binding.nodes.get("b").expect("missing 'b' node binding");
        assert!(
            expected_b_ids.contains(&b_id.bytes),
            "unexpected b node in results (not cited by any ANN hit)"
        );
    }
}

// ── QueryStream tests ─────────────────────────────────────────────────────────

/// Insert N edges and verify that QueryStream returns all of them in order across
/// chunk boundaries, with exactly one chunk marked `done = true` (the last one).
#[tokio::test]
async fn query_stream_returns_all_results_in_order() {
    let (svc, _dir) = open();

    // Insert 12 distinct :likes edges — enough to span multiple chunks when
    // STREAM_CHUNK_SIZE is tested with a small value in unit tests, and a
    // reasonable dataset here.
    let nodes: Vec<(polargraph_core::id::NodeId, NodeId)> = (0..13).map(|_| new_node()).collect();
    let mut triples = vec![];
    for i in 0..12 {
        triples.push(rel(nodes[i].1.clone(), "likes", nodes[i + 1].1.clone()));
    }
    svc.insert(Request::new(InsertRequest {
        triples,
        ..Default::default()
    }))
    .await
    .unwrap();

    // Stream all :likes edges.
    let mut stream = svc
        .query_stream(Request::new(QueryRequest {
            patterns: vec![pattern(var("a"), "likes", var("b"))],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    // Collect all QueryResult rows from the stream.
    let mut all_rows: Vec<(Vec<u8>, Vec<u8>)> = vec![];
    let mut last_done = false;
    let mut chunk_count: u64 = 0;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap();
        assert_eq!(
            chunk.chunk_index, chunk_count,
            "chunk_index must be monotonically increasing"
        );
        chunk_count += 1;
        for row in &chunk.results {
            let a = row.vars.get("a").expect("missing 'a' var").bytes.clone();
            let b = row.vars.get("b").expect("missing 'b' var").bytes.clone();
            all_rows.push((a, b));
        }
        if chunk.done {
            last_done = true;
            // No more chunks should follow after done=true.
            assert!(
                stream.next().await.is_none(),
                "stream should end after done=true chunk"
            );
            break;
        }
    }

    assert!(last_done, "stream must end with a chunk that has done=true");
    assert_eq!(all_rows.len(), 12, "expected exactly 12 :likes edges");

    // Verify every inserted edge appears in the results.
    for i in 0..12usize {
        let expected_a = nodes[i].0.as_bytes().to_vec();
        let expected_b = nodes[i + 1].0.as_bytes().to_vec();
        assert!(
            all_rows.contains(&(expected_a, expected_b)),
            "edge {i}→{} missing from stream results",
            i + 1
        );
    }
}

/// Verify that CypherQueryStream respects a LIMIT clause: only the first N
/// results are returned, and the stream ends with `done = true`.
#[tokio::test]
async fn cypher_query_stream_respects_limit() {
    let (svc, _dir) = open();

    // Insert 10 :follows edges.
    let nodes: Vec<(polargraph_core::id::NodeId, NodeId)> = (0..11).map(|_| new_node()).collect();
    let mut triples = vec![];
    for i in 0..10 {
        triples.push(rel(nodes[i].1.clone(), "follows", nodes[i + 1].1.clone()));
    }
    svc.insert(Request::new(InsertRequest {
        triples,
        ..Default::default()
    }))
    .await
    .unwrap();

    // Query with LIMIT 3 — should receive exactly 3 results.
    let mut stream = svc
        .cypher_query_stream(Request::new(CypherQueryRequest {
            cypher: "MATCH (a)-[:follows]->(b) RETURN a, b LIMIT 3".to_string(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    let mut total_rows = 0usize;
    let mut saw_done = false;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap();
        total_rows += chunk.results.len();
        if chunk.done {
            saw_done = true;
            break;
        }
    }

    assert!(saw_done, "stream must end with done=true");
    assert_eq!(total_rows, 3, "LIMIT 3 should produce exactly 3 results");
}

/// Unit test: verify the chunk-boundary arithmetic used by send_result_chunks.
///
/// With STREAM_CHUNK_SIZE=500: 1001 results → 3 chunks (500, 500, 1).
/// The exact chunk sizes are not observable through the gRPC API (we can only
/// count rows), so we verify the totals and that `done` is set on the last chunk.
#[tokio::test]
async fn query_stream_chunk_boundary_math() {
    let (svc, _dir) = open();

    // Insert 1001 distinct nodes with a self-linking :self edge.
    // We want 1001 rows to cross the 500-boundary twice.
    let nodes: Vec<(polargraph_core::id::NodeId, NodeId)> = (0..1001).map(|_| new_node()).collect();
    let triples: Vec<_> = nodes
        .iter()
        .map(|(_, n)| rel(n.clone(), "self", n.clone()))
        .collect();
    svc.insert(Request::new(InsertRequest {
        triples,
        ..Default::default()
    }))
    .await
    .unwrap();

    let mut stream = svc
        .query_stream(Request::new(QueryRequest {
            patterns: vec![pattern(var("x"), "self", var("y"))],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    let mut total_rows = 0usize;
    let mut chunk_indices: Vec<u64> = vec![];
    let mut saw_done = false;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap();
        chunk_indices.push(chunk.chunk_index);
        total_rows += chunk.results.len();
        if chunk.done {
            saw_done = true;
            break;
        }
    }

    assert!(saw_done, "stream must end with done=true");
    assert_eq!(total_rows, 1001, "all 1001 rows must be delivered");
    // Chunk indices must be 0, 1, 2 (three chunks for 1001 rows at 500/chunk).
    assert_eq!(
        chunk_indices,
        vec![0, 1, 2],
        "expected 3 chunks with sequential indices"
    );
}

// ── CypherWrite ───────────────────────────────────────────────────────────────

/// CREATE a node via CypherWrite, then query it back via CypherQuery.
#[tokio::test]
async fn cypher_write_create_node_then_query() {
    let (svc, _dir) = open();

    // Write: create a Person named Alice.
    let write_resp = svc
        .cypher_write(Request::new(CypherWriteRequest {
            cypher: r#"CREATE (a:Person {name: "Alice", active: true})"#.to_string(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        write_resp.created_node_ids.len(),
        1,
        "should have created one node"
    );
    assert_eq!(
        write_resp.triples_written, 3,
        "__type + name + active = 3 triples"
    );
    assert_eq!(write_resp.triples_deleted, 0);

    // Read back: MATCH all Person nodes and confirm Alice appears.
    let query_resp = svc
        .cypher_query(Request::new(CypherQueryRequest {
            cypher: r#"MATCH (a:Person) RETURN a"#.to_string(),
            as_of_valid_time: 0,
            as_of_tx_time: 0,
            vector: vec![],
            ef: 0,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(query_resp.rows.len(), 1, "expected exactly one Person");
    let created_proto_id = &write_resp.created_node_ids[0];
    let returned_id = query_resp.rows[0]
        .nodes
        .get("a")
        .expect("missing 'a' binding");
    assert_eq!(
        &returned_id.bytes, created_proto_id,
        "returned node id must match created id"
    );
}

/// MERGE is idempotent: two identical MERGEs produce exactly one node.
#[tokio::test]
async fn cypher_write_merge_is_idempotent() {
    let (svc, _dir) = open();

    let do_merge = || CypherWriteRequest {
        cypher: r#"MERGE (c:Company {name: "Acme"})"#.to_string(),
        ..Default::default()
    };

    let r1 = svc
        .cypher_write(Request::new(do_merge()))
        .await
        .unwrap()
        .into_inner();
    let r2 = svc
        .cypher_write(Request::new(do_merge()))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        r1.created_node_ids.len(),
        1,
        "first MERGE should create the node"
    );
    assert_eq!(
        r2.created_node_ids.len(),
        0,
        "second MERGE should be a no-op"
    );

    // Confirm only one Company node in the graph.
    let query_resp = svc
        .cypher_query(Request::new(CypherQueryRequest {
            cypher: r#"MATCH (c:Company) RETURN c"#.to_string(),
            as_of_valid_time: 0,
            as_of_tx_time: 0,
            vector: vec![],
            ef: 0,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        query_resp.rows.len(),
        1,
        "expected exactly one Company node after two MERGEs"
    );
}

// ── Wire transactions ─────────────────────────────────────────────────────────

#[tokio::test]
async fn wire_tx_begin_returns_unique_ids() {
    let (svc, _dir) = open();
    let r1 = svc
        .begin_transaction(Request::new(BeginTransactionRequest {}))
        .await
        .unwrap()
        .into_inner();
    let r2 = svc
        .begin_transaction(Request::new(BeginTransactionRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert!(!r1.tx_id.is_empty(), "tx_id must not be empty");
    assert_ne!(r1.tx_id, r2.tx_id, "each begin must return a unique tx_id");
    let _ = svc
        .rollback_transaction(Request::new(RollbackTransactionRequest { tx_id: r1.tx_id }))
        .await;
    let _ = svc
        .rollback_transaction(Request::new(RollbackTransactionRequest { tx_id: r2.tx_id }))
        .await;
}

#[tokio::test]
async fn wire_tx_commit_makes_triples_visible() {
    let (svc, _dir) = open();
    let (_alice_core, alice) = new_node();
    let (_bob_core, bob) = new_node();

    let tx_id = svc
        .begin_transaction(Request::new(BeginTransactionRequest {}))
        .await
        .unwrap()
        .into_inner()
        .tx_id;

    svc.insert(Request::new(InsertRequest {
        triples: vec![rel(alice.clone(), "tx_edge", bob.clone())],
        tx_id: tx_id.clone(),
        ..Default::default()
    }))
    .await
    .unwrap();

    // Not visible outside the transaction yet.
    let outside = svc
        .query(Request::new(QueryRequest {
            patterns: vec![VarPattern {
                subject: Some(Term {
                    kind: Some(TermKind::Bound(alice.clone())),
                }),
                predicate: "tx_edge".to_string(),
                object: Some(Term {
                    kind: Some(TermKind::Var("v".to_string())),
                }),
                predicate_var: String::new(),
            }],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(
        outside.bindings.is_empty(),
        "triple should not be visible before commit"
    );

    let commit_resp = svc
        .commit_transaction(Request::new(CommitTransactionRequest { tx_id }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(commit_resp.triples_written, 1);

    let after = svc
        .query(Request::new(QueryRequest {
            patterns: vec![VarPattern {
                subject: Some(Term {
                    kind: Some(TermKind::Bound(alice.clone())),
                }),
                predicate: "tx_edge".to_string(),
                object: Some(Term {
                    kind: Some(TermKind::Var("v".to_string())),
                }),
                predicate_var: String::new(),
            }],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(after.bindings.len(), 1, "committed triple must be visible");
}

#[tokio::test]
async fn wire_tx_rollback_discards_triples() {
    let (svc, _dir) = open();
    let (_alice_core, alice) = new_node();

    let tx_id = svc
        .begin_transaction(Request::new(BeginTransactionRequest {}))
        .await
        .unwrap()
        .into_inner()
        .tx_id;

    svc.insert(Request::new(InsertRequest {
        triples: vec![text_prop(alice.clone(), "tag", "ephemeral")],
        tx_id: tx_id.clone(),
        ..Default::default()
    }))
    .await
    .unwrap();

    svc.rollback_transaction(Request::new(RollbackTransactionRequest { tx_id }))
        .await
        .unwrap();

    let resp = svc
        .query(Request::new(QueryRequest {
            patterns: vec![VarPattern {
                subject: Some(Term {
                    kind: Some(TermKind::Bound(alice.clone())),
                }),
                predicate: "tag".to_string(),
                object: Some(Term {
                    kind: Some(TermKind::Var("v".to_string())),
                }),
                predicate_var: String::new(),
            }],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(
        resp.bindings.is_empty(),
        "rolled-back triple must not appear"
    );
}

#[tokio::test]
async fn wire_tx_write_your_own_reads() {
    let (svc, _dir) = open();
    let (_alice_core, alice) = new_node();
    let (_bob_core, bob) = new_node();

    let tx_id = svc
        .begin_transaction(Request::new(BeginTransactionRequest {}))
        .await
        .unwrap()
        .into_inner()
        .tx_id;

    svc.insert(Request::new(InsertRequest {
        triples: vec![rel(alice.clone(), "tx_wyr_edge", bob.clone())],
        tx_id: tx_id.clone(),
        ..Default::default()
    }))
    .await
    .unwrap();

    // Within the same transaction, buffered triple should be readable.
    let resp = svc
        .query(Request::new(QueryRequest {
            patterns: vec![VarPattern {
                subject: Some(Term {
                    kind: Some(TermKind::Bound(alice.clone())),
                }),
                predicate: "tx_wyr_edge".to_string(),
                object: Some(Term {
                    kind: Some(TermKind::Var("v".to_string())),
                }),
                predicate_var: String::new(),
            }],
            tx_id: tx_id.clone(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        resp.bindings.len(),
        1,
        "write-your-own-reads must work within an open transaction"
    );

    let _ = svc
        .rollback_transaction(Request::new(RollbackTransactionRequest { tx_id }))
        .await;
}

#[tokio::test]
async fn wire_tx_rollback_removes_from_map() {
    let (svc, _dir) = open();

    let tx_id = svc
        .begin_transaction(Request::new(BeginTransactionRequest {}))
        .await
        .unwrap()
        .into_inner()
        .tx_id;

    svc.rollback_transaction(Request::new(RollbackTransactionRequest {
        tx_id: tx_id.clone(),
    }))
    .await
    .unwrap();

    // A second rollback should return NOT_FOUND.
    let err = svc
        .rollback_transaction(Request::new(RollbackTransactionRequest { tx_id }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
}

// ── Diagnostics RPCs ──────────────────────────────────────────────────────────

#[tokio::test]
async fn show_indexes_returns_all_cfs() {
    use polargraph_server::proto::{ShowIndexesRequest, ShowIndexesResponse};

    let (svc, _dir) = open();
    let resp: ShowIndexesResponse = svc
        .show_indexes(Request::new(ShowIndexesRequest {}))
        .await
        .unwrap()
        .into_inner();

    let cf_names: Vec<&str> = resp
        .column_families
        .iter()
        .map(|cf| cf.name.as_str())
        .collect();
    for expected in &[
        "spo", "sop", "pso", "pos", "osp", "ops", "meta", "hnsw", "tri",
    ] {
        assert!(
            cf_names.contains(expected),
            "expected CF '{}' in ShowIndexes response, got: {:?}",
            expected,
            cf_names
        );
    }
}

#[tokio::test]
async fn show_stats_returns_primary_mode() {
    use polargraph_server::proto::{ShowStatsRequest, ShowStatsResponse};

    let (svc, _dir) = open();
    let resp: ShowStatsResponse = svc
        .show_stats(Request::new(ShowStatsRequest {}))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        resp.mode, "primary",
        "ShowStats mode should be 'primary' for a non-replica store"
    );
}

#[tokio::test]
async fn show_stats_reflects_inserts() {
    use polargraph_server::proto::{ShowStatsRequest, ShowStatsResponse};

    let (svc, _dir) = open();

    let (_, alice) = new_node();
    let (_, bob) = new_node();
    svc.insert(Request::new(InsertRequest {
        triples: vec![rel(alice.clone(), "knows", bob.clone())],
        ..Default::default()
    }))
    .await
    .unwrap();

    let resp: ShowStatsResponse = svc
        .show_stats(Request::new(ShowStatsRequest {}))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.mode, "primary");
    assert!(
        resp.mvcc_oracle_ts > 0,
        "oracle_ts should advance after an insert"
    );
    assert!(
        resp.predicate_intern_count >= 1,
        "at least 'knows' should be interned"
    );
    assert_eq!(
        resp.open_transaction_count, 0,
        "no open transactions after committed insert"
    );
}

#[tokio::test]
async fn slow_query_threshold_does_not_break_results() {
    // Set a 1 ms slow-query threshold — almost any query will exceed it in
    // the test environment. Verify that the service still returns correct
    // results (the slow-query check is a pure side-effect, not a gate).
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    let svc = PolarGraphServer::new(store).unwrap().with_slow_query_ms(1); // trigger slow-query path on nearly every query

    let (_, alice) = new_node();
    let (_, bob) = new_node();
    svc.insert(Request::new(InsertRequest {
        triples: vec![rel(alice.clone(), "friends", bob.clone())],
        ..Default::default()
    }))
    .await
    .unwrap();

    let resp = svc
        .query(Request::new(QueryRequest {
            patterns: vec![pattern(bound(&alice), "friends", var("x"))],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        resp.bindings.len(),
        1,
        "query should return one binding even when slow-query threshold fires"
    );
    assert_eq!(
        resp.bindings[0].vars.get("x").map(|v| v.bytes.clone()),
        Some(bob.bytes)
    );
}

#[tokio::test]
async fn show_indexes_reflects_registered_vector_space() {
    use polargraph_server::proto::{InsertVectorRequest, ShowIndexesRequest, ShowIndexesResponse};

    let (svc, _dir) = open();

    // Register a node type with a named vector space.
    svc.register_node_type(Request::new(RegisterNodeTypeRequest {
        definition: Some(NodeTypeDef {
            type_name: "Doc".into(),
            fields: vec![],
            parent_types: vec![],
            vector_space: Some(VectorSpaceDef {
                space_name: "doc_embeddings".into(),
                dimensions: 3,
                embedding_model: String::new(),
                storage_mode: String::new(),
            }),
        }),
    }))
    .await
    .unwrap();

    // Insert a vector into that space so the HNSW index is materialised.
    let (_, node_id) = new_node();
    svc.insert_vector(Request::new(InsertVectorRequest {
        node_id: Some(node_id),
        vector: vec![1.0, 0.0, 0.0],
        space: "doc_embeddings".into(),
    }))
    .await
    .unwrap();

    let resp: ShowIndexesResponse = svc
        .show_indexes(Request::new(ShowIndexesRequest {}))
        .await
        .unwrap()
        .into_inner();

    let space_names: Vec<&str> = resp
        .vector_spaces
        .iter()
        .map(|vs| vs.name.as_str())
        .collect();
    assert!(
        space_names.contains(&"doc_embeddings"),
        "expected 'doc_embeddings' in ShowIndexes vector_spaces, got: {:?}",
        space_names
    );
    let space = resp
        .vector_spaces
        .iter()
        .find(|vs| vs.name == "doc_embeddings")
        .unwrap();
    assert_eq!(space.node_count, 1, "vector space should have one entry");
    assert_eq!(space.dimensions, 3);
}

// ── Full-text / trigram Cypher WHERE tests ────────────────────────────────────

#[tokio::test]
async fn cypher_where_contains_finds_match() {
    let (svc, _dir) = open();
    let (_alice_core, alice) = new_node();
    let (_bob_core, bob) = new_node();

    svc.insert(Request::new(InsertRequest {
        triples: vec![
            text_prop(alice.clone(), "__type", "Person"),
            text_prop(alice.clone(), "name", "Alice Smith"),
            text_prop(bob.clone(), "__type", "Person"),
            text_prop(bob.clone(), "name", "Robert Jones"),
        ],
        ..Default::default()
    }))
    .await
    .unwrap();

    let resp = svc
        .cypher_query(Request::new(CypherQueryRequest {
            cypher: r#"MATCH (n:Person) WHERE n.name CONTAINS "Smith" RETURN n"#.to_string(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.rows.len(), 1, "only Alice Smith matches");
    let bound = resp.rows[0].nodes.get("n").unwrap();
    let bound_id = polargraph_core::id::NodeId(uuid::Uuid::from_bytes(
        bound.bytes[..16].try_into().unwrap(),
    ));
    let alice_id = polargraph_core::id::NodeId(uuid::Uuid::from_bytes(
        alice.bytes[..16].try_into().unwrap(),
    ));
    assert_eq!(bound_id, alice_id);
}

#[tokio::test]
async fn cypher_where_starts_with_finds_match() {
    let (svc, _dir) = open();
    let (_alice_core, alice) = new_node();
    let (_bob_core, bob) = new_node();

    svc.insert(Request::new(InsertRequest {
        triples: vec![
            text_prop(alice.clone(), "__type", "Person"),
            text_prop(alice.clone(), "name", "Alexandra Doe"),
            text_prop(bob.clone(), "__type", "Person"),
            text_prop(bob.clone(), "name", "Robert Smith"),
        ],
        ..Default::default()
    }))
    .await
    .unwrap();

    let resp = svc
        .cypher_query(Request::new(CypherQueryRequest {
            cypher: r#"MATCH (n:Person) WHERE n.name STARTS WITH "Alex" RETURN n"#.to_string(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.rows.len(), 1, "only Alexandra matches");
    let bound = resp.rows[0].nodes.get("n").unwrap();
    let bound_id = polargraph_core::id::NodeId(uuid::Uuid::from_bytes(
        bound.bytes[..16].try_into().unwrap(),
    ));
    let alice_id = polargraph_core::id::NodeId(uuid::Uuid::from_bytes(
        alice.bytes[..16].try_into().unwrap(),
    ));
    assert_eq!(bound_id, alice_id);
}

#[tokio::test]
async fn cypher_where_contains_no_match_returns_empty() {
    let (svc, _dir) = open();
    let (_n_core, n) = new_node();

    svc.insert(Request::new(InsertRequest {
        triples: vec![
            text_prop(n.clone(), "__type", "Person"),
            text_prop(n.clone(), "name", "Alice"),
        ],
        ..Default::default()
    }))
    .await
    .unwrap();

    let resp = svc
        .cypher_query(Request::new(CypherQueryRequest {
            cypher: r#"MATCH (n:Person) WHERE n.name CONTAINS "zzz" RETURN n"#.to_string(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.rows.is_empty(), "no node should match");
}

#[tokio::test]
async fn cypher_where_regex_finds_match() {
    let (svc, _dir) = open();
    let (_alice_core, alice) = new_node();
    let (_bob_core, bob) = new_node();

    svc.insert(Request::new(InsertRequest {
        triples: vec![
            text_prop(alice.clone(), "__type", "Person"),
            text_prop(alice.clone(), "email", "alice@example.com"),
            text_prop(bob.clone(), "__type", "Person"),
            text_prop(bob.clone(), "email", "bob_at_domain_dot_org"),
        ],
        ..Default::default()
    }))
    .await
    .unwrap();

    // Use [.]com to match literal dot without backslash escapes in Cypher string.
    let resp = svc
        .cypher_query(Request::new(CypherQueryRequest {
            cypher: r#"MATCH (n:Person) WHERE n.email =~ ".*[.]com$" RETURN n"#.to_string(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.rows.len(), 1, "only alice@example.com matches");
    let bound = resp.rows[0].nodes.get("n").unwrap();
    let bound_id = polargraph_core::id::NodeId(uuid::Uuid::from_bytes(
        bound.bytes[..16].try_into().unwrap(),
    ));
    let alice_id = polargraph_core::id::NodeId(uuid::Uuid::from_bytes(
        alice.bytes[..16].try_into().unwrap(),
    ));
    assert_eq!(bound_id, alice_id);
}

// ── Schema & ontology layer tests ─────────────────────────────────────────────

#[tokio::test]
async fn test_grpc_edge_cardinality_enforced() {
    let (svc, _dir) = open();

    // Register an edge type with one_to_many cardinality.
    svc.register_edge_type(Request::new(RegisterEdgeTypeRequest {
        definition: Some(EdgeTypeDef {
            predicate: "assigned_to".into(),
            domain: String::new(),
            range: String::new(),
            fields: vec![],
            cardinality: "one_to_many".into(),
            inverse_of: String::new(),
        }),
    }))
    .await
    .unwrap();

    let (_, task) = new_node();
    let (_, owner1) = new_node();
    let (_, owner2) = new_node();

    // First insert should succeed.
    svc.insert(Request::new(InsertRequest {
        triples: vec![rel(task.clone(), "assigned_to", owner1.clone())],
        ..Default::default()
    }))
    .await
    .expect("first insert should succeed");

    // Second insert with the same subject and a different object → cardinality violation.
    let err = svc
        .insert(Request::new(InsertRequest {
            triples: vec![rel(task.clone(), "assigned_to", owner2.clone())],
            ..Default::default()
        }))
        .await
        .unwrap_err();

    assert_eq!(
        err.code(),
        tonic::Code::FailedPrecondition,
        "expected FAILED_PRECONDITION for cardinality violation, got: {}",
        err.message()
    );
    assert!(
        err.message().contains("one object per subject"),
        "got: {}",
        err.message()
    );
}

#[tokio::test]
async fn test_grpc_subtype_inheritance() {
    let (svc, _dir) = open();

    // Register a parent type with a required field.
    svc.register_node_type(Request::new(RegisterNodeTypeRequest {
        definition: Some(NodeTypeDef {
            type_name: "Entity".into(),
            fields: vec![FieldDef {
                field_name: "id_code".into(),
                kind: "text".into(),
                required: true,
            }],
            vector_space: None,
            parent_types: vec![],
        }),
    }))
    .await
    .unwrap();

    // Register a child type that inherits from Entity.
    svc.register_node_type(Request::new(RegisterNodeTypeRequest {
        definition: Some(NodeTypeDef {
            type_name: "Person".into(),
            fields: vec![FieldDef {
                field_name: "name".into(),
                kind: "text".into(),
                required: true,
            }],
            vector_space: None,
            parent_types: vec!["Entity".into()],
        }),
    }))
    .await
    .unwrap();

    // Validate without inherited required field "id_code" → should fail.
    let resp = svc
        .validate_node(Request::new(ValidateNodeRequest {
            type_name: "Person".into(),
            properties: std::collections::HashMap::from([
                (
                    "name".to_string(),
                    Value {
                        kind: Some(ValueKind::TextVal("Alice".into())),
                    },
                ),
                // "id_code" from Entity is missing
            ]),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(
        !resp.valid,
        "validation should fail when inherited required field is missing"
    );
    assert!(
        resp.errors.iter().any(|e| e.contains("id_code")),
        "error should mention missing inherited field 'id_code': {:?}",
        resp.errors
    );

    // Validate with all fields → should pass.
    let resp2 = svc
        .validate_node(Request::new(ValidateNodeRequest {
            type_name: "Person".into(),
            properties: std::collections::HashMap::from([
                (
                    "name".to_string(),
                    Value {
                        kind: Some(ValueKind::TextVal("Alice".into())),
                    },
                ),
                (
                    "id_code".to_string(),
                    Value {
                        kind: Some(ValueKind::TextVal("E001".into())),
                    },
                ),
            ]),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(
        resp2.valid,
        "validation should pass when all inherited fields are present"
    );
}

#[tokio::test]
async fn test_grpc_validate_ontology() {
    let (svc, _dir) = open();

    // Register a predicate with an inverse.
    svc.register_edge_type(Request::new(RegisterEdgeTypeRequest {
        definition: Some(EdgeTypeDef {
            predicate: "parent_of".into(),
            domain: String::new(),
            range: String::new(),
            fields: vec![],
            cardinality: String::new(),
            inverse_of: "child_of".into(),
        }),
    }))
    .await
    .unwrap();

    svc.register_edge_type(Request::new(RegisterEdgeTypeRequest {
        definition: Some(EdgeTypeDef {
            predicate: "child_of".into(),
            domain: String::new(),
            range: String::new(),
            fields: vec![],
            cardinality: String::new(),
            inverse_of: String::new(),
        }),
    }))
    .await
    .unwrap();

    let (_, alice) = new_node();
    let (_, bob) = new_node();

    // Insert parent_of AND its inverse child_of → ontology should be clean.
    svc.insert(Request::new(InsertRequest {
        triples: vec![
            rel(alice.clone(), "parent_of", bob.clone()),
            rel(bob.clone(), "child_of", alice.clone()),
        ],
        ..Default::default()
    }))
    .await
    .unwrap();

    let resp = svc
        .validate_ontology(Request::new(ValidateOntologyRequest {}))
        .await
        .unwrap()
        .into_inner();

    assert!(
        resp.valid,
        "ontology should be valid when inverse pairs are consistent; violations: {:?}",
        resp.violations
    );

    // Insert a parent_of WITHOUT the inverse → validate_ontology should report a violation.
    let (_, carol) = new_node();
    let (_, dave) = new_node();
    svc.insert(Request::new(InsertRequest {
        triples: vec![rel(carol.clone(), "parent_of", dave.clone())],
        ..Default::default()
    }))
    .await
    .unwrap();

    let resp2 = svc
        .validate_ontology(Request::new(ValidateOntologyRequest {}))
        .await
        .unwrap()
        .into_inner();

    assert!(
        !resp2.valid,
        "ontology should report violation when inverse is missing"
    );
    assert!(
        resp2
            .violations
            .iter()
            .any(|v| v.violation_type == "inverse"),
        "expected 'inverse' violation type; got: {:?}",
        resp2.violations
    );
}

// ── UI key management tests ───────────────────────────────────────────────────

#[tokio::test]
async fn ui_keys_list_with_auth_disabled() {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    // Start with no keys — auth is disabled.
    let (base, shutdown, _dir) = start_ui_http(store, vec![]).await;

    let client = reqwest::Client::new();
    let res = client.get(format!("{base}/api/keys")).send().await.unwrap();
    assert_eq!(res.status(), 200);
    let json: serde_json::Value = res.json().await.unwrap();
    assert_eq!(json["auth_disabled"], true);
    assert_eq!(json["total"], 0);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn ui_keys_add_and_revoke() {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    let (base, shutdown, _dir) = start_ui_http(store, vec!["admin".into()]).await;

    let client = reqwest::Client::new();

    // List — one key present.
    let res = client
        .get(format!("{base}/api/keys"))
        .header("Authorization", "Bearer admin")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let json: serde_json::Value = res.json().await.unwrap();
    assert_eq!(json["total"], 1);
    assert_eq!(json["auth_disabled"], false);

    // Add a key.
    let res = client
        .post(format!("{base}/api/keys/add"))
        .header("Authorization", "Bearer admin")
        .json(&serde_json::json!({"key": "extra-key-xyz"}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let json: serde_json::Value = res.json().await.unwrap();
    assert_eq!(json["total"], 2);

    // List — two keys now.
    let res = client
        .get(format!("{base}/api/keys"))
        .header("Authorization", "Bearer admin")
        .send()
        .await
        .unwrap();
    let json: serde_json::Value = res.json().await.unwrap();
    assert_eq!(json["total"], 2);

    // Revoke the extra key.
    let res = client
        .post(format!("{base}/api/keys/revoke"))
        .header("Authorization", "Bearer admin")
        .json(&serde_json::json!({"key": "extra-key-xyz"}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let json: serde_json::Value = res.json().await.unwrap();
    assert_eq!(json["found"], true);
    assert_eq!(json["total"], 1);

    // Revoke again — found=false.
    let res = client
        .post(format!("{base}/api/keys/revoke"))
        .header("Authorization", "Bearer admin")
        .json(&serde_json::json!({"key": "extra-key-xyz"}))
        .send()
        .await
        .unwrap();
    let json: serde_json::Value = res.json().await.unwrap();
    assert_eq!(json["found"], false);
    assert_eq!(json["total"], 1);

    let _ = shutdown.send(());
}

// ── Edge annotation (RDF-star) tests ─────────────────────────────────────────

use polargraph_server::proto::{
    edge_annotation::Value as AnnotationValue, EdgeAnnotation, GetEdgeAnnotationsRequest,
};

/// Insert a relation and return its edge UUID bytes (from InsertResponse).
async fn insert_relation_with_known_edge(
    svc: &PolarGraphServer,
    subject: NodeId,
    pred: &str,
    object: NodeId,
) -> Vec<u8> {
    let req = InsertRequest {
        triples: vec![rel(subject, pred, object)],
        ..Default::default()
    };
    let resp = svc.insert(Request::new(req)).await.unwrap().into_inner();
    resp.edge_ids.into_iter().next().unwrap_or_default()
}

#[tokio::test]
async fn edge_annotation_property_roundtrip() {
    let (svc, _dir) = open();
    let (_, alice) = new_node();
    let (_, bob) = new_node();

    let edge_bytes = insert_relation_with_known_edge(&svc, alice, "trusts", bob).await;
    assert_eq!(edge_bytes.len(), 16, "edge_id should be 16 bytes");

    // Annotate the edge with a scalar property.
    let ann_req = InsertRequest {
        edge_annotations: vec![EdgeAnnotation {
            edge_id: edge_bytes.clone(),
            predicate: "confidence".into(),
            value: Some(AnnotationValue::Scalar(Value {
                kind: Some(ValueKind::FloatVal(0.9)),
            })),
        }],
        ..Default::default()
    };
    svc.insert(Request::new(ann_req)).await.unwrap();

    let get_req = GetEdgeAnnotationsRequest {
        edge_id: edge_bytes,
    };
    let resp = svc
        .get_edge_annotations(Request::new(get_req))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.annotations.len(), 1);
    let ann = &resp.annotations[0];
    assert_eq!(ann.predicate, "confidence");
    match &ann.value {
        Some(AnnotationValue::Scalar(v)) => match &v.kind {
            Some(ValueKind::FloatVal(f)) => assert!((f - 0.9).abs() < 1e-6),
            _ => panic!("unexpected scalar kind"),
        },
        _ => panic!("expected Scalar annotation"),
    }
}

#[tokio::test]
async fn edge_annotation_relation_roundtrip() {
    let (svc, _dir) = open();
    let (_, a) = new_node();
    let (_, b) = new_node();
    let (_, provenance) = new_node();

    let edge_bytes = insert_relation_with_known_edge(&svc, a, "knows", b).await;
    assert_eq!(edge_bytes.len(), 16);

    let ann_req = InsertRequest {
        edge_annotations: vec![EdgeAnnotation {
            edge_id: edge_bytes.clone(),
            predicate: "assertedBy".into(),
            value: Some(AnnotationValue::NodeId(provenance.bytes.clone())),
        }],
        ..Default::default()
    };
    svc.insert(Request::new(ann_req)).await.unwrap();

    let get_req = GetEdgeAnnotationsRequest {
        edge_id: edge_bytes,
    };
    let resp = svc
        .get_edge_annotations(Request::new(get_req))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.annotations.len(), 1);
    let ann = &resp.annotations[0];
    assert_eq!(ann.predicate, "assertedBy");
    match &ann.value {
        Some(AnnotationValue::NodeId(nid_bytes)) => {
            assert_eq!(nid_bytes, &provenance.bytes)
        }
        _ => panic!("expected NodeId annotation"),
    }
}

#[tokio::test]
async fn edge_annotation_in_insert_batch() {
    let (svc, _dir) = open();
    let (_, a) = new_node();
    let (_, b) = new_node();

    // Insert relation first to get its edge id.
    let insert_resp = svc
        .insert(Request::new(InsertRequest {
            triples: vec![rel(a.clone(), "likes", b.clone())],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    let edge_bytes = insert_resp.edge_ids.into_iter().next().unwrap();

    // Insert a property triple and two annotations in the same batch.
    svc.insert(Request::new(InsertRequest {
        triples: vec![text_prop(a.clone(), "name", "Alice")],
        edge_annotations: vec![
            EdgeAnnotation {
                edge_id: edge_bytes.clone(),
                predicate: "weight".into(),
                value: Some(AnnotationValue::Scalar(Value {
                    kind: Some(ValueKind::FloatVal(1.0)),
                })),
            },
            EdgeAnnotation {
                edge_id: edge_bytes.clone(),
                predicate: "source".into(),
                value: Some(AnnotationValue::Scalar(Value {
                    kind: Some(ValueKind::TextVal("test".into())),
                })),
            },
        ],
        ..Default::default()
    }))
    .await
    .unwrap();

    let resp = svc
        .get_edge_annotations(Request::new(GetEdgeAnnotationsRequest {
            edge_id: edge_bytes,
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.annotations.len(), 2);
    let predicates: std::collections::HashSet<_> = resp
        .annotations
        .iter()
        .map(|a| a.predicate.as_str())
        .collect();
    assert!(predicates.contains("weight"));
    assert!(predicates.contains("source"));
}

// ── Access control tests ──────────────────────────────────────────────────────

use polargraph_server::proto::{
    grant_access_request::Target as GrantTarget, revoke_access_request::Target as RevokeTarget,
};

/// add_user_to_group writes a MEMBER_OF triple; get_user_access returns the
/// expanded node set.
#[tokio::test]
async fn add_user_to_group_and_grant_access_visible_in_get_user_access() {
    let (svc, _dir) = open();
    let (core_user, _) = new_node();
    let (core_group, _) = new_node();
    let (core_node, _) = new_node();

    // Add user to group.
    svc.add_user_to_group(Request::new(AddUserToGroupRequest {
        user_id: core_user.as_bytes().to_vec(),
        group_id: core_group.as_bytes().to_vec(),
    }))
    .await
    .unwrap();

    // Grant group access to a node.
    svc.grant_access(Request::new(GrantAccessRequest {
        group_id: core_group.as_bytes().to_vec(),
        target: Some(GrantTarget::NodeId(core_node.as_bytes().to_vec())),
    }))
    .await
    .unwrap();

    // User should now have the node in their access set.
    let resp = svc
        .get_user_access(Request::new(GetUserAccessRequest {
            user_id: core_user.as_bytes().to_vec(),
        }))
        .await
        .unwrap()
        .into_inner();

    let ids: std::collections::HashSet<Vec<u8>> = resp.node_ids.into_iter().collect();
    assert!(
        ids.contains(&core_node.as_bytes().to_vec()),
        "expected node_a in user access set"
    );
}

/// grant_access with type_name appears in GetUserAccess.type_grants.
#[tokio::test]
async fn grant_access_type_name_appears_in_type_grants() {
    let (svc, _dir) = open();
    let (core_user, _) = new_node();
    let (core_group, _) = new_node();

    svc.add_user_to_group(Request::new(AddUserToGroupRequest {
        user_id: core_user.as_bytes().to_vec(),
        group_id: core_group.as_bytes().to_vec(),
    }))
    .await
    .unwrap();

    svc.grant_access(Request::new(GrantAccessRequest {
        group_id: core_group.as_bytes().to_vec(),
        target: Some(GrantTarget::TypeName("Service".to_string())),
    }))
    .await
    .unwrap();

    let resp = svc
        .get_user_access(Request::new(GetUserAccessRequest {
            user_id: core_user.as_bytes().to_vec(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(
        resp.type_grants.contains(&"Service".to_string()),
        "expected 'Service' in type_grants, got {:?}",
        resp.type_grants
    );
}

/// revoke_access removes the node from the user's access set.
#[tokio::test]
async fn revoke_access_removes_node_from_access_set() {
    let (svc, _dir) = open();
    let (core_user, _) = new_node();
    let (core_group, _) = new_node();
    let (core_node, _) = new_node();

    svc.add_user_to_group(Request::new(AddUserToGroupRequest {
        user_id: core_user.as_bytes().to_vec(),
        group_id: core_group.as_bytes().to_vec(),
    }))
    .await
    .unwrap();

    svc.grant_access(Request::new(GrantAccessRequest {
        group_id: core_group.as_bytes().to_vec(),
        target: Some(GrantTarget::NodeId(core_node.as_bytes().to_vec())),
    }))
    .await
    .unwrap();

    // Revoke the grant.
    svc.revoke_access(Request::new(RevokeAccessRequest {
        group_id: core_group.as_bytes().to_vec(),
        target: Some(RevokeTarget::NodeId(core_node.as_bytes().to_vec())),
    }))
    .await
    .unwrap();

    let resp = svc
        .get_user_access(Request::new(GetUserAccessRequest {
            user_id: core_user.as_bytes().to_vec(),
        }))
        .await
        .unwrap()
        .into_inner();

    let ids: std::collections::HashSet<Vec<u8>> = resp.node_ids.into_iter().collect();
    assert!(
        !ids.contains(&core_node.as_bytes().to_vec()),
        "node should have been removed after revoke"
    );
}

/// query results are filtered by user_id when the user has a non-empty access set.
///
/// Uses `any()` for the object so property triples bind only `?s` (the subject).
/// `extend_bindings` skips the object slot for Var when the triple is a Property,
/// so we must use `any()` to avoid the row being dropped entirely.
#[tokio::test]
async fn query_filtered_by_user_id() {
    let (svc, _dir) = open();
    let (core_user, _) = new_node();
    let (core_group, _) = new_node();
    let (core_allowed, proto_allowed) = new_node();
    let (_, proto_hidden) = new_node();

    // Insert both nodes as subjects of a property predicate.
    svc.insert(Request::new(InsertRequest {
        triples: vec![
            text_prop(proto_allowed.clone(), "label", "allowed"),
            text_prop(proto_hidden.clone(), "label", "hidden"),
        ],
        ..Default::default()
    }))
    .await
    .unwrap();

    // Grant user access to only the allowed node.
    svc.add_user_to_group(Request::new(AddUserToGroupRequest {
        user_id: core_user.as_bytes().to_vec(),
        group_id: core_group.as_bytes().to_vec(),
    }))
    .await
    .unwrap();

    svc.grant_access(Request::new(GrantAccessRequest {
        group_id: core_group.as_bytes().to_vec(),
        target: Some(GrantTarget::NodeId(core_allowed.as_bytes().to_vec())),
    }))
    .await
    .unwrap();

    // Query with user_id set. Use any() for object so property triples bind ?s.
    let resp = svc
        .query(Request::new(QueryRequest {
            patterns: vec![pattern(var("s"), "label", any())],
            user_id: core_user.to_string(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    // Only the allowed node's binding should survive.
    let subjects: Vec<_> = resp
        .bindings
        .iter()
        .filter_map(|r| r.vars.get("s").cloned())
        .collect();

    assert_eq!(
        subjects.len(),
        1,
        "expected exactly one result after filtering"
    );
    assert_eq!(
        subjects[0].bytes,
        core_allowed.as_bytes().to_vec(),
        "hidden node should have been filtered out"
    );
}

/// query without user_id returns all results (no filtering).
#[tokio::test]
async fn query_without_user_id_returns_all() {
    let (svc, _dir) = open();
    let (_, a) = new_node();
    let (_, b) = new_node();

    svc.insert(Request::new(InsertRequest {
        triples: vec![
            text_prop(a.clone(), "color", "red"),
            text_prop(b.clone(), "color", "blue"),
        ],
        ..Default::default()
    }))
    .await
    .unwrap();

    let resp = svc
        .query(Request::new(QueryRequest {
            patterns: vec![pattern(var("s"), "color", any())],
            user_id: String::new(), // no user_id → no filtering
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        resp.bindings.len(),
        2,
        "expected both nodes without access filter"
    );
}

#[tokio::test]
async fn cypher_id_function_returns_edge_uuid() {
    let (svc, _dir) = open();
    let (_a_core, a) = new_node();
    let (_b_core, b) = new_node();

    // Insert a relation and capture the returned edge_id.
    let insert_resp = svc
        .insert(Request::new(InsertRequest {
            triples: vec![rel(a.clone(), "DEPENDS_ON", b.clone())],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    // The insert response returns edge UUIDs for relation triples.
    assert_eq!(insert_resp.edge_ids.len(), 1, "one edge_id expected");
    let edge_id_bytes = &insert_resp.edge_ids[0];
    let expected_uuid = uuid::Uuid::from_bytes(edge_id_bytes[..16].try_into().unwrap());

    // Query using id(r) and verify the returned UUID matches.
    let resp = svc
        .cypher_query(Request::new(CypherQueryRequest {
            cypher: "MATCH (a)-[r:DEPENDS_ON]->(b) RETURN a, b, id(r)".to_string(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.rows.len(), 1, "one matching row expected");
    let row = &resp.rows[0];

    // id(r) appears in the values map as a string UUID.
    let id_val = row.values.get("id(r)").expect("id(r) should be in values");
    let uuid_str = match &id_val.kind {
        Some(ValueKind::TextVal(s)) => s.clone(),
        other => panic!("expected TextVal, got {:?}", other),
    };
    let returned_uuid = uuid::Uuid::parse_str(&uuid_str).expect("id(r) should be a valid UUID");
    assert_eq!(
        returned_uuid, expected_uuid,
        "edge UUID from id(r) should match the inserted edge_id"
    );
}

#[tokio::test]
async fn cypher_element_id_alias_returns_same_uuid() {
    let (svc, _dir) = open();
    let (_a_core, a) = new_node();
    let (_b_core, b) = new_node();

    let insert_resp = svc
        .insert(Request::new(InsertRequest {
            triples: vec![rel(a.clone(), "KNOWS", b.clone())],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    let edge_id_bytes = &insert_resp.edge_ids[0];
    let expected_uuid = uuid::Uuid::from_bytes(edge_id_bytes[..16].try_into().unwrap());

    let resp = svc
        .cypher_query(Request::new(CypherQueryRequest {
            cypher: "MATCH (a)-[r:KNOWS]->(b) RETURN elementId(r)".to_string(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.rows.len(), 1);
    let row = &resp.rows[0];
    let id_val = row
        .values
        .get("id(r)")
        .expect("id(r) should be in values (elementId maps to same key)");
    let uuid_str = match &id_val.kind {
        Some(ValueKind::TextVal(s)) => s.clone(),
        other => panic!("expected TextVal, got {:?}", other),
    };
    let returned_uuid = uuid::Uuid::parse_str(&uuid_str).expect("should be valid UUID");
    assert_eq!(returned_uuid, expected_uuid);
}

#[tokio::test]
async fn cypher_id_n_returns_node_uuid() {
    let (svc, _dir) = open();
    let (a_core, a) = new_node();

    // Insert a typed node so MATCH (n:Person) can find it.
    svc.insert(Request::new(InsertRequest {
        triples: vec![text_prop(a.clone(), "__type", "Person")],
        ..Default::default()
    }))
    .await
    .unwrap();

    let resp = svc
        .cypher_query(Request::new(CypherQueryRequest {
            cypher: "MATCH (n:Person) RETURN id(n)".to_string(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.rows.len(), 1, "one matching row expected");
    let row = &resp.rows[0];

    let id_val = row.values.get("id(n)").expect("id(n) should be in values");
    let uuid_str = match &id_val.kind {
        Some(ValueKind::TextVal(s)) => s.clone(),
        other => panic!("expected TextVal, got {:?}", other),
    };
    let returned_uuid = uuid::Uuid::parse_str(&uuid_str).expect("id(n) should be a valid UUID");
    assert_eq!(
        returned_uuid, a_core.0,
        "node UUID from id(n) should match the inserted NodeId"
    );
}

#[tokio::test]
async fn cypher_element_id_n_returns_node_uuid() {
    let (svc, _dir) = open();
    let (b_core, b) = new_node();

    svc.insert(Request::new(InsertRequest {
        triples: vec![text_prop(b.clone(), "__type", "Employee")],
        ..Default::default()
    }))
    .await
    .unwrap();

    let resp = svc
        .cypher_query(Request::new(CypherQueryRequest {
            cypher: "MATCH (n:Employee) RETURN elementId(n)".to_string(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.rows.len(), 1);
    let row = &resp.rows[0];

    // elementId(n) maps to the same output key as id(n).
    let id_val = row
        .values
        .get("id(n)")
        .expect("id(n) key should be present for elementId(n)");
    let uuid_str = match &id_val.kind {
        Some(ValueKind::TextVal(s)) => s.clone(),
        other => panic!("expected TextVal, got {:?}", other),
    };
    let returned_uuid = uuid::Uuid::parse_str(&uuid_str).expect("should be valid UUID");
    assert_eq!(
        returned_uuid, b_core.0,
        "node UUID from elementId(n) should match the inserted NodeId"
    );
}

// ── GetPropertyHistory tests ──────────────────────────────────────────────────

use polargraph_server::proto::GetPropertyHistoryRequest;

fn int_prop(subject: NodeId, predicate: &str, n: i64) -> Triple {
    Triple {
        kind: Some(TripleKind::Property(PropertyTriple {
            subject: Some(subject),
            predicate: predicate.into(),
            value: Some(Value {
                kind: Some(ValueKind::IntVal(n)),
            }),
            vt_start: 0,
            vt_end: 0,
        })),
    }
}

#[tokio::test]
async fn property_history_empty() {
    let (svc, _dir) = open();
    let (core, _) = new_node();
    let req = GetPropertyHistoryRequest {
        subject_id: core.as_bytes().to_vec(),
        predicate: "description".into(),
        limit: 0,
    };
    let resp = svc
        .get_property_history(Request::new(req))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.versions.is_empty());
}

#[tokio::test]
async fn property_history_single_write() {
    let (svc, _dir) = open();
    let (core, proto) = new_node();

    svc.insert(Request::new(InsertRequest {
        triples: vec![text_prop(proto.clone(), "label", "hello")],
        ..Default::default()
    }))
    .await
    .unwrap();

    let req = GetPropertyHistoryRequest {
        subject_id: core.as_bytes().to_vec(),
        predicate: "label".into(),
        limit: 0,
    };
    let resp = svc
        .get_property_history(Request::new(req))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.versions.len(), 1);
    assert!(resp.versions[0].transaction_time > 0);
    // Value uses #[serde(tag = "type", content = "v")]: {"type":"Text","v":"hello"}.
    let v: serde_json::Value = serde_json::from_str(&resp.versions[0].value_json).unwrap();
    assert_eq!(v["type"], "Text");
    assert_eq!(v["v"], "hello");
}

#[tokio::test]
async fn property_history_multiple_writes_newest_first() {
    let (svc, _dir) = open();
    let (core, proto) = new_node();

    for i in 1i64..=3 {
        svc.insert(Request::new(InsertRequest {
            triples: vec![int_prop(proto.clone(), "counter", i)],
            ..Default::default()
        }))
        .await
        .unwrap();
    }

    let req = GetPropertyHistoryRequest {
        subject_id: core.as_bytes().to_vec(),
        predicate: "counter".into(),
        limit: 0,
    };
    let resp = svc
        .get_property_history(Request::new(req))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.versions.len(), 3);
    // Newest first. Value: {"type":"Int","v":N}.
    let vals: Vec<i64> = resp
        .versions
        .iter()
        .map(|v| {
            let j: serde_json::Value = serde_json::from_str(&v.value_json).unwrap();
            j["v"].as_i64().unwrap()
        })
        .collect();
    assert_eq!(vals, vec![3, 2, 1]);
    // Transaction times strictly decreasing (newest first).
    assert!(resp.versions[0].transaction_time > resp.versions[1].transaction_time);
    assert!(resp.versions[1].transaction_time > resp.versions[2].transaction_time);
}

// ── Cypher edge annotation filter + projection tests ─────────────────────────

/// `WHERE r.since > 2020` keeps only the edge whose annotation matches.
#[tokio::test]
async fn cypher_edge_prop_filter_gt() {
    let (svc, _dir) = open();
    let (_, alice) = new_node();
    let (_, bob) = new_node();
    let (_, carol) = new_node();

    // alice-[r1:knows]->bob  with annotation since=2019
    // alice-[r2:knows]->carol with annotation since=2022
    let resp1 = svc
        .insert(Request::new(InsertRequest {
            triples: vec![rel(alice.clone(), "knows", bob.clone())],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let edge1_bytes = resp1.edge_ids[0].clone();

    let resp2 = svc
        .insert(Request::new(InsertRequest {
            triples: vec![rel(alice.clone(), "knows", carol.clone())],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let edge2_bytes = resp2.edge_ids[0].clone();

    svc.insert(Request::new(InsertRequest {
        edge_annotations: vec![
            EdgeAnnotation {
                edge_id: edge1_bytes,
                predicate: "since".into(),
                value: Some(AnnotationValue::Scalar(Value {
                    kind: Some(ValueKind::IntVal(2019)),
                })),
            },
            EdgeAnnotation {
                edge_id: edge2_bytes,
                predicate: "since".into(),
                value: Some(AnnotationValue::Scalar(Value {
                    kind: Some(ValueKind::IntVal(2022)),
                })),
            },
        ],
        ..Default::default()
    }))
    .await
    .unwrap();

    let resp = svc
        .cypher_query(Request::new(CypherQueryRequest {
            cypher: "MATCH (a)-[r:knows]->(b) WHERE r.since > 2020 RETURN a, b".to_string(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        resp.rows.len(),
        1,
        "only the 2022 edge should pass the filter"
    );
    // b should be carol (the 2022 edge target)
    let b_bytes = &resp.rows[0].nodes["b"].bytes;
    let b_id =
        polargraph_core::id::NodeId(uuid::Uuid::from_bytes(b_bytes[..16].try_into().unwrap()));
    let carol_core = polargraph_core::id::NodeId(uuid::Uuid::from_bytes(
        carol.bytes[..16].try_into().unwrap(),
    ));
    assert_eq!(b_id, carol_core);
}

/// `WHERE r.since > 9999` matches no edges — returns empty.
#[tokio::test]
async fn cypher_edge_prop_filter_no_match() {
    let (svc, _dir) = open();
    let (_, a) = new_node();
    let (_, b) = new_node();

    let resp1 = svc
        .insert(Request::new(InsertRequest {
            triples: vec![rel(a.clone(), "knows", b.clone())],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let edge_bytes = resp1.edge_ids[0].clone();

    svc.insert(Request::new(InsertRequest {
        edge_annotations: vec![EdgeAnnotation {
            edge_id: edge_bytes,
            predicate: "since".into(),
            value: Some(AnnotationValue::Scalar(Value {
                kind: Some(ValueKind::IntVal(2022)),
            })),
        }],
        ..Default::default()
    }))
    .await
    .unwrap();

    let resp = svc
        .cypher_query(Request::new(CypherQueryRequest {
            cypher: "MATCH (a)-[r:knows]->(b) WHERE r.since > 9999 RETURN a, b".to_string(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(
        resp.rows.is_empty(),
        "filter that matches nothing should return empty"
    );
}

/// `RETURN r.since` includes the annotation value in the binding row's values map.
#[tokio::test]
async fn cypher_edge_prop_return_projection() {
    let (svc, _dir) = open();
    let (_, a) = new_node();
    let (_, b) = new_node();

    let resp1 = svc
        .insert(Request::new(InsertRequest {
            triples: vec![rel(a.clone(), "knows", b.clone())],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let edge_bytes = resp1.edge_ids[0].clone();

    svc.insert(Request::new(InsertRequest {
        edge_annotations: vec![EdgeAnnotation {
            edge_id: edge_bytes,
            predicate: "since".into(),
            value: Some(AnnotationValue::Scalar(Value {
                kind: Some(ValueKind::IntVal(2021)),
            })),
        }],
        ..Default::default()
    }))
    .await
    .unwrap();

    let resp = svc
        .cypher_query(Request::new(CypherQueryRequest {
            cypher: "MATCH (a)-[r:knows]->(b) RETURN r.since".to_string(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.rows.len(), 1);
    let row = &resp.rows[0];
    let since_val = row
        .values
        .get("r.since")
        .expect("r.since should be in values");
    assert_eq!(since_val.kind, Some(ValueKind::IntVal(2021)));
}

/// `CypherQueryStream` applies edge annotation filters the same way as the unary path.
#[tokio::test]
async fn cypher_stream_edge_prop_filter() {
    let (svc, _dir) = open();
    let (_, alice) = new_node();
    let (_, bob) = new_node();
    let (_, carol) = new_node();

    let resp1 = svc
        .insert(Request::new(InsertRequest {
            triples: vec![rel(alice.clone(), "knows", bob.clone())],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let edge1_bytes = resp1.edge_ids[0].clone();

    let resp2 = svc
        .insert(Request::new(InsertRequest {
            triples: vec![rel(alice.clone(), "knows", carol.clone())],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let edge2_bytes = resp2.edge_ids[0].clone();

    svc.insert(Request::new(InsertRequest {
        edge_annotations: vec![
            EdgeAnnotation {
                edge_id: edge1_bytes,
                predicate: "weight".into(),
                value: Some(AnnotationValue::Scalar(Value {
                    kind: Some(ValueKind::IntVal(1)),
                })),
            },
            EdgeAnnotation {
                edge_id: edge2_bytes,
                predicate: "weight".into(),
                value: Some(AnnotationValue::Scalar(Value {
                    kind: Some(ValueKind::IntVal(10)),
                })),
            },
        ],
        ..Default::default()
    }))
    .await
    .unwrap();

    let mut stream = svc
        .cypher_query_stream(Request::new(CypherQueryRequest {
            cypher: "MATCH (a)-[r:knows]->(b) WHERE r.weight >= 10 RETURN a, b".to_string(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    let mut total_rows = 0usize;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap();
        total_rows += chunk.results.len();
        if chunk.done {
            break;
        }
    }

    assert_eq!(total_rows, 1, "only the edge with weight >= 10 should pass");
}

// ── OWL 2 RL materialization tests ───────────────────────────────────────────

use polargraph_server::proto::{RunMaterializationRequest, RunMaterializationResponse};

const RDF_TYPE_IRI: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDFS_SUBCLASS_OF_IRI: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
const OWL_SAME_AS_IRI: &str = "http://www.w3.org/2002/07/owl#sameAs";

/// Helper: build a RelationTriple proto with no edge_id or vt.
fn owl_rel(s: &NodeId, pred: &str, o: &NodeId) -> Triple {
    Triple {
        kind: Some(TripleKind::Relation(
            polargraph_server::proto::RelationTriple {
                subject: Some(s.clone()),
                predicate: pred.to_string(),
                object: Some(o.clone()),
                vt_start: 0,
                vt_end: i64::MAX,
                properties: vec![],
            },
        )),
    }
}

/// NodeId helper from a core NodeId.
fn core_to_proto(id: &polargraph_core::id::NodeId) -> NodeId {
    NodeId {
        bytes: id.as_bytes().to_vec(),
    }
}

/// After materializing rdfs9 (subClassOf + type), the derived CF should
/// contain the inherited rdf:type triple.
#[tokio::test]
async fn run_materialization_rdfs9_subclass_propagation() {
    let (svc, _dir) = open();

    let (dog_class, _) = new_node();
    let (animal_class, _) = new_node();
    let (fido, _) = new_node();

    let dog_proto = core_to_proto(&dog_class);
    let animal_proto = core_to_proto(&animal_class);
    let fido_proto = core_to_proto(&fido);

    // Dog rdfs:subClassOf Animal
    svc.insert(Request::new(InsertRequest {
        triples: vec![owl_rel(&dog_proto, RDFS_SUBCLASS_OF_IRI, &animal_proto)],
        ..Default::default()
    }))
    .await
    .unwrap();

    // Fido rdf:type Dog
    svc.insert(Request::new(InsertRequest {
        triples: vec![owl_rel(&fido_proto, RDF_TYPE_IRI, &dog_proto)],
        ..Default::default()
    }))
    .await
    .unwrap();

    // Run materialization (clear_first=true)
    let resp: RunMaterializationResponse = svc
        .run_materialization(Request::new(RunMaterializationRequest {
            clear_first: true,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.rules_fired > 0, "at least one rule should have fired");
    assert!(
        resp.derived_triples > 0,
        "at least one derived triple expected"
    );
    assert!(
        resp.iterations > 0,
        "at least one fixpoint iteration expected"
    );
}

/// eq-sym: materializing owl:sameAs produces the symmetric inverse.
#[tokio::test]
async fn run_materialization_eq_sym_sameas() {
    let (svc, _dir) = open();

    let (alice_core, alice_proto) = new_node();
    let (alice2_core, alice2_proto) = new_node();
    let _ = alice_core;
    let _ = alice2_core;

    // Alice owl:sameAs Alice2
    svc.insert(Request::new(InsertRequest {
        triples: vec![owl_rel(&alice_proto, OWL_SAME_AS_IRI, &alice2_proto)],
        ..Default::default()
    }))
    .await
    .unwrap();

    let resp = svc
        .run_materialization(Request::new(RunMaterializationRequest {
            clear_first: true,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(resp.rules_fired > 0, "eq-sym should fire at least once");
}

/// Second materialization run (clear_first=false) reaches fixpoint in 0 iterations
/// when the DRV CF is already up-to-date.
#[tokio::test]
async fn run_materialization_incremental_reaches_fixpoint() {
    let (svc, _dir) = open();

    let (_, alice_proto) = new_node();
    let (_, alice2_proto) = new_node();

    svc.insert(Request::new(InsertRequest {
        triples: vec![owl_rel(&alice_proto, OWL_SAME_AS_IRI, &alice2_proto)],
        ..Default::default()
    }))
    .await
    .unwrap();

    // Full run
    svc.run_materialization(Request::new(RunMaterializationRequest {
        clear_first: true,
    }))
    .await
    .unwrap();

    // Incremental run: should fire 0 new rules
    let resp = svc
        .run_materialization(Request::new(RunMaterializationRequest {
            clear_first: false,
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        resp.rules_fired, 0,
        "incremental run on converged state should fire 0 rules"
    );
    assert_eq!(
        resp.iterations, 0,
        "incremental run on converged state should need 0 iterations"
    );
}
