//! Queryable inference over gRPC (docs/design/step9-inference-vectors-stats.md).

use polargraph_server::{
    proto::{
        polar_graph_service_server::PolarGraphService, term::Kind as TermKind,
        triple::Kind as TripleKind, CreateGraphRequest, CypherQueryRequest, InsertRequest, NodeId,
        QueryRequest, RelationTriple, RunMaterializationRequest, Term, Triple, VarPattern,
    },
    service::PolarGraphServer,
};
use polargraph_storage::TripleStore;
use tempfile::TempDir;
use tonic::Request;

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const SUBCLASS: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";

fn node(iri: &str) -> NodeId {
    NodeId {
        bytes: polargraph_core::term::iri_to_node_id(iri)
            .as_bytes()
            .to_vec(),
    }
}

fn rel(s: &str, p: &str, o: &str) -> Triple {
    Triple {
        kind: Some(TripleKind::Relation(RelationTriple {
            subject: Some(node(s)),
            predicate: p.into(),
            object: Some(node(o)),
            ..Default::default()
        })),
    }
}

async fn setup() -> (PolarGraphServer, TempDir) {
    let dir = TempDir::new().unwrap();
    let svc = PolarGraphServer::new(TripleStore::open(dir.path()).unwrap()).unwrap();
    svc.insert(Request::new(InsertRequest {
        triples: vec![
            rel("urn:pg:vocab:Cat", SUBCLASS, "urn:pg:vocab:Animal"),
            rel("http://ex/tom", RDF_TYPE, "urn:pg:vocab:Cat"),
        ],
        iris: vec!["urn:pg:vocab:Cat".into(), "urn:pg:vocab:Animal".into()],
        ..Default::default()
    }))
    .await
    .unwrap();
    let r = svc
        .run_materialization(Request::new(RunMaterializationRequest::default()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.asserted, 1);
    (svc, dir)
}

fn animals(exclude_inferred: bool) -> QueryRequest {
    QueryRequest {
        patterns: vec![VarPattern {
            subject: Some(Term {
                kind: Some(TermKind::Var("a".into())),
            }),
            predicate: RDF_TYPE.into(),
            object: Some(Term {
                kind: Some(TermKind::Bound(node("urn:pg:vocab:Animal"))),
            }),
            ..Default::default()
        }],
        exclude_inferred,
        ..Default::default()
    }
}

#[tokio::test]
async fn inferred_facts_are_queryable_and_can_be_excluded() {
    let (svc, _dir) = setup().await;
    let with = svc
        .query(Request::new(animals(false)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(with.bindings.len(), 1);
    assert_eq!(with.bindings[0].vars["a"], node("http://ex/tom"));
    let without = svc
        .query(Request::new(animals(true)))
        .await
        .unwrap()
        .into_inner();
    assert!(without.bindings.is_empty());

    // Cypher labels see subclass instances (rdf:type via OWL RL).
    let rows = |exclude_inferred| CypherQueryRequest {
        cypher: "MATCH (a:Animal) RETURN a".into(),
        exclude_inferred,
        ..Default::default()
    };
    let r = svc
        .cypher_query(Request::new(rows(false)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.rows.len(), 1);
    let r = svc
        .cypher_query(Request::new(rows(true)))
        .await
        .unwrap()
        .into_inner();
    assert!(r.rows.is_empty());
}

#[tokio::test]
async fn inferred_graphs_are_written_by_inference_only() {
    let (svc, _dir) = setup().await;
    let err = svc
        .insert(Request::new(InsertRequest {
            triples: vec![rel("http://ex/x", RDF_TYPE, "http://ex/Y")],
            graph: "urn:pg:inferred:default".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    let err = svc
        .create_graph(Request::new(CreateGraphRequest {
            iri: "urn:pg:inferred:urn:mine".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn the_inference_task_keeps_inferred_graphs_current() {
    let (svc, _dir) = setup().await;
    let token = tokio_util::sync::CancellationToken::new();
    svc.spawn_inference_task(token.clone());
    // A new subclass instance, with no explicit materialization call.
    svc.insert(Request::new(InsertRequest {
        triples: vec![rel("http://ex/felix", RDF_TYPE, "urn:pg:vocab:Cat")],
        ..Default::default()
    }))
    .await
    .unwrap();
    let mut seen = 0;
    for _ in 0..50 {
        seen = svc
            .query(Request::new(animals(false)))
            .await
            .unwrap()
            .into_inner()
            .bindings
            .len();
        if seen == 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    token.cancel();
    assert_eq!(seen, 2, "tom and felix are animals within a few seconds");
}
