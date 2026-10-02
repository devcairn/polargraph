//! Value bindings over gRPC (docs/design/value-bindings.md): `Query` and
//! `QueryStream` return variables bound to property values in `values`.

use polargraph_server::{
    proto::{
        polar_graph_service_server::PolarGraphService, term::Kind as TermKind,
        triple::Kind as TripleKind, value::Kind as ValueKind, InsertRequest, NodeId,
        PropertyTriple, QueryRequest, RelationTriple, Term, Triple, Value, VarPattern,
    },
    service::PolarGraphServer,
};
use polargraph_storage::TripleStore;
use tempfile::TempDir;
use tokio_stream::StreamExt as _;
use tonic::Request;

fn node() -> NodeId {
    NodeId {
        bytes: uuid::Uuid::now_v7().as_bytes().to_vec(),
    }
}

fn term(kind: TermKind) -> Option<Term> {
    Some(Term { kind: Some(kind) })
}

fn var(name: &str) -> Option<Term> {
    term(TermKind::Var(name.into()))
}

fn pattern(s: Option<Term>, p: &str, o: Option<Term>) -> VarPattern {
    VarPattern {
        subject: s,
        predicate: p.into(),
        object: o,
        ..Default::default()
    }
}

fn text(v: &Value) -> &str {
    match &v.kind {
        Some(ValueKind::TextVal(s)) => s,
        other => panic!("expected text, got {other:?}"),
    }
}

async fn setup() -> (PolarGraphServer, TempDir, NodeId, NodeId) {
    let dir = TempDir::new().unwrap();
    let svc = PolarGraphServer::new(TripleStore::open(dir.path()).unwrap()).unwrap();
    let (alice, bob) = (node(), node());
    let prop = |s: &NodeId, p: &str, v: ValueKind| Triple {
        kind: Some(TripleKind::Property(PropertyTriple {
            subject: Some(s.clone()),
            predicate: p.into(),
            value: Some(Value { kind: Some(v) }),
            ..Default::default()
        })),
    };
    svc.insert(Request::new(InsertRequest {
        triples: vec![
            prop(&alice, "name", ValueKind::TextVal("Alice".into())),
            prop(&bob, "name", ValueKind::TextVal("Bob".into())),
            prop(&bob, "age", ValueKind::IntVal(41)),
            Triple {
                kind: Some(TripleKind::Relation(RelationTriple {
                    subject: Some(alice.clone()),
                    predicate: "knows".into(),
                    object: Some(bob.clone()),
                    ..Default::default()
                })),
            },
        ],
        ..Default::default()
    }))
    .await
    .unwrap();
    (svc, dir, alice, bob)
}

#[tokio::test]
async fn query_returns_value_bindings() {
    let (svc, _dir, alice, bob) = setup().await;
    let resp = svc
        .query(Request::new(QueryRequest {
            patterns: vec![
                pattern(term(TermKind::Bound(alice.clone())), "knows", var("f")),
                pattern(var("f"), "name", var("n")),
                pattern(var("f"), "age", var("a")),
            ],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.bindings.len(), 1);
    let b = &resp.bindings[0];
    assert_eq!(b.vars["f"], bob);
    assert_eq!(text(&b.values["n"]), "Bob");
    assert_eq!(b.values["a"].kind, Some(ValueKind::IntVal(41)));
    assert!(!b.vars.contains_key("n"));

    // Every name, with its subject.
    let resp = svc
        .query(Request::new(QueryRequest {
            patterns: vec![pattern(var("p"), "name", var("n"))],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let mut names: Vec<&str> = resp.bindings.iter().map(|b| text(&b.values["n"])).collect();
    names.sort();
    assert_eq!(names, ["Alice", "Bob"]);
}

#[tokio::test]
async fn query_stream_returns_value_bindings() {
    let (svc, _dir, _alice, bob) = setup().await;
    let mut stream = svc
        .query_stream(Request::new(QueryRequest {
            patterns: vec![pattern(var("p"), "age", var("a"))],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let mut rows = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap();
        rows.extend(chunk.results);
        if chunk.done {
            break;
        }
    }
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].vars["p"], bob);
    assert_eq!(rows[0].values["a"].kind, Some(ValueKind::IntVal(41)));
}
