//! Vocabulary RPCs, runtime type registration and the legacy conversion
//! (docs/design/cypher-rdf.md §2.4–2.5), against one running server — no
//! restart or reload between steps.

use polargraph_core::{
    id::{EdgeId, NodeId as CoreNodeId},
    temporal::{BiTemporalRange, Timestamp},
    term::iri_to_node_id,
    triple::{Predicate, Triple as CTriple},
    value::Value as CValue,
};
use polargraph_server::{
    proto::{
        polar_graph_service_server::PolarGraphService, search_vector_filtered_request::Filter,
        term::Kind as TermKind, triple::Kind as TripleKind, value::Kind as ValueKind, ChangeEvent,
        ConvertLegacyDataRequest, CypherQueryRequest, CypherWriteRequest, EdgeTypeDef, FieldDef,
        GetVocabularyRequest, InsertRequest, InsertVectorRequest, NodeId, NodeTypeDef,
        NodeTypeFilter, PutPrefixRequest, QueryRequest, RegisterEdgeTypeRequest,
        RegisterNodeTypeRequest, RelationTriple, RemovePrefixRequest, SearchVectorFilteredRequest,
        SetVocabularyBaseRequest, ShowStatsRequest, SubscribeRequest, Term, Triple,
        ValidateShapesRequest, VarPattern,
    },
    service::PolarGraphServer,
};
use polargraph_storage::{TripleStore, WriteMode};
use tempfile::TempDir;
use tokio_stream::StreamExt as _;
use tonic::Request;

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const SH: &str = "http://www.w3.org/ns/shacl#";

fn proto_node(id: CoreNodeId) -> NodeId {
    NodeId {
        bytes: id.as_bytes().to_vec(),
    }
}

fn core_node(id: &NodeId) -> CoreNodeId {
    CoreNodeId(uuid::Uuid::from_slice(&id.bytes).unwrap())
}

async fn cypher_rows(
    svc: &PolarGraphServer,
    q: &str,
) -> Vec<polargraph_server::proto::CypherBinding> {
    svc.cypher_query(Request::new(CypherQueryRequest {
        cypher: q.into(),
        ..Default::default()
    }))
    .await
    .unwrap()
    .into_inner()
    .rows
}

async fn cypher_write(svc: &PolarGraphServer, q: &str) -> Vec<CoreNodeId> {
    svc.cypher_write(Request::new(CypherWriteRequest {
        cypher: q.into(),
        ..Default::default()
    }))
    .await
    .unwrap()
    .into_inner()
    .created_node_ids
    .iter()
    .map(|b| CoreNodeId(uuid::Uuid::from_slice(b).unwrap()))
    .collect()
}

fn text(v: &polargraph_server::proto::Value) -> Option<&str> {
    match &v.kind {
        Some(ValueKind::TextVal(s)) => Some(s),
        _ => None,
    }
}

/// Instances of `class` via the Query RPC — the pattern SPARQL
/// `SELECT ?w WHERE { ?w a <class> }` translates to.
async fn instances(svc: &PolarGraphServer, class: &str) -> Vec<CoreNodeId> {
    let resp = svc
        .query(Request::new(QueryRequest {
            patterns: vec![VarPattern {
                subject: Some(Term {
                    kind: Some(TermKind::Var("w".into())),
                }),
                predicate: RDF_TYPE.into(),
                object: Some(Term {
                    kind: Some(TermKind::Bound(proto_node(iri_to_node_id(class)))),
                }),
                ..Default::default()
            }],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    resp.bindings
        .iter()
        .map(|b| core_node(&b.vars["w"]))
        .collect()
}

/// Write `(s, p, o)` rows (o an IRI or a literal) into `graph`, recording IRIs.
fn write_rdf(store: &TripleStore, graph: &str, rows: Vec<(&str, &str, Result<&str, CValue>)>) {
    let g = store.create_graph(graph, &[]).unwrap();
    let mut tx = store.begin();
    let now = BiTemporalRange::assert_now(Timestamp(0));
    for (s, p, o) in rows {
        tx.bind_iri(s);
        tx.bind_iri(p);
        let t = match o {
            Ok(iri) => {
                tx.bind_iri(iri);
                CTriple::Relation {
                    subject: iri_to_node_id(s),
                    predicate: Predicate::new(p),
                    object: iri_to_node_id(iri),
                    edge_id: EdgeId::new(),
                    temporal: now,
                }
            }
            Err(v) => CTriple::Property {
                subject: iri_to_node_id(s),
                predicate: Predicate::new(p),
                value: v,
                temporal: now,
            },
        };
        tx.insert_in(t, g, WriteMode::Add);
    }
    tx.commit().unwrap();
}

#[tokio::test]
async fn new_types_and_prefixes_are_usable_at_runtime() {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    let svc = PolarGraphServer::new(store.clone()).unwrap();

    // A prefix and a type registered on the running server.
    let vocab = svc
        .put_prefix(Request::new(PutPrefixRequest {
            name: "ex".into(),
            namespace: "http://ex/".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(vocab.prefixes.len(), 1);
    // Resume point for the Subscribe check below (after a real commit).
    let start = Timestamp(store.oracle_ts());
    svc.register_node_type(Request::new(RegisterNodeTypeRequest {
        definition: Some(NodeTypeDef {
            type_name: "ex:Widget".into(),
            fields: vec![FieldDef {
                field_name: "name".into(),
                kind: "text".into(),
                required: true,
            }],
            ..Default::default()
        }),
    }))
    .await
    .unwrap();
    svc.register_edge_type(Request::new(RegisterEdgeTypeRequest {
        definition: Some(EdgeTypeDef {
            predicate: "ex:partOf".into(),
            domain: "ex:Widget".into(),
            range: "ex:Widget".into(),
            ..Default::default()
        }),
    }))
    .await
    .unwrap();

    // (1) Cypher: create and match by the prefixed label.
    let w1 = cypher_write(&svc, r#"CREATE (w:`ex:Widget` {name: "w1"})"#).await[0];
    let w0 = cypher_write(&svc, r#"MERGE (w:`ex:Widget` {name: "w0"})"#).await[0];
    // The edge, written with the full IRI the CURIE names.
    svc.insert(Request::new(InsertRequest {
        triples: vec![Triple {
            kind: Some(TripleKind::Relation(RelationTriple {
                subject: Some(proto_node(w1)),
                predicate: "http://ex/partOf".into(),
                object: Some(proto_node(w0)),
                ..Default::default()
            })),
        }],
        ..Default::default()
    }))
    .await
    .unwrap();
    // MERGE finds the existing widget by label + name.
    assert!(cypher_write(&svc, r#"MERGE (w:`ex:Widget` {name: "w0"})"#)
        .await
        .is_empty());
    let rows = cypher_rows(&svc, "MATCH (w:`ex:Widget`) RETURN w.name").await;
    let mut names: Vec<_> = rows
        .iter()
        .filter_map(|r| r.values.get("w.name").and_then(text))
        .collect();
    names.sort();
    assert_eq!(names, ["w0", "w1"]);
    let rows = cypher_rows(
        &svc,
        "MATCH (a:`ex:Widget`)-[:`ex:partOf`]->(b:`ex:Widget`) RETURN a, b",
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(core_node(&rows[0].nodes["a"]), w1);
    // The full IRI names the same class.
    assert_eq!(
        cypher_rows(&svc, "MATCH (w:`http://ex/Widget`) RETURN w")
            .await
            .len(),
        2
    );

    // (2) The SPARQL pattern sees the same instances.
    let mut found = instances(&svc, "http://ex/Widget").await;
    found.sort_by_key(|n| *n.as_bytes());
    let mut expected = vec![w0, w1];
    expected.sort_by_key(|n| *n.as_bytes());
    assert_eq!(found, expected);

    // (5) A class first introduced by a plain rdf:type insert.
    let gadget = CoreNodeId::new();
    svc.insert(Request::new(InsertRequest {
        triples: vec![Triple {
            kind: Some(TripleKind::Relation(RelationTriple {
                subject: Some(proto_node(gadget)),
                predicate: RDF_TYPE.into(),
                object: Some(proto_node(iri_to_node_id("http://ex/Gadget"))),
                ..Default::default()
            })),
        }],
        iris: vec!["http://ex/Gadget".into()],
        ..Default::default()
    }))
    .await
    .unwrap();
    let rows = cypher_rows(&svc, "MATCH (g:`ex:Gadget`) RETURN g").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(core_node(&rows[0].nodes["g"]), gadget);

    // (3) SHACL: a shape targeting ex:Widget validates Cypher-written data.
    let shape = "http://ex/WidgetShape";
    let name_shape = "http://ex/WidgetName";
    write_rdf(
        &store,
        "urn:shapes",
        vec![
            (shape, RDF_TYPE, Ok(&format!("{SH}NodeShape"))),
            (shape, &format!("{SH}targetClass"), Ok("http://ex/Widget")),
            (shape, &format!("{SH}property"), Ok(name_shape)),
            (name_shape, &format!("{SH}path"), Ok("urn:pg:vocab:name")),
            (name_shape, &format!("{SH}minCount"), Err(CValue::Int(1))),
        ],
    );
    let validate = || async {
        svc.validate_shapes(Request::new(ValidateShapesRequest {
            shapes_graphs: vec!["urn:shapes".into()],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
    };
    assert!(validate().await.conforms, "both widgets have a name");
    cypher_write(&svc, "CREATE (w:`ex:Widget`)").await;
    let report = validate().await;
    assert!(!report.conforms, "a nameless widget violates the shape");
    assert_eq!(report.results.len(), 1);

    // (4) Typed vector filter and Subscribe types see the new class at once.
    for (node, v) in [(w1, vec![1.0_f32, 0.0]), (gadget, vec![0.9, 0.1])] {
        svc.insert_vector(Request::new(InsertVectorRequest {
            node_id: Some(proto_node(node)),
            vector: v,
            space: "default".into(),
        }))
        .await
        .unwrap();
    }
    let hits = svc
        .search_vector_filtered(Request::new(SearchVectorFilteredRequest {
            space: "default".into(),
            query: vec![1.0, 0.0],
            k: 5,
            filter: Some(Filter::NodeTypeFilter(NodeTypeFilter {
                type_name: "ex:Gadget".into(),
            })),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
        .results;
    assert_eq!(hits.len(), 1);
    assert_eq!(core_node(hits[0].node_id.as_ref().unwrap()), gadget);

    let mut events = svc
        .subscribe(Request::new(SubscribeRequest {
            types: vec!["ex:Gadget".into()],
            resume_after_ts: start.0,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let first: ChangeEvent = tokio::time::timeout(std::time::Duration::from_secs(5), events.next())
        .await
        .expect("an event for the gadget")
        .unwrap()
        .unwrap();
    let quad = first.quad.unwrap();
    let Some(TripleKind::Relation(r)) = quad.kind else {
        panic!("expected the rdf:type relation")
    };
    assert_eq!(core_node(r.subject.as_ref().unwrap()), gadget);

    // Removing a prefix takes effect immediately too.
    svc.remove_prefix(Request::new(RemovePrefixRequest {
        name: "ex".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    assert!(cypher_rows(&svc, "MATCH (g:`ex:Gadget`) RETURN g")
        .await
        .is_empty());
}

#[tokio::test]
async fn vocabulary_changes_are_service_only_and_validated() {
    let dir = TempDir::new().unwrap();
    let svc = PolarGraphServer::new(TripleStore::open(dir.path()).unwrap()).unwrap();

    let err = svc
        .put_prefix(Request::new(PutPrefixRequest {
            name: "ex".into(),
            namespace: "http://ex/".into(),
            user_id: uuid::Uuid::now_v7().to_string(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);

    for (name, ns) in [
        ("1x", "http://ex/"),
        ("ex", "not an iri"),
        ("http", "http://ex/"),
    ] {
        let err = svc
            .put_prefix(Request::new(PutPrefixRequest {
                name: name.into(),
                namespace: ns.into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{name} {ns}");
    }
    let err = svc
        .set_vocabulary_base(Request::new(SetVocabularyBaseRequest {
            base: "relative/".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    let v = svc
        .get_vocabulary(Request::new(GetVocabularyRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(v.base, "urn:pg:vocab:");
    assert!(!v.legacy.unwrap().conversion_pending);
}

#[tokio::test]
async fn legacy_data_is_converted_on_demand() {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();

    // Data as a pre-vocabulary server stored it: bare predicates, __type labels.
    store.set_canonical_predicate_names(false);
    let (alice, bob) = (CoreNodeId::new(), CoreNodeId::new());
    let now = BiTemporalRange::assert_now(Timestamp::now());
    let mut tx = store.begin();
    for (s, label, name) in [(alice, "Person", "Alice"), (bob, "Person", "Bob")] {
        tx.insert(CTriple::Property {
            subject: s,
            predicate: Predicate::new("__type"),
            value: CValue::Text(label.into()),
            temporal: now,
        });
        tx.insert(CTriple::Property {
            subject: s,
            predicate: Predicate::new("name"),
            value: CValue::Text(name.into()),
            temporal: now,
        });
    }
    tx.insert(CTriple::Relation {
        subject: alice,
        predicate: Predicate::new("knows"),
        object: bob,
        edge_id: EdgeId::new(),
        temporal: now,
    });
    tx.commit().unwrap();
    store.set_canonical_predicate_names(true);

    let svc = PolarGraphServer::new(store.clone()).unwrap();

    // Pending state is visible.
    let legacy = svc
        .get_vocabulary(Request::new(GetVocabularyRequest {}))
        .await
        .unwrap()
        .into_inner()
        .legacy
        .unwrap();
    assert!(legacy.conversion_pending);
    assert_eq!(legacy.type_labels, 2);
    let mut bare = legacy.bare_predicates.clone();
    bare.sort();
    assert_eq!(bare, ["knows", "name"]);
    let stats = svc
        .show_stats(Request::new(ShowStatsRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert!(stats.legacy_conversion_pending);
    assert_eq!(stats.legacy_type_labels, 2);

    // The window: legacy data isn't reachable by bare name or label yet.
    assert!(cypher_rows(&svc, "MATCH (p:Person) RETURN p")
        .await
        .is_empty());

    // The operator sets the base, then converts. A dry run changes nothing.
    svc.set_vocabulary_base(Request::new(SetVocabularyBaseRequest {
        base: "http://ex/ns/".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    let convert = |dry_run| {
        let svc = &svc;
        async move {
            svc.convert_legacy_data(Request::new(ConvertLegacyDataRequest {
                dry_run,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner()
        }
    };
    let dry = convert(true).await;
    assert!(dry.dry_run);
    assert_eq!(dry.labels_converted, 2);
    assert_eq!(dry.predicates.len(), 2);
    assert!(dry.legacy.unwrap().conversion_pending);
    assert!(cypher_rows(&svc, "MATCH (p:Person) RETURN p")
        .await
        .is_empty());

    let done = convert(false).await;
    assert_eq!(done.labels_converted, 2);
    let mut renamed: Vec<_> = done
        .predicates
        .iter()
        .map(|p| (p.from.as_str(), p.to.as_str(), p.merged))
        .collect();
    renamed.sort();
    assert_eq!(
        renamed,
        [
            ("knows", "http://ex/ns/knows", false),
            ("name", "http://ex/ns/name", false)
        ]
    );
    assert!(!done.legacy.unwrap().conversion_pending);

    // Everything is reachable by bare name again, under the new base.
    let rows = cypher_rows(
        &svc,
        "MATCH (a:Person)-[:knows]->(b:Person) RETURN a.name, b.name",
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(text(&rows[0].values["a.name"]), Some("Alice"));
    assert_eq!(text(&rows[0].values["b.name"]), Some("Bob"));
    assert_eq!(instances(&svc, "http://ex/ns/Person").await.len(), 2);
    assert!(
        !svc.show_stats(Request::new(ShowStatsRequest {}))
            .await
            .unwrap()
            .into_inner()
            .legacy_conversion_pending
    );

    // Idempotent: a second run has nothing to do.
    let again = convert(false).await;
    assert_eq!(again.labels_converted, 0);
    assert!(again.predicates.is_empty());
}

#[tokio::test]
async fn cypher_write_is_deprecated_but_still_works() {
    let dir = TempDir::new().unwrap();
    let svc = PolarGraphServer::new(TripleStore::open(dir.path()).unwrap()).unwrap();
    let resp = svc
        .cypher_write(Request::new(CypherWriteRequest {
            cypher: r#"CREATE (a:Person {name: "Alice"})"#.into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    let warning = resp.metadata().get("warning").unwrap().to_str().unwrap();
    assert!(warning.starts_with("299 ") && warning.contains("ApplyChanges"));
    assert_eq!(resp.into_inner().created_node_ids.len(), 1);
    assert_eq!(
        cypher_rows(&svc, "MATCH (a:Person) RETURN a").await.len(),
        1
    );
}

#[tokio::test]
async fn relations_can_name_their_object_by_iri() {
    use polargraph_server::proto::{ApplyChangesRequest, GraphTriples};

    let dir = TempDir::new().unwrap();
    let svc = PolarGraphServer::new(TripleStore::open(dir.path()).unwrap()).unwrap();
    svc.put_prefix(Request::new(PutPrefixRequest {
        name: "ex".into(),
        namespace: "http://ex/".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    let typed = |node: CoreNodeId, class: &str| Triple {
        kind: Some(TripleKind::Relation(RelationTriple {
            subject: Some(proto_node(node)),
            predicate: RDF_TYPE.into(),
            object_iri: class.into(),
            ..Default::default()
        })),
    };

    // Insert: a bare class name and a prefixed one.
    let (a, b) = (CoreNodeId::new(), CoreNodeId::new());
    svc.insert(Request::new(InsertRequest {
        triples: vec![typed(a, "Person"), typed(b, "ex:Widget")],
        ..Default::default()
    }))
    .await
    .unwrap();
    // ApplyChanges: a full IRI.
    let c = CoreNodeId::new();
    svc.apply_changes(Request::new(ApplyChangesRequest {
        adds: vec![GraphTriples {
            graph: String::new(),
            triples: vec![typed(c, "http://ex/Widget")],
        }],
        ..Default::default()
    }))
    .await
    .unwrap();

    assert_eq!(
        cypher_rows(&svc, "MATCH (p:Person) RETURN p").await.len(),
        1
    );
    assert_eq!(instances(&svc, "http://ex/Widget").await.len(), 2);
    // The class IRIs were recorded in the IRI dictionary.
    let iris = svc
        .resolve_iris(Request::new(polargraph_server::proto::ResolveIrisRequest {
            nodes: vec![proto_node(iri_to_node_id("http://ex/Widget"))],
        }))
        .await
        .unwrap()
        .into_inner()
        .iris;
    assert_eq!(iris, ["http://ex/Widget"]);

    // A conflicting object and object_iri is rejected.
    let mut bad = typed(a, "Person");
    if let Some(TripleKind::Relation(r)) = &mut bad.kind {
        r.object = Some(proto_node(b));
    }
    let err = svc
        .insert(Request::new(InsertRequest {
            triples: vec![bad],
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}
