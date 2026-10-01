//! SHACL validation over stored graphs.

use polargraph_core::{
    id::{EdgeId, GraphId},
    skolem::ImportScope,
    temporal::{BiTemporalRange, Timestamp},
    term::iri_to_node_id,
    triple::{Predicate, Triple},
};
use polargraph_shacl::{validate, DataView, Obj, Overlay, Shapes};
use polargraph_sparql::{parse_turtle, ImportedObject};
use polargraph_storage::{GraphScope, TripleStore, WriteMode};
use tempfile::TempDir;

const PREFIXES: &str = "@prefix sh: <http://www.w3.org/ns/shacl#> .\n\
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n\
    @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n\
    @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\
    @prefix ex: <http://ex/> .\n";

fn triples(ttl: &str) -> (Vec<Triple>, Vec<String>) {
    let scope = ImportScope::new("https://kb.test", "t");
    let mut out = Vec::new();
    let mut iris = Vec::new();
    for t in parse_turtle(format!("{PREFIXES}{ttl}").as_bytes()).unwrap() {
        let subject = t.subject_node_id(&scope);
        iris.push(if t.subject_is_bnode {
            scope.skolem_iri(&t.subject)
        } else {
            t.subject.clone()
        });
        iris.push(t.predicate.clone());
        let temporal = BiTemporalRange::assert_now(Timestamp(0));
        out.push(match &t.object {
            ImportedObject::Literal { value, .. } => Triple::Property {
                subject,
                predicate: Predicate::new(t.predicate.clone()),
                value: value.clone(),
                temporal,
            },
            obj => {
                iris.push(match obj {
                    ImportedObject::Iri(i) => i.clone(),
                    ImportedObject::BlankNode(b) => scope.skolem_iri(b),
                    _ => unreachable!(),
                });
                Triple::Relation {
                    subject,
                    predicate: Predicate::new(t.predicate.clone()),
                    object: obj.node_id(&scope).unwrap(),
                    edge_id: EdgeId::new(),
                    temporal,
                }
            }
        });
    }
    (out, iris)
}

fn load(store: &TripleStore, graph: &str, ttl: &str) -> GraphId {
    let g = store.create_graph(graph, &[]).unwrap();
    let (ts, iris) = triples(ttl);
    let mut tx = store.begin();
    for t in ts {
        tx.insert_in(t, g, WriteMode::Add);
    }
    for i in iris {
        tx.bind_iri(i);
    }
    tx.commit().unwrap();
    g
}

const SHAPES: &str = r#"
ex:ServiceShape a sh:NodeShape ;
    sh:targetClass ex:Service ;
    sh:property [
        sh:path ex:owner ; sh:minCount 1 ; sh:maxCount 1 ; sh:datatype xsd:string ;
        sh:pattern "^team-" ; sh:maxLength 12
    ] ;
    sh:property [
        sh:path ex:tier ; sh:in ( "gold" "silver" ) ; sh:severity sh:Warning
    ] ;
    sh:property [ sh:path ex:dependsOn ; sh:class ex:Service ; sh:nodeKind sh:IRI ] ;
    sh:property [ sh:path ex:replicas ; sh:datatype xsd:integer ; sh:minInclusive 1 ; sh:maxExclusive 10 ] ;
    sh:property [ sh:path ( ex:dependsOn ex:owner ) ; sh:minLength 6 ] ;
    sh:property [ sh:path [ sh:inversePath ex:dependsOn ] ; sh:maxCount 2 ] ;
    sh:property [ sh:path ex:contact ; sh:node ex:ContactShape ] .

ex:ContactShape a sh:NodeShape ;
    sh:property [ sh:path ex:email ; sh:minCount 1 ; sh:pattern "@" ] ;
    sh:closed true ; sh:ignoredProperties ( rdf:type ) .

ex:Critical rdfs:subClassOf ex:Service .
"#;

fn run(
    store: &TripleStore,
    shapes_g: GraphId,
    data: &[GraphId],
    overlay: &Overlay,
    focus: bool,
) -> Vec<(String, String, String)> {
    let snap = store.snapshot(Timestamp(store.oracle_ts()));
    let shapes_view = DataView::new(&snap, GraphScope::One(shapes_g), &Overlay::default());
    let shapes = Shapes::load(&shapes_view).unwrap();
    let view = DataView::new(&snap, GraphScope::set(data.to_vec()), overlay);
    let touched = view.touched();
    let report = validate(&shapes, &view, focus.then_some(&touched)).unwrap();
    let mut out: Vec<_> = report
        .results
        .iter()
        .map(|r| {
            let focus = match &r.focus_node {
                Obj::Node(n) => view.iri(n).unwrap(),
                Obj::Lit(v) => format!("{v:?}"),
            };
            (focus, r.component.to_string(), format!("{:?}", r.severity))
        })
        .collect();
    out.sort();
    out
}

#[test]
fn core_constraints() {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    let shapes_g = load(&store, "urn:shapes", SHAPES);
    // Class axioms live with the data (rdfs:subClassOf is read from the dataset).
    let data = load(
        &store,
        "urn:data",
        r#"
        ex:Critical rdfs:subClassOf ex:Service .
        ex:api a ex:Service ; ex:owner "team-api" ; ex:tier "gold" ; ex:replicas 3 ;
            ex:dependsOn ex:db ; ex:contact ex:c1 .
        ex:db a ex:Critical ; ex:owner "team-db" ; ex:replicas 2 .
        ex:c1 ex:email "a@x" .

        ex:bad a ex:Service ; ex:owner "ops" , "team-x" ; ex:tier "bronze" ; ex:replicas 10 ;
            ex:dependsOn ex:thing ; ex:contact ex:c2 .
        ex:thing ex:label "not a service" .
        ex:c2 ex:phone "123" .
        "#,
    );
    let results = run(&store, shapes_g, &[data], &Overlay::default(), false);
    let comps: Vec<_> = results
        .iter()
        .filter(|r| r.0 == "http://ex/bad")
        .map(|r| (r.1.as_str(), r.2.as_str()))
        .collect();
    assert!(
        results.iter().all(|r| r.0 == "http://ex/bad"),
        "valid nodes conform: {results:?}"
    );
    for expected in [
        ("MaxCountConstraintComponent", "Violation"),
        ("PatternConstraintComponent", "Violation"),
        ("InConstraintComponent", "Warning"),
        ("ClassConstraintComponent", "Violation"),
        ("MaxExclusiveConstraintComponent", "Violation"),
        ("NodeConstraintComponent", "Violation"),
    ] {
        assert!(
            comps.contains(&expected),
            "missing {expected:?} in {comps:?}"
        );
    }
    // ex:db is a ex:Critical (subclass) and validated as a Service.
    let snap = store.snapshot(Timestamp(store.oracle_ts()));
    let view = DataView::new(&snap, GraphScope::One(data), &Overlay::default());
    assert!(view
        .types(&iri_to_node_id("http://ex/db"))
        .unwrap()
        .contains(&iri_to_node_id("http://ex/Service")));
}

#[test]
fn overlay_validates_changes_before_commit() {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    let shapes_g = load(&store, "urn:shapes", SHAPES);
    let data = load(
        &store,
        "urn:data",
        r#"ex:api a ex:Service ; ex:owner "team-api" .
           ex:old a ex:Service ."#, // already invalid: no owner
    );

    // Retract the owner and add a bad one: only touched nodes are validated.
    let (adds, _) = triples(r#"ex:api ex:owner "nope" ."#);
    let overlay = Overlay {
        adds,
        retractions: vec![(
            data,
            iri_to_node_id("http://ex/api"),
            "http://ex/owner".into(),
            Obj::Lit(polargraph_core::value::Value::Text("team-api".into())),
        )],
    };
    let results = run(&store, shapes_g, &[data], &overlay, true);
    assert_eq!(
        results,
        vec![(
            "http://ex/api".to_string(),
            "PatternConstraintComponent".to_string(),
            "Violation".to_string()
        )],
        "ex:old's existing violation isn't reported for a scoped validation"
    );

    // Full validation of the same view sees ex:old too.
    let all = run(&store, shapes_g, &[data], &overlay, false);
    assert!(all
        .iter()
        .any(|r| r.0 == "http://ex/old" && r.1 == "MinCountConstraintComponent"));

    // The overlay wasn't committed.
    let committed = run(&store, shapes_g, &[data], &Overlay::default(), false);
    assert!(committed.iter().all(|r| r.0 != "http://ex/api"));
}
