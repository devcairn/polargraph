//! int8 vector spaces over gRPC (step 9c).

use polargraph_server::{
    proto::{
        polar_graph_service_server::PolarGraphService, InsertVectorRequest, NodeId, NodeTypeDef,
        RegisterNodeTypeRequest, SearchVectorRequest, VectorSpaceDef,
    },
    service::PolarGraphServer,
};
use polargraph_storage::TripleStore;
use tempfile::TempDir;
use tonic::Request;

fn register(quantization: &str) -> RegisterNodeTypeRequest {
    RegisterNodeTypeRequest {
        definition: Some(NodeTypeDef {
            type_name: "Doc".into(),
            vector_space: Some(VectorSpaceDef {
                space_name: "docs".into(),
                dimensions: 3,
                quantization: quantization.into(),
                ..Default::default()
            }),
            ..Default::default()
        }),
    }
}

#[tokio::test]
async fn an_int8_space_is_searchable_with_exact_scores() {
    let dir = TempDir::new().unwrap();
    let svc = PolarGraphServer::new(TripleStore::open(dir.path()).unwrap()).unwrap();
    svc.register_node_type(Request::new(register("int8")))
        .await
        .unwrap();
    let ids: Vec<NodeId> = (0..3)
        .map(|_| NodeId {
            bytes: uuid::Uuid::now_v7().as_bytes().to_vec(),
        })
        .collect();
    for (id, v) in ids
        .iter()
        .zip([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.7, 0.7, 0.0]])
    {
        svc.insert_vector(Request::new(InsertVectorRequest {
            node_id: Some(id.clone()),
            vector: v.to_vec(),
            space: "docs".into(),
        }))
        .await
        .unwrap();
    }
    assert!(dir.path().join("vectors/docs.vecs").exists());
    let hits = svc
        .search_vector(Request::new(SearchVectorRequest {
            space: "docs".into(),
            query: vec![1.0, 0.0, 0.0],
            k: 1,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
        .results;
    assert_eq!(hits[0].node_id.as_ref(), Some(&ids[0]));
    assert!((hits[0].similarity - 1.0).abs() < 1e-5);
}

#[tokio::test]
async fn unknown_quantization_is_rejected() {
    let dir = TempDir::new().unwrap();
    let svc = PolarGraphServer::new(TripleStore::open(dir.path()).unwrap()).unwrap();
    let err = svc
        .register_node_type(Request::new(register("pq")))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}
