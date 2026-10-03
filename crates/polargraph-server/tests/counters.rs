//! Counters over gRPC (step 9d).

use polargraph_server::{
    proto::{
        polar_graph_service_server::PolarGraphService, CounterIncrement, GetCountersRequest,
        IncrementCountersRequest, NodeId,
    },
    service::PolarGraphServer,
};
use polargraph_storage::TripleStore;
use tempfile::TempDir;
use tonic::Request;

fn node() -> NodeId {
    NodeId {
        bytes: uuid::Uuid::now_v7().as_bytes().to_vec(),
    }
}

#[tokio::test]
async fn counters_increment_and_read_for_services_only() {
    let dir = TempDir::new().unwrap();
    let svc = PolarGraphServer::new(TripleStore::open(dir.path()).unwrap()).unwrap();
    let (a, b) = (node(), node());
    for delta in [2, 3] {
        svc.increment_counters(Request::new(IncrementCountersRequest {
            namespace: "packed".into(),
            increments: vec![CounterIncrement {
                node: Some(a.clone()),
                delta,
            }],
            ..Default::default()
        }))
        .await
        .unwrap();
    }
    let values = svc
        .get_counters(Request::new(GetCountersRequest {
            namespace: "packed".into(),
            nodes: vec![a.clone(), b],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
        .values;
    assert_eq!(values, vec![5, 0]);

    let err = svc
        .get_counters(Request::new(GetCountersRequest {
            namespace: "packed".into(),
            nodes: vec![a],
            user_id: uuid::Uuid::now_v7().to_string(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}
