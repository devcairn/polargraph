//! End-to-end gRPC throughput/latency benchmarks for polargraphd.
//!
//! Unlike `polargraph-storage/benches/storage.rs` (storage-layer micro-benchmarks),
//! these drive a real `PolarGraphServer` over a loopback TCP gRPC connection using
//! the generated client stub, so the numbers include serialization, transport, and
//! the full request-handling path — representative of what a real client sees.
//!
//! Run with:
//!   cargo bench -p polargraph-server --bench throughput 2>&1 | tee /tmp/bench_results.txt
//!
//! Point-lookup latency percentiles (p50/p95/p99) are computed separately with
//! `hdrhistogram` and printed to stdout, since Criterion's own summary only
//! reports mean/median/stddev.

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use hdrhistogram::Histogram;
use polargraph_server::{
    proto::{
        polar_graph_service_client::PolarGraphServiceClient,
        polar_graph_service_server::PolarGraphServiceServer, term::Kind as TermKind, NodeId as PNodeId,
        PropertyTriple, QueryRequest, Term, Triple, Value, VarPattern,
    },
    service::PolarGraphServer,
};
use polargraph_storage::TripleStore;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tonic::transport::{Channel, Server};

// ── server/client bootstrap (mirrors crates/polargraph-server/tests/grpc.rs) ──

async fn start_server(store: TripleStore) -> (std::net::SocketAddr, tokio::sync::oneshot::Sender<()>) {
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

    tokio::time::sleep(Duration::from_millis(20)).await;
    (addr, shutdown_tx)
}

async fn connect(addr: std::net::SocketAddr) -> PolarGraphServiceClient<Channel> {
    PolarGraphServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap()
}

fn new_node_id() -> PNodeId {
    PNodeId {
        bytes: polargraph_core::id::NodeId::new().as_bytes().to_vec(),
    }
}

fn name_property(subject: PNodeId, text: &str) -> Triple {
    Triple {
        kind: Some(polargraph_server::proto::triple::Kind::Property(PropertyTriple {
            subject: Some(subject),
            predicate: "name".into(),
            value: Some(Value {
                kind: Some(polargraph_server::proto::value::Kind::TextVal(text.into())),
            }),
            vt_start: 0,
            vt_end: 0,
        })),
    }
}

fn versioned_property(subject: PNodeId, text: &str, vt_start: i64, vt_end: i64) -> Triple {
    Triple {
        kind: Some(polargraph_server::proto::triple::Kind::Property(PropertyTriple {
            subject: Some(subject),
            predicate: "status".into(),
            value: Some(Value {
                kind: Some(polargraph_server::proto::value::Kind::TextVal(text.into())),
            }),
            vt_start,
            vt_end,
        })),
    }
}

fn bound(id: &PNodeId) -> Term {
    Term {
        kind: Some(TermKind::Bound(id.clone())),
    }
}

fn any() -> Term {
    Term { kind: None }
}

// ── 1. single node write throughput ────────────────────────────────────────────

fn bench_single_node_write(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (store, _dir, addr, _shutdown) = rt.block_on(async {
        let dir = TempDir::new().unwrap();
        let store = TripleStore::open(dir.path()).unwrap();
        let (addr, shutdown) = start_server(store.clone()).await;
        (store, dir, addr, shutdown)
    });
    let client = rt.block_on(connect(addr));
    drop(store);

    let mut group = c.benchmark_group("single_node_write");
    group.throughput(Throughput::Elements(1));
    group.sample_size(50);
    group.bench_function("create_node", |b| {
        b.to_async(&rt).iter_batched(
            || {
                let id = new_node_id();
                polargraph_server::proto::InsertRequest {
                    triples: vec![name_property(id, "bench-node")],
                    ..Default::default()
                }
            },
            |req| {
                let mut client = client.clone();
                async move {
                    client.insert(req).await.unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

// ── 2. point lookup latency (p50/p95/p99 via hdrhistogram) ────────────────────

fn bench_point_lookup(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    const N: usize = 10_000;

    let (_dir, addr, ids) = rt.block_on(async {
        let dir = TempDir::new().unwrap();
        let store = TripleStore::open(dir.path()).unwrap();
        let (addr, _shutdown) = start_server(store.clone()).await;
        // Leak the shutdown sender so the server stays alive for the whole run.
        std::mem::forget(_shutdown);
        let mut client = connect(addr).await;

        let mut ids = Vec::with_capacity(N);
        for i in 0..N {
            let id = new_node_id();
            client
                .insert(polargraph_server::proto::InsertRequest {
                    triples: vec![name_property(id.clone(), &format!("node-{i}"))],
                    ..Default::default()
                })
                .await
                .unwrap();
            ids.push(id);
        }
        (dir, addr, ids)
    });

    let client = rt.block_on(connect(addr));

    let mut group = c.benchmark_group("point_lookup");
    group.throughput(Throughput::Elements(1));
    group.sample_size(50);

    let ids = std::sync::Arc::new(ids);
    let idx_counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let histogram = std::sync::Arc::new(std::sync::Mutex::new(Histogram::<u64>::new(3).unwrap()));

    group.bench_function("match_by_id", |b| {
        let ids = std::sync::Arc::clone(&ids);
        let idx_counter = std::sync::Arc::clone(&idx_counter);
        let histogram = std::sync::Arc::clone(&histogram);
        let client = client.clone();
        b.to_async(&rt).iter_custom(move |iters| {
            let ids = std::sync::Arc::clone(&ids);
            let idx_counter = std::sync::Arc::clone(&idx_counter);
            let histogram = std::sync::Arc::clone(&histogram);
            let mut client = client.clone();
            async move {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let idx = idx_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let id = ids[idx % ids.len()].clone();
                    let req = QueryRequest {
                        patterns: vec![VarPattern {
                            subject: Some(bound(&id)),
                            predicate: "name".into(),
                            object: Some(any()),
                            predicate_var: String::new(),
                        }],
                        ..Default::default()
                    };
                    let start = Instant::now();
                    client.query(req).await.unwrap();
                    let elapsed = start.elapsed();
                    total += elapsed;
                    let _ = histogram.lock().unwrap().record(elapsed.as_micros() as u64);
                }
                total
            }
        });
    });
    group.finish();

    let histogram = histogram.lock().unwrap();
    eprintln!("\n=== point_lookup latency percentiles (n={N} pre-populated nodes) ===");
    eprintln!(
        "p50: {:.1} us | p95: {:.1} us | p99: {:.1} us | max: {:.1} us",
        histogram.value_at_quantile(0.50) as f64,
        histogram.value_at_quantile(0.95) as f64,
        histogram.value_at_quantile(0.99) as f64,
        histogram.max() as f64,
    );
}

// ── 3. bitemporal snapshot scan (as_of latency vs number of versions N) ────────

fn bench_bitemporal_scan(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("bitemporal_scan");
    group.sample_size(50);

    for &n_versions in &[10usize, 100, 1_000] {
        let (_dir, addr, subject, as_of_points) = rt.block_on(async {
            let dir = TempDir::new().unwrap();
            let store = TripleStore::open(dir.path()).unwrap();
            let (addr, shutdown) = start_server(store.clone()).await;
            std::mem::forget(shutdown);
            let mut client = connect(addr).await;

            let subject = new_node_id();
            let mut as_of_points = Vec::with_capacity(n_versions);
            let step = 1_000_000i64; // 1s of valid time per version, in microseconds
            for i in 0..n_versions {
                let vt_start = i as i64 * step;
                let vt_end = if i + 1 == n_versions {
                    0 // open-ended (END_OF_TIME)
                } else {
                    (i as i64 + 1) * step
                };
                client
                    .insert(polargraph_server::proto::InsertRequest {
                        triples: vec![versioned_property(
                            subject.clone(),
                            &format!("v{i}"),
                            vt_start,
                            vt_end,
                        )],
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                as_of_points.push(vt_start + step / 2);
            }
            (dir, addr, subject, as_of_points)
        });

        let client = rt.block_on(connect(addr));

        group.bench_function(format!("as_of_query_n{n_versions}"), |b| {
            let subject = subject.clone();
            let as_of_points = as_of_points.clone();
            let client = client.clone();
            b.to_async(&rt).iter_batched(
                || {
                    let idx = fastrand_index(as_of_points.len());
                    as_of_points[idx]
                },
                move |as_of_valid_time| {
                    let mut client = client.clone();
                    let subject = subject.clone();
                    async move {
                        let req = QueryRequest {
                            patterns: vec![VarPattern {
                                subject: Some(bound(&subject)),
                                predicate: "status".into(),
                                object: Some(any()),
                                predicate_var: String::new(),
                            }],
                            as_of_valid_time,
                            ..Default::default()
                        };
                        client.query(req).await.unwrap();
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

/// Tiny xorshift-based index picker — avoids pulling in a `rand` dependency
/// just to pick a benchmark sample point.
fn fastrand_index(len: usize) -> usize {
    use std::cell::Cell;
    thread_local! {
        static STATE: Cell<u64> = Cell::new(0x2545F4914F6CDD1D);
    }
    STATE.with(|s| {
        let mut x = s.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        s.set(x);
        (x as usize) % len.max(1)
    })
}

// ── 4. batch write (1000 nodes in sequence) ────────────────────────────────────

fn bench_batch_write(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (addr, _shutdown) = rt.block_on(async {
        let dir = TempDir::new().unwrap();
        let store = TripleStore::open(dir.path()).unwrap();
        let (addr, shutdown) = start_server(store.clone()).await;
        std::mem::forget(dir); // keep data dir alive; TempDir drop would delete it mid-bench
        (addr, shutdown)
    });
    let client = rt.block_on(connect(addr));

    const BATCH: usize = 1000;
    let mut group = c.benchmark_group("batch_write");
    group.throughput(Throughput::Elements(BATCH as u64));
    group.sample_size(20);
    group.bench_function("sequential_1000_nodes", |b| {
        b.to_async(&rt).iter_batched(
            || (0..BATCH).map(|_| new_node_id()).collect::<Vec<_>>(),
            |ids| {
                let mut client = client.clone();
                async move {
                    for id in ids {
                        client
                            .insert(polargraph_server::proto::InsertRequest {
                                triples: vec![name_property(id, "batch-node")],
                                ..Default::default()
                            })
                            .await
                            .unwrap();
                    }
                }
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

criterion_group!(
    throughput,
    bench_single_node_write,
    bench_point_lookup,
    bench_bitemporal_scan,
    bench_batch_write
);
criterion_main!(throughput);
