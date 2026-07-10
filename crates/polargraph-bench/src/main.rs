//! polargraph-bench — end-to-end benchmark scenarios for PolarGraph DB Engine.
//!
//! Usage:
//!   polargraph-bench write           --addr <url>   # insert throughput
//!   polargraph-bench read            --addr <url>   # point-query latency
//!   polargraph-bench mixed           --addr <url>   # concurrent reads + writes
//!   polargraph-bench recovery                       # store re-open time (no server)
//!   polargraph-bench filtered-search --addr <url>  # ANN latency + recall
//!   polargraph-bench vector-near-graph              # ANN + graph join (no server)
//!   polargraph-bench bsbm                           # BSBM 12-query suite (no server)
//!
//! The write / read / mixed / filtered-search scenarios require a running
//! polargraphd instance (default: http://localhost:50051).
//! The recovery / vector-near-graph / bsbm scenarios talk directly to
//! RocksDB — no server needed.

mod bsbm;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use hdrhistogram::Histogram;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;
use tonic::transport::Channel;
use tracing::info;
use uuid::Uuid;

// ── proto imports ─────────────────────────────────────────────────────────────

use polargraph_server::proto::{
    self, polar_graph_service_client::PolarGraphServiceClient,
    search_vector_filtered_request::Filter as SvfFilter, BatchInsertVectorsRequest,
    BatchInsertVectorsResponse, InsertRequest, NodeId as ProtoNodeId,
    NodeTypeDef as ProtoNodeTypeDef, NodeTypeFilter, PropertyTriple, QueryRequest, QueryResponse,
    RegisterNodeTypeRequest, RelationTriple, SearchVectorFilteredRequest,
    SearchVectorFilteredResponse, Term, Triple as ProtoTriple, Value as ProtoValue, VarPattern,
    VectorItem, VectorSpaceDef as ProtoVectorSpaceDef,
};

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(name = "polargraph-bench", about = "PolarGraph DB benchmark runner")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Insert throughput — requires a running polargraphd
    Write(GrpcArgs),
    /// Point-query latency — requires a running polargraphd
    Read(GrpcArgs),
    /// Concurrent reads + writes — requires a running polargraphd
    Mixed(GrpcArgs),
    /// Store re-open time — no server needed
    Recovery(RecoveryArgs),
    /// ANN latency + recall — requires a running polargraphd
    FilteredSearch(FilteredSearchArgs),
    /// ANN + graph join in-process — no server needed
    VectorNearGraph,
    /// BSBM 12-query e-commerce suite — no server needed
    Bsbm(BsbmArgs),
}

#[derive(Args, Debug)]
struct GrpcArgs {
    /// gRPC server address
    #[arg(long, default_value = "http://localhost:50051")]
    addr: String,
    /// Number of nodes to insert / query
    #[arg(long, default_value_t = 10_000)]
    nodes: usize,
    /// Relation edges added per node (write / mixed)
    #[arg(long, default_value_t = 4)]
    edges_per_node: usize,
    /// Vector dimensionality
    #[arg(long, default_value_t = 128)]
    vector_dims: usize,
    /// Concurrent workers (mixed scenario)
    #[arg(long, default_value_t = 4)]
    concurrency: usize,
}

#[derive(Args, Debug)]
struct RecoveryArgs {
    /// Number of triples to write before timing re-open
    #[arg(long, default_value_t = 10_000)]
    nodes: usize,
}

#[derive(Args, Debug)]
struct FilteredSearchArgs {
    /// gRPC server address
    #[arg(long, default_value = "http://localhost:50051")]
    addr: String,
    /// Number of nodes with vectors
    #[arg(long, default_value_t = 10_000)]
    nodes: usize,
    /// Vector dimensionality
    #[arg(long, default_value_t = 128)]
    vector_dims: usize,
}

#[derive(Args, Debug)]
struct BsbmArgs {
    /// RocksDB data directory (uses a temp dir if omitted)
    #[arg(long)]
    data_dir: Option<std::path::PathBuf>,
    /// Dataset scale factor N (products = N×100, offers = N×50, …)
    #[arg(long, default_value_t = 1)]
    scale_factor: usize,
    /// Warmup runs per query (discarded from statistics)
    #[arg(long, default_value_t = 10)]
    warmup_runs: usize,
    /// Measurement runs per query
    #[arg(long, default_value_t = 100)]
    measure_runs: usize,
}

// ── node ID helpers ───────────────────────────────────────────────────────────

fn new_node_id_bytes() -> Vec<u8> {
    Uuid::now_v7().into_bytes().to_vec()
}

fn proto_node_id(bytes: Vec<u8>) -> ProtoNodeId {
    ProtoNodeId { bytes }
}

fn proto_node_id_ref(bytes: &[u8]) -> ProtoNodeId {
    ProtoNodeId {
        bytes: bytes.to_vec(),
    }
}

// ── vector helpers ────────────────────────────────────────────────────────────

fn xorshift64(s: &mut u64) -> u64 {
    let mut x = *s;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *s = x;
    x
}

fn make_vec(seed: &mut u64, dims: usize) -> Vec<f32> {
    let raw: Vec<f32> = (0..dims)
        .map(|_| {
            let bits = xorshift64(seed);
            (bits >> 11) as f32 / (1u64 << 53) as f32 * 2.0 - 1.0
        })
        .collect();
    let norm = raw.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
    raw.into_iter().map(|x| x / norm).collect()
}

// ── proto triple builders ─────────────────────────────────────────────────────

fn text_prop(subject: Vec<u8>, predicate: &str, text: &str) -> ProtoTriple {
    ProtoTriple {
        kind: Some(proto::triple::Kind::Property(PropertyTriple {
            subject: Some(proto_node_id(subject)),
            predicate: predicate.to_string(),
            value: Some(ProtoValue {
                kind: Some(proto::value::Kind::TextVal(text.to_string())),
            }),
            vt_start: 0,
            vt_end: 0,
        })),
    }
}

fn relation_triple(subject: Vec<u8>, predicate: &str, object: Vec<u8>) -> ProtoTriple {
    ProtoTriple {
        kind: Some(proto::triple::Kind::Relation(RelationTriple {
            subject: Some(proto_node_id(subject)),
            predicate: predicate.to_string(),
            object: Some(proto_node_id(object)),
            vt_start: 0,
            vt_end: 0,
            properties: vec![],
        })),
    }
}

// ── latency histogram ─────────────────────────────────────────────────────────

struct LatencyUs(Histogram<u64>);

impl LatencyUs {
    fn new() -> Self {
        Self(Histogram::new(2).expect("histogram init"))
    }

    fn record(&mut self, elapsed: Duration) {
        let us = elapsed.as_micros().min(u64::MAX as u128) as u64;
        let _ = self.0.record(us);
    }

    fn p50(&self) -> u64 {
        self.0.value_at_quantile(0.50)
    }
    fn p95(&self) -> u64 {
        self.0.value_at_quantile(0.95)
    }
    fn p99(&self) -> u64 {
        self.0.value_at_quantile(0.99)
    }
    fn mean(&self) -> f64 {
        self.0.mean()
    }
    fn count(&self) -> u64 {
        self.0.len()
    }
}

// ── results printing ──────────────────────────────────────────────────────────

fn print_row(label: &str, val: &str) {
    println!("  {label:<30} {val}");
}

fn print_latency(label: &str, h: &LatencyUs) {
    println!(
        "  {label:<30} p50={:>7}µs  p95={:>7}µs  p99={:>7}µs  (n={})",
        h.p50(),
        h.p95(),
        h.p99(),
        h.count()
    );
}

// ── scenario: write ───────────────────────────────────────────────────────────

async fn run_write(mut client: PolarGraphServiceClient<Channel>, args: &GrpcArgs) -> Result<()> {
    let n = args.nodes;
    let e = args.edges_per_node;
    const BATCH: usize = 100;

    println!("\n[write] inserting {n} nodes, {e} edges/node, batch={BATCH}");

    let node_ids: Vec<Vec<u8>> = (0..n).map(|_| new_node_id_bytes()).collect();

    let mut prop_hist = LatencyUs::new();
    let mut edge_hist = LatencyUs::new();

    let wall = Instant::now();
    for chunk in node_ids.chunks(BATCH) {
        let triples: Vec<ProtoTriple> = chunk
            .iter()
            .flat_map(|id| {
                [
                    text_prop(id.clone(), "__type", "BenchNode"),
                    text_prop(id.clone(), "name", "bench"),
                ]
            })
            .collect();
        let t0 = Instant::now();
        client
            .insert(InsertRequest {
                triples,
                ..Default::default()
            })
            .await
            .context("insert properties")?;
        prop_hist.record(t0.elapsed());
    }
    let prop_wall = wall.elapsed();

    let wall = Instant::now();
    for chunk_start in (0..n).step_by(BATCH) {
        let chunk_end = (chunk_start + BATCH).min(n);
        let mut triples = Vec::with_capacity((chunk_end - chunk_start) * e);
        for i in chunk_start..chunk_end {
            for j in 0..e {
                let obj = (i + j + 1) % n;
                triples.push(relation_triple(
                    node_ids[i].clone(),
                    "links",
                    node_ids[obj].clone(),
                ));
            }
        }
        let t0 = Instant::now();
        client
            .insert(InsertRequest {
                triples,
                ..Default::default()
            })
            .await
            .context("insert relations")?;
        edge_hist.record(t0.elapsed());
    }
    let edge_wall = wall.elapsed();

    let total_triples = n * (2 + e);
    let total_secs = (prop_wall + edge_wall).as_secs_f64();

    println!();
    print_row("nodes inserted", &n.to_string());
    print_row("total triples", &total_triples.to_string());
    print_row(
        "throughput",
        &format!("{:.0} triples/sec", total_triples as f64 / total_secs),
    );
    print_latency("property batch latency", &prop_hist);
    print_latency("relation batch latency", &edge_hist);

    Ok(())
}

// ── scenario: read ────────────────────────────────────────────────────────────

async fn run_read(mut client: PolarGraphServiceClient<Channel>, args: &GrpcArgs) -> Result<()> {
    let n = args.nodes;
    const BATCH: usize = 100;

    println!("\n[read] pre-populating {n} nodes then querying each");

    let node_ids: Vec<Vec<u8>> = (0..n).map(|_| new_node_id_bytes()).collect();

    for chunk in node_ids.chunks(BATCH) {
        let triples: Vec<ProtoTriple> = chunk
            .iter()
            .map(|id| text_prop(id.clone(), "name", "bench"))
            .collect();
        client
            .insert(InsertRequest {
                triples,
                ..Default::default()
            })
            .await
            .context("pre-populate")?;
    }

    let mut hist = LatencyUs::new();
    let wall = Instant::now();
    for id in &node_ids {
        let req = QueryRequest {
            patterns: vec![VarPattern {
                subject: Some(Term {
                    kind: Some(proto::term::Kind::Bound(proto_node_id_ref(id))),
                }),
                predicate: String::new(),
                object: Some(Term {
                    kind: Some(proto::term::Kind::Var("x".into())),
                }),
                predicate_var: String::new(),
            }],
            snapshot_ts: 0,
            ..Default::default()
        };
        let t0 = Instant::now();
        let _resp: QueryResponse = client.query(req).await.context("query")?.into_inner();
        hist.record(t0.elapsed());
    }
    let total_secs = wall.elapsed().as_secs_f64();

    println!();
    print_row("queries", &hist.count().to_string());
    print_row(
        "throughput",
        &format!("{:.0} queries/sec", hist.count() as f64 / total_secs),
    );
    print_latency("query latency", &hist);

    Ok(())
}

// ── scenario: mixed ───────────────────────────────────────────────────────────

async fn run_mixed(client: PolarGraphServiceClient<Channel>, args: &GrpcArgs) -> Result<()> {
    let n = args.nodes;
    let c = args.concurrency;
    const OPS_PER_WORKER: usize = 500;

    println!("\n[mixed] {c} workers × {OPS_PER_WORKER} ops (80% read / 20% write)");

    let corpus: Vec<Vec<u8>> = {
        let mut cl = client.clone();
        let ids: Vec<Vec<u8>> = (0..n).map(|_| new_node_id_bytes()).collect();
        for chunk in ids.chunks(100) {
            let triples: Vec<ProtoTriple> = chunk
                .iter()
                .map(|id| text_prop(id.clone(), "name", "bench"))
                .collect();
            cl.insert(InsertRequest {
                triples,
                ..Default::default()
            })
            .await
            .context("pre-populate")?;
        }
        ids
    };
    let corpus = Arc::new(corpus);

    let sem = Arc::new(Semaphore::new(c));
    let read_hist = Arc::new(tokio::sync::Mutex::new(LatencyUs::new()));
    let write_hist = Arc::new(tokio::sync::Mutex::new(LatencyUs::new()));
    let mut handles = Vec::new();

    let total_ops = c * OPS_PER_WORKER;
    let wall = Instant::now();

    for op_idx in 0..total_ops {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let mut cl = client.clone();
        let corpus = corpus.clone();
        let rh = read_hist.clone();
        let wh = write_hist.clone();

        handles.push(tokio::spawn(async move {
            let _permit = permit;
            if op_idx % 5 != 0 {
                let id = &corpus[op_idx % corpus.len()];
                let req = QueryRequest {
                    patterns: vec![VarPattern {
                        subject: Some(Term {
                            kind: Some(proto::term::Kind::Bound(proto_node_id_ref(id))),
                        }),
                        predicate: String::new(),
                        object: Some(Term {
                            kind: Some(proto::term::Kind::Var("x".into())),
                        }),
                        predicate_var: String::new(),
                    }],
                    snapshot_ts: 0,
                    ..Default::default()
                };
                let t0 = Instant::now();
                let _ = cl.query(req).await;
                rh.lock().await.record(t0.elapsed());
            } else {
                let id = new_node_id_bytes();
                let t0 = Instant::now();
                let _ = cl
                    .insert(InsertRequest {
                        triples: vec![text_prop(id, "name", "new")],
                        ..Default::default()
                    })
                    .await;
                wh.lock().await.record(t0.elapsed());
            }
        }));
    }

    for h in handles {
        h.await.context("task panicked")?;
    }

    let total_secs = wall.elapsed().as_secs_f64();
    let rh = read_hist.lock().await;
    let wh = write_hist.lock().await;
    let total = rh.count() + wh.count();

    println!();
    print_row(
        "total ops",
        &format!("{total} ({} reads, {} writes)", rh.count(), wh.count()),
    );
    print_row(
        "throughput",
        &format!("{:.0} ops/sec", total as f64 / total_secs),
    );
    print_latency("read latency", &rh);
    print_latency("write latency", &wh);

    Ok(())
}

// ── scenario: recovery ────────────────────────────────────────────────────────

async fn run_recovery(args: &RecoveryArgs) -> Result<()> {
    use polargraph_core::{
        id::NodeId,
        temporal::{BiTemporalRange, Timestamp},
        triple::{Predicate, Triple},
        value::Value,
    };
    use polargraph_storage::TripleStore;

    let n = args.nodes;
    println!("\n[recovery] writing {n} triples then timing re-open");

    let dir = tempfile::tempdir().context("tempdir")?;

    {
        let store = TripleStore::open(dir.path()).context("open for population")?;
        let mut inserted = 0;
        while inserted < n {
            let batch = (n - inserted).min(500);
            let mut tx = store.begin();
            for _ in 0..batch {
                let id = NodeId::new();
                tx.insert(Triple::Property {
                    subject: id,
                    predicate: Predicate::new("name"),
                    value: Value::Text("recovery-bench".into()),
                    temporal: BiTemporalRange::assert_now(Timestamp::now()),
                });
            }
            tx.commit().context("commit")?;
            inserted += batch;
        }
        info!("wrote {n} triples, closing store");
    }

    const RUNS: usize = 5;
    let mut times_ms = Vec::with_capacity(RUNS);
    for _ in 0..RUNS {
        let t0 = Instant::now();
        let store = TripleStore::open(dir.path()).context("re-open")?;
        let elapsed = t0.elapsed();
        times_ms.push(elapsed.as_millis());
        drop(store);
    }

    let mean_ms: f64 = times_ms.iter().sum::<u128>() as f64 / RUNS as f64;
    let min_ms = times_ms.iter().min().unwrap();
    let max_ms = times_ms.iter().max().unwrap();

    println!();
    print_row("triples on disk", &n.to_string());
    print_row("re-open runs", &RUNS.to_string());
    print_row("mean open time", &format!("{mean_ms:.1} ms"));
    print_row("min / max", &format!("{min_ms} ms / {max_ms} ms"));

    Ok(())
}

// ── scenario: filtered-search ─────────────────────────────────────────────────

async fn run_filtered_search(
    mut client: PolarGraphServiceClient<Channel>,
    args: &FilteredSearchArgs,
) -> Result<()> {
    let n = args.nodes;
    let dims = args.vector_dims;
    const K: usize = 10;
    const Q: usize = 50;
    const BATCH: usize = 200;
    const SPACE: &str = "BenchVec";
    const TYPE_NAME: &str = "BenchNode";

    println!("\n[filtered-search] {n} nodes, {dims}-dim vectors, k={K}, {Q} queries");

    client
        .register_node_type(RegisterNodeTypeRequest {
            definition: Some(ProtoNodeTypeDef {
                type_name: TYPE_NAME.into(),
                fields: vec![],
                parent_types: vec![],
                vector_space: Some(ProtoVectorSpaceDef {
                    space_name: SPACE.into(),
                    dimensions: dims as u32,
                    embedding_model: String::new(),
                    storage_mode: String::new(),
                }),
            }),
        })
        .await
        .context("register node type")?;

    let mut seed = 0x1234_5678_u64;
    let mut node_ids: Vec<Vec<u8>> = Vec::with_capacity(n);
    let mut stored_vecs: Vec<Vec<f32>> = Vec::with_capacity(n);

    for chunk_start in (0..n).step_by(BATCH) {
        let chunk_end = (chunk_start + BATCH).min(n);

        let mut triples = Vec::new();
        for _ in chunk_start..chunk_end {
            let id = new_node_id_bytes();
            triples.push(text_prop(id.clone(), "__type", TYPE_NAME));
            let vec = make_vec(&mut seed, dims);
            stored_vecs.push(vec.clone());
            node_ids.push(id);
        }
        client
            .insert(InsertRequest {
                triples,
                ..Default::default()
            })
            .await
            .context("insert nodes")?;

        let items: Vec<VectorItem> = node_ids[chunk_start..chunk_end]
            .iter()
            .zip(&stored_vecs[chunk_start..chunk_end])
            .map(|(id, vec)| VectorItem {
                node_id: Some(proto_node_id_ref(id)),
                vector: vec.clone(),
            })
            .collect();
        let resp: BatchInsertVectorsResponse = client
            .batch_insert_vectors(BatchInsertVectorsRequest {
                space: SPACE.into(),
                items,
            })
            .await
            .context("batch insert vectors")?
            .into_inner();
        if !resp.errors.is_empty() {
            anyhow::bail!("vector insert errors: {:?}", resp.errors);
        }
    }

    let mut lat = LatencyUs::new();
    let mut total_recall = 0.0f64;

    for q in 0..Q {
        let query = make_vec(&mut seed, dims);

        let mut bf_scores: Vec<(usize, f32)> = stored_vecs
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let sim: f32 = v.iter().zip(&query).map(|(a, b)| a * b).sum();
                (i, sim)
            })
            .collect();
        bf_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        let true_top_k: HashSet<Vec<u8>> = bf_scores
            .iter()
            .take(K)
            .map(|(i, _)| node_ids[*i].clone())
            .collect();

        let t0 = Instant::now();
        let resp: SearchVectorFilteredResponse = client
            .search_vector_filtered(SearchVectorFilteredRequest {
                space: SPACE.into(),
                query: query.clone(),
                k: K as u32,
                ef: 0,
                filter: Some(SvfFilter::NodeTypeFilter(NodeTypeFilter {
                    type_name: TYPE_NAME.into(),
                })),
                user_id: String::new(),
            })
            .await
            .context("search_vector_filtered")?
            .into_inner();
        lat.record(t0.elapsed());

        let ann_ids: HashSet<Vec<u8>> = resp
            .results
            .iter()
            .filter_map(|r| r.node_id.as_ref())
            .map(|id| id.bytes.clone())
            .collect();

        let recall = ann_ids.intersection(&true_top_k).count() as f64 / K as f64;
        total_recall += recall;

        if q == 0 {
            info!(
                "first query: {} ANN results, recall@{K}={:.2}",
                ann_ids.len(),
                recall
            );
        }
    }

    let mean_recall = total_recall / Q as f64;

    println!();
    print_row("nodes with vectors", &n.to_string());
    print_row("vector dims", &dims.to_string());
    print_row("queries run", &Q.to_string());
    print_row(&format!("mean recall@{K}"), &format!("{:.3}", mean_recall));
    print_latency("filtered ANN latency", &lat);

    Ok(())
}

// ── scenario: vector-near-graph ───────────────────────────────────────────────

async fn run_vector_near_graph() -> Result<()> {
    use polargraph_core::{
        id::NodeId,
        schema::StorageMode,
        temporal::{BiTemporalRange, Timestamp},
        triple::{Predicate, Triple},
        value::Value,
    };
    use polargraph_query::datalog::{execute_query_seeded, Query, Term, VarPattern};
    use polargraph_storage::TripleStore;

    const DIMS: usize = 128;
    const CITES_PER_NODE: usize = 5;
    const SPACE: &str = "bench";
    const ITERS: usize = 100;
    const EF: usize = 50;
    const K_VALUES: &[usize] = &[5, 10, 25];
    const SCALES: &[usize] = &[1_000, 10_000, 100_000];

    println!("\n[vector-near-graph] dims={DIMS} cites_per_node={CITES_PER_NODE} iters={ITERS}");
    println!();

    for &n in SCALES {
        print!("  setup nodes={n:<7} ... ");
        std::io::Write::flush(&mut std::io::stdout()).ok();

        let dir = tempfile::tempdir().context("tempdir")?;
        let store = TripleStore::open(dir.path()).context("open store")?;

        let mut seed = 0xDEAD_BEEF_u64.wrapping_add(n as u64);
        let node_ids: Vec<NodeId> = (0..n).map(|_| NodeId::new()).collect();

        const TRI_BATCH: usize = 500;
        let type_pred = Predicate::new("__type");
        let cites_pred = Predicate::new("cites");

        for chunk in node_ids.chunks(TRI_BATCH) {
            let mut tx = store.begin();
            for &id in chunk {
                tx.insert(Triple::Property {
                    subject: id,
                    predicate: type_pred.clone(),
                    value: Value::Text("Document".into()),
                    temporal: BiTemporalRange::assert_now(Timestamp::now()),
                });
            }
            tx.commit().context("commit type triples")?;

            let items: Vec<(NodeId, Vec<f32>)> = chunk
                .iter()
                .map(|&id| (id, make_vec(&mut seed, DIMS)))
                .collect();
            store.batch_insert_vectors(SPACE, &items, StorageMode::Memory);
        }

        for chunk_start in (0..n).step_by(TRI_BATCH) {
            let chunk_end = (chunk_start + TRI_BATCH).min(n);
            let mut tx = store.begin();
            for i in chunk_start..chunk_end {
                for j in 1..=CITES_PER_NODE {
                    let obj = (i + j * 7 + 3) % n;
                    tx.insert(Triple::Relation {
                        subject: node_ids[i],
                        predicate: cites_pred.clone(),
                        object: node_ids[obj],
                        temporal: BiTemporalRange::assert_now(Timestamp::now()),
                        edge_id: polargraph_core::id::EdgeId::new(),
                    });
                }
            }
            tx.commit().context("commit cites triples")?;
        }

        println!("done");

        let snap_ts = store.begin().read_ts;
        let snapshot = store.snapshot(snap_ts);

        let query = Query::new().pattern(
            VarPattern::new()
                .subject(Term::var("a"))
                .predicate("cites")
                .object(Term::var("b")),
        );

        for &k in K_VALUES {
            let mut ann_hist = LatencyUs::new();
            let mut graph_hist = LatencyUs::new();
            let mut total_hist = LatencyUs::new();

            for iter in 0..ITERS {
                let qvec = make_vec(&mut seed.wrapping_add(iter as u64 * 31), DIMS);

                let t_total = Instant::now();

                let t_ann = Instant::now();
                let ann_results = store.search_vector_ef(SPACE, &qvec, k, EF);
                ann_hist.record(t_ann.elapsed());

                let seed_bindings: Vec<HashMap<String, NodeId>> = ann_results
                    .iter()
                    .map(|(node_id, _)| {
                        let mut b = HashMap::new();
                        b.insert("a".to_string(), *node_id);
                        b
                    })
                    .collect();

                let t_graph = Instant::now();
                let _results = execute_query_seeded(&query, &snapshot, seed_bindings, None, None)
                    .context("execute_query_seeded")?;
                graph_hist.record(t_graph.elapsed());

                total_hist.record(t_total.elapsed());
            }

            println!(
                "  vector-near-graph | nodes={n:<7} k={k:<3} | \
                 p50={:>6}µs  p95={:>6}µs  p99={:>6}µs  mean={:>6.0}µs",
                total_hist.p50(),
                total_hist.p95(),
                total_hist.p99(),
                total_hist.mean(),
            );
            println!(
                "                    |               ann:  p50={:>6}µs  p95={:>6}µs  mean={:>6.0}µs  \
                 graph: p50={:>6}µs  p95={:>6}µs  mean={:>6.0}µs",
                ann_hist.p50(), ann_hist.p95(), ann_hist.mean(),
                graph_hist.p50(), graph_hist.p95(), graph_hist.mean(),
            );
        }
        println!();
    }

    Ok(())
}

// ── scenario: bsbm ───────────────────────────────────────────────────────────

async fn run_bsbm_scenario(args: &BsbmArgs) -> Result<()> {
    use polargraph_storage::TripleStore;

    let _tmpdir;
    let data_path: std::path::PathBuf = if let Some(ref p) = args.data_dir {
        p.clone()
    } else {
        _tmpdir = tempfile::tempdir().context("tempdir for bsbm")?;
        _tmpdir.path().to_path_buf()
    };

    let store = TripleStore::open(&data_path).context("open store for bsbm")?;
    bsbm::run_bsbm(
        &store,
        args.scale_factor,
        args.warmup_runs,
        args.measure_runs,
    );
    Ok(())
}

// ── main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .init();

    match &cli.command {
        Command::Recovery(args) => {
            run_recovery(args).await?;
        }
        Command::VectorNearGraph => {
            run_vector_near_graph().await?;
        }
        Command::Bsbm(args) => {
            run_bsbm_scenario(args).await?;
        }
        Command::Write(args) => {
            let client = PolarGraphServiceClient::connect(args.addr.clone())
                .await
                .with_context(|| format!("connecting to {}", args.addr))?;
            run_write(client, args).await?;
        }
        Command::Read(args) => {
            let client = PolarGraphServiceClient::connect(args.addr.clone())
                .await
                .with_context(|| format!("connecting to {}", args.addr))?;
            run_read(client, args).await?;
        }
        Command::Mixed(args) => {
            let client = PolarGraphServiceClient::connect(args.addr.clone())
                .await
                .with_context(|| format!("connecting to {}", args.addr))?;
            run_mixed(client, args).await?;
        }
        Command::FilteredSearch(args) => {
            let client = PolarGraphServiceClient::connect(args.addr.clone())
                .await
                .with_context(|| format!("connecting to {}", args.addr))?;
            run_filtered_search(client, args).await?;
        }
    }

    println!("\ndone.");
    Ok(())
}
