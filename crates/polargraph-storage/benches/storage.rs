//! Criterion micro-benchmarks for polargraph-storage.
//!
//! Run with:
//!   cargo bench -p polargraph-storage
//!   cargo bench -p polargraph-storage -- triple_writes  # filter by name
//!
//! HTML reports land in target/criterion/.

use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use polargraph_core::{
    id::{EdgeId, NodeId},
    temporal::{BiTemporalRange, Timestamp},
    triple::{Predicate, Triple},
    value::Value,
};
use polargraph_storage::TripleStore;
use std::collections::HashSet;

// ── helpers ───────────────────────────────────────────────────────────────────

fn prop(subject: NodeId, key: &str, val: &str) -> Triple {
    Triple::Property {
        subject,
        predicate: Predicate::new(key),
        value: Value::Text(val.into()),
        temporal: BiTemporalRange::assert_now(Timestamp::now()),
    }
}

fn rel(subject: NodeId, predicate: &str, object: NodeId) -> Triple {
    Triple::Relation {
        subject,
        predicate: Predicate::new(predicate),
        object,
        edge_id: EdgeId::new(),
        temporal: BiTemporalRange::assert_now(Timestamp::now()),
    }
}

/// Deterministic LCG — produces unit-normalised f32 vectors without any
/// external crate dependency. Not cryptographically random; that's fine here.
fn lcg_vec(seed: u64, dims: usize) -> Vec<f32> {
    let mut s = seed;
    let raw: Vec<f32> = (0..dims)
        .map(|_| {
            s = s
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            // Map to [-1, 1]
            (s >> 11) as f32 / (1u64 << 53) as f32 * 2.0 - 1.0
        })
        .collect();
    let norm = raw.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
    raw.into_iter().map(|x| x / norm).collect()
}

// ── triple write benchmarks ───────────────────────────────────────────────────

fn bench_triple_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("triple_writes");

    for &n in &[1usize, 10, 100, 1_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let store = TripleStore::open(dir.path()).unwrap();
                    let triples: Vec<Triple> = (0..n)
                        .map(|i| prop(NodeId::new(), "name", &format!("node-{i}")))
                        .collect();
                    (dir, store, triples)
                },
                |(_dir, store, triples)| {
                    let mut tx = store.begin();
                    for t in triples {
                        tx.insert(t);
                    }
                    std::hint::black_box(tx.commit().unwrap())
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

// ── pattern query benchmarks ──────────────────────────────────────────────────

fn bench_pattern_query(c: &mut Criterion) {
    // Shared setup: 500 nodes × 2 properties + a ring of relation triples.
    // We do this outside the timed loop so all iterations hit the same data.
    const N: usize = 500;
    let dir = tempfile::tempdir().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();

    let ids: Vec<NodeId> = (0..N).map(|_| NodeId::new()).collect();
    let mut tx = store.begin();
    for &id in &ids {
        tx.insert(prop(id, "name", "bench-node"));
        tx.insert(prop(id, "kind", "entity"));
    }
    // Ring of `follows` relations so scan_by_predicate returns N results.
    for i in 0..N {
        tx.insert(rel(ids[i], "follows", ids[(i + 1) % N]));
    }
    let ts = tx.commit().unwrap();
    let snap = store.snapshot(ts);

    let mut group = c.benchmark_group("pattern_query");
    let mut idx = 0usize;

    group.bench_function("scan_by_subject", |b| {
        b.iter(|| {
            let id = ids[idx % N];
            idx = idx.wrapping_add(1);
            std::hint::black_box(snap.scan_by_subject(&id).unwrap())
        });
    });

    group.bench_function("scan_by_predicate_name", |b| {
        b.iter(|| std::hint::black_box(snap.scan_by_predicate("name").unwrap()));
    });

    group.bench_function("scan_by_predicate_object", |b| {
        b.iter(|| {
            let obj = ids[idx % N];
            idx = idx.wrapping_add(1);
            std::hint::black_box(snap.scan_by_predicate_object("follows", &obj).unwrap())
        });
    });

    group.bench_function("scan_by_object", |b| {
        b.iter(|| {
            let obj = ids[idx % N];
            idx = idx.wrapping_add(1);
            std::hint::black_box(snap.scan_by_object(&obj).unwrap())
        });
    });

    group.finish();
    // Keep the dir alive until the group is done.
    drop(dir);
}

// ── HNSW vector benchmarks ────────────────────────────────────────────────────

fn bench_hnsw_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_insert");

    for &dims in &[32usize, 128, 512] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("dims", dims), &dims, |b, &dims| {
            // Shared store: pre-warm the space so HNSW entry-point exists.
            let dir = tempfile::tempdir().unwrap();
            let store = TripleStore::open(dir.path()).unwrap();
            // Seed with a few nodes so the index structure is non-trivial.
            for i in 0u64..20 {
                store
                    .insert_vector("bench", NodeId::new(), lcg_vec(i * 137, dims))
                    .unwrap();
            }
            let mut seed = 999u64;
            b.iter(|| {
                seed = seed.wrapping_add(1);
                let id = NodeId::new();
                let vec = lcg_vec(seed, dims);
                std::hint::black_box(store.insert_vector("bench", id, vec).unwrap())
            });
            drop(dir);
        });
    }

    group.finish();
}

fn bench_hnsw_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_search");

    for &(n, dims) in &[(500usize, 128usize), (2_000, 128), (500, 512)] {
        let label = format!("n{n}_d{dims}");
        group.bench_function(&label, |b| {
            let dir = tempfile::tempdir().unwrap();
            let store = TripleStore::open(dir.path()).unwrap();
            let items: Vec<(NodeId, Vec<f32>)> = (0..n as u64)
                .map(|i| (NodeId::new(), lcg_vec(i, dims)))
                .collect();
            store.batch_insert_vectors("bench", &items).0;
            let query = lcg_vec(u64::MAX, dims);
            b.iter(|| {
                std::hint::black_box(store.search_vector("bench", query.clone(), 10))
            });
            drop(dir);
        });
    }

    group.finish();
}

fn bench_hnsw_recall(c: &mut Criterion) {
    // Recall@10 is a data-quality check, not a latency benchmark, but we
    // measure it here as a single-sample "benchmark" so it appears in CI output.
    let mut group = c.benchmark_group("hnsw_recall");
    group.sample_size(10); // one warm-up + 10 samples is enough for correctness

    const N: usize = 1_000;
    const DIMS: usize = 128;
    const K: usize = 10;

    group.bench_function("recall_at_10", |b| {
        let dir = tempfile::tempdir().unwrap();
        let store = TripleStore::open(dir.path()).unwrap();
        let items: Vec<(NodeId, Vec<f32>)> = (0..N as u64)
            .map(|i| (NodeId::new(), lcg_vec(i, DIMS)))
            .collect();
        store.batch_insert_vectors("bench", &items).0;

        let mut q_seed = 0xdeadbeef_u64;
        b.iter(|| {
            q_seed = q_seed.wrapping_add(7);
            let query = lcg_vec(q_seed, DIMS);

            // ANN results
            let ann: HashSet<NodeId> = store
                .search_vector("bench", query.clone(), K)
                .into_iter()
                .map(|(id, _)| id)
                .collect();

            // Brute-force: dot product of unit vectors = cosine similarity.
            let mut scores: Vec<(NodeId, f32)> = items
                .iter()
                .map(|(id, v)| {
                    let sim: f32 = v.iter().zip(&query).map(|(a, b)| a * b).sum();
                    (*id, sim)
                })
                .collect();
            scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
            let true_nn: HashSet<NodeId> = scores.iter().take(K).map(|(id, _)| *id).collect();

            let recall = ann.intersection(&true_nn).count() as f32 / K as f32;
            // Surface the recall value so the optimizer can't discard the work.
            std::hint::black_box(recall)
        });
        drop(dir);
    });

    group.finish();
}

// ── filtered-search comparison ────────────────────────────────────────────────

fn bench_filtered_search(c: &mut Criterion) {
    // Compare search_vector_ef (large candidate pool, post-filter) vs
    // search_vector_in_set (linear scan over an explicit allowed set).
    const N: usize = 500;
    const DIMS: usize = 128;
    const K: usize = 10;

    let dir = tempfile::tempdir().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();

    let ids: Vec<NodeId> = (0..N as u64)
        .map(|i| {
            let id = NodeId::new();
            store
                .insert_vector("bench", id, lcg_vec(i, DIMS))
                .unwrap();
            id
        })
        .collect();

    // allowed = a 10 % subset (50 nodes)
    let allowed: Vec<NodeId> = ids.iter().step_by(10).copied().collect();

    let mut group = c.benchmark_group("filtered_search");
    let query = lcg_vec(0xc0ffee, DIMS);

    group.bench_function("search_vector_ef", |b| {
        b.iter(|| {
            std::hint::black_box(store.search_vector_ef(
                "bench",
                &query,
                K,
                K * 10, // ef = k * 10
            ))
        });
    });

    group.bench_function("search_in_set", |b| {
        b.iter(|| {
            std::hint::black_box(store.search_vector_in_set("bench", &query, K, &allowed))
        });
    });

    group.finish();
    drop(dir);
}

// ── criterion wiring ──────────────────────────────────────────────────────────

criterion_group!(writes, bench_triple_writes);
criterion_group!(queries, bench_pattern_query);
criterion_group!(vector, bench_hnsw_insert, bench_hnsw_search, bench_hnsw_recall, bench_filtered_search);
criterion_main!(writes, queries, vector);
