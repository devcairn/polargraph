//! Criterion micro-benchmarks for polargraph-storage.
//!
//! Run with:
//!   cargo bench -p polargraph-storage
//!   cargo bench -p polargraph-storage -- triple_writes  # filter by name
//!
//! HTML reports land in target/criterion/.

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use polargraph_core::{
    id::{EdgeId, NodeId},
    schema::StorageMode,
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
                    .insert_vector(
                        "bench",
                        NodeId::new(),
                        lcg_vec(i * 137, dims),
                        StorageMode::Memory,
                    )
                    .unwrap();
            }
            let mut seed = 999u64;
            b.iter(|| {
                seed = seed.wrapping_add(1);
                let id = NodeId::new();
                let vec = lcg_vec(seed, dims);
                std::hint::black_box(
                    store
                        .insert_vector("bench", id, vec, StorageMode::Memory)
                        .unwrap(),
                )
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
            store
                .batch_insert_vectors("bench", &items, StorageMode::Memory)
                .0;
            let query = lcg_vec(u64::MAX, dims);
            b.iter(|| std::hint::black_box(store.search_vector("bench", query.clone(), 10)));
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
        store
            .batch_insert_vectors("bench", &items, StorageMode::Memory)
            .0;

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
                .insert_vector("bench", id, lcg_vec(i, DIMS), StorageMode::Memory)
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
        b.iter(|| std::hint::black_box(store.search_vector_in_set("bench", &query, K, &allowed)));
    });

    group.finish();
    drop(dir);
}

// ── annotation benchmarks ─────────────────────────────────────────────────────

fn bench_annotation_write(c: &mut Criterion) {
    // Measure: insert N relations each with M EdgeProperty annotations.
    let mut group = c.benchmark_group("annotation_write");

    for &(n, m) in &[(100usize, 5usize), (100, 20)] {
        let label = format!("{n}rels_{m}ann");
        group.throughput(Throughput::Elements((n * m) as u64));
        group.bench_function(&label, |b| {
            b.iter_batched(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let store = TripleStore::open(dir.path()).unwrap();
                    let nodes: Vec<(NodeId, NodeId, EdgeId)> = (0..n)
                        .map(|_| (NodeId::new(), NodeId::new(), EdgeId::new()))
                        .collect();
                    (dir, store, nodes)
                },
                |(_dir, store, nodes)| {
                    let mut tx = store.begin();
                    for (a, b, edge_id) in &nodes {
                        tx.insert(Triple::Relation {
                            subject: *a,
                            predicate: polargraph_core::triple::Predicate::new("relates"),
                            object: *b,
                            edge_id: *edge_id,
                            temporal: BiTemporalRange::assert_now(Timestamp::now()),
                        });
                        for i in 0..m {
                            tx.insert(Triple::EdgeProperty {
                                edge: *edge_id,
                                predicate: polargraph_core::triple::Predicate::new("weight"),
                                value: polargraph_core::value::Value::Float(i as f64 * 0.1),
                                temporal: BiTemporalRange::assert_now(Timestamp::now()),
                            });
                        }
                    }
                    std::hint::black_box(tx.commit().unwrap())
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_annotation_scan(c: &mut Criterion) {
    // EPA scan: point lookup by edge_id vs PEA scan by predicate.
    const N: usize = 1_000;
    const M: usize = 5; // annotations per edge

    let dir = tempfile::tempdir().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();

    let edge_ids: Vec<EdgeId> = (0..N).map(|_| EdgeId::new()).collect();
    let mut tx = store.begin();
    for &eid in &edge_ids {
        let a = NodeId::new();
        let b = NodeId::new();
        tx.insert(Triple::Relation {
            subject: a,
            predicate: polargraph_core::triple::Predicate::new("relates"),
            object: b,
            edge_id: eid,
            temporal: BiTemporalRange::assert_now(Timestamp::now()),
        });
        for i in 0..M {
            tx.insert(Triple::EdgeProperty {
                edge: eid,
                predicate: polargraph_core::triple::Predicate::new(&format!("prop{i}")),
                value: polargraph_core::value::Value::Float(i as f64),
                temporal: BiTemporalRange::assert_now(Timestamp::now()),
            });
        }
    }
    let ts = tx.commit().unwrap();

    let mut idx = 0usize;
    let mut group = c.benchmark_group("annotation_scan");

    group.bench_function("scan_by_edge_1k_edges", |b| {
        b.iter(|| {
            let eid = edge_ids[idx % N];
            idx = idx.wrapping_add(1);
            std::hint::black_box(store.scan_edge_annotations(eid, ts).unwrap())
        });
    });

    group.bench_function("scan_by_predicate_1k_edges", |b| {
        b.iter(|| std::hint::black_box(store.scan_annotations_by_predicate("prop0", ts).unwrap()));
    });

    group.finish();
    drop(dir);
}

// ── BSBM storage-layer micro-benchmarks ──────────────────────────────────────
//
// Implement Q1 and Q7 using raw storage scans only (no polargraph-query dep).
// Q1 = product-search: PSO scan → SPO feature filter → property scan.
// Q7 = 5-way join: POS scan × 2 + SPO scan × 4 (offer/vendor/review/reviewer).

const BSBM_TYPE: &str = "bsbm:type";
const BSBM_FEATURE: &str = "bsbm:feature";
const BSBM_NUM1: &str = "bsbm:productPropertyNumeric1";
const BSBM_OFFER_FOR: &str = "bsbm:offerFor";
const BSBM_VENDOR: &str = "bsbm:vendor";
const BSBM_PRICE: &str = "bsbm:price";
const BSBM_REVIEW_FOR: &str = "bsbm:reviewFor";
const BSBM_REVIEWER: &str = "bsbm:reviewer";

/// Hash an IRI string to a deterministic NodeId (same as bsbm.rs / owl_rl.rs).
fn bsbm_node_id(iri: &str) -> NodeId {
    // FNV-1a variant to produce a stable 128-bit hash without xxhash dep.
    let mut h1: u64 = 0xcbf29ce484222325;
    let mut h2: u64 = 0x100000001b3;
    for &b in iri.as_bytes() {
        h1 ^= b as u64;
        h1 = h1.wrapping_mul(0x100000001b3);
        h2 ^= b as u64;
        h2 = h2.wrapping_mul(0xcbf29ce484222325);
    }
    let hash128 = ((h1 as u128) << 64) | h2 as u128;
    NodeId(uuid::Uuid::from_u128(hash128))
}

fn bsbm_rel(subject: NodeId, predicate: &str, object: NodeId) -> Triple {
    Triple::Relation {
        subject,
        predicate: polargraph_core::triple::Predicate::new(predicate),
        object,
        edge_id: EdgeId::new(),
        temporal: BiTemporalRange::assert_now(Timestamp::now()),
    }
}

fn bsbm_prop_int(subject: NodeId, predicate: &str, value: i64) -> Triple {
    Triple::Property {
        subject,
        predicate: polargraph_core::triple::Predicate::new(predicate),
        value: polargraph_core::value::Value::Int(value),
        temporal: BiTemporalRange::assert_now(Timestamp::now()),
    }
}

fn bsbm_prop_float(subject: NodeId, predicate: &str, value: f64) -> Triple {
    Triple::Property {
        subject,
        predicate: polargraph_core::triple::Predicate::new(predicate),
        value: polargraph_core::value::Value::Float(value),
        temporal: BiTemporalRange::assert_now(Timestamp::now()),
    }
}

/// Build a scale-1 BSBM store (100 products, 10 types, 20 features, 5 vendors,
/// 50 offers, 20 reviews) and return (store, tempdir, snap_ts).
fn setup_bsbm_store() -> (TripleStore, tempfile::TempDir, i64) {
    let dir = tempfile::tempdir().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();

    let n_products = 100usize;
    let n_types = 10usize;
    let n_features = 20usize;
    let n_vendors = 5usize;
    let n_offers = 50usize;
    let n_reviews = 20usize;

    let products: Vec<NodeId> = (0..n_products)
        .map(|i| bsbm_node_id(&format!("urn:bsbm:product:{i}")))
        .collect();
    let types: Vec<NodeId> = (0..n_types)
        .map(|i| bsbm_node_id(&format!("urn:bsbm:productType:{i}")))
        .collect();
    let features: Vec<NodeId> = (0..n_features)
        .map(|i| bsbm_node_id(&format!("urn:bsbm:feature:{i}")))
        .collect();
    let vendors: Vec<NodeId> = (0..n_vendors)
        .map(|i| bsbm_node_id(&format!("urn:bsbm:vendor:{i}")))
        .collect();
    let offers: Vec<NodeId> = (0..n_offers)
        .map(|i| bsbm_node_id(&format!("urn:bsbm:offer:{i}")))
        .collect();
    let reviews: Vec<NodeId> = (0..n_reviews)
        .map(|i| bsbm_node_id(&format!("urn:bsbm:review:{i}")))
        .collect();
    let reviewers: Vec<NodeId> = (0..n_reviews)
        .map(|i| bsbm_node_id(&format!("urn:bsbm:reviewer:{i}")))
        .collect();

    const BATCH: usize = 500;
    let mut all_triples: Vec<Triple> = Vec::new();

    // Products
    for (i, &pid) in products.iter().enumerate() {
        let type_idx = if n_types <= 5 {
            0
        } else {
            5 + (i % (n_types - 5))
        };
        all_triples.push(bsbm_rel(pid, BSBM_TYPE, types[type_idx]));
        let n_feats = 2 + (i % 3);
        for f in 0..n_feats {
            all_triples.push(bsbm_rel(
                pid,
                BSBM_FEATURE,
                features[(i + f * 7) % n_features],
            ));
        }
        all_triples.push(bsbm_prop_int(pid, BSBM_NUM1, ((i * 7 + 13) % 500) as i64));
    }

    // Offers
    for (i, &oid) in offers.iter().enumerate() {
        all_triples.push(bsbm_rel(oid, BSBM_OFFER_FOR, products[i % n_products]));
        all_triples.push(bsbm_rel(oid, BSBM_VENDOR, vendors[i % n_vendors]));
        all_triples.push(bsbm_prop_float(
            oid,
            BSBM_PRICE,
            10.0 + (i as f64 * 3.7) % 990.0,
        ));
    }

    // Reviews
    for (i, (&rid, &rvrid)) in reviews.iter().zip(reviewers.iter()).enumerate() {
        all_triples.push(bsbm_rel(rid, BSBM_REVIEW_FOR, products[i % n_products]));
        all_triples.push(bsbm_rel(rid, BSBM_REVIEWER, rvrid));
    }

    let mut last_ts = 0i64;
    for chunk in all_triples.chunks(BATCH) {
        let mut tx = store.begin();
        for t in chunk {
            tx.insert(t.clone());
        }
        last_ts = tx.commit().unwrap().0;
    }

    (store, dir, last_ts)
}

fn bench_bsbm_q1(c: &mut Criterion) {
    // Q1: products of ProductType[0] with Feature[0] and numericProp1 > 100.
    // Exercises: PSO scan (type lookup) + SPO scan (feature filter) + property scan.
    let (store, _dir, snap_ts_raw) = setup_bsbm_store();
    let snap_ts = polargraph_core::temporal::Timestamp(snap_ts_raw);
    let snap = store.snapshot(snap_ts);

    // Products are assigned to leaf types (indices 5–9); use type 5 for non-empty results.
    let type_node = bsbm_node_id("urn:bsbm:productType:5");
    let feature_node = bsbm_node_id("urn:bsbm:feature:0");
    let threshold: i64 = 100;

    let mut group = c.benchmark_group("bsbm");
    group.bench_function("q1_product_search", |b| {
        b.iter(|| {
            // PSO: all subjects with bsbm:type = type_node
            let type_triples = snap
                .scan_by_predicate_object(BSBM_TYPE, &type_node)
                .unwrap();
            let candidates: Vec<NodeId> = type_triples
                .into_iter()
                .filter_map(|t| match t {
                    Triple::Relation { subject, .. } => Some(subject),
                    _ => None,
                })
                .collect();

            // Feature + numeric filter
            let results: Vec<NodeId> =
                candidates
                    .into_iter()
                    .filter(|pid| {
                        let has_feat =
                            snap.scan_by_subject_predicate(pid, BSBM_FEATURE)
                                .ok()
                                .map(|ts| {
                                    ts.into_iter().any(|t| matches!(
                                t, Triple::Relation { object, .. } if object == feature_node
                            ))
                                })
                                .unwrap_or(false);
                        if !has_feat {
                            return false;
                        }
                        snap.scan_by_subject_predicate(pid, BSBM_NUM1)
                            .ok()
                            .and_then(|ts| {
                                ts.into_iter().find_map(|t| match t {
                                    Triple::Property {
                                        value: polargraph_core::value::Value::Int(n),
                                        ..
                                    } => Some(n),
                                    _ => None,
                                })
                            })
                            .is_some_and(|n| n > threshold)
                    })
                    .collect();
            std::hint::black_box(results)
        });
    });
    group.finish();
    drop(_dir);
}

fn bench_bsbm_q7(c: &mut Criterion) {
    // Q7: cheapest offer + review for a product (5-way join).
    // Exercises: POS scan × 2 + SPO scan × 4.
    let (store, _dir, snap_ts_raw) = setup_bsbm_store();
    let snap_ts = polargraph_core::temporal::Timestamp(snap_ts_raw);
    let snap = store.snapshot(snap_ts);

    // Product index 33 (middle of the 100-product dataset).
    let product = bsbm_node_id("urn:bsbm:product:33");

    let mut group = c.benchmark_group("bsbm");
    group.bench_function("q7_five_way_join", |b| {
        b.iter(|| {
            // POS: all offers for this product
            let offer_triples = snap
                .scan_by_predicate_object(BSBM_OFFER_FOR, &product)
                .unwrap();
            // POS: all reviews for this product
            let review_triples = snap
                .scan_by_predicate_object(BSBM_REVIEW_FOR, &product)
                .unwrap();

            // Enrich offers: fetch vendor + price
            let mut cheapest: Option<(NodeId, f64)> = None;
            for t in &offer_triples {
                let Some(oid) = (match t {
                    Triple::Relation { object, .. } => Some(*object),
                    _ => None,
                }) else {
                    continue;
                };
                let _vendor = snap
                    .scan_by_subject_predicate(&oid, BSBM_VENDOR)
                    .ok()
                    .and_then(|ts| {
                        ts.into_iter().find_map(|t| match t {
                            Triple::Relation { object, .. } => Some(object),
                            _ => None,
                        })
                    });
                let price = snap
                    .scan_by_subject_predicate(&oid, BSBM_PRICE)
                    .ok()
                    .and_then(|ts| {
                        ts.into_iter().find_map(|t| match t {
                            Triple::Property {
                                value: polargraph_core::value::Value::Float(f),
                                ..
                            } => Some(f),
                            _ => None,
                        })
                    })
                    .unwrap_or(f64::MAX);
                if cheapest.map_or(true, |(_, p)| price < p) {
                    cheapest = Some((oid, price));
                }
            }

            // Enrich reviews: fetch reviewer
            let _reviewers: Vec<NodeId> = review_triples
                .iter()
                .filter_map(|t| match t {
                    Triple::Relation { object, .. } => Some(*object),
                    _ => None,
                })
                .filter_map(|rid| {
                    snap.scan_by_subject_predicate(&rid, BSBM_REVIEWER)
                        .ok()
                        .and_then(|ts| {
                            ts.into_iter().find_map(|t| match t {
                                Triple::Relation { object, .. } => Some(object),
                                _ => None,
                            })
                        })
                })
                .collect();

            std::hint::black_box((cheapest, _reviewers))
        });
    });
    group.finish();
    drop(_dir);
}

// ── criterion wiring ──────────────────────────────────────────────────────────

criterion_group!(writes, bench_triple_writes);
criterion_group!(queries, bench_pattern_query);
criterion_group!(
    vector,
    bench_hnsw_insert,
    bench_hnsw_search,
    bench_hnsw_recall,
    bench_filtered_search
);
criterion_group!(annotations, bench_annotation_write, bench_annotation_scan);
criterion_group!(bsbm, bench_bsbm_q1, bench_bsbm_q7);
criterion_main!(writes, queries, vector, annotations, bsbm);
