//! Integration tests for the HNSW vector index wired into TripleStore.

use polargraph_core::id::NodeId;
use polargraph_core::schema::{NodeTypeDef, FieldDef, FieldKind, StorageMode, VectorSpaceDef};
use polargraph_storage::{NodeTypeRegistry, TripleStore};
use tempfile::TempDir;
use uuid::Uuid;

fn open_store() -> (TripleStore, TempDir) {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    (store, dir)
}

fn node(seed: u8) -> NodeId {
    NodeId(Uuid::from_bytes([seed; 16]))
}

fn unit_vec(dim: usize, hot: usize) -> Vec<f32> {
    let mut v = vec![0.0_f32; dim];
    v[hot % dim] = 1.0;
    v
}

// ── basic insert + search ─────────────────────────────────────────────────────

#[test]
fn insert_and_search_single_vector() {
    let (store, _dir) = open_store();
    let id = node(1);
    store.insert_vector("default", id, vec![1.0, 0.0, 0.0], StorageMode::Memory).unwrap();
    let results = store.search_vector("default", vec![1.0, 0.0, 0.0], 1);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, id);
    assert!((results[0].1 - 1.0).abs() < 1e-5);
}

#[test]
fn search_returns_empty_on_empty_store() {
    let (store, _dir) = open_store();
    let results = store.search_vector("default", vec![1.0, 0.0], 5);
    assert!(results.is_empty());
}

#[test]
fn search_on_unknown_space_returns_empty() {
    let (store, _dir) = open_store();
    let results = store.search_vector("no_such_space", vec![1.0, 0.0], 5);
    assert!(results.is_empty());
}

#[test]
fn nearest_neighbor_is_most_similar() {
    let (store, _dir) = open_store();
    let ids: Vec<NodeId> = (1u8..=8).map(node).collect();
    for (i, &id) in ids.iter().enumerate() {
        store.insert_vector("default", id, unit_vec(8, i), StorageMode::Memory).unwrap();
    }
    let results = store.search_vector("default", unit_vec(8, 5), 1);
    assert_eq!(results[0].0, ids[5]);
}

#[test]
fn top_k_results_are_ordered_by_similarity() {
    let (store, _dir) = open_store();
    let ids: Vec<NodeId> = (1u8..=5).map(node).collect();
    for (i, &id) in ids.iter().enumerate() {
        store.insert_vector("default", id, unit_vec(5, i), StorageMode::Memory).unwrap();
    }
    let results = store.search_vector("default", unit_vec(5, 0), 5);
    assert_eq!(results[0].0, ids[0]);
    assert!(results[0].1 >= results[1].1);
}

// ── named spaces ──────────────────────────────────────────────────────────────

#[test]
fn named_spaces_are_independent() {
    let (store, _dir) = open_store();
    let id_a = node(1);
    let id_b = node(2);
    store.insert_vector("space_a", id_a, unit_vec(4, 0), StorageMode::Memory).unwrap();
    store.insert_vector("space_b", id_b, unit_vec(4, 1), StorageMode::Memory).unwrap();

    // space_a does not contain id_b
    let res_a = store.search_vector("space_a", unit_vec(4, 0), 5);
    assert_eq!(res_a.len(), 1);
    assert_eq!(res_a[0].0, id_a);

    // space_b does not contain id_a
    let res_b = store.search_vector("space_b", unit_vec(4, 1), 5);
    assert_eq!(res_b.len(), 1);
    assert_eq!(res_b[0].0, id_b);
}

#[test]
fn named_spaces_persist_across_reopen() {
    let dir = TempDir::new().unwrap();
    let id_a = node(1);
    let id_b = node(2);

    {
        let store = TripleStore::open(dir.path()).unwrap();
        store.insert_vector("space_a", id_a, unit_vec(4, 0), StorageMode::Memory).unwrap();
        store.insert_vector("space_b", id_b, unit_vec(4, 1), StorageMode::Memory).unwrap();
    }

    let store2 = TripleStore::open(dir.path()).unwrap();
    let res_a = store2.search_vector("space_a", unit_vec(4, 0), 1);
    assert_eq!(res_a.len(), 1);
    assert_eq!(res_a[0].0, id_a);

    let res_b = store2.search_vector("space_b", unit_vec(4, 1), 1);
    assert_eq!(res_b.len(), 1);
    assert_eq!(res_b[0].0, id_b);
}

// ── persistence ───────────────────────────────────────────────────────────────

#[test]
fn vector_index_persists_across_reopen() {
    let dir = TempDir::new().unwrap();
    let ids: Vec<NodeId> = (1u8..=4).map(node).collect();

    {
        let store = TripleStore::open(dir.path()).unwrap();
        for (i, &id) in ids.iter().enumerate() {
            store.insert_vector("default", id, unit_vec(4, i), StorageMode::Memory).unwrap();
        }
    }

    let store2 = TripleStore::open(dir.path()).unwrap();
    let results = store2.search_vector("default", unit_vec(4, 2), 1);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, ids[2]);
}

// ── update ────────────────────────────────────────────────────────────────────

#[test]
fn insert_vector_twice_updates_index() {
    let (store, _dir) = open_store();
    let id = node(1);
    store.insert_vector("default", id, unit_vec(2, 0), StorageMode::Memory).unwrap();
    store.insert_vector("default", id, unit_vec(2, 1), StorageMode::Memory).unwrap();

    let id2 = node(2);
    store.insert_vector("default", id2, unit_vec(2, 0), StorageMode::Memory).unwrap();

    let results = store.search_vector("default", unit_vec(2, 1), 1);
    assert_eq!(results[0].0, id);
}

// ── search_vector_in_set ──────────────────────────────────────────────────────

#[test]
fn search_in_set_returns_only_allowed_nodes() {
    let (store, _dir) = open_store();
    let ids: Vec<NodeId> = (1u8..=6).map(node).collect();
    for (i, &id) in ids.iter().enumerate() {
        store.insert_vector("default", id, unit_vec(6, i), StorageMode::Memory).unwrap();
    }

    // Best match for dim 0 is ids[0], but only ids[1..=3] are allowed.
    let allowed = &ids[1..=3];
    let results = store.search_vector_in_set("default", &unit_vec(6, 0), 2, allowed);
    assert!(results.iter().all(|(id, _)| allowed.contains(id)));
    assert!(!results.iter().any(|(id, _)| *id == ids[0]));
}

#[test]
fn search_in_set_on_unknown_space_returns_empty() {
    let (store, _dir) = open_store();
    let allowed = vec![node(1)];
    let results = store.search_vector_in_set("no_such_space", &[1.0, 0.0], 5, &allowed);
    assert!(results.is_empty());
}

#[test]
fn search_in_set_skips_nodes_not_in_index() {
    let (store, _dir) = open_store();
    store.insert_vector("default", node(1), unit_vec(4, 0), StorageMode::Memory).unwrap();
    // node(2) was never inserted
    let allowed = vec![node(1), node(2)];
    let results = store.search_vector_in_set("default", &unit_vec(4, 0), 5, &allowed);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, node(1));
}

// ── batch_insert_vectors ──────────────────────────────────────────────────────

#[test]
fn batch_insert_all_items() {
    let (store, _dir) = open_store();
    let ids: Vec<NodeId> = (1u8..=4).map(node).collect();
    let items: Vec<(NodeId, Vec<f32>)> = ids
        .iter()
        .enumerate()
        .map(|(i, &id)| (id, unit_vec(4, i)))
        .collect();

    let (count, errors) = store.batch_insert_vectors("default", &items, StorageMode::Memory);
    assert_eq!(count, 4);
    assert!(errors.is_empty());

    let results = store.search_vector("default", unit_vec(4, 2), 1);
    assert_eq!(results[0].0, ids[2]);
}

#[test]
fn batch_insert_empty_is_noop() {
    let (store, _dir) = open_store();
    let (count, errors) = store.batch_insert_vectors("default", &[], StorageMode::Memory);
    assert_eq!(count, 0);
    assert!(errors.is_empty());
}

// ── vector space def + dimension validation via registry ──────────────────────

#[test]
fn node_type_with_vector_space_persists() {
    let dir = TempDir::new().unwrap();
    let def = NodeTypeDef::new("Widget", vec![
        FieldDef::required("name", FieldKind::Text),
    ]).with_vector_space(VectorSpaceDef::new("widget_space", 4));

    {
        let store = TripleStore::open(dir.path()).unwrap();
        let reg = NodeTypeRegistry::new(store).unwrap();
        reg.register_type(def.clone()).unwrap();
    }

    let store2 = TripleStore::open(dir.path()).unwrap();
    let reg2 = NodeTypeRegistry::new(store2).unwrap();
    let loaded = reg2.get_type("Widget").unwrap();
    assert_eq!(loaded.vector_space, def.vector_space);
}

#[test]
fn get_space_def_returns_registered_space() {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    let reg = NodeTypeRegistry::new(store).unwrap();

    let space = VectorSpaceDef::new("my_space", 128).with_model("text-embedding-3-small");
    let def = NodeTypeDef::new("Doc", vec![]).with_vector_space(space.clone());
    reg.register_type(def).unwrap();

    let found = reg.get_space_def("my_space").unwrap();
    assert_eq!(found.dimensions, 128);
    assert_eq!(found.embedding_model.as_deref(), Some("text-embedding-3-small"));
}

#[test]
fn get_space_def_returns_none_for_unknown_space() {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    let reg = NodeTypeRegistry::new(store).unwrap();
    assert!(reg.get_space_def("no_such_space").is_none());
}

// ── memmap storage mode ───────────────────────────────────────────────────────

/// Insert 20 vectors in mmap mode; confirm top-1 search returns the correct node.
#[test]
fn mmap_insert_and_search() {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    let ids: Vec<NodeId> = (1u8..=20).map(node).collect();
    for (i, &id) in ids.iter().enumerate() {
        store
            .insert_vector("mspace", id, unit_vec(20, i), StorageMode::Mmap)
            .unwrap();
    }
    let results = store.search_vector("mspace", unit_vec(20, 7), 1);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, ids[7]);
}

/// Recall parity: same vectors inserted in memory vs mmap mode should return
/// the same top-k results.
#[test]
fn mmap_recall_matches_in_memory() {
    const N: usize = 50;
    const DIM: usize = 16;
    // Build deterministic vectors.
    let ids: Vec<NodeId> = (0u8..N as u8).map(node).collect();
    let vecs: Vec<Vec<f32>> = (0..N)
        .map(|i| {
            let mut v = vec![0.0_f32; DIM];
            v[i % DIM] += 1.0;
            v[(i + 3) % DIM] += 0.5;
            v
        })
        .collect();

    let dir_mem  = TempDir::new().unwrap();
    let dir_mmap = TempDir::new().unwrap();

    let store_mem  = TripleStore::open(dir_mem.path()).unwrap();
    let store_mmap = TripleStore::open(dir_mmap.path()).unwrap();

    for (i, &id) in ids.iter().enumerate() {
        store_mem.insert_vector("s", id, vecs[i].clone(), StorageMode::Memory).unwrap();
        store_mmap.insert_vector("s", id, vecs[i].clone(), StorageMode::Mmap).unwrap();
    }

    let query = vecs[17].clone();
    let k = 5;
    let res_mem  = store_mem.search_vector("s", query.clone(), k);
    let res_mmap = store_mmap.search_vector("s", query, k);

    assert_eq!(res_mem.len(), res_mmap.len());
    for ((id_m, _), (id_mm, _)) in res_mem.iter().zip(res_mmap.iter()) {
        assert_eq!(id_m, id_mm, "mmap and memory results diverged");
    }
}

/// Insert in mmap mode, close the store, reopen — same nearest neighbor.
#[test]
fn mmap_persists_across_reopen() {
    let dir = TempDir::new().unwrap();
    let ids: Vec<NodeId> = (1u8..=10).map(node).collect();

    {
        let store = TripleStore::open(dir.path()).unwrap();
        for (i, &id) in ids.iter().enumerate() {
            store
                .insert_vector("mspace", id, unit_vec(10, i), StorageMode::Mmap)
                .unwrap();
        }
    }

    let store2 = TripleStore::open(dir.path()).unwrap();
    let results = store2.search_vector("mspace", unit_vec(10, 4), 1);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, ids[4]);
}

/// batch_insert_vectors in mmap mode: insert and verify correct top-1 result.
#[test]
fn mmap_batch_insert_and_search() {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    // Use 10 nodes so the HNSW graph is small enough for exact recall.
    let ids: Vec<NodeId> = (1u8..=10).map(node).collect();
    let items: Vec<(NodeId, Vec<f32>)> = ids
        .iter()
        .enumerate()
        .map(|(i, &id)| (id, unit_vec(10, i)))
        .collect();

    let (count, errors) = store.batch_insert_vectors("bs", &items, StorageMode::Mmap);
    assert_eq!(count, 10);
    assert!(errors.is_empty());

    let results = store.search_vector("bs", unit_vec(10, 5), 1);
    assert_eq!(results[0].0, ids[5]);
}

// ── HNSW recall at scale ──────────────────────────────────────────────────────

/// LCG-based pseudo-random unit vector for deterministic recall testing.
fn lcg_unit_vec(mut seed: u64, dims: usize) -> Vec<f32> {
    let raw: Vec<f32> = (0..dims)
        .map(|_| {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (seed >> 11) as f32 / (1u64 << 53) as f32 * 2.0 - 1.0
        })
        .collect();
    let norm = raw.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
    raw.into_iter().map(|x| x / norm).collect()
}

/// Insert 10 000 random 128-d vectors and verify recall@10 stays above 90%.
///
/// This exercises the HNSW index at realistic scale. Recall is measured by
/// comparing the ANN top-10 results against a brute-force cosine sort.
/// A threshold of 90% is conservative enough to be deterministic across
/// platforms while still catching serious regressions in the index quality.
#[test]
fn hnsw_recall_at_10_with_10k_insertions() {
    const N: usize = 10_000;
    const DIMS: usize = 128;
    const K: usize = 10;
    const MIN_RECALL: f32 = 0.90;

    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();

    // Build deterministic items.
    let items: Vec<(NodeId, Vec<f32>)> = (0..N as u64)
        .map(|i| {
            let id = NodeId(uuid::Uuid::from_u128(i as u128 + 0x10000));
            let v = lcg_unit_vec(i.wrapping_mul(0xdeadbeef), DIMS);
            (id, v)
        })
        .collect();

    let (inserted, errors) = store.batch_insert_vectors("recall", &items, StorageMode::Memory);
    assert_eq!(inserted, N, "all items should be inserted");
    assert!(errors.is_empty(), "no errors during batch insert");

    // Run recall over 20 random queries.
    let mut total_recall = 0.0f32;
    let num_queries = 20usize;
    for q in 0..num_queries {
        let query = lcg_unit_vec(q as u64 * 0x1234 + 0xabcd, DIMS);

        // Use ef=200 (well above K=10) to get reliable high recall at 10k nodes.
        let ann: std::collections::HashSet<NodeId> = store
            .search_vector_ef("recall", &query, K, 200)
            .into_iter()
            .map(|(id, _)| id)
            .collect();

        // Brute-force: sort all N vectors by cosine similarity (dot product of unit vecs).
        let mut scores: Vec<(NodeId, f32)> = items
            .iter()
            .map(|(id, v)| {
                let sim: f32 = v.iter().zip(&query).map(|(a, b)| a * b).sum();
                (*id, sim)
            })
            .collect();
        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        let true_nn: std::collections::HashSet<NodeId> =
            scores.iter().take(K).map(|(id, _)| *id).collect();

        let recall = ann.intersection(&true_nn).count() as f32 / K as f32;
        total_recall += recall;
    }

    let avg_recall = total_recall / num_queries as f32;
    assert!(
        avg_recall >= MIN_RECALL,
        "recall@{K} at 10k nodes = {avg_recall:.2} < {MIN_RECALL} threshold"
    );
}
