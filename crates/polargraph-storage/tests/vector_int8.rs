//! int8 vector quantization (docs/design/step9-inference-vectors-stats.md, 9c).

use polargraph_core::{id::NodeId, schema::StorageMode};
use polargraph_storage::{hnsw::SpaceOptions, TripleStore};
use tempfile::TempDir;

const INT8: SpaceOptions = SpaceOptions {
    mode: StorageMode::Memory,
    int8: true,
};

/// Deterministic pseudo-random vectors (xorshift).
fn vectors(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut x = seed;
    let mut next = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        (x % 2000) as f32 / 1000.0 - 1.0
    };
    (0..n).map(|_| (0..dim).map(|_| next()).collect()).collect()
}

fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    dot / (na * nb)
}

#[test]
fn int8_recall_and_exact_scores() {
    const N: usize = 2000;
    const DIM: usize = 64;
    const K: usize = 10;
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    let ids: Vec<NodeId> = (0..N).map(|_| NodeId::new()).collect();
    let data = vectors(N, DIM, 0x2545F4914F6CDD1D);
    let items: Vec<(NodeId, Vec<f32>)> = ids.iter().copied().zip(data.clone()).collect();
    let (n, errs) = store.batch_insert_vectors("q", &items, INT8);
    assert_eq!((n, errs.len()), (N, 0));

    let queries = vectors(50, DIM, 0x9E3779B97F4A7C15);
    let mut hits = 0;
    for q in &queries {
        // Ground truth: brute-force exact cosine.
        let mut exact: Vec<(usize, f32)> = data
            .iter()
            .enumerate()
            .map(|(i, v)| (i, cosine(v, q)))
            .collect();
        exact.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        let truth: Vec<NodeId> = exact.iter().take(K).map(|(i, _)| ids[*i]).collect();

        let got = store.search_vector_ef("q", q, K, 100);
        assert_eq!(got.len(), K);
        hits += got.iter().filter(|(id, _)| truth.contains(id)).count();
        // Scores are exact (re-ranked with the f32 vectors).
        for (id, score) in &got {
            let i = ids.iter().position(|x| x == id).unwrap();
            assert!((score - cosine(&data[i], q)).abs() < 1e-5);
        }
    }
    let recall = hits as f32 / (queries.len() * K) as f32;
    eprintln!("int8 recall@10 = {recall}");
    assert!(recall >= 0.97, "recall@10 = {recall}");
}

#[test]
fn codes_persist_and_existing_spaces_convert() {
    let dir = TempDir::new().unwrap();
    let ids: Vec<NodeId> = (0..200).map(|_| NodeId::new()).collect();
    let data = vectors(200, 32, 7);
    {
        let store = TripleStore::open(dir.path()).unwrap();
        // A full-precision memory space…
        for (id, v) in ids.iter().zip(&data).take(150) {
            store
                .insert_vector("s", *id, v.clone(), StorageMode::Memory)
                .unwrap();
        }
        // …quantized on its next insert.
        for (id, v) in ids.iter().zip(&data).skip(150) {
            store.insert_vector("s", *id, v.clone(), INT8).unwrap();
        }
        assert!(
            dir.path().join("vectors/s.vecs").exists(),
            "full vectors moved to disk"
        );
    }
    // Reopen: codes load, search still finds each vector exactly.
    let store = TripleStore::open(dir.path()).unwrap();
    for (id, v) in ids.iter().zip(&data).step_by(17) {
        let got = store.search_vector_ef("s", v, 1, 64);
        assert_eq!(got[0].0, *id);
        assert!((got[0].1 - 1.0).abs() < 1e-5);
    }
    // More inserts after reopen keep working.
    let extra = NodeId::new();
    let v = vectors(1, 32, 99).remove(0);
    store.insert_vector("s", extra, v.clone(), INT8).unwrap();
    assert_eq!(store.search_vector_ef("s", &v, 1, 64)[0].0, extra);
}
