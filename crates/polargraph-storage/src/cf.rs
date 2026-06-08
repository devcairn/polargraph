//! Column family names and helpers.

/// The six triple-index column families.
pub const SPO: &str = "spo";
pub const SOP: &str = "sop";
pub const PSO: &str = "pso";
pub const POS: &str = "pos";
pub const OSP: &str = "osp";
pub const OPS: &str = "ops";

/// A metadata CF for predicate interning and schema info.
pub const META: &str = "meta";

/// HNSW vector index — one entry per node, plus a `__ep` entry-point record.
pub const HNSW: &str = "hnsw";

pub const ALL: &[&str] = &[SPO, SOP, PSO, POS, OSP, OPS, META, HNSW];
