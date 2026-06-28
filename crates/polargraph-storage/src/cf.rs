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

/// Full-text / trigram inverted index.
/// Key layout: [trigram(3)][pred_id LE(4)][subject(16)] = 23 bytes, value empty.
pub const TRI: &str = "tri";

/// RDF-star edge-property annotations (scalar values keyed by edge).
/// Key layout: [edge_id(16)][pred_id BE(4)][tt BE(8)] = 28 bytes.
/// Value: same codec as Property triple (DISC_PROPERTY + temporal + json_value).
pub const EPA: &str = "epa";

/// RDF-star edge-relation annotations (node references keyed by edge).
/// Key layout: [edge_id(16)][pred_id BE(4)][obj_id(16)][tt BE(8)] = 44 bytes.
/// Value: [vt_start BE(8)][vt_end BE(8)] = 16 bytes.
pub const EPO: &str = "epo";

/// Predicate-first secondary index for edge property annotations.
/// Key layout: [pred_id BE(4)][edge_id(16)][tt BE(8)] = 28 bytes.
/// Value: same bytes as the corresponding EPA entry.
pub const PEA: &str = "pea";

/// Derived triple store for OWL 2 RL materialized facts.
/// Key layout: same as SPO — [subject(16)][pred_id BE(4)][object(16)][tt BE(8)] = 44 bytes.
/// Value: same codec as SPO relation entries.
/// Derived triples are kept separate from base data and can be wiped and rebuilt cleanly.
pub const DRV: &str = "drv";

pub const ALL: &[&str] = &[SPO, SOP, PSO, POS, OSP, OPS, META, HNSW, TRI, EPA, EPO, PEA, DRV];
