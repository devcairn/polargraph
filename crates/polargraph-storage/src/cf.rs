//! Column family names (storage format v3, see `docs/design/v3-key-layout.md`).

/// The eight quad-index column families. Key: 48 bytes, see [`crate::keys::Order`].
pub const SPOG: &str = "spog";
pub const SOPG: &str = "sopg";
pub const PSOG: &str = "psog";
pub const POSG: &str = "posg";
pub const OSPG: &str = "ospg";
pub const OPSG: &str = "opsg";
pub const GSPO: &str = "gspo";
pub const GPOS: &str = "gpos";

/// Predicate / graph intern tables, timestamp oracle, format and migration versions.
pub const META: &str = "meta";

/// HNSW vector index — one entry per node, plus a `__ep` entry-point record.
pub const HNSW: &str = "hnsw";

/// Trigram full-text index.
/// Key: `[trigram(3)][pred_id BE(4)][g BE(4)][subject(16)]` = 27 bytes, value empty.
pub const TRI: &str = "trig";

/// RDF-star edge-property annotations.
/// Key: `[edge_id(16)][pred_id(4)][g(4)][tt(8)]` = 32 bytes. Value: Property codec.
pub const EPA: &str = "epag";

/// RDF-star edge-relation annotations.
/// Key: `[edge_id(16)][pred_id(4)][obj_id(16)][g(4)][tt(8)]` = 48 bytes.
/// Value: `[vt_start BE(8)][vt_end BE(8)]`.
pub const EPO: &str = "epog";

/// Predicate-first index over EPA.
/// Key: `[pred_id(4)][edge_id(16)][g(4)][tt(8)]` = 32 bytes. Value: same as EPA.
pub const PEA: &str = "peag";

/// OWL 2 RL derived facts, `spog` key layout (g = the derived graph).
pub const DRV: &str = "drvg";

/// IRI dictionary: `[node_id(16)]` → IRI (UTF-8) for hashed NodeIds.
pub const IRI: &str = "iri";

/// Out-of-line property values: `[value_hash(16)]` → `[disc][payload]`.
pub const BLOB: &str = "blob";

/// Change log for `Subscribe`: `[commit_ts BE(8)]` → one commit's changes
/// (see [`crate::changes`]).
pub const CHG: &str = "chg";

/// Counters (step 9d): `[namespace][0x00][node(16)]` → `i64` LE, updated
/// with an add merge operator — not versioned, not in the change log.
pub const STS: &str = "sts";

/// Every column family of the current format.
pub const ALL: &[&str] = &[
    SPOG, SOPG, PSOG, POSG, OSPG, OPSG, GSPO, GPOS, META, HNSW, TRI, EPA, EPO, PEA, DRV, IRI, BLOB,
    CHG, STS,
];

/// Column families of storage format v2, read only by `migrate_v3` and
/// dropped once a store is migrated.
pub mod v2 {
    pub const SPO: &str = "spo";
    pub const SOP: &str = "sop";
    pub const PSO: &str = "pso";
    pub const POS: &str = "pos";
    pub const OSP: &str = "osp";
    pub const OPS: &str = "ops";
    pub const TRI: &str = "tri";
    pub const EPA: &str = "epa";
    pub const EPO: &str = "epo";
    pub const PEA: &str = "pea";
    pub const DRV: &str = "drv";

    pub const ALL: &[&str] = &[SPO, SOP, PSO, POS, OSP, OPS, TRI, EPA, EPO, PEA, DRV];
}
