//! Index key encoding and decoding.
//!
//! Keys are fixed-width byte sequences so that RocksDB's default
//! lexicographic ordering gives us correct sorted-range scans.
//!
//! Component widths:
//!   NodeId  = 16 bytes (UUID v7, big-endian)
//!   PredId  = 4 bytes  (u32 big-endian interned ID)
//!   tt      = 8 bytes  (i64 big-endian microseconds since epoch)
//!
//! Total key size per CF: 16 + 4 + 16 + 8 = 44 bytes
//!
//! CF permutations and their byte layout:
//!   SPO  [subject(16)][pred(4)][object(16)][tt(8)]
//!   SOP  [subject(16)][object(16)][pred(4)][tt(8)]
//!   PSO  [pred(4)][subject(16)][object(16)][tt(8)]
//!   POS  [pred(4)][object(16)][subject(16)][tt(8)]
//!   OSP  [object(16)][subject(16)][pred(4)][tt(8)]
//!   OPS  [object(16)][pred(4)][subject(16)][tt(8)]

use crate::error::StorageError;
use polargraph_core::{
    id::{EdgeId, NodeId},
    temporal::Timestamp,
};
use uuid::Uuid;

/// Interned predicate ID — 32-bit so keys stay compact.
pub type PredId = u32;

/// Sentinel NodeId used in the object slot for property triples.
pub const PROPERTY_SENTINEL: [u8; 16] = [0xFF; 16];

/// Returns true if a 16-byte NodeId slot bytes are the property sentinel.
#[inline]
pub fn is_property_sentinel(bytes: &[u8]) -> bool {
    bytes == PROPERTY_SENTINEL
}

// ── private decode helpers ────────────────────────────────────────────────────

#[inline]
fn node_id_from(b: &[u8]) -> NodeId {
    NodeId(Uuid::from_bytes(b.try_into().expect("16 bytes")))
}
#[inline]
fn pred_from(b: &[u8]) -> PredId {
    u32::from_be_bytes(b.try_into().expect("4 bytes"))
}
#[inline]
fn tt_from(b: &[u8]) -> Timestamp {
    Timestamp::from_be_bytes(b.try_into().expect("8 bytes"))
}

// ── decoded key types ─────────────────────────────────────────────────────────

pub struct DecodedSpo {
    pub subject: NodeId,
    pub pred_id: PredId,
    pub object: NodeId,
    pub tt: Timestamp,
}

pub struct DecodedPso {
    pub pred_id: PredId,
    pub subject: NodeId,
    pub object: NodeId,
    pub tt: Timestamp,
}

pub struct DecodedPos {
    pub pred_id: PredId,
    pub object: NodeId,
    pub subject: NodeId,
    pub tt: Timestamp,
}

pub struct DecodedSop {
    pub subject: NodeId,
    pub object: NodeId,
    pub pred_id: PredId,
    pub tt: Timestamp,
}

pub struct DecodedOsp {
    pub object: NodeId,
    pub subject: NodeId,
    pub pred_id: PredId,
    pub tt: Timestamp,
}

// ── encode ────────────────────────────────────────────────────────────────────

// SPO: [subject(16)][pred(4)][object(16)][tt(8)]
pub fn encode_spo(s: &NodeId, p: PredId, o: &NodeId, tt: Timestamp) -> [u8; 44] {
    let mut k = [0u8; 44];
    k[0..16].copy_from_slice(s.as_bytes());
    k[16..20].copy_from_slice(&p.to_be_bytes());
    k[20..36].copy_from_slice(o.as_bytes());
    k[36..44].copy_from_slice(&tt.to_be_bytes());
    k
}

// SOP: [subject(16)][object(16)][pred(4)][tt(8)]
pub fn encode_sop(s: &NodeId, o: &NodeId, p: PredId, tt: Timestamp) -> [u8; 44] {
    let mut k = [0u8; 44];
    k[0..16].copy_from_slice(s.as_bytes());
    k[16..32].copy_from_slice(o.as_bytes());
    k[32..36].copy_from_slice(&p.to_be_bytes());
    k[36..44].copy_from_slice(&tt.to_be_bytes());
    k
}

// PSO: [pred(4)][subject(16)][object(16)][tt(8)]
pub fn encode_pso(p: PredId, s: &NodeId, o: &NodeId, tt: Timestamp) -> [u8; 44] {
    let mut k = [0u8; 44];
    k[0..4].copy_from_slice(&p.to_be_bytes());
    k[4..20].copy_from_slice(s.as_bytes());
    k[20..36].copy_from_slice(o.as_bytes());
    k[36..44].copy_from_slice(&tt.to_be_bytes());
    k
}

// POS: [pred(4)][object(16)][subject(16)][tt(8)]
pub fn encode_pos(p: PredId, o: &NodeId, s: &NodeId, tt: Timestamp) -> [u8; 44] {
    let mut k = [0u8; 44];
    k[0..4].copy_from_slice(&p.to_be_bytes());
    k[4..20].copy_from_slice(o.as_bytes());
    k[20..36].copy_from_slice(s.as_bytes());
    k[36..44].copy_from_slice(&tt.to_be_bytes());
    k
}

// OSP: [object(16)][subject(16)][pred(4)][tt(8)]
pub fn encode_osp(o: &NodeId, s: &NodeId, p: PredId, tt: Timestamp) -> [u8; 44] {
    let mut k = [0u8; 44];
    k[0..16].copy_from_slice(o.as_bytes());
    k[16..32].copy_from_slice(s.as_bytes());
    k[32..36].copy_from_slice(&p.to_be_bytes());
    k[36..44].copy_from_slice(&tt.to_be_bytes());
    k
}

// ── Trigram CF key encoding ───────────────────────────────────────────────────

/// Extract all 3-gram byte sequences from `text` (UTF-8 byte-level sliding window).
///
/// For text shorter than 3 bytes, the single gram is zero-padded to 3 bytes.
/// Returns a deduplicated set. An empty string returns an empty vec.
pub fn extract_trigrams(text: &str) -> Vec<[u8; 3]> {
    let bytes = text.as_bytes();
    let mut set = std::collections::HashSet::new();
    if bytes.is_empty() {
        return vec![];
    }
    if bytes.len() < 3 {
        let mut gram = [0u8; 3];
        gram[..bytes.len()].copy_from_slice(bytes);
        set.insert(gram);
    } else {
        for window in bytes.windows(3) {
            set.insert([window[0], window[1], window[2]]);
        }
    }
    set.into_iter().collect()
}

/// TRI key layout: `[trigram(3)][pred_id LE(4)][subject(16)]` = 23 bytes. Value is empty.
pub fn encode_tri(trigram: [u8; 3], pred_id: PredId, subject: &NodeId) -> [u8; 23] {
    let mut k = [0u8; 23];
    k[0..3].copy_from_slice(&trigram);
    k[3..7].copy_from_slice(&pred_id.to_le_bytes());
    k[7..23].copy_from_slice(subject.as_bytes());
    k
}

/// TRI prefix for scanning all subjects that have `(trigram, pred_id)`.
pub fn tri_prefix_tp(trigram: [u8; 3], pred_id: PredId) -> [u8; 7] {
    let mut k = [0u8; 7];
    k[0..3].copy_from_slice(&trigram);
    k[3..7].copy_from_slice(&pred_id.to_le_bytes());
    k
}

/// Extract the subject `NodeId` from a 23-byte TRI key.
pub fn decode_tri_subject(key: &[u8]) -> Result<NodeId, StorageError> {
    check_len(key, 23, "TRI")?;
    Ok(node_id_from(&key[7..23]))
}

// OPS: [object(16)][pred(4)][subject(16)][tt(8)]
pub fn encode_ops(o: &NodeId, p: PredId, s: &NodeId, tt: Timestamp) -> [u8; 44] {
    let mut k = [0u8; 44];
    k[0..16].copy_from_slice(o.as_bytes());
    k[16..20].copy_from_slice(&p.to_be_bytes());
    k[20..36].copy_from_slice(s.as_bytes());
    k[36..44].copy_from_slice(&tt.to_be_bytes());
    k
}

// ── decode ────────────────────────────────────────────────────────────────────

pub fn decode_spo(key: &[u8]) -> Result<DecodedSpo, StorageError> {
    check_len(key, 44, "SPO")?;
    Ok(DecodedSpo {
        subject: node_id_from(&key[0..16]),
        pred_id: pred_from(&key[16..20]),
        object: node_id_from(&key[20..36]),
        tt: tt_from(&key[36..44]),
    })
}

pub fn decode_sop(key: &[u8]) -> Result<DecodedSop, StorageError> {
    check_len(key, 44, "SOP")?;
    Ok(DecodedSop {
        subject: node_id_from(&key[0..16]),
        object: node_id_from(&key[16..32]),
        pred_id: pred_from(&key[32..36]),
        tt: tt_from(&key[36..44]),
    })
}

pub fn decode_pso(key: &[u8]) -> Result<DecodedPso, StorageError> {
    check_len(key, 44, "PSO")?;
    Ok(DecodedPso {
        pred_id: pred_from(&key[0..4]),
        subject: node_id_from(&key[4..20]),
        object: node_id_from(&key[20..36]),
        tt: tt_from(&key[36..44]),
    })
}

pub fn decode_pos(key: &[u8]) -> Result<DecodedPos, StorageError> {
    check_len(key, 44, "POS")?;
    Ok(DecodedPos {
        pred_id: pred_from(&key[0..4]),
        object: node_id_from(&key[4..20]),
        subject: node_id_from(&key[20..36]),
        tt: tt_from(&key[36..44]),
    })
}

pub fn decode_osp(key: &[u8]) -> Result<DecodedOsp, StorageError> {
    check_len(key, 44, "OSP")?;
    Ok(DecodedOsp {
        object: node_id_from(&key[0..16]),
        subject: node_id_from(&key[16..32]),
        pred_id: pred_from(&key[32..36]),
        tt: tt_from(&key[36..44]),
    })
}

#[inline]
fn check_len(key: &[u8], expected: usize, name: &str) -> Result<(), StorageError> {
    if key.len() != expected {
        Err(StorageError::KeyDecode(format!(
            "{name} key must be {expected} bytes, got {}",
            key.len()
        )))
    } else {
        Ok(())
    }
}

// ── RDF-star EPA / EPO key encoding ──────────────────────────────────────────

/// EPA key layout: `[edge_id(16)][pred_id BE(4)][tt BE(8)]` = 28 bytes.
pub fn encode_epa_key(edge: &EdgeId, pred_id: PredId, tt: Timestamp) -> [u8; 28] {
    let mut k = [0u8; 28];
    k[0..16].copy_from_slice(edge.as_bytes());
    k[16..20].copy_from_slice(&pred_id.to_be_bytes());
    k[20..28].copy_from_slice(&tt.to_be_bytes());
    k
}

/// Decoded EPA key.
pub struct DecodedEpa {
    pub edge_id: EdgeId,
    pub pred_id: PredId,
    pub tt: Timestamp,
}

pub fn decode_epa_key(key: &[u8]) -> Result<DecodedEpa, StorageError> {
    check_len(key, 28, "EPA")?;
    Ok(DecodedEpa {
        edge_id: EdgeId(Uuid::from_bytes(key[0..16].try_into().expect("16 bytes"))),
        pred_id: pred_from(&key[16..20]),
        tt: tt_from(&key[20..28]),
    })
}

/// EPO key layout: `[edge_id(16)][pred_id BE(4)][obj_id(16)][tt BE(8)]` = 44 bytes.
pub fn encode_epo_key(edge: &EdgeId, pred_id: PredId, obj: &NodeId, tt: Timestamp) -> [u8; 44] {
    let mut k = [0u8; 44];
    k[0..16].copy_from_slice(edge.as_bytes());
    k[16..20].copy_from_slice(&pred_id.to_be_bytes());
    k[20..36].copy_from_slice(obj.as_bytes());
    k[36..44].copy_from_slice(&tt.to_be_bytes());
    k
}

/// Decoded EPO key.
pub struct DecodedEpo {
    pub edge_id: EdgeId,
    pub pred_id: PredId,
    pub object: NodeId,
    pub tt: Timestamp,
}

pub fn decode_epo_key(key: &[u8]) -> Result<DecodedEpo, StorageError> {
    check_len(key, 44, "EPO")?;
    Ok(DecodedEpo {
        edge_id: EdgeId(Uuid::from_bytes(key[0..16].try_into().expect("16 bytes"))),
        pred_id: pred_from(&key[16..20]),
        object: node_id_from(&key[20..36]),
        tt: tt_from(&key[36..44]),
    })
}

/// EPA prefix: all property annotations for a given edge.
pub fn epa_prefix_edge(edge: &EdgeId) -> [u8; 16] {
    *edge.as_bytes()
}

/// EPA prefix: all property annotations for (edge, predicate).
pub fn epa_prefix_edge_pred(edge: &EdgeId, pred_id: PredId) -> [u8; 20] {
    let mut k = [0u8; 20];
    k[0..16].copy_from_slice(edge.as_bytes());
    k[16..20].copy_from_slice(&pred_id.to_be_bytes());
    k
}

/// EPO prefix: all relation annotations for a given edge.
pub fn epo_prefix_edge(edge: &EdgeId) -> [u8; 16] {
    *edge.as_bytes()
}

// ── PEA (predicate-first annotation index) key encoding ──────────────────────

/// PEA key layout: `[pred_id BE(4)][edge_id(16)][tt BE(8)]` = 28 bytes.
pub fn encode_pea_key(pred_id: PredId, edge: &EdgeId, tt: Timestamp) -> [u8; 28] {
    let mut k = [0u8; 28];
    k[0..4].copy_from_slice(&pred_id.to_be_bytes());
    k[4..20].copy_from_slice(edge.as_bytes());
    k[20..28].copy_from_slice(&tt.to_be_bytes());
    k
}

/// Decoded PEA key.
pub struct DecodedPea {
    pub pred_id: PredId,
    pub edge_id: EdgeId,
    pub tt: Timestamp,
}

pub fn decode_pea_key(key: &[u8]) -> Result<DecodedPea, StorageError> {
    check_len(key, 28, "PEA")?;
    Ok(DecodedPea {
        pred_id: pred_from(&key[0..4]),
        edge_id: EdgeId(Uuid::from_bytes(key[4..20].try_into().expect("16 bytes"))),
        tt: tt_from(&key[20..28]),
    })
}

/// PEA prefix: all property annotations with a given predicate.
pub fn pea_prefix_pred(pred_id: PredId) -> [u8; 4] {
    pred_id.to_be_bytes()
}

/// PEA prefix: all annotations for a specific (predicate, edge) pair.
pub fn pea_prefix_pred_edge(pred_id: PredId, edge: &EdgeId) -> [u8; 20] {
    let mut k = [0u8; 20];
    k[0..4].copy_from_slice(&pred_id.to_be_bytes());
    k[4..20].copy_from_slice(edge.as_bytes());
    k
}

// ── prefix helpers for range scans ───────────────────────────────────────────

/// SPO prefix: all triples with the given subject.
pub fn spo_prefix_s(s: &NodeId) -> [u8; 16] {
    *s.as_bytes()
}

/// SPO prefix: all triples with given (subject, predicate).
pub fn spo_prefix_sp(s: &NodeId, p: PredId) -> [u8; 20] {
    let mut k = [0u8; 20];
    k[0..16].copy_from_slice(s.as_bytes());
    k[16..20].copy_from_slice(&p.to_be_bytes());
    k
}

/// SPO prefix: exactly the triple (subject, predicate, object) — all tt variants.
pub fn spo_prefix_spo(s: &NodeId, p: PredId, o: &NodeId) -> [u8; 36] {
    let mut k = [0u8; 36];
    k[0..16].copy_from_slice(s.as_bytes());
    k[16..20].copy_from_slice(&p.to_be_bytes());
    k[20..36].copy_from_slice(o.as_bytes());
    k
}

/// SOP prefix: all triples with given (subject, object).
pub fn sop_prefix_so(s: &NodeId, o: &NodeId) -> [u8; 32] {
    let mut k = [0u8; 32];
    k[0..16].copy_from_slice(s.as_bytes());
    k[16..32].copy_from_slice(o.as_bytes());
    k
}

/// PSO prefix: all triples with the given predicate.
pub fn pso_prefix_p(p: PredId) -> [u8; 4] {
    p.to_be_bytes()
}

/// POS prefix: all triples with given (predicate, object).
pub fn pos_prefix_po(p: PredId, o: &NodeId) -> [u8; 20] {
    let mut k = [0u8; 20];
    k[0..4].copy_from_slice(&p.to_be_bytes());
    k[4..20].copy_from_slice(o.as_bytes());
    k
}

/// OSP prefix: all triples with the given object.
pub fn osp_prefix_o(o: &NodeId) -> [u8; 16] {
    *o.as_bytes()
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use polargraph_core::{id::NodeId, temporal::Timestamp};
    use uuid::Uuid;

    fn node(seed: u8) -> NodeId {
        NodeId(Uuid::from_bytes([seed; 16]))
    }

    fn ts(v: i64) -> Timestamp {
        Timestamp(v)
    }

    // ── encode/decode round-trips ─────────────────────────────────────────────

    #[test]
    fn spo_round_trip() {
        let s = node(0xAA);
        let o = node(0xBB);
        let p: PredId = 0x0000_0042;
        let tt = ts(999_999);

        let key = encode_spo(&s, p, &o, tt);
        let d = decode_spo(&key).unwrap();

        assert_eq!(d.subject, s);
        assert_eq!(d.pred_id, p);
        assert_eq!(d.object, o);
        assert_eq!(d.tt, tt);
    }

    #[test]
    fn pso_round_trip() {
        let s = node(0x01);
        let o = node(0x02);
        let p: PredId = 7;
        let tt = ts(12345);

        let key = encode_pso(p, &s, &o, tt);
        let d = decode_pso(&key).unwrap();

        assert_eq!(d.pred_id, p);
        assert_eq!(d.subject, s);
        assert_eq!(d.object, o);
        assert_eq!(d.tt, tt);
    }

    #[test]
    fn pos_round_trip() {
        let s = node(0x03);
        let o = node(0x04);
        let p: PredId = 255;
        let tt = ts(0);

        let key = encode_pos(p, &o, &s, tt);
        let d = decode_pos(&key).unwrap();

        assert_eq!(d.pred_id, p);
        assert_eq!(d.object, o);
        assert_eq!(d.subject, s);
        assert_eq!(d.tt, tt);
    }

    #[test]
    fn osp_round_trip() {
        let s = node(0x05);
        let o = node(0x06);
        let p: PredId = u32::MAX;
        let tt = ts(i64::MAX);

        let key = encode_osp(&o, &s, p, tt);
        let d = decode_osp(&key).unwrap();

        assert_eq!(d.object, o);
        assert_eq!(d.subject, s);
        assert_eq!(d.pred_id, p);
        assert_eq!(d.tt, tt);
    }

    #[test]
    fn sop_round_trip() {
        let s = node(0x07);
        let o = node(0x08);
        let p: PredId = 42;
        let tt = ts(7777);

        let key = encode_sop(&s, &o, p, tt);
        let d = decode_sop(&key).unwrap();

        assert_eq!(d.subject, s);
        assert_eq!(d.object, o);
        assert_eq!(d.pred_id, p);
        assert_eq!(d.tt, tt);
    }

    #[test]
    fn sop_prefix_so_is_prefix_of_sop_key() {
        let s = node(0x07);
        let o = node(0x08);
        let key = encode_sop(&s, &o, 1, ts(0));
        let prefix = sop_prefix_so(&s, &o);
        assert!(key.starts_with(&prefix));
    }

    #[test]
    fn sop_encode_is_44_bytes() {
        let s = node(0x07);
        let o = node(0x08);
        assert_eq!(encode_sop(&s, &o, 1, ts(0)).len(), 44);
    }

    #[test]
    fn ops_encode_is_44_bytes() {
        let s = node(0x09);
        let o = node(0x0A);
        assert_eq!(encode_ops(&o, 1, &s, ts(0)).len(), 44);
    }

    // ── keys sort in the right order ──────────────────────────────────────────

    #[test]
    fn spo_keys_sort_by_subject_first() {
        let s1 = node(0x01);
        let s2 = node(0x02);
        let o = node(0xCC);
        let p = 1u32;

        let k1 = encode_spo(&s1, p, &o, ts(0));
        let k2 = encode_spo(&s2, p, &o, ts(0));
        assert!(k1 < k2, "key for lower subject should sort first");
    }

    #[test]
    fn spo_keys_sort_by_predicate_second() {
        let s = node(0xAA);
        let o = node(0xBB);

        let k1 = encode_spo(&s, 1, &o, ts(0));
        let k2 = encode_spo(&s, 2, &o, ts(0));
        assert!(k1 < k2);
    }

    #[test]
    fn spo_keys_sort_by_tt_last() {
        let s = node(0xAA);
        let o = node(0xBB);
        let p = 5u32;

        let k1 = encode_spo(&s, p, &o, ts(100));
        let k2 = encode_spo(&s, p, &o, ts(200));
        assert!(k1 < k2);
    }

    // ── prefix helpers ────────────────────────────────────────────────────────

    #[test]
    fn spo_prefix_s_is_prefix_of_spo_key() {
        let s = node(0xAA);
        let o = node(0xBB);
        let key = encode_spo(&s, 1, &o, ts(0));
        let prefix = spo_prefix_s(&s);
        assert!(key.starts_with(&prefix));
    }

    #[test]
    fn spo_prefix_sp_is_prefix_of_spo_key() {
        let s = node(0xAA);
        let o = node(0xBB);
        let p = 42u32;
        let key = encode_spo(&s, p, &o, ts(0));
        let prefix = spo_prefix_sp(&s, p);
        assert!(key.starts_with(&prefix));
    }

    #[test]
    fn pso_prefix_p_is_prefix_of_pso_key() {
        let s = node(0xCC);
        let o = node(0xDD);
        let p = 9u32;
        let key = encode_pso(p, &s, &o, ts(0));
        let prefix = pso_prefix_p(p);
        assert!(key.starts_with(&prefix));
    }

    #[test]
    fn pos_prefix_po_is_prefix_of_pos_key() {
        let s = node(0xEE);
        let o = node(0xFF);
        let p = 3u32;
        let key = encode_pos(p, &o, &s, ts(0));
        let prefix = pos_prefix_po(p, &o);
        assert!(key.starts_with(&prefix));
    }

    #[test]
    fn osp_prefix_o_is_prefix_of_osp_key() {
        let s = node(0x11);
        let o = node(0x22);
        let key = encode_osp(&o, &s, 1, ts(0));
        let prefix = osp_prefix_o(&o);
        assert!(key.starts_with(&prefix));
    }

    // ── non-prefix keys do NOT start with a different node's prefix ───────────

    #[test]
    fn spo_prefix_does_not_match_different_subject() {
        let s1 = node(0x01);
        let s2 = node(0x02);
        let o = node(0xBB);
        let key = encode_spo(&s1, 1, &o, ts(0));
        let wrong_prefix = spo_prefix_s(&s2);
        assert!(!key.starts_with(&wrong_prefix));
    }

    // ── property sentinel ─────────────────────────────────────────────────────

    #[test]
    fn property_sentinel_detected() {
        assert!(is_property_sentinel(&PROPERTY_SENTINEL));
    }

    #[test]
    fn normal_node_id_not_sentinel() {
        let n = node(0xAB);
        assert!(!is_property_sentinel(n.as_bytes()));
    }

    // ── decode error on wrong length ──────────────────────────────────────────

    #[test]
    fn decode_spo_wrong_length_errors() {
        assert!(decode_spo(&[0u8; 43]).is_err());
        assert!(decode_spo(&[0u8; 45]).is_err());
        assert!(decode_spo(&[]).is_err());
    }

    #[test]
    fn decode_pso_wrong_length_errors() {
        assert!(decode_pso(&[0u8; 43]).is_err());
    }

    #[test]
    fn decode_pos_wrong_length_errors() {
        assert!(decode_pos(&[0u8; 43]).is_err());
    }

    #[test]
    fn decode_osp_wrong_length_errors() {
        assert!(decode_osp(&[0u8; 43]).is_err());
    }

    // ── PEA encode/decode round-trips ─────────────────────────────────────────

    #[test]
    fn pea_round_trip() {
        use polargraph_core::id::EdgeId;
        let edge = EdgeId(Uuid::from_bytes([0xAB; 16]));
        let pred: PredId = 0x0000_007F;
        let tt = ts(123_456_789);

        let key = encode_pea_key(pred, &edge, tt);
        assert_eq!(key.len(), 28);
        let d = decode_pea_key(&key).unwrap();

        assert_eq!(d.pred_id, pred);
        assert_eq!(d.edge_id.as_bytes(), edge.as_bytes());
        assert_eq!(d.tt, tt);
    }

    #[test]
    fn pea_prefix_pred_is_prefix_of_pea_key() {
        use polargraph_core::id::EdgeId;
        let edge = EdgeId(Uuid::from_bytes([0xCD; 16]));
        let pred: PredId = 42;
        let key = encode_pea_key(pred, &edge, ts(0));
        let prefix = pea_prefix_pred(pred);
        assert!(key.starts_with(&prefix));
    }

    #[test]
    fn pea_prefix_pred_edge_is_prefix_of_pea_key() {
        use polargraph_core::id::EdgeId;
        let edge = EdgeId(Uuid::from_bytes([0xEF; 16]));
        let pred: PredId = 7;
        let key = encode_pea_key(pred, &edge, ts(999));
        let prefix = pea_prefix_pred_edge(pred, &edge);
        assert!(key.starts_with(&prefix));
    }

    #[test]
    fn pea_different_pred_does_not_match_prefix() {
        use polargraph_core::id::EdgeId;
        let edge = EdgeId(Uuid::from_bytes([0x11; 16]));
        let key = encode_pea_key(1, &edge, ts(0));
        let wrong_prefix = pea_prefix_pred(2);
        assert!(!key.starts_with(&wrong_prefix));
    }

    #[test]
    fn decode_pea_wrong_length_errors() {
        assert!(decode_pea_key(&[0u8; 27]).is_err());
        assert!(decode_pea_key(&[0u8; 29]).is_err());
        assert!(decode_pea_key(&[]).is_err());
    }

    #[test]
    fn pea_keys_sort_by_pred_then_edge_then_tt() {
        use polargraph_core::id::EdgeId;
        let edge1 = EdgeId(Uuid::from_bytes([0x01; 16]));
        let edge2 = EdgeId(Uuid::from_bytes([0x02; 16]));

        // Same pred, different edge
        let k1 = encode_pea_key(5, &edge1, ts(0));
        let k2 = encode_pea_key(5, &edge2, ts(0));
        assert!(k1 < k2, "lower edge bytes should sort first");

        // Different pred
        let k3 = encode_pea_key(4, &edge2, ts(0));
        let k4 = encode_pea_key(5, &edge1, ts(0));
        assert!(k3 < k4, "lower pred_id should sort first");

        // Same pred+edge, different tt
        let k5 = encode_pea_key(5, &edge1, ts(100));
        let k6 = encode_pea_key(5, &edge1, ts(200));
        assert!(k5 < k6, "lower tt should sort first");
    }
}
