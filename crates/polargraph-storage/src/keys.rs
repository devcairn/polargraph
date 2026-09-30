//! Index key encoding and decoding (storage format v3).
//!
//! Keys are fixed-width byte sequences so that RocksDB's default
//! lexicographic ordering gives correct sorted-range scans.
//!
//! Component widths:
//!   NodeId  = 16 bytes (UUID bytes)
//!   PredId  = 4 bytes  (u32 big-endian interned ID)
//!   GraphId = 4 bytes  (u32 big-endian interned ID; 0 = default graph)
//!   tt      = 8 bytes  (i64 big-endian microseconds since epoch)
//!
//! Every quad is written to eight orders (`docs/design/v3-key-layout.md` §3):
//!
//! ```text
//! spog [s][p][o][g][tt]   sopg [s][o][p][g][tt]   psog [p][s][o][g][tt]
//! posg [p][o][s][g][tt]   ospg [o][s][p][g][tt]   opsg [o][p][s][g][tt]
//! gspo [g][s][p][o][tt]   gpos [g][p][o][s][tt]
//! ```
//!
//! All are 48 bytes; the first 40 (the quad) are shared by every version of
//! one quad, and `tt` sorts versions oldest-first. For property triples the
//! object slot holds the value's content hash ([`value_object`]).

use crate::error::StorageError;
use polargraph_core::{
    id::{EdgeId, GraphId, NodeId},
    temporal::Timestamp,
    value::Value,
};
use uuid::Uuid;

/// Interned predicate ID — 32-bit so keys stay compact.
pub type PredId = u32;

/// Width of a quad index key.
pub const QUAD_KEY_LEN: usize = 48;

/// Width of the quad prefix of a key — everything but `tt`. All versions of
/// one quad share this prefix and sort together, oldest `tt` first.
pub const QUAD_TUPLE_LEN: usize = QUAD_KEY_LEN - 8;

/// A full quad index key.
pub type QuadKeyBytes = [u8; QUAD_KEY_LEN];

/// The quad prefix of a key.
pub type QuadTuple = [u8; QUAD_TUPLE_LEN];

/// Offset of the graph slot in the six orders that don't lead with `g`.
pub const GRAPH_OFFSET_NON_LEADING: usize = 36;

/// A decoded quad key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuadKey {
    pub s: NodeId,
    pub p: PredId,
    /// Object node, or the value hash for property triples.
    pub o: NodeId,
    pub g: GraphId,
    pub tt: Timestamp,
}

#[derive(Clone, Copy)]
enum Slot {
    S,
    P,
    O,
    G,
}

impl Slot {
    const fn width(self) -> usize {
        match self {
            Slot::S | Slot::O => 16,
            Slot::P | Slot::G => 4,
        }
    }
}

/// One of the eight quad index orders.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Order {
    Spog,
    Sopg,
    Psog,
    Posg,
    Ospg,
    Opsg,
    Gspo,
    Gpos,
}

impl Order {
    /// Every order a quad is written to.
    pub const ALL: [Order; 8] = [
        Order::Spog,
        Order::Sopg,
        Order::Psog,
        Order::Posg,
        Order::Ospg,
        Order::Opsg,
        Order::Gspo,
        Order::Gpos,
    ];

    /// The column family holding this order.
    pub const fn cf(self) -> &'static str {
        match self {
            Order::Spog => crate::cf::SPOG,
            Order::Sopg => crate::cf::SOPG,
            Order::Psog => crate::cf::PSOG,
            Order::Posg => crate::cf::POSG,
            Order::Ospg => crate::cf::OSPG,
            Order::Opsg => crate::cf::OPSG,
            Order::Gspo => crate::cf::GSPO,
            Order::Gpos => crate::cf::GPOS,
        }
    }

    const fn slots(self) -> [Slot; 4] {
        use Slot::*;
        match self {
            Order::Spog => [S, P, O, G],
            Order::Sopg => [S, O, P, G],
            Order::Psog => [P, S, O, G],
            Order::Posg => [P, O, S, G],
            Order::Ospg => [O, S, P, G],
            Order::Opsg => [O, P, S, G],
            Order::Gspo => [G, S, P, O],
            Order::Gpos => [G, P, O, S],
        }
    }

    /// Encode `k` in this order.
    pub fn encode(self, k: &QuadKey) -> QuadKeyBytes {
        let mut out = [0u8; QUAD_KEY_LEN];
        let mut at = 0;
        for slot in self.slots() {
            let w = slot.width();
            match slot {
                Slot::S => out[at..at + w].copy_from_slice(k.s.as_bytes()),
                Slot::P => out[at..at + w].copy_from_slice(&k.p.to_be_bytes()),
                Slot::O => out[at..at + w].copy_from_slice(k.o.as_bytes()),
                Slot::G => out[at..at + w].copy_from_slice(&k.g.to_be_bytes()),
            }
            at += w;
        }
        out[QUAD_TUPLE_LEN..].copy_from_slice(&k.tt.to_be_bytes());
        out
    }

    /// Decode a key written in this order.
    pub fn decode(self, key: &[u8]) -> Result<QuadKey, StorageError> {
        check_len(key, QUAD_KEY_LEN, self.cf())?;
        let mut k = QuadKey {
            s: NodeId(Uuid::nil()),
            p: 0,
            o: NodeId(Uuid::nil()),
            g: GraphId::DEFAULT,
            tt: tt_from(&key[QUAD_TUPLE_LEN..]),
        };
        let mut at = 0;
        for slot in self.slots() {
            let w = slot.width();
            let b = &key[at..at + w];
            match slot {
                Slot::S => k.s = node_id_from(b),
                Slot::P => k.p = u32_from(b),
                Slot::O => k.o = node_id_from(b),
                Slot::G => k.g = GraphId(u32_from(b)),
            }
            at += w;
        }
        Ok(k)
    }

    /// Key prefix for a scan: the leading slots of this order that are bound,
    /// stopping at the first unbound one. Built on the stack (no allocation).
    pub fn prefix(
        self,
        s: Option<&NodeId>,
        p: Option<PredId>,
        o: Option<&NodeId>,
        g: Option<GraphId>,
    ) -> KeyPrefix {
        let mut out = KeyPrefix {
            bytes: [0; QUAD_TUPLE_LEN],
            len: 0,
        };
        for slot in self.slots() {
            match slot {
                Slot::S => match s {
                    Some(s) => out.push(s.as_bytes()),
                    None => break,
                },
                Slot::P => match p {
                    Some(p) => out.push(&p.to_be_bytes()),
                    None => break,
                },
                Slot::O => match o {
                    Some(o) => out.push(o.as_bytes()),
                    None => break,
                },
                Slot::G => match g {
                    Some(g) => out.push(&g.to_be_bytes()),
                    None => break,
                },
            }
        }
        out
    }

    /// The graph of a key in this order, read without a full decode.
    #[inline]
    pub fn graph_of(self, key: &[u8]) -> GraphId {
        let at = match self {
            Order::Gspo | Order::Gpos => 0,
            _ => GRAPH_OFFSET_NON_LEADING,
        };
        GraphId(u32_from(&key[at..at + 4]))
    }
}

/// A scan prefix of up to [`QUAD_TUPLE_LEN`] bytes, held inline.
#[derive(Clone, Copy)]
pub struct KeyPrefix {
    bytes: [u8; QUAD_TUPLE_LEN],
    len: usize,
}

impl KeyPrefix {
    #[inline]
    fn push(&mut self, part: &[u8]) {
        self.bytes[self.len..self.len + part.len()].copy_from_slice(part);
        self.len += part.len();
    }
}

impl std::ops::Deref for KeyPrefix {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

impl AsRef<[u8]> for KeyPrefix {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self
    }
}

/// The key that sorts after every version of `tuple` (its `tt` bytes set to
/// 0xFF), for newest-first reverse scans.
pub fn tuple_end(tuple: &[u8]) -> Vec<u8> {
    let mut k = tuple.to_vec();
    k.resize(QUAD_KEY_LEN.max(tuple.len() + 8), 0xFF);
    k
}

/// Read the transaction time from the last 8 bytes of a versioned key.
#[inline]
pub fn key_tt(key: &[u8]) -> Timestamp {
    tt_from(&key[key.len() - 8..])
}

/// The object slot of a property triple: the value's content hash as a NodeId.
#[inline]
pub fn value_object(value: &Value) -> NodeId {
    NodeId(Uuid::from_bytes(value.content_hash()))
}

// ── private decode helpers ────────────────────────────────────────────────────

#[inline]
fn node_id_from(b: &[u8]) -> NodeId {
    NodeId(Uuid::from_bytes(b.try_into().expect("16 bytes")))
}
#[inline]
fn u32_from(b: &[u8]) -> u32 {
    u32::from_be_bytes(b.try_into().expect("4 bytes"))
}
#[inline]
fn tt_from(b: &[u8]) -> Timestamp {
    Timestamp::from_be_bytes(b.try_into().expect("8 bytes"))
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

// ── Trigram CF ────────────────────────────────────────────────────────────────

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

/// TRI key: `[trigram(3)][pred_id BE(4)][g BE(4)][subject(16)]` = 27 bytes.
/// Value is empty. The graph precedes the subject so graph ACLs can skip
/// candidates without decoding them.
pub fn encode_tri(trigram: [u8; 3], pred_id: PredId, g: GraphId, subject: &NodeId) -> [u8; 27] {
    let mut k = [0u8; 27];
    k[0..3].copy_from_slice(&trigram);
    k[3..7].copy_from_slice(&pred_id.to_be_bytes());
    k[7..11].copy_from_slice(&g.to_be_bytes());
    k[11..27].copy_from_slice(subject.as_bytes());
    k
}

/// TRI prefix for all subjects that have `(trigram, pred_id)` in any graph.
pub fn tri_prefix_tp(trigram: [u8; 3], pred_id: PredId) -> [u8; 7] {
    let mut k = [0u8; 7];
    k[0..3].copy_from_slice(&trigram);
    k[3..7].copy_from_slice(&pred_id.to_be_bytes());
    k
}

/// `(graph, subject)` of a TRI key.
pub fn decode_tri(key: &[u8]) -> Result<(GraphId, NodeId), StorageError> {
    check_len(key, 27, "TRI")?;
    Ok((GraphId(u32_from(&key[7..11])), node_id_from(&key[11..27])))
}

// ── RDF-star annotations (EPA / EPO / PEA) ───────────────────────────────────

/// EPA key: `[edge(16)][pred_id(4)][g(4)][tt(8)]` = 32 bytes.
pub fn encode_epa_key(edge: &EdgeId, pred_id: PredId, g: GraphId, tt: Timestamp) -> [u8; 32] {
    let mut k = [0u8; 32];
    k[0..16].copy_from_slice(edge.as_bytes());
    k[16..20].copy_from_slice(&pred_id.to_be_bytes());
    k[20..24].copy_from_slice(&g.to_be_bytes());
    k[24..32].copy_from_slice(&tt.to_be_bytes());
    k
}

/// Decoded EPA key.
pub struct DecodedEpa {
    pub edge_id: EdgeId,
    pub pred_id: PredId,
    pub g: GraphId,
    pub tt: Timestamp,
}

pub fn decode_epa_key(key: &[u8]) -> Result<DecodedEpa, StorageError> {
    check_len(key, 32, "EPA")?;
    Ok(DecodedEpa {
        edge_id: EdgeId(Uuid::from_bytes(key[0..16].try_into().expect("16 bytes"))),
        pred_id: u32_from(&key[16..20]),
        g: GraphId(u32_from(&key[20..24])),
        tt: tt_from(&key[24..32]),
    })
}

/// EPO key: `[edge(16)][pred_id(4)][obj(16)][g(4)][tt(8)]` = 48 bytes.
pub fn encode_epo_key(
    edge: &EdgeId,
    pred_id: PredId,
    obj: &NodeId,
    g: GraphId,
    tt: Timestamp,
) -> [u8; 48] {
    let mut k = [0u8; 48];
    k[0..16].copy_from_slice(edge.as_bytes());
    k[16..20].copy_from_slice(&pred_id.to_be_bytes());
    k[20..36].copy_from_slice(obj.as_bytes());
    k[36..40].copy_from_slice(&g.to_be_bytes());
    k[40..48].copy_from_slice(&tt.to_be_bytes());
    k
}

/// Decoded EPO key.
pub struct DecodedEpo {
    pub edge_id: EdgeId,
    pub pred_id: PredId,
    pub object: NodeId,
    pub g: GraphId,
    pub tt: Timestamp,
}

pub fn decode_epo_key(key: &[u8]) -> Result<DecodedEpo, StorageError> {
    check_len(key, 48, "EPO")?;
    Ok(DecodedEpo {
        edge_id: EdgeId(Uuid::from_bytes(key[0..16].try_into().expect("16 bytes"))),
        pred_id: u32_from(&key[16..20]),
        object: node_id_from(&key[20..36]),
        g: GraphId(u32_from(&key[36..40])),
        tt: tt_from(&key[40..48]),
    })
}

/// EPA / EPO prefix: all annotations for a given edge.
pub fn annotation_prefix_edge(edge: &EdgeId) -> [u8; 16] {
    *edge.as_bytes()
}

/// EPA / EPO prefix: all annotations for (edge, predicate).
pub fn annotation_prefix_edge_pred(edge: &EdgeId, pred_id: PredId) -> [u8; 20] {
    let mut k = [0u8; 20];
    k[0..16].copy_from_slice(edge.as_bytes());
    k[16..20].copy_from_slice(&pred_id.to_be_bytes());
    k
}

/// PEA key: `[pred_id(4)][edge(16)][g(4)][tt(8)]` = 32 bytes.
pub fn encode_pea_key(pred_id: PredId, edge: &EdgeId, g: GraphId, tt: Timestamp) -> [u8; 32] {
    let mut k = [0u8; 32];
    k[0..4].copy_from_slice(&pred_id.to_be_bytes());
    k[4..20].copy_from_slice(edge.as_bytes());
    k[20..24].copy_from_slice(&g.to_be_bytes());
    k[24..32].copy_from_slice(&tt.to_be_bytes());
    k
}

/// Decoded PEA key.
pub struct DecodedPea {
    pub pred_id: PredId,
    pub edge_id: EdgeId,
    pub g: GraphId,
    pub tt: Timestamp,
}

pub fn decode_pea_key(key: &[u8]) -> Result<DecodedPea, StorageError> {
    check_len(key, 32, "PEA")?;
    Ok(DecodedPea {
        pred_id: u32_from(&key[0..4]),
        edge_id: EdgeId(Uuid::from_bytes(key[4..20].try_into().expect("16 bytes"))),
        g: GraphId(u32_from(&key[20..24])),
        tt: tt_from(&key[24..32]),
    })
}

/// PEA prefix: all property annotations with a given predicate.
pub fn pea_prefix_pred(pred_id: PredId) -> [u8; 4] {
    pred_id.to_be_bytes()
}

// ── Storage format v2 (read-only, for migration) ─────────────────────────────

/// The v2 (pre-graph) key layouts, kept only so `migrate_v3` can read old
/// stores. Encoders exist for building v2 fixtures in tests.
pub mod v2 {
    use super::*;

    /// v2 hexastore key width: `[a16|4][b][c][tt8]` = 44 bytes.
    pub const KEY_LEN: usize = 44;

    /// Object slot of v2 property keys.
    pub const PROPERTY_SENTINEL: [u8; 16] = [0xFF; 16];

    /// A decoded v2 SPO / DRV key.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Spo {
        pub s: NodeId,
        pub p: PredId,
        pub o: NodeId,
        pub tt: Timestamp,
    }

    impl Spo {
        pub fn is_property(&self) -> bool {
            self.o.as_bytes() == &PROPERTY_SENTINEL
        }
    }

    pub fn decode_spo(key: &[u8]) -> Result<Spo, StorageError> {
        check_len(key, KEY_LEN, "v2 SPO")?;
        Ok(Spo {
            s: node_id_from(&key[0..16]),
            p: u32_from(&key[16..20]),
            o: node_id_from(&key[20..36]),
            tt: tt_from(&key[36..44]),
        })
    }

    pub fn encode_spo(s: &NodeId, p: PredId, o: &NodeId, tt: Timestamp) -> [u8; KEY_LEN] {
        let mut k = [0u8; KEY_LEN];
        k[0..16].copy_from_slice(s.as_bytes());
        k[16..20].copy_from_slice(&p.to_be_bytes());
        k[20..36].copy_from_slice(o.as_bytes());
        k[36..44].copy_from_slice(&tt.to_be_bytes());
        k
    }

    /// `(edge, pred, tt)` of a v2 EPA key `[edge16][p4][tt8]`.
    pub fn decode_epa(key: &[u8]) -> Result<(EdgeId, PredId, Timestamp), StorageError> {
        check_len(key, 28, "v2 EPA")?;
        Ok((
            EdgeId(Uuid::from_bytes(key[0..16].try_into().expect("16 bytes"))),
            u32_from(&key[16..20]),
            tt_from(&key[20..28]),
        ))
    }

    pub fn encode_epa(edge: &EdgeId, p: PredId, tt: Timestamp) -> [u8; 28] {
        let mut k = [0u8; 28];
        k[0..16].copy_from_slice(edge.as_bytes());
        k[16..20].copy_from_slice(&p.to_be_bytes());
        k[20..28].copy_from_slice(&tt.to_be_bytes());
        k
    }

    /// `(edge, pred, object, tt)` of a v2 EPO key `[edge16][p4][o16][tt8]`.
    pub fn decode_epo(key: &[u8]) -> Result<(EdgeId, PredId, NodeId, Timestamp), StorageError> {
        check_len(key, 44, "v2 EPO")?;
        Ok((
            EdgeId(Uuid::from_bytes(key[0..16].try_into().expect("16 bytes"))),
            u32_from(&key[16..20]),
            node_id_from(&key[20..36]),
            tt_from(&key[36..44]),
        ))
    }

    pub fn encode_epo(edge: &EdgeId, p: PredId, o: &NodeId, tt: Timestamp) -> [u8; 44] {
        let mut k = [0u8; 44];
        k[0..16].copy_from_slice(edge.as_bytes());
        k[16..20].copy_from_slice(&p.to_be_bytes());
        k[20..36].copy_from_slice(o.as_bytes());
        k[36..44].copy_from_slice(&tt.to_be_bytes());
        k
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn node(seed: u8) -> NodeId {
        NodeId(Uuid::from_bytes([seed; 16]))
    }

    fn quad(s: u8, p: PredId, o: u8, g: u32, tt: i64) -> QuadKey {
        QuadKey {
            s: node(s),
            p,
            o: node(o),
            g: GraphId(g),
            tt: Timestamp(tt),
        }
    }

    #[test]
    fn every_order_round_trips() {
        let k = quad(0xAA, 0x42, 0xBB, 7, 999_999);
        for order in Order::ALL {
            let bytes = order.encode(&k);
            assert_eq!(bytes.len(), QUAD_KEY_LEN);
            assert_eq!(order.decode(&bytes).unwrap(), k, "{order:?}");
            assert_eq!(order.graph_of(&bytes), GraphId(7), "{order:?}");
            assert_eq!(key_tt(&bytes), Timestamp(999_999));
        }
    }

    #[test]
    fn slot_positions_match_the_design() {
        let k = quad(0x11, 0x22, 0x33, 0x44, 0x55);
        let spog = Order::Spog.encode(&k);
        assert_eq!(&spog[0..16], &[0x11; 16]);
        assert_eq!(&spog[16..20], &0x22u32.to_be_bytes());
        assert_eq!(&spog[20..36], &[0x33; 16]);
        assert_eq!(&spog[36..40], &0x44u32.to_be_bytes());
        let gpos = Order::Gpos.encode(&k);
        assert_eq!(&gpos[0..4], &0x44u32.to_be_bytes());
        assert_eq!(&gpos[4..8], &0x22u32.to_be_bytes());
        assert_eq!(&gpos[8..24], &[0x33; 16]);
        assert_eq!(&gpos[24..40], &[0x11; 16]);
    }

    #[test]
    fn prefixes_stop_at_the_first_unbound_slot() {
        let k = quad(1, 2, 3, 4, 5);
        let s = node(1);
        let o = node(3);
        for (order, prefix) in [
            (
                Order::Spog,
                Order::Spog.prefix(Some(&s), Some(2), None, None),
            ),
            (
                Order::Posg,
                Order::Posg.prefix(None, Some(2), Some(&o), None),
            ),
            (
                Order::Gspo,
                Order::Gspo.prefix(Some(&s), Some(2), None, Some(GraphId(4))),
            ),
        ] {
            assert_eq!(prefix.len(), 20 + if order == Order::Gspo { 4 } else { 0 });
            assert!(order.encode(&k).starts_with(&prefix), "{order:?}");
        }
        // Unbound leading slot → empty prefix (full scan).
        assert!(Order::Spog.prefix(None, Some(2), None, None).is_empty());
        // Every slot bound → the whole quad.
        assert_eq!(
            Order::Spog
                .prefix(Some(&s), Some(2), Some(&o), Some(GraphId(4)))
                .len(),
            QUAD_TUPLE_LEN
        );
    }

    #[test]
    fn versions_of_one_quad_sort_together_oldest_first() {
        let a = Order::Spog.encode(&quad(1, 2, 3, 0, 10));
        let b = Order::Spog.encode(&quad(1, 2, 3, 0, 20));
        let other_graph = Order::Spog.encode(&quad(1, 2, 3, 1, 5));
        assert!(a < b);
        assert!(b < other_graph, "graph sorts before tt");
        assert_eq!(a[..QUAD_TUPLE_LEN], b[..QUAD_TUPLE_LEN]);
        assert!(tuple_end(&a[..QUAD_TUPLE_LEN]).as_slice() > b.as_slice());
    }

    #[test]
    fn value_objects_are_content_hashes() {
        let v = Value::Text("hello".into());
        assert_eq!(value_object(&v).as_bytes(), &v.content_hash());
        assert_ne!(value_object(&v), value_object(&Value::Text("world".into())));
    }

    #[test]
    fn ancillary_keys_round_trip() {
        let e = EdgeId(Uuid::from_bytes([9; 16]));
        let d = decode_epa_key(&encode_epa_key(&e, 3, GraphId(2), Timestamp(7))).unwrap();
        assert_eq!(
            (d.edge_id, d.pred_id, d.g, d.tt),
            (e, 3, GraphId(2), Timestamp(7))
        );
        let d = decode_epo_key(&encode_epo_key(&e, 3, &node(5), GraphId(2), Timestamp(7))).unwrap();
        assert_eq!((d.object, d.g, d.tt), (node(5), GraphId(2), Timestamp(7)));
        let d = decode_pea_key(&encode_pea_key(3, &e, GraphId(2), Timestamp(7))).unwrap();
        assert_eq!((d.pred_id, d.edge_id, d.g), (3, e, GraphId(2)));
        let tri = encode_tri(*b"abc", 3, GraphId(2), &node(5));
        assert!(tri.starts_with(&tri_prefix_tp(*b"abc", 3)));
        assert_eq!(decode_tri(&tri).unwrap(), (GraphId(2), node(5)));
    }

    #[test]
    fn wrong_lengths_are_errors() {
        assert!(Order::Spog.decode(&[0u8; 44]).is_err());
        assert!(decode_epa_key(&[0u8; 28]).is_err());
        assert!(v2::decode_spo(&[0u8; 48]).is_err());
    }

    #[test]
    fn v2_keys_round_trip() {
        let k = v2::encode_spo(
            &node(1),
            2,
            &NodeId(Uuid::from_bytes(v2::PROPERTY_SENTINEL)),
            Timestamp(3),
        );
        let d = v2::decode_spo(&k).unwrap();
        assert!(d.is_property());
        assert_eq!((d.s, d.p, d.tt), (node(1), 2, Timestamp(3)));
    }

    #[test]
    fn trigram_extraction() {
        assert!(extract_trigrams("").is_empty());
        assert_eq!(extract_trigrams("ab"), vec![[b'a', b'b', 0]]);
        let mut t = extract_trigrams("abcd");
        t.sort();
        assert_eq!(t, vec![*b"abc", *b"bcd"]);
    }
}
