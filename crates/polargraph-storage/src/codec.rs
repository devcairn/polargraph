//! Binary codec for RocksDB triple values.
//!
//! The index *key* encodes (slot_a, slot_b, slot_c, tx_time) — see keys.rs.
//! The index *value* carries everything else needed to reconstruct a Triple:
//!
//! Relation triple value layout (33 bytes):
//!   [0x01 discriminant (1)]
//!   [edge_id: 16 bytes  ]
//!   [vt_start: 8 bytes  ]   ← big-endian i64 microseconds
//!   [vt_end:   8 bytes  ]   ← big-endian i64 microseconds
//!
//! Property triple value layout (17 + N bytes):
//!   [0x02 discriminant (1)]
//!   [vt_start: 8 bytes  ]
//!   [vt_end:   8 bytes  ]
//!   [json_value: N bytes ]   ← serde_json-encoded Value

use polargraph_core::{
    id::EdgeId,
    temporal::{BiTemporalRange, Timestamp},
    value::Value,
};
use uuid::Uuid;

use crate::error::StorageError;

pub const DISC_RELATION: u8 = 0x01;
pub const DISC_PROPERTY: u8 = 0x02;
/// Binary-encoded `Value::Vector` — avoids JSON overhead for float arrays.
/// Layout: [0x03][vt_start: 8 BE][vt_end: 8 BE][len: 4 LE][f32 × len LE]
pub const DISC_VECTOR: u8 = 0x03;

// ── encode ───────────────────────────────────────────────────────────────────

pub fn encode_relation(edge_id: &EdgeId, temporal: &BiTemporalRange) -> Vec<u8> {
    let mut v = Vec::with_capacity(33);
    v.push(DISC_RELATION);
    v.extend_from_slice(edge_id.as_bytes());
    v.extend_from_slice(&temporal.vt_start.to_be_bytes());
    v.extend_from_slice(&temporal.vt_end.to_be_bytes());
    v
}

pub fn encode_property(value: &Value, temporal: &BiTemporalRange) -> Result<Vec<u8>, StorageError> {
    // Vectors get a dedicated binary encoding to avoid JSON overhead.
    if let Value::Vector(floats) = value {
        let len = floats.len() as u32;
        let mut v = Vec::with_capacity(21 + floats.len() * 4);
        v.push(DISC_VECTOR);
        v.extend_from_slice(&temporal.vt_start.to_be_bytes());
        v.extend_from_slice(&temporal.vt_end.to_be_bytes());
        v.extend_from_slice(&len.to_le_bytes());
        for &f in floats {
            v.extend_from_slice(&f.to_le_bytes());
        }
        return Ok(v);
    }
    let json = serde_json::to_vec(value)?;
    let mut v = Vec::with_capacity(17 + json.len());
    v.push(DISC_PROPERTY);
    v.extend_from_slice(&temporal.vt_start.to_be_bytes());
    v.extend_from_slice(&temporal.vt_end.to_be_bytes());
    v.extend_from_slice(&json);
    Ok(v)
}

// ── decode ───────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum DecodedValue {
    Relation {
        edge_id: EdgeId,
        temporal: BiTemporalRange,
    },
    Property {
        value: Value,
        temporal: BiTemporalRange,
    },
}

pub fn decode_value(bytes: &[u8]) -> Result<DecodedValue, StorageError> {
    if bytes.is_empty() {
        return Err(StorageError::KeyDecode("empty value bytes".into()));
    }
    match bytes[0] {
        DISC_RELATION => {
            if bytes.len() < 33 {
                return Err(StorageError::KeyDecode("relation value too short".into()));
            }
            let edge_id = EdgeId(Uuid::from_bytes(bytes[1..17].try_into().unwrap()));
            let vt_start = Timestamp::from_be_bytes(bytes[17..25].try_into().unwrap());
            let vt_end = Timestamp::from_be_bytes(bytes[25..33].try_into().unwrap());
            Ok(DecodedValue::Relation {
                edge_id,
                temporal: BiTemporalRange {
                    vt_start,
                    vt_end,
                    tt: Timestamp(0), // tt comes from the key; caller fills it in
                },
            })
        }
        DISC_PROPERTY => {
            if bytes.len() < 17 {
                return Err(StorageError::KeyDecode("property value too short".into()));
            }
            let vt_start = Timestamp::from_be_bytes(bytes[1..9].try_into().unwrap());
            let vt_end = Timestamp::from_be_bytes(bytes[9..17].try_into().unwrap());
            let value: Value = serde_json::from_slice(&bytes[17..])?;
            Ok(DecodedValue::Property {
                value,
                temporal: BiTemporalRange {
                    vt_start,
                    vt_end,
                    tt: Timestamp(0),
                },
            })
        }
        DISC_VECTOR => {
            if bytes.len() < 21 {
                return Err(StorageError::KeyDecode("vector value too short".into()));
            }
            let vt_start = Timestamp::from_be_bytes(bytes[1..9].try_into().unwrap());
            let vt_end = Timestamp::from_be_bytes(bytes[9..17].try_into().unwrap());
            let len = u32::from_le_bytes(bytes[17..21].try_into().unwrap()) as usize;
            if bytes.len() < 21 + len * 4 {
                return Err(StorageError::KeyDecode("vector value truncated".into()));
            }
            let mut floats = Vec::with_capacity(len);
            for i in 0..len {
                let off = 21 + i * 4;
                floats.push(f32::from_le_bytes(bytes[off..off + 4].try_into().unwrap()));
            }
            Ok(DecodedValue::Property {
                value: Value::Vector(floats),
                temporal: BiTemporalRange {
                    vt_start,
                    vt_end,
                    tt: Timestamp(0),
                },
            })
        }
        d => Err(StorageError::KeyDecode(format!("unknown discriminant 0x{d:02x}"))),
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use polargraph_core::{id::EdgeId, temporal::{BiTemporalRange, Timestamp}, value::Value};

    fn temporal(vt_start: i64, vt_end: i64, tt: i64) -> BiTemporalRange {
        BiTemporalRange {
            vt_start: Timestamp(vt_start),
            vt_end: Timestamp(vt_end),
            tt: Timestamp(tt),
        }
    }

    fn edge_id_from_seed(seed: u8) -> EdgeId {
        EdgeId(uuid::Uuid::from_bytes([seed; 16]))
    }

    // ── relation round-trips ──────────────────────────────────────────────────

    #[test]
    fn relation_round_trip() {
        let eid = edge_id_from_seed(0xAB);
        let t = temporal(1_000, i64::MAX, 2_000);

        let encoded = encode_relation(&eid, &t);
        assert_eq!(encoded.len(), 33);
        assert_eq!(encoded[0], DISC_RELATION);

        match decode_value(&encoded).unwrap() {
            DecodedValue::Relation { edge_id, temporal } => {
                assert_eq!(edge_id, eid);
                assert_eq!(temporal.vt_start, Timestamp(1_000));
                assert_eq!(temporal.vt_end, Timestamp(i64::MAX));
                // tt is placeholder (key-derived); caller fills it in
                assert_eq!(temporal.tt, Timestamp(0));
            }
            DecodedValue::Property { .. } => panic!("expected Relation"),
        }
    }

    #[test]
    fn relation_edge_id_survives_all_bytes() {
        // Use a non-trivial UUID to catch byte-order bugs.
        let eid = EdgeId(uuid::Uuid::from_bytes([
            0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF,
            0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54, 0x32, 0x10,
        ]));
        let t = temporal(100, 200, 300);
        let encoded = encode_relation(&eid, &t);
        match decode_value(&encoded).unwrap() {
            DecodedValue::Relation { edge_id, .. } => assert_eq!(edge_id, eid),
            _ => panic!(),
        }
    }

    // ── property round-trips — all Value variants ─────────────────────────────

    fn property_round_trip(value: Value) -> Value {
        let t = temporal(500, i64::MAX, 600);
        let encoded = encode_property(&value, &t).unwrap();
        assert_eq!(encoded[0], DISC_PROPERTY);
        match decode_value(&encoded).unwrap() {
            DecodedValue::Property { value: v, temporal } => {
                assert_eq!(temporal.vt_start, Timestamp(500));
                assert_eq!(temporal.vt_end, Timestamp(i64::MAX));
                v
            }
            DecodedValue::Relation { .. } => panic!("expected Property"),
        }
    }

    #[test]
    fn property_null_round_trip() {
        assert_eq!(property_round_trip(Value::Null), Value::Null);
    }

    #[test]
    fn property_bool_round_trip() {
        assert_eq!(property_round_trip(Value::Bool(true)), Value::Bool(true));
        assert_eq!(property_round_trip(Value::Bool(false)), Value::Bool(false));
    }

    #[test]
    fn property_int_round_trip() {
        assert_eq!(property_round_trip(Value::Int(0)), Value::Int(0));
        assert_eq!(property_round_trip(Value::Int(i64::MIN)), Value::Int(i64::MIN));
        assert_eq!(property_round_trip(Value::Int(i64::MAX)), Value::Int(i64::MAX));
    }

    #[test]
    fn property_float_round_trip() {
        let v = property_round_trip(Value::Float(std::f64::consts::PI));
        match v {
            Value::Float(f) => assert!((f - std::f64::consts::PI).abs() < 1e-15),
            other => panic!("expected Float, got {other:?}"),
        }
    }

    #[test]
    fn property_text_round_trip() {
        let s = "hello, PolarGraph! 🌐".to_string();
        assert_eq!(property_round_trip(Value::Text(s.clone())), Value::Text(s));
    }

    #[test]
    fn property_blob_round_trip() {
        let data = vec![0u8, 1, 127, 128, 255];
        assert_eq!(
            property_round_trip(Value::Blob(data.clone())),
            Value::Blob(data)
        );
    }

    #[test]
    fn property_temporal_survives_round_trip() {
        let t = temporal(12345, 99999, 0);
        let encoded = encode_property(&Value::Bool(true), &t).unwrap();
        match decode_value(&encoded).unwrap() {
            DecodedValue::Property { temporal, .. } => {
                assert_eq!(temporal.vt_start, Timestamp(12345));
                assert_eq!(temporal.vt_end, Timestamp(99999));
            }
            _ => panic!(),
        }
    }

    // ── error cases ───────────────────────────────────────────────────────────

    #[test]
    fn decode_empty_bytes_errors() {
        assert!(decode_value(&[]).is_err());
    }

    #[test]
    fn decode_unknown_discriminant_errors() {
        let bad = [0x00u8; 33];
        let err = decode_value(&bad).unwrap_err();
        assert!(err.to_string().contains("unknown discriminant"));

        let bad2 = [0xFFu8; 33];
        assert!(decode_value(&bad2).is_err());
    }

    #[test]
    fn decode_truncated_relation_errors() {
        let eid = edge_id_from_seed(0x01);
        let t = temporal(0, 0, 0);
        let full = encode_relation(&eid, &t);
        // One byte short
        assert!(decode_value(&full[..32]).is_err());
    }

    #[test]
    fn decode_truncated_property_errors() {
        let t = temporal(0, 0, 0);
        let full = encode_property(&Value::Bool(true), &t).unwrap();
        // Just the discriminant + partial timestamps, no JSON
        assert!(decode_value(&full[..16]).is_err());
    }

    #[test]
    fn property_vector_round_trip() {
        let v = Value::Vector(vec![1.0_f32, -2.5, 0.0, f32::MAX, f32::MIN_POSITIVE]);
        let t = temporal(100, i64::MAX, 0);
        let encoded = encode_property(&v, &t).unwrap();
        assert_eq!(encoded[0], DISC_VECTOR);
        // 1 disc + 8 vt_start + 8 vt_end + 4 len + 5 * 4 floats = 41
        assert_eq!(encoded.len(), 41);
        match decode_value(&encoded).unwrap() {
            DecodedValue::Property { value: Value::Vector(floats), temporal } => {
                assert_eq!(floats.len(), 5);
                assert_eq!(floats[0], 1.0_f32);
                assert_eq!(floats[1], -2.5_f32);
                assert_eq!(temporal.vt_start, Timestamp(100));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn property_empty_vector_round_trip() {
        let v = Value::Vector(vec![]);
        let t = temporal(0, 0, 0);
        let encoded = encode_property(&v, &t).unwrap();
        match decode_value(&encoded).unwrap() {
            DecodedValue::Property { value: Value::Vector(floats), .. } => {
                assert!(floats.is_empty());
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn decode_truncated_vector_errors() {
        let v = Value::Vector(vec![1.0, 2.0, 3.0]);
        let t = temporal(0, 0, 0);
        let full = encode_property(&v, &t).unwrap();
        // Truncate last float
        assert!(decode_value(&full[..full.len() - 1]).is_err());
    }
}
