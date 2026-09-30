//! Property values stored on nodes and edges.

use serde::{Deserialize, Serialize};

/// A typed property value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "v")]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
    /// Dense float embedding vector. Stored in binary (LE f32) rather than JSON.
    Vector(Vec<f32>),
    /// A language-tagged string (`rdf:langString`), e.g. `"Acme"@en`.
    /// `lang` is a BCP 47 tag as written (not normalised).
    LangText {
        text: String,
        lang: String,
    },
    /// An RDF literal whose datatype has no native variant, e.g.
    /// `"2026-09-29"^^xsd:date`. `datatype` is the full IRI; `lexical` is the
    /// literal's lexical form, unchanged.
    Typed {
        lexical: String,
        datatype: String,
    },
}

impl Value {
    /// Canonical, stable byte encoding used for [`Value::content_hash`].
    ///
    /// Deliberately not `serde_json` (whose float and escaping output isn't a
    /// stable contract). See `docs/design/v3-key-layout.md` §4.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            Value::Null => out.push(0x00),
            Value::Bool(b) => out.extend_from_slice(&[0x01, *b as u8]),
            Value::Int(i) => {
                out.push(0x02);
                out.extend_from_slice(&i.to_be_bytes());
            }
            Value::Float(f) => {
                out.push(0x03);
                let canonical = if f.is_nan() {
                    f64::NAN
                } else if *f == 0.0 {
                    0.0 // folds -0.0 into 0.0
                } else {
                    *f
                };
                out.extend_from_slice(&canonical.to_bits().to_be_bytes());
            }
            Value::Text(s) => {
                out.push(0x04);
                out.extend_from_slice(s.as_bytes());
            }
            Value::Blob(b) => {
                out.push(0x05);
                out.extend_from_slice(b);
            }
            Value::Vector(v) => {
                out.push(0x06);
                for f in v {
                    out.extend_from_slice(&f.to_le_bytes());
                }
            }
            Value::LangText { text, lang } => {
                out.push(0x07);
                // RDF 1.1 compares language tags case-insensitively.
                out.extend_from_slice(lang.to_ascii_lowercase().as_bytes());
                out.push(0x00);
                out.extend_from_slice(text.as_bytes());
            }
            Value::Typed { lexical, datatype } => {
                out.push(0x08);
                out.extend_from_slice(datatype.as_bytes());
                out.push(0x00);
                out.extend_from_slice(lexical.as_bytes());
            }
        }
        out
    }

    /// 128-bit content hash of the value (xxHash3-128 of
    /// [`Value::canonical_bytes`], little-endian). Two values with the same
    /// hash are the same value; it is the object slot of property keys and
    /// the key of out-of-line value storage.
    pub fn content_hash(&self) -> [u8; 16] {
        xxhash_rust::xxh3::xxh3_128(&self.canonical_bytes()).to_le_bytes()
    }

    /// The string content of textual values (`Text` and `LangText`), for
    /// text search and string predicates.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Value::Text(s) | Value::LangText { text: s, .. } => Some(s),
            _ => None,
        }
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}
impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int(v)
    }
}
impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}
impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::Text(v)
    }
}
impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Text(v.to_owned())
    }
}
impl From<Vec<f32>> for Value {
    fn from(v: Vec<f32>) -> Self {
        Value::Vector(v)
    }
}

#[cfg(test)]
mod hash_tests {
    use super::*;

    #[test]
    fn content_hash_distinguishes_types_tags_and_datatypes() {
        let h = |v: Value| v.content_hash();
        assert_ne!(h(Value::Text("30".into())), h(Value::Int(30)));
        assert_ne!(
            h(Value::Text("Acme".into())),
            h(Value::LangText {
                text: "Acme".into(),
                lang: "en".into()
            })
        );
        assert_ne!(
            h(Value::LangText {
                text: "Acme".into(),
                lang: "en".into()
            }),
            h(Value::LangText {
                text: "Acme".into(),
                lang: "fr".into()
            })
        );
        // Language tags compare case-insensitively.
        assert_eq!(
            h(Value::LangText {
                text: "Acme".into(),
                lang: "en-GB".into()
            }),
            h(Value::LangText {
                text: "Acme".into(),
                lang: "en-gb".into()
            })
        );
        // Separator keeps (datatype, lexical) splits apart.
        assert_ne!(
            h(Value::Typed {
                lexical: "b".into(),
                datatype: "a".into()
            }),
            h(Value::Typed {
                lexical: "".into(),
                datatype: "a\0b".into()
            })
        );
    }

    #[test]
    fn content_hash_normalises_float_zero_and_nan() {
        assert_eq!(
            Value::Float(0.0).content_hash(),
            Value::Float(-0.0).content_hash()
        );
        assert_eq!(
            Value::Float(f64::NAN).content_hash(),
            Value::Float(-f64::NAN).content_hash()
        );
        assert_ne!(
            Value::Float(1.0).content_hash(),
            Value::Float(2.0).content_hash()
        );
    }

    #[test]
    fn content_hash_is_stable() {
        // Pinned: changing the canonical encoding invalidates every stored key.
        assert_eq!(
            hex(&Value::Text("hello".into()).content_hash()),
            hex(&xxhash_rust::xxh3::xxh3_128(b"\x04hello").to_le_bytes())
        );
    }

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }
}
