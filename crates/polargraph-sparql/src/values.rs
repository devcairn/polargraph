//! SPARQL value semantics for bound literals (`docs/design/value-bindings.md`).
//!
//! - [`SparqlValue::from_value`] / [`SparqlValue::to_value`]: stored values ↔
//!   result values, losslessly for every bindable [`Value`].
//! - [`sparql_eq`] / [`sparql_cmp`]: the SPARQL `=` and `<` operators, with
//!   numeric promotion and type errors (`None`) as SPARQL 1.1 §17.3 defines.
//! - [`order_cmp`]: the total order `ORDER BY`, `MIN` and `MAX` use.
//! - [`term_key`]: RDF term identity, for `GROUP BY`, `DISTINCT` and joins.

use std::cmp::Ordering;

use polargraph_core::{term::iri_to_node_id, value::Value};

use crate::{names::IriNames, response::SparqlValue};

pub const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
const XSD_HEX_BINARY: &str = "http://www.w3.org/2001/XMLSchema#hexBinary";

impl SparqlValue {
    /// A stored property value as a result value. `None` for values SPARQL
    /// has no form for (vectors) and for `Null`.
    pub fn from_value(value: &Value) -> Option<Self> {
        Some(match value {
            Value::Null | Value::Vector(_) => return None,
            Value::Bool(b) => Self::LiteralBool(*b),
            Value::Int(n) => Self::LiteralInt(*n),
            Value::Float(f) => Self::LiteralFloat(*f),
            Value::Text(s) => Self::Literal(s.clone()),
            Value::Blob(b) => Self::TypedLiteral {
                lexical: b.iter().map(|x| format!("{x:02X}")).collect(),
                datatype: XSD_HEX_BINARY.to_string(),
            },
            Value::LangText { text, lang } => Self::LangLiteral {
                text: text.clone(),
                lang: lang.clone(),
            },
            Value::Typed { lexical, datatype } => Self::TypedLiteral {
                lexical: lexical.clone(),
                datatype: datatype.clone(),
            },
        })
    }

    /// The stored form of a literal (the inverse of [`Self::from_value`]),
    /// or `None` for an IRI.
    pub fn to_value(&self) -> Option<Value> {
        Some(match self {
            Self::Uri(_) | Self::Iri(_) => return None,
            Self::Literal(s) => Value::Text(s.clone()),
            Self::LiteralInt(n) => Value::Int(*n),
            Self::LiteralFloat(f) => Value::Float(*f),
            Self::LiteralBool(b) => Value::Bool(*b),
            Self::LangLiteral { text, lang } => Value::LangText {
                text: text.clone(),
                lang: lang.clone(),
            },
            Self::TypedLiteral { lexical, datatype } if datatype == XSD_HEX_BINARY => {
                match decode_hex(lexical) {
                    Some(bytes) => Value::Blob(bytes),
                    None => Value::Typed {
                        lexical: lexical.clone(),
                        datatype: datatype.clone(),
                    },
                }
            }
            Self::TypedLiteral { lexical, datatype } => Value::Typed {
                lexical: lexical.clone(),
                datatype: datatype.clone(),
            },
        })
    }

    /// The literal's lexical form (`STR`), or `None` for an IRI.
    pub fn lexical(&self) -> Option<String> {
        Some(match self {
            Self::Uri(_) | Self::Iri(_) => return None,
            Self::Literal(s) => s.clone(),
            Self::LiteralInt(n) => n.to_string(),
            Self::LiteralFloat(f) => f.to_string(),
            Self::LiteralBool(b) => b.to_string(),
            Self::LangLiteral { text, .. } => text.clone(),
            Self::TypedLiteral { lexical, .. } => lexical.clone(),
        })
    }
}

fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

/// A numeric value under SPARQL numeric promotion.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Num {
    Int(i64),
    Dbl(f64),
}

impl Num {
    pub(crate) fn as_f64(self) -> f64 {
        match self {
            Num::Int(n) => n as f64,
            Num::Dbl(f) => f,
        }
    }

    fn cmp(self, other: Num) -> Option<Ordering> {
        match (self, other) {
            // Integers compare exactly (no precision loss above 2^53).
            (Num::Int(a), Num::Int(b)) => Some(a.cmp(&b)),
            (a, b) => a.as_f64().partial_cmp(&b.as_f64()),
        }
    }
}

const XSD_INTEGERS: [&str; 13] = [
    "integer",
    "long",
    "int",
    "short",
    "byte",
    "nonNegativeInteger",
    "positiveInteger",
    "nonPositiveInteger",
    "negativeInteger",
    "unsignedLong",
    "unsignedInt",
    "unsignedShort",
    "unsignedByte",
];

/// The numeric value of a numeric literal: integers and doubles, plus typed
/// literals of an XSD numeric datatype (parsed from their lexical form).
pub(crate) fn numeric(v: &SparqlValue) -> Option<Num> {
    match v {
        SparqlValue::LiteralInt(n) => Some(Num::Int(*n)),
        SparqlValue::LiteralFloat(f) => Some(Num::Dbl(*f)),
        SparqlValue::TypedLiteral { lexical, datatype } => {
            let local = datatype.strip_prefix(XSD)?;
            if XSD_INTEGERS.contains(&local) {
                lexical.trim().parse().ok().map(Num::Int)
            } else if matches!(local, "decimal" | "double" | "float") {
                lexical.trim().parse().ok().map(Num::Dbl)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// XSD date / time datatypes whose lexical forms compare in order (same
/// format; timezones are compared as written).
fn is_temporal(datatype: &str) -> bool {
    matches!(
        datatype.strip_prefix(XSD),
        Some("date" | "dateTime" | "time" | "gYear" | "gYearMonth")
    )
}

/// The node an IRI value names.
fn node_of(v: &SparqlValue) -> Option<polargraph_core::id::NodeId> {
    match v {
        SparqlValue::Uri(id) => Some(*id),
        SparqlValue::Iri(iri) => Some(iri_to_node_id(iri)),
        _ => None,
    }
}

/// An IRI value's IRI text (for ordering).
fn iri_text(v: &SparqlValue, names: &IriNames) -> String {
    match v {
        SparqlValue::Uri(id) => names.iri(id),
        SparqlValue::Iri(iri) => iri.clone(),
        _ => String::new(),
    }
}

/// RDF term identity: equal keys ⇔ same term (`1` ≠ `1.0`, `"a"` ≠ `"a"@en`;
/// language tags compare case-insensitively).
pub fn term_key(v: &SparqlValue) -> String {
    match v {
        SparqlValue::Uri(id) => format!("U{}", id.0),
        SparqlValue::Iri(iri) => format!("U{}", iri_to_node_id(iri).0),
        SparqlValue::Literal(s) => format!("S{s}"),
        SparqlValue::LiteralInt(n) => format!("I{n}"),
        SparqlValue::LiteralFloat(f) => format!("D{}", f.to_bits()),
        SparqlValue::LiteralBool(b) => format!("B{b}"),
        SparqlValue::LangLiteral { text, lang } => {
            format!("L{}\u{0}{text}", lang.to_ascii_lowercase())
        }
        SparqlValue::TypedLiteral { lexical, datatype } => format!("T{datatype}\u{0}{lexical}"),
    }
}

/// SPARQL `=`: `Some(result)`, or `None` for a type error.
///
/// Numbers compare by value (`1 = 1.0`); strings, booleans, language-tagged
/// strings (text and tag) and same-datatype typed literals compare as
/// values; an IRI equals only the same IRI and never a literal. Literals of
/// different, non-comparable types are a type error (RDFterm-equal), unless
/// they are the same term.
pub fn sparql_eq(a: &SparqlValue, b: &SparqlValue) -> Option<bool> {
    use SparqlValue::*;
    if let (Some(x), Some(y)) = (numeric(a), numeric(b)) {
        return x.cmp(y).map(|o| o == Ordering::Equal);
    }
    if let (Some(x), Some(y)) = (node_of(a), node_of(b)) {
        return Some(x == y);
    }
    match (a, b) {
        (Uri(_) | Iri(_), _) | (_, Uri(_) | Iri(_)) => Some(false),
        (Literal(x), Literal(y)) => Some(x == y),
        (LiteralBool(x), LiteralBool(y)) => Some(x == y),
        (LangLiteral { text: t1, lang: l1 }, LangLiteral { text: t2, lang: l2 }) => {
            Some(t1 == t2 && l1.eq_ignore_ascii_case(l2))
        }
        (
            TypedLiteral {
                lexical: x,
                datatype: d1,
            },
            TypedLiteral {
                lexical: y,
                datatype: d2,
            },
        ) if d1 == d2 => Some(x == y),
        _ if term_key(a) == term_key(b) => Some(true),
        _ => None,
    }
}

/// SPARQL `<` family: the ordering of `a` and `b`, or `None` for a type
/// error. Defined for numbers (with promotion), strings, booleans and
/// same-datatype dates / times; language-tagged strings, IRIs and other
/// typed literals don't compare (SPARQL 1.1 §17.3).
pub fn sparql_cmp(a: &SparqlValue, b: &SparqlValue) -> Option<Ordering> {
    use SparqlValue::*;
    if let (Some(x), Some(y)) = (numeric(a), numeric(b)) {
        return x.cmp(y);
    }
    match (a, b) {
        (Literal(x), Literal(y)) => Some(x.cmp(y)),
        (LiteralBool(x), LiteralBool(y)) => Some(x.cmp(y)),
        (
            TypedLiteral {
                lexical: x,
                datatype: d1,
            },
            TypedLiteral {
                lexical: y,
                datatype: d2,
            },
        ) if d1 == d2 && is_temporal(d1) => Some(x.cmp(y)),
        _ => None,
    }
}

/// `ORDER BY`'s total order (also `MIN` / `MAX`): unbound < IRIs (by IRI) <
/// numbers (by value) < booleans < plain strings < language-tagged strings
/// (text, then tag) < other typed literals (datatype, then lexical form).
/// SPARQL fixes unbound < IRIs < literals; the order between literal kinds
/// is ours, so results are deterministic.
pub fn order_cmp(a: Option<&SparqlValue>, b: Option<&SparqlValue>, names: &IriNames) -> Ordering {
    fn rank(v: Option<&SparqlValue>) -> u8 {
        match v {
            None => 0,
            Some(SparqlValue::Uri(_) | SparqlValue::Iri(_)) => 1,
            Some(v) if numeric(v).is_some() => 2,
            Some(SparqlValue::LiteralBool(_)) => 3,
            Some(SparqlValue::Literal(_)) => 4,
            Some(SparqlValue::LangLiteral { .. }) => 5,
            Some(_) => 6,
        }
    }
    let by_rank = rank(a).cmp(&rank(b));
    if by_rank != Ordering::Equal {
        return by_rank;
    }
    let (Some(a), Some(b)) = (a, b) else {
        return Ordering::Equal;
    };
    let within = match (a, b) {
        (SparqlValue::Uri(_) | SparqlValue::Iri(_), _) => {
            iri_text(a, names).cmp(&iri_text(b, names))
        }
        (SparqlValue::LiteralBool(x), SparqlValue::LiteralBool(y)) => x.cmp(y),
        (SparqlValue::Literal(x), SparqlValue::Literal(y)) => x.cmp(y),
        (
            SparqlValue::LangLiteral { text: t1, lang: l1 },
            SparqlValue::LangLiteral { text: t2, lang: l2 },
        ) => t1
            .cmp(t2)
            .then_with(|| l1.to_ascii_lowercase().cmp(&l2.to_ascii_lowercase())),
        (
            SparqlValue::TypedLiteral {
                lexical: x,
                datatype: d1,
            },
            SparqlValue::TypedLiteral {
                lexical: y,
                datatype: d2,
            },
        ) if numeric(a).is_none() => d1.cmp(d2).then_with(|| x.cmp(y)),
        _ => match (numeric(a), numeric(b)) {
            (Some(x), Some(y)) => x.cmp(y).unwrap_or(Ordering::Equal),
            _ => Ordering::Equal,
        },
    };
    // Equal by value but different terms (1 vs 1.0): order by term so the
    // result is stable.
    within.then_with(|| term_key(a).cmp(&term_key(b)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use SparqlValue::*;

    fn lang(t: &str, l: &str) -> SparqlValue {
        LangLiteral {
            text: t.into(),
            lang: l.into(),
        }
    }
    fn typed(x: &str, d: &str) -> SparqlValue {
        TypedLiteral {
            lexical: x.into(),
            datatype: format!("{XSD}{d}"),
        }
    }

    #[test]
    fn equality_follows_sparql() {
        assert_eq!(sparql_eq(&LiteralInt(1), &LiteralFloat(1.0)), Some(true));
        assert_eq!(
            sparql_eq(&LiteralInt(1), &typed("01", "unsignedInt")),
            Some(true)
        );
        assert_eq!(sparql_eq(&lang("a", "EN"), &lang("a", "en")), Some(true));
        assert_eq!(sparql_eq(&Literal("a".into()), &lang("a", "en")), None);
        assert_eq!(sparql_eq(&Literal("a".into()), &LiteralInt(1)), None);
        assert_eq!(
            sparql_eq(&typed("x", "foo"), &typed("x", "foo")),
            Some(true)
        );
        assert_eq!(sparql_eq(&typed("x", "foo"), &typed("x", "bar")), None);
    }

    #[test]
    fn comparison_follows_sparql() {
        assert_eq!(
            sparql_cmp(&LiteralInt(2), &LiteralFloat(1.5)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            sparql_cmp(&LiteralInt(i64::MAX), &LiteralInt(i64::MAX - 1)),
            Some(Ordering::Greater),
            "integers compare exactly"
        );
        assert_eq!(
            sparql_cmp(&typed("2020-01-02", "date"), &typed("2020-01-10", "date")),
            Some(Ordering::Less)
        );
        assert_eq!(sparql_cmp(&lang("a", "en"), &lang("b", "en")), None);
        assert_eq!(sparql_cmp(&Literal("a".into()), &LiteralInt(1)), None);
    }

    #[test]
    fn term_keys_distinguish_terms() {
        assert_ne!(term_key(&LiteralInt(1)), term_key(&LiteralFloat(1.0)));
        assert_ne!(term_key(&Literal("a".into())), term_key(&lang("a", "en")));
        assert_eq!(term_key(&lang("a", "EN")), term_key(&lang("a", "en")));
    }

    #[test]
    fn values_round_trip() {
        for v in [
            Value::Int(3),
            Value::Float(2.5),
            Value::Bool(true),
            Value::Text("x".into()),
            Value::Blob(vec![0, 255, 16]),
            Value::LangText {
                text: "hei".into(),
                lang: "no".into(),
            },
            Value::Typed {
                lexical: "2020-01-01".into(),
                datatype: format!("{XSD}date"),
            },
        ] {
            assert_eq!(SparqlValue::from_value(&v).unwrap().to_value(), Some(v));
        }
        assert!(SparqlValue::from_value(&Value::Vector(vec![1.0])).is_none());
    }
}
