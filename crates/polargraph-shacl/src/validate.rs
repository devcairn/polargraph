//! Shape evaluation.

use std::collections::HashSet;

use polargraph_core::{id::NodeId, value::Value};

use crate::{
    data::{DataView, Obj},
    report::{ValidationReport, ValidationResult},
    shapes::{Constraint, NodeKind, Path, Shape, ShapeError, Shapes, Target},
    vocab::{RDF_LANG_STRING, XSD},
};

/// Validate `data` against `shapes`. With `focus`, only focus nodes in that
/// set are validated (incremental validation of an overlay: pass
/// [`DataView::touched`]).
pub fn validate(
    shapes: &Shapes,
    data: &DataView<'_>,
    focus: Option<&HashSet<NodeId>>,
) -> Result<ValidationReport, ShapeError> {
    let mut ctx = Ctx {
        shapes,
        data,
        stack: Vec::new(),
    };
    let mut report = ValidationReport::default();
    let mut ids: Vec<&NodeId> = shapes.shapes.keys().collect();
    ids.sort_by_key(|id| *id.as_bytes());
    for id in ids {
        let shape = &shapes.shapes[id];
        if shape.deactivated || shape.targets.is_empty() {
            continue;
        }
        let mut nodes: Vec<Obj> = Vec::new();
        let mut seen = HashSet::new();
        for target in &shape.targets {
            for node in ctx.target_nodes(target)? {
                if focus.is_some_and(|f| node.as_node().map_or(true, |n| !f.contains(&n))) {
                    continue;
                }
                if seen.insert(node.key()) {
                    nodes.push(node);
                }
            }
        }
        for node in nodes {
            ctx.validate_shape(shape, &node, &mut report.results)?;
        }
    }
    Ok(report)
}

struct Ctx<'a, 'b> {
    shapes: &'a Shapes,
    data: &'a DataView<'b>,
    /// `(shape, focus)` pairs being evaluated through `sh:node`, to stop
    /// recursive shapes.
    stack: Vec<(NodeId, NodeId)>,
}

impl Ctx<'_, '_> {
    fn target_nodes(&self, target: &Target) -> Result<Vec<Obj>, ShapeError> {
        Ok(match target {
            Target::Class(c) => self.data.instances(c)?.into_iter().map(Obj::Node).collect(),
            Target::Node(o) => vec![o.clone()],
            Target::SubjectsOf(p) => self
                .data
                .subjects_of(p)?
                .into_iter()
                .map(Obj::Node)
                .collect(),
            Target::ObjectsOf(p) => self.data.objects_of(p)?,
        })
    }

    fn path_values(&self, focus: &Obj, path: &Path) -> Result<Vec<Obj>, ShapeError> {
        Ok(match path {
            Path::Predicate(p) => match focus {
                Obj::Node(n) => self.data.objects(n, p)?,
                Obj::Lit(_) => vec![],
            },
            Path::Inverse(inner) => match inner.as_ref() {
                Path::Predicate(p) => self
                    .data
                    .subjects(p, focus)?
                    .into_iter()
                    .map(Obj::Node)
                    .collect(),
                _ => {
                    return Err(ShapeError::Invalid {
                        shape: String::new(),
                        message: "only predicate paths can be inverted".into(),
                    })
                }
            },
            Path::Sequence(steps) => {
                let mut cur = vec![focus.clone()];
                for step in steps {
                    let mut next = Vec::new();
                    let mut seen = HashSet::new();
                    for o in &cur {
                        for v in self.path_values(o, step)? {
                            if seen.insert(v.key()) {
                                next.push(v);
                            }
                        }
                    }
                    cur = next;
                }
                cur
            }
        })
    }

    fn validate_shape(
        &mut self,
        shape: &Shape,
        focus: &Obj,
        out: &mut Vec<ValidationResult>,
    ) -> Result<(), ShapeError> {
        if shape.deactivated {
            return Ok(());
        }
        let values = match &shape.path {
            Some(path) => {
                let mut seen = HashSet::new();
                self.path_values(focus, path)?
                    .into_iter()
                    .filter(|v| seen.insert(v.key()))
                    .collect()
            }
            None => vec![focus.clone()],
        };
        let mut push = |c: &Constraint, value: Option<Obj>, detail: String| {
            out.push(ValidationResult {
                focus_node: focus.clone(),
                path: shape.path.clone(),
                value,
                source_shape: shape.id,
                component: c.component(),
                severity: shape.severity,
                message: shape.message.clone().unwrap_or(detail),
            });
        };

        for c in &shape.constraints {
            match c {
                Constraint::MinCount(n) if (values.len() as u64) < *n => push(
                    c,
                    None,
                    format!("expected at least {n} value(s), found {}", values.len()),
                ),
                Constraint::MaxCount(n) if (values.len() as u64) > *n => push(
                    c,
                    None,
                    format!("expected at most {n} value(s), found {}", values.len()),
                ),
                Constraint::MinCount(_) | Constraint::MaxCount(_) => {}
                Constraint::Closed(allowed) => {
                    if let Obj::Node(n) = focus {
                        let mut extra: Vec<String> = self
                            .data
                            .predicates_of(n)?
                            .into_iter()
                            .filter(|p| !allowed.contains(p))
                            .collect();
                        extra.sort();
                        for p in extra {
                            for v in self.data.objects(n, &p)? {
                                push(
                                    c,
                                    Some(v),
                                    format!("property {p} is not allowed by a closed shape"),
                                );
                            }
                        }
                    }
                }
                _ => {
                    for v in &values {
                        if let Some(detail) = self.check_value(c, v)? {
                            push(c, Some(v.clone()), detail);
                        }
                    }
                }
            }
        }

        for p in &shape.properties {
            if let Some(ps) = self.shapes.shapes.get(p) {
                self.validate_shape(ps, focus, out)?;
            }
        }
        Ok(())
    }

    /// `Some(reason)` if `v` violates the value-level constraint `c`.
    fn check_value(&mut self, c: &Constraint, v: &Obj) -> Result<Option<String>, ShapeError> {
        let fail = |ok: bool, msg: String| if ok { None } else { Some(msg) };
        Ok(match c {
            Constraint::Datatype(dt) => fail(
                matches!(v, Obj::Lit(val) if has_datatype(val, dt)),
                format!("value does not have datatype {dt}"),
            ),
            Constraint::Class(class) => {
                let ok = match v {
                    Obj::Node(n) => self.data.types(n)?.contains(class),
                    Obj::Lit(_) => false,
                };
                fail(
                    ok,
                    format!("value is not an instance of {}", self.data.iri(class)?),
                )
            }
            Constraint::NodeKind(kind) => {
                let actual = match v {
                    Obj::Lit(_) => NodeKind::Literal,
                    Obj::Node(n) if self.data.is_blank(n)? => NodeKind::BlankNode,
                    Obj::Node(_) => NodeKind::Iri,
                };
                let ok = match kind {
                    NodeKind::BlankNodeOrIri => actual != NodeKind::Literal,
                    NodeKind::BlankNodeOrLiteral => actual != NodeKind::Iri,
                    NodeKind::IriOrLiteral => actual != NodeKind::BlankNode,
                    k => *k == actual,
                };
                fail(ok, format!("value node kind is not {kind:?}"))
            }
            Constraint::MinInclusive(b) => range(v, b, |o| o.is_ge(), ">="),
            Constraint::MaxInclusive(b) => range(v, b, |o| o.is_le(), "<="),
            Constraint::MinExclusive(b) => range(v, b, |o| o.is_gt(), ">"),
            Constraint::MaxExclusive(b) => range(v, b, |o| o.is_lt(), "<"),
            Constraint::Pattern(re) => {
                let s = self.string_value(v)?;
                fail(
                    s.as_deref().is_some_and(|s| re.is_match(s)),
                    format!("value does not match pattern {}", re.as_str()),
                )
            }
            Constraint::MinLength(n) => {
                let s = self.string_value(v)?;
                fail(
                    s.is_some_and(|s| s.chars().count() as u64 >= *n),
                    format!("value is shorter than {n} characters"),
                )
            }
            Constraint::MaxLength(n) => {
                let s = self.string_value(v)?;
                fail(
                    s.is_some_and(|s| s.chars().count() as u64 <= *n),
                    format!("value is longer than {n} characters"),
                )
            }
            Constraint::In(allowed) => fail(
                allowed.iter().any(|a| a.key() == v.key()),
                "value is not in the allowed list".to_string(),
            ),
            Constraint::Node(shape_id) => {
                let ok = self.conforms_to(shape_id, v)?;
                fail(
                    ok,
                    format!(
                        "value does not conform to shape {}",
                        self.data.iri(shape_id)?
                    ),
                )
            }
            Constraint::MinCount(_) | Constraint::MaxCount(_) | Constraint::Closed(_) => None,
        })
    }

    fn conforms_to(&mut self, shape_id: &NodeId, v: &Obj) -> Result<bool, ShapeError> {
        let Some(shape) = self.shapes.shapes.get(shape_id) else {
            return Ok(true);
        };
        let key = (*shape_id, v.key());
        if self.stack.contains(&key) {
            return Ok(true); // recursive shape: assume conformance
        }
        self.stack.push(key);
        let mut scratch = Vec::new();
        let r = self.validate_shape(shape, v, &mut scratch);
        self.stack.pop();
        r?;
        Ok(scratch.is_empty())
    }

    /// The string form for `sh:pattern` / length checks: a literal's lexical
    /// form or an IRI; `None` for blank nodes.
    fn string_value(&self, v: &Obj) -> Result<Option<String>, ShapeError> {
        Ok(match v {
            Obj::Lit(val) => lexical(val),
            Obj::Node(n) if self.data.is_blank(n)? => None,
            Obj::Node(n) => Some(self.data.iri(n)?),
        })
    }
}

/// A literal's lexical form.
fn lexical(v: &Value) -> Option<String> {
    match v {
        Value::Text(s) | Value::LangText { text: s, .. } => Some(s.clone()),
        Value::Typed { lexical, .. } => Some(lexical.clone()),
        Value::Int(n) => Some(n.to_string()),
        Value::Float(f) => Some(f.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Whether `v` has datatype `dt` (an IRI), following how RDF literals map to
/// values (`term::literal_to_value`).
fn has_datatype(v: &Value, dt: &str) -> bool {
    let xsd = dt.strip_prefix(XSD);
    match v {
        Value::Text(_) => xsd == Some("string"),
        Value::LangText { .. } => dt == RDF_LANG_STRING,
        Value::Int(n) => match xsd {
            Some("integer" | "long" | "int" | "short" | "byte") => true,
            Some("nonNegativeInteger") => *n >= 0,
            Some("positiveInteger") => *n > 0,
            _ => false,
        },
        Value::Float(_) => matches!(xsd, Some("double" | "float" | "decimal")),
        Value::Bool(_) => xsd == Some("boolean"),
        Value::Typed { datatype, .. } => datatype == dt,
        _ => false,
    }
}

/// Range constraint: `cmp(value vs bound)` must hold; incomparable values
/// violate (as in SHACL).
fn range(
    v: &Obj,
    bound: &Value,
    ok: impl Fn(std::cmp::Ordering) -> bool,
    op: &str,
) -> Option<String> {
    let ord = match v {
        Obj::Lit(val) => compare(val, bound),
        Obj::Node(_) => None,
    };
    if ord.is_some_and(ok) {
        None
    } else {
        Some(format!(
            "value is not {op} {}",
            lexical(bound).unwrap_or_default()
        ))
    }
}

fn compare(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    let num = |v: &Value| match v {
        Value::Int(n) => Some(*n as f64),
        Value::Float(f) => Some(*f),
        _ => None,
    };
    match (a, b) {
        (
            Value::Typed {
                lexical: x,
                datatype: dx,
            },
            Value::Typed {
                lexical: y,
                datatype: dy,
            },
        ) if dx == dy => Some(x.cmp(y)),
        (Value::Text(x), Value::Text(y)) => Some(x.cmp(y)),
        _ => num(a)?.partial_cmp(&num(b)?),
    }
}
