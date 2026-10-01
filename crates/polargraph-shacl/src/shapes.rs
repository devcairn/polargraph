//! Shapes read from shapes graphs.

use std::collections::{HashMap, HashSet};

use polargraph_core::{id::NodeId, term::iri_to_node_id, value::Value};
use polargraph_storage::StorageError;
use regex::Regex;

use crate::{
    data::{properties, DataView, Obj},
    report::Severity,
    vocab::{sh, RDFS_CLASS, RDF_FIRST, RDF_NIL, RDF_REST, RDF_TYPE},
};

#[derive(Debug, thiserror::Error)]
pub enum ShapeError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("invalid shape {shape}: {message}")]
    Invalid { shape: String, message: String },
}

#[derive(Debug, Clone)]
pub enum Target {
    Class(NodeId),
    Node(Obj),
    SubjectsOf(String),
    ObjectsOf(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Path {
    Predicate(String),
    Inverse(Box<Path>),
    Sequence(Vec<Path>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    Iri,
    BlankNode,
    Literal,
    BlankNodeOrIri,
    BlankNodeOrLiteral,
    IriOrLiteral,
}

#[derive(Debug, Clone)]
pub enum Constraint {
    MinCount(u64),
    MaxCount(u64),
    Datatype(String),
    Class(NodeId),
    NodeKind(NodeKind),
    MinInclusive(Value),
    MaxInclusive(Value),
    MinExclusive(Value),
    MaxExclusive(Value),
    Pattern(Regex),
    MinLength(u64),
    MaxLength(u64),
    In(Vec<Obj>),
    Node(NodeId),
    /// `sh:closed true` with the allowed predicates (declared paths +
    /// `sh:ignoredProperties`).
    Closed(HashSet<String>),
}

impl Constraint {
    /// The SHACL constraint component IRI's local name.
    pub fn component(&self) -> &'static str {
        match self {
            Constraint::MinCount(_) => "MinCountConstraintComponent",
            Constraint::MaxCount(_) => "MaxCountConstraintComponent",
            Constraint::Datatype(_) => "DatatypeConstraintComponent",
            Constraint::Class(_) => "ClassConstraintComponent",
            Constraint::NodeKind(_) => "NodeKindConstraintComponent",
            Constraint::MinInclusive(_) => "MinInclusiveConstraintComponent",
            Constraint::MaxInclusive(_) => "MaxInclusiveConstraintComponent",
            Constraint::MinExclusive(_) => "MinExclusiveConstraintComponent",
            Constraint::MaxExclusive(_) => "MaxExclusiveConstraintComponent",
            Constraint::Pattern(_) => "PatternConstraintComponent",
            Constraint::MinLength(_) => "MinLengthConstraintComponent",
            Constraint::MaxLength(_) => "MaxLengthConstraintComponent",
            Constraint::In(_) => "InConstraintComponent",
            Constraint::Node(_) => "NodeConstraintComponent",
            Constraint::Closed(_) => "ClosedConstraintComponent",
        }
    }
}

/// A node shape (no path) or property shape (with a path).
#[derive(Debug, Clone)]
pub struct Shape {
    pub id: NodeId,
    pub targets: Vec<Target>,
    pub path: Option<Path>,
    pub constraints: Vec<Constraint>,
    /// `sh:property` shapes.
    pub properties: Vec<NodeId>,
    pub severity: Severity,
    pub message: Option<String>,
    pub deactivated: bool,
}

/// Every shape in the shapes graphs, by node.
#[derive(Debug, Clone, Default)]
pub struct Shapes {
    pub shapes: HashMap<NodeId, Shape>,
}

impl Shapes {
    /// Read the shapes declared in `view` (the shapes graphs): every node
    /// typed `sh:NodeShape` / `sh:PropertyShape`, with a target, or
    /// referenced by `sh:property` / `sh:node`.
    pub fn load(view: &DataView<'_>) -> Result<Self, ShapeError> {
        let mut ids: HashSet<NodeId> = HashSet::new();
        for kind in ["NodeShape", "PropertyShape"] {
            ids.extend(view.subjects(RDF_TYPE, &Obj::Node(iri_to_node_id(&sh(kind))))?);
        }
        for p in [
            "targetClass",
            "targetNode",
            "targetSubjectsOf",
            "targetObjectsOf",
        ] {
            ids.extend(view.subjects_of(&sh(p))?);
        }
        for p in ["property", "node"] {
            ids.extend(view.objects_of(&sh(p))?.iter().filter_map(Obj::as_node));
        }
        let mut shapes = HashMap::new();
        for id in ids {
            shapes.insert(id, parse_shape(view, id)?);
        }
        Ok(Self { shapes })
    }
}

fn parse_shape(view: &DataView<'_>, id: NodeId) -> Result<Shape, ShapeError> {
    let name = view.iri(&id)?;
    let invalid = |message: String| ShapeError::Invalid {
        shape: name.clone(),
        message,
    };
    let props = properties(view, &id)?;
    let get = |local: &str| props.get(&sh(local)).cloned().unwrap_or_default();
    let int = |local: &str| -> Result<Option<u64>, ShapeError> {
        match get(local).first() {
            None => Ok(None),
            Some(Obj::Lit(Value::Int(n))) if *n >= 0 => Ok(Some(*n as u64)),
            Some(other) => Err(invalid(format!(
                "sh:{local} must be a non-negative integer, got {other:?}"
            ))),
        }
    };
    let node = |o: &Obj, local: &str| -> Result<NodeId, ShapeError> {
        o.as_node()
            .ok_or_else(|| invalid(format!("sh:{local} must be an IRI")))
    };

    let mut targets = Vec::new();
    for o in get("targetClass") {
        targets.push(Target::Class(node(&o, "targetClass")?));
    }
    for o in get("targetNode") {
        targets.push(Target::Node(o));
    }
    for o in get("targetSubjectsOf") {
        targets.push(Target::SubjectsOf(
            view.iri(&node(&o, "targetSubjectsOf")?)?,
        ));
    }
    for o in get("targetObjectsOf") {
        targets.push(Target::ObjectsOf(view.iri(&node(&o, "targetObjectsOf")?)?));
    }
    // Implicit class target: a shape that is also an rdfs:Class.
    let types = props.get(RDF_TYPE).cloned().unwrap_or_default();
    if types.contains(&Obj::Node(iri_to_node_id(RDFS_CLASS))) {
        targets.push(Target::Class(id));
    }

    let path = match get("path").first() {
        Some(p) => Some(parse_path(view, &node(p, "path")?, &invalid)?),
        None => None,
    };

    let mut constraints = Vec::new();
    if let Some(n) = int("minCount")? {
        constraints.push(Constraint::MinCount(n));
    }
    if let Some(n) = int("maxCount")? {
        constraints.push(Constraint::MaxCount(n));
    }
    for o in get("datatype") {
        constraints.push(Constraint::Datatype(view.iri(&node(&o, "datatype")?)?));
    }
    for o in get("class") {
        constraints.push(Constraint::Class(node(&o, "class")?));
    }
    for o in get("nodeKind") {
        let kind = view.iri(&node(&o, "nodeKind")?)?;
        let kind = match kind.strip_prefix(crate::vocab::SH) {
            Some("IRI") => NodeKind::Iri,
            Some("BlankNode") => NodeKind::BlankNode,
            Some("Literal") => NodeKind::Literal,
            Some("BlankNodeOrIRI") => NodeKind::BlankNodeOrIri,
            Some("BlankNodeOrLiteral") => NodeKind::BlankNodeOrLiteral,
            Some("IRIOrLiteral") => NodeKind::IriOrLiteral,
            _ => return Err(invalid(format!("unknown sh:nodeKind {kind}"))),
        };
        constraints.push(Constraint::NodeKind(kind));
    }
    for (local, make) in [
        (
            "minInclusive",
            Constraint::MinInclusive as fn(Value) -> Constraint,
        ),
        ("maxInclusive", Constraint::MaxInclusive),
        ("minExclusive", Constraint::MinExclusive),
        ("maxExclusive", Constraint::MaxExclusive),
    ] {
        for o in get(local) {
            match o {
                Obj::Lit(v) => constraints.push(make(v)),
                Obj::Node(_) => return Err(invalid(format!("sh:{local} must be a literal"))),
            }
        }
    }
    if let Some(Obj::Lit(p)) = get("pattern").first() {
        let flags = match get("flags").first() {
            Some(Obj::Lit(f)) => f.as_text().unwrap_or_default().to_string(),
            _ => String::new(),
        };
        let pattern = p.as_text().unwrap_or_default();
        let source = if flags.is_empty() {
            pattern.to_string()
        } else {
            format!("(?{flags}){pattern}")
        };
        let re = Regex::new(&source).map_err(|e| invalid(format!("bad sh:pattern: {e}")))?;
        constraints.push(Constraint::Pattern(re));
    }
    if let Some(n) = int("minLength")? {
        constraints.push(Constraint::MinLength(n));
    }
    if let Some(n) = int("maxLength")? {
        constraints.push(Constraint::MaxLength(n));
    }
    if let Some(head) = get("in").first() {
        constraints.push(Constraint::In(list(view, &node(head, "in")?)?));
    }
    for o in get("node") {
        constraints.push(Constraint::Node(node(&o, "node")?));
    }
    let properties: Vec<NodeId> = get("property")
        .iter()
        .map(|o| node(o, "property"))
        .collect::<Result<_, _>>()?;
    if matches!(get("closed").first(), Some(Obj::Lit(Value::Bool(true)))) {
        let mut allowed = HashSet::new();
        for p in &properties {
            if let Some(Obj::Node(path)) = properties_of(view, p)?
                .get(&sh("path"))
                .and_then(|v| v.first())
            {
                if let Path::Predicate(pred) = parse_path(view, path, &invalid)? {
                    allowed.insert(pred);
                }
            }
        }
        if let Some(head) = get("ignoredProperties").first() {
            for o in list(view, &node(head, "ignoredProperties")?)? {
                allowed.insert(view.iri(&node(&o, "ignoredProperties")?)?);
            }
        }
        constraints.push(Constraint::Closed(allowed));
    }

    let severity = match get("severity").first() {
        None => Severity::Violation,
        Some(o) => match view
            .iri(&node(o, "severity")?)?
            .strip_prefix(crate::vocab::SH)
        {
            Some("Warning") => Severity::Warning,
            Some("Info") => Severity::Info,
            _ => Severity::Violation,
        },
    };
    let message = match get("message").first() {
        Some(Obj::Lit(v)) => v.as_text().map(str::to_string),
        _ => None,
    };
    let deactivated = matches!(
        get("deactivated").first(),
        Some(Obj::Lit(Value::Bool(true)))
    );

    Ok(Shape {
        id,
        targets,
        path,
        constraints,
        properties,
        severity,
        message,
        deactivated,
    })
}

fn properties_of(view: &DataView<'_>, n: &NodeId) -> Result<HashMap<String, Vec<Obj>>, ShapeError> {
    Ok(properties(view, n)?)
}

fn parse_path(
    view: &DataView<'_>,
    n: &NodeId,
    invalid: &dyn Fn(String) -> ShapeError,
) -> Result<Path, ShapeError> {
    let props = properties_of(view, n)?;
    if props.contains_key(RDF_FIRST) {
        let steps = list(view, n)?
            .iter()
            .map(|o| match o {
                Obj::Node(step) => parse_path(view, step, invalid),
                Obj::Lit(_) => Err(invalid("sequence path steps must be IRIs".into())),
            })
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(Path::Sequence(steps));
    }
    if let Some(Obj::Node(inner)) = props.get(&sh("inversePath")).and_then(|v| v.first()) {
        return Ok(Path::Inverse(Box::new(parse_path(view, inner, invalid)?)));
    }
    for unsupported in [
        "alternativePath",
        "zeroOrMorePath",
        "oneOrMorePath",
        "zeroOrOnePath",
    ] {
        if props.contains_key(&sh(unsupported)) {
            return Err(invalid(format!("sh:{unsupported} is not supported")));
        }
    }
    Ok(Path::Predicate(view.iri(n)?))
}

/// The members of the RDF list starting at `head`.
fn list(view: &DataView<'_>, head: &NodeId) -> Result<Vec<Obj>, ShapeError> {
    let nil = iri_to_node_id(RDF_NIL);
    let mut out = Vec::new();
    let mut cur = *head;
    let mut seen = HashSet::new();
    while cur != nil && seen.insert(cur) {
        let props = properties(view, &cur)?;
        if let Some(first) = props.get(RDF_FIRST).and_then(|v| v.first()) {
            out.push(first.clone());
        }
        match props.get(RDF_REST).and_then(|v| v.first()) {
            Some(Obj::Node(next)) => cur = *next,
            _ => break,
        }
    }
    Ok(out)
}
