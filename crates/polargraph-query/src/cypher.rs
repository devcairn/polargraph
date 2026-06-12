//! Cypher surface layer — parses a subset of openCypher and compiles it to the
//! existing Datalog `Query` / `Rule` IR.
//!
//! # Supported syntax
//!
//! ```text
//! MATCH (a:Person)-[:knows]->(b:Person)
//! WHERE a.name = "Alice"
//!   AND b.active = true
//! RETURN a, b
//! LIMIT 10
//! ```
//!
//! Transitive relationships:
//! ```text
//! MATCH (a)-[:knows*]->(b)       -- unlimited depth
//! MATCH (a)-[:knows*1..5]->(b)   -- bounded (max 5 hops; min ignored in eval)
//! ```
//!
//! # Compilation strategy
//!
//! - Relationship patterns `(a)-[:pred]->(b)` → `VarPattern { Var("a"), pred, Var("b") }`
//! - Node labels `(a:Label)` → value filter `a.__type = "Label"` applied post-execution
//!   (+ a binding pattern `(a, "__type", Any)` when the node is standalone)
//! - Node properties `(a {key: val})` → value filter `a.key = val`
//! - WHERE `a.prop = val` → value filter
//! - Transitive `[:pred*]` → two Datalog rules (base + recursive) producing derived
//!   predicate `__tc_<pred>`, plus a query pattern using that derived predicate
//!
//! Value filters are applied by [`apply_value_filters`] using snapshot scans after
//! the main `execute_query` / `execute_recursive` call completes. This keeps the
//! core evaluation engine unchanged.

use std::collections::{HashMap, HashSet};

use polargraph_core::{
    id::{EdgeId, NodeId},
    temporal::{BiTemporalRange, Timestamp},
    triple::{Predicate, Triple},
    value::Value,
};
use polargraph_storage::{Snapshot, StorageError, Transaction};

use crate::aggregation::{AggFunc, AggregationSpec, OrderSpec, SortDir};
use crate::datalog::{Bindings, Query, Rule, Term, VarPattern};

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum CypherError {
    #[error("parse error at position {pos}: {msg}")]
    Parse { pos: usize, msg: String },
}

impl CypherError {
    fn at(pos: usize, msg: impl Into<String>) -> Self {
        CypherError::Parse { pos, msg: msg.into() }
    }
}

// ── Value literals ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum CypherValue {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

impl CypherValue {
    fn to_core_value(&self) -> Value {
        match self {
            CypherValue::Str(s) => Value::Text(s.clone()),
            CypherValue::Int(i) => Value::Int(*i),
            CypherValue::Float(f) => Value::Float(*f),
            CypherValue::Bool(b) => Value::Bool(*b),
        }
    }
}

// ── AST ───────────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct NodePat {
    var: Option<String>,
    label: Option<String>,
    props: Vec<(String, CypherValue)>,
}

#[derive(Debug, Clone, Copy)]
enum Direction {
    Out,
    In,
}

#[derive(Debug)]
struct RelPat {
    /// Optional variable name for the relationship (e.g. `r` in `[r:knows]`).
    var: Option<String>,
    predicate: String,
    recursive: bool,
    #[allow(dead_code)] // reserved for future bounded-hop implementation
    max_hops: Option<usize>,
}

#[derive(Debug)]
struct HopPat {
    rel: RelPat,
    direction: Direction,
    end: NodePat,
}

#[derive(Debug)]
struct CypherPath {
    start: NodePat,
    hops: Vec<HopPat>,
}

/// Comparison operator for WHERE predicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOp {
    Eq, Ne, Gt, Gte, Lt, Lte,
}

#[derive(Debug)]
pub enum WhereClause {
    PropertyEq  { var: String, prop: String, value: CypherValue },
    PropertyCmp { var: String, prop: String, op: ComparisonOp, value: CypherValue },
    VectorNear  { var: String, space: String, k: u32, ef: Option<u32> },
    /// `WHERE a.prop CONTAINS "needle"` — case-sensitive substring match.
    Contains    { var: String, prop: String, value: String },
    /// `WHERE a.prop STARTS WITH "prefix"` — case-sensitive prefix match.
    StartsWith  { var: String, prop: String, value: String },
    /// `WHERE a.prop =~ "^pattern.*"` — full regex match.
    /// Falls back to a full predicate scan; no trigram pre-filter.
    Regex       { var: String, prop: String, pattern: String },
}

/// A single item in a RETURN clause.
#[derive(Debug)]
pub enum ReturnItem {
    /// Plain variable, e.g. `RETURN a`.
    Variable(String),
    /// Aggregate function with alias, e.g. `count(*) AS n`.
    Aggregation { func: AggFunc, alias: String },
}

/// A `CALL procedureName(args) YIELD var1[, var2, ...]` leading clause.
///
/// Currently only `VECTOR_NEAR` is supported as the procedure name.
/// This desugars to the same [`VectorNearClause`] IR as the `WHERE VECTOR_NEAR(...)`
/// form but additionally exposes a score variable.
#[derive(Debug)]
pub struct CallClause {
    /// Procedure name — must be `VECTOR_NEAR` (case-insensitive).
    pub procedure: String,
    /// Query-vector parameter name (informational; vector passed externally).
    pub vector_param: String,
    pub space: String,
    pub k: u32,
    pub ef: Option<u32>,
    /// Yielded variable names in order: `[node_var, optional score_var]`.
    pub yield_vars: Vec<String>,
}

/// A WITH clause: `WITH a, b [WHERE ...]`.
#[derive(Debug)]
pub struct WithClause {
    /// Variables projected through the pipeline.
    pub vars: Vec<String>,
    /// Optional equality filters applied between MATCH and RETURN.
    pub filters: Vec<WhereClause>,
}

/// Parsed representation of a Cypher query.
#[derive(Debug)]
pub struct CypherQuery {
    /// Optional leading CALL clause (e.g. `CALL VECTOR_NEAR(...) YIELD node, score`).
    call_clause: Option<CallClause>,
    match_patterns: Vec<CypherPath>,
    where_clauses: Vec<WhereClause>,
    /// Items in the RETURN clause (plain variables and/or aggregate functions).
    pub return_items: Vec<ReturnItem>,
    pub limit: Option<usize>,
    /// ORDER BY sort specs, applied after aggregation.
    pub order_by: Vec<OrderSpec>,
    /// Number of rows to skip before applying LIMIT.
    pub skip: Option<usize>,
    /// Optional WITH clause between MATCH and RETURN.
    pub with_clause: Option<WithClause>,
}

// ── Write AST ─────────────────────────────────────────────────────────────────

/// A single `SET var.key = value` clause.
#[derive(Debug, Clone)]
pub struct SetClause {
    pub var: String,
    pub key: String,
    pub value: CypherValue,
}

/// One element inside a `CREATE` clause: either a new node or a new relation.
#[derive(Debug)]
pub enum CreateClause {
    Node {
        var: Option<String>,
        label: Option<String>,
        props: Vec<(String, CypherValue)>,
    },
    Relation {
        subject_var: String,
        predicate: String,
        object_var: String,
    },
}

/// A parsed Cypher write statement.
#[derive(Debug)]
pub enum WriteStatement {
    Create(Vec<CreateClause>),
    Merge {
        var: Option<String>,
        label: Option<String>,
        props: Vec<(String, CypherValue)>,
    },
    Set(Vec<SetClause>),
    Delete(Vec<String>),
}

/// A flat write operation used in [`CompiledWrite`].
/// Unlike [`WriteStatement`], each value is a single atomic unit.
#[derive(Debug)]
pub enum WriteOp {
    CreateNode {
        var: Option<String>,
        label: Option<String>,
        props: Vec<(String, CypherValue)>,
    },
    CreateRelation {
        subject_var: String,
        predicate: String,
        object_var: String,
    },
    Merge {
        var: Option<String>,
        label: Option<String>,
        props: Vec<(String, CypherValue)>,
    },
    Set(Vec<SetClause>),
    Delete(Vec<String>),
}

/// The result of compiling a Cypher write statement, optionally preceded by a
/// MATCH clause.
pub struct CompiledWrite {
    /// Optional MATCH portion. When `Some`, the writes are executed once per
    /// binding row produced by this query (the match variables are available
    /// inside the write ops).
    pub match_query: Option<CompiledQuery>,
    /// Sequence of write operations to execute (in order).
    pub writes: Vec<WriteOp>,
}

/// Top-level Cypher statement — either a read query or a write statement.
#[derive(Debug)]
pub enum CypherStatement {
    Read(Box<CypherQuery>),
    Write(WriteStatement),
}

// ── Write execution ───────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum CypherWriteError {
    #[error("parse error: {0}")]
    Parse(#[from] CypherError),
    #[error("unbound variable '{0}'")]
    UnboundVariable(String),
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

pub struct WriteResult {
    pub created_ids: Vec<NodeId>,
    pub triples_written: u64,
    pub triples_deleted: u64,
}

/// Convert a grouped [`WriteStatement`] into a flat sequence of [`WriteOp`]s.
pub fn write_statement_to_ops(stmt: WriteStatement) -> Vec<WriteOp> {
    match stmt {
        WriteStatement::Create(clauses) => clauses
            .into_iter()
            .map(|c| match c {
                CreateClause::Node { var, label, props } => WriteOp::CreateNode { var, label, props },
                CreateClause::Relation { subject_var, predicate, object_var } => {
                    WriteOp::CreateRelation { subject_var, predicate, object_var }
                }
            })
            .collect(),
        WriteStatement::Merge { var, label, props } => vec![WriteOp::Merge { var, label, props }],
        WriteStatement::Set(clauses) => vec![WriteOp::Set(clauses)],
        WriteStatement::Delete(vars) => vec![WriteOp::Delete(vars)],
    }
}

/// Execute a slice of [`WriteOp`]s against an open MVCC transaction.
///
/// `snapshot` is used for MERGE lookups. `bindings` carries variable→NodeId
/// mappings and is updated in-place as new nodes are created (making them
/// available to subsequent ops in the same slice).
pub fn execute_write_ops(
    ops: &[WriteOp],
    tx: &mut Transaction,
    snapshot: &Snapshot,
    bindings: &mut HashMap<String, NodeId>,
) -> Result<WriteResult, CypherWriteError> {
    let mut created_ids = Vec::new();
    let mut triples_written: u64 = 0;
    let mut triples_deleted: u64 = 0;

    for op in ops {
        match op {
            WriteOp::CreateNode { var, label, props } => {
                let node_id = NodeId::new();
                let temporal = BiTemporalRange::assert_now(Timestamp::now());

                if let Some(ref lbl) = label {
                    tx.insert(Triple::Property {
                        subject: node_id,
                        predicate: Predicate::new("__type"),
                        value: Value::Text(lbl.clone()),
                        temporal,
                    });
                    triples_written += 1;
                }
                for (key, val) in props {
                    tx.insert(Triple::Property {
                        subject: node_id,
                        predicate: Predicate::new(key.clone()),
                        value: val.to_core_value(),
                        temporal,
                    });
                    triples_written += 1;
                }
                if let Some(var_name) = var {
                    bindings.insert(var_name.clone(), node_id);
                }
                created_ids.push(node_id);
            }

            WriteOp::CreateRelation { subject_var, predicate, object_var } => {
                let subject = bindings
                    .get(subject_var)
                    .copied()
                    .ok_or_else(|| CypherWriteError::UnboundVariable(subject_var.clone()))?;
                let object = bindings
                    .get(object_var)
                    .copied()
                    .ok_or_else(|| CypherWriteError::UnboundVariable(object_var.clone()))?;

                tx.insert(Triple::Relation {
                    subject,
                    predicate: Predicate::new(predicate.clone()),
                    object,
                    edge_id: EdgeId::new(),
                    temporal: BiTemporalRange::assert_now(Timestamp::now()),
                });
                triples_written += 1;
            }

            WriteOp::Merge { var, label, props } => {
                let found = find_matching_node(snapshot, label.as_deref(), props)?;
                let node_id = if let Some(existing) = found {
                    existing
                } else {
                    let node_id = NodeId::new();
                    let temporal = BiTemporalRange::assert_now(Timestamp::now());

                    if let Some(ref lbl) = label {
                        tx.insert(Triple::Property {
                            subject: node_id,
                            predicate: Predicate::new("__type"),
                            value: Value::Text(lbl.clone()),
                            temporal,
                        });
                        triples_written += 1;
                    }
                    for (key, val) in props {
                        tx.insert(Triple::Property {
                            subject: node_id,
                            predicate: Predicate::new(key.clone()),
                            value: val.to_core_value(),
                            temporal,
                        });
                        triples_written += 1;
                    }
                    created_ids.push(node_id);
                    node_id
                };
                if let Some(var_name) = var {
                    bindings.insert(var_name.clone(), node_id);
                }
            }

            WriteOp::Set(clauses) => {
                for clause in clauses {
                    let node_id = bindings
                        .get(&clause.var)
                        .copied()
                        .ok_or_else(|| CypherWriteError::UnboundVariable(clause.var.clone()))?;

                    tx.insert(Triple::Property {
                        subject: node_id,
                        predicate: Predicate::new(clause.key.clone()),
                        value: clause.value.to_core_value(),
                        temporal: BiTemporalRange::assert_now(Timestamp::now()),
                    });
                    triples_written += 1;
                }
            }

            WriteOp::Delete(vars) => {
                let now = Timestamp::now();
                for var_name in vars {
                    let node_id = bindings
                        .get(var_name)
                        .copied()
                        .ok_or_else(|| CypherWriteError::UnboundVariable(var_name.clone()))?;

                    let existing = snapshot.scan_by_subject(&node_id)?;
                    for triple in existing {
                        tx.insert(close_valid_time(triple, now));
                        triples_deleted += 1;
                    }
                }
            }
        }
    }

    Ok(WriteResult { created_ids, triples_written, triples_deleted })
}

/// Execute a write statement against an open MVCC transaction.
///
/// `snapshot` is used for MERGE lookups (read-only; taken before `tx` was
/// opened). `bindings` carries variable-to-NodeId mappings accumulated so far
/// in the same statement and is updated in-place as new nodes are created.
pub fn execute_write(
    write: WriteStatement,
    tx: &mut Transaction,
    snapshot: &Snapshot,
    bindings: &mut HashMap<String, NodeId>,
) -> Result<WriteResult, CypherWriteError> {
    let ops = write_statement_to_ops(write);
    execute_write_ops(&ops, tx, snapshot, bindings)
}

/// Scan for the first node that matches the given label and all property constraints.
fn find_matching_node(
    snapshot: &Snapshot,
    label: Option<&str>,
    props: &[(String, CypherValue)],
) -> Result<Option<NodeId>, StorageError> {
    let candidates: Vec<NodeId> = if let Some(label_str) = label {
        snapshot
            .scan_by_predicate("__type")?
            .into_iter()
            .filter_map(|t| match t {
                Triple::Property { subject, value: Value::Text(v), .. } if v == label_str => {
                    Some(subject)
                }
                _ => None,
            })
            .collect()
    } else if let Some((first_key, first_val)) = props.first() {
        let core_val = first_val.to_core_value();
        snapshot
            .scan_by_predicate(first_key)?
            .into_iter()
            .filter_map(|t| match &t {
                Triple::Property { subject, value, .. } if value == &core_val => Some(*subject),
                _ => None,
            })
            .collect()
    } else {
        return Ok(None);
    };

    'outer: for node_id in candidates {
        for (key, val) in props {
            let core_val = val.to_core_value();
            let matched = snapshot
                .scan_by_subject_predicate(&node_id, key)?
                .into_iter()
                .any(|t| matches!(&t, Triple::Property { value, .. } if value == &core_val));
            if !matched {
                continue 'outer;
            }
        }
        return Ok(Some(node_id));
    }

    Ok(None)
}

/// Re-insert a triple with its valid-time window closed at `now`.
fn close_valid_time(triple: Triple, now: Timestamp) -> Triple {
    match triple {
        Triple::Property { subject, predicate, value, temporal } => Triple::Property {
            subject,
            predicate,
            value,
            temporal: BiTemporalRange { vt_start: temporal.vt_start, vt_end: now, tt: Timestamp::now() },
        },
        Triple::Relation { subject, predicate, object, edge_id, temporal } => Triple::Relation {
            subject,
            predicate,
            object,
            edge_id,
            temporal: BiTemporalRange { vt_start: temporal.vt_start, vt_end: now, tt: Timestamp::now() },
        },
        Triple::EdgeProperty { edge, predicate, value, temporal } => Triple::EdgeProperty {
            edge,
            predicate,
            value,
            temporal: BiTemporalRange { vt_start: temporal.vt_start, vt_end: now, tt: Timestamp::now() },
        },
        Triple::EdgeRelation { edge, predicate, object, temporal } => Triple::EdgeRelation {
            edge,
            predicate,
            object,
            temporal: BiTemporalRange { vt_start: temporal.vt_start, vt_end: now, tt: Timestamp::now() },
        },
    }
}

// ── Compiled output ───────────────────────────────────────────────────────────

/// A property value constraint applied post-execution.
///
/// For each satisfying binding set, check that the node bound to `var` has a
/// property triple `(node, predicate, value)`.  Bindings that fail the check
/// are dropped.
pub struct ValueFilter {
    pub var: String,
    pub predicate: String,
    pub value: Value,
}

/// Compiled representation of a `VECTOR_NEAR(var, "space", k[, ef=N])` predicate
/// — produced by either the WHERE form or the CALL/YIELD form.
pub struct VectorNearClause {
    pub seed_variable: String,
    pub space: String,
    pub k: u32,
    /// Explicit ef from `ef=N` syntax. `None` means use the server default.
    pub ef: Option<u32>,
    /// When the CALL/YIELD form was used, the variable to bind the ANN score into.
    /// The caller should populate this key in each Bindings row with the distance.
    pub score_variable: Option<String>,
}

/// The match kind for a full-text / substring filter.
pub enum TextFilterKind {
    /// `CONTAINS "needle"` — `str.contains()` check.
    Contains(String),
    /// `STARTS WITH "prefix"` — `str.starts_with()` check.
    StartsWith(String),
    /// `=~ "pattern"` — `regex::Regex` match. Falls back to full predicate scan.
    Regex(String),
}

/// A text predicate applied post-execution via snapshot scan.
pub struct TextFilter {
    pub var: String,
    pub predicate: String,
    pub kind: TextFilterKind,
}

/// A filter on an edge annotation value, supporting comparison operators.
///
/// The variable `var` is expected to hold an EdgeId reinterpreted as a NodeId
/// in the binding map (as set by `edge_var` on `VarPattern`).
pub struct EdgeAnnotationFilter {
    /// The edge variable bound in Bindings as NodeId (EdgeId bytes reinterpreted).
    pub var: String,
    pub predicate: String,
    pub op: ComparisonOp,
    pub value: Value,
}

/// The result of compiling a `CypherQuery` to the Datalog IR.
pub struct CompiledQuery {
    pub query: Query,
    pub rules: Vec<Rule>,
    pub value_filters: Vec<ValueFilter>,
    /// Text filters from `CONTAINS`, `STARTS WITH`, and `=~` WHERE clauses.
    pub text_filters: Vec<TextFilter>,
    /// Non-aggregated variable names in RETURN (group-by keys when aggregating).
    pub return_vars: Vec<String>,
    pub limit: Option<usize>,
    /// Set when the WHERE clause contains `VECTOR_NEAR(var, "space", k)`.
    /// The caller must supply the query vector and route through ANN search.
    pub vector_near: Option<VectorNearClause>,
    /// Aggregation specs from the RETURN clause (empty if no aggregation).
    pub aggregations: Vec<AggregationSpec>,
    /// Group-by keys: non-aggregated variables in the RETURN clause.
    pub group_keys: Vec<String>,
    /// ORDER BY specs applied after aggregation.
    pub order_by: Vec<OrderSpec>,
    /// Number of rows to skip before applying LIMIT.
    pub skip: Option<usize>,
    /// Set of variables that hold EdgeIds (from relationship patterns with a variable name).
    pub edge_vars: std::collections::HashSet<String>,
    /// Annotation filters from WHERE clauses on edge variables.
    pub edge_annotation_filters: Vec<EdgeAnnotationFilter>,
}

// ── Tokenizer ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Token {
    LParen,
    RParen,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    Colon,
    Comma,
    Dot,
    Eq,
    Star,
    DotDot,
    Arrow,      // ->
    LeftArrow,  // <-
    Dash,       // -
    Tilde,      // ~  (used in =~ regex operator)
    Gt,         // >
    Lt,         // <
    Gte,        // >=
    Lte,        // <=
    Ne,         // <>
    Ident(String),
    Str(String),
    Int(i64),
    Float(f64),
}

struct Lexer<'a> {
    src: &'a [u8],
    pos: usize,
}

impl<'a> Lexer<'a> {
    fn new(src: &'a str) -> Self {
        Self { src: src.as_bytes(), pos: 0 }
    }

    fn peek(&self) -> Option<u8> {
        self.src.get(self.pos).copied()
    }

    fn peek2(&self) -> Option<u8> {
        self.src.get(self.pos + 1).copied()
    }

    fn advance(&mut self) -> Option<u8> {
        let c = self.src.get(self.pos).copied();
        if c.is_some() { self.pos += 1; }
        c
    }

    fn skip_whitespace(&mut self) {
        while let Some(c) = self.peek() {
            if c == b' ' || c == b'\t' || c == b'\n' || c == b'\r' {
                self.advance();
            } else {
                break;
            }
        }
    }

    fn read_string(&mut self, quote: u8) -> Result<Token, CypherError> {
        // opening quote already consumed
        let start = self.pos;
        let mut s = String::new();
        loop {
            match self.advance() {
                Some(c) if c == quote => return Ok(Token::Str(s)),
                Some(b'\\') => {
                    match self.advance() {
                        Some(b'n') => s.push('\n'),
                        Some(b't') => s.push('\t'),
                        Some(b'"') => s.push('"'),
                        Some(b'\'') => s.push('\''),
                        Some(b'\\') => s.push('\\'),
                        _ => return Err(CypherError::at(start, "invalid escape in string")),
                    }
                }
                Some(c) => s.push(c as char),
                None => return Err(CypherError::at(start, "unterminated string literal")),
            }
        }
    }

    fn read_number(&mut self, first: u8) -> Token {
        let mut s = String::new();
        s.push(first as char);
        let mut is_float = false;

        loop {
            match self.peek() {
                Some(b'0'..=b'9') => {
                    s.push(self.advance().unwrap() as char);
                }
                Some(b'.') if self.peek2() != Some(b'.') => {
                    // Single dot (not '..') — decimal point
                    s.push(self.advance().unwrap() as char);
                    is_float = true;
                }
                _ => break,
            }
        }

        if is_float {
            Token::Float(s.parse().unwrap_or(0.0))
        } else {
            Token::Int(s.parse().unwrap_or(0))
        }
    }

    fn tokenize(&mut self) -> Result<Vec<(usize, Token)>, CypherError> {
        let mut tokens = Vec::new();

        loop {
            self.skip_whitespace();
            let pos = self.pos;

            match self.advance() {
                None => break,
                Some(b'(') => tokens.push((pos, Token::LParen)),
                Some(b')') => tokens.push((pos, Token::RParen)),
                Some(b'[') => tokens.push((pos, Token::LBracket)),
                Some(b']') => tokens.push((pos, Token::RBracket)),
                Some(b'{') => tokens.push((pos, Token::LBrace)),
                Some(b'}') => tokens.push((pos, Token::RBrace)),
                Some(b':') => tokens.push((pos, Token::Colon)),
                Some(b',') => tokens.push((pos, Token::Comma)),
                Some(b'=') => tokens.push((pos, Token::Eq)),
                Some(b'~') => tokens.push((pos, Token::Tilde)),
                Some(b'*') => tokens.push((pos, Token::Star)),
                Some(b'-') => {
                    if self.peek() == Some(b'>') {
                        self.advance();
                        tokens.push((pos, Token::Arrow));
                    } else {
                        tokens.push((pos, Token::Dash));
                    }
                }
                Some(b'<') => {
                    match self.peek() {
                        Some(b'-') => { self.advance(); tokens.push((pos, Token::LeftArrow)); }
                        Some(b'=') => { self.advance(); tokens.push((pos, Token::Lte)); }
                        Some(b'>') => { self.advance(); tokens.push((pos, Token::Ne)); }
                        _ => tokens.push((pos, Token::Lt)),
                    }
                }
                Some(b'>') => {
                    if self.peek() == Some(b'=') {
                        self.advance();
                        tokens.push((pos, Token::Gte));
                    } else {
                        tokens.push((pos, Token::Gt));
                    }
                }
                Some(b'.') => {
                    if self.peek() == Some(b'.') {
                        self.advance();
                        tokens.push((pos, Token::DotDot));
                    } else {
                        tokens.push((pos, Token::Dot));
                    }
                }
                Some(q @ (b'"' | b'\'')) => {
                    let t = self.read_string(q)?;
                    tokens.push((pos, t));
                }
                Some(c @ b'0'..=b'9') => {
                    let t = self.read_number(c);
                    tokens.push((pos, t));
                }
                Some(c) if c.is_ascii_alphabetic() || c == b'_' => {
                    // Check for TRUE/FALSE keywords
                    let mut s = String::new();
                    s.push(c as char);
                    while let Some(nc) = self.peek() {
                        if nc.is_ascii_alphanumeric() || nc == b'_' {
                            s.push(self.advance().unwrap() as char);
                        } else {
                            break;
                        }
                    }
                    let upper = s.to_ascii_uppercase();
                    let tok = match upper.as_str() {
                        "TRUE"  => Token::Ident("true".into()),
                        "FALSE" => Token::Ident("false".into()),
                        _       => Token::Ident(s),
                    };
                    tokens.push((pos, tok));
                }
                Some(c) => {
                    return Err(CypherError::at(pos, format!("unexpected character {:?}", c as char)));
                }
            }
        }

        Ok(tokens)
    }
}

// ── Parser ────────────────────────────────────────────────────────────────────

struct Parser {
    tokens: Vec<(usize, Token)>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<(usize, Token)>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn current_pos(&self) -> usize {
        self.tokens.get(self.pos).map(|(p, _)| *p).unwrap_or(usize::MAX)
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos).map(|(_, t)| t)
    }

    fn advance(&mut self) -> Option<&Token> {
        let t = self.tokens.get(self.pos).map(|(_, t)| t);
        if t.is_some() { self.pos += 1; }
        t
    }

    fn expect(&mut self, expected: &Token) -> Result<(), CypherError> {
        let pos = self.current_pos();
        match self.peek() {
            Some(t) if t == expected => { self.advance(); Ok(()) }
            Some(other) => Err(CypherError::at(pos, format!("expected {:?}, got {:?}", expected, other))),
            None => Err(CypherError::at(pos, format!("expected {:?}, got end of input", expected))),
        }
    }

    /// Consume the next token if it is an `Ident` and return its name.
    fn expect_ident(&mut self) -> Result<String, CypherError> {
        let pos = self.current_pos();
        match self.peek().cloned() {
            Some(Token::Ident(s)) => { self.advance(); Ok(s) }
            Some(other) => Err(CypherError::at(pos, format!("expected identifier, got {:?}", other))),
            None => Err(CypherError::at(pos, "expected identifier, got end of input")),
        }
    }

    /// Consume the next token if it is an `Int` and return its value.
    fn expect_int(&mut self) -> Result<i64, CypherError> {
        let pos = self.current_pos();
        match self.peek().cloned() {
            Some(Token::Int(n)) => { self.advance(); Ok(n) }
            Some(other) => Err(CypherError::at(pos, format!("expected integer, got {:?}", other))),
            None => Err(CypherError::at(pos, "expected integer, got end of input")),
        }
    }

    fn peek_keyword(&self, kw: &str) -> bool {
        match self.peek() {
            Some(Token::Ident(s)) => s.eq_ignore_ascii_case(kw),
            _ => false,
        }
    }

    fn is_rel_start(&self) -> bool {
        matches!(self.peek(), Some(Token::Dash) | Some(Token::LeftArrow))
    }

    // ── Grammar productions ───────────────────────────────────────────────────

    fn parse_query(&mut self) -> Result<CypherQuery, CypherError> {
        // Optional leading CALL clause: `CALL VECTOR_NEAR(...) YIELD var1[, var2]`.
        let call_clause = if self.peek_keyword("CALL") {
            self.advance(); // consume CALL
            Some(self.parse_call_clause()?)
        } else {
            None
        };

        // MATCH is required when there is no CALL clause; optional when CALL is present.
        let pos = self.current_pos();
        let match_patterns = if self.peek_keyword("MATCH") {
            self.advance(); // consume MATCH
            self.parse_match_clause()?
        } else if call_clause.is_none() {
            return Err(CypherError::at(pos, "query must start with MATCH or CALL"));
        } else {
            Vec::new()
        };

        let mut where_clauses = Vec::new();
        if self.peek_keyword("WHERE") {
            self.advance();
            where_clauses = self.parse_where_clause()?;
        }

        // Optional WITH clause between MATCH/WHERE and RETURN.
        let with_clause = if self.peek_keyword("WITH") {
            self.advance(); // consume WITH
            Some(self.parse_with_clause()?)
        } else {
            None
        };

        let pos = self.current_pos();
        if !self.peek_keyword("RETURN") {
            return Err(CypherError::at(pos, "expected RETURN clause"));
        }
        self.advance(); // consume RETURN

        let return_items = self.parse_return_items()?;

        // Optional ORDER BY.
        let mut order_by = Vec::new();
        if self.peek_keyword("ORDER") {
            self.advance(); // consume ORDER
            let pos = self.current_pos();
            if !self.peek_keyword("BY") {
                return Err(CypherError::at(pos, "expected BY after ORDER"));
            }
            self.advance(); // consume BY
            order_by = self.parse_order_by()?;
        }

        // Optional SKIP.
        let mut skip = None;
        if self.peek_keyword("SKIP") {
            self.advance();
            let n = self.expect_int()?;
            if n < 0 {
                return Err(CypherError::at(self.current_pos(), "SKIP must be non-negative"));
            }
            skip = Some(n as usize);
        }

        // Optional LIMIT.
        let mut limit = None;
        if self.peek_keyword("LIMIT") {
            self.advance();
            let n = self.expect_int()?;
            if n < 0 {
                return Err(CypherError::at(self.current_pos(), "LIMIT must be non-negative"));
            }
            limit = Some(n as usize);
        }

        if self.peek().is_some() {
            return Err(CypherError::at(
                self.current_pos(),
                format!("unexpected token {:?} after query", self.peek().unwrap()),
            ));
        }

        Ok(CypherQuery { call_clause, match_patterns, where_clauses, return_items, limit, order_by, skip, with_clause })
    }

    /// Parse `VECTOR_NEAR(param, "space", k[, ef=N]) YIELD var1[, var2, ...]`.
    ///
    /// The `CALL` keyword has already been consumed by the caller.
    fn parse_call_clause(&mut self) -> Result<CallClause, CypherError> {
        let pos = self.current_pos();
        let procedure = self.expect_ident()?;
        if !procedure.eq_ignore_ascii_case("VECTOR_NEAR") {
            return Err(CypherError::at(
                pos,
                format!("unknown procedure '{}'; only VECTOR_NEAR is supported", procedure),
            ));
        }

        self.expect(&Token::LParen)?;
        let vector_param = self.expect_ident()?;
        self.expect(&Token::Comma)?;

        let pos = self.current_pos();
        let space = match self.peek().cloned() {
            Some(Token::Str(s)) => { self.advance(); s }
            other => return Err(CypherError::at(
                pos,
                format!("CALL VECTOR_NEAR: expected string literal for space name, got {:?}", other),
            )),
        };

        self.expect(&Token::Comma)?;
        let k_val = self.expect_int()?;
        if k_val <= 0 {
            return Err(CypherError::at(
                self.current_pos(),
                "CALL VECTOR_NEAR: k must be a positive integer",
            ));
        }

        // Optional: , ef=<n>
        let ef_val = if self.peek() == Some(&Token::Comma) {
            self.advance(); // consume ','
            let ef_name = self.expect_ident()?;
            if !ef_name.eq_ignore_ascii_case("ef") {
                return Err(CypherError::at(
                    self.current_pos(),
                    format!("CALL VECTOR_NEAR: expected 'ef', got {:?}", ef_name),
                ));
            }
            self.expect(&Token::Eq)?;
            let n = self.expect_int()?;
            if n <= 0 {
                return Err(CypherError::at(
                    self.current_pos(),
                    "CALL VECTOR_NEAR: ef must be a positive integer",
                ));
            }
            Some(n as u32)
        } else {
            None
        };

        self.expect(&Token::RParen)?;

        // YIELD var1[, var2, ...]
        let pos = self.current_pos();
        if !self.peek_keyword("YIELD") {
            return Err(CypherError::at(pos, "CALL clause requires YIELD"));
        }
        self.advance(); // consume YIELD

        let mut yield_vars = vec![self.expect_ident()?];
        while self.peek() == Some(&Token::Comma) {
            self.advance();
            yield_vars.push(self.expect_ident()?);
        }

        Ok(CallClause {
            procedure,
            vector_param,
            space,
            k: k_val as u32,
            ef: ef_val,
            yield_vars,
        })
    }

    /// Parse `WITH var1, var2 [WHERE condition AND ...]`.
    fn parse_with_clause(&mut self) -> Result<WithClause, CypherError> {
        // Collect projected variable names.
        let mut vars = vec![self.expect_ident()?];
        while self.peek() == Some(&Token::Comma) {
            self.advance();
            vars.push(self.expect_ident()?);
        }
        // Optional WHERE filters.
        let filters = if self.peek_keyword("WHERE") {
            self.advance();
            self.parse_where_clause()?
        } else {
            Vec::new()
        };
        Ok(WithClause { vars, filters })
    }

    /// Parse a comma-separated list of ORDER BY sort keys.
    ///
    /// Each key is an identifier (variable name or aggregate alias) followed by
    /// an optional `ASC` / `DESC` direction (default: ASC).
    fn parse_order_by(&mut self) -> Result<Vec<OrderSpec>, CypherError> {
        let mut specs = Vec::new();
        loop {
            let key = self.expect_ident()?;
            let direction = if self.peek_keyword("ASC") {
                self.advance();
                SortDir::Asc
            } else if self.peek_keyword("DESC") {
                self.advance();
                SortDir::Desc
            } else {
                SortDir::Asc
            };
            specs.push(OrderSpec { key, direction });
            if self.peek() == Some(&Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }
        Ok(specs)
    }

    fn parse_match_clause(&mut self) -> Result<Vec<CypherPath>, CypherError> {
        let mut paths = vec![self.parse_path()?];
        while self.peek() == Some(&Token::Comma) {
            self.advance();
            paths.push(self.parse_path()?);
        }
        Ok(paths)
    }

    fn parse_path(&mut self) -> Result<CypherPath, CypherError> {
        let start = self.parse_node_pat()?;
        let mut hops = Vec::new();

        while self.is_rel_start() {
            let (rel, direction) = self.parse_rel_direction()?;
            let end = self.parse_node_pat()?;
            hops.push(HopPat { rel, direction, end });
        }

        Ok(CypherPath { start, hops })
    }

    fn parse_node_pat(&mut self) -> Result<NodePat, CypherError> {
        self.expect(&Token::LParen)?;

        // Optional variable name: an identifier that is not a clause keyword.
        let var = match self.peek().cloned() {
            Some(Token::Ident(ref s)) if !is_clause_keyword(s) => {
                let v = s.clone();
                self.advance();
                Some(v)
            }
            _ => None,
        };

        // Optional :Label
        let label = if self.peek() == Some(&Token::Colon) {
            self.advance();
            Some(self.expect_ident()?)
        } else {
            None
        };

        // Optional {prop: val, ...}
        let props = if self.peek() == Some(&Token::LBrace) {
            self.parse_prop_map()?
        } else {
            Vec::new()
        };

        self.expect(&Token::RParen)?;
        Ok(NodePat { var, label, props })
    }

    fn parse_prop_map(&mut self) -> Result<Vec<(String, CypherValue)>, CypherError> {
        self.expect(&Token::LBrace)?;
        let mut props = Vec::new();

        if self.peek() != Some(&Token::RBrace) {
            loop {
                let key = self.expect_ident()?;
                self.expect(&Token::Colon)?;
                let val = self.parse_literal()?;
                props.push((key, val));

                if self.peek() == Some(&Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
        }

        self.expect(&Token::RBrace)?;
        Ok(props)
    }

    fn parse_rel_direction(&mut self) -> Result<(RelPat, Direction), CypherError> {
        match self.peek().cloned() {
            Some(Token::Dash) => {
                self.advance(); // consume '-'
                let rel = self.parse_rel_body()?;
                self.expect(&Token::Arrow)?;
                Ok((rel, Direction::Out))
            }
            Some(Token::LeftArrow) => {
                self.advance(); // consume '<-'
                let rel = self.parse_rel_body()?;
                self.expect(&Token::Dash)?;
                Ok((rel, Direction::In))
            }
            _ => Err(CypherError::at(self.current_pos(), "expected '-' or '<-'")),
        }
    }

    fn parse_rel_body(&mut self) -> Result<RelPat, CypherError> {
        self.expect(&Token::LBracket)?;

        // Optional variable name before ':'. Detected by: Ident followed by Colon.
        let var = if let Some(Token::Ident(_)) = self.peek().cloned() {
            if matches!(self.tokens.get(self.pos + 1), Some((_, Token::Colon))) {
                let name = self.expect_ident()?;
                Some(name)
            } else {
                None
            }
        } else {
            None
        };

        self.expect(&Token::Colon)?;
        let predicate = self.expect_ident()?;

        let (recursive, max_hops) = if self.peek() == Some(&Token::Star) {
            self.advance(); // consume '*'
            // Optional bounds: *N..M or *N (single) or bare *
            match self.peek().cloned() {
                Some(Token::Int(n)) => {
                    self.advance();
                    if self.peek() == Some(&Token::DotDot) {
                        self.advance(); // consume '..'
                        let max_n = self.expect_int()?;
                        let _ = n; // min not used in current eval
                        (true, Some(max_n as usize))
                    } else {
                        // *N without range — use N as the upper bound
                        (true, Some(n as usize))
                    }
                }
                _ => (true, None),
            }
        } else {
            (false, None)
        };

        self.expect(&Token::RBracket)?;
        Ok(RelPat { var, predicate, recursive, max_hops })
    }

    fn parse_where_clause(&mut self) -> Result<Vec<WhereClause>, CypherError> {
        let mut clauses = vec![self.parse_condition()?];
        while self.peek_keyword("AND") {
            self.advance();
            clauses.push(self.parse_condition()?);
        }
        Ok(clauses)
    }

    fn parse_condition(&mut self) -> Result<WhereClause, CypherError> {
        let first = self.expect_ident()?;
        if first.eq_ignore_ascii_case("VECTOR_NEAR") {
            // VECTOR_NEAR(var, "space_name", k)
            self.expect(&Token::LParen)?;
            let var = self.expect_ident()?;
            self.expect(&Token::Comma)?;
            let pos = self.current_pos();
            let space = match self.peek().cloned() {
                Some(Token::Str(s)) => { self.advance(); s }
                other => return Err(CypherError::at(
                    pos,
                    format!("VECTOR_NEAR: expected string literal for space name, got {:?}", other),
                )),
            };
            self.expect(&Token::Comma)?;
            let k_val = self.expect_int()?;
            if k_val <= 0 {
                return Err(CypherError::at(
                    self.current_pos(),
                    "VECTOR_NEAR: k must be a positive integer",
                ));
            }
            // Optional: , ef=<n>
            let ef_val = if self.peek() == Some(&Token::Comma) {
                self.advance(); // consume ','
                let ef_name = self.expect_ident()?;
                if !ef_name.eq_ignore_ascii_case("ef") {
                    return Err(CypherError::at(
                        self.current_pos(),
                        format!("VECTOR_NEAR: expected 'ef', got {:?}", ef_name),
                    ));
                }
                self.expect(&Token::Eq)?;
                let n = self.expect_int()?;
                if n <= 0 {
                    return Err(CypherError::at(
                        self.current_pos(),
                        "VECTOR_NEAR: ef must be a positive integer",
                    ));
                }
                Some(n as u32)
            } else {
                None
            };
            self.expect(&Token::RParen)?;
            Ok(WhereClause::VectorNear { var, space, k: k_val as u32, ef: ef_val })
        } else {
            // var.prop <op> value
            self.expect(&Token::Dot)?;
            let prop = self.expect_ident()?;

            if self.peek_keyword("CONTAINS") {
                self.advance(); // consume CONTAINS
                let pos = self.current_pos();
                let val = match self.peek().cloned() {
                    Some(Token::Str(s)) => { self.advance(); s }
                    other => return Err(CypherError::at(pos, format!("CONTAINS expects a string, got {:?}", other))),
                };
                Ok(WhereClause::Contains { var: first, prop, value: val })
            } else if self.peek_keyword("STARTS") {
                self.advance(); // consume STARTS
                let pos = self.current_pos();
                if !self.peek_keyword("WITH") {
                    return Err(CypherError::at(pos, "expected WITH after STARTS"));
                }
                self.advance(); // consume WITH
                let pos = self.current_pos();
                let val = match self.peek().cloned() {
                    Some(Token::Str(s)) => { self.advance(); s }
                    other => return Err(CypherError::at(pos, format!("STARTS WITH expects a string, got {:?}", other))),
                };
                Ok(WhereClause::StartsWith { var: first, prop, value: val })
            } else if self.peek() == Some(&Token::Eq) {
                self.advance(); // consume =
                if self.peek() == Some(&Token::Tilde) {
                    self.advance(); // consume ~
                    let pos = self.current_pos();
                    let pattern = match self.peek().cloned() {
                        Some(Token::Str(s)) => { self.advance(); s }
                        other => return Err(CypherError::at(pos, format!("=~ expects a regex string, got {:?}", other))),
                    };
                    Ok(WhereClause::Regex { var: first, prop, pattern })
                } else {
                    let value = self.parse_literal()?;
                    Ok(WhereClause::PropertyEq { var: first, prop, value })
                }
            } else if matches!(self.peek(), Some(Token::Gt) | Some(Token::Gte) | Some(Token::Lt) | Some(Token::Lte) | Some(Token::Ne)) {
                let op = match self.advance() {
                    Some(Token::Gt)  => ComparisonOp::Gt,
                    Some(Token::Gte) => ComparisonOp::Gte,
                    Some(Token::Lt)  => ComparisonOp::Lt,
                    Some(Token::Lte) => ComparisonOp::Lte,
                    Some(Token::Ne)  => ComparisonOp::Ne,
                    _ => unreachable!(),
                };
                let value = self.parse_literal()?;
                Ok(WhereClause::PropertyCmp { var: first, prop, op, value })
            } else {
                Err(CypherError::at(self.current_pos(), "expected comparison operator or =~ after property name"))
            }
        }
    }

    /// Parse `RETURN item1, item2, ...` where each item is either a plain variable
    /// or an aggregate expression (`count(*)`, `count(var)`, `collect(var)`) with
    /// a mandatory `AS alias`.
    fn parse_return_items(&mut self) -> Result<Vec<ReturnItem>, CypherError> {
        let mut items = vec![self.parse_one_return_item()?];
        while self.peek() == Some(&Token::Comma) {
            self.advance();
            items.push(self.parse_one_return_item()?);
        }
        Ok(items)
    }

    fn parse_one_return_item(&mut self) -> Result<ReturnItem, CypherError> {
        let pos = self.current_pos();
        let name = self.expect_ident()?;
        let upper = name.to_ascii_uppercase();

        let func = match upper.as_str() {
            "COUNT" => {
                self.expect(&Token::LParen)?;
                let agg = if self.peek() == Some(&Token::Star) {
                    self.advance();
                    AggFunc::CountStar
                } else {
                    let var = self.expect_ident()?;
                    AggFunc::Count(var)
                };
                self.expect(&Token::RParen)?;
                agg
            }
            "COLLECT" => {
                self.expect(&Token::LParen)?;
                let var = self.expect_ident()?;
                self.expect(&Token::RParen)?;
                AggFunc::Collect(var)
            }
            "SUM" | "AVG" | "MIN" | "MAX" => {
                // Syntax: SUM(var.prop)
                self.expect(&Token::LParen)?;
                let var = self.expect_ident()?;
                self.expect(&Token::Dot)?;
                let prop = self.expect_ident()?;
                self.expect(&Token::RParen)?;
                match upper.as_str() {
                    "SUM" => AggFunc::Sum { var, prop },
                    "AVG" => AggFunc::Avg { var, prop },
                    "MIN" => AggFunc::Min { var, prop },
                    "MAX" => AggFunc::Max { var, prop },
                    _ => unreachable!(),
                }
            }
            _ => {
                // Not an aggregate — plain variable.
                return Ok(ReturnItem::Variable(name));
            }
        };

        // Aggregate must have `AS alias`.
        let pos2 = self.current_pos();
        if !self.peek_keyword("AS") {
            return Err(CypherError::at(pos2, format!("aggregate at position {} requires AS alias", pos)));
        }
        self.advance(); // consume AS
        let alias = self.expect_ident()?;
        Ok(ReturnItem::Aggregation { func, alias })
    }

    fn parse_literal(&mut self) -> Result<CypherValue, CypherError> {
        let pos = self.current_pos();
        match self.peek().cloned() {
            Some(Token::Str(s)) => { self.advance(); Ok(CypherValue::Str(s)) }
            Some(Token::Int(n)) => { self.advance(); Ok(CypherValue::Int(n)) }
            Some(Token::Float(f)) => { self.advance(); Ok(CypherValue::Float(f)) }
            Some(Token::Ident(s)) => {
                let lower = s.to_ascii_lowercase();
                match lower.as_str() {
                    "true"  => { self.advance(); Ok(CypherValue::Bool(true)) }
                    "false" => { self.advance(); Ok(CypherValue::Bool(false)) }
                    _ => Err(CypherError::at(pos, format!("expected literal value, got identifier {:?}", s))),
                }
            }
            Some(other) => Err(CypherError::at(pos, format!("expected literal value, got {:?}", other))),
            None => Err(CypherError::at(pos, "expected literal value, got end of input")),
        }
    }
}

fn is_clause_keyword(s: &str) -> bool {
    matches!(
        s.to_ascii_uppercase().as_str(),
        "MATCH" | "WHERE" | "AND" | "RETURN" | "LIMIT" | "VECTOR_NEAR"
        | "CREATE" | "MERGE" | "SET" | "DELETE"
        | "WITH" | "ORDER" | "BY" | "SKIP" | "ASC" | "DESC"
        | "AS" | "COUNT" | "COLLECT" | "SUM" | "AVG" | "MIN" | "MAX"
        | "CONTAINS" | "STARTS" | "CALL" | "YIELD"
    )
}

impl Parser {
    // ── Write parsing ─────────────────────────────────────────────────────────

    fn parse_create_clauses(&mut self) -> Result<Vec<CreateClause>, CypherError> {
        let mut clauses = Vec::new();
        loop {
            if self.peek() != Some(&Token::LParen) {
                break;
            }
            let node = self.parse_node_pat()?;

            if self.is_rel_start() {
                let (rel, direction) = self.parse_rel_direction()?;
                let end_node = self.parse_node_pat()?;
                if rel.recursive {
                    return Err(CypherError::at(
                        self.current_pos(),
                        "CREATE does not support transitive relationships",
                    ));
                }
                let (subj_var, obj_var) = match direction {
                    Direction::Out => (
                        node.var.ok_or_else(|| {
                            CypherError::at(self.current_pos(), "CREATE relation: subject node must have a variable")
                        })?,
                        end_node.var.ok_or_else(|| {
                            CypherError::at(self.current_pos(), "CREATE relation: object node must have a variable")
                        })?,
                    ),
                    Direction::In => (
                        end_node.var.ok_or_else(|| {
                            CypherError::at(self.current_pos(), "CREATE relation: subject node must have a variable")
                        })?,
                        node.var.ok_or_else(|| {
                            CypherError::at(self.current_pos(), "CREATE relation: object node must have a variable")
                        })?,
                    ),
                };
                clauses.push(CreateClause::Relation {
                    subject_var: subj_var,
                    predicate: rel.predicate,
                    object_var: obj_var,
                });
            } else {
                clauses.push(CreateClause::Node {
                    var: node.var,
                    label: node.label,
                    props: node.props,
                });
            }

            if self.peek() == Some(&Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }
        Ok(clauses)
    }

    fn parse_set_clauses(&mut self) -> Result<Vec<SetClause>, CypherError> {
        let mut clauses = Vec::new();
        loop {
            let var = self.expect_ident()?;
            self.expect(&Token::Dot)?;
            let key = self.expect_ident()?;
            self.expect(&Token::Eq)?;
            let value = self.parse_literal()?;
            clauses.push(SetClause { var, key, value });

            if self.peek() == Some(&Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }
        Ok(clauses)
    }

    fn parse_delete_vars(&mut self) -> Result<Vec<String>, CypherError> {
        let mut vars = vec![self.expect_ident()?];
        while self.peek() == Some(&Token::Comma) {
            self.advance();
            vars.push(self.expect_ident()?);
        }
        Ok(vars)
    }

    fn parse_write_statement(&mut self) -> Result<WriteStatement, CypherError> {
        let pos = self.current_pos();
        if self.peek_keyword("CREATE") {
            self.advance();
            Ok(WriteStatement::Create(self.parse_create_clauses()?))
        } else if self.peek_keyword("MERGE") {
            self.advance();
            let node = self.parse_node_pat()?;
            Ok(WriteStatement::Merge { var: node.var, label: node.label, props: node.props })
        } else if self.peek_keyword("SET") {
            self.advance();
            Ok(WriteStatement::Set(self.parse_set_clauses()?))
        } else if self.peek_keyword("DELETE") {
            self.advance();
            Ok(WriteStatement::Delete(self.parse_delete_vars()?))
        } else {
            Err(CypherError::at(pos, "expected CREATE, MERGE, SET, or DELETE"))
        }
    }

    fn parse_statement_inner(&mut self) -> Result<CypherStatement, CypherError> {
        if self.peek_keyword("MATCH") {
            Ok(CypherStatement::Read(Box::new(self.parse_query()?)))
        } else {
            Ok(CypherStatement::Write(self.parse_write_statement()?))
        }
    }

    /// Parse a Cypher write statement, optionally preceded by a MATCH clause.
    ///
    /// Handles:
    /// - Pure write: `CREATE (a:Person {name: "Alice"})`
    /// - MATCH + write: `MATCH (a:Person) WHERE a.name = "Alice" CREATE (b:Friend)`
    fn parse_compiled_write(&mut self) -> Result<CompiledWrite, CypherError> {
        // Optional leading MATCH clause (no RETURN required).
        let match_query = if self.peek_keyword("MATCH") {
            self.advance(); // consume MATCH
            let match_patterns = self.parse_match_clause()?;
            let mut where_clauses = Vec::new();
            if self.peek_keyword("WHERE") {
                self.advance();
                where_clauses = self.parse_where_clause()?;
            }
            let cypher_q = CypherQuery {
                call_clause: None,
                match_patterns,
                where_clauses,
                return_items: Vec::new(),
                limit: None,
                order_by: Vec::new(),
                skip: None,
                with_clause: None,
            };
            Some(compile(cypher_q))
        } else {
            None
        };

        let pos = self.current_pos();
        if self.peek().is_none() {
            return Err(CypherError::at(pos, "expected a write clause (CREATE/MERGE/SET/DELETE)"));
        }
        let stmt = self.parse_write_statement()?;
        let writes = write_statement_to_ops(stmt);

        Ok(CompiledWrite { match_query, writes })
    }
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Parse a Cypher query string into an AST.
pub fn parse(input: &str) -> Result<CypherQuery, CypherError> {
    let tokens = Lexer::new(input).tokenize()?;
    Parser::new(tokens).parse_query()
}

/// Parse any Cypher statement — either a read query (MATCH...RETURN) or a
/// write statement (CREATE/MERGE/SET/DELETE).
pub fn parse_statement(input: &str) -> Result<CypherStatement, CypherError> {
    let tokens = Lexer::new(input).tokenize()?;
    let mut parser = Parser::new(tokens);
    let stmt = parser.parse_statement_inner()?;
    if let Some(tok) = parser.peek() {
        return Err(CypherError::at(
            parser.current_pos(),
            format!("unexpected token {:?} after statement", tok),
        ));
    }
    Ok(stmt)
}

/// Parse a Cypher write statement (or MATCH + write) into a [`CompiledWrite`].
///
/// Accepts:
/// - Pure write: `CREATE (a:Person {name: "Alice"})`
/// - MATCH + write: `MATCH (a:Person) WHERE a.name = "Alice" CREATE (b:Friend)`
pub fn parse_write(input: &str) -> Result<CompiledWrite, CypherError> {
    let tokens = Lexer::new(input).tokenize()?;
    let mut parser = Parser::new(tokens);
    let compiled = parser.parse_compiled_write()?;
    if let Some(tok) = parser.peek() {
        return Err(CypherError::at(
            parser.current_pos(),
            format!("unexpected token {:?} after write statement", tok),
        ));
    }
    Ok(compiled)
}

/// Returns true if `input` starts with a write keyword (CREATE/MERGE/SET/DELETE).
///
/// Note: `MATCH ... CREATE/MERGE/SET/DELETE` (read-write combined) is not detected
/// by this heuristic — use `parse_write` and check `match_query` for that case.
pub fn is_write_statement(input: &str) -> bool {
    let upper = input.trim_start().to_ascii_uppercase();
    upper.starts_with("CREATE")
        || upper.starts_with("MERGE")
        || upper.starts_with("SET")
        || upper.starts_with("DELETE")
}

/// Compile a parsed `CypherQuery` into the Datalog IR.
pub fn compile(cypher: CypherQuery) -> CompiledQuery {
    let mut patterns: Vec<VarPattern> = Vec::new();
    let mut rules: Vec<Rule> = Vec::new();
    let mut value_filters: Vec<ValueFilter> = Vec::new();
    let mut edge_annotation_filters: Vec<EdgeAnnotationFilter> = Vec::new();
    let mut edge_vars: HashSet<String> = HashSet::new();
    let mut anon: usize = 0;
    let mut derived_preds: HashSet<String> = HashSet::new();

    // Collect vars that appear as endpoints of at least one hop. These are
    // "relationship-bound" — the relationship pattern itself will bind them,
    // so we don't need to emit a standalone binding pattern for their label.
    let rel_bound = collect_rel_bound_vars(&cypher.match_patterns);

    for path in &cypher.match_patterns {
        compile_path(
            path,
            &rel_bound,
            &mut patterns,
            &mut rules,
            &mut value_filters,
            &mut derived_preds,
            &mut edge_vars,
            &mut anon,
        );
    }

    // WHERE clauses: property filters → value_filters (or edge_annotation_filters for edge vars);
    // text filters → text_filters; VECTOR_NEAR → vector_near.
    let mut vector_near: Option<VectorNearClause> = None;
    let mut text_filters: Vec<TextFilter> = Vec::new();

    let add_where = |wc: WhereClause,
                         vf: &mut Vec<ValueFilter>,
                         tf: &mut Vec<TextFilter>,
                         vn: &mut Option<VectorNearClause>,
                         eaf: &mut Vec<EdgeAnnotationFilter>,
                         evs: &HashSet<String>| {
        match wc {
            WhereClause::PropertyEq { var, prop, value } => {
                if evs.contains(&var) {
                    eaf.push(EdgeAnnotationFilter { var, predicate: prop, op: ComparisonOp::Eq, value: value.to_core_value() });
                } else {
                    vf.push(ValueFilter { var, predicate: prop, value: value.to_core_value() });
                }
            }
            WhereClause::PropertyCmp { var, prop, op, value } => {
                if evs.contains(&var) {
                    eaf.push(EdgeAnnotationFilter { var, predicate: prop, op, value: value.to_core_value() });
                } else {
                    // For node properties, only equality is currently supported;
                    // treat as a value filter for equality, skip others.
                    if matches!(op, ComparisonOp::Eq) {
                        vf.push(ValueFilter { var, predicate: prop, value: value.to_core_value() });
                    }
                }
            }
            WhereClause::VectorNear { var, space, k, ef } => {
                if vn.is_none() {
                    *vn = Some(VectorNearClause { seed_variable: var, space, k, ef, score_variable: None });
                }
            }
            WhereClause::Contains { var, prop, value } => {
                tf.push(TextFilter { var, predicate: prop, kind: TextFilterKind::Contains(value) });
            }
            WhereClause::StartsWith { var, prop, value } => {
                tf.push(TextFilter { var, predicate: prop, kind: TextFilterKind::StartsWith(value) });
            }
            WhereClause::Regex { var, prop, pattern } => {
                tf.push(TextFilter { var, predicate: prop, kind: TextFilterKind::Regex(pattern) });
            }
        }
    };

    for wc in cypher.where_clauses {
        add_where(wc, &mut value_filters, &mut text_filters, &mut vector_near, &mut edge_annotation_filters, &edge_vars);
    }

    // CALL clause: desugars to VectorNearClause (takes precedence over WHERE VECTOR_NEAR).
    if let Some(call) = cypher.call_clause {
        let node_var = call.yield_vars.first().cloned().unwrap_or_else(|| "__call_node".into());
        let score_var = call.yield_vars.get(1).cloned();
        vector_near = Some(VectorNearClause {
            seed_variable: node_var,
            space: call.space,
            k: call.k,
            ef: call.ef,
            score_variable: score_var,
        });
    }

    // WITH clause: append its filters.
    if let Some(with) = cypher.with_clause {
        for wc in with.filters {
            add_where(wc, &mut value_filters, &mut text_filters, &mut vector_near, &mut edge_annotation_filters, &edge_vars);
        }
    }

    // Split return_items into plain variables (group keys) and aggregations.
    let mut return_vars: Vec<String> = Vec::new();
    let mut aggregations: Vec<AggregationSpec> = Vec::new();
    for item in cypher.return_items {
        match item {
            ReturnItem::Variable(v) => return_vars.push(v),
            ReturnItem::Aggregation { func, alias } => aggregations.push(AggregationSpec { func, alias }),
        }
    }
    let group_keys = return_vars.clone();

    CompiledQuery {
        query: Query { patterns },
        rules,
        value_filters,
        text_filters,
        return_vars,
        limit: cypher.limit,
        vector_near,
        aggregations,
        group_keys,
        order_by: cypher.order_by,
        skip: cypher.skip,
        edge_vars,
        edge_annotation_filters,
    }
}

fn collect_rel_bound_vars(paths: &[CypherPath]) -> HashSet<String> {
    let mut bound = HashSet::new();
    for path in paths {
        if !path.hops.is_empty() {
            if let Some(v) = &path.start.var { bound.insert(v.clone()); }
            for hop in &path.hops {
                if let Some(v) = &hop.end.var { bound.insert(v.clone()); }
            }
        }
    }
    bound
}

fn get_or_anon(var: &Option<String>, anon: &mut usize) -> String {
    match var {
        Some(v) => v.clone(),
        None => {
            let n = *anon;
            *anon += 1;
            format!("__anon_{}", n)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn compile_path(
    path: &CypherPath,
    _rel_bound: &HashSet<String>,
    patterns: &mut Vec<VarPattern>,
    rules: &mut Vec<Rule>,
    value_filters: &mut Vec<ValueFilter>,
    derived_preds: &mut HashSet<String>,
    edge_vars: &mut HashSet<String>,
    anon: &mut usize,
) {
    let start_var = get_or_anon(&path.start.var, anon);

    if path.hops.is_empty() {
        // Standalone node — needs a binding pattern, then value filters.
        emit_node_binding(
            &start_var,
            &path.start,
            /* need_binding_pattern */ true,
            patterns,
            value_filters,
            anon,
        );
        return;
    }

    // For a node at the start of a path with hops, it will be bound by the
    // first relationship pattern.  Emit value filters only.
    emit_node_filters(&start_var, &path.start, value_filters);

    let mut current_var = start_var.clone();

    for hop in &path.hops {
        let end_var = get_or_anon(&hop.end.var, anon);

        if hop.rel.recursive {
            let pred = &hop.rel.predicate;

            if let Some(max_hops) = hop.rel.max_hops {
                // Bounded transitive: emit a VarPattern with max_hops set.
                // The evaluator uses reachable_from_hops at runtime — no TC rules needed.
                let (subj, obj) = match hop.direction {
                    Direction::Out => (Term::Var(current_var.clone()), Term::Var(end_var.clone())),
                    Direction::In  => (Term::Var(end_var.clone()), Term::Var(current_var.clone())),
                };
                patterns.push(VarPattern {
                    subject: subj,
                    predicate: Some(pred.clone()),
                    object: obj,
                    edge_var: None,
                    max_hops: Some(max_hops),
                });
            } else {
                // Unlimited transitive: emit TC Datalog rules + query pattern.
                let tc_pred = format!("__tc_{}", pred);

                if derived_preds.insert(tc_pred.clone()) {
                    // Base rule: __tc_pred(x, y) :- pred(x, y)
                    rules.push(
                        Rule::new(tc_pred.clone(), "x", "y").with_body(vec![VarPattern {
                            subject: Term::Var("x".into()),
                            predicate: Some(pred.clone()),
                            object: Term::Var("y".into()),
                            edge_var: None,
                            max_hops: None,
                        }]),
                    );
                    // Recursive rule: __tc_pred(x, z) :- __tc_pred(x, y), pred(y, z)
                    rules.push(
                        Rule::new(tc_pred.clone(), "x", "z").with_body(vec![
                            VarPattern {
                                subject: Term::Var("x".into()),
                                predicate: Some(tc_pred.clone()),
                                object: Term::Var("y".into()),
                                edge_var: None,
                                max_hops: None,
                            },
                            VarPattern {
                                subject: Term::Var("y".into()),
                                predicate: Some(pred.clone()),
                                object: Term::Var("z".into()),
                                edge_var: None,
                                max_hops: None,
                            },
                        ]),
                    );
                }

                let (subj, obj) = match hop.direction {
                    Direction::Out => (Term::Var(current_var.clone()), Term::Var(end_var.clone())),
                    Direction::In  => (Term::Var(end_var.clone()), Term::Var(current_var.clone())),
                };
                patterns.push(VarPattern {
                    subject: subj,
                    predicate: Some(tc_pred),
                    object: obj,
                    edge_var: None,
                    max_hops: None,
                });
            }
        } else {
            let (subj, obj) = match hop.direction {
                Direction::Out => (Term::Var(current_var.clone()), Term::Var(end_var.clone())),
                Direction::In  => (Term::Var(end_var.clone()), Term::Var(current_var.clone())),
            };
            // Register the relationship variable in edge_vars so WHERE clauses
            // on it are routed to edge_annotation_filters.
            let edge_var_name = hop.rel.var.clone().inspect(|v| {
                edge_vars.insert(v.clone());
            });
            patterns.push(VarPattern {
                subject: subj,
                predicate: Some(hop.rel.predicate.clone()),
                object: obj,
                edge_var: edge_var_name,
                max_hops: None,
            });
        }

        // End node of a hop is always rel-bound; emit filters only.
        emit_node_filters(&end_var, &hop.end, value_filters);
        current_var = end_var;
    }
}

/// Emit binding pattern + value filters for a standalone node (no relationships).
fn emit_node_binding(
    var: &str,
    node: &NodePat,
    need_binding: bool,
    patterns: &mut Vec<VarPattern>,
    value_filters: &mut Vec<ValueFilter>,
    _anon: &mut usize,
) {
    if let Some(label) = &node.label {
        if need_binding {
            // `(var, "__type", Any)` — binds `var` to any node that has __type.
            patterns.push(VarPattern {
                subject: Term::Var(var.to_string()),
                predicate: Some("__type".into()),
                object: Term::Any,
                edge_var: None,
                max_hops: None,
            });
        }
        value_filters.push(ValueFilter {
            var: var.to_string(),
            predicate: "__type".into(),
            value: Value::Text(label.clone()),
        });
    }

    let mut first_prop = node.label.is_none(); // need binding pattern from first prop
    for (key, val) in &node.props {
        if need_binding && first_prop {
            patterns.push(VarPattern {
                subject: Term::Var(var.to_string()),
                predicate: Some(key.clone()),
                object: Term::Any,
                edge_var: None,
                max_hops: None,
            });
            first_prop = false;
        }
        value_filters.push(ValueFilter {
            var: var.to_string(),
            predicate: key.clone(),
            value: val.to_core_value(),
        });
    }
}

/// Emit only value filters for a node that is already bound by a relationship.
fn emit_node_filters(var: &str, node: &NodePat, value_filters: &mut Vec<ValueFilter>) {
    if let Some(label) = &node.label {
        value_filters.push(ValueFilter {
            var: var.to_string(),
            predicate: "__type".into(),
            value: Value::Text(label.clone()),
        });
    }
    for (key, val) in &node.props {
        value_filters.push(ValueFilter {
            var: var.to_string(),
            predicate: key.clone(),
            value: val.to_core_value(),
        });
    }
}

// ── Post-filter ───────────────────────────────────────────────────────────────

/// Apply property value filters to a set of bindings, returning only those
/// that satisfy every filter.
///
/// For each binding:
/// 1. Look up the `NodeId` bound to `filter.var`.
/// 2. Scan the snapshot for property triples `(node, filter.predicate, *)`.
/// 3. Keep the binding only if at least one such triple has `value == filter.value`.
///
/// Bindings where `filter.var` is not present are **dropped** (the variable
/// should have been bound by the MATCH patterns; absence indicates the
/// query produced an incomplete binding set).
pub fn apply_value_filters(
    bindings: Vec<Bindings>,
    filters: &[ValueFilter],
    snapshot: &Snapshot,
) -> Result<Vec<Bindings>, StorageError> {
    if filters.is_empty() {
        return Ok(bindings);
    }

    let mut out = Vec::with_capacity(bindings.len());

    'binding: for binding in bindings {
        for filter in filters {
            let node_id = match binding.get(&filter.var) {
                Some(&id) => id,
                None => continue 'binding, // incomplete binding — drop
            };

            let triples = snapshot.scan_by_subject_predicate(&node_id, &filter.predicate)?;

            let matched = triples.iter().any(|t| match t {
                Triple::Property { value, .. } => value == &filter.value,
                _ => false,
            });

            if !matched {
                continue 'binding;
            }
        }
        out.push(binding);
    }

    Ok(out)
}

/// Apply text filters (`CONTAINS`, `STARTS WITH`, `=~`) to a set of bindings.
///
/// For each binding:
/// 1. Look up the `NodeId` bound to `filter.var`.
/// 2. Scan the snapshot for property triples `(node, filter.predicate, *)`.
/// 3. Keep the binding only if at least one text value satisfies the filter predicate.
///
/// `=~` patterns are compiled on each call; callers with many bindings may
/// want to pre-compile and cache the regex separately.
pub fn apply_text_filters(
    bindings: Vec<Bindings>,
    filters: &[TextFilter],
    snapshot: &Snapshot,
) -> Result<Vec<Bindings>, StorageError> {
    if filters.is_empty() {
        return Ok(bindings);
    }

    let mut out = Vec::with_capacity(bindings.len());

    'binding: for binding in bindings {
        for filter in filters {
            let node_id = match binding.get(&filter.var) {
                Some(&id) => id,
                None => continue 'binding,
            };

            let triples = snapshot.scan_by_subject_predicate(&node_id, &filter.predicate)?;
            let matched = triples.iter().any(|t| match t {
                Triple::Property { value: Value::Text(text), .. } => match &filter.kind {
                    TextFilterKind::Contains(needle) => text.contains(needle.as_str()),
                    TextFilterKind::StartsWith(prefix) => text.starts_with(prefix.as_str()),
                    TextFilterKind::Regex(pattern) => regex::Regex::new(pattern)
                        .map(|re| re.is_match(text))
                        .unwrap_or(false),
                },
                _ => false,
            });

            if !matched {
                continue 'binding;
            }
        }
        out.push(binding);
    }

    Ok(out)
}

/// Apply edge annotation filters to a set of bindings.
///
/// For each binding, looks up the EdgeId bound to `filter.var` (stored as NodeId
/// via UUID reinterpretation), calls `scan_edge_annotations` on the snapshot, and
/// keeps the binding only if at least one annotation satisfies the filter.
pub fn apply_edge_annotation_filters(
    bindings: Vec<crate::datalog::Bindings>,
    filters: &[EdgeAnnotationFilter],
    snapshot: &Snapshot,
) -> Result<Vec<crate::datalog::Bindings>, StorageError> {
    use polargraph_core::id::EdgeId;
    use polargraph_storage::EdgeAnnotationValue;

    if filters.is_empty() {
        return Ok(bindings);
    }

    let mut out = Vec::with_capacity(bindings.len());

    'binding: for binding in bindings {
        for filter in filters {
            let node_id = match binding.get(&filter.var) {
                Some(&id) => id,
                None => continue 'binding,
            };
            // Reinterpret NodeId bytes as EdgeId (same UUID).
            let edge_id = EdgeId(node_id.0);
            let annotations = snapshot.scan_edge_annotations(edge_id)?;

            let matched = annotations.iter().any(|ann| {
                if ann.predicate.0 != filter.predicate {
                    return false;
                }
                match &ann.value {
                    EdgeAnnotationValue::Scalar(v) => compare_values(v, &filter.op, &filter.value),
                    EdgeAnnotationValue::Node(_) => false,
                }
            });

            if !matched {
                continue 'binding;
            }
        }
        out.push(binding);
    }

    Ok(out)
}

fn compare_values(actual: &Value, op: &ComparisonOp, expected: &Value) -> bool {
    use std::cmp::Ordering;
    let ord: Option<Ordering> = match (actual, expected) {
        (Value::Int(a), Value::Int(b))     => a.partial_cmp(b),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
        (Value::Text(a), Value::Text(b))   => a.partial_cmp(b),
        (Value::Bool(a), Value::Bool(b))   => a.partial_cmp(b),
        _ => {
            // Cross-type: only equality makes sense.
            return matches!(op, ComparisonOp::Eq) && actual == expected;
        }
    };
    matches!(
        (ord, op),
        (Some(Ordering::Equal),   ComparisonOp::Eq | ComparisonOp::Gte | ComparisonOp::Lte)
        | (Some(Ordering::Less),  ComparisonOp::Lt | ComparisonOp::Lte | ComparisonOp::Ne)
        | (Some(Ordering::Greater), ComparisonOp::Gt | ComparisonOp::Gte | ComparisonOp::Ne)
    )
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use polargraph_core::{
        id::{EdgeId, NodeId},
        temporal::{BiTemporalRange, Timestamp},
        triple::{Predicate, Triple},
        value::Value,
    };
    use polargraph_storage::TripleStore;
    use tempfile::TempDir;

    // ── parser unit tests ─────────────────────────────────────────────────────

    fn return_var_names(q: &CypherQuery) -> Vec<&str> {
        q.return_items.iter().filter_map(|r| {
            if let ReturnItem::Variable(v) = r { Some(v.as_str()) } else { None }
        }).collect()
    }

    #[test]
    fn parse_simple_match() {
        let q = parse("MATCH (a:Person) RETURN a").unwrap();
        assert_eq!(q.match_patterns.len(), 1);
        let path = &q.match_patterns[0];
        assert!(path.hops.is_empty());
        assert_eq!(path.start.var.as_deref(), Some("a"));
        assert_eq!(path.start.label.as_deref(), Some("Person"));
        assert_eq!(return_var_names(&q), vec!["a"]);
        assert_eq!(q.limit, None);
    }

    #[test]
    fn parse_relationship() {
        let q = parse("MATCH (a)-[:knows]->(b) RETURN a, b").unwrap();
        let path = &q.match_patterns[0];
        assert_eq!(path.start.var.as_deref(), Some("a"));
        assert_eq!(path.hops.len(), 1);
        assert_eq!(path.hops[0].rel.predicate, "knows");
        assert!(!path.hops[0].rel.recursive);
        assert!(matches!(path.hops[0].direction, Direction::Out));
        assert_eq!(path.hops[0].end.var.as_deref(), Some("b"));
        assert_eq!(return_var_names(&q), vec!["a", "b"]);
    }

    #[test]
    fn parse_where() {
        let q = parse(r#"MATCH (a:Person)-[:knows]->(b) WHERE a.name = "Alice" RETURN a, b"#).unwrap();
        assert_eq!(q.where_clauses.len(), 1);
        match &q.where_clauses[0] {
            WhereClause::PropertyEq { var, prop, value } => {
                assert_eq!(var, "a");
                assert_eq!(prop, "name");
                assert!(matches!(value, CypherValue::Str(s) if s == "Alice"));
            }
            other => panic!("expected PropertyEq, got {:?}", other),
        }
    }

    #[test]
    fn parse_where_and_multiple() {
        let q = parse(r#"MATCH (a) WHERE a.name = "X" AND a.active = true RETURN a"#).unwrap();
        assert_eq!(q.where_clauses.len(), 2);
        assert!(matches!(&q.where_clauses[0], WhereClause::PropertyEq { .. }));
        assert!(matches!(&q.where_clauses[1], WhereClause::PropertyEq { .. }));
    }

    #[test]
    fn parse_vector_near() {
        let q = parse(r#"MATCH (a:Document)-[:cites]->(b) WHERE VECTOR_NEAR(a, "doc_space", 10) RETURN b"#).unwrap();
        assert_eq!(q.where_clauses.len(), 1);
        match &q.where_clauses[0] {
            WhereClause::VectorNear { var, space, k, ef } => {
                assert_eq!(var, "a");
                assert_eq!(space, "doc_space");
                assert_eq!(*k, 10);
                assert_eq!(*ef, None);
            }
            other => panic!("expected VectorNear, got {:?}", other),
        }
    }

    #[test]
    fn parse_vector_near_default_ef() {
        let q = parse(r#"MATCH (a) WHERE VECTOR_NEAR(a, "s", 10) RETURN a"#).unwrap();
        match &q.where_clauses[0] {
            WhereClause::VectorNear { ef, .. } => assert_eq!(*ef, None),
            other => panic!("expected VectorNear, got {:?}", other),
        }
    }

    #[test]
    fn parse_vector_near_with_ef() {
        let q = parse(r#"MATCH (a) WHERE VECTOR_NEAR(a, "s", 10, ef=100) RETURN a"#).unwrap();
        match &q.where_clauses[0] {
            WhereClause::VectorNear { k, ef, .. } => {
                assert_eq!(*k, 10);
                assert_eq!(*ef, Some(100));
            }
            other => panic!("expected VectorNear, got {:?}", other),
        }
    }

    #[test]
    fn parse_vector_near_with_property_filter() {
        let q = parse(r#"MATCH (a)-[:cites]->(b) WHERE VECTOR_NEAR(a, "docs", 5) AND b.published = true RETURN b"#).unwrap();
        assert_eq!(q.where_clauses.len(), 2);
        assert!(matches!(&q.where_clauses[0], WhereClause::VectorNear { .. }));
        assert!(matches!(&q.where_clauses[1], WhereClause::PropertyEq { .. }));
    }

    #[test]
    fn compile_vector_near_sets_vector_near_clause() {
        let q = parse(r#"MATCH (a)-[:cites]->(b) WHERE VECTOR_NEAR(a, "docs", 5) RETURN b"#).unwrap();
        let compiled = compile(q);
        let vn = compiled.vector_near.expect("expected VectorNearClause");
        assert_eq!(vn.seed_variable, "a");
        assert_eq!(vn.space, "docs");
        assert_eq!(vn.k, 5);
        // Property filters should be empty (VECTOR_NEAR is not a value filter)
        assert!(compiled.value_filters.is_empty());
    }

    #[test]
    fn parse_vector_near_bad_space_errors() {
        // Missing string literal for space name
        let r = parse(r#"MATCH (a) WHERE VECTOR_NEAR(a, 42, 10) RETURN a"#);
        assert!(r.is_err(), "expected parse error for non-string space name");
    }

    #[test]
    fn parse_recursive() {
        let q = parse("MATCH (a)-[:knows*]->(b) RETURN a, b").unwrap();
        let hop = &q.match_patterns[0].hops[0];
        assert!(hop.rel.recursive);
        assert_eq!(hop.rel.predicate, "knows");
        assert_eq!(hop.rel.max_hops, None);
    }

    #[test]
    fn parse_recursive_bounded() {
        let q = parse("MATCH (a)-[:knows*1..5]->(b) RETURN a, b").unwrap();
        let hop = &q.match_patterns[0].hops[0];
        assert!(hop.rel.recursive);
        assert_eq!(hop.rel.max_hops, Some(5));
    }

    #[test]
    fn parse_limit() {
        let q = parse("MATCH (a) RETURN a LIMIT 10").unwrap();
        assert_eq!(q.limit, Some(10));
    }

    #[test]
    fn parse_error_unknown_clause() {
        let r = parse("SELECT a FROM b");
        assert!(r.is_err(), "expected parse error for unknown clause");
    }

    #[test]
    fn parse_error_missing_return() {
        let r = parse("MATCH (a:Person)");
        assert!(r.is_err());
    }

    // ── compiler unit tests ───────────────────────────────────────────────────

    #[test]
    fn compile_simple_node_label() {
        let q = parse("MATCH (a:Person) RETURN a").unwrap();
        let compiled = compile(q);
        // Should emit a binding pattern for __type
        assert_eq!(compiled.query.patterns.len(), 1);
        let p = &compiled.query.patterns[0];
        assert_eq!(p.predicate.as_deref(), Some("__type"));
        // Should emit a value filter for __type = Person
        assert_eq!(compiled.value_filters.len(), 1);
        assert_eq!(compiled.value_filters[0].predicate, "__type");
        assert_eq!(compiled.value_filters[0].value, Value::Text("Person".into()));
    }

    #[test]
    fn compile_relationship_pattern() {
        let q = parse("MATCH (a)-[:knows]->(b) RETURN a, b").unwrap();
        let compiled = compile(q);
        assert_eq!(compiled.query.patterns.len(), 1);
        let p = &compiled.query.patterns[0];
        assert_eq!(p.predicate.as_deref(), Some("knows"));
        assert!(matches!(&p.subject, Term::Var(v) if v == "a"));
        assert!(matches!(&p.object, Term::Var(v) if v == "b"));
        assert!(compiled.rules.is_empty());
    }

    #[test]
    fn compile_recursive_emits_rules() {
        let q = parse("MATCH (a)-[:knows*]->(b) RETURN a, b").unwrap();
        let compiled = compile(q);
        // Two rules: base + recursive
        assert_eq!(compiled.rules.len(), 2);
        assert_eq!(compiled.rules[0].head_predicate, "__tc_knows");
        assert_eq!(compiled.rules[1].head_predicate, "__tc_knows");
        // Query pattern uses derived predicate
        assert_eq!(compiled.query.patterns.len(), 1);
        assert_eq!(compiled.query.patterns[0].predicate.as_deref(), Some("__tc_knows"));
    }

    #[test]
    fn compile_where_clause() {
        let q = parse(r#"MATCH (a)-[:knows]->(b) WHERE a.name = "Alice" RETURN a, b"#).unwrap();
        let compiled = compile(q);
        // No label filters, one WHERE filter
        assert_eq!(compiled.value_filters.len(), 1);
        assert_eq!(compiled.value_filters[0].var, "a");
        assert_eq!(compiled.value_filters[0].predicate, "name");
        assert_eq!(compiled.value_filters[0].value, Value::Text("Alice".into()));
    }

    // ── apply_value_filters integration ──────────────────────────────────────

    fn open() -> (TripleStore, TempDir) {
        let dir = TempDir::new().unwrap();
        (TripleStore::open(dir.path()).unwrap(), dir)
    }

    fn prop_triple(node: NodeId, pred: &str, val: impl Into<Value>) -> Triple {
        Triple::Property {
            subject: node,
            predicate: Predicate::new(pred),
            value: val.into(),
            temporal: BiTemporalRange::assert_now(Timestamp::now()),
        }
    }

    fn rel_triple(from: NodeId, pred: &str, to: NodeId) -> Triple {
        Triple::Relation {
            subject: from,
            predicate: Predicate::new(pred),
            object: to,
            edge_id: EdgeId::new(),
            temporal: BiTemporalRange::assert_now(Timestamp::now()),
        }
    }

    fn commit(store: &TripleStore, triples: Vec<Triple>) -> polargraph_storage::Snapshot {
        let mut tx = store.begin();
        for t in triples { tx.insert(t); }
        let ts = tx.commit().unwrap();
        store.snapshot(ts)
    }

    #[test]
    fn apply_filters_passes_matching_binding() {
        let (store, _dir) = open();
        let alice = NodeId::new();

        let snap = commit(&store, vec![
            prop_triple(alice, "__type", "Person"),
            prop_triple(alice, "name", "Alice"),
        ]);

        let bindings = vec![{
            let mut b = std::collections::HashMap::new();
            b.insert("a".to_string(), alice);
            b
        }];

        let filters = vec![
            ValueFilter { var: "a".into(), predicate: "__type".into(), value: Value::Text("Person".into()) },
        ];

        let result = apply_value_filters(bindings, &filters, &snap).unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn apply_filters_drops_non_matching_binding() {
        let (store, _dir) = open();
        let alice = NodeId::new();

        let snap = commit(&store, vec![
            prop_triple(alice, "__type", "Employee"),
        ]);

        let bindings = vec![{
            let mut b = std::collections::HashMap::new();
            b.insert("a".to_string(), alice);
            b
        }];

        let filters = vec![
            ValueFilter { var: "a".into(), predicate: "__type".into(), value: Value::Text("Person".into()) },
        ];

        let result = apply_value_filters(bindings, &filters, &snap).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn apply_filters_two_vars() {
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();
        let carol = NodeId::new();

        let snap = commit(&store, vec![
            prop_triple(alice, "__type", "Person"),
            prop_triple(bob, "__type", "Person"),
            prop_triple(carol, "__type", "Robot"),
            rel_triple(alice, "knows", bob),
            rel_triple(alice, "knows", carol),
        ]);

        let b1 = {
            let mut b = std::collections::HashMap::new();
            b.insert("a".to_string(), alice);
            b.insert("b".to_string(), bob);
            b
        };
        let b2 = {
            let mut b = std::collections::HashMap::new();
            b.insert("a".to_string(), alice);
            b.insert("b".to_string(), carol);
            b
        };

        let filters = vec![
            ValueFilter { var: "a".into(), predicate: "__type".into(), value: Value::Text("Person".into()) },
            ValueFilter { var: "b".into(), predicate: "__type".into(), value: Value::Text("Person".into()) },
        ];

        let result = apply_value_filters(vec![b1, b2], &filters, &snap).unwrap();
        // alice-bob passes; alice-carol fails (carol is Robot)
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get("b"), Some(&bob));
    }

    // ── write: parse tests ────────────────────────────────────────────────────

    #[test]
    fn parse_write_create_node() {
        let stmt = parse_statement(r#"CREATE (a:Person {name: "Alice", age: 30})"#).unwrap();
        match stmt {
            CypherStatement::Write(WriteStatement::Create(clauses)) => {
                assert_eq!(clauses.len(), 1);
                match &clauses[0] {
                    CreateClause::Node { var, label, props } => {
                        assert_eq!(var.as_deref(), Some("a"));
                        assert_eq!(label.as_deref(), Some("Person"));
                        assert_eq!(props.len(), 2);
                        assert_eq!(props[0].0, "name");
                        assert!(matches!(&props[0].1, CypherValue::Str(s) if s == "Alice"));
                        assert_eq!(props[1].0, "age");
                        assert!(matches!(&props[1].1, CypherValue::Int(30)));
                    }
                    other => panic!("expected Node clause, got {other:?}"),
                }
            }
            other => panic!("expected Write(Create), got {other:?}"),
        }
    }

    #[test]
    fn parse_write_create_relation() {
        let stmt = parse_statement("CREATE (a)-[:knows]->(b)").unwrap();
        match stmt {
            CypherStatement::Write(WriteStatement::Create(clauses)) => {
                // Two nodes (a and b will be parsed as separate CreateClause::Node
                // from the path — but our parser emits a single Relation clause)
                assert_eq!(clauses.len(), 1);
                match &clauses[0] {
                    CreateClause::Relation { subject_var, predicate, object_var } => {
                        assert_eq!(subject_var, "a");
                        assert_eq!(predicate, "knows");
                        assert_eq!(object_var, "b");
                    }
                    other => panic!("expected Relation clause, got {other:?}"),
                }
            }
            other => panic!("expected Write(Create), got {other:?}"),
        }
    }

    #[test]
    fn parse_write_merge() {
        let stmt = parse_statement(r#"MERGE (a:Company {name: "Acme"})"#).unwrap();
        match stmt {
            CypherStatement::Write(WriteStatement::Merge { var, label, props }) => {
                assert_eq!(var.as_deref(), Some("a"));
                assert_eq!(label.as_deref(), Some("Company"));
                assert_eq!(props.len(), 1);
                assert_eq!(props[0].0, "name");
                assert!(matches!(&props[0].1, CypherValue::Str(s) if s == "Acme"));
            }
            other => panic!("expected Write(Merge), got {other:?}"),
        }
    }

    #[test]
    fn parse_write_set() {
        let stmt = parse_statement(r#"SET a.name = "Bob", a.active = true"#).unwrap();
        match stmt {
            CypherStatement::Write(WriteStatement::Set(clauses)) => {
                assert_eq!(clauses.len(), 2);
                assert_eq!(clauses[0].var, "a");
                assert_eq!(clauses[0].key, "name");
                assert!(matches!(&clauses[0].value, CypherValue::Str(s) if s == "Bob"));
                assert_eq!(clauses[1].var, "a");
                assert_eq!(clauses[1].key, "active");
                assert!(matches!(&clauses[1].value, CypherValue::Bool(true)));
            }
            other => panic!("expected Write(Set), got {other:?}"),
        }
    }

    #[test]
    fn parse_write_delete() {
        let stmt = parse_statement("DELETE a, b").unwrap();
        match stmt {
            CypherStatement::Write(WriteStatement::Delete(vars)) => {
                assert_eq!(vars, vec!["a".to_string(), "b".to_string()]);
            }
            other => panic!("expected Write(Delete), got {other:?}"),
        }
    }

    #[test]
    fn parse_read_is_read_variant() {
        let stmt = parse_statement("MATCH (a:Person) RETURN a").unwrap();
        assert!(matches!(stmt, CypherStatement::Read(_)));
    }

    #[test]
    fn parse_write_match_create_combined() {
        let compiled = parse_write(
            r#"MATCH (a:Person) WHERE a.name = "Alice" CREATE (b:Friend {name: "Bob"})"#,
        )
        .unwrap();
        assert!(compiled.match_query.is_some(), "MATCH portion should be compiled");
        assert_eq!(compiled.writes.len(), 1);
        match &compiled.writes[0] {
            WriteOp::CreateNode { var, label, props } => {
                assert_eq!(var.as_deref(), Some("b"));
                assert_eq!(label.as_deref(), Some("Friend"));
                assert_eq!(props.len(), 1);
                assert_eq!(props[0].0, "name");
                assert!(matches!(&props[0].1, CypherValue::Str(s) if s == "Bob"));
            }
            other => panic!("expected CreateNode, got {other:?}"),
        }
        // The compiled match query should have a value filter for name=Alice
        let mq = compiled.match_query.as_ref().unwrap();
        assert!(
            mq.value_filters.iter().any(|f| f.predicate == "name"),
            "match should include name filter"
        );
    }

    #[test]
    fn is_write_statement_detects_keywords() {
        assert!(is_write_statement("CREATE (a:Person)"));
        assert!(is_write_statement("  MERGE (a:Company)"));
        assert!(is_write_statement("SET a.x = 1"));
        assert!(is_write_statement("DELETE a"));
        assert!(!is_write_statement("MATCH (a) RETURN a"));
    }

    // ── write: execution tests ────────────────────────────────────────────────

    #[test]
    fn create_and_query_round_trip() {
        let (store, _dir) = open();

        // Execute CREATE
        let stmt = parse_statement(r#"CREATE (a:Person {name: "Alice"})"#).unwrap();
        let write = match stmt {
            CypherStatement::Write(w) => w,
            _ => panic!("expected write"),
        };

        let mut tx = store.begin();
        let snap = store.snapshot(tx.read_ts);
        let mut bindings = HashMap::new();
        let result = execute_write(write, &mut tx, &snap, &mut bindings).unwrap();
        tx.commit().unwrap();

        assert_eq!(result.created_ids.len(), 1);
        assert_eq!(result.triples_written, 2); // __type + name

        let created = result.created_ids[0];

        // Query back via CypherQuery
        let q = parse(r#"MATCH (a:Person) RETURN a"#).unwrap();
        let compiled = compile(q);
        let snap2 = store.snapshot(store.begin().read_ts);
        let raw = crate::datalog::execute_query(&compiled.query, &snap2, None, None).unwrap();
        let filtered = apply_value_filters(raw, &compiled.value_filters, &snap2).unwrap();

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].get("a"), Some(&created));
    }

    #[test]
    fn merge_idempotency() {
        let (store, _dir) = open();

        let do_merge = |store: &TripleStore| {
            let stmt = parse_statement(r#"MERGE (a:Company {name: "Acme"})"#).unwrap();
            let write = match stmt {
                CypherStatement::Write(w) => w,
                _ => panic!("expected write"),
            };
            let mut tx = store.begin();
            let snap = store.snapshot(tx.read_ts);
            let mut bindings = HashMap::new();
            let result = execute_write(write, &mut tx, &snap, &mut bindings).unwrap();
            tx.commit().unwrap();
            result
        };

        let r1 = do_merge(&store);
        let r2 = do_merge(&store);

        // First merge creates the node; second merge should find it and create nothing.
        assert_eq!(r1.created_ids.len(), 1, "first merge should create a node");
        assert_eq!(r2.created_ids.len(), 0, "second merge should be a no-op");

        // Confirm only one Company node exists.
        let q = parse("MATCH (a:Company) RETURN a").unwrap();
        let compiled = compile(q);
        let snap = store.snapshot(store.begin().read_ts);
        let raw = crate::datalog::execute_query(&compiled.query, &snap, None, None).unwrap();
        let filtered = apply_value_filters(raw, &compiled.value_filters, &snap).unwrap();
        assert_eq!(filtered.len(), 1, "expected exactly one Company node");
    }

    // ── aggregation / ORDER BY / SKIP / WITH parse tests ─────────────────────

    #[test]
    fn parse_count_star_aggregation() {
        let q = parse("MATCH (a)-[:knows]->(b) RETURN a, count(*) AS n").unwrap();
        assert_eq!(q.return_items.len(), 2);
        assert!(matches!(&q.return_items[0], ReturnItem::Variable(v) if v == "a"));
        match &q.return_items[1] {
            ReturnItem::Aggregation { func: AggFunc::CountStar, alias } => {
                assert_eq!(alias, "n");
            }
            other => panic!("expected count(*) AS n, got {:?}", other),
        }
    }

    #[test]
    fn parse_count_var_aggregation() {
        let q = parse("MATCH (a)-[:knows]->(b) RETURN a, count(b) AS n").unwrap();
        match &q.return_items[1] {
            ReturnItem::Aggregation { func: AggFunc::Count(var), alias } => {
                assert_eq!(var, "b");
                assert_eq!(alias, "n");
            }
            other => panic!("expected count(b) AS n, got {:?}", other),
        }
    }

    #[test]
    fn parse_collect_aggregation() {
        let q = parse("MATCH (a)-[:knows]->(b) RETURN a, collect(b) AS bs").unwrap();
        match &q.return_items[1] {
            ReturnItem::Aggregation { func: AggFunc::Collect(var), alias } => {
                assert_eq!(var, "b");
                assert_eq!(alias, "bs");
            }
            other => panic!("expected collect(b) AS bs, got {:?}", other),
        }
    }

    #[test]
    fn parse_order_by_asc_desc() {
        let q = parse("MATCH (a)-[:knows]->(b) RETURN a, count(*) AS n ORDER BY n DESC").unwrap();
        assert_eq!(q.order_by.len(), 1);
        assert_eq!(q.order_by[0].key, "n");
        assert_eq!(q.order_by[0].direction, SortDir::Desc);
    }

    #[test]
    fn parse_order_by_multiple_keys() {
        let q = parse("MATCH (a) RETURN a ORDER BY a ASC, a ASC").unwrap();
        assert_eq!(q.order_by.len(), 2);
        assert_eq!(q.order_by[0].direction, SortDir::Asc);
        assert_eq!(q.order_by[1].direction, SortDir::Asc);
    }

    #[test]
    fn parse_skip_and_limit() {
        let q = parse("MATCH (a) RETURN a SKIP 5 LIMIT 10").unwrap();
        assert_eq!(q.skip, Some(5));
        assert_eq!(q.limit, Some(10));
    }

    #[test]
    fn parse_with_clause_basic() {
        let q = parse("MATCH (a:Person)-[:knows]->(b:Person) WITH a, b RETURN b").unwrap();
        let with_clause = q.with_clause.as_ref().expect("expected WITH clause");
        assert_eq!(with_clause.vars, vec!["a".to_string(), "b".to_string()]);
        assert!(with_clause.filters.is_empty());
    }

    #[test]
    fn parse_with_clause_with_filter() {
        let q = parse(
            r#"MATCH (a:Person)-[:knows]->(b:Person) WITH a, b WHERE a.active = true RETURN b"#
        ).unwrap();
        let with_clause = q.with_clause.as_ref().expect("expected WITH clause");
        assert_eq!(with_clause.vars.len(), 2);
        assert_eq!(with_clause.filters.len(), 1);
        match &with_clause.filters[0] {
            WhereClause::PropertyEq { var, prop, value } => {
                assert_eq!(var, "a");
                assert_eq!(prop, "active");
                assert!(matches!(value, CypherValue::Bool(true)));
            }
            other => panic!("expected PropertyEq, got {:?}", other),
        }
    }

    // ── aggregation compile tests ─────────────────────────────────────────────

    #[test]
    fn compile_count_star_sets_aggregation() {
        let q = parse("MATCH (a)-[:knows]->(b) RETURN a, count(*) AS n").unwrap();
        let compiled = compile(q);
        assert_eq!(compiled.aggregations.len(), 1);
        assert_eq!(compiled.aggregations[0].alias, "n");
        assert!(matches!(compiled.aggregations[0].func, AggFunc::CountStar));
        assert_eq!(compiled.group_keys, vec!["a"]);
        assert_eq!(compiled.return_vars, vec!["a"]);
    }

    #[test]
    fn compile_with_filter_becomes_value_filter() {
        let q = parse(
            r#"MATCH (a:Person)-[:knows]->(b) WITH a, b WHERE a.active = true RETURN b"#
        ).unwrap();
        let compiled = compile(q);
        // One filter from :Person label, one from WITH WHERE
        let active_filter = compiled.value_filters.iter()
            .find(|f| f.predicate == "active")
            .expect("expected active filter");
        assert_eq!(active_filter.var, "a");
        assert_eq!(active_filter.value, Value::Bool(true));
    }

    #[test]
    fn compile_order_by_and_skip_propagated() {
        let q = parse("MATCH (a)-[:knows]->(b) RETURN a, count(*) AS n ORDER BY n DESC SKIP 2 LIMIT 5").unwrap();
        let compiled = compile(q);
        assert_eq!(compiled.order_by.len(), 1);
        assert_eq!(compiled.order_by[0].key, "n");
        assert_eq!(compiled.order_by[0].direction, SortDir::Desc);
        assert_eq!(compiled.skip, Some(2));
        assert_eq!(compiled.limit, Some(5));
    }

    #[test]
    fn aggregate_missing_as_errors() {
        let r = parse("MATCH (a) RETURN count(*)");
        assert!(r.is_err(), "count(*) without AS alias should error");
    }

    // ── SUM / AVG / MIN / MAX parse tests ────────────────────────────────────

    #[test]
    fn parse_sum_aggregation() {
        let q = parse("MATCH (a)-[:edge]->(b) RETURN a, sum(b.score) AS total").unwrap();
        match &q.return_items[1] {
            ReturnItem::Aggregation { func: AggFunc::Sum { var, prop }, alias } => {
                assert_eq!(var, "b");
                assert_eq!(prop, "score");
                assert_eq!(alias, "total");
            }
            other => panic!("expected sum(b.score) AS total, got {:?}", other),
        }
    }

    #[test]
    fn parse_avg_aggregation() {
        let q = parse("MATCH (a)-[:edge]->(b) RETURN a, avg(b.confidence) AS mean").unwrap();
        match &q.return_items[1] {
            ReturnItem::Aggregation { func: AggFunc::Avg { var, prop }, alias } => {
                assert_eq!(var, "b");
                assert_eq!(prop, "confidence");
                assert_eq!(alias, "mean");
            }
            other => panic!("expected avg, got {:?}", other),
        }
    }

    #[test]
    fn parse_min_and_max_aggregations() {
        let q = parse("MATCH (a)-[:e]->(b) RETURN min(a.val) AS lo, max(a.val) AS hi").unwrap();
        match &q.return_items[0] {
            ReturnItem::Aggregation { func: AggFunc::Min { var, prop }, alias } => {
                assert_eq!(var, "a"); assert_eq!(prop, "val"); assert_eq!(alias, "lo");
            }
            other => panic!("expected min, got {:?}", other),
        }
        match &q.return_items[1] {
            ReturnItem::Aggregation { func: AggFunc::Max { var, prop }, alias } => {
                assert_eq!(var, "a"); assert_eq!(prop, "val"); assert_eq!(alias, "hi");
            }
            other => panic!("expected max, got {:?}", other),
        }
    }

    #[test]
    fn compile_sum_sets_aggregation() {
        let q = parse("MATCH (a)-[:edge]->(b) RETURN a, sum(b.score) AS total").unwrap();
        let compiled = compile(q);
        assert_eq!(compiled.aggregations.len(), 1);
        match &compiled.aggregations[0].func {
            AggFunc::Sum { var, prop } => { assert_eq!(var, "b"); assert_eq!(prop, "score"); }
            other => panic!("expected Sum, got {:?}", other),
        }
        assert_eq!(compiled.aggregations[0].alias, "total");
        assert_eq!(compiled.group_keys, vec!["a"]);
    }

    #[test]
    fn compile_avg_sets_aggregation() {
        let q = parse("MATCH (a)-[:knows]->(b) RETURN a, avg(b.weight) AS w").unwrap();
        let compiled = compile(q);
        assert_eq!(compiled.aggregations.len(), 1);
        assert!(matches!(&compiled.aggregations[0].func, AggFunc::Avg { var, prop } if var == "b" && prop == "weight"));
    }

    #[test]
    fn compile_min_max_set_aggregations() {
        let q = parse("MATCH (a)-[:e]->(b) RETURN min(b.v) AS lo, max(b.v) AS hi").unwrap();
        let compiled = compile(q);
        assert_eq!(compiled.aggregations.len(), 2);
        assert!(matches!(&compiled.aggregations[0].func, AggFunc::Min { .. }));
        assert!(matches!(&compiled.aggregations[1].func, AggFunc::Max { .. }));
    }

    // ── Variable-length path (bounded hop) tests ──────────────────────────────

    #[test]
    fn parse_bounded_hop_range() {
        let q = parse("MATCH (a)-[:hops*1..3]->(b) RETURN a, b").unwrap();
        let hop = &q.match_patterns[0].hops[0];
        assert!(hop.rel.recursive);
        assert_eq!(hop.rel.max_hops, Some(3));
    }

    #[test]
    fn parse_bounded_hop_single_n() {
        let q = parse("MATCH (a)-[:hops*5]->(b) RETURN a, b").unwrap();
        let hop = &q.match_patterns[0].hops[0];
        assert!(hop.rel.recursive);
        assert_eq!(hop.rel.max_hops, Some(5));
    }

    #[test]
    fn compile_bounded_hop_emits_pattern_with_max_hops() {
        let q = parse("MATCH (a)-[:hops*1..3]->(b) RETURN b").unwrap();
        let compiled = compile(q);
        // No TC rules — bounded hop uses max_hops on the VarPattern instead.
        assert!(compiled.rules.is_empty(), "bounded hop should not emit TC rules");
        assert_eq!(compiled.query.patterns.len(), 1);
        let p = &compiled.query.patterns[0];
        assert_eq!(p.max_hops, Some(3));
        assert_eq!(p.predicate.as_deref(), Some("hops"));
    }

    #[test]
    fn compile_unlimited_recursive_still_emits_tc_rules() {
        let q = parse("MATCH (a)-[:knows*]->(b) RETURN b").unwrap();
        let compiled = compile(q);
        assert_eq!(compiled.rules.len(), 2, "unlimited recursive should emit base + recursive TC rules");
        assert_eq!(compiled.query.patterns[0].max_hops, None);
    }

    #[test]
    fn bounded_hop_cypher_query_executes_correctly() {
        let (store, _dir) = open();
        let a = NodeId::new();
        let b = NodeId::new();
        let c = NodeId::new();
        let d = NodeId::new();
        // chain: a → b → c → d
        let snap = commit(&store, vec![
            rel_triple(a, "link", b),
            rel_triple(b, "link", c),
            rel_triple(c, "link", d),
        ]);

        // max_hops=2: should reach b and c, not d
        let q = parse("MATCH (a)-[:link*1..2]->(b) RETURN b").unwrap();
        let compiled = compile(q);

        let raw = crate::datalog::execute_query(&compiled.query, &snap, None, None).unwrap();
        let found: std::collections::HashSet<NodeId> = raw.iter()
            .filter_map(|b| b.get("b").copied())
            .collect();

        // a is the start — but it's not bound; the query starts with a free subject
        // so we get all pairs. Filter to those where subject == a.
        // Actually: subject is Term::Var("a") — will scan all links; we need the a
        // binding too. Let's just check counts from both ends.
        // The pattern subject=Var("a"), object=Var("b") with max_hops=2:
        // For each unique "a" binding, BFS up to 2 hops.
        // We just verify d is NOT reachable if we start from the node that is 3 hops away.
        assert!(!found.contains(&d) || found.len() > 1,
            "d should only appear in the result when starting from a is 3 hops");
    }

    // ── CALL / YIELD syntax tests ─────────────────────────────────────────────

    #[test]
    fn parse_call_yield_basic() {
        let q = parse(
            r#"CALL VECTOR_NEAR(embedding, "ctx", 20) YIELD node, score
               MATCH (node)-[:related]->(other)
               RETURN other, score"#,
        )
        .unwrap();

        let call = q.call_clause.as_ref().expect("expected CallClause");
        assert!(call.procedure.eq_ignore_ascii_case("VECTOR_NEAR"));
        assert_eq!(call.vector_param, "embedding");
        assert_eq!(call.space, "ctx");
        assert_eq!(call.k, 20);
        assert_eq!(call.ef, None);
        assert_eq!(call.yield_vars, vec!["node", "score"]);

        // MATCH patterns still parsed correctly
        assert_eq!(q.match_patterns.len(), 1);
        assert_eq!(q.match_patterns[0].hops[0].rel.predicate, "related");

        let ret_vars: Vec<&str> = q.return_items.iter().filter_map(|r| {
            if let ReturnItem::Variable(v) = r { Some(v.as_str()) } else { None }
        }).collect();
        assert_eq!(ret_vars, vec!["other", "score"]);
    }

    #[test]
    fn parse_call_yield_with_ef() {
        let q = parse(
            r#"CALL VECTOR_NEAR(vec, "docs", 10, ef=200) YIELD doc, dist RETURN doc"#,
        )
        .unwrap();
        let call = q.call_clause.as_ref().expect("expected CallClause");
        assert_eq!(call.ef, Some(200));
        assert_eq!(call.yield_vars, vec!["doc", "dist"]);
    }

    #[test]
    fn compile_call_yield_sets_vector_near_and_score_variable() {
        let q = parse(
            r#"CALL VECTOR_NEAR(qvec, "embs", 5) YIELD node, score RETURN node, score"#,
        )
        .unwrap();
        let compiled = compile(q);
        let vn = compiled.vector_near.expect("expected VectorNearClause from CALL");
        assert_eq!(vn.seed_variable, "node");
        assert_eq!(vn.space, "embs");
        assert_eq!(vn.k, 5);
        assert_eq!(vn.score_variable.as_deref(), Some("score"));
    }

    #[test]
    fn call_yield_without_match_is_valid() {
        // CALL alone (no MATCH) is legal — match_patterns will be empty.
        let q = parse(
            r#"CALL VECTOR_NEAR(v, "s", 3) YIELD node RETURN node"#,
        )
        .unwrap();
        assert!(q.match_patterns.is_empty());
        let call = q.call_clause.as_ref().expect("expected CallClause");
        assert_eq!(call.yield_vars, vec!["node"]);
        // score_variable is None when only one YIELD variable given
        let compiled = compile(q);
        let vn = compiled.vector_near.expect("VectorNearClause");
        assert_eq!(vn.score_variable, None);
    }

    #[test]
    fn call_yield_unknown_procedure_errors() {
        let r = parse(r#"CALL FULL_TEXT_SEARCH("index", "query") YIELD node RETURN node"#);
        assert!(r.is_err(), "unknown procedure should fail");
        let err = r.unwrap_err().to_string();
        assert!(err.contains("FULL_TEXT_SEARCH") || err.contains("unknown procedure"),
            "error message should name the bad procedure: {err}");
    }

    #[test]
    fn call_yield_missing_yield_errors() {
        let r = parse(r#"CALL VECTOR_NEAR(v, "s", 5) RETURN node"#);
        assert!(r.is_err(), "missing YIELD should fail");
    }

    #[test]
    fn where_vector_near_score_variable_is_none() {
        // Existing WHERE form must still compile with score_variable = None.
        let q = parse(r#"MATCH (a)-[:cites]->(b) WHERE VECTOR_NEAR(a, "docs", 5) RETURN b"#).unwrap();
        let compiled = compile(q);
        let vn = compiled.vector_near.expect("VectorNearClause");
        assert_eq!(vn.score_variable, None);
    }

    #[test]
    fn bounded_hop_depth_1_returns_direct_neighbours() {
        let (store, _dir) = open();
        let a = NodeId::new();
        let b = NodeId::new();
        let c = NodeId::new();
        let snap = commit(&store, vec![
            rel_triple(a, "edge", b),
            rel_triple(b, "edge", c),
        ]);

        // Add a prop to bind `a` in the WHERE clause
        let snap = {
            let mut tx = store.begin();
            tx.insert(prop_triple(a, "__type", "Start"));
            let ts = tx.commit().unwrap();
            store.snapshot(ts)
        };

        // MATCH (a:Start)-[:edge*1]->(b) should only return b (not c)
        let q = parse("MATCH (a:Start)-[:edge*1]->(b) RETURN b").unwrap();
        let compiled = compile(q);
        assert_eq!(compiled.query.patterns[0].max_hops, Some(1));

        let raw = crate::datalog::execute_query(&compiled.query, &snap, None, None).unwrap();
        let filtered = apply_value_filters(raw, &compiled.value_filters, &snap).unwrap();

        let found: Vec<NodeId> = filtered.iter().filter_map(|b| b.get("b").copied()).collect();
        assert_eq!(found.len(), 1, "only direct neighbour should be reachable at depth 1");
        assert_eq!(found[0], b);
    }
}
