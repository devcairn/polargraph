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

use std::collections::HashSet;

use polargraph_core::{triple::Triple, value::Value};
use polargraph_storage::{Snapshot, StorageError};

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

#[derive(Debug)]
pub struct WhereClause {
    pub var: String,
    pub prop: String,
    pub value: CypherValue,
}

/// Parsed representation of a Cypher query.
#[derive(Debug)]
pub struct CypherQuery {
    match_patterns: Vec<CypherPath>,
    where_clauses: Vec<WhereClause>,
    pub return_vars: Vec<String>,
    pub limit: Option<usize>,
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

/// The result of compiling a `CypherQuery` to the Datalog IR.
pub struct CompiledQuery {
    pub query: Query,
    pub rules: Vec<Rule>,
    pub value_filters: Vec<ValueFilter>,
    pub return_vars: Vec<String>,
    pub limit: Option<usize>,
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
                    if self.peek() == Some(b'-') {
                        self.advance();
                        tokens.push((pos, Token::LeftArrow));
                    } else {
                        return Err(CypherError::at(pos, "unexpected '<'; only '<-' is supported"));
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
        let pos = self.current_pos();
        if !self.peek_keyword("MATCH") {
            return Err(CypherError::at(pos, "query must start with MATCH"));
        }
        self.advance(); // consume MATCH

        let match_patterns = self.parse_match_clause()?;

        let mut where_clauses = Vec::new();
        if self.peek_keyword("WHERE") {
            self.advance();
            where_clauses = self.parse_where_clause()?;
        }

        let pos = self.current_pos();
        if !self.peek_keyword("RETURN") {
            return Err(CypherError::at(pos, "expected RETURN clause"));
        }
        self.advance(); // consume RETURN

        let return_vars = self.parse_return_clause()?;

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

        Ok(CypherQuery { match_patterns, where_clauses, return_vars, limit })
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

        // Optional variable name (ignored in compilation for now)
        if let Some(Token::Ident(s)) = self.peek().cloned() {
            if !is_clause_keyword(&s) && s != "r" || matches!(self.tokens.get(self.pos + 1), Some((_, Token::Colon))) {
                // Consume if it's a var name before ':'
                if matches!(self.tokens.get(self.pos + 1), Some((_, Token::Colon))) {
                    self.advance(); // var name (ignored)
                }
            }
        }

        self.expect(&Token::Colon)?;
        let predicate = self.expect_ident()?;

        let (recursive, max_hops) = if self.peek() == Some(&Token::Star) {
            self.advance(); // consume '*'
            // Optional N..M bounds
            match self.peek().cloned() {
                Some(Token::Int(min_n)) => {
                    self.advance();
                    self.expect(&Token::DotDot)?;
                    let max_n = self.expect_int()?;
                    let _ = min_n; // min not used in current eval
                    (true, Some(max_n as usize))
                }
                _ => (true, None),
            }
        } else {
            (false, None)
        };

        self.expect(&Token::RBracket)?;
        Ok(RelPat { predicate, recursive, max_hops })
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
        let var = self.expect_ident()?;
        self.expect(&Token::Dot)?;
        let prop = self.expect_ident()?;
        self.expect(&Token::Eq)?;
        let value = self.parse_literal()?;
        Ok(WhereClause { var, prop, value })
    }

    fn parse_return_clause(&mut self) -> Result<Vec<String>, CypherError> {
        let mut vars = vec![self.expect_ident()?];
        while self.peek() == Some(&Token::Comma) {
            self.advance();
            vars.push(self.expect_ident()?);
        }
        Ok(vars)
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
    matches!(s.to_ascii_uppercase().as_str(), "MATCH" | "WHERE" | "AND" | "RETURN" | "LIMIT")
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Parse a Cypher query string into an AST.
pub fn parse(input: &str) -> Result<CypherQuery, CypherError> {
    let tokens = Lexer::new(input).tokenize()?;
    Parser::new(tokens).parse_query()
}

/// Compile a parsed `CypherQuery` into the Datalog IR.
pub fn compile(cypher: CypherQuery) -> CompiledQuery {
    let mut patterns: Vec<VarPattern> = Vec::new();
    let mut rules: Vec<Rule> = Vec::new();
    let mut value_filters: Vec<ValueFilter> = Vec::new();
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
            &mut anon,
        );
    }

    // WHERE clauses compile to value filters only (the MATCH patterns already
    // ensure the referenced vars are bound).
    for wc in &cypher.where_clauses {
        value_filters.push(ValueFilter {
            var: wc.var.clone(),
            predicate: wc.prop.clone(),
            value: wc.value.to_core_value(),
        });
    }

    CompiledQuery {
        query: Query { patterns },
        rules,
        value_filters,
        return_vars: cypher.return_vars,
        limit: cypher.limit,
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

fn compile_path(
    path: &CypherPath,
    _rel_bound: &HashSet<String>,
    patterns: &mut Vec<VarPattern>,
    rules: &mut Vec<Rule>,
    value_filters: &mut Vec<ValueFilter>,
    derived_preds: &mut HashSet<String>,
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
            let tc_pred = format!("__tc_{}", pred);

            // Emit two rules the first time we see this transitive predicate.
            if derived_preds.insert(tc_pred.clone()) {
                // Base rule: __tc_pred(x, y) :- pred(x, y)
                rules.push(
                    Rule::new(tc_pred.clone(), "x", "y").with_body(vec![VarPattern {
                        subject: Term::Var("x".into()),
                        predicate: Some(pred.clone()),
                        object: Term::Var("y".into()),
                    }]),
                );
                // Recursive rule: __tc_pred(x, z) :- __tc_pred(x, y), pred(y, z)
                rules.push(
                    Rule::new(tc_pred.clone(), "x", "z").with_body(vec![
                        VarPattern {
                            subject: Term::Var("x".into()),
                            predicate: Some(tc_pred.clone()),
                            object: Term::Var("y".into()),
                        },
                        VarPattern {
                            subject: Term::Var("y".into()),
                            predicate: Some(pred.clone()),
                            object: Term::Var("z".into()),
                        },
                    ]),
                );
            }

            let (subj, obj) = match hop.direction {
                Direction::Out => (Term::Var(current_var.clone()), Term::Var(end_var.clone())),
                Direction::In  => (Term::Var(end_var.clone()), Term::Var(current_var.clone())),
            };
            patterns.push(VarPattern { subject: subj, predicate: Some(tc_pred), object: obj });
        } else {
            let (subj, obj) = match hop.direction {
                Direction::Out => (Term::Var(current_var.clone()), Term::Var(end_var.clone())),
                Direction::In  => (Term::Var(end_var.clone()), Term::Var(current_var.clone())),
            };
            patterns.push(VarPattern {
                subject: subj,
                predicate: Some(hop.rel.predicate.clone()),
                object: obj,
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

    #[test]
    fn parse_simple_match() {
        let q = parse("MATCH (a:Person) RETURN a").unwrap();
        assert_eq!(q.match_patterns.len(), 1);
        let path = &q.match_patterns[0];
        assert!(path.hops.is_empty());
        assert_eq!(path.start.var.as_deref(), Some("a"));
        assert_eq!(path.start.label.as_deref(), Some("Person"));
        assert_eq!(q.return_vars, vec!["a".to_string()]);
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
        assert_eq!(q.return_vars, vec!["a", "b"]);
    }

    #[test]
    fn parse_where() {
        let q = parse(r#"MATCH (a:Person)-[:knows]->(b) WHERE a.name = "Alice" RETURN a, b"#).unwrap();
        assert_eq!(q.where_clauses.len(), 1);
        let wc = &q.where_clauses[0];
        assert_eq!(wc.var, "a");
        assert_eq!(wc.prop, "name");
        assert!(matches!(&wc.value, CypherValue::Str(s) if s == "Alice"));
    }

    #[test]
    fn parse_where_and_multiple() {
        let q = parse(r#"MATCH (a) WHERE a.name = "X" AND a.active = true RETURN a"#).unwrap();
        assert_eq!(q.where_clauses.len(), 2);
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
}
