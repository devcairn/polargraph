//! `polargraph-import` — bulk N-Triples importer for PolarGraph DB.
//!
//! Reads an N-Triples file (`.nt`) and loads it into a RocksDB store via SST
//! file ingestion, bypassing gRPC overhead entirely.
//!
//! # Why offline-only
//!
//! SST ingestion requires exclusive access to the RocksDB database. Run this
//! tool only while `polargraphd` is stopped. After import completes, start the
//! server as normal — all imported triples will be immediately visible.
//!
//! # N-Triples support
//!
//! Each line is parsed with `rio_turtle`'s N-Triples parser:
//!   - `<iri> <iri> <iri> .` and blank-node subjects/objects → `Triple::Relation`
//!   - `<iri> <iri> "literal" .` → `Triple::Property`; `xsd:integer`/`long`/`int`,
//!     `double`/`float`/`decimal` and `boolean` literals become typed values,
//!     everything else (including language-tagged strings) is stored as text.
//!   - Comment and blank lines are skipped; unparseable lines are counted and
//!     skipped rather than aborting the import.
//!
//! IRIs map to NodeIds via [`term::iri_to_node_id`] (`urn:uuid:` IRIs keep
//! their UUID; others are hashed). Blank nodes are
//! skolemized per import (`{skolem-base}/.well-known/genid/{import-id}/{label}`),
//! so `_:b0` in two different files is two different nodes. Re-running with the
//! same `--import-id` reproduces the same blank-node NodeIds.
//!
//! # Example
//!
//! ```bash
//! polargraph-import \
//!   --data-dir /var/lib/polargraph \
//!   --input    ./dump.nt \
//!   --batch-size 100000
//! ```

use std::{
    io::{BufRead, BufReader, Read},
    path::PathBuf,
    time::Instant,
};

use anyhow::{Context, Result};
use clap::Parser;
use polargraph_core::{
    skolem::{ImportScope, DEFAULT_SKOLEM_BASE},
    temporal::{BiTemporalRange, Timestamp},
    term,
    triple::{Predicate, Triple},
    value::Value,
};
use polargraph_storage::{SstImporter, TripleStore};
use tracing::info;

// ── CLI ───────────────────────────────────────────────────────────────────────

/// PolarGraph bulk N-Triples importer.
///
/// Loads large datasets directly into RocksDB via SST file ingestion —
/// no gRPC server required. The server must be stopped before running this.
#[derive(Debug, Parser)]
#[command(name = "polargraph-import", version, about, long_about = None)]
struct Cli {
    /// RocksDB data directory (same path used by polargraphd --data-dir).
    #[arg(long = "data-dir", env = "POLARGRAPH_DATA_DIR", value_name = "PATH")]
    data_dir: PathBuf,

    /// Input N-Triples file (.nt). Use `-` to read from stdin.
    #[arg(long = "input", short = 'i', value_name = "FILE")]
    input: PathBuf,

    /// Number of triples per import batch.
    ///
    /// Larger batches build fewer SST files and are faster overall, but use
    /// more memory during encoding. Default 100 000 works well up to ~10 M triples.
    #[arg(long = "batch-size", default_value = "100000", value_name = "N")]
    batch_size: usize,

    /// Directory for temporary SST files.
    ///
    /// Defaults to `<data-dir>/sst_tmp`. Files are written here during each
    /// batch and can be deleted after import completes.
    #[arg(long = "temp-dir", value_name = "PATH")]
    temp_dir: Option<PathBuf>,

    /// RDF serialization format of the input file.
    ///
    /// `ntriples` (default) — one triple per line, no prefixes.
    /// `turtle`             — Turtle / Turtle-star with prefix declarations.
    /// `jsonld`             — JSON-LD @graph format.
    #[arg(long = "format", default_value = "ntriples", value_name = "FORMAT")]
    format: String,

    /// Identifier for this import, used to skolemize blank nodes.
    ///
    /// Re-running with the same id maps the same blank-node labels to the same
    /// NodeIds (idempotent re-import). Defaults to a fresh UUIDv7, which is
    /// printed at startup.
    #[arg(long = "import-id", value_name = "ID")]
    import_id: Option<String>,

    /// Base IRI for skolemized blank nodes — normally the instance's public
    /// origin, e.g. `https://kb.example.com`.
    #[arg(
        long = "skolem-base",
        env = "POLARGRAPH_SKOLEM_BASE",
        default_value = DEFAULT_SKOLEM_BASE,
        value_name = "IRI"
    )]
    skolem_base: String,

    /// Log filter directive (same syntax as `RUST_LOG`).
    #[arg(
        long = "log",
        env = "RUST_LOG",
        default_value = "info",
        value_name = "FILTER"
    )]
    log_filter: String,
}

// ── Entry point ───────────────────────────────────────────────────────────────

fn main() -> Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_new(&cli.log_filter)
                .with_context(|| format!("invalid log filter: {:?}", cli.log_filter))?,
        )
        .init();

    let temp_dir = cli.temp_dir.unwrap_or_else(|| cli.data_dir.join("sst_tmp"));

    let scope = match cli.import_id {
        Some(id) => ImportScope::new(&cli.skolem_base, id),
        None => ImportScope::fresh(&cli.skolem_base),
    };
    let conv = Converter::new(scope);
    println!("Import id: {}", conv.scope.import_id());

    info!(data_dir = %cli.data_dir.display(), input = %cli.input.display(), batch_size = cli.batch_size, "polargraph-import starting");

    std::fs::create_dir_all(&cli.data_dir)
        .with_context(|| format!("failed to create data dir: {}", cli.data_dir.display()))?;

    let store = TripleStore::open(&cli.data_dir)
        .with_context(|| format!("failed to open TripleStore at {}", cli.data_dir.display()))?;

    let file = std::fs::File::open(&cli.input)
        .with_context(|| format!("failed to open input file: {}", cli.input.display()))?;

    let total_start = Instant::now();
    let mut total_imported = 0usize;
    let mut batch_num = 0usize;

    match cli.format.as_str() {
        "turtle" | "ttl" => {
            // Read entire file into memory (rio_turtle needs BufRead internally).
            let mut buf = Vec::new();
            BufReader::new(file).read_to_end(&mut buf)?;
            let triples = parse_input_turtle(&buf, &conv)?;
            let mut current_batch: Vec<Triple> = Vec::with_capacity(cli.batch_size);
            for t in triples {
                current_batch.push(t);
                if current_batch.len() >= cli.batch_size {
                    batch_num += 1;
                    total_imported += flush_batch(
                        &current_batch,
                        conv.take_iris(),
                        &store,
                        &temp_dir,
                        batch_num,
                    )?;
                    current_batch.clear();
                }
            }
            if !current_batch.is_empty() {
                batch_num += 1;
                total_imported += flush_batch(
                    &current_batch,
                    conv.take_iris(),
                    &store,
                    &temp_dir,
                    batch_num,
                )?;
            }
        }
        "jsonld" | "json-ld" => {
            let mut text = String::new();
            BufReader::new(file).read_to_string(&mut text)?;
            let triples = parse_input_jsonld(&text, &conv)?;
            let mut current_batch: Vec<Triple> = Vec::with_capacity(cli.batch_size);
            for t in triples {
                current_batch.push(t);
                if current_batch.len() >= cli.batch_size {
                    batch_num += 1;
                    total_imported += flush_batch(
                        &current_batch,
                        conv.take_iris(),
                        &store,
                        &temp_dir,
                        batch_num,
                    )?;
                    current_batch.clear();
                }
            }
            if !current_batch.is_empty() {
                batch_num += 1;
                total_imported += flush_batch(
                    &current_batch,
                    conv.take_iris(),
                    &store,
                    &temp_dir,
                    batch_num,
                )?;
            }
        }
        _ => {
            let reader = BufReader::new(file);
            let mut current_batch: Vec<Triple> = Vec::with_capacity(cli.batch_size);
            let mut line_num = 0usize;
            let mut skipped = 0usize;

            for line in reader.lines() {
                line_num = line_num.wrapping_add(1);
                let line = line.with_context(|| format!("I/O error reading line {line_num}"))?;

                match parse_line(&line, &conv) {
                    Some(triple) => current_batch.push(triple),
                    None => {
                        let trimmed = line.trim();
                        if !trimmed.is_empty() && !trimmed.starts_with('#') {
                            skipped += 1;
                        }
                        continue;
                    }
                }

                if current_batch.len() >= cli.batch_size {
                    batch_num += 1;
                    total_imported += flush_batch(
                        &current_batch,
                        conv.take_iris(),
                        &store,
                        &temp_dir,
                        batch_num,
                    )?;
                    current_batch.clear();
                }
            }
            if !current_batch.is_empty() {
                batch_num += 1;
                total_imported += flush_batch(
                    &current_batch,
                    conv.take_iris(),
                    &store,
                    &temp_dir,
                    batch_num,
                )?;
            }
            if skipped > 0 {
                info!(skipped, "lines skipped (unparseable — not blank/comment)");
            }
        }
    }

    let total_ms = total_start.elapsed().as_millis() as u64;
    let triples_per_sec = (total_imported as u64 * 1000)
        .checked_div(total_ms)
        .unwrap_or(total_imported as u64);

    println!(
        "Total: {} triples in {}ms ({} triples/sec)",
        total_imported, total_ms, triples_per_sec,
    );

    Ok(())
}

// ── Batch flush ───────────────────────────────────────────────────────────────

fn flush_batch(
    triples: &[Triple],
    iris: Vec<String>,
    store: &TripleStore,
    temp_dir: &std::path::Path,
    batch_num: usize,
) -> Result<usize> {
    let batch_dir = temp_dir.join(format!("batch_{batch_num}"));
    let mut importer = SstImporter::new(&batch_dir)
        .with_context(|| format!("failed to create SstImporter for batch {batch_num}"))?;

    for triple in triples {
        importer.add_triple(triple);
    }
    for iri in iris {
        importer.add_iri(iri);
    }

    let stats = importer
        .finish(store)
        .with_context(|| format!("SST ingestion failed for batch {batch_num}"))?;

    println!(
        "Imported {} triples (batch {}) in {}ms",
        stats.triples_imported, batch_num, stats.duration_ms,
    );

    Ok(stats.triples_imported)
}

// ── Triple conversion ─────────────────────────────────────────────────────────

/// Converts parsed RDF terms into PolarGraph triples within one import scope.
struct Converter {
    scope: ImportScope,
    temporal: BiTemporalRange,
    /// IRIs named since the last `take_iris`, for the IRI dictionary.
    iris: std::cell::RefCell<std::collections::BTreeSet<String>>,
}

impl Converter {
    fn new(scope: ImportScope) -> Self {
        Self {
            scope,
            temporal: BiTemporalRange {
                vt_start: Timestamp::now(),
                vt_end: Timestamp::END_OF_TIME,
                tt: Timestamp(0), // overwritten by SstImporter::finish()
            },
            iris: Default::default(),
        }
    }

    fn record_iri(&self, iri: &str) {
        if term::needs_dictionary(iri) {
            self.iris.borrow_mut().insert(iri.to_string());
        }
    }

    /// Drain the IRIs named since the last call.
    fn take_iris(&self) -> Vec<String> {
        std::mem::take(&mut *self.iris.borrow_mut())
            .into_iter()
            .collect()
    }

    /// The identifying IRI for a node reference: the IRI itself, or the skolem
    /// IRI for a blank-node label.
    fn node_iri(&self, iri_or_bnode: NodeRef<'_>) -> String {
        match iri_or_bnode {
            NodeRef::Iri(iri) => iri.to_string(),
            NodeRef::Blank(label) => self.scope.skolem_iri(label),
        }
    }

    fn relation(&self, subject: &str, predicate: &str, object: &str) -> Triple {
        self.record_iri(subject);
        self.record_iri(object);
        Triple::Relation {
            subject: term::iri_to_node_id(subject),
            predicate: Predicate::new(predicate),
            object: term::iri_to_node_id(object),
            edge_id: term::edge_id_for(subject, predicate, object),
            temporal: self.temporal,
        }
    }

    fn property(&self, subject: &str, predicate: &str, value: Value) -> Triple {
        self.record_iri(subject);
        Triple::Property {
            subject: term::iri_to_node_id(subject),
            predicate: Predicate::new(predicate),
            value,
            temporal: self.temporal,
        }
    }

    /// Convert one rio triple. Quoted-triple (RDF-star) terms are skipped.
    fn convert(&self, t: &rio_api::model::Triple<'_>) -> Option<Triple> {
        use rio_api::model::{Literal, Subject, Term};

        let subject = self.node_iri(match &t.subject {
            Subject::NamedNode(n) => NodeRef::Iri(n.iri),
            Subject::BlankNode(b) => NodeRef::Blank(b.id),
            Subject::Triple(_) => return None,
        });
        let predicate = t.predicate.iri;
        Some(match &t.object {
            Term::NamedNode(n) => self.relation(&subject, predicate, n.iri),
            Term::BlankNode(b) => self.relation(&subject, predicate, &self.scope.skolem_iri(b.id)),
            Term::Literal(lit) => {
                let value = match lit {
                    Literal::Simple { value } | Literal::LanguageTaggedString { value, .. } => {
                        Value::Text(value.to_string())
                    }
                    Literal::Typed { value, datatype } => xsd_to_value(value, datatype.iri),
                };
                self.property(&subject, predicate, value)
            }
            Term::Triple(_) => return None,
        })
    }
}

/// An IRI or a blank-node label, before skolemization.
#[derive(Clone, Copy)]
enum NodeRef<'a> {
    Iri(&'a str),
    Blank(&'a str),
}

impl<'a> NodeRef<'a> {
    /// Interpret a JSON-LD `@id`: `_:label` is a blank node, anything else an IRI.
    fn from_jsonld_id(id: &'a str) -> Self {
        match id.strip_prefix("_:") {
            Some(label) => NodeRef::Blank(label),
            None => NodeRef::Iri(id),
        }
    }
}

// ── N-Triples parser ──────────────────────────────────────────────────────────

/// Parse one N-Triples line. Returns `None` for blank lines, comments, and
/// lines that fail to parse (the caller counts those as skipped).
fn parse_line(line: &str, conv: &Converter) -> Option<Triple> {
    use rio_api::parser::TriplesParser;

    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return None;
    }
    let mut parsed = None;
    rio_turtle::NTriplesParser::new(trimmed.as_bytes())
        .parse_all(&mut |t| -> Result<(), rio_turtle::TurtleError> {
            if parsed.is_none() {
                parsed = conv.convert(&t);
            }
            Ok(())
        })
        .ok()?;
    parsed
}

// ── Turtle / JSON-LD parsers ──────────────────────────────────────────────────

/// Parse a Turtle document into PolarGraph triples using rio_turtle.
fn parse_input_turtle(input: &[u8], conv: &Converter) -> Result<Vec<Triple>> {
    use rio_api::parser::TriplesParser;

    let mut parser = rio_turtle::TurtleParser::new(std::io::Cursor::new(input), None);
    let mut triples = Vec::new();
    parser
        .parse_all(&mut |t| -> Result<(), rio_turtle::TurtleError> {
            triples.extend(conv.convert(&t));
            Ok(())
        })
        .map_err(|e| anyhow::anyhow!("Turtle parse error: {}", e))?;
    Ok(triples)
}

/// Parse a JSON-LD document into PolarGraph triples.
fn parse_input_jsonld(input: &str, conv: &Converter) -> Result<Vec<Triple>> {
    let doc: serde_json::Value = serde_json::from_str(input).context("JSON parse error")?;

    let graph = doc
        .get("@graph")
        .and_then(|v: &serde_json::Value| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("JSON-LD document missing @graph array"))?;

    let mut triples = Vec::new();

    for node in graph.iter() {
        let obj = match node.as_object() {
            Some(o) => o,
            None => continue,
        };
        let subject_iri = match obj.get("@id").and_then(|v| v.as_str()) {
            Some(s) => conv.node_iri(NodeRef::from_jsonld_id(s)),
            None => continue,
        };

        for (key, val) in obj {
            if key.starts_with('@') {
                continue;
            }
            let items: Vec<&serde_json::Value> = if val.is_array() {
                val.as_array().unwrap().iter().collect()
            } else {
                vec![val]
            };

            for item in items {
                if let Some(id) = item.get("@id").and_then(|v| v.as_str()) {
                    let object_iri = conv.node_iri(NodeRef::from_jsonld_id(id));
                    triples.push(conv.relation(&subject_iri, key, &object_iri));
                } else if let Some(raw) = item.get("@value") {
                    let type_str = item
                        .get("@type")
                        .and_then(|t| t.as_str())
                        .unwrap_or("xsd:string");
                    let full_dt = expand_xsd_prefix(type_str);
                    let value = xsd_to_value(raw.as_str().unwrap_or(&raw.to_string()), &full_dt);
                    triples.push(conv.property(&subject_iri, key, value));
                }
            }
        }
    }

    Ok(triples)
}

/// Convert an XSD literal value string to a PolarGraph [`Value`].
fn xsd_to_value(s: &str, datatype: &str) -> Value {
    match datatype {
        "http://www.w3.org/2001/XMLSchema#integer"
        | "http://www.w3.org/2001/XMLSchema#long"
        | "http://www.w3.org/2001/XMLSchema#int" => s
            .parse::<i64>()
            .map(Value::Int)
            .unwrap_or_else(|_| Value::Text(s.to_string())),
        "http://www.w3.org/2001/XMLSchema#double"
        | "http://www.w3.org/2001/XMLSchema#float"
        | "http://www.w3.org/2001/XMLSchema#decimal" => s
            .parse::<f64>()
            .map(Value::Float)
            .unwrap_or_else(|_| Value::Text(s.to_string())),
        "http://www.w3.org/2001/XMLSchema#boolean" => Value::Bool(matches!(s, "true" | "1")),
        _ => Value::Text(s.to_string()),
    }
}

fn expand_xsd_prefix(dt: &str) -> String {
    if let Some(local) = dt.strip_prefix("xsd:") {
        format!("http://www.w3.org/2001/XMLSchema#{}", local)
    } else {
        dt.to_string()
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn conv(import_id: &str) -> Converter {
        Converter::new(ImportScope::new("https://kb.example.com", import_id))
    }

    #[test]
    fn parse_relation_line() {
        let line =
            "<http://example.org/Alice> <http://schema.org/knows> <http://example.org/Bob> .";
        let triple = parse_line(line, &conv("t")).unwrap();
        match triple {
            Triple::Relation {
                subject, object, ..
            } => {
                assert_eq!(subject, term::iri_to_node_id("http://example.org/Alice"));
                assert_eq!(object, term::iri_to_node_id("http://example.org/Bob"));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_property_line() {
        let line = r#"<http://example.org/Alice> <http://schema.org/name> "Alice" ."#;
        match parse_line(line, &conv("t")).unwrap() {
            Triple::Property {
                value: Value::Text(s),
                ..
            } => assert_eq!(s, "Alice"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_property_with_lang_tag() {
        let line = r#"<http://example.org/Alice> <http://schema.org/name> "Alice"@en ."#;
        let triple = parse_line(line, &conv("t")).unwrap();
        assert!(matches!(triple, Triple::Property { .. }));
    }

    #[test]
    fn parse_typed_literal_and_escapes() {
        let c = conv("t");
        let int =
            r#"<http://ex/x> <http://ex/age> "30"^^<http://www.w3.org/2001/XMLSchema#integer> ."#;
        assert!(matches!(
            parse_line(int, &c),
            Some(Triple::Property {
                value: Value::Int(30),
                ..
            })
        ));
        let esc = r#"<http://ex/x> <http://ex/says> "a \"quoted\" word" ."#;
        match parse_line(esc, &c) {
            Some(Triple::Property {
                value: Value::Text(s),
                ..
            }) => assert_eq!(s, r#"a "quoted" word"#),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_comment_blank_and_garbage_return_none() {
        let c = conv("t");
        assert!(parse_line("# this is a comment", &c).is_none());
        assert!(parse_line("   ", &c).is_none());
        assert!(parse_line("", &c).is_none());
        assert!(parse_line("not n-triples at all", &c).is_none());
    }

    #[test]
    fn converter_collects_iris_for_the_dictionary() {
        let c = conv("t");
        parse_line(
            r#"<http://ex/a> <http://ex/p> <urn:uuid:0191c1f6-2b1e-7c3a-9f00-000000000001> ."#,
            &c,
        );
        parse_line(r#"_:b0 <http://ex/name> "x" ."#, &c);
        let iris = c.take_iris();
        assert_eq!(
            iris,
            vec![
                "http://ex/a".to_string(),
                "https://kb.example.com/.well-known/genid/t/b0".to_string(),
            ],
            "hashed IRIs (incl. skolem) are collected; urn:uuid is not"
        );
        assert!(c.take_iris().is_empty(), "take_iris drains");
    }

    #[test]
    fn blank_nodes_are_imported_and_scoped() {
        let line = "_:b0 <http://schema.org/knows> _:b1 .";
        let ends = |c: &Converter| match parse_line(line, c) {
            Some(Triple::Relation {
                subject,
                object,
                edge_id,
                ..
            }) => (subject, object, edge_id),
            other => panic!("bnode relation should import, got {other:?}"),
        };
        let a = ends(&conv("one"));
        let b = ends(&conv("two"));
        assert_ne!(a.0, b.0, "_:b0 differs across imports");
        assert_ne!(a.1, b.1, "_:b1 differs across imports");
        assert_ne!(a.2, b.2, "edge ids differ across imports");
        assert_eq!(a, ends(&conv("one")), "same import id is idempotent");
    }

    #[test]
    fn jsonld_blank_node_ids_are_scoped() {
        let doc = r#"{"@graph": [
            {"@id": "_:b0", "http://schema.org/knows": {"@id": "_:b1"}},
            {"@id": "_:b1", "http://schema.org/name": {"@value": "Bob"}}
        ]}"#;
        let a = parse_input_jsonld(doc, &conv("one")).unwrap();
        let b = parse_input_jsonld(doc, &conv("two")).unwrap();
        let (a_obj, a_named) = match (&a[0], &a[1]) {
            (Triple::Relation { object, .. }, Triple::Property { subject, .. }) => {
                (*object, *subject)
            }
            other => panic!("unexpected: {other:?}"),
        };
        assert_eq!(a_obj, a_named, "_:b1 is one node within an import");
        assert_ne!(
            a[0].subject(),
            b[0].subject(),
            "_:b0 differs across imports"
        );
    }

    #[test]
    fn turtle_blank_nodes_are_scoped() {
        let ttl = b"@prefix ex: <http://ex/> .\n[] ex:knows ex:Bob .\n";
        let a = parse_input_turtle(ttl, &conv("one")).unwrap();
        let b = parse_input_turtle(ttl, &conv("two")).unwrap();
        assert_eq!(a.len(), 1);
        assert_ne!(a[0].subject(), b[0].subject());
    }
}
