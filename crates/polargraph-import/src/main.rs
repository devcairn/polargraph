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
//! Handles the common subset:
//!   - `<uri> <uri> <uri> .`   → `Triple::Relation`
//!   - `<uri> <uri> "literal" .` → `Triple::Property` (Value::Text)
//!   - Lines starting with `#` and blank lines are skipped.
//!   - Typed (`"val"^^<type>`) and language-tagged (`"val"@lang`) literals are
//!     accepted; the tag/type is stripped and the string value is stored.
//!
//! URIs are hashed to stable NodeIds using xxHash3-128. The same URI always
//! produces the same NodeId across runs.
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
    id::{EdgeId, NodeId},
    temporal::{BiTemporalRange, Timestamp},
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
    #[arg(
        long = "data-dir",
        env = "POLARGRAPH_DATA_DIR",
        value_name = "PATH"
    )]
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

    /// Log filter directive (same syntax as `RUST_LOG`).
    #[arg(long = "log", env = "RUST_LOG", default_value = "info", value_name = "FILTER")]
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
            let triples = parse_input_turtle(&buf)?;
            let mut current_batch: Vec<Triple> = Vec::with_capacity(cli.batch_size);
            for t in triples {
                current_batch.push(t);
                if current_batch.len() >= cli.batch_size {
                    batch_num += 1;
                    total_imported +=
                        flush_batch(&current_batch, &store, &temp_dir, batch_num)?;
                    current_batch.clear();
                }
            }
            if !current_batch.is_empty() {
                batch_num += 1;
                total_imported +=
                    flush_batch(&current_batch, &store, &temp_dir, batch_num)?;
            }
        }
        "jsonld" | "json-ld" => {
            let mut text = String::new();
            BufReader::new(file).read_to_string(&mut text)?;
            let triples = parse_input_jsonld(&text)?;
            let mut current_batch: Vec<Triple> = Vec::with_capacity(cli.batch_size);
            for t in triples {
                current_batch.push(t);
                if current_batch.len() >= cli.batch_size {
                    batch_num += 1;
                    total_imported +=
                        flush_batch(&current_batch, &store, &temp_dir, batch_num)?;
                    current_batch.clear();
                }
            }
            if !current_batch.is_empty() {
                batch_num += 1;
                total_imported +=
                    flush_batch(&current_batch, &store, &temp_dir, batch_num)?;
            }
        }
        _ => {
            let reader = BufReader::new(file);
            let mut current_batch: Vec<Triple> = Vec::with_capacity(cli.batch_size);
            let mut line_num = 0usize;
            let mut skipped = 0usize;

            for line in reader.lines() {
                line_num = line_num.wrapping_add(1);
                let line =
                    line.with_context(|| format!("I/O error reading line {line_num}"))?;

                match parse_line(&line) {
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
                    total_imported +=
                        flush_batch(&current_batch, &store, &temp_dir, batch_num)?;
                    current_batch.clear();
                }
            }
            if !current_batch.is_empty() {
                batch_num += 1;
                total_imported +=
                    flush_batch(&current_batch, &store, &temp_dir, batch_num)?;
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

    let stats = importer
        .finish(store)
        .with_context(|| format!("SST ingestion failed for batch {batch_num}"))?;

    println!(
        "Imported {} triples (batch {}) in {}ms",
        stats.triples_imported, batch_num, stats.duration_ms,
    );

    Ok(stats.triples_imported)
}

// ── N-Triples parser ──────────────────────────────────────────────────────────

/// Parse one N-Triples line. Returns `None` for blank lines, comments, and
/// lines that don't match the expected format.
fn parse_line(line: &str) -> Option<Triple> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }

    // Strip trailing ` .` (with optional whitespace before the dot).
    let line = line.strip_suffix('.')?.trim_end();

    // Subject (must be a URI)
    let (subject_uri, rest) = parse_uri(line)?;
    let rest = rest.trim_start();

    // Predicate (must be a URI)
    let (pred_uri, rest) = parse_uri(rest)?;
    let rest = rest.trim_start();

    let now = Timestamp::now();
    let temporal = BiTemporalRange {
        vt_start: now,
        vt_end: Timestamp::END_OF_TIME,
        tt: Timestamp(0), // overwritten by SstImporter::finish()
    };

    let subject = uri_to_node_id(subject_uri);
    let predicate = Predicate::new(pred_uri);

    if rest.starts_with('<') {
        // Object is a URI → Relation triple
        let (obj_uri, _) = parse_uri(rest)?;
        let object = uri_to_node_id(obj_uri);
        Some(Triple::Relation {
            subject,
            predicate,
            object,
            edge_id: edge_id_for(subject_uri, pred_uri, obj_uri),
            temporal,
        })
    } else if rest.starts_with('"') {
        // Object is a literal → Property triple (Value::Text)
        let literal = parse_literal(rest)?;
        Some(Triple::Property {
            subject,
            predicate,
            value: Value::Text(literal.to_string()),
            temporal,
        })
    } else {
        // Blank nodes (`_:…`) and other forms are not supported.
        None
    }
}

/// Extract the URI content from `<...>` and return `(uri_str, remainder)`.
fn parse_uri(s: &str) -> Option<(&str, &str)> {
    let s = s.strip_prefix('<')?;
    let end = s.find('>')?;
    Some((&s[..end], &s[end + 1..]))
}

/// Extract the literal string from `"..."` (ignoring trailing `@lang` /
/// `^^<type>`). Returns the raw string content without escape processing.
fn parse_literal(s: &str) -> Option<&str> {
    let s = s.strip_prefix('"')?;
    // Find the closing quote (not handling escaped quotes for simplicity).
    let end = s.find('"')?;
    Some(&s[..end])
}

/// Hash a URI string to a stable, deterministic `NodeId` using xxHash3-128.
fn uri_to_node_id(uri: &str) -> NodeId {
    let hash: u128 = xxhash_rust::xxh3::xxh3_128(uri.as_bytes());
    NodeId(uuid::Uuid::from_bytes(hash.to_le_bytes()))
}

/// Generate a deterministic `EdgeId` from the three URI strings of a relation.
fn edge_id_for(subject: &str, predicate: &str, object: &str) -> EdgeId {
    let mut buf = Vec::with_capacity(subject.len() + predicate.len() + object.len() + 2);
    buf.extend_from_slice(subject.as_bytes());
    buf.push(b'\x00');
    buf.extend_from_slice(predicate.as_bytes());
    buf.push(b'\x00');
    buf.extend_from_slice(object.as_bytes());
    let hash: u128 = xxhash_rust::xxh3::xxh3_128(&buf);
    EdgeId(uuid::Uuid::from_bytes(hash.to_le_bytes()))
}

// ── Turtle / JSON-LD parsers ──────────────────────────────────────────────────

/// Parse a Turtle document into PolarGraph triples using rio_turtle.
fn parse_input_turtle(input: &[u8]) -> Result<Vec<Triple>> {
    use rio_api::{
        model::{Literal, Subject, Term, Triple as RioTriple},
        parser::TriplesParser,
    };
    use rio_turtle::TurtleParser;

    let cursor = std::io::Cursor::new(input);
    let mut parser = TurtleParser::new(cursor, None);
    let mut triples = Vec::new();

    let now = Timestamp::now();
    let temporal_template = BiTemporalRange {
        vt_start: now,
        vt_end: Timestamp::END_OF_TIME,
        tt: Timestamp(0),
    };

    parser
        .parse_all(&mut |t: RioTriple<'_>| -> Result<(), rio_turtle::TurtleError> {
            let subject_str = match &t.subject {
                Subject::NamedNode(n) => n.iri.to_string(),
                Subject::BlankNode(b) => format!("_:bnode_{}", b.id),
                Subject::Triple(_) => return Ok(()), // skip quoted triple subjects
            };
            let subject = uri_to_node_id(&subject_str);
            let predicate = Predicate::new(t.predicate.iri);

            let triple = match &t.object {
                Term::NamedNode(n) => Triple::Relation {
                    subject,
                    predicate,
                    object: uri_to_node_id(n.iri),
                    edge_id: edge_id_for(&subject_str, t.predicate.iri, n.iri),
                    temporal: temporal_template,
                },
                Term::BlankNode(b) => {
                    let bnode_uri = format!("_:bnode_{}", b.id);
                    Triple::Relation {
                        subject,
                        predicate,
                        object: uri_to_node_id(&bnode_uri),
                        edge_id: edge_id_for(&subject_str, t.predicate.iri, &bnode_uri),
                        temporal: temporal_template,
                    }
                }
                Term::Literal(lit) => {
                    let value = match lit {
                        Literal::Simple { value } | Literal::LanguageTaggedString { value, .. } => {
                            Value::Text(value.to_string())
                        }
                        Literal::Typed { value, datatype } => {
                            xsd_to_value(value, datatype.iri)
                        }
                    };
                    Triple::Property { subject, predicate, value, temporal: temporal_template }
                }
                Term::Triple(_) => return Ok(()), // skip quoted triple objects
            };
            triples.push(triple);
            Ok(())
        })
        .map_err(|e| anyhow::anyhow!("Turtle parse error: {}", e))?;

    Ok(triples)
}

/// Parse a JSON-LD document into PolarGraph triples.
fn parse_input_jsonld(input: &str) -> Result<Vec<Triple>> {
    let doc: serde_json::Value =
        serde_json::from_str(input).context("JSON parse error")?;

    let graph = doc
        .get("@graph")
        .and_then(|v: &serde_json::Value| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("JSON-LD document missing @graph array"))?;

    let now = Timestamp::now();
    let temporal_template = BiTemporalRange {
        vt_start: now,
        vt_end: Timestamp::END_OF_TIME,
        tt: Timestamp(0),
    };

    let mut triples = Vec::new();

    for node in graph.iter() {
        let obj = match node.as_object() {
            Some(o) => o,
            None => continue,
        };
        let subject_iri = match obj.get("@id").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => continue,
        };
        let subject = uri_to_node_id(&subject_iri);

        for (key, val) in obj {
            if key.starts_with('@') {
                continue;
            }
            let predicate = Predicate::new(key.as_str());

            let items: Vec<&serde_json::Value> = if val.is_array() {
                val.as_array().unwrap().iter().collect()
            } else {
                vec![val]
            };

            for item in items {
                if let Some(id) = item.get("@id").and_then(|v| v.as_str()) {
                    triples.push(Triple::Relation {
                        subject,
                        predicate: predicate.clone(),
                        object: uri_to_node_id(id),
                        edge_id: edge_id_for(&subject_iri, key, id),
                        temporal: temporal_template,
                    });
                } else if let Some(raw) = item.get("@value") {
                    let type_str = item
                        .get("@type")
                        .and_then(|t| t.as_str())
                        .unwrap_or("xsd:string");
                    let full_dt = expand_xsd_prefix(type_str);
                    let value =
                        xsd_to_value(raw.as_str().unwrap_or(&raw.to_string()), &full_dt);
                    triples.push(Triple::Property {
                        subject,
                        predicate: predicate.clone(),
                        value,
                        temporal: temporal_template,
                    });
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
        | "http://www.w3.org/2001/XMLSchema#int" => {
            s.parse::<i64>().map(Value::Int).unwrap_or_else(|_| Value::Text(s.to_string()))
        }
        "http://www.w3.org/2001/XMLSchema#double"
        | "http://www.w3.org/2001/XMLSchema#float"
        | "http://www.w3.org/2001/XMLSchema#decimal" => {
            s.parse::<f64>().map(Value::Float).unwrap_or_else(|_| Value::Text(s.to_string()))
        }
        "http://www.w3.org/2001/XMLSchema#boolean" => {
            Value::Bool(matches!(s, "true" | "1"))
        }
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

    #[test]
    fn parse_relation_line() {
        let line = "<http://example.org/Alice> <http://schema.org/knows> <http://example.org/Bob> .";
        let triple = parse_line(line).unwrap();
        assert!(matches!(triple, Triple::Relation { .. }));
    }

    #[test]
    fn parse_property_line() {
        let line = r#"<http://example.org/Alice> <http://schema.org/name> "Alice" ."#;
        let triple = parse_line(line).unwrap();
        match triple {
            Triple::Property { value: Value::Text(s), .. } => assert_eq!(s, "Alice"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_property_with_lang_tag() {
        let line = r#"<http://example.org/Alice> <http://schema.org/name> "Alice"@en ."#;
        let triple = parse_line(line).unwrap();
        assert!(matches!(triple, Triple::Property { .. }));
    }

    #[test]
    fn parse_comment_returns_none() {
        assert!(parse_line("# this is a comment").is_none());
    }

    #[test]
    fn parse_blank_returns_none() {
        assert!(parse_line("   ").is_none());
        assert!(parse_line("").is_none());
    }

    #[test]
    fn uri_to_node_id_is_deterministic() {
        let id1 = uri_to_node_id("http://example.org/Alice");
        let id2 = uri_to_node_id("http://example.org/Alice");
        let id3 = uri_to_node_id("http://example.org/Bob");
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn blank_node_returns_none() {
        let line = "<http://example.org/Alice> <http://schema.org/knows> _:b0 .";
        assert!(parse_line(line).is_none());
    }
}
