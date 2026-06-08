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
    io::{BufRead, BufReader},
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

    let reader = BufReader::new(file);

    let total_start = Instant::now();
    let mut total_imported = 0usize;
    let mut batch_num = 0usize;
    let mut current_batch: Vec<Triple> = Vec::with_capacity(cli.batch_size);
    let mut line_num = 0usize;
    let mut skipped = 0usize;

    for line in reader.lines() {
        line_num += 1;
        let line = line.with_context(|| format!("I/O error reading line {line_num}"))?;

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
            let imported = flush_batch(&current_batch, &store, &temp_dir, batch_num)?;
            total_imported += imported;
            current_batch.clear();
        }
    }

    // Flush the final (possibly partial) batch.
    if !current_batch.is_empty() {
        batch_num += 1;
        let imported = flush_batch(&current_batch, &store, &temp_dir, batch_num)?;
        total_imported += imported;
    }

    let total_ms = total_start.elapsed().as_millis() as u64;
    let triples_per_sec = if total_ms > 0 {
        (total_imported as u64 * 1000) / total_ms
    } else {
        total_imported as u64
    };

    println!(
        "Total: {} triples in {}ms ({} triples/sec)",
        total_imported, total_ms, triples_per_sec,
    );

    if skipped > 0 {
        info!(skipped, "lines skipped (unparseable — not blank/comment)");
    }

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
