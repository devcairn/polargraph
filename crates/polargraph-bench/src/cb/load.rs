//! Loading: the seed and the generated base go in through SST import
//! (`docs/design/cb-bench.md` §3); supersession runs on the live write path
//! so decision statuses have history; then grants, the inference schema
//! graph (which runs the full materialization) and the vector spaces.

use std::{collections::HashMap, path::PathBuf, time::Instant};

use anyhow::{Context, Result};
use polargraph_core::{
    id::{GraphId, NodeId},
    schema::{GraphAccessLevel, StorageMode},
    skolem::ImportScope,
    term,
    triple::Triple,
    value::Value,
};
use polargraph_storage::{hnsw::SpaceOptions, owl_rl, SstImporter, TripleStore, WriteMode};

use super::generate::{self as gen, Mentions, Plan, Sink, Vectors};

const ONTOLOGY_TTL: &str = include_str!("../../seed/ontology.ttl");
const SHAPES_TTL: &str = include_str!("../../seed/shapes.ttl");
const SEED_TRIG: &str = include_str!("../../seed/seed.trig");

/// Which vector spaces to build.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum VectorSpaces {
    Both,
    F32,
    Int8,
    None,
}

impl VectorSpaces {
    pub fn spaces(self) -> Vec<(&'static str, bool)> {
        match self {
            VectorSpaces::Both => vec![(gen::SPACE_F32, false), (gen::SPACE_INT8, true)],
            VectorSpaces::F32 => vec![(gen::SPACE_F32, false)],
            VectorSpaces::Int8 => vec![(gen::SPACE_INT8, true)],
            VectorSpaces::None => vec![],
        }
    }
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct LoadReport {
    pub base_quads: u64,
    pub base_load_secs: f64,
    pub supersessions: u64,
    pub supersession_secs: f64,
    /// Read point just before the supersession pass (for time travel).
    pub pre_supersession_ts: i64,
    pub grants: u64,
    pub materialize_secs: f64,
    pub inferred_quads: u64,
    /// Per space: (name, vectors, build seconds, RSS growth in bytes).
    pub spaces: Vec<SpaceReport>,
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct SpaceReport {
    pub space: String,
    pub int8: bool,
    pub vectors: u64,
    pub build_secs: f64,
    pub rss_growth_bytes: Option<u64>,
    /// Vector payload held in RAM: f32 = 4·dims, int8 codes = dims + 4.
    pub payload_ram_bytes: u64,
}

/// SST-import sink: buffers up to `batch` quads, then ingests them.
struct SstSink<'a> {
    store: &'a TripleStore,
    dir: PathBuf,
    batch: usize,
    importer: SstImporter,
    pending: usize,
    total: u64,
    graphs: HashMap<String, GraphId>,
}

impl<'a> SstSink<'a> {
    fn new(store: &'a TripleStore, dir: PathBuf, batch: usize) -> Result<Self> {
        Ok(Self {
            store,
            importer: SstImporter::new(&dir)?,
            dir,
            batch,
            pending: 0,
            total: 0,
            graphs: HashMap::new(),
        })
    }

    fn graph(&mut self, iri: &str) -> GraphId {
        if iri.is_empty() {
            return GraphId::DEFAULT;
        }
        if let Some(g) = self.graphs.get(iri) {
            return *g;
        }
        let g = self
            .store
            .create_graph(iri, &[])
            .expect("create generated graph");
        self.graphs.insert(iri.to_string(), g);
        g
    }

    fn flush(&mut self) -> Result<()> {
        if self.pending == 0 {
            return Ok(());
        }
        let importer = std::mem::replace(&mut self.importer, SstImporter::new(&self.dir)?);
        importer.finish(self.store)?;
        self.pending = 0;
        Ok(())
    }
}

impl Sink for SstSink<'_> {
    fn quad(&mut self, graph: &str, triple: Triple) {
        let g = self.graph(graph);
        self.importer.add_triple_in(&triple, g);
        self.pending += 1;
        self.total += 1;
        if self.pending >= self.batch {
            self.flush().expect("SST ingest");
        }
    }

    fn iri(&mut self, iri: &str) {
        if term::needs_dictionary(iri) {
            self.importer.add_iri(iri);
        }
    }
}

/// Parse Turtle or TriG (`trig`) into the sink; blank nodes are skolemized.
fn load_rdf(sink: &mut impl Sink, text: &str, default_graph: &str, trig: bool) -> Result<()> {
    use rio_api::model::{GraphName, Literal, Quad, Subject, Term};
    use rio_api::parser::{QuadsParser, TriplesParser};

    let scope = ImportScope::new("https://polargraph.invalid", "cb-seed");
    let node = |s: &Subject<'_>| -> Option<String> {
        match s {
            Subject::NamedNode(n) => Some(n.iri.to_string()),
            Subject::BlankNode(b) => Some(scope.skolem_iri(b.id)),
            Subject::Triple(_) => None,
        }
    };
    let mut on = |q: Quad<'_>| -> Result<(), std::io::Error> {
        let graph = match q.graph_name {
            Some(GraphName::NamedNode(n)) => n.iri.to_string(),
            Some(GraphName::BlankNode(b)) => scope.skolem_iri(b.id),
            None => default_graph.to_string(),
        };
        let Some(s) = node(&q.subject) else {
            return Ok(());
        };
        let pred = q.predicate.iri;
        let triple = match &q.object {
            Term::NamedNode(n) => {
                sink.iri(n.iri);
                gen::rel(&s, pred, n.iri)
            }
            Term::BlankNode(b) => gen::rel(&s, pred, &scope.skolem_iri(b.id)),
            Term::Literal(lit) => {
                let v: Value = match lit {
                    Literal::Simple { value } => term::literal_to_value(value, None, None),
                    Literal::LanguageTaggedString { value, language } => {
                        term::literal_to_value(value, None, Some(language))
                    }
                    Literal::Typed { value, datatype } => {
                        term::literal_to_value(value, Some(datatype.iri), None)
                    }
                };
                gen::prop(&s, pred, v)
            }
            Term::Triple(_) => return Ok(()),
        };
        sink.iri(&s);
        sink.quad(&graph, triple);
        Ok(())
    };
    let cursor = std::io::Cursor::new(text.as_bytes());
    if trig {
        rio_turtle::TriGParser::new(cursor, None).parse_all(&mut on)?;
    } else {
        rio_turtle::TurtleParser::new(cursor, None).parse_all(&mut |t| {
            on(Quad {
                subject: t.subject,
                predicate: t.predicate,
                object: t.object,
                graph_name: None,
            })
        })?;
    }
    Ok(())
}

/// Load everything; returns the report.
pub fn load(
    store: &TripleStore,
    data_dir: &std::path::Path,
    plan: &Plan,
    vectors: VectorSpaces,
    batch: usize,
    log: &mut dyn FnMut(&str),
) -> Result<LoadReport> {
    let mut report = LoadReport::default();
    let mentions = Mentions::new(plan);

    // ── Base: seed + generated, via SST import ───────────────────────────────
    let t0 = Instant::now();
    let tmp = data_dir.join("cb_sst_tmp");
    let mut sink = SstSink::new(store, tmp.clone(), batch)?;
    for g in [gen::ORG_GRAPH, gen::DECISIONS_GRAPH] {
        sink.graph(g);
    }
    for t in 0..plan.teams {
        sink.graph(&gen::records_graph(t));
        sink.graph(&gen::proposals_graph(t));
    }
    load_rdf(&mut sink, ONTOLOGY_TTL, gen::ONTOLOGY_GRAPH, false).context("ontology.ttl")?;
    load_rdf(&mut sink, SHAPES_TTL, gen::SHAPES_GRAPH, false).context("shapes.ttl")?;
    load_rdf(&mut sink, SEED_TRIG, "", true).context("seed.trig")?;
    gen::spine(plan, &mut sink);
    log(&format!("  spine: {} quads", sink.total));
    let base_ts = 1_790_000_000i64;
    let step = (plan.records / 10).max(1);
    for i in 0..plan.records {
        for iri in gen::record_iris(plan, i) {
            sink.iri(&iri);
        }
        let mut quads = Vec::with_capacity(20 + 3 * plan.chunks_per_record);
        gen::record_quads(plan, &mentions, i, base_ts + i as i64, |g, t| {
            quads.push((g.to_string(), t))
        });
        for (g, t) in quads {
            sink.quad(&g, t);
        }
        if (i + 1) % step == 0 {
            log(&format!(
                "  records: {}/{} ({} quads, {:.0}s)",
                i + 1,
                plan.records,
                sink.total,
                t0.elapsed().as_secs_f64()
            ));
        }
    }
    for pi in 0..plan.projects {
        sink.iri(&gen::project(pi));
        for k in 0..plan.decisions_in(pi) {
            sink.iri(&gen::decision(pi, k));
            sink.iri(&gen::change(pi, k));
        }
        let mut quads = Vec::new();
        gen::project_quads(plan, &mentions, pi, base_ts, |g, t| {
            quads.push((g.to_string(), t))
        });
        for (g, t) in quads {
            sink.quad(&g, t);
        }
    }
    sink.flush()?;
    report.base_quads = sink.total;
    report.base_load_secs = t0.elapsed().as_secs_f64();
    let graphs = sink.graphs.clone();
    drop(sink);
    let _ = std::fs::remove_dir_all(&tmp);
    log(&format!(
        "  base: {} quads in {:.1}s",
        report.base_quads, report.base_load_secs
    ));

    // ── Supersession on the live path (status history) ───────────────────────
    report.pre_supersession_ts = store.oracle_ts();
    let t0 = Instant::now();
    for pi in 0..plan.projects {
        let pairs = gen::supersessions(plan, pi);
        if pairs.is_empty() {
            continue;
        }
        let g = graphs[&gen::project_graph(pi)];
        let mut tx = store.begin();
        for (newer, older) in &pairs {
            tx.insert_in(
                gen::prop(
                    &gen::decision(pi, *older),
                    &gen::p("status"),
                    Value::Text("superseded".into()),
                ),
                g,
                WriteMode::Replace,
            );
            tx.insert_in(
                gen::rel(
                    &gen::decision(pi, *newer),
                    &gen::p("supersedes"),
                    &gen::decision(pi, *older),
                ),
                g,
                WriteMode::Add,
            );
        }
        tx.commit()?;
        report.supersessions += pairs.len() as u64;
    }
    report.supersession_secs = t0.elapsed().as_secs_f64();

    // ── Grants: each team's group ────────────────────────────────────────────
    let group = |t: usize| term::iri_to_node_id(&gen::team_group(t));
    let mut grant = |principal: NodeId, iri: &str, level: GraphAccessLevel| -> Result<()> {
        let g = store
            .graph_id(iri)
            .with_context(|| format!("graph {iri}"))?;
        store.grant_graph_access(principal, g, level)?;
        report.grants += 1;
        Ok(())
    };
    for t in 0..plan.teams {
        for iri in [
            gen::ORG_GRAPH,
            gen::DECISIONS_GRAPH,
            gen::ONTOLOGY_GRAPH,
            gen::SHAPES_GRAPH,
        ] {
            grant(group(t), iri, GraphAccessLevel::Read)?;
        }
        grant(group(t), &gen::records_graph(t), GraphAccessLevel::Read)?;
        grant(group(t), &gen::proposals_graph(t), GraphAccessLevel::Write)?;
    }
    for pi in 0..plan.projects {
        grant(
            group(plan.team_of_project(pi)),
            &gen::project_graph(pi),
            GraphAccessLevel::Read,
        )?;
    }

    // ── Inference: schema from the ontology graph; full materialization ──────
    let t0 = Instant::now();
    let stats = owl_rl::set_schema_graphs(store, Some(&[gen::ONTOLOGY_GRAPH.to_string()]))?;
    report.materialize_secs = t0.elapsed().as_secs_f64();
    report.inferred_quads = stats.derived_triples;
    log(&format!(
        "  inference: {} inferred quads in {:.1}s",
        report.inferred_quads, report.materialize_secs
    ));

    // ── Vectors ──────────────────────────────────────────────────────────────
    let vecs = Vectors::new(plan);
    for (space, int8) in vectors.spaces() {
        let rss0 = rss_bytes();
        let t0 = Instant::now();
        let opts = SpaceOptions {
            mode: if int8 {
                StorageMode::Mmap
            } else {
                StorageMode::Memory
            },
            int8,
        };
        let mut items = Vec::with_capacity(5_000);
        let mut n = 0u64;
        for i in 0..plan.records {
            for j in 0..plan.chunks_per_record {
                items.push((gen::node(&gen::chunk(i, j)), vecs.chunk(plan, i, j)));
                if items.len() == 5_000 {
                    n += store.batch_insert_vectors(space, &items, opts).0 as u64;
                    items.clear();
                }
            }
        }
        n += store.batch_insert_vectors(space, &items, opts).0 as u64;
        let build_secs = t0.elapsed().as_secs_f64();
        let rss_growth = match (rss0, rss_bytes()) {
            (Some(a), Some(b)) => Some(b.saturating_sub(a)),
            _ => None,
        };
        log(&format!("  vectors {space}: {n} in {build_secs:.1}s"));
        report.spaces.push(SpaceReport {
            space: space.to_string(),
            int8,
            vectors: n,
            build_secs,
            rss_growth_bytes: rss_growth,
            payload_ram_bytes: n * if int8 {
                plan.dims as u64 + 4
            } else {
                4 * plan.dims as u64
            },
        });
    }
    Ok(report)
}

/// Resident set size of this process (via `ps`; `None` where unavailable).
pub fn rss_bytes() -> Option<u64> {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()?;
    String::from_utf8(out.stdout)
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(|kb| kb * 1024)
}
