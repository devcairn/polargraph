//! Measurements against the plan's targets (`docs/design/cb-bench.md` §3).
//!
//! Everything runs in-process through `PolarGraphServer` — the same RPC
//! handlers `polargraphd` serves, including the graph ACL — so a run needs
//! no server and the numbers exclude network time. Reads run as a sampled
//! person (a member of one team's group), so every scan carries a real
//! readable-graph bitmap.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{bail, Result};
use polargraph_core::id::NodeId;
use polargraph_server::{
    convert,
    proto::{
        polar_graph_service_server::PolarGraphService, quad_ref, term::Kind as TermKind,
        value::Kind as ValueKind, ApplyChangesRequest, BatchInsertVectorsRequest,
        ExportGraphRequest, GraphTriples, QuadRef, QueryRequest, SearchVectorInSetRequest,
        SearchVectorRequest, Term, ValidateShapesRequest, VarPattern, VectorItem,
        VectorSeedQueryRequest,
    },
    service::PolarGraphServer,
};
use polargraph_storage::owl_rl;
use tokio_stream::StreamExt as _;
use tonic::Request;

use super::generate::{self as gen, Mentions, Plan, Rng, Vectors};

/// One measured operation: latency samples (ms) and its plan target.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Measurement {
    pub name: String,
    /// The plan target, e.g. "p95 ≤ 10 ms" (none for supporting numbers).
    pub target: Option<String>,
    pub n: usize,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
    /// For throughput rows (records/s); latency fields are per operation.
    pub rate_per_s: Option<f64>,
    /// Whether the target is met (none without a target).
    pub meets: Option<bool>,
    pub note: String,
}

#[derive(Clone, Copy)]
enum Goal {
    P50(f64),
    P95(f64),
    Rate(f64),
    None,
}

impl Goal {
    fn label(self) -> Option<String> {
        match self {
            Goal::P50(ms) => Some(format!("p50 ≤ {ms} ms")),
            Goal::P95(ms) => Some(format!("p95 ≤ {ms} ms")),
            Goal::Rate(r) => Some(format!("≥ {r} records/s")),
            Goal::None => None,
        }
    }
}

fn summarize(
    name: &str,
    goal: Goal,
    mut ms: Vec<f64>,
    rate: Option<f64>,
    note: String,
) -> Measurement {
    ms.sort_by(|a, b| a.total_cmp(b));
    let pct = |q: f64| -> f64 {
        if ms.is_empty() {
            return f64::NAN;
        }
        ms[((ms.len() as f64 * q).ceil() as usize).clamp(1, ms.len()) - 1]
    };
    let (p50, p95) = (pct(0.50), pct(0.95));
    let meets = match goal {
        Goal::P50(t) => Some(p50 <= t),
        Goal::P95(t) => Some(p95 <= t),
        Goal::Rate(t) => rate.map(|r| r >= t),
        Goal::None => None,
    };
    Measurement {
        name: name.to_string(),
        target: goal.label(),
        n: ms.len(),
        p50_ms: p50,
        p95_ms: p95,
        p99_ms: pct(0.99),
        max_ms: ms.last().copied().unwrap_or(f64::NAN),
        rate_per_s: rate,
        meets: if ms.is_empty() { None } else { meets },
        note,
    }
}

/// Sampling limits per scenario.
#[derive(Debug, Clone, Copy)]
pub struct Budget {
    pub samples: usize,
    pub secs: f64,
}

impl Budget {
    fn more(&self, n: usize, start: Instant) -> bool {
        n < self.samples && start.elapsed().as_secs_f64() < self.secs
    }
}

pub struct Ctx {
    pub server: PolarGraphServer,
    pub store: polargraph_storage::TripleStore,
    pub plan: Plan,
    pub mentions: Mentions,
    pub vectors: Vectors,
    pub spaces: Vec<(&'static str, bool)>,
    pub budget: Budget,
}

fn as_user<T>(mut req: Request<T>, user: &str) -> Request<T> {
    req.metadata_mut()
        .insert("x-polargraph-user-id", user.parse().expect("ascii user id"));
    req
}

fn bound(iri: &str) -> Option<Term> {
    Some(Term {
        kind: Some(TermKind::Bound(convert::node_id_to_proto(gen::node(iri)))),
    })
}
fn var(name: &str) -> Option<Term> {
    Some(Term {
        kind: Some(TermKind::Var(name.into())),
    })
}
fn pattern(s: Option<Term>, pred: &str, o: Option<Term>) -> VarPattern {
    VarPattern {
        subject: s,
        predicate: pred.into(),
        object: o,
        predicate_var: String::new(),
        graph: None,
    }
}

fn ms(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1e3
}

impl Ctx {
    /// A person who mentions-samples see: rotate through people.
    fn user(&self, rng: &mut Rng) -> String {
        gen::person(rng.below(self.plan.people))
    }

    async fn query(
        &self,
        user: &str,
        patterns: Vec<VarPattern>,
    ) -> Result<Vec<HashMap<String, NodeId>>> {
        let resp = self
            .server
            .query(as_user(
                Request::new(QueryRequest {
                    patterns,
                    ..Default::default()
                }),
                user,
            ))
            .await?
            .into_inner();
        Ok(resp
            .bindings
            .into_iter()
            .map(|b| {
                b.vars
                    .into_iter()
                    .filter_map(|(k, v)| convert::node_id_from_proto(&v).ok().map(|n| (k, n)))
                    .collect()
            })
            .collect())
    }

    /// `describe(entity)`: every fact about it — outgoing (any predicate)
    /// and incoming — in the user's visible graphs. Returns the fact count.
    async fn describe(&self, user: &str, entity: &str) -> Result<usize> {
        let mut out = pattern(bound(entity), "", var("o"));
        out.predicate_var = "p".into();
        let mut inc = pattern(var("s"), "", bound(entity));
        inc.predicate_var = "p".into();
        let a = self.server.query(as_user(
            Request::new(QueryRequest {
                patterns: vec![out],
                ..Default::default()
            }),
            user,
        ));
        let n_out = a.await?.into_inner().bindings.len();
        let n_in = self
            .server
            .query(as_user(
                Request::new(QueryRequest {
                    patterns: vec![inc],
                    ..Default::default()
                }),
                user,
            ))
            .await?
            .into_inner()
            .bindings
            .len();
        Ok(n_out + n_in)
    }

    /// The chunks of records that mention `entity`, as the user sees them.
    async fn mention_chunks(&self, user: &str, entity: &str) -> Result<Vec<NodeId>> {
        let rows = self
            .query(
                user,
                vec![
                    pattern(var("r"), &gen::p("mentions"), bound(entity)),
                    pattern(var("c"), &gen::p("chunkOf"), var("r")),
                ],
            )
            .await?;
        Ok(rows.into_iter().filter_map(|mut r| r.remove("c")).collect())
    }

    /// Hybrid search over an entity's mention set: mention → chunks query,
    /// then exact k=20 ranking in that set. Returns (top ids, set size,
    /// ms spent building the set).
    async fn hybrid_in_set(
        &self,
        user: &str,
        entity: &str,
        space: &str,
        query: Vec<f32>,
    ) -> Result<(Vec<NodeId>, usize, f64)> {
        let t = Instant::now();
        let set = self.mention_chunks(user, entity).await?;
        let set_ms = ms(t);
        let n = set.len();
        let resp = self
            .server
            .search_vector_in_set(as_user(
                Request::new(SearchVectorInSetRequest {
                    space: space.into(),
                    query,
                    k: 20,
                    node_ids: set.into_iter().map(convert::node_id_to_proto).collect(),
                    graphs: vec![],
                }),
                user,
            ))
            .await?
            .into_inner();
        let ids = resp
            .results
            .iter()
            .filter_map(|r| r.node_id.as_ref())
            .filter_map(|n| convert::node_id_from_proto(n).ok())
            .collect();
        Ok((ids, n, set_ms))
    }

    /// ANN-seeded join: k=200 candidates joined to chunks of records that
    /// mention the entity. Returns the results kept (≤ 200).
    async fn hybrid_seed_join(
        &self,
        user: &str,
        entity: &str,
        space: &str,
        query: Vec<f32>,
    ) -> Result<usize> {
        let resp = self
            .server
            .vector_seed_query(as_user(
                Request::new(VectorSeedQueryRequest {
                    space: space.into(),
                    query_vector: query,
                    k: 200,
                    seed_variable: "c".into(),
                    patterns: vec![
                        pattern(var("c"), &gen::p("chunkOf"), var("r")),
                        pattern(var("r"), &gen::p("mentions"), bound(entity)),
                    ],
                    ef: 200,
                    ..Default::default()
                }),
                user,
            ))
            .await?
            .into_inner();
        Ok(resp.bindings.len())
    }

    /// A query vector near the topic of a random record (queries land
    /// near data, as real ones do).
    fn entity_query(&self, rng: &mut Rng) -> Vec<f32> {
        let topic = self.plan.topic_of(rng.below(self.plan.records));
        self.vectors.query(topic, rng.next_u64())
    }
}

// ── Scenarios ─────────────────────────────────────────────────────────────────

/// Load one graph (~1K quads): `ExportGraph` of decision-project graphs
/// whose size is closest to 1 000 quads, read by a member of the owning team.
pub async fn load_graph(ctx: &Ctx) -> Result<Measurement> {
    let quads_of = |pi: usize| {
        let d = ctx.plan.decisions_in(pi);
        3 + d * 9 + d / 2 * 5
    };
    let mut projects: Vec<usize> = (0..ctx.plan.projects).collect();
    projects.sort_by_key(|pi| (quads_of(*pi) as i64 - 1000).abs());
    projects.truncate(8);
    let mut samples = Vec::new();
    let mut sizes = Vec::new();
    let start = Instant::now();
    let mut i = 0;
    while ctx.budget.more(samples.len(), start) {
        let pi = projects[i % projects.len()];
        i += 1;
        let team = ctx.plan.team_of_project(pi);
        let user = gen::person(team); // person `team` is in group `team`
        let t = Instant::now();
        let mut stream = ctx
            .server
            .export_graph(as_user(
                Request::new(ExportGraphRequest {
                    iri: gen::project_graph(pi),
                    all_graphs: false,
                }),
                &user,
            ))
            .await?
            .into_inner();
        let mut n = 0;
        while let Some(chunk) = stream.next().await {
            n += chunk?.quads.len();
        }
        samples.push(ms(t));
        sizes.push(n);
    }
    let avg = sizes.iter().sum::<usize>() as f64 / sizes.len().max(1) as f64;
    Ok(summarize(
        "Load one graph (ExportGraph)",
        Goal::P50(2.0),
        samples,
        None,
        format!("{avg:.0} quads per graph on average"),
    ))
}

/// `describe(entity)` for Zipf-sampled entities (hot entities dominate the
/// tail), as a sampled user.
pub async fn describe(ctx: &Ctx) -> Result<Measurement> {
    let mut rng = Rng::of(100, 0);
    let mut samples = Vec::new();
    let mut facts = Vec::new();
    let start = Instant::now();
    while ctx.budget.more(samples.len(), start) {
        let user = ctx.user(&mut rng);
        let entity = ctx.plan.entity(ctx.mentions.sample(&mut rng));
        let t = Instant::now();
        facts.push(ctx.describe(&user, &entity).await?);
        samples.push(ms(t));
    }
    facts.sort_unstable();
    let note = format!(
        "facts returned: median {}, max {}",
        facts.get(facts.len() / 2).copied().unwrap_or(0),
        facts.last().copied().unwrap_or(0)
    );
    Ok(summarize(
        "describe(entity)",
        Goal::P95(10.0),
        samples,
        None,
        note,
    ))
}

/// Hybrid search k=20 over an entity's mention set, per vector space.
pub async fn hybrid(ctx: &Ctx) -> Result<Vec<Measurement>> {
    let mut out = Vec::new();
    for (space, int8) in &ctx.spaces {
        let label = if *int8 { "int8" } else { "f32" };
        let mut rng = Rng::of(101, 0);
        let (mut samples, mut sizes, mut set_ms) = (Vec::new(), Vec::new(), Vec::new());
        let start = Instant::now();
        while ctx.budget.more(samples.len(), start) {
            let user = ctx.user(&mut rng);
            let entity = ctx.plan.entity(ctx.mentions.sample(&mut rng));
            let q = ctx.entity_query(&mut rng);
            let t = Instant::now();
            let (_, n, set) = ctx.hybrid_in_set(&user, &entity, space, q).await?;
            samples.push(ms(t));
            sizes.push(n);
            set_ms.push(set);
        }
        sizes.sort_unstable();
        set_ms.sort_by(|a, b| a.total_cmp(b));
        let set_p95 = set_ms
            .get((set_ms.len() * 95).div_ceil(100).saturating_sub(1))
            .copied()
            .unwrap_or(f64::NAN);
        out.push(summarize(
            &format!("Hybrid search k=20, mention set ({label})"),
            Goal::P95(25.0),
            samples,
            None,
            format!(
                "set size: median {}, max {}; building the set (mention query) p95 {set_p95:.0} ms, \
                 the rest is SearchVectorInSet",
                sizes.get(sizes.len() / 2).copied().unwrap_or(0),
                sizes.last().copied().unwrap_or(0)
            ),
        ));

        let mut rng = Rng::of(102, 0);
        let (mut samples, mut kept) = (Vec::new(), Vec::new());
        let start = Instant::now();
        while ctx.budget.more(samples.len(), start) {
            let user = ctx.user(&mut rng);
            let entity = ctx.plan.entity(ctx.mentions.sample(&mut rng));
            let q = ctx.entity_query(&mut rng);
            let t = Instant::now();
            kept.push(ctx.hybrid_seed_join(&user, &entity, space, q).await?);
            samples.push(ms(t));
        }
        kept.sort_unstable();
        out.push(summarize(
            &format!("Hybrid search, ANN k=200 + join ({label})"),
            Goal::None,
            samples,
            None,
            format!(
                "results kept: median {}, max {}",
                kept.get(kept.len() / 2).copied().unwrap_or(0),
                kept.last().copied().unwrap_or(0)
            ),
        ));
    }
    Ok(out)
}

/// Context assembly proxy: describe + hybrid search + fetching the top
/// chunks' text (the real assembly is application-side).
pub async fn context_assembly(ctx: &Ctx) -> Result<Option<Measurement>> {
    let Some((space, _)) = ctx.spaces.first() else {
        return Ok(None);
    };
    let mut rng = Rng::of(103, 0);
    let mut samples = Vec::new();
    let mut bytes = Vec::new();
    let start = Instant::now();
    while ctx.budget.more(samples.len(), start) {
        let user = ctx.user(&mut rng);
        let entity = ctx.plan.entity(ctx.mentions.sample(&mut rng));
        let q = ctx.entity_query(&mut rng);
        let t = Instant::now();
        ctx.describe(&user, &entity).await?;
        let (top, _, _) = ctx.hybrid_in_set(&user, &entity, space, q).await?;
        let mut text = 0usize;
        for c in top {
            let resp = ctx
                .server
                .query(as_user(
                    Request::new(QueryRequest {
                        patterns: vec![pattern(
                            Some(Term {
                                kind: Some(TermKind::Bound(convert::node_id_to_proto(c))),
                            }),
                            &gen::p("text"),
                            var("t"),
                        )],
                        ..Default::default()
                    }),
                    &user,
                ))
                .await?
                .into_inner();
            for b in resp.bindings {
                for v in b.values.values() {
                    if let Some(ValueKind::TextVal(s)) = &v.kind {
                        text += s.len();
                    }
                }
            }
        }
        samples.push(ms(t));
        bytes.push(text);
    }
    let avg = bytes.iter().sum::<usize>() as f64 / bytes.len().max(1) as f64;
    Ok(Some(summarize(
        "Context assembly proxy (describe + hybrid + chunk text)",
        Goal::P95(300.0),
        samples,
        None,
        format!("{avg:.0} bytes of chunk text (~{:.0} tokens)", avg / 4.0),
    )))
}

/// A ~500-quad proposal of decisions for team 0's first project.
fn proposal(ctx: &Ctx, round: usize) -> Vec<polargraph_core::triple::Triple> {
    let pi = (0..ctx.plan.projects)
        .find(|pi| ctx.plan.team_of_project(*pi) == 0)
        .unwrap_or(0);
    let mut out = Vec::new();
    let mut rng = Rng::of(104, round as u64);
    let mut k = 0;
    while out.len() < 500 {
        let d = format!("{}proposal-{round}-{k}", gen::ID);
        k += 1;
        out.push(gen::rel(&d, gen::RDF_TYPE, &gen::p("Decision")));
        out.push(gen::prop(
            &d,
            &gen::p("title"),
            text(gen::prose(&mut rng, 40)),
        ));
        out.push(gen::prop(
            &d,
            &gen::p("createdAt"),
            polargraph_core::value::Value::Int(1_800_000_000),
        ));
        out.push(gen::prop(&d, &gen::p("sensitivity"), text("internal")));
        out.push(gen::prop(&d, &gen::p("status"), text("proposed")));
        out.push(gen::rel(&d, &gen::p("decidedIn"), &gen::project(pi)));
        out.push(gen::prop(
            &d,
            &gen::p("rationale"),
            text(gen::prose(&mut rng, 120)),
        ));
        out.push(gen::rel(
            &d,
            &gen::p("mentions"),
            &ctx.plan.entity(ctx.mentions.sample(&mut rng)),
        ));
    }
    out
}

fn text(s: impl Into<String>) -> polargraph_core::value::Value {
    polargraph_core::value::Value::Text(s.into())
}

fn quad_ref(t: &polargraph_core::triple::Triple, graph: &str) -> QuadRef {
    use polargraph_core::triple::Triple;
    let (subject, predicate, object) = match t {
        Triple::Relation {
            subject,
            predicate,
            object,
            ..
        } => (
            *subject,
            predicate,
            quad_ref::Object::Node(convert::node_id_to_proto(*object)),
        ),
        Triple::Property {
            subject,
            predicate,
            value,
            ..
        } => (
            *subject,
            predicate,
            quad_ref::Object::Value(convert::value_to_proto(value)),
        ),
        _ => unreachable!("proposals hold relations and properties"),
    };
    QuadRef {
        subject: Some(convert::node_id_to_proto(subject)),
        predicate: predicate.0.clone(),
        object: Some(object),
        graph: graph.into(),
        all_graphs: false,
    }
}

/// Promote a 500-quad proposal: SHACL validation of the overlay, then one
/// `ApplyChanges` (adds into the approved graph, retractions from the
/// proposals graph) conditional on the proposal's read point.
pub async fn promote(ctx: &Ctx) -> Result<Measurement> {
    let proposals = gen::proposals_graph(0);
    let mut samples = Vec::new();
    let mut violations = 0usize;
    let start = Instant::now();
    let mut round = 0;
    while ctx.budget.more(samples.len(), start) {
        let triples = proposal(ctx, round);
        round += 1;
        let proto: Vec<_> = triples
            .iter()
            .filter_map(|t| convert::triple_to_proto(t, true))
            .collect();
        // The team writes its proposal (not timed).
        let read_ts = ctx
            .server
            .apply_changes(Request::new(ApplyChangesRequest {
                adds: vec![GraphTriples {
                    graph: proposals.clone(),
                    triples: proto.clone(),
                }],
                ..Default::default()
            }))
            .await?
            .into_inner()
            .commit_ts;

        let t = Instant::now();
        let report = ctx
            .server
            .validate_shapes(Request::new(ValidateShapesRequest {
                shapes_graphs: vec![gen::SHAPES_GRAPH.into()],
                data_graphs: vec![gen::DECISIONS_GRAPH.into()],
                overlay_adds: vec![GraphTriples {
                    graph: gen::DECISIONS_GRAPH.into(),
                    triples: proto.clone(),
                }],
                read_ts,
                ..Default::default()
            }))
            .await?
            .into_inner();
        violations += report.results.len();
        ctx.server
            .apply_changes(Request::new(ApplyChangesRequest {
                adds: vec![GraphTriples {
                    graph: gen::DECISIONS_GRAPH.into(),
                    triples: proto,
                }],
                retractions: triples.iter().map(|t| quad_ref(t, &proposals)).collect(),
                read_ts,
                strict: true,
                ..Default::default()
            }))
            .await?;
        samples.push(ms(t));
    }
    Ok(summarize(
        "Promote 500-quad proposal incl. SHACL",
        Goal::P95(200.0),
        samples,
        None,
        format!("{violations} SHACL results in total (expected 0)"),
    ))
}

/// Sustained ingestion: per record, one `ApplyChanges` (~80 quads) and one
/// vector batch (20 chunks), sequentially, as a service.
pub async fn ingestion(ctx: &Ctx, records: usize) -> Result<Measurement> {
    let space = ctx.spaces.first().map(|(s, _)| *s);
    let mut samples = Vec::new();
    let start = Instant::now();
    for n in 0..records {
        let i = ctx.plan.records + n; // new records past the generated ones
        let mut by_graph: HashMap<String, Vec<_>> = HashMap::new();
        gen::record_quads(
            &ctx.plan,
            &ctx.mentions,
            i,
            1_800_000_000 + n as i64,
            |g, t| {
                by_graph
                    .entry(g.to_string())
                    .or_default()
                    .extend(convert::triple_to_proto(&t, true))
            },
        );
        let iris: Vec<String> = gen::record_iris(&ctx.plan, i).collect();
        let t = Instant::now();
        ctx.server
            .apply_changes(Request::new(ApplyChangesRequest {
                adds: by_graph
                    .into_iter()
                    .map(|(graph, triples)| GraphTriples { graph, triples })
                    .collect(),
                iris,
                ..Default::default()
            }))
            .await?;
        if let Some(space) = space {
            let items = (0..ctx.plan.chunks_per_record)
                .map(|j| VectorItem {
                    node_id: Some(convert::node_id_to_proto(gen::node(&gen::chunk(i, j)))),
                    vector: ctx.vectors.chunk(&ctx.plan, i, j),
                })
                .collect();
            ctx.server
                .batch_insert_vectors(Request::new(BatchInsertVectorsRequest {
                    space: space.into(),
                    items,
                }))
                .await?;
        }
        samples.push(ms(t));
    }
    let rate = records as f64 / start.elapsed().as_secs_f64();
    Ok(summarize(
        "Sustained ingestion (ApplyChanges + 20 vectors per record)",
        Goal::Rate(50.0),
        samples,
        Some(rate),
        format!(
            "{records} records, single writer, vectors into {}",
            space.unwrap_or("no space")
        ),
    ))
}

/// Incremental materialization lag: the background inference task
/// (`--inference`) runs while decisions are written at `rate` per second;
/// lag = time from a commit to inference having covered it.
pub async fn inference_lag(ctx: &Ctx, secs: f64, rate: f64) -> Result<Measurement> {
    // Catch up first (ingestion left a backlog).
    let store = ctx.store.clone();
    tokio::task::spawn_blocking(move || owl_rl::infer_changes(&store)).await??;

    let token = tokio_util::sync::CancellationToken::new();
    ctx.server.spawn_inference_task(token.clone());
    let pending: Arc<Mutex<Vec<(i64, Instant)>>> = Arc::default();
    let lags: Arc<Mutex<Vec<f64>>> = Arc::default();
    let poller = {
        let (store, pending, lags) = (ctx.store.clone(), pending.clone(), lags.clone());
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let applied = owl_rl::inference_applied(&store).ok().flatten();
                let mut p = pending.lock().unwrap();
                if let Some(applied) = applied {
                    let now = Instant::now();
                    p.retain(|(ts, at)| {
                        if *ts <= applied.0 {
                            lags.lock().unwrap().push((now - *at).as_secs_f64() * 1e3);
                            false
                        } else {
                            true
                        }
                    });
                }
            }
        })
    };

    let pi = 0;
    let start = Instant::now();
    let mut k = 0usize;
    let period = Duration::from_secs_f64(1.0 / rate);
    while start.elapsed().as_secs_f64() < secs {
        let d = format!("{}lag-decision-{k}", gen::ID);
        k += 1;
        let triples = [
            gen::rel(&d, gen::RDF_TYPE, &gen::p("Decision")),
            gen::rel(&d, &gen::p("decidedIn"), &gen::project(pi)),
            gen::rel(&d, &gen::p("supersedes"), &gen::decision(pi, 0)),
        ];
        let commit_ts = ctx
            .server
            .apply_changes(Request::new(ApplyChangesRequest {
                adds: vec![GraphTriples {
                    graph: gen::project_graph(pi),
                    triples: triples
                        .iter()
                        .filter_map(|t| convert::triple_to_proto(t, true))
                        .collect(),
                }],
                ..Default::default()
            }))
            .await?
            .into_inner()
            .commit_ts;
        pending.lock().unwrap().push((commit_ts, Instant::now()));
        tokio::time::sleep(period).await;
    }
    // Let the last commits be covered (up to 10 s).
    let drain = Instant::now();
    while !pending.lock().unwrap().is_empty() && drain.elapsed() < Duration::from_secs(10) {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    token.cancel();
    poller.abort();
    let uncovered = pending.lock().unwrap().len();
    let samples = lags.lock().unwrap().clone();
    if samples.is_empty() {
        bail!("inference never caught up");
    }
    let mut m = summarize(
        "Incremental materialization lag",
        Goal::P95(2000.0),
        samples,
        None,
        format!("{k} commits at {rate}/s over {secs:.0} s; {uncovered} not covered within 10 s"),
    );
    m.target = Some("≤ 2 s (p95)".into());
    Ok(m)
}

/// Time travel: a superseded decision's status as of the load (before the
/// supersession pass) vs now. Supporting number, no plan target.
pub async fn time_travel(ctx: &Ctx, as_of_tx: i64) -> Result<Measurement> {
    let pairs: Vec<(usize, usize)> = (0..ctx.plan.projects)
        .flat_map(|pi| {
            gen::supersessions(&ctx.plan, pi)
                .into_iter()
                .map(move |(_, old)| (pi, old))
        })
        .take(200)
        .collect();
    if pairs.is_empty() {
        return Ok(summarize(
            "Time travel",
            Goal::None,
            vec![],
            None,
            "no supersessions".into(),
        ));
    }
    let mut samples = Vec::new();
    let mut changed = 0;
    let start = Instant::now();
    let mut i = 0;
    while ctx.budget.more(samples.len(), start) {
        let (pi, k) = pairs[i % pairs.len()];
        i += 1;
        let user = gen::person(ctx.plan.team_of_project(pi));
        let t = Instant::now();
        let mut req = QueryRequest {
            patterns: vec![pattern(
                bound(&gen::decision(pi, k)),
                &gen::p("status"),
                var("s"),
            )],
            as_of_tx_time: as_of_tx,
            ..Default::default()
        };
        let then = ctx
            .server
            .query(as_user(Request::new(req.clone()), &user))
            .await?
            .into_inner();
        req.as_of_tx_time = 0;
        let now = ctx
            .server
            .query(as_user(Request::new(req), &user))
            .await?
            .into_inner();
        samples.push(ms(t));
        if then.bindings.first().map(|b| &b.values) != now.bindings.first().map(|b| &b.values) {
            changed += 1;
        }
    }
    let n = samples.len();
    Ok(summarize(
        "Time travel: decision status as of load vs now",
        Goal::None,
        samples,
        None,
        format!("{changed}/{n} differed (expected all)"),
    ))
}

/// ANN recall@20 against exact search, per space and ef.
pub async fn recall(ctx: &Ctx, queries: usize) -> Result<Vec<(String, f64)>> {
    if queries == 0 || ctx.spaces.is_empty() {
        return Ok(vec![]);
    }
    let mut rng = Rng::of(105, 0);
    let qs: Vec<Vec<f32>> = (0..queries)
        .map(|i| {
            let topic = ctx.plan.topic_of(rng.below(ctx.plan.records));
            ctx.vectors.query(topic, 9_000 + i as u64)
        })
        .collect();
    // Exact top-20 per query, regenerating the chunk vectors.
    let mut best: Vec<Vec<(f32, NodeId)>> = vec![Vec::new(); queries];
    for r in 0..ctx.plan.records {
        for j in 0..ctx.plan.chunks_per_record {
            let v = ctx.vectors.chunk(&ctx.plan, r, j);
            let id = gen::node(&gen::chunk(r, j));
            for (q, top) in qs.iter().zip(best.iter_mut()) {
                let s: f32 = q.iter().zip(&v).map(|(a, b)| a * b).sum();
                if top.len() < 20 || s > top[top.len() - 1].0 {
                    let at = top.partition_point(|(x, _)| *x > s);
                    top.insert(at, (s, id));
                    top.truncate(20);
                }
            }
        }
    }
    let mut out = Vec::new();
    for (space, _) in &ctx.spaces {
        for ef in [100u32, 400] {
            let mut hit = 0usize;
            for (q, exact) in qs.iter().zip(&best) {
                let resp = ctx
                    .server
                    .search_vector(Request::new(SearchVectorRequest {
                        query: q.clone(),
                        k: 20,
                        space: (*space).into(),
                        ef,
                        graphs: vec![],
                    }))
                    .await?
                    .into_inner();
                let got: std::collections::HashSet<NodeId> = resp
                    .results
                    .iter()
                    .filter_map(|r| r.node_id.as_ref())
                    .filter_map(|n| convert::node_id_from_proto(n).ok())
                    .collect();
                hit += exact.iter().filter(|(_, id)| got.contains(id)).count();
            }
            out.push((
                format!("{space} ef={ef}"),
                hit as f64 / (20 * queries) as f64,
            ));
        }
    }
    Ok(out)
}
