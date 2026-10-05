//! Synthetic company generator (`docs/design/cb-bench.md` §3).
//!
//! Deterministic: every item is derived from its kind and index, so a run
//! at a given scale always produces the same graph, and vectors can be
//! regenerated for exact recall checks without keeping them in memory.
//! Instances follow the hand-built seed (`seed/seed.trig`): the same
//! predicates, classes and graph layout, multiplied.

use polargraph_core::{
    id::NodeId,
    temporal::{BiTemporalRange, Timestamp},
    term,
    triple::{Predicate, Triple},
    value::Value,
};

pub const NS: &str = "https://cb.example/ont#";
pub const ID: &str = "https://cb.example/id/";
pub const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

pub const ORG_GRAPH: &str = "urn:cb:approved:org";
pub const DECISIONS_GRAPH: &str = "urn:cb:approved:decisions";
pub const ONTOLOGY_GRAPH: &str = "urn:cb:ontology";
pub const SHAPES_GRAPH: &str = "urn:cb:shapes";

/// Vector spaces: the same chunk vectors, f32 and int8.
pub const SPACE_F32: &str = "cb_chunks_f32";
pub const SPACE_INT8: &str = "cb_chunks_int8";

/// `cb:<local>`.
pub fn p(local: &str) -> String {
    format!("{NS}{local}")
}

pub fn records_graph(team: usize) -> String {
    format!("urn:cb:records:team-{team}")
}
pub fn proposals_graph(team: usize) -> String {
    format!("urn:cb:proposals:team-{team}")
}
pub fn project_graph(project: usize) -> String {
    format!("urn:cb:project:{project}")
}
pub fn team_group(team: usize) -> String {
    format!("urn:cb:group:team-{team}")
}

pub fn person(i: usize) -> String {
    format!("{ID}person-{i}")
}
pub fn team(i: usize) -> String {
    format!("{ID}team-{i}")
}
pub fn service(i: usize) -> String {
    format!("{ID}service-{i}")
}
pub fn customer(i: usize) -> String {
    format!("{ID}customer-{i}")
}
pub fn record(i: usize) -> String {
    format!("{ID}record-{i}")
}
pub fn chunk(record: usize, j: usize) -> String {
    format!("{ID}record-{record}-c{j}")
}
pub fn project(i: usize) -> String {
    format!("{ID}project-{i}")
}
pub fn decision(project: usize, k: usize) -> String {
    format!("{ID}decision-{project}-{k}")
}
pub fn change(project: usize, k: usize) -> String {
    format!("{ID}change-{project}-{k}")
}

pub fn node(iri: &str) -> NodeId {
    term::iri_to_node_id(iri)
}

// ── Scale ─────────────────────────────────────────────────────────────────────

/// Entity and record counts for a scale factor. `SF = 1` ≈ a team-scale
/// deployment (~10M quads, 2M chunk vectors); counts grow linearly.
#[derive(Debug, Clone)]
pub struct Plan {
    pub scale: f64,
    pub people: usize,
    pub teams: usize,
    pub services: usize,
    pub customers: usize,
    pub records: usize,
    pub chunks_per_record: usize,
    pub mentions_per_record: usize,
    pub projects: usize,
    pub dims: usize,
    pub clusters: usize,
    /// Zipf exponent of entity mentions.
    pub zipf_s: f64,
}

impl Plan {
    pub fn new(scale: f64) -> Self {
        let n = |base: f64, min: usize| ((base * scale).round() as usize).max(min);
        Self {
            scale,
            people: n(300.0, 20),
            teams: n(15.0, 3),
            services: n(120.0, 10),
            customers: n(400.0, 10),
            records: n(100_000.0, 200),
            chunks_per_record: 20,
            mentions_per_record: 5,
            projects: n(200.0, 5),
            dims: 384,
            clusters: 256,
            zipf_s: 1.1,
        }
    }

    pub fn chunks(&self) -> usize {
        self.records * self.chunks_per_record
    }

    /// Mentionable entities: people, then services, then customers.
    pub fn entities(&self) -> usize {
        self.people + self.services + self.customers
    }

    pub fn entity(&self, i: usize) -> String {
        if i < self.people {
            person(i)
        } else if i < self.people + self.services {
            service(i - self.people)
        } else {
            customer(i - self.people - self.services)
        }
    }

    pub fn team_of_person(&self, person: usize) -> usize {
        person % self.teams
    }

    pub fn team_of_project(&self, project: usize) -> usize {
        project % self.teams
    }

    /// Records are written by people round-robin over a shuffled order.
    pub fn author_of(&self, record: usize) -> usize {
        Rng::of(1, record as u64).below(self.people)
    }

    /// Decisions in a project: Zipf-sized by project rank (a few big
    /// projects, a long tail of small ones).
    pub fn decisions_in(&self, project: usize) -> usize {
        ((300.0 / ((project + 1) as f64).powf(0.7)).round() as usize).max(5)
    }

    /// The record's topic cluster (its chunks' vectors cluster around it).
    pub fn topic_of(&self, record: usize) -> usize {
        Rng::of(2, record as u64).below(self.clusters)
    }
}

// ── Randomness ────────────────────────────────────────────────────────────────

/// SplitMix64: small, fast, deterministic.
#[derive(Clone)]
pub struct Rng(u64);

impl Rng {
    /// A generator for item `index` of stream `kind`.
    pub fn of(kind: u64, index: u64) -> Self {
        let mut r = Rng(kind.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ index);
        r.next_u64();
        r
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, 1)`.
    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    pub fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n.max(1) as u64) as usize
    }

    /// Standard normal (Box–Muller).
    pub fn normal(&mut self) -> f64 {
        let u1 = self.unit().max(f64::MIN_POSITIVE);
        let u2 = self.unit();
        (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
    }
}

/// Zipf sampler over ranks `0..n` with exponent `s`.
pub struct Zipf {
    cdf: Vec<f64>,
}

impl Zipf {
    pub fn new(n: usize, s: f64) -> Self {
        let mut cdf = Vec::with_capacity(n);
        let mut acc = 0.0;
        for k in 1..=n {
            acc += 1.0 / (k as f64).powf(s);
            cdf.push(acc);
        }
        for c in &mut cdf {
            *c /= acc;
        }
        Zipf { cdf }
    }

    pub fn sample(&self, rng: &mut Rng) -> usize {
        let u = rng.unit();
        self.cdf.partition_point(|c| *c < u).min(self.cdf.len() - 1)
    }

    /// Probability of rank `k`.
    #[cfg(test)]
    pub fn weight(&self, k: usize) -> f64 {
        self.cdf[k] - if k == 0 { 0.0 } else { self.cdf[k - 1] }
    }
}

// ── Vectors ───────────────────────────────────────────────────────────────────

/// Clustered unit vectors: each record's chunks sit around its topic's
/// centroid, so nearest-neighbour recall is meaningful.
pub struct Vectors {
    pub dims: usize,
    centroids: Vec<Vec<f32>>,
}

impl Vectors {
    pub fn new(plan: &Plan) -> Self {
        let centroids = (0..plan.clusters)
            .map(|c| unit_vector(&mut Rng::of(3, c as u64), plan.dims, None, 1.0))
            .collect();
        Vectors {
            dims: plan.dims,
            centroids,
        }
    }

    pub fn chunk(&self, plan: &Plan, record: usize, j: usize) -> Vec<f32> {
        let c = &self.centroids[plan.topic_of(record)];
        let mut rng = Rng::of(4, (record * plan.chunks_per_record + j) as u64);
        unit_vector(&mut rng, self.dims, Some(c), 0.9)
    }

    /// A query near cluster `c`.
    pub fn query(&self, c: usize, seed: u64) -> Vec<f32> {
        let c = &self.centroids[c % self.centroids.len()];
        unit_vector(&mut Rng::of(5, seed), self.dims, Some(c), 0.9)
    }
}

/// `normalize(center + noise)`, where the noise has norm ≈ `spread`.
fn unit_vector(rng: &mut Rng, dims: usize, center: Option<&[f32]>, spread: f64) -> Vec<f32> {
    let sigma = spread / (dims as f64).sqrt();
    let mut v: Vec<f32> = (0..dims)
        .map(|i| (center.map_or(0.0, |c| c[i] as f64) + sigma * rng.normal()) as f32)
        .collect();
    let norm = v
        .iter()
        .map(|x| x * x)
        .sum::<f32>()
        .sqrt()
        .max(f32::MIN_POSITIVE);
    for x in &mut v {
        *x /= norm;
    }
    v
}

// ── Triples ───────────────────────────────────────────────────────────────────

/// Receives the generated quads (graph IRI, "" = default graph) and the IRIs
/// to record in the dictionary.
pub trait Sink {
    fn quad(&mut self, graph: &str, triple: Triple);
    fn iri(&mut self, iri: &str);
}

fn temporal() -> BiTemporalRange {
    BiTemporalRange::assert_now(Timestamp::now())
}

pub fn rel(s: &str, pred: &str, o: &str) -> Triple {
    Triple::Relation {
        subject: node(s),
        predicate: Predicate::new(pred),
        object: node(o),
        edge_id: term::edge_id_for(s, pred, o),
        temporal: temporal(),
    }
}

pub fn prop(s: &str, pred: &str, value: Value) -> Triple {
    Triple::Property {
        subject: node(s),
        predicate: Predicate::new(pred),
        value,
        temporal: temporal(),
    }
}

fn text(s: impl Into<String>) -> Value {
    Value::Text(s.into())
}

const WORDS: [&str; 64] = [
    "ledger",
    "gate",
    "latency",
    "budget",
    "rollout",
    "schema",
    "customer",
    "invoice",
    "retry",
    "queue",
    "shard",
    "cache",
    "token",
    "quota",
    "deploy",
    "incident",
    "review",
    "owner",
    "contract",
    "renewal",
    "migration",
    "index",
    "replica",
    "backup",
    "alert",
    "metric",
    "trace",
    "region",
    "tenant",
    "billing",
    "refund",
    "export",
    "import",
    "policy",
    "access",
    "audit",
    "service",
    "pipeline",
    "release",
    "feature",
    "flag",
    "pricing",
    "support",
    "escalation",
    "runbook",
    "capacity",
    "storage",
    "network",
    "partner",
    "roadmap",
    "decision",
    "proposal",
    "risk",
    "estimate",
    "milestone",
    "dependency",
    "interface",
    "payload",
    "version",
    "graph",
    "vector",
    "search",
    "summary",
    "context",
];

/// About `bytes` bytes of filler prose.
pub fn prose(rng: &mut Rng, bytes: usize) -> String {
    let mut s = String::with_capacity(bytes + 16);
    while s.len() < bytes {
        if !s.is_empty() {
            s.push(' ');
        }
        s.push_str(WORDS[rng.below(WORDS.len())]);
    }
    s
}

/// The spine: org, teams, people, services, customers (`ORG_GRAPH`), and
/// team groups with their members (default graph, access control).
pub fn spine(plan: &Plan, sink: &mut impl Sink) {
    let org = format!("{ID}org");
    for iri in std::iter::once(org.clone())
        .chain((0..plan.teams).map(team))
        .chain((0..plan.entities()).map(|i| plan.entity(i)))
    {
        sink.iri(&iri);
    }
    let mut emit = |t: Triple| sink.quad(ORG_GRAPH, t);
    let ty = |emit: &mut dyn FnMut(Triple), s: &str, class: &str| {
        emit(rel(s, RDF_TYPE, &p(class)));
    };
    ty(&mut emit, &org, "Org");
    emit(prop(&org, &p("name"), text("Example Co")));
    for t in 0..plan.teams {
        let s = team(t);
        ty(&mut emit, &s, "Team");
        emit(prop(&s, &p("name"), text(format!("Team {t}"))));
        emit(rel(&s, &p("teamOf"), &org));
    }
    for i in 0..plan.people {
        let s = person(i);
        let mut rng = Rng::of(6, i as u64);
        ty(&mut emit, &s, "Person");
        emit(prop(&s, &p("name"), text(format!("Person {i}"))));
        emit(prop(
            &s,
            &p("role"),
            text(["engineer", "manager", "analyst", "designer"][rng.below(4)]),
        ));
        emit(rel(&s, &p("memberOf"), &team(plan.team_of_person(i))));
    }
    for i in 0..plan.services {
        let s = service(i);
        let mut rng = Rng::of(7, i as u64);
        ty(&mut emit, &s, "Service");
        emit(prop(&s, &p("name"), text(format!("svc-{i}"))));
        emit(prop(&s, &p("tier"), Value::Int(1 + rng.below(3) as i64)));
        emit(rel(&team(i % plan.teams), &p("owns"), &s));
        for _ in 0..2 {
            let d = rng.below(plan.services);
            if d != i {
                emit(rel(&s, &p("dependsOn"), &service(d)));
            }
        }
    }
    for i in 0..plan.customers {
        let s = customer(i);
        let mut rng = Rng::of(8, i as u64);
        ty(&mut emit, &s, "Customer");
        emit(prop(&s, &p("name"), text(format!("Customer {i}"))));
        emit(prop(
            &s,
            &p("segment"),
            text(["enterprise", "midmarket", "smb"][rng.below(3)]),
        ));
        for _ in 0..1 + rng.below(3) {
            emit(rel(&service(rng.below(plan.services)), &p("serves"), &s));
        }
    }
    // Access control: one group per team; people are members of their team's.
    for i in 0..plan.people {
        sink.quad(
            "",
            Triple::Relation {
                subject: node(&person(i)),
                predicate: Predicate::new(polargraph_core::schema::BUILTIN_MEMBER_OF_PRED),
                object: node(&team_group(plan.team_of_person(i))),
                edge_id: polargraph_core::id::EdgeId::new(),
                temporal: temporal(),
            },
        );
    }
}

/// Mentions, ranked by Zipf popularity over a fixed shuffle of the entities
/// (so the hottest entity isn't always person 0).
pub struct Mentions {
    zipf: Zipf,
    order: Vec<usize>,
}

impl Mentions {
    pub fn new(plan: &Plan) -> Self {
        let n = plan.entities();
        let mut order: Vec<usize> = (0..n).collect();
        let mut rng = Rng::of(9, 0);
        for i in (1..n).rev() {
            order.swap(i, rng.below(i + 1));
        }
        Mentions {
            zipf: Zipf::new(n, plan.zipf_s),
            order,
        }
    }

    pub fn sample(&self, rng: &mut Rng) -> usize {
        self.order[self.zipf.sample(rng)]
    }
}

/// The record envelope (~20 quads) and its chunks (3 quads each). Returns
/// the record's graph.
pub fn record_quads(
    plan: &Plan,
    mentions: &Mentions,
    i: usize,
    created_at: i64,
    mut emit: impl FnMut(&str, Triple),
) -> String {
    let author = plan.author_of(i);
    let team_ix = plan.team_of_person(author);
    let g = records_graph(team_ix);
    let s = record(i);
    let mut rng = Rng::of(10, i as u64);
    let kind = ["Document", "Ticket", "Thread"][i % 3];
    let mut e = |t: Triple| emit(&g, t);
    e(rel(&s, RDF_TYPE, &p(kind)));
    e(prop(
        &s,
        &p("title"),
        text(format!("Record {i}: {}", prose(&mut rng, 40))),
    ));
    e(prop(&s, &p("createdAt"), Value::Int(created_at)));
    e(rel(&s, &p("author"), &person(author)));
    e(rel(&s, &p("team"), &team(team_ix)));
    e(prop(
        &s,
        &p("sensitivity"),
        text(["public", "internal", "confidential"][rng.below(3)]),
    ));
    e(prop(
        &s,
        &p("source"),
        text(["wiki", "tickets", "chat", "email"][rng.below(4)]),
    ));
    e(prop(
        &s,
        &p("url"),
        text(format!("https://records.cb.example/{i}")),
    ));
    e(prop(&s, &p("lang"), text("en")));
    e(prop(&s, &p("summary"), text(prose(&mut rng, 150))));
    e(prop(
        &s,
        &p("wordCount"),
        Value::Int(200 + rng.below(4000) as i64),
    ));
    e(prop(&s, &p("topic"), Value::Int(plan.topic_of(i) as i64)));
    for _ in 0..3 {
        e(prop(&s, &p("tag"), text(WORDS[rng.below(WORDS.len())])));
    }
    let mut seen = Vec::with_capacity(plan.mentions_per_record);
    for _ in 0..plan.mentions_per_record {
        let m = mentions.sample(&mut rng);
        if !seen.contains(&m) {
            seen.push(m);
            e(rel(&s, &p("mentions"), &plan.entity(m)));
        }
    }
    for j in 0..plan.chunks_per_record {
        let c = chunk(i, j);
        let mut crng = Rng::of(11, (i * plan.chunks_per_record + j) as u64);
        e(rel(&c, &p("chunkOf"), &s));
        e(prop(&c, &p("position"), Value::Int(j as i64)));
        // Over `inline_value_max_bytes`: stored once in the blob CF.
        e(prop(&c, &p("text"), text(prose(&mut crng, 700))));
    }
    g
}

/// IRIs a record names (for the dictionary).
pub fn record_iris(plan: &Plan, i: usize) -> impl Iterator<Item = String> + '_ {
    std::iter::once(record(i)).chain((0..plan.chunks_per_record).map(move |j| chunk(i, j)))
}

/// A decision project (its own graph): decisions (all `accepted` at load;
/// supersession is applied afterwards on the live path, so status changes
/// have history) and changes implementing some of them.
pub fn project_quads(
    plan: &Plan,
    mentions: &Mentions,
    pi: usize,
    created_at: i64,
    mut emit: impl FnMut(&str, Triple),
) {
    let g = project_graph(pi);
    let pr = project(pi);
    let mut rng = Rng::of(12, pi as u64);
    let mut e = |t: Triple| emit(&g, t);
    e(rel(&pr, RDF_TYPE, &p("Project")));
    e(prop(&pr, &p("name"), text(format!("Project {pi}"))));
    e(rel(&pr, &p("ownedByTeam"), &team(plan.team_of_project(pi))));
    for k in 0..plan.decisions_in(pi) {
        let d = decision(pi, k);
        e(rel(&d, RDF_TYPE, &p("Decision")));
        e(prop(
            &d,
            &p("title"),
            text(format!("Decision {pi}.{k}: {}", prose(&mut rng, 30))),
        ));
        e(prop(&d, &p("createdAt"), Value::Int(created_at + k as i64)));
        e(prop(&d, &p("sensitivity"), text("internal")));
        e(prop(&d, &p("status"), text("accepted")));
        e(rel(&d, &p("decidedIn"), &pr));
        e(prop(&d, &p("rationale"), text(prose(&mut rng, 120))));
        for _ in 0..2 {
            e(rel(
                &d,
                &p("mentions"),
                &plan.entity(mentions.sample(&mut rng)),
            ));
        }
        if k % 2 == 1 {
            let c = change(pi, k);
            e(rel(&c, RDF_TYPE, &p("Change")));
            e(prop(&c, &p("title"), text(format!("Change {pi}.{k}"))));
            e(prop(&c, &p("createdAt"), Value::Int(created_at + k as i64)));
            e(prop(&c, &p("sensitivity"), text("internal")));
            e(rel(&c, &p("implements"), &d));
        }
    }
}

/// Supersession chains: decision `k` supersedes `k - 1` with probability
/// 0.3 (so chain lengths vary). Returns `(superseding, superseded)` pairs.
pub fn supersessions(plan: &Plan, pi: usize) -> Vec<(usize, usize)> {
    let mut rng = Rng::of(13, pi as u64);
    (1..plan.decisions_in(pi))
        .filter(|_| rng.unit() < 0.3)
        .map(|k| (k, k - 1))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zipf_is_skewed_and_normalized() {
        let z = Zipf::new(1000, 1.1);
        let total: f64 = (0..1000).map(|k| z.weight(k)).sum();
        assert!((total - 1.0).abs() < 1e-9);
        assert!(z.weight(0) > 50.0 * z.weight(999));
    }

    #[test]
    fn generation_is_deterministic() {
        let plan = Plan::new(0.001);
        let m = Mentions::new(&plan);
        let collect = || {
            let mut out = Vec::new();
            record_quads(&plan, &m, 7, 1, |g, t| {
                out.push((g.to_string(), format!("{t:?}")))
            });
            out
        };
        let (a, b) = (collect(), collect());
        // Temporal stamps differ; compare graph and quad count.
        assert_eq!(a.len(), b.len());
        assert!(a.len() >= 20 + 3 * plan.chunks_per_record - 5);
        let v = Vectors::new(&plan);
        assert_eq!(v.chunk(&plan, 3, 4), v.chunk(&plan, 3, 4));
        let norm: f32 = v.chunk(&plan, 3, 4).iter().map(|x| x * x).sum();
        assert!((norm - 1.0).abs() < 1e-4);
    }
}
