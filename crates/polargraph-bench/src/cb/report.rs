//! The run report: JSON (for tracking) and a Markdown table (for
//! `BENCHMARKS.md` and the CI job summary).

use super::{load::LoadReport, measure::Measurement};

#[derive(Debug, Clone, serde::Serialize)]
pub struct Report {
    pub scale: f64,
    pub quads: u64,
    pub chunks: u64,
    pub load: LoadReport,
    pub measurements: Vec<Measurement>,
    /// ("space ef=N", recall@20).
    pub recall: Vec<(String, f64)>,
    pub disk_bytes: u64,
    pub bytes_per_quad: f64,
    /// Column families by size after compaction (RocksDB estimate).
    pub cf_sizes: Vec<(String, u64)>,
    /// `.vecs` files (mmap / int8 full vectors).
    pub vectors_dir_bytes: u64,
    pub host: String,
}

fn fmt_ms(v: f64) -> String {
    if v.is_nan() {
        "—".into()
    } else if v < 10.0 {
        format!("{v:.2}")
    } else {
        format!("{v:.0}")
    }
}

fn mib(b: u64) -> String {
    format!("{:.1} MiB", b as f64 / (1024.0 * 1024.0))
}

/// The plan's performance targets (`docs/contxtbroker-platform-plan.md`,
/// "Performance targets") and the measurement that answers each.
const PLAN_TARGETS: [(&str, &str); 7] = [
    ("Load one graph (1K quads)", "Load one graph"),
    ("describe(entity) across visible graphs", "describe(entity)"),
    ("Hybrid search, k=20", "Hybrid search k=20"),
    ("Context assembly, 4K tokens", "Context assembly"),
    (
        "Promote a 500-quad proposal incl. SHACL",
        "Promote 500-quad",
    ),
    (
        "Incremental materialization lag",
        "Incremental materialization lag",
    ),
    ("Sustained ingestion", "Sustained ingestion"),
];

impl Report {
    /// Per plan target: "met", "missed" or "unmeasured", with the rows.
    pub fn target_status(&self) -> Vec<(&'static str, &'static str, Vec<&Measurement>)> {
        PLAN_TARGETS
            .iter()
            .map(|(target, prefix)| {
                let rows: Vec<&Measurement> = self
                    .measurements
                    .iter()
                    .filter(|m| m.name.starts_with(prefix) && m.target.is_some())
                    .collect();
                let status = if rows.is_empty() || rows.iter().any(|m| m.meets.is_none()) {
                    "unmeasured"
                } else if rows.iter().all(|m| m.meets == Some(true)) {
                    "met"
                } else {
                    "missed"
                };
                (*target, status, rows)
            })
            .collect()
    }

    pub fn markdown(&self) -> String {
        let mut s = format!(
            "### cb-bench — scale {} ({} base quads, {} inferred, {} chunks)\n\n\
             Host: {}. In-process (no network); reads as a team member with the graph ACL.\n\n\
             {}\n\
             | Measurement | Target | n | p50 ms | p95 ms | p99 ms | max ms | Rate | Meets | Notes |\n\
             |---|---|---:|---:|---:|---:|---:|---:|:-:|---|\n",
            self.scale,
            self.load.base_quads,
            self.load.inferred_quads,
            self.chunks,
            self.host,
            self.targets_markdown(),
        );
        for m in &self.measurements {
            s.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
                m.name,
                m.target.as_deref().unwrap_or(""),
                m.n,
                fmt_ms(m.p50_ms),
                fmt_ms(m.p95_ms),
                fmt_ms(m.p99_ms),
                fmt_ms(m.max_ms),
                m.rate_per_s
                    .map(|r| format!("{r:.0}/s"))
                    .unwrap_or_default(),
                match m.meets {
                    Some(true) => "✅",
                    Some(false) => "❌",
                    None => "",
                },
                m.note,
            ));
        }
        s.push_str("\n| Load / capacity | Value |\n|---|---|\n");
        let l = &self.load;
        s.push_str(&format!(
            "| Base load (SST import) | {} quads in {:.1} s ({:.0} quads/s) |\n",
            l.base_quads,
            l.base_load_secs,
            l.base_quads as f64 / l.base_load_secs.max(1e-9)
        ));
        s.push_str(&format!(
            "| Supersession pass (live writes) | {} status changes in {:.1} s |\n",
            l.supersessions, l.supersession_secs
        ));
        s.push_str(&format!("| Graph grants | {} |\n", l.grants));
        s.push_str(&format!(
            "| Full materialization | {} inferred quads in {:.1} s |\n",
            l.inferred_quads, l.materialize_secs
        ));
        for sp in &l.spaces {
            s.push_str(&format!(
                "| Vector space `{}` ({}) | {} vectors in {:.1} s; vector RAM {}; RSS growth {} |\n",
                sp.space,
                if sp.int8 { "int8" } else { "f32" },
                sp.vectors,
                sp.build_secs,
                mib(sp.payload_ram_bytes),
                sp.rss_growth_bytes.map(mib).unwrap_or_else(|| "n/a".into()),
            ));
        }
        for (space, r) in &self.recall {
            s.push_str(&format!("| Recall@20 `{space}` | {r:.3} |\n"));
        }
        s.push_str(&format!(
            "| Disk (after compaction) | {} ({:.0} bytes per quad, incl. inferred quads, blobs and vectors) |\n",
            mib(self.disk_bytes),
            self.bytes_per_quad
        ));
        let by_cf: Vec<String> = self
            .cf_sizes
            .iter()
            .take(8)
            .map(|(cf, b)| format!("`{cf}` {}", mib(*b)))
            .collect();
        s.push_str(&format!(
            "| Disk by column family | {}; `.vecs` files {} |\n",
            by_cf.join(", "),
            mib(self.vectors_dir_bytes)
        ));
        // The plan's team-scenario estimates, scaled linearly.
        s.push_str(&format!(
            "| Disk vs plan estimate (~9 GB × scale) | {:.1} GB vs ~{:.1} GB |\n",
            self.disk_bytes as f64 / 1e9,
            9.0 * self.scale
        ));
        if let Some(sp) = l.spaces.iter().find(|sp| !sp.int8) {
            if let Some(rss) = sp.rss_growth_bytes {
                s.push_str(&format!(
                    "| HNSW RAM, memory mode, vs plan (~4 GB × scale) | {:.2} GB vs ~{:.2} GB |\n",
                    rss as f64 / 1e9,
                    4.0 * self.scale
                ));
            }
        }
        s
    }

    fn targets_markdown(&self) -> String {
        let mut s = String::from("| Plan target | Status | Measured |\n|---|:-:|---|\n");
        for (target, status, rows) in self.target_status() {
            let measured = rows
                .iter()
                .map(|m| match m.rate_per_s {
                    Some(r) => format!("{r:.0}/s"),
                    None if m.target.as_deref().is_some_and(|t| t.starts_with("p50")) => {
                        format!("p50 {} ms", fmt_ms(m.p50_ms))
                    }
                    None => format!("p95 {} ms", fmt_ms(m.p95_ms)),
                })
                .zip(&rows)
                .map(|(v, m)| match m.name.rfind('(') {
                    // Variants of one target (f32 / int8) are labelled.
                    Some(i) if rows.len() > 1 => format!("{} {v}", &m.name[i..]),
                    _ => v,
                })
                .collect::<Vec<_>>()
                .join("; ");
            let mark = match status {
                "met" => "✅ met",
                "missed" => "❌ missed",
                _ => "— unmeasured",
            };
            s.push_str(&format!("| {target} | {mark} | {measured} |\n"));
        }
        s
    }
}

/// Total size of the files under `dir`.
pub fn dir_size(dir: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|e| match e.metadata() {
            Ok(m) if m.is_dir() => dir_size(&e.path()),
            Ok(m) => m.len(),
            Err(_) => 0,
        })
        .sum()
}
