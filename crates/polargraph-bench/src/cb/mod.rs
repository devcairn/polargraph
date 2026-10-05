//! `polargraph-bench cb` — the ContxtBroker engine benchmark
//! (`docs/design/cb-bench.md`): a synthetic company generated from the
//! hand-built seed at a scale factor, bulk-loaded, then measured against
//! the plan's performance targets.

pub mod generate;
pub mod load;
pub mod measure;
pub mod report;

use std::{path::PathBuf, time::Instant};

use anyhow::{Context, Result};
use polargraph_server::service::PolarGraphServer;
use polargraph_storage::TripleStore;

use generate::{Mentions, Plan, Vectors};
use load::VectorSpaces;
use measure::{Budget, Ctx};

#[derive(clap::Args, Debug)]
pub struct CbArgs {
    /// Scale factor: 1 ≈ team (~10M quads, 2M chunk vectors), 20 ≈ company.
    #[arg(long, default_value_t = 0.05)]
    pub scale: f64,
    /// Data directory (must be empty or absent; a temp dir if omitted).
    #[arg(long)]
    pub data_dir: Option<PathBuf>,
    /// Vector spaces to build and measure.
    #[arg(long, value_enum, default_value_t = VectorSpaces::Both)]
    pub vectors: VectorSpaces,
    /// Quads per SST import batch.
    #[arg(long, default_value_t = 1_000_000)]
    pub batch: usize,
    /// Samples per latency measurement.
    #[arg(long, default_value_t = 200)]
    pub samples: usize,
    /// Time budget per latency measurement (seconds).
    #[arg(long, default_value_t = 60.0)]
    pub budget_secs: f64,
    /// Records written in the ingestion measurement.
    #[arg(long, default_value_t = 500)]
    pub ingest_records: usize,
    /// Duration of the inference-lag measurement (seconds).
    #[arg(long, default_value_t = 20.0)]
    pub lag_secs: f64,
    /// Commits per second during the inference-lag measurement.
    #[arg(long, default_value_t = 20.0)]
    pub lag_rate: f64,
    /// Queries for the recall check (0 = skip).
    #[arg(long, default_value_t = 20)]
    pub recall_queries: usize,
    /// Write the JSON report here.
    #[arg(long)]
    pub json: Option<PathBuf>,
    /// Write the Markdown report here (e.g. `$GITHUB_STEP_SUMMARY`; appended).
    #[arg(long)]
    pub markdown: Option<PathBuf>,
}

pub async fn run(args: &CbArgs) -> Result<()> {
    let plan = Plan::new(args.scale);
    let _tmp;
    let dir = match &args.data_dir {
        Some(d) => {
            if d.exists() && std::fs::read_dir(d)?.next().is_some() {
                anyhow::bail!("{} is not empty", d.display());
            }
            d.clone()
        }
        None => {
            _tmp = tempfile::TempDir::new()?;
            _tmp.path().to_path_buf()
        }
    };
    println!(
        "cb-bench scale {}: {} people, {} teams, {} services, {} customers, \
         {} records ({} chunks), {} projects",
        plan.scale,
        plan.people,
        plan.teams,
        plan.services,
        plan.customers,
        plan.records,
        plan.chunks(),
        plan.projects
    );
    let store = TripleStore::open(&dir).context("open store")?;
    let t0 = Instant::now();
    let loaded = load::load(&store, &dir, &plan, args.vectors, args.batch, &mut |m| {
        println!("{m}")
    })?;
    println!("loaded in {:.1}s", t0.elapsed().as_secs_f64());

    let server = PolarGraphServer::new(store.clone()).context("server")?;
    let ctx = Ctx {
        server,
        store: store.clone(),
        mentions: Mentions::new(&plan),
        vectors: Vectors::new(&plan),
        spaces: args.vectors.spaces(),
        plan: plan.clone(),
        budget: Budget {
            samples: args.samples,
            secs: args.budget_secs,
        },
    };

    let mut ms = Vec::new();
    let step = |name: &str| println!("measuring: {name}");
    step("load one graph");
    ms.push(measure::load_graph(&ctx).await?);
    step("describe(entity)");
    ms.push(measure::describe(&ctx).await?);
    step("hybrid search");
    ms.extend(measure::hybrid(&ctx).await?);
    step("context assembly");
    ms.extend(measure::context_assembly(&ctx).await?);
    step("promotion");
    ms.push(measure::promote(&ctx).await?);
    step("time travel");
    ms.push(measure::time_travel(&ctx, loaded.pre_supersession_ts).await?);
    // Before ingestion: its vectors aren't in the exact baseline.
    step("recall");
    let recall = measure::recall(&ctx, args.recall_queries).await?;
    step("ingestion");
    ms.push(measure::ingestion(&ctx, args.ingest_records).await?);
    step("inference lag");
    ms.push(measure::inference_lag(&ctx, args.lag_secs, args.lag_rate).await?);

    let quads = loaded.base_quads + loaded.inferred_quads;
    // Steady-state size: flush memtables and compact, so the WAL and
    // superseded files don't count.
    for cf in polargraph_storage::cf::ALL {
        let _ = store.compact_cf(cf);
    }
    let disk = report::dir_size(&dir);
    let mut cf_sizes: Vec<(String, u64)> = {
        use polargraph_server::proto::{
            polar_graph_service_server::PolarGraphService, ShowIndexesRequest,
        };
        ctx.server
            .show_indexes(tonic::Request::new(ShowIndexesRequest {}))
            .await?
            .into_inner()
            .column_families
            .into_iter()
            .map(|c| (c.name, c.approx_size_bytes))
            .filter(|(_, b)| *b > 0)
            .collect()
    };
    cf_sizes.sort_by_key(|c| std::cmp::Reverse(c.1));
    let vectors_dir = report::dir_size(&dir.join("vectors"));
    let report = report::Report {
        scale: plan.scale,
        quads,
        chunks: plan.chunks() as u64,
        disk_bytes: disk,
        bytes_per_quad: disk as f64 / quads.max(1) as f64,
        cf_sizes,
        vectors_dir_bytes: vectors_dir,
        load: loaded,
        measurements: ms,
        recall,
        host: host(),
    };
    let md = report.markdown();
    println!("\n{md}");
    if let Some(p) = &args.json {
        std::fs::write(p, serde_json::to_string_pretty(&report)?)?;
    }
    if let Some(p) = &args.markdown {
        use std::io::Write as _;
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(p)?
            .write_all(md.as_bytes())?;
    }
    Ok(())
}

/// OS, CPU model, core count and RAM.
fn host() -> String {
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(0);
    let sysctl = |key: &str| -> Option<String> {
        let out = std::process::Command::new("sysctl")
            .args(["-n", key])
            .output()
            .ok()?;
        Some(String::from_utf8(out.stdout).ok()?.trim().to_string()).filter(|s| !s.is_empty())
    };
    let proc_line = |file: &str, key: &str| -> Option<String> {
        std::fs::read_to_string(file)
            .ok()?
            .lines()
            .find(|l| l.starts_with(key))
            .and_then(|l| l.split(':').nth(1))
            .map(|v| v.trim().to_string())
    };
    let model = sysctl("machdep.cpu.brand_string")
        .or_else(|| proc_line("/proc/cpuinfo", "model name"))
        .unwrap_or_else(|| std::env::consts::ARCH.to_string());
    let ram_gib = sysctl("hw.memsize")
        .and_then(|b| b.parse::<f64>().ok())
        .map(|b| b / 1024f64.powi(3))
        .or_else(|| {
            proc_line("/proc/meminfo", "MemTotal")
                .and_then(|v| v.split_whitespace().next()?.parse::<f64>().ok())
                .map(|kb| kb / 1024f64.powi(2))
        });
    format!(
        "{} · {model} · {cpus} cores · {} RAM",
        std::env::consts::OS,
        ram_gib.map_or("? GiB".into(), |g| format!("{g:.0} GiB"))
    )
}
