//! `hft run`: paired A/B experiment over a recording or fixture, written to `reports/<id>/`.

use hft_core::Instrument;
use hft_experiment::{ExperimentConfig, Window, run_scenario};
use hft_replay::{ItemStream, open_source};
use std::path::PathBuf;

#[derive(clap::Args)]
pub struct RunArgs {
    /// Experiment config (TOML).
    #[arg(long)]
    config: PathBuf,
    /// Recording directory or fixture file.
    #[arg(long)]
    data: PathBuf,
    /// Output directory (default: reports/<config-stem>-<split>-<git rev>).
    #[arg(long)]
    out: Option<PathBuf>,
    /// Restrict the strategy to a named chronological split from the config.
    #[arg(long)]
    split: Option<String>,
    /// Only run these scenarios (comma-separated names).
    #[arg(long)]
    only: Option<String>,
}

pub fn run(a: RunArgs) -> anyhow::Result<()> {
    let text = std::fs::read_to_string(&a.config)?;
    let mut cfg = ExperimentConfig::from_toml(&text)?;
    if let Some(name) = &a.split {
        let s = cfg.splits.iter().find(|s| &s.name == name).ok_or_else(|| anyhow::anyhow!("no split named {name}"))?;
        cfg.active = Some(Window { start_s: s.start_s, end_s: s.end_s });
    }
    if let Some(only) = &a.only {
        let keep: Vec<&str> = only.split(',').collect();
        cfg.scenarios.retain(|s| keep.contains(&s.name.as_str()));
    }
    anyhow::ensure!(!cfg.scenarios.is_empty(), "no scenarios to run");
    let inst = Instrument::btc_usd();
    let data = a.data.clone();
    let open = || -> anyhow::Result<Box<dyn ItemStream>> { open_source(&data, inst.clone()) };

    let stem = a.config.file_stem().and_then(|s| s.to_str()).unwrap_or("run");
    let out = a.out.clone().unwrap_or_else(|| {
        PathBuf::from("reports").join(format!("{stem}-{}-{}", a.split.as_deref().unwrap_or("all"), crate::GIT_REV))
    });
    std::fs::create_dir_all(&out)?;

    let mut results = vec![];
    let mut metrics = vec![];
    for sc in &cfg.scenarios {
        let t = std::time::Instant::now();
        let r = run_scenario(&open, &cfg, sc)?;
        let m = hft_report::scenario_metrics(&inst, &r);
        println!(
            "{:<24} A: {:>4} fills gross {:>10.4}  |  B: {:>4} fills gross {:>10.4}  |  Δ {:>+10.4}  ({:.1}s)",
            sc.name,
            m.a.fills,
            m.a.gross_pnl,
            m.b.fills,
            m.b.gross_pnl,
            m.diff_gross,
            t.elapsed().as_secs_f64()
        );
        metrics.push(m);
        results.push(r);
    }

    let meta = vec![
        ("data".to_string(), a.data.display().to_string()),
        ("config".to_string(), a.config.display().to_string()),
        ("split".to_string(), a.split.clone().unwrap_or_else(|| "entire recording".into())),
        ("git revision".to_string(), crate::GIT_REV.to_string()),
        ("clock offset (recv − exch, min)".to_string(), format!("{:.3} ms", results[0].clock_offset_ns as f64 / 1e6)),
        ("active window".to_string(), format!("{:.1} s", metrics[0].a.active_s)),
    ];
    let md = hft_report::render_markdown("Stale-data protection: paired A/B report", &meta, &metrics);
    std::fs::write(out.join("report.md"), md)?;
    std::fs::write(out.join("metrics.json"), serde_json::to_string_pretty(&metrics)?)?;
    std::fs::write(out.join("config.toml"), toml::to_string(&cfg)?)?;
    // Fills per arm for external analysis.
    let mut csv = String::from(
        "scenario,arm,t_ns,side,px_ticks,qty_lots,mid_x2,mid_x2_100ms,mid_x2_1s,mid_x2_10s,during_pause\n",
    );
    for r in &results {
        for arm in [&r.a, &r.b] {
            for f in &arm.fills {
                let o = |x: Option<i64>| x.map_or(String::new(), |v| v.to_string());
                csv.push_str(&format!(
                    "{},{},{},{:?},{},{},{},{},{},{},{}\n",
                    r.scenario.name,
                    arm.arm,
                    f.t_ns,
                    f.side,
                    f.px,
                    f.qty,
                    f.mid_x2,
                    o(f.mid_x2_after[0]),
                    o(f.mid_x2_after[1]),
                    o(f.mid_x2_after[2]),
                    f.during_pause
                ));
            }
        }
    }
    std::fs::write(out.join("fills.csv"), csv)?;
    let mut eq = String::from("scenario,arm,t_ns,equity_cash_units,gross_cash_units,inventory_lots\n");
    for r in &results {
        for arm in [&r.a, &r.b] {
            for e in &arm.equity {
                eq.push_str(&format!(
                    "{},{},{},{},{},{}\n",
                    r.scenario.name, arm.arm, e.t_ns, e.equity, e.gross, e.inventory
                ));
            }
        }
    }
    std::fs::write(out.join("equity.csv"), eq)?;
    println!("report: {}", out.join("report.md").display());
    Ok(())
}
