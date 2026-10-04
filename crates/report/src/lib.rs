//! Turn experiment results into metrics and a Markdown + JSON report.
//!
//! Fees never influence the strategy's decisions, so fills are identical across
//! fee tiers. Net P&L for each tier is recomputed from the fill list, which
//! guarantees every tier is evaluated on exactly the same trades.

use hft_core::{Cash, Instrument, Liquidity, Px, Qty, Side};
use hft_experiment::{ArmResult, MARKOUT_HORIZONS_MS, ScenarioResult};
use hft_sim::Fees;
use rand::Rng;
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;
use serde::Serialize;
use std::fmt::Write;

pub const TIERS: [(&str, Fees); 2] = [("professional", Fees::PROFESSIONAL), ("retail", Fees::RETAIL)];

#[derive(Clone, Debug, Serialize)]
pub struct ArmMetrics {
    pub arm: String,
    /// Mark-to-mid P&L before fees (USD).
    pub gross_pnl: f64,
    /// Mark-to-mid P&L after fees, per tier (USD), same order as [`TIERS`].
    pub net_pnl: [f64; 2],
    /// P&L if inventory were flattened with a taker order at the end (run fee tier).
    pub liquidation_pnl: Option<f64>,
    pub final_inventory_btc: f64,
    pub max_abs_inventory_btc: f64,
    pub fills: usize,
    pub volume_btc: f64,
    /// Average favorable mid move per BTC filled at each horizon (USD/BTC);
    /// negative = adverse selection.
    pub markout_per_btc: [Option<f64>; 3],
    /// Gross P&L per fill (USD).
    pub gross_per_fill: Option<f64>,
    pub max_drawdown: f64,
    pub active_s: f64,
    pub paused_essential_pct: f64,
    pub paused_stale_pct: f64,
    pub orders: u64,
    pub cancels: u64,
    pub rejects: u64,
    pub fills_while_cancel_pending: u64,
    pub guard_trips: u64,
    pub visible_lag_p50_ms: f64,
    pub visible_lag_p99_ms: f64,
    pub digest: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct PairedDiff {
    pub bucket_s: f64,
    pub buckets: usize,
    /// Mean per-bucket (B − A) gross P&L, USD, with a 95% bootstrap CI.
    pub mean: f64,
    pub ci95: (f64, f64),
}

#[derive(Clone, Debug, Serialize)]
pub struct ScenarioMetrics {
    pub name: String,
    pub injected: bool,
    pub a: ArmMetrics,
    pub b: ArmMetrics,
    pub diff_gross: f64,
    pub diff_net: [f64; 2],
    pub paired: Option<PairedDiff>,
}

fn usd(inst: &Instrument, c: i128) -> f64 {
    inst.cash_f64(Cash(c))
}

pub fn arm_metrics(inst: &Instrument, r: &ArmResult) -> ArmMetrics {
    let mut gross_cash: i128 = 0;
    let mut fees = [0i128; 2];
    let mut inv = 0i64;
    let mut max_inv = 0i64;
    let mut vol = 0i64;
    let mut mk_sum = [0i128; 3];
    let mut mk_qty = [0i64; 3];
    for f in &r.fills {
        let n = Px(f.px).notional(Qty(f.qty));
        gross_cash += match f.side {
            Side::Bid => -n.0,
            Side::Ask => n.0,
        };
        for (i, (_, t)) in TIERS.iter().enumerate() {
            fees[i] += t.fee(n, Liquidity::Maker).0;
        }
        inv += f.side.sign() * f.qty;
        max_inv = max_inv.max(inv.abs());
        vol += f.qty;
        for h in 0..3 {
            if let Some(m) = f.mid_x2_after[h] {
                // Favorable move (×2 ticks): buys gain when mid rises, sells when it falls.
                let mv = f.side.sign() as i128 * (m as i128 - 2 * f.px as i128);
                mk_sum[h] += mv * f.qty as i128;
                mk_qty[h] += f.qty;
            }
        }
    }
    let gross = r.final_mid_x2.map(|m| gross_cash + (m as i128 * inv as i128).div_euclid(2));
    let gross_usd = gross.map_or(f64::NAN, |g| usd(inst, g));
    let markout = |h: usize| {
        (mk_qty[h] > 0).then(|| {
            // mk_sum is in (half-tick × lot); per BTC → divide by qty in BTC.
            let usd_total = usd(inst, mk_sum[h]) / 2.0;
            usd_total / inst.qty_f64(Qty(mk_qty[h]))
        })
    };
    let mut peak = f64::MIN;
    let mut mdd: f64 = 0.0;
    for e in &r.equity {
        let v = usd(inst, e.equity);
        peak = peak.max(v);
        mdd = mdd.max(peak - v);
    }
    let active = r.pause.active_ns.max(1) as f64;
    ArmMetrics {
        arm: r.arm.clone(),
        gross_pnl: gross_usd,
        net_pnl: [gross_usd - usd(inst, fees[0]), gross_usd - usd(inst, fees[1])],
        liquidation_pnl: r.liquidation_equity.map(|c| usd(inst, c)),
        final_inventory_btc: inst.qty_f64(Qty(inv)),
        max_abs_inventory_btc: inst.qty_f64(Qty(max_inv)),
        fills: r.fills.len(),
        volume_btc: inst.qty_f64(Qty(vol)),
        markout_per_btc: [markout(0), markout(1), markout(2)],
        gross_per_fill: (!r.fills.is_empty()).then(|| gross_usd / r.fills.len() as f64),
        max_drawdown: mdd,
        active_s: r.pause.active_ns as f64 / 1e9,
        paused_essential_pct: 100.0 * r.pause.essential_ns as f64 / active,
        paused_stale_pct: 100.0 * r.pause.stale_ns as f64 / active,
        orders: r.orders_sent,
        cancels: r.cancels_sent,
        rejects: r.sim.rejected,
        fills_while_cancel_pending: r.sim.fills_while_cancel_pending,
        guard_trips: r.guard_trips,
        visible_lag_p50_ms: r.visible_lag.p50_us as f64 / 1e3,
        visible_lag_p99_ms: r.visible_lag.p99_us as f64 / 1e3,
        digest: r.digest.clone(),
    }
}

/// Per-bucket gross P&L changes from an equity series.
fn bucket_changes(r: &ArmResult, bucket_ns: i64) -> Vec<(i64, f64)> {
    let mut out = vec![];
    let mut last: Option<(i64, i128)> = None;
    for e in &r.equity {
        let b = (e.t_ns - r.start_ns) / bucket_ns;
        match last {
            Some((lb, _)) if lb == b => {}
            _ => {
                if let Some((lb, lv)) = last {
                    out.push((lb, (e.gross - lv) as f64));
                }
                last = Some((b, e.gross));
            }
        }
    }
    if let (Some((lb, lv)), Some(e)) = (last, r.equity.last()) {
        out.push((lb, (e.gross - lv) as f64));
    }
    out
}

/// Paired bootstrap over time buckets: hourly when the run is long enough,
/// otherwise 20 equal buckets.
pub fn paired_diff(inst: &Instrument, a: &ArmResult, b: &ArmResult, seed: u64) -> Option<PairedDiff> {
    let dur = (a.end_ns - a.start_ns).max(1);
    let hour = 3_600_000_000_000i64;
    let bucket_ns = if dur / hour >= 10 { hour } else { (dur / 20).max(1) };
    let ba = bucket_changes(a, bucket_ns);
    let bb = bucket_changes(b, bucket_ns);
    let diffs: Vec<f64> = ba
        .iter()
        .filter_map(|(k, va)| {
            bb.iter().find(|(kb, _)| kb == k).map(|(_, vb)| (vb - va) * 10f64.powi(-(inst.cash_decimals() as i32)))
        })
        .collect();
    if diffs.len() < 2 {
        return None;
    }
    let n = diffs.len();
    let mean = diffs.iter().sum::<f64>() / n as f64;
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut means: Vec<f64> =
        (0..2000).map(|_| (0..n).map(|_| diffs[rng.random_range(0..n)]).sum::<f64>() / n as f64).collect();
    means.sort_by(|x, y| x.total_cmp(y));
    Some(PairedDiff { bucket_s: bucket_ns as f64 / 1e9, buckets: n, mean, ci95: (means[49], means[1949]) })
}

pub fn scenario_metrics(inst: &Instrument, r: &ScenarioResult) -> ScenarioMetrics {
    let a = arm_metrics(inst, &r.a);
    let b = arm_metrics(inst, &r.b);
    ScenarioMetrics {
        name: r.scenario.name.clone(),
        injected: r.scenario.injected,
        diff_gross: b.gross_pnl - a.gross_pnl,
        diff_net: [b.net_pnl[0] - a.net_pnl[0], b.net_pnl[1] - a.net_pnl[1]],
        paired: paired_diff(inst, &r.a, &r.b, 0x5EED),
        a,
        b,
    }
}

fn f(x: f64) -> String {
    if x.is_nan() { "n/a".into() } else { format!("{x:.4}") }
}
fn of(x: Option<f64>) -> String {
    x.map_or("n/a".into(), |v| format!("{v:.4}"))
}

/// Markdown report.
pub fn render_markdown(title: &str, meta: &[(String, String)], ms: &[ScenarioMetrics]) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "# {title}\n");
    for (k, v) in meta {
        let _ = writeln!(s, "- **{k}:** {v}");
    }
    let _ = writeln!(
        s,
        "\nArm A = baseline market maker. Arm B = identical, plus stale-data guard. \
         P&L in USD, marked to exchange-side mid. Fee tiers: professional = 0% maker, \
         retail = 0.60% maker.\n"
    );
    for (heading, injected) in [("Natural (recorded) conditions", false), ("Injected faults", true)] {
        let group: Vec<_> = ms.iter().filter(|m| m.injected == injected).collect();
        if group.is_empty() {
            continue;
        }
        let _ = writeln!(s, "## {heading}\n");
        let _ = writeln!(
            s,
            "| scenario | arm | gross P&L | net (pro) | net (retail) | fills | vol BTC | markout 1s $/BTC | max DD | paused stale % | paused essential % | cancels |"
        );
        let _ = writeln!(s, "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
        for m in &group {
            for a in [&m.a, &m.b] {
                let _ = writeln!(
                    s,
                    "| {} | {} | {} | {} | {} | {} | {:.4} | {} | {} | {:.1} | {:.1} | {} |",
                    m.name,
                    a.arm,
                    f(a.gross_pnl),
                    f(a.net_pnl[0]),
                    f(a.net_pnl[1]),
                    a.fills,
                    a.volume_btc,
                    of(a.markout_per_btc[1]),
                    f(a.max_drawdown),
                    a.paused_stale_pct,
                    a.paused_essential_pct,
                    a.cancels
                );
            }
        }
        let _ = writeln!(s, "\n**Paired difference (B − A)**\n");
        let _ = writeln!(
            s,
            "| scenario | Δ gross | Δ net (pro) | Δ net (retail) | Δ fills | per-bucket mean Δ [95% CI] | buckets |"
        );
        let _ = writeln!(s, "|---|---:|---:|---:|---:|---|---:|");
        for m in &group {
            let p = m.paired.as_ref().map_or("n/a".into(), |p| {
                format!("{} [{}, {}] ({}s)", f(p.mean), f(p.ci95.0), f(p.ci95.1), p.bucket_s.round())
            });
            let _ = writeln!(
                s,
                "| {} | {} | {} | {} | {} | {} | {} |",
                m.name,
                f(m.diff_gross),
                f(m.diff_net[0]),
                f(m.diff_net[1]),
                m.b.fills as i64 - m.a.fills as i64,
                p,
                m.paired.as_ref().map_or(0, |p| p.buckets)
            );
        }
        let _ = writeln!(s);
    }
    let _ = writeln!(s, "## Execution detail\n");
    let _ = writeln!(
        s,
        "| scenario | arm | gross/fill | markouts {:?} ms ($/BTC) | rejects | fills during cancel | guard trips | visible lag p50/p99 ms | digest |",
        MARKOUT_HORIZONS_MS
    );
    let _ = writeln!(s, "|---|---|---:|---|---:|---:|---:|---|---|");
    for m in ms {
        for a in [&m.a, &m.b] {
            let _ = writeln!(
                s,
                "| {} | {} | {} | {} / {} / {} | {} | {} | {} | {:.1} / {:.1} | `{}` |",
                m.name,
                a.arm,
                of(a.gross_per_fill),
                of(a.markout_per_btc[0]),
                of(a.markout_per_btc[1]),
                of(a.markout_per_btc[2]),
                a.rejects,
                a.fills_while_cancel_pending,
                a.guard_trips,
                a.visible_lag_p50_ms,
                a.visible_lag_p99_ms,
                a.digest
            );
        }
    }
    let _ = writeln!(s, "\n## How to read this\n");
    let _ = writeln!(
        s,
        "- Fewer fills is **not** better execution by itself. Compare markouts (adverse selection per BTC) \
           and gross P&L per fill alongside the fill count: protection is useful only if the fills it avoids \
           were worse than the fills it keeps.\n\
         - Injected-fault scenarios measure behavior under conditions we created; natural scenarios measure \
           what happened in the recording. They answer different questions and are not pooled.\n\
         - The per-bucket CI is a paired bootstrap over time buckets; buckets are autocorrelated, so treat it \
           as optimistic.\n\n\
         ## Assumptions and limitations\n\n\
         - Fill rule v1: a resting order fills when a real trade prints at or through its price (or strictly through, \
           in the conservative variant), up to the trade's size. Queue position is ignored, so at-touch fills are optimistic.\n\
         - Our orders do not affect the recorded market (no market impact), and a real trade's volume is not \
           reduced by our fill.\n\
         - Strategy-visible time is `recv_ts − min(recv_ts − exch_ts)`; the capture clock is assumed to drift negligibly.\n\
         - Results depend on these fill-model assumptions and should not be read as achievable live P&L."
    );
    s
}
