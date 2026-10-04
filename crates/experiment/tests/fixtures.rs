//! End-to-end vertical slice: fixture → replay → strategy → simulator.

use hft_core::{Instrument, Qty};
use hft_experiment::{ExperimentConfig, Scenario, ScenarioResult, run_scenario};
use hft_replay::{FixtureSource, ItemStream, verify_stream};
use hft_sim::{Fees, FillModel, SimConfig};
use hft_strategy::{MmConfig, StaleGuardConfig};
use std::path::PathBuf;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures").join(name)
}

fn cfg(fees: Fees) -> ExperimentConfig {
    ExperimentConfig {
        sim: SimConfig { fill_model: FillModel::TradeTouch, fees, ..Default::default() },
        mm: MmConfig { quote_qty: Qty(1_000_000), max_inventory: Qty(5_000_000), ..Default::default() },
        // Fixtures model a venue with ~20 ms normal lag, so the guard is tuned tighter
        // than the Coinbase Advanced Trade defaults.
        guard: StaleGuardConfig {
            lag_floor_ns: 150_000_000,
            lag_mult: 5.0,
            ewma_alpha: 0.001,
            heartbeat_timeout_ns: 2_000_000_000,
            resume_after_ns: 500_000_000,
        },
        base_feed_latency_ms: 0.0,
        active: None,
        splits: vec![],
        scenarios: vec![Scenario::healthy()],
        equity_sample_ms: 100.0,
        reorder_window_ms: 2000.0,
    }
}

fn run(name: &str, c: &ExperimentConfig, sc: &Scenario) -> ScenarioResult {
    let p = fixture(name);
    let open =
        || -> anyhow::Result<Box<dyn ItemStream>> { Ok(Box::new(FixtureSource::open(&p, Instrument::btc_usd())?)) };
    run_scenario(&open, c, sc).unwrap()
}

#[test]
fn fixtures_replay_cleanly() {
    for name in ["basic.log", "stale.log"] {
        let mut src = FixtureSource::open(&fixture(name), Instrument::btc_usd()).unwrap();
        let r = verify_stream(&mut src, 1).unwrap();
        assert_eq!(r.feed.gaps, 0, "{name}");
        assert_eq!(r.feed.book_errors, 0, "{name}");
        assert_eq!(r.feed.snapshots_applied, 1, "{name}");
        let mut again = FixtureSource::open(&fixture(name), Instrument::btc_usd()).unwrap();
        assert_eq!(verify_stream(&mut again, 0).unwrap().digest, r.digest, "replay must be deterministic");
    }
}

/// Hand-calculated (see header of fixtures/basic.log):
/// buy 0.01 @ 100.00, sell 0.01 @ 100.01 → cash +0.0001 USD = 1_000_000 cash units (1e-10 USD).
#[test]
fn basic_round_trip_exact() {
    let r = run("basic.log", &cfg(Fees::PROFESSIONAL), &Scenario::healthy());
    for arm in [&r.a, &r.b] {
        let fills: Vec<_> = arm.fills.iter().map(|f| (f.side, f.px, f.qty)).collect();
        assert_eq!(
            fills,
            vec![(hft_core::Side::Bid, 10000, 1_000_000), (hft_core::Side::Ask, 10001, 1_000_000)],
            "{}",
            arm.arm
        );
        assert_eq!(arm.portfolio.inventory, Qty(0));
        assert_eq!(arm.portfolio.cash.0, 1_000_000);
        assert_eq!(arm.portfolio.fees_paid.0, 0);
        assert_eq!(arm.guard_trips, 0, "constant 20 ms lag must not trip the guard");
    }
    // Retail: fees = 0.6% × 1.00 + 0.6% × 1.0001 = 0.006 + 0.0060006 = 0.0120006 USD.
    let r = run("basic.log", &cfg(Fees::RETAIL), &Scenario::healthy());
    assert_eq!(r.a.portfolio.fees_paid.0, 120_006_000);
    assert_eq!(r.a.portfolio.net_cash().0, 1_000_000 - 120_006_000);
}

#[test]
fn runs_are_deterministic() {
    let c = cfg(Fees::PROFESSIONAL);
    let x = run("stale.log", &c, &Scenario::healthy());
    let y = run("stale.log", &c, &Scenario::healthy());
    assert_eq!(x.a.digest, y.a.digest);
    assert_eq!(x.b.digest, y.b.digest);
}

#[test]
fn stale_guard_avoids_later_pickoffs() {
    let r = run("stale.log", &cfg(Fees::PROFESSIONAL), &Scenario::healthy());
    let (a, b) = (&r.a, &r.b);
    eprintln!("A fills {:?}", a.fills.iter().map(|f| (f.t_ns - a.start_ns, f.side, f.px)).collect::<Vec<_>>());
    eprintln!("B fills {:?}", b.fills.iter().map(|f| (f.t_ns - b.start_ns, f.side, f.px)).collect::<Vec<_>>());
    eprintln!(
        "A eq {:?} B eq {:?} B trips {} paused {:?}",
        a.final_equity_mid, b.final_equity_mid, b.guard_trips, b.pause
    );
    assert!(b.guard_trips >= 1);
    assert!(b.pause.stale_ns > 0);
    assert!(a.fills.len() > b.fills.len(), "A keeps getting picked off");
    assert!(b.final_equity_mid.unwrap() > a.final_equity_mid.unwrap());
}

#[test]
fn injected_outage_resyncs_and_guard_reacts_first() {
    let mut sc = Scenario::healthy();
    sc.name = "outage".into();
    sc.injected = true;
    sc.outages = vec![hft_experiment::Window { start_s: 1.0, end_s: 8.0 }];
    let r = run("stale.log", &cfg(Fees::PROFESSIONAL), &sc);
    assert!(r.a.vis_lost_in_outage > 0);
    // Both arms see the reset at 8 s (essential pause); B's heartbeat timeout pauses it earlier.
    assert!(r.a.pause.essential_ns > 0);
    assert!(r.b.pause.stale_ns > 0);
    eprintln!("A {:?} B {:?}", r.a.pause, r.b.pause);
}

#[test]
fn guard_config_roundtrips_toml() {
    let c = cfg(Fees::RETAIL);
    let s = toml::to_string(&c).unwrap();
    assert_eq!(ExperimentConfig::from_toml(&s).unwrap(), c);
}

/// Same economics as `basic.log`, but on the L2 Advanced Trade format where trades
/// arrive in a batch 200 ms after the level update they caused.
#[test]
fn advanced_trade_round_trip_exact() {
    let r = run("at_basic.log", &cfg(Fees::PROFESSIONAL), &Scenario::healthy());
    for arm in [&r.a, &r.b] {
        let fills: Vec<_> = arm.fills.iter().map(|f| (f.side, f.px, f.qty, f.t_ns - arm.start_ns)).collect();
        // Fill times are the trades' engine times (200 ms, 600 ms), not batch arrival (400, 800).
        // t0 is the first heartbeat frame's timestamp, 21 ms.
        assert_eq!(
            fills,
            vec![
                (hft_core::Side::Bid, 10000, 1_000_000, 179_000_000),
                (hft_core::Side::Ask, 10001, 1_000_000, 579_000_000)
            ],
            "{}",
            arm.arm
        );
        assert_eq!(arm.portfolio.cash.0, 1_000_000);
        assert_eq!(arm.late_trades, 0);
    }
}

#[test]
fn advanced_trade_gap_invalidates_until_resubscribe() {
    let mut src = FixtureSource::open(&fixture("at_gap.log"), Instrument::btc_usd()).unwrap();
    let mut feed = hft_feed::Feed::new(16);
    let mut ev = vec![];
    let mut saw_gap = false;
    while let Some(item) = src.next_item().unwrap() {
        match item {
            hft_replay::Item::Md { msg, .. } => {
                let before = feed.is_live();
                feed.on_msg(&msg, &mut ev);
                if saw_gap && before {
                    panic!("book must stay invalid between the gap and the new snapshot");
                }
            }
            hft_replay::Item::Disconnect { reason, .. } => {
                saw_gap |= reason.starts_with("sequence gap");
                feed.reset(&mut ev);
                if !reason.starts_with("sequence gap") {
                    saw_gap = false; // reconnect: next snapshot may resync
                }
            }
            _ => {}
        }
    }
    assert!(feed.is_live());
    assert_eq!(feed.book().best_bid(), Some((hft_core::Px(10000), Qty(400_000_000))));
}
