//! Paired experiments: the same recording, configuration and seed run through
//! arm A (baseline) and arm B (baseline + stale-data guard).
//!
//! ## Two views of the market
//! * **Exchange side**, on the exchange time axis: the recorded book evolves at each
//!   message's `exch_ts`. Our orders are accepted and filled against this view.
//! * **Strategy side**: the same messages become visible at
//!   `recv_ts - clock_offset + injected faults`. The strategy only ever reads this view.
//!
//! `clock_offset` = min over the run of `recv_ts - exch_ts`, i.e. the fastest
//! message is assumed to have arrived with `base_feed_latency` delay; every other
//! message keeps its real recorded extra delay. (Assumption: the capture host's
//! clock is NTP-disciplined and drifts negligibly within a run.)

mod config;
mod runner;

pub use config::{DelayWindow, ExperimentConfig, Scenario, Split, Window};
pub use runner::{
    ArmResult, EquityPoint, FillRec, MARKOUT_HORIZONS_MS, PauseStats, ScenarioResult, estimate_clock_offset,
    run_scenario,
};
