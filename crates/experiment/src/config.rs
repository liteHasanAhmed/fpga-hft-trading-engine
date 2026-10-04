use hft_sim::SimConfig;
use hft_strategy::{MmConfig, StaleGuardConfig};
use serde::{Deserialize, Serialize};

/// Time window in seconds relative to the first market message of the run.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Window {
    pub start_s: f64,
    pub end_s: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DelayWindow {
    pub start_s: f64,
    pub end_s: f64,
    pub extra_ms: f64,
}

/// A set of injected conditions applied identically to both arms.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Scenario {
    pub name: String,
    /// Extra feed delay for messages that would become visible inside the window.
    #[serde(default)]
    pub feed_delay: Vec<DelayWindow>,
    /// Feed outage: messages are lost, then the connection drops at `end_s` and the
    /// strategy-side feed must resync from a snapshot.
    #[serde(default)]
    pub outages: Vec<Window>,
    /// Processing stall: the strategy thread is blocked; everything queued is
    /// delivered after the window, draining at `stall_drain_us_per_msg`.
    #[serde(default)]
    pub stalls: Vec<Window>,
    #[serde(default = "default_drain")]
    pub stall_drain_us_per_msg: f64,
    /// Time from reconnect to applied snapshot after an injected outage.
    #[serde(default = "default_snap_latency")]
    pub snapshot_latency_ms: f64,
    /// Optional overrides of order/cancel/response latency (ms).
    #[serde(default)]
    pub order_latency_ms: Option<f64>,
    #[serde(default)]
    pub cancel_latency_ms: Option<f64>,
    #[serde(default)]
    pub response_latency_ms: Option<f64>,
    #[serde(default)]
    pub fill_model: Option<hft_sim::FillModel>,
    /// Repeat every fault window with this period (seconds) until the end of the run.
    #[serde(default)]
    pub repeat_every_s: Option<f64>,
    /// `natural` scenarios only reflect recorded conditions; `injected` ones add faults.
    /// Reported in separate sections.
    #[serde(default)]
    pub injected: bool,
}

fn default_drain() -> f64 {
    2.0
}
fn default_snap_latency() -> f64 {
    300.0
}

impl Scenario {
    pub fn healthy() -> Scenario {
        Scenario {
            name: "healthy".into(),
            feed_delay: vec![],
            outages: vec![],
            stalls: vec![],
            stall_drain_us_per_msg: default_drain(),
            snapshot_latency_ms: default_snap_latency(),
            order_latency_ms: None,
            cancel_latency_ms: None,
            response_latency_ms: None,
            fill_model: None,
            repeat_every_s: None,
            injected: false,
        }
    }

    pub fn sim_config(&self, base: &SimConfig) -> SimConfig {
        let ms = |x: f64| (x * 1e6) as i64;
        let mut c = base.clone();
        if let Some(v) = self.order_latency_ms {
            c.order_latency_ns = ms(v);
        }
        if let Some(v) = self.cancel_latency_ms {
            c.cancel_latency_ns = ms(v);
        }
        if let Some(v) = self.response_latency_ms {
            c.response_latency_ns = ms(v);
        }
        if let Some(f) = self.fill_model {
            c.fill_model = f;
        }
        c
    }
}

/// Chronological split boundaries (seconds relative to recording start).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Split {
    pub name: String,
    pub start_s: f64,
    pub end_s: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExperimentConfig {
    pub sim: SimConfig,
    pub mm: MmConfig,
    pub guard: StaleGuardConfig,
    /// Latency assumed for the fastest message of the run (ms).
    #[serde(default)]
    pub base_feed_latency_ms: f64,
    /// Strategy is active only within this window (seconds from first message).
    /// Messages before it warm up the books.
    #[serde(default)]
    pub active: Option<Window>,
    #[serde(default)]
    pub splits: Vec<Split>,
    pub scenarios: Vec<Scenario>,
    /// Equity sampling interval (ms) for drawdown and hourly buckets.
    #[serde(default = "default_sample")]
    pub equity_sample_ms: f64,
    /// How far ahead (receive time) to read so late-arriving trade prints are
    /// still applied at their true engine time. Must exceed the venue's batching
    /// delay plus network jitter (Coinbase Advanced Trade batches trades over 250 ms).
    #[serde(default = "default_reorder")]
    pub reorder_window_ms: f64,
}

fn default_reorder() -> f64 {
    2000.0
}

fn default_sample() -> f64 {
    1000.0
}

impl ExperimentConfig {
    pub fn from_toml(s: &str) -> anyhow::Result<ExperimentConfig> {
        Ok(toml::from_str(s)?)
    }
}
