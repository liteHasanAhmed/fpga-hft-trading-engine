//! Tick-to-decision latency harness.
//!
//! **Measurement boundary:** from a raw frame already in memory (bytes as read off
//! the socket) to the strategy having produced its order requests:
//! `decode (parse + sequence check) → feed/book update → strategy.on_market` for every
//! message in the frame. One sample per frame. Excludes network, disk,
//! simulator, and order transmission. Recording and replay throughput are
//! measured separately.

pub mod alloc;

use hft_core::{Instrument, OrderReq, Ts};
use hft_feed::Feed;
use hft_recorder::Kind;
use hft_replay::{Decoder, Item, Venue};
use hft_strategy::{Ctx, MarketMaker, MmConfig, StaleGuardConfig, Strategy};
use serde::Serialize;
use std::collections::VecDeque;
use std::time::Instant;

#[derive(Debug, Serialize)]
pub struct LatencyReport {
    pub venue: String,
    pub frames: u64,
    pub md_msgs: u64,
    pub p50_ns: u64,
    pub p90_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub max_ns: u64,
    pub mean_ns: f64,
    /// Untimed pass: frames per second through the same path.
    pub throughput_msgs_per_s: f64,
    pub allocs_per_frame: f64,
    pub bytes: u64,
    pub host: HostInfo,
}

#[derive(Debug, Serialize)]
pub struct HostInfo {
    pub cpu: String,
    pub os: String,
    pub rustc: String,
    pub profile: &'static str,
}

impl HostInfo {
    pub fn detect() -> HostInfo {
        let cmd = |c: &str, a: &[&str]| {
            std::process::Command::new(c)
                .args(a)
                .output()
                .ok()
                .and_then(|o| String::from_utf8(o.stdout).ok())
                .map(|s| s.trim().to_string())
                .unwrap_or_else(|| "unknown".into())
        };
        let cpu = if cfg!(target_os = "macos") {
            cmd("sysctl", &["-n", "machdep.cpu.brand_string"])
        } else {
            cmd("sh", &["-c", "grep -m1 'model name' /proc/cpuinfo | cut -d: -f2"])
        };
        HostInfo {
            cpu,
            os: format!("{} {}", std::env::consts::OS, cmd("uname", &["-r"])),
            rustc: cmd("rustc", &["--version"]),
            profile: if cfg!(debug_assertions) { "debug" } else { "release" },
        }
    }
}

struct Pipeline {
    dec: Decoder,
    items: VecDeque<Item>,
    feed: Feed,
    strat: MarketMaker,
    out: Vec<hft_feed::FeedEvent>,
    reqs: Vec<OrderReq>,
}

impl Pipeline {
    fn new(venue: Venue, inst: Instrument) -> Pipeline {
        let cfg = MmConfig { stale_guard: Some(StaleGuardConfig::default()), ..Default::default() };
        Pipeline {
            dec: Decoder::new(venue, inst),
            items: VecDeque::with_capacity(64),
            feed: Feed::new(1 << 20),
            strat: MarketMaker::new("bench", cfg),
            out: Vec::with_capacity(64),
            reqs: Vec::with_capacity(16),
        }
    }

    /// One raw record through the measured path. Returns the number of market messages.
    #[inline]
    fn step(&mut self, kind: Kind, recv_ns: i64, payload: &[u8]) -> u64 {
        if self.dec.decode(kind, recv_ns, payload, &mut self.items).is_err() {
            return 0;
        }
        let mut n = 0;
        while let Some(item) = self.items.pop_front() {
            match item {
                Item::Md { msg, .. } => {
                    n += 1;
                    self.out.clear();
                    self.feed.on_msg(&msg, &mut self.out);
                    let ctx = Ctx {
                        now: Ts(recv_ns),
                        book: self.feed.book(),
                        book_valid: self.feed.is_live(),
                        msg_exch_ts: (msg.exch_ts > Ts::ZERO && msg.event.is_timely()).then_some(msg.exch_ts),
                        last_lag: Ts::ZERO,
                        last_heartbeat_exch_ts: self.feed.last_heartbeat_exch_ts(),
                        last_msg_at: Ts(recv_ns),
                    };
                    self.reqs.clear();
                    self.strat.on_market(&ctx, &mut self.reqs);
                }
                Item::Disconnect { .. } => {
                    self.out.clear();
                    self.feed.reset(&mut self.out);
                }
                _ => {}
            }
        }
        n
    }
}

/// Run the harness over records already in memory.
pub fn tick_to_decision(
    records: &[(Kind, i64, Vec<u8>)],
    venue: Venue,
    inst: &Instrument,
) -> anyhow::Result<LatencyReport> {
    let bytes: u64 = records.iter().map(|r| r.2.len() as u64).sum();
    // Pass 1: untimed throughput (also warms caches and the allocator).
    let mut p = Pipeline::new(venue, inst.clone());
    let t = Instant::now();
    let mut md = 0u64;
    for (k, ts, payload) in records {
        md += p.step(*k, *ts, payload);
    }
    let throughput = md as f64 / t.elapsed().as_secs_f64();

    // Pass 2: per-frame timing on a fresh pipeline.
    let mut p = Pipeline::new(venue, inst.clone());
    let mut h = hdrhistogram::Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3)?;
    let a0 = alloc::count();
    for (k, ts, payload) in records {
        let s = Instant::now();
        let n = p.step(*k, *ts, payload);
        let d = s.elapsed().as_nanos() as u64;
        if n > 0 {
            h.saturating_record(d.max(1));
        }
    }
    let allocs = alloc::count() - a0;
    Ok(LatencyReport {
        venue: venue.name().into(),
        frames: records.len() as u64,
        md_msgs: md,
        p50_ns: h.value_at_quantile(0.5),
        p90_ns: h.value_at_quantile(0.9),
        p99_ns: h.value_at_quantile(0.99),
        p999_ns: h.value_at_quantile(0.999),
        max_ns: h.max(),
        mean_ns: h.mean(),
        throughput_msgs_per_s: throughput,
        allocs_per_frame: allocs as f64 / records.len().max(1) as f64,
        bytes,
        host: HostInfo::detect(),
    })
}
