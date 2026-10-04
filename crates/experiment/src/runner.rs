use crate::config::{ExperimentConfig, Scenario};
use hft_core::{Cash, MdEvent, MdMsg, OrderEvt, OrderReq, Side, Ts};
use hft_feed::{Feed, FeedEvent};
use hft_replay::{Item, ItemStream, Scheduler};
use hft_sim::{ExchangeSim, Portfolio, SimStats, liquidation_value};
use hft_strategy::{Ctx, MarketMaker, Pause, Strategy};
use serde::Serialize;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

/// Markout horizons: mid price this long after each fill (exchange side).
pub const MARKOUT_HORIZONS_MS: [i64; 3] = [100, 1_000, 10_000];

// Equal-time ordering. Market data at the exchange happens first, so an order
// arriving at the same instant as a trade does not get that trade (conservative).
const P_EXCH: u8 = 0;
const P_ARRIVE: u8 = 1;
const P_SNAP: u8 = 2;
const P_VIS: u8 = 3;
const P_DELIVER: u8 = 4;
const P_TIMER: u8 = 5;

enum Ev {
    ExchMd(MdMsg),
    ExchReset,
    VisMd(MdMsg),
    VisReset,
    /// Injected-outage recovery: snapshot of the exchange-side book delivered to the strategy feed.
    SnapTake,
    Arrive(OrderReq),
    Deliver(OrderEvt),
    Timer,
}

#[derive(Clone, Debug, Serialize)]
pub struct FillRec {
    pub t_ns: i64,
    pub side: Side,
    pub px: i64,
    pub qty: i64,
    /// Exchange-side mid (×2, ticks) just before the fill.
    pub mid_x2: i64,
    /// Exchange-side mid (×2) at each [`MARKOUT_HORIZONS_MS`] after the fill.
    pub mid_x2_after: [Option<i64>; 3],
    /// Fill happened while the strategy was pausing (cancel in flight).
    pub during_pause: bool,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct EquityPoint {
    pub t_ns: i64,
    /// Mark-to-mid equity at the run's fee tier, cash units.
    pub equity: i128,
    /// Gross (pre-fee) mark-to-mid equity, cash units.
    pub gross: i128,
    pub inventory: i64,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct PauseStats {
    pub active_ns: i64,
    pub essential_ns: i64,
    pub stale_ns: i64,
}

#[derive(Clone, Debug, Serialize)]
pub struct LagStats {
    pub samples: u64,
    pub p50_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub max_us: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ArmResult {
    pub arm: String,
    pub guarded: bool,
    pub fills: Vec<FillRec>,
    pub equity: Vec<EquityPoint>,
    pub portfolio: Portfolio,
    pub final_mid_x2: Option<i64>,
    pub final_equity_mid: Option<i128>,
    pub liquidation_equity: Option<i128>,
    pub sim: SimStats,
    pub orders_sent: u64,
    pub cancels_sent: u64,
    pub guard_trips: u64,
    pub pause: PauseStats,
    pub md_msgs: u64,
    /// Trade prints that arrived later than the reorder window (processed late).
    pub late_trades: u64,
    pub vis_lost_in_outage: u64,
    pub visible_lag: LagStats,
    pub start_ns: i64,
    pub end_ns: i64,
    pub wall_ms: u64,
    /// Hash over every fill and order statistic — identical across reruns.
    pub digest: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ScenarioResult {
    pub scenario: Scenario,
    pub clock_offset_ns: i64,
    pub a: ArmResult,
    pub b: ArmResult,
}

/// Pre-pass: (clock offset = min(recv - exch), first exch_ts, last exch_ts).
pub fn estimate_clock_offset(src: &mut dyn ItemStream) -> anyhow::Result<(i64, Ts, Ts)> {
    let mut off = i64::MAX;
    let (mut first, mut last) = (None, Ts::ZERO);
    while let Some(item) = src.next_item()? {
        if let Item::Md { recv_ts, msg } = &item
            && msg.exch_ts > Ts::ZERO
            && msg.event.is_timely()
        {
            off = off.min(recv_ts.0 - msg.exch_ts.0);
            first.get_or_insert(msg.exch_ts);
            last = last.max(msg.exch_ts);
        }
    }
    anyhow::ensure!(off != i64::MAX, "recording contains no market messages");
    Ok((off, first.unwrap(), last))
}

/// Run arms A and B (in parallel threads) over fresh streams from `open`.
pub fn run_scenario<F>(open: &F, cfg: &ExperimentConfig, sc: &Scenario) -> anyhow::Result<ScenarioResult>
where
    F: Fn() -> anyhow::Result<Box<dyn ItemStream>> + Sync,
{
    let (offset, t0, t_last) = estimate_clock_offset(&mut *open()?)?;
    let (a, b) = std::thread::scope(|s| {
        let ha = s.spawn(|| run_arm(&mut *open()?, cfg, sc, false, offset, t0, t_last));
        let hb = s.spawn(|| run_arm(&mut *open()?, cfg, sc, true, offset, t0, t_last));
        (ha.join().expect("arm A panicked"), hb.join().expect("arm B panicked"))
    });
    Ok(ScenarioResult { scenario: sc.clone(), clock_offset_ns: offset, a: a?, b: b? })
}

struct Faults {
    delays: Vec<(Ts, Ts, Ts)>,
    outages: Vec<(Ts, Ts)>,
    stalls: Vec<(Ts, Ts)>,
    stall_count: Vec<u64>,
    drain: Ts,
}

impl Faults {
    fn new(sc: &Scenario, t0: Ts, t_last: Ts) -> Faults {
        let at = |s: f64| Ts(t0.0 + (s * 1e9) as i64);
        // Tile each window across the run if `repeat_every_s` is set.
        let tile = |start: f64, end: f64| -> Vec<(Ts, Ts)> {
            let mut v = vec![];
            let mut k = 0.0;
            loop {
                let off = sc.repeat_every_s.map_or(0.0, |p| p * k);
                let (a, b) = (at(start + off), at(end + off));
                if a > t_last || (k > 0.0 && sc.repeat_every_s.is_none()) {
                    break;
                }
                v.push((a, b));
                if sc.repeat_every_s.is_none_or(|p| p <= 0.0) {
                    break;
                }
                k += 1.0;
            }
            v
        };
        let delays: Vec<_> = sc
            .feed_delay
            .iter()
            .flat_map(|w| tile(w.start_s, w.end_s).into_iter().map(move |(a, b)| (a, b, Ts((w.extra_ms * 1e6) as i64))))
            .collect();
        let outages: Vec<_> = sc.outages.iter().flat_map(|w| tile(w.start_s, w.end_s)).collect();
        let stalls: Vec<_> = sc.stalls.iter().flat_map(|w| tile(w.start_s, w.end_s)).collect();
        Faults {
            stall_count: vec![0; stalls.len()],
            delays,
            outages,
            stalls,
            drain: Ts((sc.stall_drain_us_per_msg * 1e3) as i64),
        }
    }

    /// Visible time for a message nominally visible at `t`, or `None` if lost in an outage.
    fn visible(&mut self, mut t: Ts) -> Option<Ts> {
        for &(a, b, extra) in &self.delays {
            if t >= a && t < b {
                t = t + extra;
            }
        }
        if self.outages.iter().any(|&(a, b)| t >= a && t < b) {
            return None;
        }
        for (i, &(a, b)) in self.stalls.iter().enumerate() {
            if t >= a && t < b {
                self.stall_count[i] += 1;
                t = Ts(b.0 + self.drain.0 * self.stall_count[i] as i64);
            }
        }
        Some(t)
    }

    /// If `t` falls in a stall, when the strategy thread becomes free again.
    fn stalled_until(&self, t: Ts) -> Option<Ts> {
        self.stalls.iter().find(|&&(a, b)| t >= a && t < b).map(|&(_, b)| b)
    }
}

struct Fnv(u64);
impl Fnv {
    fn mix(&mut self, v: u64) {
        for b in v.to_le_bytes() {
            self.0 ^= b as u64;
            self.0 = self.0.wrapping_mul(0x100000001b3);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn run_arm(
    src: &mut dyn ItemStream,
    cfg: &ExperimentConfig,
    sc: &Scenario,
    guarded: bool,
    offset: i64,
    t0: Ts,
    t_last: Ts,
) -> anyhow::Result<ArmResult> {
    let wall = Instant::now();
    let mut mm_cfg = cfg.mm.clone();
    mm_cfg.stale_guard = guarded.then(|| cfg.guard.clone());
    let arm = if guarded { "B (stale guard)" } else { "A (baseline)" };
    let mut strat = MarketMaker::new(arm, mm_cfg);
    let mut sim = ExchangeSim::new(sc.sim_config(&cfg.sim));
    let resp = sim.response_latency();

    let rel = |s: f64| Ts(t0.0 + (s * 1e9) as i64);
    let (active_start, mut active_end) = match &cfg.active {
        Some(w) => (rel(w.start_s), rel(w.end_s)),
        None => (t0, Ts::MAX),
    };
    active_end = active_end.min(t_last + Ts::from_millis(1));
    let base_lat = Ts((cfg.base_feed_latency_ms * 1e6) as i64);
    let mut faults = Faults::new(sc, t0, t_last);
    let snap_lat = Ts((sc.snapshot_latency_ms * 1e6) as i64);

    let mut sched: Scheduler<Ev> = Scheduler::new();
    for &(_, b) in &faults.outages {
        sched.push(b, P_VIS, Ev::VisReset);
        sched.push(b + snap_lat, P_SNAP, Ev::SnapTake);
    }
    sched.push(active_start, P_TIMER, Ev::Timer);

    let mut exch = Feed::new(1 << 20);
    let mut vis = Feed::new(1 << 20);
    let mut fev: Vec<FeedEvent> = Vec::with_capacity(64);
    let mut sev: Vec<OrderEvt> = Vec::with_capacity(8);
    let mut reqs: Vec<OrderReq> = Vec::with_capacity(8);

    let mut fills: Vec<FillRec> = vec![];
    let mut markouts: [VecDeque<(Ts, usize)>; 3] = Default::default();
    let mut equity: Vec<EquityPoint> = vec![];
    let sample_iv = Ts((cfg.equity_sample_ms * 1e6) as i64);
    let mut next_sample = active_start;
    let mut lag = hdrhistogram::Histogram::<u64>::new_with_bounds(1, 3_600_000_000, 3)?;

    let mut pause = PauseStats::default();
    let mut pause_state: Option<Pause> = Some(Pause::Essential);
    let mut pause_since = active_start;

    let mut last_tex = Ts(i64::MIN);
    let mut last_tvis = Ts(i64::MIN);
    let mut last_msg_at = Ts::ZERO;
    let mut last_lag = Ts::ZERO;
    let mut md_msgs = 0u64;
    let (mut same_ts_at, mut same_ts_count) = (Ts(i64::MIN), 0u32);
    let mut lost = 0u64;

    // Exchange-side time of each item:
    // * book-affecting messages keep stream order (their times are clamped to be
    //   non-decreasing), because the book must be applied in sequence;
    // * trade prints use their true engine time even when they arrive late in a
    //   batch, so fills and cancels are resolved in the order they really happened.
    // Reading runs `reorder_window` ahead of the clock (on the receive axis), so a
    // trade printed up to that long before it was received is still scheduled in time.
    let reorder = Ts((cfg.reorder_window_ms * 1e6) as i64);
    let read_next = |src: &mut dyn ItemStream, last_tex: &mut Ts| -> anyhow::Result<Option<(Ts, Ts, Item)>> {
        let Some(item) = src.next_item()? else {
            return Ok(None);
        };
        let arrival = Ts(item.recv_ts().0 - offset);
        let tex = match &item {
            Item::Md { msg, .. } if matches!(msg.event, MdEvent::TradePrint { .. }) => msg.exch_ts,
            Item::Md { msg, .. } if msg.exch_ts > Ts::ZERO => {
                *last_tex = msg.exch_ts.max(*last_tex);
                *last_tex
            }
            _ => {
                *last_tex = arrival.max(*last_tex);
                *last_tex
            }
        };
        Ok(Some((tex, arrival, item)))
    };
    let mut pending = read_next(src, &mut last_tex)?;
    let mut late_trades = 0u64;

    macro_rules! ctx {
        ($now:expr, $msg_ts:expr) => {
            Ctx {
                now: $now,
                book: vis.book(),
                book_valid: vis.is_live(),
                msg_exch_ts: $msg_ts,
                last_lag,
                last_heartbeat_exch_ts: vis.last_heartbeat_exch_ts(),
                last_msg_at,
            }
        };
    }
    macro_rules! send {
        ($now:expr) => {
            for r in reqs.drain(..) {
                if let OrderReq::Cancel { cl_id } = r {
                    sim.cancel_sent(cl_id);
                }
                sched.push($now + sim.request_latency(&r), P_ARRIVE, Ev::Arrive(r));
            }
        };
    }
    macro_rules! track_pause {
        ($now:expr) => {
            let p = strat.paused();
            if p != pause_state {
                let d = ($now - pause_since).0;
                match pause_state {
                    Some(Pause::Essential) => pause.essential_ns += d,
                    Some(Pause::Stale) => pause.stale_ns += d,
                    None => {}
                }
                pause_state = p;
                pause_since = $now;
            }
        };
    }

    loop {
        while let Some((_, arrival, _)) = &pending {
            if sched.peek_ts().is_some_and(|t| t + reorder < *arrival) {
                break;
            }
            let (tex, _, item) = pending.take().unwrap();
            if tex < sched.now() {
                late_trades += 1; // arrived later than the reorder window; processed now
            }
            let nominal_vis = Ts(item.recv_ts().0 - offset + base_lat.0).max(tex);
            let tvis = faults.visible(nominal_vis).map(|t| t.max(last_tvis));
            match item {
                Item::Md { msg, .. } => {
                    md_msgs += 1;
                    match tvis {
                        Some(tv) => {
                            last_tvis = tv;
                            sched.push(tv, P_VIS, Ev::VisMd(msg.clone()));
                        }
                        None => lost += 1,
                    }
                    sched.push(tex, P_EXCH, Ev::ExchMd(msg));
                }
                Item::Disconnect { .. } => {
                    sched.push(tex, P_EXCH, Ev::ExchReset);
                    let tv = Ts(nominal_vis.0.max(last_tvis.0));
                    last_tvis = tv;
                    sched.push(tv, P_VIS, Ev::VisReset);
                }
                _ => {}
            }
            pending = read_next(src, &mut last_tex)?;
        }
        let Some((now, ev)) = sched.pop() else { break };
        if now >= active_end {
            break;
        }
        // Livelock guard: a strategy/simulator loop that never advances time.
        if now == same_ts_at {
            same_ts_count += 1;
            anyhow::ensure!(same_ts_count < 1_000_000, "livelock: >1M events at t={}", now.0);
        } else {
            same_ts_at = now;
            same_ts_count = 0;
        }
        let active = now >= active_start;
        match ev {
            Ev::ExchMd(msg) => {
                let mid_before = if exch.is_live() { exch.book().mid_x2() } else { None };
                if let Some(mid) = mid_before {
                    for (h, q) in markouts.iter_mut().enumerate() {
                        while q.front().is_some_and(|&(t, _)| t <= now) {
                            let (_, i) = q.pop_front().unwrap();
                            fills[i].mid_x2_after[h] = Some(mid);
                        }
                    }
                    if active {
                        while next_sample <= now {
                            let p = &sim.portfolio;
                            equity.push(EquityPoint {
                                t_ns: next_sample.0,
                                equity: p.equity_at_mid(mid).0,
                                gross: p.equity_at_mid(mid).0 + p.fees_paid.0,
                                inventory: p.inventory.0,
                            });
                            next_sample = next_sample + sample_iv;
                        }
                    }
                }
                fev.clear();
                exch.on_msg(&msg, &mut fev);
                for e in &fev {
                    if let FeedEvent::Trade(t) = e {
                        sev.clear();
                        sim.on_trade(t, &mut sev);
                        for f in sev.drain(..) {
                            if let OrderEvt::Fill { side, px, qty, .. } = f {
                                let i = fills.len();
                                fills.push(FillRec {
                                    t_ns: now.0,
                                    side,
                                    px: px.0,
                                    qty: qty.0,
                                    mid_x2: mid_before.unwrap_or(2 * px.0),
                                    mid_x2_after: [None; 3],
                                    during_pause: strat.paused().is_some(),
                                });
                                for (h, ms) in MARKOUT_HORIZONS_MS.iter().enumerate() {
                                    markouts[h].push_back((now + Ts::from_millis(*ms), i));
                                }
                            }
                            sched.push(now + resp, P_DELIVER, Ev::Deliver(f));
                        }
                    }
                }
            }
            Ev::ExchReset => {
                fev.clear();
                exch.reset(&mut fev);
            }
            Ev::Arrive(req) => {
                sev.clear();
                let book = exch.is_live().then(|| exch.book());
                sim.on_arrival(now, req, book, &mut sev);
                for e in sev.drain(..) {
                    sched.push(now + resp, P_DELIVER, Ev::Deliver(e));
                }
            }
            Ev::VisMd(msg) => {
                fev.clear();
                vis.on_msg(&msg, &mut fev);
                last_msg_at = now;
                // Batched trade prints and snapshots don't measure feed lag.
                let msg_ts = (msg.exch_ts > Ts::ZERO && msg.event.is_timely()).then_some(msg.exch_ts);
                if let Some(t) = msg_ts {
                    last_lag = now - t;
                    lag.saturating_record((last_lag.0 / 1000).max(1) as u64);
                }
                if active {
                    strat.on_market(&ctx!(now, msg_ts), &mut reqs);
                    send!(now);
                    track_pause!(now);
                }
            }
            Ev::VisReset => {
                fev.clear();
                vis.reset(&mut fev);
                if active {
                    strat.on_market(&ctx!(now, None), &mut reqs);
                    send!(now);
                    track_pause!(now);
                }
            }
            Ev::SnapTake => {
                if exch.is_live() {
                    let s = exch.book().to_snapshot(exch.last_seq());
                    let msg = MdMsg { seq: s.seq, exch_ts: Ts::ZERO, event: MdEvent::Snapshot(Arc::new(s)) };
                    fev.clear();
                    vis.on_msg(&msg, &mut fev);
                    last_msg_at = now; // a snapshot is fresh data for the watchdog
                    if active {
                        strat.on_market(&ctx!(now, None), &mut reqs);
                        send!(now);
                        track_pause!(now);
                    }
                } else {
                    sched.push(now + Ts::from_millis(1000), P_SNAP, Ev::SnapTake);
                }
            }
            Ev::Deliver(evt) => {
                if let Some(until) = faults.stalled_until(now) {
                    sched.push(until, P_DELIVER, Ev::Deliver(evt));
                    continue;
                }
                strat.on_order(&ctx!(now, None), &evt, &mut reqs);
                send!(now);
                track_pause!(now);
            }
            Ev::Timer => {
                if let Some(until) = faults.stalled_until(now) {
                    sched.push(until, P_TIMER, Ev::Timer);
                    continue;
                }
                strat.on_timer(&ctx!(now, None), &mut reqs);
                send!(now);
                track_pause!(now);
                sched.push(now + strat.timer_interval(), P_TIMER, Ev::Timer);
            }
        }
    }

    let end = sched.now().min(active_end).max(active_start);
    let d = (end - pause_since).0;
    match pause_state {
        Some(Pause::Essential) => pause.essential_ns += d,
        Some(Pause::Stale) => pause.stale_ns += d,
        None => {}
    }
    pause.active_ns = (end - active_start).0;

    let final_mid = if exch.is_live() { exch.book().mid_x2() } else { None };
    let mut digest = Fnv(0xcbf29ce484222325);
    for f in &fills {
        digest.mix(f.t_ns as u64);
        digest.mix(f.px as u64 ^ (f.qty as u64).rotate_left(32) ^ f.side as u64);
    }
    let s = sim.stats;
    for v in [s.placed, s.acked, s.rejected, s.canceled, s.cancel_rejects, s.fills, md_msgs] {
        digest.mix(v);
    }
    let q = |p: f64| lag.value_at_quantile(p);
    Ok(ArmResult {
        arm: arm.into(),
        guarded,
        final_equity_mid: final_mid.map(|m| sim.portfolio.equity_at_mid(m).0),
        liquidation_equity: if exch.is_live() {
            liquidation_value(&sim.portfolio, exch.book()).map(|c: Cash| c.0)
        } else {
            None
        },
        final_mid_x2: final_mid,
        portfolio: sim.portfolio,
        sim: sim.stats,
        orders_sent: strat.orders_sent,
        cancels_sent: strat.cancels_sent,
        guard_trips: strat.guard_trips(),
        pause,
        md_msgs,
        late_trades,
        vis_lost_in_outage: lost,
        visible_lag: LagStats {
            samples: lag.len(),
            p50_us: q(0.5),
            p99_us: q(0.99),
            p999_us: q(0.999),
            max_us: lag.max(),
        },
        start_ns: active_start.0,
        end_ns: end.0,
        wall_ms: wall.elapsed().as_millis() as u64,
        digest: format!("{:016x}", digest.0),
        fills,
        equity,
    })
}
