//! Baseline market maker.
//!
//! * Reference price: mid of the visible book.
//! * Quotes: bid at `floor(mid - half_spread - skew)`, ask at `ceil(mid + half_spread - skew)`,
//!   never crossing the visible opposite best (post-only).
//! * Inventory skew: `skew = skew_ticks_per_unit × inventory / quote_qty` ticks.
//! * Hard inventory limit: when the worst case (inventory + open same-side size)
//!   would exceed `max_inventory`, that side is not quoted.
//! * One order per side. To move a quote we cancel and only re-place after the
//!   cancel is acknowledged (never two live orders on one side).
//! * Requote on market updates when the target moves ≥ `requote_ticks`, and on a timer.
//!
//! Essential checks (both arms): book invalid → cancel all; no visible market
//! message for `watchdog` → cancel all.

use crate::{Ctx, Pause, StaleGuard, StaleGuardConfig, Strategy};
use hft_core::{ClOrdId, OrderEvt, OrderReq, Px, Qty, Side, Ts};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MmConfig {
    /// Half spread around mid, in half-ticks (1 = join a 1-tick spread).
    pub half_spread_half_ticks: i64,
    pub quote_qty: Qty,
    pub max_inventory: Qty,
    /// Skew in half-ticks per `quote_qty` of inventory.
    pub skew_half_ticks_per_unit: i64,
    pub requote_ticks: i64,
    pub timer_ns: i64,
    /// Essential watchdog: no market data for this long → cancel all.
    pub watchdog_ns: i64,
    /// After a reject, don't re-place that side for this long (prevents a
    /// reject/re-place livelock when our view of the book is wrong).
    pub reject_cooldown_ns: i64,
    /// Present only in arm B.
    pub stale_guard: Option<StaleGuardConfig>,
}

impl Default for MmConfig {
    fn default() -> Self {
        MmConfig {
            half_spread_half_ticks: 1,
            quote_qty: Qty(1_000_000), // 0.01 BTC
            max_inventory: Qty(5_000_000),
            skew_half_ticks_per_unit: 1,
            requote_ticks: 1,
            timer_ns: 100_000_000,
            watchdog_ns: 5_000_000_000,
            reject_cooldown_ns: 100_000_000,
            stale_guard: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OState {
    PendingNew,
    Live,
    PendingCancel,
}

#[derive(Clone, Copy, Debug)]
struct Own {
    id: ClOrdId,
    px: Px,
    leaves: Qty,
    state: OState,
}

pub struct MarketMaker {
    name: String,
    cfg: MmConfig,
    guard: Option<StaleGuard>,
    next_id: u64,
    bid: Option<Own>,
    ask: Option<Own>,
    inventory: Qty,
    /// Per side (bid, ask): no new orders before this time.
    cooldown_until: [Ts; 2],
    paused: Option<Pause>,
    pub cancels_sent: u64,
    pub orders_sent: u64,
}

impl MarketMaker {
    pub fn new(name: &str, cfg: MmConfig) -> MarketMaker {
        MarketMaker {
            name: name.into(),
            guard: cfg.stale_guard.clone().map(StaleGuard::new),
            cfg,
            next_id: 1,
            bid: None,
            ask: None,
            inventory: Qty::ZERO,
            cooldown_until: [Ts(i64::MIN); 2],
            paused: Some(Pause::Essential),
            cancels_sent: 0,
            orders_sent: 0,
        }
    }

    pub fn guard_trips(&self) -> u64 {
        self.guard.as_ref().map_or(0, |g| g.trips)
    }

    fn slot(&mut self, side: Side) -> &mut Option<Own> {
        match side {
            Side::Bid => &mut self.bid,
            Side::Ask => &mut self.ask,
        }
    }

    fn cancel_side(&mut self, side: Side, out: &mut Vec<OrderReq>) {
        let mut sent = false;
        if let Some(o) = self.slot(side)
            && o.state != OState::PendingCancel
        {
            o.state = OState::PendingCancel;
            out.push(OrderReq::Cancel { cl_id: o.id });
            sent = true;
        }
        if sent {
            self.cancels_sent += 1;
        }
    }

    fn decide(&mut self, ctx: &Ctx<'_>, out: &mut Vec<OrderReq>) {
        let essential_ok = ctx.book_valid && (ctx.now - ctx.last_msg_at).0 <= self.cfg.watchdog_ns;
        // The guard is updated on every decision so its state machine sees time pass.
        let guard_ok = match &mut self.guard {
            Some(g) if essential_ok => g.update(ctx),
            _ => true,
        };
        self.paused = if !essential_ok {
            Some(Pause::Essential)
        } else if !guard_ok {
            Some(Pause::Stale)
        } else {
            None
        };
        if self.paused.is_some() {
            self.cancel_side(Side::Bid, out);
            self.cancel_side(Side::Ask, out);
            return;
        }
        let (Some((bb, _)), Some((ba, _))) = (ctx.book.best_bid(), ctx.book.best_ask()) else {
            self.cancel_side(Side::Bid, out);
            self.cancel_side(Side::Ask, out);
            return;
        };
        let mid_x2 = bb.0 + ba.0;
        let skew_x2 = self.cfg.skew_half_ticks_per_unit * self.inventory.0 / self.cfg.quote_qty.0.max(1);
        let bid_x2 = mid_x2 - self.cfg.half_spread_half_ticks - skew_x2;
        let ask_x2 = mid_x2 + self.cfg.half_spread_half_ticks - skew_x2;
        // `*_x2` are prices in half-ticks; round bids down and asks up (away from mid).
        let bid_px = Px(bid_x2.div_euclid(2)).min(Px(ba.0 - 1));
        let ask_px = Px((ask_x2 + 1).div_euclid(2)).max(Px(bb.0 + 1));

        // One order per side, so worst-case inventory if it fills is inventory ± q.
        let q = self.cfg.quote_qty;
        let max = self.cfg.max_inventory;
        let want_bid = self.inventory + q <= max;
        let want_ask = -(self.inventory - q) <= max;

        for (side, target, want) in [(Side::Bid, bid_px, want_bid), (Side::Ask, ask_px, want_ask)] {
            let cooled = ctx.now >= self.cooldown_until[side as usize];
            match *self.slot(side) {
                Some(o) if o.state == OState::Live => {
                    if !want || (o.px.0 - target.0).abs() >= self.cfg.requote_ticks {
                        self.cancel_side(side, out);
                    }
                }
                Some(_) => {} // pending new or pending cancel: wait for the response
                None if want && cooled => {
                    let id = ClOrdId(self.next_id);
                    self.next_id += 1;
                    *self.slot(side) = Some(Own { id, px: target, leaves: q, state: OState::PendingNew });
                    self.orders_sent += 1;
                    out.push(OrderReq::Place { cl_id: id, side, px: target, qty: q });
                }
                None => {}
            }
        }
    }

    fn side_of(&self, id: ClOrdId) -> Option<Side> {
        if self.bid.is_some_and(|o| o.id == id) {
            Some(Side::Bid)
        } else if self.ask.is_some_and(|o| o.id == id) {
            Some(Side::Ask)
        } else {
            None
        }
    }
}

impl Strategy for MarketMaker {
    fn name(&self) -> &str {
        &self.name
    }

    fn on_market(&mut self, ctx: &Ctx<'_>, out: &mut Vec<OrderReq>) {
        self.decide(ctx, out);
    }

    fn on_order(&mut self, ctx: &Ctx<'_>, evt: &OrderEvt, out: &mut Vec<OrderReq>) {
        if let OrderEvt::Fill { side, qty, .. } = *evt {
            // Inventory is tracked from fills even if the order slot was already cleared.
            self.inventory += Qty(side.sign() * qty.0);
        }
        let Some(side) = self.side_of(evt.cl_id()) else {
            return;
        };
        let slot = self.slot(side);
        match *evt {
            OrderEvt::Ack { .. } => {
                if let Some(o) = slot
                    && o.state == OState::PendingNew
                {
                    o.state = OState::Live;
                }
            }
            OrderEvt::Fill { leaves, .. } => {
                if leaves.is_zero() {
                    *slot = None;
                } else if let Some(o) = slot {
                    o.leaves = leaves;
                }
            }
            OrderEvt::Reject { .. } => {
                *slot = None;
                self.cooldown_until[side as usize] = ctx.now + Ts(self.cfg.reject_cooldown_ns);
            }
            OrderEvt::CancelAck { .. } | OrderEvt::CancelReject { .. } => *slot = None,
        }
        self.decide(ctx, out);
    }

    fn on_timer(&mut self, ctx: &Ctx<'_>, out: &mut Vec<OrderReq>) {
        self.decide(ctx, out);
    }

    fn timer_interval(&self) -> Ts {
        Ts(self.cfg.timer_ns)
    }

    fn paused(&self) -> Option<Pause> {
        self.paused
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hft_book::Book;
    use hft_core::OrderId;

    fn book(bid: i64, ask: i64) -> Book {
        let mut b = Book::new();
        b.open(OrderId(1), Side::Bid, Px(bid), Qty(1)).unwrap();
        b.open(OrderId(2), Side::Ask, Px(ask), Qty(1)).unwrap();
        b
    }
    fn ctx(b: &Book, now: i64, msg: Option<i64>, hb: i64) -> Ctx<'_> {
        Ctx {
            now: Ts(now),
            book: b,
            book_valid: true,
            msg_exch_ts: msg.map(Ts),
            last_lag: Ts(now - msg.unwrap_or(now)),
            last_heartbeat_exch_ts: Ts(hb),
            last_msg_at: Ts(now),
        }
    }

    #[test]
    fn joins_one_tick_spread_and_skews() {
        let b = book(100, 101);
        let mut mm =
            MarketMaker::new("A", MmConfig { quote_qty: Qty(10), max_inventory: Qty(30), ..Default::default() });
        let mut out = vec![];
        mm.on_market(&ctx(&b, 1, Some(1), 1), &mut out);
        assert_eq!(
            out,
            vec![
                OrderReq::Place { cl_id: ClOrdId(1), side: Side::Bid, px: Px(100), qty: Qty(10) },
                OrderReq::Place { cl_id: ClOrdId(2), side: Side::Ask, px: Px(101), qty: Qty(10) },
            ]
        );
        // Bid fills fully → long 10 → skew down by 1 half-tick: bid 99.5→99, ask 100.5→101.
        out.clear();
        let c = ctx(&b, 2, None, 1);
        mm.on_order(&c, &OrderEvt::Ack { cl_id: ClOrdId(1), ts: Ts(1) }, &mut out);
        mm.on_order(&c, &OrderEvt::Ack { cl_id: ClOrdId(2), ts: Ts(1) }, &mut out);
        let fill = OrderEvt::Fill {
            cl_id: ClOrdId(1),
            ts: Ts(2),
            side: Side::Bid,
            px: Px(100),
            qty: Qty(10),
            fee: hft_core::Cash(0),
            liquidity: hft_core::Liquidity::Maker,
            leaves: Qty(0),
        };
        out.clear();
        mm.on_order(&c, &fill, &mut out);
        assert_eq!(out, vec![OrderReq::Place { cl_id: ClOrdId(3), side: Side::Bid, px: Px(99), qty: Qty(10) }]);
    }

    #[test]
    fn inventory_limit_stops_side() {
        let b = book(100, 101);
        let mut mm = MarketMaker::new(
            "A",
            MmConfig { quote_qty: Qty(10), max_inventory: Qty(10), skew_half_ticks_per_unit: 0, ..Default::default() },
        );
        let mut out = vec![];
        let c = ctx(&b, 1, Some(1), 1);
        mm.on_market(&c, &mut out);
        let fill = OrderEvt::Fill {
            cl_id: ClOrdId(1),
            ts: Ts(2),
            side: Side::Bid,
            px: Px(100),
            qty: Qty(10),
            fee: hft_core::Cash(0),
            liquidity: hft_core::Liquidity::Maker,
            leaves: Qty(0),
        };
        out.clear();
        mm.on_order(&c, &fill, &mut out);
        assert!(out.iter().all(|r| !matches!(r, OrderReq::Place { side: Side::Bid, .. })), "{out:?}");
    }

    #[test]
    fn invalid_book_cancels_in_both_arms() {
        for guard in [None, Some(StaleGuardConfig::default())] {
            let b = book(100, 101);
            let mut mm = MarketMaker::new("x", MmConfig { stale_guard: guard, ..Default::default() });
            let mut out = vec![];
            mm.on_market(&ctx(&b, 1, Some(1), 1), &mut out);
            let c = ctx(&b, 2, None, 1);
            mm.on_order(&c, &OrderEvt::Ack { cl_id: ClOrdId(1), ts: Ts(1) }, &mut out);
            out.clear();
            let mut c = ctx(&b, 3, Some(3), 1);
            c.book_valid = false;
            mm.on_market(&c, &mut out);
            assert!(out.contains(&OrderReq::Cancel { cl_id: ClOrdId(1) }));
            assert_eq!(mm.paused(), Some(Pause::Essential));
        }
    }

    #[test]
    fn guard_trips_on_lag_and_silence_but_not_quiet_market() {
        let b = book(100, 101);
        let ms = 1_000_000i64;
        let guard = StaleGuardConfig {
            lag_floor_ns: 150 * ms,
            lag_mult: 5.0,
            ewma_alpha: 0.001,
            heartbeat_timeout_ns: 2000 * ms,
            resume_after_ns: 500 * ms,
        };
        let mut mm = MarketMaker::new("B", MmConfig { stale_guard: Some(guard), ..Default::default() });
        let mut out = vec![];
        // Healthy: 10 ms lag.
        mm.on_market(&ctx(&b, 1000 * ms, Some(990 * ms), 990 * ms), &mut out);
        assert_eq!(mm.paused(), None);
        // Quiet market: no messages for 1.5 s but heartbeats fresh → still quoting.
        mm.on_timer(&Ctx { last_msg_at: Ts(1000 * ms), ..ctx(&b, 2500 * ms, None, 2400 * ms) }, &mut out);
        assert_eq!(mm.paused(), None);
        // Lagged message: 400 ms old on arrival → pause.
        mm.on_market(&ctx(&b, 3000 * ms, Some(2600 * ms), 2600 * ms), &mut out);
        assert_eq!(mm.paused(), Some(Pause::Stale));
        // Healthy again, but hysteresis holds the pause for 500 ms.
        mm.on_market(&ctx(&b, 3100 * ms, Some(3090 * ms), 3090 * ms), &mut out);
        assert_eq!(mm.paused(), Some(Pause::Stale));
        mm.on_market(&ctx(&b, 3700 * ms, Some(3690 * ms), 3690 * ms), &mut out);
        assert_eq!(mm.paused(), None);
        // Heartbeat silence of 2.5 s (outage) → pause, even with no lagged message.
        mm.on_timer(
            &Ctx { last_msg_at: Ts(3700 * ms), last_lag: Ts(10 * ms), ..ctx(&b, 6200 * ms, None, 3690 * ms) },
            &mut out,
        );
        assert_eq!(mm.paused(), Some(Pause::Stale));
    }
}
