//! Historical-replay execution simulator.
//!
//! Our orders are *virtual*: they never change the recorded market. The simulator
//! decides acceptance against the exchange-side book at the moment the order
//! arrives, and fills against real trades that print after arrival.
//!
//! ## Fill rule v1 (see `docs/ASSUMPTIONS.md`)
//! A resting buy at `p` fills when a real trade prints with a sell aggressor
//! (`maker_side == Bid`) at a price `<= p` ([`FillModel::TradeTouch`]) or `< p`
//! ([`FillModel::TradeThrough`]). Fill size is `min(leaves, trade size)`, at our
//! limit price, as maker. No queue position, so `TradeTouch` is optimistic about
//! fills at the touch; `TradeThrough` is the conservative bound.
//!
//! ## Limitations (stated in every report)
//! * Touching a price does not guarantee a fill (queue position is ignored in v1).
//! * Our orders have no market impact; the recorded market does not react to us.
//! * A real trade's volume is not consumed by our fill (each trade can fill each of
//!   our orders at most once, capped at the trade's size).

mod accounting;

pub use accounting::{Fees, Portfolio, liquidation_value};

use hft_book::Book;
use hft_core::{Cash, ClOrdId, Liquidity, OrderEvt, OrderReq, Px, Qty, RejectReason, Side, Trade, Ts};
use rand::Rng;
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FillModel {
    /// Fill when a trade prints at or through our price.
    TradeTouch,
    /// Fill only when a trade prints strictly through our price.
    TradeThrough,
    /// Through always fills; at-price trades fill with probability `p` (seeded RNG).
    TouchProbability { p: f64 },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SimConfig {
    pub fill_model: FillModel,
    pub fees: Fees,
    /// Strategy decision → order arrives at exchange.
    pub order_latency_ns: i64,
    /// Strategy decision → cancel arrives at exchange.
    pub cancel_latency_ns: i64,
    /// Exchange event → acknowledgement/fill visible to strategy.
    pub response_latency_ns: i64,
    pub min_qty: Qty,
    pub seed: u64,
}

impl Default for SimConfig {
    fn default() -> Self {
        SimConfig {
            fill_model: FillModel::TradeTouch,
            fees: Fees::PROFESSIONAL,
            order_latency_ns: 0,
            cancel_latency_ns: 0,
            response_latency_ns: 0,
            min_qty: Qty(1),
            seed: 1,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SimOrder {
    side: Side,
    px: Px,
    leaves: Qty,
    /// When the order reached the exchange; only later trades can fill it.
    arrived: Ts,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimStats {
    pub placed: u64,
    pub acked: u64,
    pub rejected: u64,
    pub cancel_requests: u64,
    pub canceled: u64,
    pub cancel_rejects: u64,
    pub fills: u64,
    /// Fills that happened while our cancel was in flight.
    pub fills_while_cancel_pending: u64,
}

pub struct ExchangeSim {
    cfg: SimConfig,
    /// Live orders in arrival order (we have very few; a Vec beats a map).
    orders: Vec<(ClOrdId, SimOrder)>,
    /// Ids ever seen, to reject duplicates.
    seen: rustc_hash::FxHashSet<ClOrdId>,
    /// Cancels sent but not yet arrived (for race statistics only).
    cancels_in_flight: rustc_hash::FxHashSet<ClOrdId>,
    pub portfolio: Portfolio,
    pub stats: SimStats,
    rng: ChaCha8Rng,
}

impl ExchangeSim {
    pub fn new(cfg: SimConfig) -> ExchangeSim {
        let rng = ChaCha8Rng::seed_from_u64(cfg.seed);
        ExchangeSim {
            portfolio: Portfolio::new(cfg.fees),
            cfg,
            orders: Vec::with_capacity(8),
            seen: Default::default(),
            cancels_in_flight: Default::default(),
            stats: SimStats::default(),
            rng,
        }
    }

    pub fn config(&self) -> &SimConfig {
        &self.cfg
    }

    /// Delay from decision to arrival for a request.
    pub fn request_latency(&self, req: &OrderReq) -> Ts {
        Ts(match req {
            OrderReq::Place { .. } => self.cfg.order_latency_ns,
            OrderReq::Cancel { .. } => self.cfg.cancel_latency_ns,
        })
    }
    pub fn response_latency(&self) -> Ts {
        Ts(self.cfg.response_latency_ns)
    }

    /// Note that a cancel has been sent (it will arrive later).
    pub fn cancel_sent(&mut self, id: ClOrdId) {
        self.cancels_in_flight.insert(id);
    }

    /// Open exposure: (sum of live bid leaves, sum of live ask leaves).
    pub fn open_qty(&self) -> (Qty, Qty) {
        let mut b = Qty::ZERO;
        let mut a = Qty::ZERO;
        for (_, o) in &self.orders {
            match o.side {
                Side::Bid => b += o.leaves,
                Side::Ask => a += o.leaves,
            }
        }
        (b, a)
    }

    pub fn live_orders(&self) -> usize {
        self.orders.len()
    }

    /// A request arrives at the exchange at `now`. `book` is the exchange-side book
    /// at that instant, or `None` if the recording has no valid book then.
    pub fn on_arrival(&mut self, now: Ts, req: OrderReq, book: Option<&Book>, out: &mut Vec<OrderEvt>) {
        match req {
            OrderReq::Place { cl_id, side, px, qty } => {
                self.stats.placed += 1;
                let reject = if !self.seen.insert(cl_id) {
                    Some(RejectReason::DuplicateId)
                } else if qty < self.cfg.min_qty {
                    Some(RejectReason::BelowMinSize)
                } else if px.0 <= 0 {
                    Some(RejectReason::InvalidPrice)
                } else if let Some(book) = book {
                    let crosses = match side {
                        Side::Bid => book.best_ask().is_some_and(|(a, _)| px >= a),
                        Side::Ask => book.best_bid().is_some_and(|(b, _)| px <= b),
                    };
                    crosses.then_some(RejectReason::WouldCross)
                } else {
                    Some(RejectReason::MarketUnavailable)
                };
                if let Some(reason) = reject {
                    self.stats.rejected += 1;
                    out.push(OrderEvt::Reject { cl_id, ts: now, reason });
                } else {
                    self.stats.acked += 1;
                    self.orders.push((cl_id, SimOrder { side, px, leaves: qty, arrived: now }));
                    out.push(OrderEvt::Ack { cl_id, ts: now });
                }
            }
            OrderReq::Cancel { cl_id } => {
                self.stats.cancel_requests += 1;
                self.cancels_in_flight.remove(&cl_id);
                if let Some(i) = self.orders.iter().position(|(id, _)| *id == cl_id) {
                    self.orders.remove(i);
                    self.stats.canceled += 1;
                    out.push(OrderEvt::CancelAck { cl_id, ts: now });
                } else {
                    self.stats.cancel_rejects += 1;
                    out.push(OrderEvt::CancelReject { cl_id, ts: now });
                }
            }
        }
    }

    /// A real trade printed at the exchange.
    pub fn on_trade(&mut self, t: &Trade, out: &mut Vec<OrderEvt>) {
        let mut i = 0;
        while i < self.orders.len() {
            let (cl_id, o) = self.orders[i];
            // Our bid can only be hit by a sell aggressor, i.e. a trade against bids.
            let through = match o.side {
                Side::Bid => t.px < o.px,
                Side::Ask => t.px > o.px,
            };
            let at = t.px == o.px;
            let fills = t.maker_side == o.side
                && t.exch_ts >= o.arrived
                && match self.cfg.fill_model {
                    FillModel::TradeTouch => through || at,
                    FillModel::TradeThrough => through,
                    FillModel::TouchProbability { p } => through || (at && self.rng.random_bool(p.clamp(0.0, 1.0))),
                };
            if !fills {
                i += 1;
                continue;
            }
            let qty = o.leaves.min(t.qty);
            let fee = self.portfolio.fill(o.side, o.px, qty, Liquidity::Maker);
            self.stats.fills += 1;
            if self.cancels_in_flight.contains(&cl_id) {
                self.stats.fills_while_cancel_pending += 1;
            }
            let leaves = o.leaves - qty;
            out.push(OrderEvt::Fill {
                cl_id,
                ts: t.exch_ts,
                side: o.side,
                px: o.px,
                qty,
                fee,
                liquidity: Liquidity::Maker,
                leaves,
            });
            if leaves.is_zero() {
                self.orders.remove(i);
            } else {
                self.orders[i].1.leaves = leaves;
                i += 1;
            }
        }
    }

    /// Mark-to-mid equity (cash + inventory at mid). `mid_x2` is bid + ask in ticks.
    pub fn equity_at_mid(&self, mid_x2: i64) -> Cash {
        self.portfolio.equity_at_mid(mid_x2)
    }
}

#[cfg(test)]
mod tests;
