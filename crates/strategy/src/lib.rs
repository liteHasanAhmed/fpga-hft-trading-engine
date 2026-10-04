//! Strategies see only the *strategy-visible* market (delayed by feed latency and
//! injected faults) and their own order events (delayed by response latency).
//! They never see the exchange-side book or the future.

mod guard;
mod mm;

pub use guard::{GuardState, StaleGuard, StaleGuardConfig};
pub use mm::{MarketMaker, MmConfig};

use hft_book::Book;
use hft_core::{OrderEvt, OrderReq, Ts};

/// Everything a strategy may observe at a decision point.
pub struct Ctx<'a> {
    /// Virtual time of this decision.
    pub now: Ts,
    /// Strategy-visible book. Meaningful only when `book_valid`.
    pub book: &'a Book,
    /// Feed synchronized (no gap/disconnect/resync in progress).
    pub book_valid: bool,
    /// Exchange timestamp of the market message just delivered, if it is a timely
    /// message (book update or heartbeat — not a batched trade print or snapshot).
    pub msg_exch_ts: Option<Ts>,
    /// Lag (`delivery − exch_ts`) of the most recent timely message.
    pub last_lag: Ts,
    /// Exchange time of the newest heartbeat received.
    pub last_heartbeat_exch_ts: Ts,
    /// Virtual time at which the most recent market message was delivered.
    pub last_msg_at: Ts,
}

/// Why a strategy is not quoting at a given moment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub enum Pause {
    /// Essential (both arms): book invalid or watchdog expired.
    Essential,
    /// Stale-data protection (arm B only).
    Stale,
}

pub trait Strategy {
    fn name(&self) -> &str;
    fn on_market(&mut self, ctx: &Ctx<'_>, out: &mut Vec<OrderReq>);
    fn on_order(&mut self, ctx: &Ctx<'_>, evt: &OrderEvt, out: &mut Vec<OrderReq>);
    fn on_timer(&mut self, ctx: &Ctx<'_>, out: &mut Vec<OrderReq>);
    fn timer_interval(&self) -> Ts;
    /// Current pause state (for time-paused metrics).
    fn paused(&self) -> Option<Pause>;
}
