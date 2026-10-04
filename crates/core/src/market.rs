//! Normalized market-data events (L3, per order).

use crate::{OrderId, Px, Qty, Side, Ts};
use std::sync::Arc;

/// One normalized market-data message for a single product.
#[derive(Clone, Debug, PartialEq)]
pub struct MdMsg {
    /// Exchange sequence number. Contiguous per product for book-affecting
    /// messages; heartbeats carry the latest sequence without consuming one.
    pub seq: u64,
    /// Matching-engine time.
    pub exch_ts: Ts,
    pub event: MdEvent,
}

#[derive(Clone, Debug, PartialEq)]
pub enum MdEvent {
    /// Order accepted by the matching engine; not on the book yet.
    Received { order_id: OrderId, side: Side, px: Option<Px>, qty: Option<Qty> },
    /// Order now resting on the book with `remaining` size.
    Open { order_id: OrderId, side: Side, px: Px, remaining: Qty },
    /// Order left the book (or never reached it).
    Done { order_id: OrderId, side: Side, px: Option<Px>, remaining: Option<Qty>, canceled: bool },
    /// Trade. `maker_side` is the side of the resting order.
    Match { trade_id: u64, maker_id: OrderId, taker_id: OrderId, maker_side: Side, px: Px, qty: Qty },
    /// Resting order resized (self-trade prevention) or modified.
    Change { order_id: OrderId, side: Side, new_qty: Qty, new_px: Option<Px> },
    /// Liveness message; `seq` is the latest sequence, not a new one.
    Heartbeat { last_trade_id: u64 },
    /// L2: absolute size at a price level (`qty == 0` removes the level).
    Level { side: Side, px: Px, qty: Qty },
    /// L2: full aggregated book.
    L2Snapshot(Arc<L2Snapshot>),
    /// A trade print with no book effect (L2 venues publish book changes separately).
    TradePrint { trade_id: u64, maker_side: Side, px: Px, qty: Qty },
    /// Full L3 snapshot (REST). `seq` is the snapshot sequence.
    Snapshot(Arc<L3Snapshot>),
}

impl MdEvent {
    /// Whether this message consumes a sequence number in the book stream.
    #[inline]
    ///
    /// L2 events are never sequenced individually: on venues with per-connection
    /// frame sequence numbers, continuity is checked when decoding frames.
    pub fn is_sequenced(&self) -> bool {
        matches!(
            self,
            MdEvent::Received { .. }
                | MdEvent::Open { .. }
                | MdEvent::Done { .. }
                | MdEvent::Match { .. }
                | MdEvent::Change { .. }
        )
    }

    /// Whether this message reflects exchange state at its `exch_ts` with no
    /// batching delay (used for feed-lag measurement; batched trade prints are excluded).
    pub fn is_timely(&self) -> bool {
        !matches!(self, MdEvent::TradePrint { .. } | MdEvent::Snapshot(_) | MdEvent::L2Snapshot(_))
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct L3Snapshot {
    pub seq: u64,
    pub bids: Vec<SnapshotOrder>,
    pub asks: Vec<SnapshotOrder>,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct L2Snapshot {
    /// Best first.
    pub bids: Vec<(Px, Qty)>,
    /// Best first.
    pub asks: Vec<(Px, Qty)>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SnapshotOrder {
    pub order_id: OrderId,
    pub px: Px,
    pub qty: Qty,
}

/// A trade as seen by downstream consumers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Trade {
    pub exch_ts: Ts,
    pub trade_id: u64,
    pub px: Px,
    pub qty: Qty,
    /// Side of the resting (maker) order. A trade with `maker_side == Bid` was a
    /// sell aggressor hitting bids.
    pub maker_side: Side,
}
