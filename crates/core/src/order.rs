//! Our order requests and the simulator's responses.

use crate::{Cash, ClOrdId, Px, Qty, Side, Ts};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderReq {
    /// Post-only limit order: rejected if it would take liquidity on arrival.
    Place {
        cl_id: ClOrdId,
        side: Side,
        px: Px,
        qty: Qty,
    },
    Cancel {
        cl_id: ClOrdId,
    },
}

impl OrderReq {
    pub fn cl_id(&self) -> ClOrdId {
        match *self {
            OrderReq::Place { cl_id, .. } | OrderReq::Cancel { cl_id } => cl_id,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RejectReason {
    WouldCross,
    BelowMinSize,
    InvalidPrice,
    DuplicateId,
    /// Book unknown at the exchange image (recording gap) — no basis for acceptance.
    MarketUnavailable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Liquidity {
    Maker,
    Taker,
}

/// Every event carries `ts`: the time it happened at the exchange.
/// Delivery to the strategy happens later (response latency).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderEvt {
    Ack {
        cl_id: ClOrdId,
        ts: Ts,
    },
    Reject {
        cl_id: ClOrdId,
        ts: Ts,
        reason: RejectReason,
    },
    Fill {
        cl_id: ClOrdId,
        ts: Ts,
        side: Side,
        px: Px,
        qty: Qty,
        fee: Cash,
        liquidity: Liquidity,
        leaves: Qty,
    },
    CancelAck {
        cl_id: ClOrdId,
        ts: Ts,
    },
    /// Cancel arrived after the order was fully filled (or never existed).
    CancelReject {
        cl_id: ClOrdId,
        ts: Ts,
    },
}

impl OrderEvt {
    pub fn cl_id(&self) -> ClOrdId {
        match *self {
            OrderEvt::Ack { cl_id, .. }
            | OrderEvt::Reject { cl_id, .. }
            | OrderEvt::Fill { cl_id, .. }
            | OrderEvt::CancelAck { cl_id, .. }
            | OrderEvt::CancelReject { cl_id, .. } => cl_id,
        }
    }
    pub fn ts(&self) -> Ts {
        match *self {
            OrderEvt::Ack { ts, .. }
            | OrderEvt::Reject { ts, .. }
            | OrderEvt::Fill { ts, .. }
            | OrderEvt::CancelAck { ts, .. }
            | OrderEvt::CancelReject { ts, .. } => ts,
        }
    }
}
