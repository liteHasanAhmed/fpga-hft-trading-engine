//! Shared contracts between every module of the system.
//!
//! Everything numeric that touches prices, sizes or money is fixed-point integer
//! arithmetic. Floating point is only used for reporting.

pub mod fixed;
pub mod ids;
pub mod instrument;
pub mod market;
pub mod order;
pub mod time;

pub use fixed::{Cash, Px, Qty};
pub use ids::{ClOrdId, OrderId};
pub use instrument::Instrument;
pub use market::{L2Snapshot, L3Snapshot, MdEvent, MdMsg, SnapshotOrder, Trade};
pub use order::{Liquidity, OrderEvt, OrderReq, RejectReason};
pub use time::Ts;

/// Order-book side.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub enum Side {
    Bid,
    Ask,
}

impl Side {
    #[inline]
    pub fn opposite(self) -> Side {
        match self {
            Side::Bid => Side::Ask,
            Side::Ask => Side::Bid,
        }
    }

    /// +1 for bids (buying increases inventory), -1 for asks.
    #[inline]
    pub fn sign(self) -> i64 {
        match self {
            Side::Bid => 1,
            Side::Ask => -1,
        }
    }
}
