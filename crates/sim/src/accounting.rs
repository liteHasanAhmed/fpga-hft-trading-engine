use hft_book::Book;
use hft_core::{Cash, Liquidity, Px, Qty, Side};
use serde::{Deserialize, Serialize};

/// Fee rates in parts-per-million of notional (0.60% = 6000).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fees {
    pub maker_ppm: i64,
    pub taker_ppm: i64,
}

impl Fees {
    /// Coinbase top volume tier (0% maker / 0.05% taker). Source: docs/PLAN.md §3.
    pub const PROFESSIONAL: Fees = Fees { maker_ppm: 0, taker_ppm: 500 };
    /// Coinbase entry tier (0.60% maker / 1.20% taker).
    pub const RETAIL: Fees = Fees { maker_ppm: 6000, taker_ppm: 12000 };

    /// Fee for a fill, rounded up to the next cash unit (conservative).
    pub fn fee(&self, notional: Cash, liq: Liquidity) -> Cash {
        let ppm = match liq {
            Liquidity::Maker => self.maker_ppm,
            Liquidity::Taker => self.taker_ppm,
        } as i128;
        let n = notional.0.abs() * ppm;
        Cash((n + 999_999) / 1_000_000 * ppm.signum())
    }
}

/// Cash, inventory and fees. Cash is in `tick * lot` units (see `hft_core::Cash`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Portfolio {
    pub fees: Fees,
    /// Cash from trading, before fees.
    pub cash: Cash,
    pub fees_paid: Cash,
    pub inventory: Qty,
    pub bought: Qty,
    pub sold: Qty,
    pub fills: u64,
}

impl Portfolio {
    pub fn new(fees: Fees) -> Portfolio {
        Portfolio {
            fees,
            cash: Cash::default(),
            fees_paid: Cash::default(),
            inventory: Qty::ZERO,
            bought: Qty::ZERO,
            sold: Qty::ZERO,
            fills: 0,
        }
    }

    /// Book a fill; returns the fee charged.
    pub fn fill(&mut self, side: Side, px: Px, qty: Qty, liq: Liquidity) -> Cash {
        let n = px.notional(qty);
        let fee = self.fees.fee(n, liq);
        match side {
            Side::Bid => {
                self.cash -= n;
                self.inventory += qty;
                self.bought += qty;
            }
            Side::Ask => {
                self.cash += n;
                self.inventory -= qty;
                self.sold += qty;
            }
        }
        self.fees_paid += fee;
        self.fills += 1;
        fee
    }

    /// Net cash after fees.
    pub fn net_cash(&self) -> Cash {
        self.cash - self.fees_paid
    }

    /// Equity marked at mid, after fees. `mid_x2` = best bid + best ask (ticks).
    /// Half-unit rounding is toward negative infinity.
    pub fn equity_at_mid(&self, mid_x2: i64) -> Cash {
        let inv_val = (mid_x2 as i128 * self.inventory.0 as i128).div_euclid(2);
        self.net_cash() + Cash(inv_val)
    }
}

/// Equity if the inventory were flattened immediately with a taker order walking
/// the visible book, paying the taker fee. Returns `None` if depth is insufficient.
pub fn liquidation_value(p: &Portfolio, book: &Book) -> Option<Cash> {
    let inv = p.inventory;
    if inv.is_zero() {
        return Some(p.net_cash());
    }
    let (bids, asks) = book.depth(usize::MAX);
    let (levels, sign) = if inv.0 > 0 { (bids, 1i128) } else { (asks, -1i128) };
    let mut left = Qty(inv.0.abs());
    let mut proceeds = Cash::default();
    for (px, q) in levels {
        let take = left.min(q);
        proceeds += px.notional(take);
        left -= take;
        if left.is_zero() {
            break;
        }
    }
    if !left.is_zero() {
        return None;
    }
    let fee = p.fees.fee(proceeds, Liquidity::Taker);
    // Selling inventory adds proceeds; buying back a short subtracts the cost.
    Some(p.net_cash() + Cash(sign * proceeds.0) - fee)
}
