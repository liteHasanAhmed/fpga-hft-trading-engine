//! Instrument metadata.

use crate::fixed::{format_scaled, parse_scaled};
use crate::{Cash, Px, Qty};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Instrument {
    /// Exchange symbol, e.g. `BTC-USD`.
    pub symbol: String,
    /// Decimal places of one price tick (BTC-USD quote_increment 0.01 → 2).
    pub px_decimals: u32,
    /// Decimal places of one size lot (BTC-USD base_increment 1e-8 → 8).
    pub qty_decimals: u32,
    /// Minimum order size in lots.
    pub min_qty: Qty,
}

impl Instrument {
    /// Coinbase BTC-USD as documented by `GET /products/BTC-USD`
    /// (quote_increment 0.01, base_increment 0.00000001, base_min_size 0.00000001).
    pub fn btc_usd() -> Instrument {
        Instrument { symbol: "BTC-USD".into(), px_decimals: 2, qty_decimals: 8, min_qty: Qty(1) }
    }

    #[inline]
    pub fn parse_px(&self, s: &[u8]) -> Option<Px> {
        parse_scaled(s, self.px_decimals).map(Px)
    }
    #[inline]
    pub fn parse_qty(&self, s: &[u8]) -> Option<Qty> {
        parse_scaled(s, self.qty_decimals).map(Qty)
    }
    /// Decimal places of [`Cash`] units.
    #[inline]
    pub fn cash_decimals(&self) -> u32 {
        self.px_decimals + self.qty_decimals
    }

    pub fn fmt_px(&self, p: Px) -> String {
        format_scaled(p.0 as i128, self.px_decimals)
    }
    pub fn fmt_qty(&self, q: Qty) -> String {
        format_scaled(q.0 as i128, self.qty_decimals)
    }
    pub fn fmt_cash(&self, c: Cash) -> String {
        format_scaled(c.0, self.cash_decimals())
    }

    /// Cash as f64 in quote currency, for reporting only.
    pub fn cash_f64(&self, c: Cash) -> f64 {
        c.0 as f64 / 10f64.powi(self.cash_decimals() as i32)
    }
    pub fn px_f64(&self, p: Px) -> f64 {
        p.0 as f64 / 10f64.powi(self.px_decimals as i32)
    }
    pub fn qty_f64(&self, q: Qty) -> f64 {
        q.0 as f64 / 10f64.powi(self.qty_decimals as i32)
    }
}
