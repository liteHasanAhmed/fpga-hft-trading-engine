//! Fixed-point prices, quantities and cash.
//!
//! * [`Px`] is an integer number of price ticks (BTC-USD: 1 tick = 0.01 USD).
//! * [`Qty`] is an integer number of base lots (BTC-USD: 1 lot = 1e-8 BTC).
//! * [`Cash`] is quote currency in units of `tick * lot` (BTC-USD: 1e-10 USD),
//!   so `px * qty` is exact with no rounding.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::{Add, AddAssign, Neg, Sub, SubAssign};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Px(pub i64);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Qty(pub i64);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Cash(pub i128);

impl Qty {
    pub const ZERO: Qty = Qty(0);
    #[inline]
    pub fn is_zero(self) -> bool {
        self.0 == 0
    }
    #[inline]
    pub fn min(self, o: Qty) -> Qty {
        Qty(self.0.min(o.0))
    }
}

impl Px {
    /// Exact notional `px * qty` in cash units.
    #[inline]
    pub fn notional(self, qty: Qty) -> Cash {
        Cash(self.0 as i128 * qty.0 as i128)
    }
}

macro_rules! arith {
    ($t:ident, $inner:ty) => {
        impl Add for $t {
            type Output = $t;
            #[inline]
            fn add(self, o: $t) -> $t {
                $t(self.0 + o.0)
            }
        }
        impl Sub for $t {
            type Output = $t;
            #[inline]
            fn sub(self, o: $t) -> $t {
                $t(self.0 - o.0)
            }
        }
        impl AddAssign for $t {
            #[inline]
            fn add_assign(&mut self, o: $t) {
                self.0 += o.0
            }
        }
        impl SubAssign for $t {
            #[inline]
            fn sub_assign(&mut self, o: $t) {
                self.0 -= o.0
            }
        }
        impl Neg for $t {
            type Output = $t;
            #[inline]
            fn neg(self) -> $t {
                $t(-self.0)
            }
        }
    };
}
arith!(Px, i64);
arith!(Qty, i64);
arith!(Cash, i128);

/// Parse a decimal string (e.g. `"62345.67"`, `"0.00100000"`, `"-1.5"`) into an
/// integer scaled by `10^decimals`.
///
/// Returns `None` on malformed input, overflow, or if the string has non-zero
/// digits beyond `decimals` places (that would silently lose precision).
pub fn parse_scaled(s: &[u8], decimals: u32) -> Option<i64> {
    let (neg, digits) = match s.first()? {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    if digits.is_empty() {
        return None;
    }
    let mut int_part: i64 = 0;
    let mut frac_part: i64 = 0;
    let mut frac_digits: u32 = 0;
    let mut seen_dot = false;
    let mut seen_digit = false;
    for &c in digits {
        match c {
            b'0'..=b'9' => {
                seen_digit = true;
                let d = (c - b'0') as i64;
                if !seen_dot {
                    int_part = int_part.checked_mul(10)?.checked_add(d)?;
                } else if frac_digits < decimals {
                    frac_part = frac_part * 10 + d;
                    frac_digits += 1;
                } else if d != 0 {
                    return None;
                }
            }
            b'.' if !seen_dot => seen_dot = true,
            _ => return None,
        }
    }
    if !seen_digit {
        return None;
    }
    frac_part *= 10i64.checked_pow(decimals - frac_digits)?;
    let v = int_part.checked_mul(10i64.checked_pow(decimals)?)?.checked_add(frac_part)?;
    Some(if neg { -v } else { v })
}

/// Format a scaled integer back to a decimal string with exactly `decimals` places.
pub fn format_scaled(v: i128, decimals: u32) -> String {
    let neg = v < 0;
    let a = v.unsigned_abs();
    let scale = 10u128.pow(decimals);
    let (i, f) = (a / scale, a % scale);
    let sign = if neg { "-" } else { "" };
    if decimals == 0 { format!("{sign}{i}") } else { format!("{sign}{i}.{f:0width$}", width = decimals as usize) }
}

impl fmt::Display for Px {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}t", self.0)
    }
}
impl fmt::Display for Qty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}l", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn parses_common_forms() {
        assert_eq!(parse_scaled(b"62345.67", 2), Some(6234567));
        assert_eq!(parse_scaled(b"62345.6", 2), Some(6234560));
        assert_eq!(parse_scaled(b"62345", 2), Some(6234500));
        assert_eq!(parse_scaled(b"0.00100000", 8), Some(100000));
        assert_eq!(parse_scaled(b"-1.5", 1), Some(-15));
        assert_eq!(parse_scaled(b"1.2300", 2), Some(123)); // trailing zeros ok
        assert_eq!(parse_scaled(b".5", 1), Some(5));
    }

    #[test]
    fn rejects_bad_input() {
        assert_eq!(parse_scaled(b"1.234", 2), None); // precision loss
        assert_eq!(parse_scaled(b"", 2), None);
        assert_eq!(parse_scaled(b"-", 2), None);
        assert_eq!(parse_scaled(b".", 2), None);
        assert_eq!(parse_scaled(b"1.2.3", 2), None);
        assert_eq!(parse_scaled(b"1e5", 2), None);
        assert_eq!(parse_scaled(b"99999999999999999999", 2), None);
    }

    #[test]
    fn notional_is_exact() {
        // 62345.67 USD * 0.5 BTC = 31172.835 USD = 311728350000000 * 1e-10
        let n = Px(6234567).notional(Qty(50_000_000));
        assert_eq!(n, Cash(311_728_350_000_000));
        assert_eq!(format_scaled(n.0, 10), "31172.8350000000");
    }

    proptest! {
        #[test]
        fn roundtrip(v in -1_000_000_000_000i64..1_000_000_000_000, d in 0u32..9) {
            let s = format_scaled(v as i128, d);
            prop_assert_eq!(parse_scaled(s.as_bytes(), d), Some(v));
        }
    }
}
