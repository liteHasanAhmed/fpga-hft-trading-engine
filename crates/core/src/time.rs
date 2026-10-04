//! Timestamps.
//!
//! All timestamps are `i64` nanoseconds since the Unix epoch. The *meaning* of a
//! timestamp is carried by the field that holds it:
//!
//! * `exch_ts` — matching-engine time from the exchange (`time` on Coinbase).
//! * `recv_ts` — local wall clock when the frame was read off the socket.
//! * `now` in replay/simulation — the virtual clock, on the exchange time axis.

use serde::{Deserialize, Serialize};
use std::ops::{Add, Sub};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Ts(pub i64);

impl Ts {
    pub const ZERO: Ts = Ts(0);
    pub const MAX: Ts = Ts(i64::MAX);

    pub const fn from_millis(ms: i64) -> Ts {
        Ts(ms * 1_000_000)
    }
    pub const fn from_micros(us: i64) -> Ts {
        Ts(us * 1_000)
    }
    pub fn as_secs_f64(self) -> f64 {
        self.0 as f64 / 1e9
    }

    /// Wall-clock now. Only used at capture time, never in replay.
    pub fn wall_now() -> Ts {
        let d = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
        Ts(d.as_nanos() as i64)
    }

    /// Parse RFC3339 UTC timestamps as sent by Coinbase,
    /// e.g. `2026-10-04T15:11:02.123456Z` (0–9 fractional digits, `Z` or `+00:00`).
    pub fn parse_rfc3339(s: &[u8]) -> Option<Ts> {
        if s.len() < 20 {
            return None;
        }
        let num = |a: usize, b: usize| -> Option<i64> {
            let mut v = 0i64;
            for &c in &s[a..b] {
                if !c.is_ascii_digit() {
                    return None;
                }
                v = v * 10 + (c - b'0') as i64;
            }
            Some(v)
        };
        if s[4] != b'-' || s[7] != b'-' || (s[10] != b'T' && s[10] != b' ') || s[13] != b':' || s[16] != b':' {
            return None;
        }
        let (y, mo, d) = (num(0, 4)?, num(5, 7)?, num(8, 10)?);
        let (h, mi, se) = (num(11, 13)?, num(14, 16)?, num(17, 19)?);
        if !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 23 || mi > 59 || se > 60 {
            return None;
        }
        let mut i = 19;
        let mut nanos = 0i64;
        if s[i] == b'.' {
            i += 1;
            let mut digits = 0;
            while i < s.len() && s[i].is_ascii_digit() {
                if digits < 9 {
                    nanos = nanos * 10 + (s[i] - b'0') as i64;
                    digits += 1;
                }
                i += 1;
            }
            for _ in digits..9 {
                nanos *= 10;
            }
        }
        match &s[i..] {
            b"Z" | b"+00:00" => {}
            _ => return None,
        }
        let days = days_from_civil(y, mo, d);
        Some(Ts(((days * 86_400 + h * 3600 + mi * 60 + se) * 1_000_000_000) + nanos))
    }
}

/// Days since 1970-01-01 for a proleptic Gregorian date (Howard Hinnant's algorithm).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

impl Add for Ts {
    type Output = Ts;
    #[inline]
    fn add(self, o: Ts) -> Ts {
        Ts(self.0.saturating_add(o.0))
    }
}
impl Sub for Ts {
    type Output = Ts;
    #[inline]
    fn sub(self, o: Ts) -> Ts {
        Ts(self.0.saturating_sub(o.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_coinbase_times() {
        // 2014-11-07T08:19:27.028459Z → 1415348367.028459
        assert_eq!(Ts::parse_rfc3339(b"2014-11-07T08:19:27.028459Z"), Some(Ts(1_415_348_367_028_459_000)));
        assert_eq!(Ts::parse_rfc3339(b"1970-01-01T00:00:00Z"), Some(Ts(0)));
        assert_eq!(Ts::parse_rfc3339(b"2000-03-01T00:00:00.5+00:00"), Some(Ts(951_868_800_500_000_000)));
        assert_eq!(Ts::parse_rfc3339(b"2014-11-07T08:19:27.028459"), None);
        assert_eq!(Ts::parse_rfc3339(b"2014-13-07T08:19:27Z"), None);
    }
}
