//! Identifiers.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Exchange order id. Coinbase uses UUIDs; we store the 128 bits directly so the
/// hot path never allocates or hashes strings.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OrderId(pub u128);

impl OrderId {
    /// Parse a UUID string (`8-4-4-4-12` hex, case-insensitive).
    pub fn parse_uuid(s: &[u8]) -> Option<OrderId> {
        if s.len() != 36 {
            return None;
        }
        let mut v: u128 = 0;
        for (i, &c) in s.iter().enumerate() {
            if matches!(i, 8 | 13 | 18 | 23) {
                if c != b'-' {
                    return None;
                }
                continue;
            }
            let d = match c {
                b'0'..=b'9' => c - b'0',
                b'a'..=b'f' => c - b'a' + 10,
                b'A'..=b'F' => c - b'A' + 10,
                _ => return None,
            };
            v = (v << 4) | d as u128;
        }
        Some(OrderId(v))
    }
}

impl fmt::Display for OrderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let h = format!("{:032x}", self.0);
        write!(f, "{}-{}-{}-{}-{}", &h[0..8], &h[8..12], &h[12..16], &h[16..20], &h[20..32])
    }
}

/// Client order id for our own (simulated) orders.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ClOrdId(pub u64);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_roundtrip() {
        let s = "d50ec984-77a8-460a-b958-66f114b0de9b";
        let id = OrderId::parse_uuid(s.as_bytes()).unwrap();
        assert_eq!(id.to_string(), s);
        assert!(OrderId::parse_uuid(b"d50ec984x77a8-460a-b958-66f114b0de9b").is_none());
        assert!(OrderId::parse_uuid(b"short").is_none());
    }
}
