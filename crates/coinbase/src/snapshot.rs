//! REST level-3 snapshot: `GET /products/{id}/book?level=3`.

use hft_core::{Instrument, L3Snapshot, OrderId, SnapshotOrder};
use serde::Deserialize;

#[derive(Deserialize)]
struct Raw<'a> {
    sequence: u64,
    #[serde(borrow)]
    bids: Vec<(&'a str, &'a str, &'a str)>,
    #[serde(borrow)]
    asks: Vec<(&'a str, &'a str, &'a str)>,
    #[serde(default)]
    auction_mode: Option<bool>,
}

pub fn parse_snapshot(inst: &Instrument, body: &[u8]) -> anyhow::Result<L3Snapshot> {
    let r: Raw<'_> = serde_json::from_slice(body)?;
    if r.auction_mode == Some(true) {
        anyhow::bail!("snapshot taken in auction mode; book may be crossed");
    }
    let conv = |v: &[(&str, &str, &str)]| -> anyhow::Result<Vec<SnapshotOrder>> {
        v.iter()
            .map(|(p, q, id)| {
                Ok(SnapshotOrder {
                    px: inst.parse_px(p.as_bytes()).ok_or_else(|| anyhow::anyhow!("bad price {p}"))?,
                    qty: inst.parse_qty(q.as_bytes()).ok_or_else(|| anyhow::anyhow!("bad size {q}"))?,
                    order_id: OrderId::parse_uuid(id.as_bytes()).ok_or_else(|| anyhow::anyhow!("bad id {id}"))?,
                })
            })
            .collect()
    };
    Ok(L3Snapshot { seq: r.sequence, bids: conv(&r.bids)?, asks: conv(&r.asks)? })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses() {
        let body = br#"{"bids":[["100.01","0.5","d50ec984-77a8-460a-b958-66f114b0de9b"]],"asks":[],"sequence":42,"auction_mode":false,"auction":null,"time":"2026-10-04T00:00:00Z"}"#;
        let s = parse_snapshot(&Instrument::btc_usd(), body).unwrap();
        assert_eq!(s.seq, 42);
        assert_eq!(s.bids[0].px.0, 10001);
        assert_eq!(s.bids[0].qty.0, 50_000_000);
    }
}
