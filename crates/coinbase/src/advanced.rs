//! Coinbase **Advanced Trade** public market data (`wss://advanced-trade-ws.coinbase.com`).
//!
//! Channels used: `level2` (aggregated book: snapshot then absolute level sizes),
//! `market_trades` (trades, batched every 250 ms; `side` is the maker's side),
//! `heartbeats` (once per second).
//!
//! Every frame on a connection carries `sequence_num`, a per-connection counter
//! across all channels. A jump of more than one means frames were dropped; the
//! book can only be recovered by resubscribing (which delivers a new snapshot).
//! [`SeqTracker`] performs that check in stream order — identically live and in replay.
//!
//! Docs: <https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/websocket/level2>,
//! <https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/websocket/market-trades>.

use hft_core::{Instrument, L2Snapshot, MdEvent, MdMsg, Px, Qty, Side, Ts};
use serde::Deserialize;
use std::sync::Arc;

pub const WS_URL: &str = "wss://advanced-trade-ws.coinbase.com";
pub const VENUE: &str = "coinbase-advanced";

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum AtError {
    #[error("invalid json: {0}")]
    Json(String),
    #[error("missing or invalid field `{0}`")]
    Field(&'static str),
}

/// What a frame was, beyond the market messages it produced.
#[derive(Debug, PartialEq)]
pub enum FrameInfo {
    /// A sequenced channel frame.
    Data {
        seq: u64,
    },
    Error(String),
    Unknown,
}

#[derive(Deserialize)]
struct Frame<'a> {
    #[serde(borrow, default)]
    channel: Option<&'a str>,
    #[serde(rename = "type", borrow, default)]
    ty: Option<&'a str>,
    #[serde(borrow, default)]
    message: Option<&'a str>,
    #[serde(borrow, default)]
    timestamp: Option<&'a str>,
    #[serde(default)]
    sequence_num: Option<u64>,
    #[serde(borrow, default)]
    events: Vec<Event<'a>>,
}

#[derive(Deserialize)]
struct Event<'a> {
    #[serde(rename = "type", borrow, default)]
    ty: Option<&'a str>,
    #[serde(borrow, default)]
    product_id: Option<&'a str>,
    #[serde(borrow, default)]
    updates: Vec<Update<'a>>,
    #[serde(borrow, default)]
    trades: Vec<AtTrade<'a>>,
}

#[derive(Deserialize)]
struct Update<'a> {
    side: &'a str,
    #[serde(borrow, default)]
    event_time: Option<&'a str>,
    price_level: &'a str,
    new_quantity: &'a str,
}

#[derive(Deserialize)]
struct AtTrade<'a> {
    trade_id: &'a str,
    #[serde(borrow, default)]
    product_id: Option<&'a str>,
    price: &'a str,
    size: &'a str,
    side: &'a str,
    time: &'a str,
}

pub struct AtParser {
    inst: Instrument,
}

impl AtParser {
    pub fn new(inst: Instrument) -> AtParser {
        AtParser { inst }
    }

    pub fn instrument(&self) -> &Instrument {
        &self.inst
    }

    /// Parse one frame, appending normalized messages to `out`. Messages carry
    /// `seq = 0`: continuity is the frame's `sequence_num`, returned in [`FrameInfo`].
    pub fn parse(&self, frame: &[u8], out: &mut Vec<MdMsg>) -> Result<FrameInfo, AtError> {
        let f: Frame<'_> = serde_json::from_slice(frame).map_err(|e| AtError::Json(e.to_string()))?;
        if f.ty == Some("error") {
            return Ok(FrameInfo::Error(f.message.unwrap_or("").to_string()));
        }
        let Some(seq) = f.sequence_num else { return Ok(FrameInfo::Unknown) };
        let frame_ts = f.timestamp.and_then(|t| Ts::parse_rfc3339(t.as_bytes()));
        let mine = |p: Option<&str>| p.is_none_or(|p| p == self.inst.symbol);
        match f.channel {
            Some("l2_data") => {
                for ev in f.events.iter().filter(|e| mine(e.product_id)) {
                    match ev.ty {
                        Some("snapshot") => {
                            let mut s = L2Snapshot::default();
                            for u in &ev.updates {
                                let (side, px, qty) = self.level(u)?;
                                match side {
                                    Side::Bid => s.bids.push((px, qty)),
                                    Side::Ask => s.asks.push((px, qty)),
                                }
                            }
                            s.bids.sort_by_key(|l| std::cmp::Reverse(l.0));
                            s.asks.sort_by_key(|l| l.0);
                            // Snapshot event_times are not meaningful; use the frame time.
                            let ts = frame_ts.ok_or(AtError::Field("timestamp"))?;
                            out.push(MdMsg { seq: 0, exch_ts: ts, event: MdEvent::L2Snapshot(Arc::new(s)) });
                        }
                        Some("update") => {
                            for u in &ev.updates {
                                let (side, px, qty) = self.level(u)?;
                                let ts = u
                                    .event_time
                                    .and_then(|t| Ts::parse_rfc3339(t.as_bytes()))
                                    .or(frame_ts)
                                    .ok_or(AtError::Field("event_time"))?;
                                out.push(MdMsg { seq: 0, exch_ts: ts, event: MdEvent::Level { side, px, qty } });
                            }
                        }
                        _ => return Err(AtError::Field("l2_data type")),
                    }
                }
            }
            Some("market_trades") => {
                // The snapshot event replays recent history; only updates are new trades.
                for ev in f.events.iter().filter(|e| e.ty == Some("update")) {
                    for t in ev.trades.iter().filter(|t| mine(t.product_id)) {
                        let maker_side = match t.side {
                            "BUY" => Side::Bid,
                            "SELL" => Side::Ask,
                            _ => return Err(AtError::Field("side")),
                        };
                        out.push(MdMsg {
                            seq: 0,
                            exch_ts: Ts::parse_rfc3339(t.time.as_bytes()).ok_or(AtError::Field("time"))?,
                            event: MdEvent::TradePrint {
                                trade_id: t.trade_id.parse().map_err(|_| AtError::Field("trade_id"))?,
                                maker_side,
                                px: self.inst.parse_px(t.price.as_bytes()).ok_or(AtError::Field("price"))?,
                                qty: self.inst.parse_qty(t.size.as_bytes()).ok_or(AtError::Field("size"))?,
                            },
                        });
                    }
                }
            }
            Some("heartbeats") => {
                let ts = frame_ts.ok_or(AtError::Field("timestamp"))?;
                out.push(MdMsg { seq: 0, exch_ts: ts, event: MdEvent::Heartbeat { last_trade_id: 0 } });
            }
            _ => {} // subscriptions acks etc.: sequenced, no market content
        }
        Ok(FrameInfo::Data { seq })
    }

    fn level(&self, u: &Update<'_>) -> Result<(Side, Px, Qty), AtError> {
        let side = match u.side {
            "bid" => Side::Bid,
            "offer" | "ask" => Side::Ask,
            _ => return Err(AtError::Field("side")),
        };
        Ok((
            side,
            self.inst.parse_px(u.price_level.as_bytes()).ok_or(AtError::Field("price_level"))?,
            self.inst.parse_qty(u.new_quantity.as_bytes()).ok_or(AtError::Field("new_quantity"))?,
        ))
    }
}

/// Per-connection `sequence_num` continuity.
#[derive(Default)]
pub struct SeqTracker {
    last: Option<u64>,
}

impl SeqTracker {
    /// New connection: sequence restarts.
    pub fn reset(&mut self) {
        self.last = None;
    }

    /// Returns `Some((expected, got))` on a gap or out-of-order frame.
    pub fn check(&mut self, seq: u64) -> Option<(u64, u64)> {
        let r = match self.last {
            Some(l) if seq != l + 1 => Some((l + 1, seq)),
            _ => None,
        };
        self.last = Some(seq);
        r
    }
}

/// Subscribe messages (one channel per message, as the API requires).
pub fn subscribe_msgs(product: &str) -> Vec<String> {
    ["level2", "market_trades", "heartbeats"]
        .iter()
        .map(|ch| serde_json::json!({"type": "subscribe", "product_ids": [product], "channel": ch}).to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p() -> AtParser {
        AtParser::new(Instrument::btc_usd())
    }

    #[test]
    fn parses_level2_snapshot_and_update() {
        let snap = br#"{"channel":"l2_data","client_id":"","timestamp":"2023-02-09T20:32:50.714964855Z","sequence_num":0,"events":[{"type":"snapshot","product_id":"BTC-USD","updates":[{"side":"bid","event_time":"1970-01-01T00:00:00Z","price_level":"21921.73","new_quantity":"0.06317902"},{"side":"offer","event_time":"1970-01-01T00:00:00Z","price_level":"21921.74","new_quantity":"0.5"},{"side":"bid","event_time":"1970-01-01T00:00:00Z","price_level":"21921.70","new_quantity":"1"}]}]}"#;
        let mut out = vec![];
        assert_eq!(p().parse(snap, &mut out).unwrap(), FrameInfo::Data { seq: 0 });
        let MdEvent::L2Snapshot(s) = &out[0].event else { panic!() };
        assert_eq!(s.bids, vec![(Px(2192173), Qty(6_317_902)), (Px(2192170), Qty(100_000_000))]);
        assert_eq!(s.asks, vec![(Px(2192174), Qty(50_000_000))]);
        assert_eq!(out[0].exch_ts, Ts::parse_rfc3339(b"2023-02-09T20:32:50.714964855Z").unwrap());

        out.clear();
        let upd = br#"{"channel":"l2_data","client_id":"","timestamp":"2023-02-09T20:32:50.8Z","sequence_num":1,"events":[{"type":"update","product_id":"BTC-USD","updates":[{"side":"bid","event_time":"2023-02-09T20:32:50.70Z","price_level":"21921.73","new_quantity":"0"}]}]}"#;
        assert_eq!(p().parse(upd, &mut out).unwrap(), FrameInfo::Data { seq: 1 });
        assert_eq!(out[0].event, MdEvent::Level { side: Side::Bid, px: Px(2192173), qty: Qty(0) });
        assert_eq!(out[0].exch_ts, Ts::parse_rfc3339(b"2023-02-09T20:32:50.70Z").unwrap());
    }

    #[test]
    fn trades_skip_history_and_use_maker_side() {
        let mut out = vec![];
        let hist = br#"{"channel":"market_trades","client_id":"","timestamp":"2023-02-09T20:19:35.39Z","sequence_num":2,"events":[{"type":"snapshot","trades":[{"trade_id":"1","product_id":"BTC-USD","price":"100.00","size":"1","side":"BUY","time":"2019-08-14T20:42:27.265Z"}]}]}"#;
        p().parse(hist, &mut out).unwrap();
        assert!(out.is_empty());
        let upd = br#"{"channel":"market_trades","client_id":"","timestamp":"2023-02-09T20:19:35.39Z","sequence_num":3,"events":[{"type":"update","trades":[{"trade_id":"12345","product_id":"BTC-USD","price":"100.01","size":"0.25","side":"SELL","time":"2023-02-09T20:19:35.2Z"}]}]}"#;
        p().parse(upd, &mut out).unwrap();
        assert_eq!(
            out[0].event,
            MdEvent::TradePrint { trade_id: 12345, maker_side: Side::Ask, px: Px(10001), qty: Qty(25_000_000) }
        );
        assert!(!out[0].event.is_timely());
    }

    #[test]
    fn heartbeat_error_and_seq() {
        let mut out = vec![];
        let hb = br#"{"channel":"heartbeats","client_id":"","timestamp":"2023-06-23T20:31:56.121961769Z","sequence_num":4,"events":[{"current_time":"2023-06-23 20:31:56.121961769 +0000 UTC m=+91717.525857105","heartbeat_counter":3049}]}"#;
        assert_eq!(p().parse(hb, &mut out).unwrap(), FrameInfo::Data { seq: 4 });
        assert!(matches!(out[0].event, MdEvent::Heartbeat { .. }));
        let err = br#"{"type":"error","message":"failure to subscribe"}"#;
        assert_eq!(p().parse(err, &mut out).unwrap(), FrameInfo::Error("failure to subscribe".into()));

        let mut t = SeqTracker::default();
        assert_eq!(t.check(0), None);
        assert_eq!(t.check(1), None);
        assert_eq!(t.check(3), Some((2, 3)));
        assert_eq!(t.check(4), None);
        t.reset();
        assert_eq!(t.check(0), None);
    }
}
