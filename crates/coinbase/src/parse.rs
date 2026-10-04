//! WebSocket frame parser for the `full` and `heartbeat` channels.
//!
//! Deserializes into borrowed `&str` fields, so parsing a frame does not allocate.

use hft_core::{Instrument, MdEvent, MdMsg, OrderId, Px, Qty, Side, Ts};
use serde::Deserialize;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum ParseError {
    #[error("invalid json: {0}")]
    Json(String),
    #[error("missing or invalid field `{0}`")]
    Field(&'static str),
}

/// Result of parsing one frame.
#[derive(Debug, PartialEq)]
pub enum Parsed {
    Md(MdMsg),
    /// Subscription acknowledgement.
    Subscribed,
    /// Exchange-reported error (e.g. authentication required).
    Error(String),
    /// Message for another product, or a type we deliberately ignore (`activate`).
    Ignored,
}

#[derive(Deserialize)]
struct Raw<'a> {
    #[serde(rename = "type", borrow)]
    ty: &'a str,
    #[serde(borrow, default)]
    product_id: Option<&'a str>,
    #[serde(default)]
    sequence: Option<u64>,
    #[serde(borrow, default)]
    time: Option<&'a str>,
    #[serde(borrow, default)]
    order_id: Option<&'a str>,
    #[serde(borrow, default)]
    side: Option<&'a str>,
    #[serde(borrow, default)]
    price: Option<&'a str>,
    #[serde(borrow, default)]
    size: Option<&'a str>,
    #[serde(borrow, default)]
    remaining_size: Option<&'a str>,
    #[serde(borrow, default)]
    reason: Option<&'a str>,
    #[serde(default)]
    trade_id: Option<u64>,
    #[serde(borrow, default)]
    maker_order_id: Option<&'a str>,
    #[serde(borrow, default)]
    taker_order_id: Option<&'a str>,
    #[serde(borrow, default)]
    new_size: Option<&'a str>,
    #[serde(borrow, default)]
    new_price: Option<&'a str>,
    #[serde(default)]
    last_trade_id: Option<u64>,
    #[serde(borrow, default)]
    message: Option<&'a str>,
}

pub struct Parser {
    inst: Instrument,
}

impl Parser {
    pub fn new(inst: Instrument) -> Parser {
        Parser { inst }
    }

    pub fn instrument(&self) -> &Instrument {
        &self.inst
    }

    pub fn parse(&self, frame: &[u8]) -> Result<Parsed, ParseError> {
        let r: Raw<'_> = serde_json::from_slice(frame).map_err(|e| ParseError::Json(e.to_string()))?;
        match r.ty {
            "subscriptions" => return Ok(Parsed::Subscribed),
            "error" => {
                let mut m = r.message.unwrap_or("").to_string();
                if let Some(reason) = r.reason {
                    m.push_str(": ");
                    m.push_str(reason);
                }
                return Ok(Parsed::Error(m));
            }
            "activate" => return Ok(Parsed::Ignored),
            _ => {}
        }
        if r.product_id.is_some_and(|p| p != self.inst.symbol) {
            return Ok(Parsed::Ignored);
        }
        let seq = r.sequence.ok_or(ParseError::Field("sequence"))?;
        let exch_ts = r.time.and_then(|t| Ts::parse_rfc3339(t.as_bytes())).ok_or(ParseError::Field("time"))?;
        let event = match r.ty {
            "received" => MdEvent::Received {
                order_id: self.oid(r.order_id, "order_id")?,
                side: side(r.side)?,
                px: self.opt_px(r.price, "price")?,
                qty: self.opt_qty(r.size, "size")?,
            },
            "open" => MdEvent::Open {
                order_id: self.oid(r.order_id, "order_id")?,
                side: side(r.side)?,
                px: self.px(r.price, "price")?,
                remaining: self.qty(r.remaining_size, "remaining_size")?,
            },
            "done" => MdEvent::Done {
                order_id: self.oid(r.order_id, "order_id")?,
                side: side(r.side)?,
                px: self.opt_px(r.price, "price")?,
                remaining: self.opt_qty(r.remaining_size, "remaining_size")?,
                canceled: match r.reason {
                    Some("canceled") => true,
                    Some("filled") => false,
                    _ => return Err(ParseError::Field("reason")),
                },
            },
            "match" | "last_match" => MdEvent::Match {
                trade_id: r.trade_id.ok_or(ParseError::Field("trade_id"))?,
                maker_id: self.oid(r.maker_order_id, "maker_order_id")?,
                taker_id: self.oid(r.taker_order_id, "taker_order_id")?,
                maker_side: side(r.side)?,
                px: self.px(r.price, "price")?,
                qty: self.qty(r.size, "size")?,
            },
            "change" => MdEvent::Change {
                order_id: self.oid(r.order_id, "order_id")?,
                side: side(r.side)?,
                new_qty: self.qty(r.new_size, "new_size")?,
                new_px: self.opt_px(r.new_price, "new_price")?,
            },
            "heartbeat" => MdEvent::Heartbeat { last_trade_id: r.last_trade_id.unwrap_or(0) },
            _ => return Ok(Parsed::Ignored),
        };
        Ok(Parsed::Md(MdMsg { seq, exch_ts, event }))
    }

    fn oid(&self, s: Option<&str>, f: &'static str) -> Result<OrderId, ParseError> {
        s.and_then(|s| OrderId::parse_uuid(s.as_bytes())).ok_or(ParseError::Field(f))
    }
    fn px(&self, s: Option<&str>, f: &'static str) -> Result<Px, ParseError> {
        s.and_then(|s| self.inst.parse_px(s.as_bytes())).ok_or(ParseError::Field(f))
    }
    fn qty(&self, s: Option<&str>, f: &'static str) -> Result<Qty, ParseError> {
        s.and_then(|s| self.inst.parse_qty(s.as_bytes())).ok_or(ParseError::Field(f))
    }
    fn opt_px(&self, s: Option<&str>, f: &'static str) -> Result<Option<Px>, ParseError> {
        s.map(|s| self.inst.parse_px(s.as_bytes()).ok_or(ParseError::Field(f))).transpose()
    }
    fn opt_qty(&self, s: Option<&str>, f: &'static str) -> Result<Option<Qty>, ParseError> {
        s.map(|s| self.inst.parse_qty(s.as_bytes()).ok_or(ParseError::Field(f))).transpose()
    }
}

fn side(s: Option<&str>) -> Result<Side, ParseError> {
    match s {
        Some("buy") => Ok(Side::Bid),
        Some("sell") => Ok(Side::Ask),
        _ => Err(ParseError::Field("side")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p() -> Parser {
        Parser::new(Instrument::btc_usd())
    }
    const ID: &str = "d50ec984-77a8-460a-b958-66f114b0de9b";

    #[test]
    fn parses_doc_examples() {
        let open = format!(
            r#"{{"type":"open","time":"2014-11-07T08:19:27.028459Z","product_id":"BTC-USD","sequence":10,"order_id":"{ID}","price":"200.2","remaining_size":"1.00","side":"sell"}}"#
        );
        let Parsed::Md(m) = p().parse(open.as_bytes()).unwrap() else { panic!() };
        assert_eq!(m.seq, 10);
        assert_eq!(
            m.event,
            MdEvent::Open {
                order_id: OrderId::parse_uuid(ID.as_bytes()).unwrap(),
                side: Side::Ask,
                px: Px(20020),
                remaining: Qty(100_000_000)
            }
        );

        let mtch = format!(
            r#"{{"type":"match","trade_id":10,"sequence":50,"maker_order_id":"{ID}","taker_order_id":"{ID}","time":"2014-11-07T08:19:27.028459Z","product_id":"BTC-USD","size":"5.23512","price":"400.23","side":"sell"}}"#
        );
        let Parsed::Md(m) = p().parse(mtch.as_bytes()).unwrap() else { panic!() };
        assert!(matches!(m.event, MdEvent::Match { maker_side: Side::Ask, px: Px(40023), qty: Qty(523_512_000), .. }));

        let done_mkt = format!(
            r#"{{"type":"done","time":"2014-11-07T08:19:27.028459Z","product_id":"BTC-USD","sequence":11,"order_id":"{ID}","reason":"filled","side":"buy"}}"#
        );
        let Parsed::Md(m) = p().parse(done_mkt.as_bytes()).unwrap() else { panic!() };
        assert!(matches!(m.event, MdEvent::Done { px: None, remaining: None, canceled: false, .. }));

        let hb = r#"{"type":"heartbeat","sequence":90,"last_trade_id":20,"product_id":"BTC-USD","time":"2014-11-07T08:19:28.464459Z"}"#;
        let Parsed::Md(m) = p().parse(hb.as_bytes()).unwrap() else { panic!() };
        assert_eq!(m.event, MdEvent::Heartbeat { last_trade_id: 20 });
        assert!(!m.event.is_sequenced());
    }

    #[test]
    fn filters_and_errors() {
        let other = r#"{"type":"heartbeat","sequence":1,"last_trade_id":2,"product_id":"ETH-USD","time":"2014-11-07T08:19:28Z"}"#;
        assert_eq!(p().parse(other.as_bytes()).unwrap(), Parsed::Ignored);
        let err = r#"{"type":"error","message":"Failed to subscribe","reason":"authentication required"}"#;
        assert_eq!(
            p().parse(err.as_bytes()).unwrap(),
            Parsed::Error("Failed to subscribe: authentication required".into())
        );
        assert!(p().parse(b"{not json").is_err());
        let bad_px = format!(
            r#"{{"type":"open","time":"2014-11-07T08:19:27Z","product_id":"BTC-USD","sequence":10,"order_id":"{ID}","price":"200.123","remaining_size":"1","side":"sell"}}"#
        );
        assert_eq!(p().parse(bad_px.as_bytes()), Err(ParseError::Field("price")));
    }
}
