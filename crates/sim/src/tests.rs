use super::*;
use hft_core::OrderId;

fn book(bid: i64, ask: i64) -> Book {
    let mut b = Book::new();
    b.open(OrderId(1), Side::Bid, Px(bid), Qty(1_000)).unwrap();
    b.open(OrderId(2), Side::Ask, Px(ask), Qty(1_000)).unwrap();
    b
}
fn trade(maker_side: Side, px: i64, qty: i64) -> Trade {
    Trade { exch_ts: Ts(100), trade_id: 1, px: Px(px), qty: Qty(qty), maker_side }
}
fn place(id: u64, side: Side, px: i64, qty: i64) -> OrderReq {
    OrderReq::Place { cl_id: ClOrdId(id), side, px: Px(px), qty: Qty(qty) }
}
fn sim(model: FillModel, fees: Fees) -> ExchangeSim {
    ExchangeSim::new(SimConfig { fill_model: model, fees, ..Default::default() })
}

#[test]
fn post_only_rejects_cross_and_unavailable() {
    let mut s = sim(FillModel::TradeTouch, Fees::PROFESSIONAL);
    let b = book(100, 101);
    let mut out = vec![];
    s.on_arrival(Ts(1), place(1, Side::Bid, 101, 10), Some(&b), &mut out);
    s.on_arrival(Ts(1), place(2, Side::Ask, 100, 10), Some(&b), &mut out);
    s.on_arrival(Ts(1), place(3, Side::Bid, 100, 10), None, &mut out);
    s.on_arrival(Ts(1), place(4, Side::Bid, 100, 0), Some(&b), &mut out);
    let reasons: Vec<_> = out
        .iter()
        .map(|e| match e {
            OrderEvt::Reject { reason, .. } => *reason,
            _ => panic!("{e:?}"),
        })
        .collect();
    assert_eq!(
        reasons,
        vec![
            RejectReason::WouldCross,
            RejectReason::WouldCross,
            RejectReason::MarketUnavailable,
            RejectReason::BelowMinSize
        ]
    );
}

#[test]
fn fill_rules() {
    for (model, px, expect_fill) in [
        (FillModel::TradeTouch, 100, true),
        (FillModel::TradeTouch, 99, true),
        (FillModel::TradeThrough, 100, false),
        (FillModel::TradeThrough, 99, true),
    ] {
        let mut s = sim(model, Fees::PROFESSIONAL);
        let mut out = vec![];
        s.on_arrival(Ts(1), place(1, Side::Bid, 100, 10), Some(&book(100, 101)), &mut out);
        out.clear();
        s.on_trade(&trade(Side::Bid, px, 4), &mut out);
        assert_eq!(!out.is_empty(), expect_fill, "{model:?} trade@{px}");
        // A trade against asks (buy aggressor) never fills our bid.
        out.clear();
        s.on_trade(&trade(Side::Ask, 98, 4), &mut out);
        assert!(out.is_empty());
    }
}

#[test]
fn partial_fills_capped_by_trade_size() {
    let mut s = sim(FillModel::TradeTouch, Fees::PROFESSIONAL);
    let mut out = vec![];
    s.on_arrival(Ts(1), place(1, Side::Ask, 101, 10), Some(&book(100, 101)), &mut out);
    out.clear();
    s.on_trade(&trade(Side::Ask, 101, 4), &mut out);
    s.on_trade(&trade(Side::Ask, 101, 100), &mut out);
    let fills: Vec<_> = out
        .iter()
        .map(|e| match e {
            OrderEvt::Fill { qty, leaves, .. } => (qty.0, leaves.0),
            _ => panic!(),
        })
        .collect();
    assert_eq!(fills, vec![(4, 6), (6, 0)]);
    assert_eq!(s.live_orders(), 0);
    assert_eq!(s.portfolio.inventory, Qty(-10));
}

#[test]
fn cancel_fill_race() {
    // Cancel sent at t=1 with 10 ns latency; a trade at t=5 fills first.
    let mut s = ExchangeSim::new(SimConfig { cancel_latency_ns: 10, ..Default::default() });
    let mut out = vec![];
    s.on_arrival(Ts(0), place(1, Side::Bid, 100, 10), Some(&book(100, 101)), &mut out);
    s.cancel_sent(ClOrdId(1));
    s.on_trade(&trade(Side::Bid, 100, 10), &mut out);
    s.on_arrival(Ts(11), OrderReq::Cancel { cl_id: ClOrdId(1) }, Some(&book(100, 101)), &mut out);
    assert!(matches!(out[1], OrderEvt::Fill { leaves: Qty(0), .. }));
    assert!(matches!(out[2], OrderEvt::CancelReject { .. }));
    assert_eq!(s.stats.fills_while_cancel_pending, 1);

    // Partial fill, then the cancel removes the remainder.
    let mut s = sim(FillModel::TradeTouch, Fees::PROFESSIONAL);
    out.clear();
    s.on_arrival(Ts(0), place(1, Side::Bid, 100, 10), Some(&book(100, 101)), &mut out);
    s.on_trade(&trade(Side::Bid, 100, 3), &mut out);
    s.on_arrival(Ts(2), OrderReq::Cancel { cl_id: ClOrdId(1) }, Some(&book(100, 101)), &mut out);
    assert!(matches!(out[2], OrderEvt::CancelAck { .. }));
    assert_eq!(s.portfolio.inventory, Qty(3));
}

/// Hand-calculated accounting example at retail fees (maker 0.60%):
///
/// | fill | side | px (USD) | qty (BTC) | notional     | fee (0.6%)  |
/// |------|------|----------|-----------|--------------|-------------|
/// | 1    | buy  | 100.00   | 2         | 200.00       | 1.20        |
/// | 2    | sell | 101.00   | 1         | 101.00       | 0.606       |
/// | 3    | sell | 102.00   | 0.5       | 51.00        | 0.306       |
///
/// cash = -200 + 101 + 51 = -48.00; fees = 2.112; inventory = 0.5 BTC
/// equity at mid 101.50 = -48 - 2.112 + 0.5 * 101.5 = 0.638 USD
#[test]
fn accounting_hand_example() {
    let inst = hft_core::Instrument::btc_usd();
    let mut p = Portfolio::new(Fees::RETAIL);
    let btc = |x: f64| Qty((x * 1e8) as i64);
    p.fill(Side::Bid, Px(10000), btc(2.0), Liquidity::Maker);
    p.fill(Side::Ask, Px(10100), btc(1.0), Liquidity::Maker);
    p.fill(Side::Ask, Px(10200), btc(0.5), Liquidity::Maker);
    assert_eq!(inst.fmt_cash(p.cash), "-48.0000000000");
    assert_eq!(inst.fmt_cash(p.fees_paid), "2.1120000000");
    assert_eq!(p.inventory, btc(0.5));
    assert_eq!(inst.fmt_cash(p.equity_at_mid(10100 + 10200)), "0.6380000000");

    // Liquidation: sell 0.5 BTC into bids 101.00 x 0.3, 100.00 x 1 at taker 1.2%:
    // proceeds = 30.30 + 20.00 = 50.30; fee = 0.6036; equity = -50.112 + 50.30 - 0.6036 = -0.4156
    let mut b = Book::new();
    b.open(OrderId(1), Side::Bid, Px(10100), btc(0.3)).unwrap();
    b.open(OrderId(2), Side::Bid, Px(10000), btc(1.0)).unwrap();
    assert_eq!(inst.fmt_cash(liquidation_value(&p, &b).unwrap()), "-0.4156000000");
}

#[test]
fn seeded_probability_model_is_reproducible() {
    let run = |seed| {
        let mut s = ExchangeSim::new(SimConfig {
            fill_model: FillModel::TouchProbability { p: 0.5 },
            seed,
            ..Default::default()
        });
        let mut out = vec![];
        let mut fills = vec![];
        for i in 0..50 {
            out.clear();
            s.on_arrival(Ts(i), place(i as u64, Side::Bid, 100, 1), Some(&book(100, 101)), &mut out);
            s.on_trade(&trade(Side::Bid, 100, 1), &mut out);
            fills.push(out.iter().any(|e| matches!(e, OrderEvt::Fill { .. })));
        }
        fills
    };
    assert_eq!(run(7), run(7));
    assert_ne!(run(7), run(8));
}

#[test]
fn trade_before_arrival_never_fills() {
    // A batched trade print processed after our order arrived, but which happened
    // before it, must not fill us.
    let mut s = sim(FillModel::TradeTouch, Fees::PROFESSIONAL);
    let mut out = vec![];
    s.on_arrival(Ts(200), place(1, Side::Bid, 100, 10), Some(&book(100, 101)), &mut out);
    out.clear();
    s.on_trade(&trade(Side::Bid, 100, 5), &mut out); // trade at t=100 < arrival 200
    assert!(out.is_empty());
}
