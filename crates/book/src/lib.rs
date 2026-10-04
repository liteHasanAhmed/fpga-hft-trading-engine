//! Level-3 order book.
//!
//! Tracks every resting order by id plus an aggregated per-price view. All state
//! changes come from exchange messages; the book never infers anything.
//!
//! Message semantics (Coinbase `full` channel):
//! * `open` adds an order. `received` has no book effect.
//! * `match` reduces the *maker* order's size. The trade is the only book effect;
//!   the following `done` for a fully filled maker removes a zero-size entry, so
//!   trade volume is never subtracted twice.
//! * `done` removes the order if it is on the book (otherwise it never rested).
//! * `change` resizes (and possibly reprices) a resting order.
//!
//! The same type also holds an **L2** (aggregated) book for venues that only
//! publish price-level totals: [`Book::load_l2`] and [`Book::set_level`]. An L2
//! book has no individual orders; `Level::count` is 0. L2 cannot tell us how many
//! orders make up a level, their time priority, or whether a size decrease was a
//! cancel or a fill — which is why queue position must be estimated on L2 venues.

use hft_core::{L2Snapshot, L3Snapshot, OrderId, Px, Qty, Side, SnapshotOrder};
use rustc_hash::FxHashMap;
use std::collections::BTreeMap;

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum BookError {
    #[error("order {0} opened twice")]
    DuplicateOrder(OrderId),
    #[error("match maker {0} not on book")]
    UnknownMaker(OrderId),
    #[error("match of {matched:?} exceeds maker {id} size {resting:?}")]
    MatchExceedsSize { id: OrderId, matched: Qty, resting: Qty },
    #[error("order {id} side/price mismatch")]
    Mismatch { id: OrderId },
    #[error("done for {id} reports remaining {reported:?}, book has {resting:?}")]
    RemainingMismatch { id: OrderId, reported: Qty, resting: Qty },
    #[error("non-positive size for {0}")]
    BadSize(OrderId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestingOrder {
    pub side: Side,
    pub px: Px,
    pub qty: Qty,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Level {
    pub qty: Qty,
    /// Orders at this price, including zero-size makers awaiting their `done`.
    pub count: u32,
}

/// Price levels best-first: `(price, total size)`.
pub type Levels = Vec<(Px, Qty)>;

#[derive(Default, Clone)]
pub struct Book {
    orders: FxHashMap<OrderId, RestingOrder>,
    bids: BTreeMap<Px, Level>,
    asks: BTreeMap<Px, Level>,
    l2: bool,
}

impl Book {
    pub fn new() -> Book {
        Book::default()
    }

    pub fn clear(&mut self) {
        self.orders.clear();
        self.bids.clear();
        self.asks.clear();
        self.l2 = false;
    }

    /// True if this book holds aggregated levels only.
    pub fn is_l2(&self) -> bool {
        self.l2
    }

    pub fn load_l2(&mut self, s: &L2Snapshot) {
        self.clear();
        self.l2 = true;
        for &(p, q) in &s.bids {
            self.set_level(Side::Bid, p, q);
        }
        for &(p, q) in &s.asks {
            self.set_level(Side::Ask, p, q);
        }
    }

    /// L2 update: absolute size at `px`; zero removes the level.
    pub fn set_level(&mut self, side: Side, px: Px, qty: Qty) {
        debug_assert!(self.l2 || self.orders.is_empty(), "set_level on an L3 book");
        self.l2 = true;
        let levels = self.levels(side);
        if qty.0 <= 0 {
            levels.remove(&px);
        } else {
            levels.insert(px, Level { qty, count: 0 });
        }
    }

    pub fn to_l2_snapshot(&self) -> L2Snapshot {
        let (bids, asks) = self.depth(usize::MAX);
        L2Snapshot { bids, asks }
    }

    pub fn load_snapshot(&mut self, s: &L3Snapshot) -> Result<(), BookError> {
        self.clear();
        self.orders.reserve(s.bids.len() + s.asks.len());
        for (side, list) in [(Side::Bid, &s.bids), (Side::Ask, &s.asks)] {
            for o in list {
                self.open(o.order_id, side, o.px, o.qty)?;
            }
        }
        Ok(())
    }

    #[inline]
    fn levels(&mut self, side: Side) -> &mut BTreeMap<Px, Level> {
        match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        }
    }

    pub fn open(&mut self, id: OrderId, side: Side, px: Px, qty: Qty) -> Result<(), BookError> {
        if qty.0 <= 0 {
            return Err(BookError::BadSize(id));
        }
        if self.orders.contains_key(&id) {
            return Err(BookError::DuplicateOrder(id));
        }
        self.orders.insert(id, RestingOrder { side, px, qty });
        let l = self.levels(side).entry(px).or_default();
        l.qty += qty;
        l.count += 1;
        Ok(())
    }

    /// Apply a trade against resting maker `id`.
    pub fn match_maker(&mut self, id: OrderId, side: Side, px: Px, qty: Qty) -> Result<(), BookError> {
        let o = self.orders.get_mut(&id).ok_or(BookError::UnknownMaker(id))?;
        if o.side != side || o.px != px {
            return Err(BookError::Mismatch { id });
        }
        if qty > o.qty || qty.0 <= 0 {
            return Err(BookError::MatchExceedsSize { id, matched: qty, resting: o.qty });
        }
        o.qty -= qty;
        let l = self.levels(side).get_mut(&px).expect("level exists for resting order");
        l.qty -= qty;
        Ok(())
    }

    /// Remove an order. Returns `Ok(false)` if it was never on the book.
    pub fn done(&mut self, id: OrderId, reported_remaining: Option<Qty>) -> Result<bool, BookError> {
        let Some(o) = self.orders.remove(&id) else {
            return Ok(false);
        };
        if let Some(r) = reported_remaining
            && r != o.qty
        {
            // Put it back so the error leaves state unchanged.
            self.orders.insert(id, o);
            return Err(BookError::RemainingMismatch { id, reported: r, resting: o.qty });
        }
        self.remove_from_level(o);
        Ok(true)
    }

    /// Resize/reprice a resting order. Returns `Ok(false)` if not on the book.
    pub fn change(&mut self, id: OrderId, new_qty: Qty, new_px: Option<Px>) -> Result<bool, BookError> {
        let Some(o) = self.orders.remove(&id) else {
            return Ok(false);
        };
        self.remove_from_level(o);
        if new_qty.0 > 0 {
            self.open(id, o.side, new_px.unwrap_or(o.px), new_qty)?;
        }
        Ok(true)
    }

    fn remove_from_level(&mut self, o: RestingOrder) {
        let levels = self.levels(o.side);
        let l = levels.get_mut(&o.px).expect("level exists for resting order");
        l.qty -= o.qty;
        l.count -= 1;
        if l.count == 0 {
            levels.remove(&o.px);
        }
    }

    /// Best bid, ignoring levels whose remaining size is zero.
    pub fn best_bid(&self) -> Option<(Px, Qty)> {
        self.bids.iter().rev().find(|(_, l)| l.qty.0 > 0).map(|(p, l)| (*p, l.qty))
    }
    pub fn best_ask(&self) -> Option<(Px, Qty)> {
        self.asks.iter().find(|(_, l)| l.qty.0 > 0).map(|(p, l)| (*p, l.qty))
    }
    pub fn best(&self, side: Side) -> Option<(Px, Qty)> {
        match side {
            Side::Bid => self.best_bid(),
            Side::Ask => self.best_ask(),
        }
    }

    /// Mid price in half-ticks (`bid + ask`), avoiding fractions.
    pub fn mid_x2(&self) -> Option<i64> {
        Some(self.best_bid()?.0.0 + self.best_ask()?.0.0)
    }

    pub fn level(&self, side: Side, px: Px) -> Level {
        let m = match side {
            Side::Bid => &self.bids,
            Side::Ask => &self.asks,
        };
        m.get(&px).copied().unwrap_or_default()
    }

    pub fn order(&self, id: OrderId) -> Option<&RestingOrder> {
        self.orders.get(&id)
    }

    /// Export as an L3 snapshot at `seq` (orders sorted by price then id; true
    /// time priority within a level is not tracked in v1).
    pub fn to_snapshot(&self, seq: u64) -> L3Snapshot {
        let mut s = L3Snapshot { seq, bids: vec![], asks: vec![] };
        // Zero-size makers (matched, awaiting `done`) are not resting liquidity.
        for (id, o) in self.orders.iter().filter(|(_, o)| o.qty.0 > 0) {
            let so = SnapshotOrder { order_id: *id, px: o.px, qty: o.qty };
            match o.side {
                Side::Bid => s.bids.push(so),
                Side::Ask => s.asks.push(so),
            }
        }
        s.bids.sort_by(|a, b| b.px.cmp(&a.px).then(a.order_id.cmp(&b.order_id)));
        s.asks.sort_by(|a, b| a.px.cmp(&b.px).then(a.order_id.cmp(&b.order_id)));
        s
    }

    pub fn order_count(&self) -> usize {
        self.orders.len()
    }

    /// Top `n` non-empty levels per side, best first.
    pub fn depth(&self, n: usize) -> (Levels, Levels) {
        let f = |(p, l): (&Px, &Level)| (l.qty.0 > 0).then_some((*p, l.qty));
        (self.bids.iter().rev().filter_map(f).take(n).collect(), self.asks.iter().filter_map(f).take(n).collect())
    }

    /// Deterministic 64-bit hash of the full L3 state (levels + every order),
    /// independent of hash-map iteration order. Used to prove live == replay.
    pub fn state_hash(&self) -> u64 {
        // FNV-1a over levels, then an order-independent sum over orders.
        let mut h: u64 = 0xcbf29ce484222325;
        let mut mix = |v: u64| {
            for b in v.to_le_bytes() {
                h ^= b as u64;
                h = h.wrapping_mul(0x100000001b3);
            }
        };
        for (tag, side) in [(1u64, &self.bids), (2u64, &self.asks)] {
            for (p, l) in side {
                mix(tag);
                mix(p.0 as u64);
                mix(l.qty.0 as u64);
                mix(l.count as u64);
            }
        }
        let mut acc: u64 = 0;
        for (id, o) in &self.orders {
            let mut x = (id.0 as u64) ^ ((id.0 >> 64) as u64).rotate_left(17);
            x ^= (o.qty.0 as u64).wrapping_mul(0x9E3779B97F4A7C15);
            x ^= (o.px.0 as u64).rotate_left(29) ^ (o.side as u64);
            x = x.wrapping_mul(0xff51afd7ed558ccd);
            acc = acc.wrapping_add(x ^ (x >> 33));
        }
        mix(acc);
        mix(self.orders.len() as u64);
        h
    }

    /// Full consistency check. O(orders); used in tests and verify mode.
    pub fn check_invariants(&self) -> Result<(), String> {
        let mut agg: BTreeMap<(Side, Px), Level> = BTreeMap::new();
        for (id, o) in &self.orders {
            if o.qty.0 < 0 {
                return Err(format!("order {id} negative size"));
            }
            let l = agg.entry((o.side, o.px)).or_default();
            l.qty += o.qty;
            l.count += 1;
        }
        for (side, m) in [(Side::Bid, &self.bids), (Side::Ask, &self.asks)] {
            for (p, l) in m {
                if self.l2 {
                    if l.qty.0 <= 0 || l.count != 0 {
                        return Err(format!("L2 {side:?} level {p} invalid: {l:?}"));
                    }
                    continue;
                }
                if agg.remove(&(side, *p)) != Some(*l) {
                    return Err(format!("{side:?} level {p} disagrees with orders"));
                }
            }
        }
        if !agg.is_empty() {
            return Err("orders reference missing levels".into());
        }
        if let (Some((b, _)), Some((a, _))) = (self.best_bid(), self.best_ask())
            && b >= a
        {
            return Err(format!("crossed book: bid {b} >= ask {a}"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u128) -> OrderId {
        OrderId(n)
    }

    /// Hand-worked example (prices in ticks, sizes in lots):
    /// open B1 bid 100 x10, B2 bid 100 x20, A1 ask 101 x30
    /// match 4 against B1          → level 100 = 26 (2 orders)
    /// done B1 canceled, rem 6     → level 100 = 20 (1 order)
    /// match 30 against A1, done   → ask side empty
    #[test]
    fn hand_example() {
        let mut b = Book::new();
        b.open(id(1), Side::Bid, Px(100), Qty(10)).unwrap();
        b.open(id(2), Side::Bid, Px(100), Qty(20)).unwrap();
        b.open(id(3), Side::Ask, Px(101), Qty(30)).unwrap();
        assert_eq!(b.best_bid(), Some((Px(100), Qty(30))));
        assert_eq!(b.best_ask(), Some((Px(101), Qty(30))));

        b.match_maker(id(1), Side::Bid, Px(100), Qty(4)).unwrap();
        assert_eq!(b.level(Side::Bid, Px(100)), Level { qty: Qty(26), count: 2 });

        assert_eq!(b.done(id(1), Some(Qty(6))), Ok(true));
        assert_eq!(b.level(Side::Bid, Px(100)), Level { qty: Qty(20), count: 1 });

        // Taker fully fills A1: match reduces to 0, done(filled, remaining 0) removes.
        b.match_maker(id(3), Side::Ask, Px(101), Qty(30)).unwrap();
        assert_eq!(b.best_ask(), None, "zero-size level is not a best price");
        assert_eq!(b.done(id(3), Some(Qty(0))), Ok(true));
        assert_eq!(b.level(Side::Ask, Px(101)), Level::default());

        // done for an order that never rested is a no-op.
        assert_eq!(b.done(id(99), None), Ok(false));
        b.check_invariants().unwrap();
    }

    #[test]
    fn errors_leave_state_consistent() {
        let mut b = Book::new();
        b.open(id(1), Side::Bid, Px(100), Qty(10)).unwrap();
        assert_eq!(b.open(id(1), Side::Bid, Px(100), Qty(10)), Err(BookError::DuplicateOrder(id(1))));
        assert!(b.match_maker(id(1), Side::Bid, Px(100), Qty(11)).is_err());
        assert!(b.match_maker(id(2), Side::Bid, Px(100), Qty(1)).is_err());
        assert!(b.done(id(1), Some(Qty(9))).is_err());
        assert_eq!(b.order(id(1)).unwrap().qty, Qty(10));
        b.check_invariants().unwrap();
    }

    #[test]
    fn change_reprices() {
        let mut b = Book::new();
        b.open(id(1), Side::Ask, Px(105), Qty(10)).unwrap();
        b.change(id(1), Qty(5), Some(Px(104))).unwrap();
        assert_eq!(b.best_ask(), Some((Px(104), Qty(5))));
        assert_eq!(b.level(Side::Ask, Px(105)), Level::default());
        b.check_invariants().unwrap();
    }

    #[test]
    fn l2_levels() {
        let mut b = Book::new();
        b.load_l2(&L2Snapshot { bids: vec![(Px(100), Qty(5)), (Px(99), Qty(7))], asks: vec![(Px(101), Qty(3))] });
        assert!(b.is_l2());
        assert_eq!(b.best_bid(), Some((Px(100), Qty(5))));
        b.set_level(Side::Bid, Px(100), Qty(0));
        assert_eq!(b.best_bid(), Some((Px(99), Qty(7))));
        b.set_level(Side::Ask, Px(101), Qty(9));
        assert_eq!(b.best_ask(), Some((Px(101), Qty(9))));
        b.check_invariants().unwrap();
        assert_eq!(b.to_l2_snapshot().bids, vec![(Px(99), Qty(7))]);
    }

    #[test]
    fn hash_is_order_independent_and_sensitive() {
        let mut a = Book::new();
        let mut b = Book::new();
        a.open(id(1), Side::Bid, Px(100), Qty(10)).unwrap();
        a.open(id(2), Side::Ask, Px(101), Qty(10)).unwrap();
        b.open(id(2), Side::Ask, Px(101), Qty(10)).unwrap();
        b.open(id(1), Side::Bid, Px(100), Qty(10)).unwrap();
        assert_eq!(a.state_hash(), b.state_hash());
        b.match_maker(id(1), Side::Bid, Px(100), Qty(1)).unwrap();
        assert_ne!(a.state_hash(), b.state_hash());
    }

    mod prop {
        use super::*;
        use proptest::prelude::*;

        #[derive(Debug, Clone)]
        enum Op {
            Open(u8, bool, i64, i64),
            Match(u8, i64),
            Done(u8),
            Change(u8, i64),
        }

        fn op() -> impl Strategy<Value = Op> {
            prop_oneof![
                (any::<u8>(), any::<bool>(), 0i64..20, 1i64..100).prop_map(|(i, s, p, q)| Op::Open(i, s, p, q)),
                (any::<u8>(), 1i64..50).prop_map(|(i, q)| Op::Match(i, q)),
                any::<u8>().prop_map(Op::Done),
                (any::<u8>(), 0i64..100).prop_map(|(i, q)| Op::Change(i, q)),
            ]
        }

        proptest! {
            /// Random op streams (errors allowed) never break aggregate consistency.
            #[test]
            fn invariants_hold(ops in proptest::collection::vec(op(), 1..300)) {
                let mut b = Book::new();
                for o in ops {
                    match o {
                        // Bids below 10, asks at/above 10 → never crossed.
                        Op::Open(i, bid, p, q) => {
                            let (side, px) = if bid { (Side::Bid, Px(p % 10)) } else { (Side::Ask, Px(10 + p % 10)) };
                            let _ = b.open(id(i as u128), side, px, Qty(q));
                        }
                        Op::Match(i, q) => {
                            if let Some(r) = b.order(id(i as u128)).copied() {
                                let _ = b.match_maker(id(i as u128), r.side, r.px, Qty(q));
                            }
                        }
                        Op::Done(i) => { let _ = b.done(id(i as u128), None); }
                        Op::Change(i, q) => { let _ = b.change(id(i as u128), Qty(q), None); }
                    }
                    prop_assert!(b.check_invariants().is_ok(), "{:?}", b.check_invariants());
                }
            }
        }
    }
}
