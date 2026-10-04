//! Independent verification of the live book against a REST snapshot.
//!
//! A snapshot reflects sequence `S`, but it arrives after the live feed has moved
//! on to `L > S`. We keep a ring of recently applied messages, build a *second*
//! book from the snapshot, roll it forward through `S+1..=L`, and compare full
//! L3 state hashes. If the snapshot is ahead of the feed (`S > L`), the check is
//! deferred until the feed reaches `S`.

use hft_book::Book;
use hft_core::{L3Snapshot, MdEvent, MdMsg, Px, Qty, Side};
use hft_feed::{Feed, FeedEvent};
use std::collections::VecDeque;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub enum SnapOutcome {
    Match {
        snap_seq: u64,
        at_seq: u64,
    },
    Mismatch {
        snap_seq: u64,
        at_seq: u64,
        first_diffs: Vec<String>,
    },
    /// Ring buffer no longer covers `snap_seq + 1` (snapshot too old) or feed not live.
    Unverifiable {
        snap_seq: u64,
        why: &'static str,
    },
}

pub struct SnapshotVerifier {
    ring: VecDeque<MdMsg>,
    cap: usize,
    pending: Vec<Arc<L3Snapshot>>,
}

impl SnapshotVerifier {
    pub fn new(cap: usize) -> SnapshotVerifier {
        SnapshotVerifier { ring: VecDeque::with_capacity(cap.min(1 << 16)), cap, pending: vec![] }
    }

    /// Call after every `feed.on_msg`. Tracks applied messages and resolves deferred checks.
    pub fn after_msg(&mut self, feed: &Feed, msg: &MdMsg, events: &[FeedEvent], out: &mut Vec<SnapOutcome>) {
        if events.iter().any(|e| matches!(e, FeedEvent::Invalid(_) | FeedEvent::Synced { .. })) {
            self.ring.clear();
        }
        if feed.is_live() && msg.event.is_sequenced() && msg.seq == feed.last_seq() {
            if self.ring.len() == self.cap {
                self.ring.pop_front();
            }
            self.ring.push_back(msg.clone());
        }
        if !self.pending.is_empty() && feed.is_live() {
            let ready: Vec<_> = self.pending.iter().filter(|s| s.seq <= feed.last_seq()).cloned().collect();
            self.pending.retain(|s| s.seq > feed.last_seq());
            for s in ready {
                out.push(self.check(feed, &s));
            }
        }
    }

    pub fn reset(&mut self) {
        self.ring.clear();
        self.pending.clear();
    }

    pub fn submit(&mut self, feed: &Feed, snap: Arc<L3Snapshot>, out: &mut Vec<SnapOutcome>) {
        if !feed.is_live() {
            out.push(SnapOutcome::Unverifiable { snap_seq: snap.seq, why: "feed not live" });
        } else if snap.seq > feed.last_seq() {
            self.pending.push(snap);
        } else {
            out.push(self.check(feed, &snap));
        }
    }

    fn check(&self, feed: &Feed, snap: &L3Snapshot) -> SnapOutcome {
        let live_seq = feed.last_seq();
        if snap.seq < live_seq && !self.ring.front().is_some_and(|m| m.seq <= snap.seq + 1) {
            return SnapOutcome::Unverifiable { snap_seq: snap.seq, why: "ring does not cover snapshot" };
        }
        let mut shadow = Feed::new(1);
        let mut sink = Vec::new();
        shadow.on_msg(
            &MdMsg { seq: snap.seq, exch_ts: hft_core::Ts::ZERO, event: MdEvent::Snapshot(Arc::new(snap.clone())) },
            &mut sink,
        );
        for m in self.ring.iter().filter(|m| m.seq > snap.seq) {
            shadow.on_msg(m, &mut sink);
        }
        if shadow.last_seq() == live_seq && shadow.book().state_hash() == feed.book().state_hash() {
            SnapOutcome::Match { snap_seq: snap.seq, at_seq: live_seq }
        } else {
            SnapOutcome::Mismatch {
                snap_seq: snap.seq,
                at_seq: live_seq,
                first_diffs: diff_books(feed.book(), shadow.book(), 5),
            }
        }
    }
}

/// First `n` price levels where two books disagree (best prices first).
pub fn diff_books(live: &Book, reference: &Book, n: usize) -> Vec<String> {
    let mut out = vec![];
    let (lb, la) = live.depth(usize::MAX);
    let (rb, ra) = reference.depth(usize::MAX);
    for (side, l, r) in [(Side::Bid, lb, rb), (Side::Ask, la, ra)] {
        let mut prices: Vec<Px> = l.iter().chain(r.iter()).map(|x| x.0).collect();
        prices.sort();
        prices.dedup();
        if side == Side::Bid {
            prices.reverse();
        }
        let get = |v: &[(Px, Qty)], p: Px| v.iter().find(|x| x.0 == p).map(|x| x.1).unwrap_or_default();
        for p in prices {
            let (a, b) = (get(&l, p), get(&r, p));
            if a != b && out.len() < n {
                out.push(format!("{side:?} {}: live {} vs snapshot {}", p.0, a.0, b.0));
            }
        }
    }
    if out.is_empty() && live.order_count() != reference.order_count() {
        out.push(format!("order count live {} vs snapshot {}", live.order_count(), reference.order_count()));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use hft_core::{OrderId, SnapshotOrder, Ts};

    fn open(seq: u64, id: u128, px: i64) -> MdMsg {
        MdMsg {
            seq,
            exch_ts: Ts(0),
            event: MdEvent::Open { order_id: OrderId(id), side: Side::Bid, px: Px(px), remaining: Qty(1) },
        }
    }
    fn snap(seq: u64, ids: &[(u128, i64)]) -> Arc<L3Snapshot> {
        Arc::new(L3Snapshot {
            seq,
            bids: ids.iter().map(|&(i, p)| SnapshotOrder { order_id: OrderId(i), px: Px(p), qty: Qty(1) }).collect(),
            asks: vec![],
        })
    }

    #[test]
    fn rolls_snapshot_forward() {
        let mut feed = Feed::new(100);
        let mut v = SnapshotVerifier::new(100);
        let (mut ev, mut out) = (vec![], vec![]);
        let s0 = MdMsg { seq: 10, exch_ts: Ts(0), event: MdEvent::Snapshot(snap(10, &[])) };
        feed.on_msg(&s0, &mut ev);
        v.after_msg(&feed, &s0, &ev, &mut out);
        for (seq, id) in [(11, 1), (12, 2), (13, 3)] {
            ev.clear();
            let m = open(seq, id, 90);
            feed.on_msg(&m, &mut ev);
            v.after_msg(&feed, &m, &ev, &mut out);
        }
        // Snapshot at 12 (contains orders 1, 2); live is at 13.
        v.submit(&feed, snap(12, &[(1, 90), (2, 90)]), &mut out);
        assert_eq!(out, vec![SnapOutcome::Match { snap_seq: 12, at_seq: 13 }]);

        out.clear();
        v.submit(&feed, snap(12, &[(1, 90), (2, 91)]), &mut out); // wrong price
        assert!(matches!(&out[0], SnapOutcome::Mismatch { first_diffs, .. } if !first_diffs.is_empty()));

        // Snapshot ahead of the feed is deferred until seq 14 is applied.
        out.clear();
        v.submit(&feed, snap(14, &[(1, 90), (2, 90), (3, 90), (4, 90)]), &mut out);
        assert!(out.is_empty());
        let m = open(14, 4, 90);
        ev.clear();
        feed.on_msg(&m, &mut ev);
        v.after_msg(&feed, &m, &ev, &mut out);
        assert_eq!(out, vec![SnapOutcome::Match { snap_seq: 14, at_seq: 14 }]);
    }
}
