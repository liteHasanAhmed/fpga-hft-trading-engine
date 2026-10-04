//! Keeps one [`Book`] synchronized with a sequenced L3 stream.
//!
//! The same code runs live (capture/verify) and in replay, so a recording replays
//! through exactly the decisions the live process made.
//!
//! ```text
//!            subscribe / reset
//!  ┌──────────────────────────────────┐
//!  ▼                                  │
//! AwaitingSnapshot ──snapshot + contiguous buffer──▶ Live
//!  ▲                                                  │
//!  └───── gap / book error / buffer overflow / reset ─┘
//! ```
//!
//! * While awaiting a snapshot, sequenced messages are buffered (bounded).
//! * On snapshot: drop buffered `seq <= snapshot.seq`, require the rest to be
//!   contiguous from `snapshot.seq + 1`, apply them, go Live.
//! * Live: `seq <= last` is a duplicate (dropped), `seq > last + 1` is a gap.
//! * Heartbeats report the latest sequence. If a heartbeat's sequence was still
//!   ahead of us when the *next* heartbeat arrives (~1 s later), messages were
//!   lost even though no later message revealed it (catches gaps in quiet markets).

use hft_book::{Book, BookError};
use hft_core::{L3Snapshot, MdEvent, MdMsg, Trade, Ts};
use std::collections::VecDeque;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeedState {
    AwaitingSnapshot,
    Live,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InvalidReason {
    Gap {
        expected: u64,
        got: u64,
    },
    HeartbeatAhead {
        hb_seq: u64,
        last: u64,
    },
    Book,
    BufferOverflow,
    /// Caller reset (disconnect, injected outage).
    Reset,
}

#[derive(Clone, Debug, PartialEq)]
pub enum FeedEvent {
    Trade(Trade),
    /// Book became valid at `seq`.
    Synced {
        seq: u64,
    },
    /// Book became invalid; a snapshot is needed.
    Invalid(InvalidReason),
    Heartbeat {
        exch_ts: Ts,
        seq: u64,
    },
    /// Snapshot did not connect to the buffered stream; another one is needed.
    SnapshotRejected {
        snap_seq: u64,
        first_buffered: Option<u64>,
    },
    BookError(BookError),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FeedStats {
    pub applied: u64,
    pub duplicates: u64,
    pub gaps: u64,
    pub book_errors: u64,
    pub snapshots_applied: u64,
    pub snapshots_rejected: u64,
    pub buffer_overflows: u64,
}

pub struct Feed {
    book: Book,
    state: FeedState,
    last_seq: u64,
    buffer: VecDeque<MdMsg>,
    buffer_cap: usize,
    overflowed: bool,
    /// Sequence reported by the previous heartbeat (for quiet-gap detection).
    prev_hb_seq: Option<u64>,
    last_exch_ts: Ts,
    last_hb_exch_ts: Ts,
    stats: FeedStats,
}

impl Feed {
    pub fn new(buffer_cap: usize) -> Feed {
        Feed {
            book: Book::new(),
            state: FeedState::AwaitingSnapshot,
            last_seq: 0,
            buffer: VecDeque::with_capacity(buffer_cap.min(1 << 16)),
            buffer_cap,
            overflowed: false,
            prev_hb_seq: None,
            last_exch_ts: Ts::ZERO,
            last_hb_exch_ts: Ts::ZERO,
            stats: FeedStats::default(),
        }
    }

    pub fn book(&self) -> &Book {
        &self.book
    }
    pub fn state(&self) -> FeedState {
        self.state
    }
    pub fn is_live(&self) -> bool {
        self.state == FeedState::Live
    }
    pub fn needs_snapshot(&self) -> bool {
        self.state == FeedState::AwaitingSnapshot
    }
    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }
    /// Exchange time of the most recent message of any kind.
    pub fn last_exch_ts(&self) -> Ts {
        self.last_exch_ts
    }
    pub fn last_heartbeat_exch_ts(&self) -> Ts {
        self.last_hb_exch_ts
    }
    pub fn stats(&self) -> FeedStats {
        self.stats
    }

    /// Drop sync (disconnect or injected outage). Buffering restarts empty.
    pub fn reset(&mut self, out: &mut Vec<FeedEvent>) {
        self.buffer.clear();
        self.overflowed = false;
        self.prev_hb_seq = None;
        self.invalidate(InvalidReason::Reset, out);
    }

    fn invalidate(&mut self, r: InvalidReason, out: &mut Vec<FeedEvent>) {
        if matches!(r, InvalidReason::Gap { .. } | InvalidReason::HeartbeatAhead { .. }) {
            self.stats.gaps += 1;
        }
        let was_live = self.state == FeedState::Live;
        self.state = FeedState::AwaitingSnapshot;
        self.book.clear();
        if was_live || r == InvalidReason::Reset {
            out.push(FeedEvent::Invalid(r));
        }
    }

    pub fn on_msg(&mut self, msg: &MdMsg, out: &mut Vec<FeedEvent>) {
        if msg.exch_ts > self.last_exch_ts {
            self.last_exch_ts = msg.exch_ts;
        }
        match &msg.event {
            MdEvent::Snapshot(s) => self.on_snapshot(s, out),
            MdEvent::Heartbeat { .. } => self.on_heartbeat(msg, out),
            // L2 venues: the snapshot arrives on the stream right after subscribing,
            // and continuity is checked per frame before messages reach the feed.
            MdEvent::L2Snapshot(s) => {
                self.book.load_l2(s);
                self.buffer.clear();
                self.state = FeedState::Live;
                self.stats.snapshots_applied += 1;
                out.push(FeedEvent::Synced { seq: msg.seq });
            }
            MdEvent::Level { side, px, qty } => {
                if self.state == FeedState::Live {
                    self.book.set_level(*side, *px, *qty);
                    self.stats.applied += 1;
                }
            }
            MdEvent::TradePrint { trade_id, maker_side, px, qty } => {
                out.push(FeedEvent::Trade(Trade {
                    exch_ts: msg.exch_ts,
                    trade_id: *trade_id,
                    px: *px,
                    qty: *qty,
                    maker_side: *maker_side,
                }));
            }
            _ => match self.state {
                FeedState::AwaitingSnapshot => self.buffer_msg(msg.clone()),
                FeedState::Live => self.on_live(msg, out),
            },
        }
    }

    fn buffer_msg(&mut self, msg: MdMsg) {
        if self.buffer.len() >= self.buffer_cap {
            self.buffer.pop_front();
            if !self.overflowed {
                self.overflowed = true;
                self.stats.buffer_overflows += 1;
            }
        }
        self.buffer.push_back(msg);
    }

    fn on_heartbeat(&mut self, msg: &MdMsg, out: &mut Vec<FeedEvent>) {
        self.last_hb_exch_ts = msg.exch_ts;
        out.push(FeedEvent::Heartbeat { exch_ts: msg.exch_ts, seq: msg.seq });
        if self.state == FeedState::Live {
            if let Some(prev) = self.prev_hb_seq
                && prev > self.last_seq
            {
                self.invalidate(InvalidReason::HeartbeatAhead { hb_seq: prev, last: self.last_seq }, out);
                self.prev_hb_seq = None;
                return;
            }
            self.prev_hb_seq = Some(msg.seq);
        }
    }

    fn on_live(&mut self, msg: &MdMsg, out: &mut Vec<FeedEvent>) {
        if msg.seq <= self.last_seq {
            self.stats.duplicates += 1;
            return;
        }
        if msg.seq != self.last_seq + 1 {
            let r = InvalidReason::Gap { expected: self.last_seq + 1, got: msg.seq };
            self.invalidate(r, out);
            self.buffer_msg(msg.clone());
            return;
        }
        self.apply(msg, out);
    }

    fn apply(&mut self, msg: &MdMsg, out: &mut Vec<FeedEvent>) {
        self.last_seq = msg.seq;
        self.stats.applied += 1;
        let r = match msg.event {
            MdEvent::Open { order_id, side, px, remaining } => self.book.open(order_id, side, px, remaining),
            MdEvent::Done { order_id, remaining, .. } => self.book.done(order_id, remaining).map(|_| ()),
            MdEvent::Change { order_id, new_qty, new_px, .. } => {
                self.book.change(order_id, new_qty, new_px).map(|_| ())
            }
            MdEvent::Match { trade_id, maker_id, maker_side, px, qty, .. } => {
                let r = self.book.match_maker(maker_id, maker_side, px, qty);
                if r.is_ok() {
                    out.push(FeedEvent::Trade(Trade { exch_ts: msg.exch_ts, trade_id, px, qty, maker_side }));
                }
                r
            }
            MdEvent::Received { .. } => Ok(()),
            MdEvent::Heartbeat { .. }
            | MdEvent::Snapshot(_)
            | MdEvent::L2Snapshot(_)
            | MdEvent::Level { .. }
            | MdEvent::TradePrint { .. } => unreachable!("not sequenced"),
        };
        if let Err(e) = r {
            self.stats.book_errors += 1;
            out.push(FeedEvent::BookError(e));
            self.invalidate(InvalidReason::Book, out);
        }
    }

    fn on_snapshot(&mut self, s: &L3Snapshot, out: &mut Vec<FeedEvent>) {
        if self.state == FeedState::Live {
            // Verification snapshots are compared by the caller, never applied.
            return;
        }
        while self.buffer.front().is_some_and(|m| m.seq <= s.seq) {
            self.buffer.pop_front();
        }
        let first = self.buffer.front().map(|m| m.seq);
        // Contiguity: buffered stream must start exactly after the snapshot. With an
        // empty buffer we accept and expect `s.seq + 1` next. After an overflow the
        // dropped prefix may be needed, which the check below catches.
        if first.is_some_and(|f| f != s.seq + 1) {
            self.stats.snapshots_rejected += 1;
            out.push(FeedEvent::SnapshotRejected { snap_seq: s.seq, first_buffered: first });
            return;
        }
        if let Err(e) = self.book.load_snapshot(s) {
            self.stats.book_errors += 1;
            self.stats.snapshots_rejected += 1;
            out.push(FeedEvent::BookError(e));
            self.book.clear();
            return;
        }
        self.stats.snapshots_applied += 1;
        self.overflowed = false;
        self.state = FeedState::Live;
        self.last_seq = s.seq;
        self.prev_hb_seq = None;
        out.push(FeedEvent::Synced { seq: s.seq });
        while let Some(m) = self.buffer.pop_front() {
            if self.state != FeedState::Live {
                // A buffered message broke the book; keep the rest for the next snapshot.
                self.buffer.push_front(m);
                break;
            }
            self.on_live(&m, out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hft_core::{OrderId, Px, Qty, Side, SnapshotOrder};
    use std::sync::Arc;

    fn open(seq: u64, id: u128, side: Side, px: i64, q: i64) -> MdMsg {
        MdMsg {
            seq,
            exch_ts: Ts(seq as i64),
            event: MdEvent::Open { order_id: OrderId(id), side, px: Px(px), remaining: Qty(q) },
        }
    }
    fn snap(seq: u64) -> MdMsg {
        MdMsg {
            seq,
            exch_ts: Ts(seq as i64),
            event: MdEvent::Snapshot(Arc::new(L3Snapshot {
                seq,
                bids: vec![SnapshotOrder { order_id: OrderId(1000), px: Px(99), qty: Qty(5) }],
                asks: vec![],
            })),
        }
    }
    fn hb(seq: u64) -> MdMsg {
        MdMsg { seq, exch_ts: Ts(seq as i64), event: MdEvent::Heartbeat { last_trade_id: 0 } }
    }

    #[test]
    fn startup_sync_drops_stale_buffer() {
        let mut f = Feed::new(100);
        let mut out = vec![];
        for s in 8..=12 {
            f.on_msg(&open(s, s as u128, Side::Bid, 90, 1), &mut out);
        }
        f.on_msg(&snap(10), &mut out); // 8,9,10 dropped; 11,12 applied
        assert!(f.is_live());
        assert_eq!(f.last_seq(), 12);
        assert_eq!(f.book().order_count(), 3); // snapshot order + 11 + 12
        assert_eq!(out, vec![FeedEvent::Synced { seq: 10 }]);
    }

    #[test]
    fn snapshot_older_than_buffer_is_rejected() {
        let mut f = Feed::new(100);
        let mut out = vec![];
        f.on_msg(&open(20, 20, Side::Bid, 90, 1), &mut out);
        f.on_msg(&snap(10), &mut out);
        assert!(f.needs_snapshot());
        assert_eq!(out, vec![FeedEvent::SnapshotRejected { snap_seq: 10, first_buffered: Some(20) }]);
        out.clear();
        f.on_msg(&snap(19), &mut out);
        assert!(f.is_live());
        assert_eq!(f.last_seq(), 20);
    }

    #[test]
    fn gap_and_duplicate() {
        let mut f = Feed::new(100);
        let mut out = vec![];
        f.on_msg(&snap(10), &mut out);
        f.on_msg(&open(11, 11, Side::Bid, 90, 1), &mut out);
        f.on_msg(&open(11, 11, Side::Bid, 90, 1), &mut out); // duplicate: ignored
        assert_eq!(f.stats().duplicates, 1);
        out.clear();
        f.on_msg(&open(13, 13, Side::Bid, 90, 1), &mut out); // 12 missing
        assert_eq!(out, vec![FeedEvent::Invalid(InvalidReason::Gap { expected: 12, got: 13 })]);
        assert!(f.needs_snapshot());
        assert_eq!(f.book().order_count(), 0);
        // Recovery: snapshot at 12 + buffered 13 → live at 13.
        out.clear();
        f.on_msg(&snap(12), &mut out);
        assert!(f.is_live());
        assert_eq!(f.last_seq(), 13);
    }

    #[test]
    fn heartbeat_detects_quiet_gap() {
        let mut f = Feed::new(100);
        let mut out = vec![];
        f.on_msg(&snap(10), &mut out);
        f.on_msg(&hb(12), &mut out); // exchange is at 12, we're at 10 — may still be in flight
        assert!(f.is_live());
        f.on_msg(&hb(12), &mut out); // next heartbeat: 11, 12 never arrived
        assert!(f.needs_snapshot());
        assert!(out.contains(&FeedEvent::Invalid(InvalidReason::HeartbeatAhead { hb_seq: 12, last: 10 })));

        // In-flight case: messages arrive between heartbeats → no gap.
        let mut g = Feed::new(100);
        g.on_msg(&snap(10), &mut out);
        g.on_msg(&hb(12), &mut out);
        g.on_msg(&open(11, 11, Side::Bid, 90, 1), &mut out);
        g.on_msg(&open(12, 12, Side::Bid, 90, 1), &mut out);
        g.on_msg(&hb(12), &mut out);
        assert!(g.is_live());
    }

    #[test]
    fn overflow_then_recover() {
        let mut f = Feed::new(3);
        let mut out = vec![];
        for s in 11..=15 {
            f.on_msg(&open(s, s as u128, Side::Bid, 90, 1), &mut out);
        }
        assert_eq!(f.stats().buffer_overflows, 1);
        f.on_msg(&snap(10), &mut out); // 11, 12 were dropped
        assert!(f.needs_snapshot());
        f.on_msg(&snap(12), &mut out);
        assert!(f.is_live());
        assert_eq!(f.last_seq(), 15);
    }

    #[test]
    fn book_error_invalidates() {
        let mut f = Feed::new(100);
        let mut out = vec![];
        f.on_msg(&snap(10), &mut out);
        out.clear();
        f.on_msg(&open(11, 1000, Side::Bid, 90, 1), &mut out); // duplicate id of snapshot order
        assert!(matches!(out[0], FeedEvent::BookError(_)));
        assert_eq!(out[1], FeedEvent::Invalid(InvalidReason::Book));
    }
}
