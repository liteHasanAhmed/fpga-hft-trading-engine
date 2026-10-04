use crate::{Item, ItemStream, SnapOutcome, SnapshotVerifier};
use hft_feed::{Feed, FeedEvent, FeedStats};

/// Result of replaying a stream through the feed with no strategy.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct VerifyReport {
    pub items: u64,
    pub md_msgs: u64,
    pub checkpoints_ok: u64,
    pub checkpoint_mismatches: Vec<(u64, u64, u64)>, // (seq, expected, got)
    pub verify_snapshots_ok: u64,
    pub verify_snapshot_mismatches: u64,
    pub verify_snapshots_unverifiable: u64,
    pub mismatch_details: Vec<String>,
    pub disconnects: u64,
    pub exchange_errors: Vec<String>,
    pub final_seq: u64,
    pub final_hash: u64,
    pub feed: FeedStatsLite,
    /// Hash over every feed event and checkpoint — two replays must agree.
    pub digest: u64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FeedStatsLite {
    pub applied: u64,
    pub duplicates: u64,
    pub gaps: u64,
    pub book_errors: u64,
    pub snapshots_applied: u64,
    pub snapshots_rejected: u64,
}

impl From<FeedStats> for FeedStatsLite {
    fn from(s: FeedStats) -> Self {
        FeedStatsLite {
            applied: s.applied,
            duplicates: s.duplicates,
            gaps: s.gaps,
            book_errors: s.book_errors,
            snapshots_applied: s.snapshots_applied,
            snapshots_rejected: s.snapshots_rejected,
        }
    }
}

struct Fnv(u64);
impl Fnv {
    fn mix(&mut self, v: u64) {
        for b in v.to_le_bytes() {
            self.0 ^= b as u64;
            self.0 = self.0.wrapping_mul(0x100000001b3);
        }
    }
}

/// Replay `src` through a [`Feed`] and check every recorded live checkpoint and
/// verification snapshot against the reconstructed book.
pub fn verify_stream(src: &mut dyn ItemStream, check_invariants_every: u64) -> anyhow::Result<VerifyReport> {
    let mut feed = Feed::new(1 << 20);
    let mut out = Vec::with_capacity(64);
    let mut rep = VerifyReport::default();
    let mut d = Fnv(0xcbf29ce484222325);
    let mut sv = SnapshotVerifier::new(1 << 18);
    let mut outcomes = Vec::new();
    while let Some(item) = src.next_item()? {
        rep.items += 1;
        match item {
            Item::Md { msg, .. } => {
                rep.md_msgs += 1;
                out.clear();
                feed.on_msg(&msg, &mut out);
                sv.after_msg(&feed, &msg, &out, &mut outcomes);
                for e in &out {
                    d.mix(match e {
                        FeedEvent::Trade(t) => t.trade_id ^ (t.qty.0 as u64) << 1,
                        FeedEvent::Synced { seq } => 0x5100 ^ seq,
                        FeedEvent::Invalid(_) => 0x1A,
                        FeedEvent::Heartbeat { seq, .. } => 0x4B ^ seq,
                        FeedEvent::SnapshotRejected { snap_seq, .. } => 0x5E ^ snap_seq,
                        FeedEvent::BookError(_) => 0xBE,
                    });
                }
                if check_invariants_every > 0 && feed.is_live() && rep.md_msgs % check_invariants_every == 0 {
                    feed.book().check_invariants().map_err(|e| anyhow::anyhow!("seq {}: {e}", feed.last_seq()))?;
                }
            }
            Item::Disconnect { .. } => {
                rep.disconnects += 1;
                out.clear();
                feed.reset(&mut out);
                sv.reset();
            }
            Item::Checkpoint { cp, .. } => {
                let got = feed.book().state_hash();
                if feed.last_seq() == cp.seq && got == cp.hash {
                    rep.checkpoints_ok += 1;
                } else {
                    rep.checkpoint_mismatches.push((cp.seq, cp.hash, got));
                }
                d.mix(cp.hash);
            }
            Item::VerifySnapshot { snap, .. } => sv.submit(&feed, snap, &mut outcomes),
            Item::ExchangeError { msg, .. } => rep.exchange_errors.push(msg),
            Item::Connect { .. } => {}
        }
    }
    for o in outcomes {
        match o {
            SnapOutcome::Match { .. } => rep.verify_snapshots_ok += 1,
            SnapOutcome::Mismatch { snap_seq, at_seq, first_diffs } => {
                rep.verify_snapshot_mismatches += 1;
                rep.mismatch_details.push(format!("snapshot {snap_seq} @ {at_seq}: {}", first_diffs.join("; ")));
            }
            SnapOutcome::Unverifiable { .. } => rep.verify_snapshots_unverifiable += 1,
        }
    }
    rep.final_seq = feed.last_seq();
    rep.final_hash = feed.book().state_hash();
    rep.feed = feed.stats().into();
    d.mix(rep.final_hash);
    rep.digest = d.0;
    Ok(rep)
}
