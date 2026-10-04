//! Stale-data protection: the *only* behavior that differs between arm A and arm B.
//!
//! Signals, all strategy-observable:
//! 1. **Feed lag**: `now - exch_ts` of each delivered message. Covers network delay
//!    and our own processing backlog (a backlogged message is delivered late).
//!    Triggers above `max(lag_floor, lag_mult × baseline)`, where the baseline is
//!    an EWMA of every lag sample, each clipped at the current threshold so a
//!    burst of stale data cannot drag the baseline up and hide itself. The
//!    baseline matters because "normal" lag is venue-specific: Coinbase Advanced
//!    Trade level2 updates routinely arrive ~200 ms after engine time.
//! 2. **Heartbeat silence**: `now - last heartbeat exch_ts`. Coinbase heartbeats
//!    arrive about once a second even when nothing trades, so silence means a dead
//!    or stalled feed, not a quiet market. This is what prevents false alarms in
//!    quiet periods: we never use time-since-last-trade.
//!
//! Pause as soon as any signal fires; resume after `resume_after` with every
//! signal clear (hysteresis, so we don't flap).

use crate::Ctx;
use hft_core::Ts;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StaleGuardConfig {
    /// Absolute minimum lag that counts as stale.
    pub lag_floor_ns: i64,
    /// Lag threshold as a multiple of the healthy-lag EWMA.
    pub lag_mult: f64,
    /// EWMA weight per message for the healthy-lag baseline.
    pub ewma_alpha: f64,
    /// Heartbeat silence that counts as stale.
    pub heartbeat_timeout_ns: i64,
    /// All-clear duration required before quoting resumes.
    pub resume_after_ns: i64,
}

impl Default for StaleGuardConfig {
    fn default() -> Self {
        StaleGuardConfig {
            lag_floor_ns: 1_000_000_000,
            lag_mult: 3.0,
            ewma_alpha: 0.01,
            heartbeat_timeout_ns: 2_500_000_000,
            resume_after_ns: 1_000_000_000,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum GuardState {
    Clear,
    Tripped {
        since: Ts,
    },
    /// Signals cleared at `since`; waiting out the hysteresis window.
    Recovering {
        since: Ts,
    },
}

pub struct StaleGuard {
    cfg: StaleGuardConfig,
    /// Baseline lag (ns); `None` until the first sample.
    ewma_lag: Option<f64>,
    state: GuardState,
    pub trips: u64,
}

impl StaleGuard {
    pub fn new(cfg: StaleGuardConfig) -> StaleGuard {
        StaleGuard { ewma_lag: None, cfg, state: GuardState::Clear, trips: 0 }
    }

    pub fn state(&self) -> GuardState {
        self.state
    }

    pub fn threshold_ns(&self) -> f64 {
        (self.cfg.lag_mult * self.ewma_lag.unwrap_or(0.0)).max(self.cfg.lag_floor_ns as f64)
    }

    /// Update with the current context. Returns `true` if quoting is allowed.
    pub fn update(&mut self, ctx: &Ctx<'_>) -> bool {
        let lag_bad = match ctx.msg_exch_ts {
            Some(t) => {
                let lag = (ctx.now - t).0 as f64;
                let threshold = self.threshold_ns();
                let clipped = lag.min(threshold);
                self.ewma_lag = Some(match self.ewma_lag {
                    None => clipped,
                    Some(b) => b + self.cfg.ewma_alpha * (clipped - b),
                });
                lag > threshold
            }
            // No new message (timer/order event): judge by how old the newest data was
            // when it arrived. Time spent waiting since then is not lag — a quiet
            // market with fresh heartbeats must not trip the guard.
            None => (ctx.last_lag.0 as f64) > self.threshold_ns(),
        };
        let hb_bad = ctx.last_heartbeat_exch_ts > Ts::ZERO
            && (ctx.now - ctx.last_heartbeat_exch_ts).0 > self.cfg.heartbeat_timeout_ns;
        let bad = lag_bad || hb_bad;
        self.state = match (self.state, bad) {
            (GuardState::Clear, true) => {
                self.trips += 1;
                GuardState::Tripped { since: ctx.now }
            }
            (GuardState::Clear, false) => GuardState::Clear,
            (GuardState::Tripped { since }, true) => GuardState::Tripped { since },
            (GuardState::Recovering { .. }, true) => GuardState::Tripped { since: ctx.now },
            (GuardState::Tripped { .. }, false) => GuardState::Recovering { since: ctx.now },
            (GuardState::Recovering { since }, false) => {
                if (ctx.now - since).0 >= self.cfg.resume_after_ns {
                    GuardState::Clear
                } else {
                    GuardState::Recovering { since }
                }
            }
        };
        self.state == GuardState::Clear
    }
}
