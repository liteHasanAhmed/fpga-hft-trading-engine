# Decision log

Accepted decisions, with the evidence behind them. Revisit only on new evidence.

| # | Decision | Choice | Date |
|---|---|---|---|
| D1 | Priority when goals conflict | **Execution realism** — correctness and honest timing are never traded for speed | 2026-10-04 |
| D2 | Time / depth | Delegated; full depth, built end to end | 2026-10-04 |
| D3 | Venue / data (**revised**, see below) | **Coinbase Advanced Trade, BTC-USD, public L2** (`level2` + `market_trades` + `heartbeats`). Coinbase Exchange L3 code kept; Kraken L3 planned once an account exists | 2026-10-04 |
| D4 | Demo headline | Low-latency engine, on a correct and reproducible pipeline | 2026-10-04 |
| D5 | Fees | Report both: professional (0% maker / 0.05% taker) and retail (0.60% / 1.20%) | 2026-10-04 |
| D6 | Fill model v1 | Instant fill (up to the trade's size, at our price) when a real trade prints at or through our resting price; no queue. Conservative variant: strictly through. Latency model and queue model later | 2026-10-04 |
| D7 | Latency metric | In-process tick-to-decision (decode → book → strategy), HDR percentiles + throughput, on replayed data; hardware recorded | 2026-10-04 |
| D8 | Recorder host | Old laptop running 24/7 (see `ops/`); cloud VM revisited if gaps or jitter limit the data | 2026-10-04 |

## D3 revision — why L2 instead of L3

Originally: Coinbase Exchange `full` channel (per-order L3). Evidence from `hft probe` (2026-10-04):

```
{"type":"error","message":"Failed to subscribe","reason":"level2, level3, and full channels now require authentication. ..."}
REST L3 snapshot OK: seq 137236858058, 64736 bids, 40679 asks, 6.5 MB
```

The `full` channel needs a **Coinbase Exchange** API key, which requires an institutional
Exchange account ([Advanced Trade API FAQ](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/faq)).
Options considered: Coinbase Advanced Trade public L2 (chosen: works today, same venue and fee schedule),
Kraken WS v2 `level3` (per-order, CRC32 checksum, needs a free Kraken account — planned next).

What changed in the build: a second parser (`hft_coinbase::advanced`), an L2 mode in the
book, and true-time ordering of batched trades in the simulator. Everything else is venue-agnostic.

Consequences:
* Queue position can only be estimated on L2 (the v1 fill rule ignores queues anyway).
* `market_trades` are batched over 250 ms; the runner reorders exchange-side events by engine
  time (`reorder_window_ms`) so a late-arriving trade cannot be missed by a cancel that really
  arrived after it — otherwise the cancel-happy arm B would be flattered.
* Measured natural feed lag is large (level2 updates p50 ≈ 200–300 ms, p99 ≈ 1 s after engine time,
  with the capture clock within ±3 ms of NTP), so stale-data thresholds are venue-specific.

## Guard thresholds (tuning parameter, not a decision)

The first live minute showed the initial 150 ms floor treated normal Advanced Trade traffic as
stale (14 trips/minute). The guard now uses an adaptive baseline (EWMA of clipped lag) with a
1 s floor and 3× multiplier. Final values must be chosen on the **dev** split and confirmed on
**validation** before the **test** split is run once.
