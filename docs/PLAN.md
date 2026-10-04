# Rust Market-Making Research System — Project Plan

> **Status (2026-10-04):** M0–M6 implemented and tested; M2/M3 running on live Coinbase
> Advanced Trade data. **D3 was revised** after the live probe: the Coinbase Exchange L3 feed
> requires an institutional account, so the primary feed is the public Advanced Trade L2 feed.
> See `docs/DECISIONS.md`. Sections below that mention L3 describe the original plan;
> where they conflict, `DECISIONS.md` and `ASSUMPTIONS.md` are authoritative.

## Context
The repo `fpga-hft-trading-engine` currently holds a toy Verilator counter (`rtl/`, `tb/`, committed `obj_dir/` build output). The user has authorized deleting all of it. We are replacing it with a Rust system that:
1. captures Coinbase BTC-USD market data;
2. maintains a verified local order book;
3. records and deterministically replays that data;
4. runs a baseline market maker (A) and the same market maker with stale-data protection (B) inside a self-built simulator;
5. reports a paired A/B comparison after fees.

The headline demonstration is a fast engine with reproducible benchmarks, built on a pipeline that is correct and reproducible first. FPGA work is out of scope until a measured limitation justifies it.

Note: we are an observer/simulator only. No real orders are placed in any milestone below.

---

## 1. Decisions (accepted)
| # | Decision | Choice | Why / consequence |
|---|---|---|---|
| D1 | Priority when goals conflict | **Execution realism** | Correctness and honest timing are never traded for speed |
| D2 | Time / depth | **Delegated to Claude, full depth** | Claude implements end to end; sizes below are relative (S/M/L) |
| D3 | Venue / data | **Coinbase Exchange, BTC-USD, L3 `full` channel** | Per-order data plus sequence numbers allow exact gap detection and later honest queue modeling. Fallback: Kraken WS v2 L2 with CRC32 checksum |
| D4 | Demo headline | **Low-latency engine** | Allocation-free hot path from day one; benchmarks are first-class deliverables |
| D5 | Fees | **Report both tiers** | Professional tier (0% maker / 0.05% taker) and retail (0.60% / 1.20%), plus a fee sweep table |
| D6 | Fill model v1 | **Instant full fill when a real trade prints at or through our resting price**; no queue | Simple, and stale quotes still get picked off realistically. The fill model is a trait, so latency and L3 queue models plug in later |
| D7 | Latency metric | **In-process tick-to-decision** | Raw bytes → parsed → book → strategy decision. HDR histograms (p50/p99/p99.9/max) plus throughput on replayed data, with hardware documented |
| D8 | Recorder host | **Old laptop running 24/7** (cloud VM revisited later) | Sleep and Wi-Fi drops become explicit, recorded gaps |

## 2. Scope boundaries
- **First working pipeline (M1–M3):** live Coinbase L3 → book with sync and gap recovery → recorder → replay that reconstructs a bit-identical book.
- **First complete experiment (M4–M6):** A vs B on identical recordings across healthy, delayed, outage, backlog and recovery scenarios. Uses chronological dev/validation/test splits. Results tables at both fee tiers.
- **Performance (M7):** benchmarks plus an optimization pass with before/after evidence.
- **Later improvements:** a latency model ("what would really happen") and an L3 queue-position fill model. Both reuse the same experiment harness.
- **Optional extensions (§13):** second venue, price-pressure signal, live paper trading, cloud recorder, FPGA.
- **Success types, kept separate:**
  - *Engineering success* is binary and testable: the book matches, replay is deterministic, accounting reconciles, benchmarks are reproducible.
  - *Trading evidence* is a measured difference with uncertainty. It can be null or negative, and that is a valid result.

## 3. Exchange & data (sources)
Documented in the official docs:
- **WS `full` channel** ([channels](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels)):
  - message types `received`/`open`/`done`/`match`/`change`, each with a per-product `sequence`;
  - `time` = "time of the event as recorded by our trading engine";
  - sync = buffer WS messages → fetch the REST L3 snapshot → drop messages with seq ≤ snapshot seq → apply the rest;
  - "Not all done or change messages result in changing the order book."
- **Dropped messages** ([overview](https://docs.cdp.coinbase.com/exchange/websocket-feed/overview)): "WebSocket servers receive market data in a manner that can result in dropped messages". Clients must handle sequence gaps and out-of-order messages.
  - Endpoints: prod `wss://ws-feed.exchange.coinbase.com`, sandbox `wss://ws-feed-public.sandbox.exchange.coinbase.com`.
  - `heartbeat` channel available.
- **REST snapshot** ([product book](https://docs.cdp.coinbase.com/exchange/reference/exchangerestapi_getproductbook)): `GET /products/{id}/book?level=3` is public and returns `sequence`, `time`, `auction_mode`. Warning: "Abuse of Level 3 via polling can cause your access to be limited" → fetch only at startup and on resync, with exponential backoff.
- **Fees** ([secondary summary](https://cryptofeediscount.com/academy/coinbase-fees-2026/)): 0.60%/1.20% at retail down to 0% maker at the top tier. Verify against Coinbase's own fee page in M6.

Assumptions and open items:
- **⚠ Does `full` need auth on ws-feed?** The docs conflict. An M2 probe settles it. If auth is needed: a free API key with HMAC-SHA256 signing (key, passphrase, timestamp). If L3 is inaccessible: fall back to `level2_batch` (public, 50 ms batches) or Kraken v2 L2 + CRC32.
- **⚠ Redistribution of recorded data:** licensing terms are unverified. The plan ships the capture tool and a minutes-long sample, pending a terms check in M8.
- **What L2 couldn't tell us, and L3 can:** individual order arrivals, cancellations and queue order at a price. Because we're on L3, we know exactly which resting orders a `match` consumed. Even L3 can't reveal hidden intent, or how the market would have reacted to *our* orders (market impact).

## 4. Architecture & repo layout (Cargo workspace)
```
Cargo.toml                 # workspace, shared lints/profile (release: lto, codegen-units=1)
crates/
  core/        # fixed-point Px/Qty, Instrument, ids, Event enums, Timestamp types, Clock trait
  coinbase/    # WS + REST client, zero-copy JSON parser → normalized events, auth (optional)
  book/        # L3 book (order map + per-level FIFO), L2 view, invariants, state hash
  feed/        # sync state machine: Connecting → Buffering → Snapshot → Live / Stale / Invalid → Resync
  recorder/    # append-only segmented log, CRC per record, crash recovery, gap markers
  replay/      # log reader, virtual clock, deterministic event scheduler, failure injection
  sim/         # exchange simulator: order lifecycle, FillModel trait, latency model, fees, accounting
  strategy/    # Strategy trait, BaselineMM, StaleGuard wrapper
  experiment/  # scenario configs, paired runs, data splits, metrics collection
  report/      # tables/plots → reports/<run-id>/ (markdown + CSV + JSON + HTML)
  bench/       # criterion benches + tick-to-decision HDR harness
apps/hft/      # single CLI: capture | verify | replay | run | bench | report
fixtures/      # tiny hand-written event logs with hand-calculated expected results
reports/       # generated experiment outputs (gitignored except curated examples)
docs/          # PLAN.md (this), ARCHITECTURE.md, ASSUMPTIONS.md, DECISIONS.md
```
Runtime model:
- **Live:** `tokio` handles only network I/O. Raw frames are timestamped at receive and go over a bounded SPSC ring to one single-threaded core loop (parse → book → strategy).
- **Replay:** no async; the same core loop is driven by the virtual clock.
- The same `book` and `feed` code runs live and in replay.

## 5. Shared data contracts (sketches)
```rust
pub struct Px(i64);   // price in ticks (BTC-USD tick = 0.01 USD) — from Instrument
pub struct Qty(i64);  // base units (1e-8 BTC)
pub struct Instrument { id: InstrumentId, symbol: Symbol, tick: Decimal, lot: Decimal, min_size: Qty }

pub struct ExchTs(i64);   // exchange matching-engine time, ns since epoch (from `time`)
pub struct RecvTs(i64);   // local wall-clock at socket receive, ns (+ monotonic for intervals)
pub struct SimTs(i64);    // virtual clock time in replay/sim

pub enum MdEvent {           // normalized, per product, carries `seq`
    Received { order_id, side, px: Option<Px>, qty, kind },
    Open { order_id, side, px, remaining },
    Done { order_id, side, px: Option<Px>, remaining, reason: Filled|Canceled },
    Match { trade_id, maker_id, taker_id, maker_side, px, qty },
    Change { order_id, new_qty, .. },
    Snapshot(L3Snapshot),
}
pub enum FeedHealth { Synced{seq}, Gap{expected, got}, Resyncing, Disconnected, Heartbeat{seq, exch_ts} }
pub enum BookState { Valid, Stale, Invalid }   // see §6

pub enum OrderReq { Place { cl_id, side, px, qty, post_only: true }, Cancel { cl_id } }
pub enum OrderEvt { Ack{cl_id, ts}, Reject{cl_id, reason}, Fill{cl_id, px, qty, fee, liquidity: Maker}, CancelAck{cl_id}, CancelReject{cl_id, TooLateFilled} }
```
Ownership and ordering rules:
- `feed` owns the real book.
- `sim` owns our virtual orders, cash and inventory. The strategy only sees `OrderEvt`s.
- `strategy` owns its intents (what it wants quoted) and nothing else.
- Every event carries `(ExchTs, seq)` and, where it exists, `RecvTs`.
- Ordering within a product is by `seq`.
- Simulator events at equal time are ordered by fixed priority, then insertion counter: market data < order arrival at exchange < cancel arrival < timers < responses to strategy.

## 6. Feed & order book
- **Startup:** subscribe to `full` + `heartbeat`, buffer up to a bounded cap, then fetch the REST L3 snapshot. Drop messages with seq ≤ snapshot seq. If the first remaining seq ≠ snapshot+1 and the buffer doesn't cover it, re-fetch.
- **Incremental:** apply `open`/`done`/`match`/`change` to the order map. Handle `received` without book effect (it is book-relevant only once `open`). A `match` decreases the maker order, and the following `done` removes the zero remainder. This is the rule that avoids double-counting trade vs book decrease.
- **Gaps and duplicates:**
  - seq ≤ last → duplicate, drop and count it;
  - seq > last+1 → `Invalid`, emit `Gap`, enter resync (new snapshot with backoff; WS kept open and buffering).
- **Reconnect:** jittered exponential backoff; the book stays `Invalid` until resynced. Each disconnect is written to the recording as an explicit gap.
- **Book states:**
  - `Valid` = synced and recent;
  - `Stale` = synced, but the last update or heartbeat is older than a threshold (signal only; the book is still correct as far as we know);
  - `Invalid` = gap, disconnect or resync.
- **Bursts:** the bounded ring between network and core is sized for the observed p99.99 burst. On overflow → mark `Invalid` and resync. Never drop silently. Queue depth and event age are exported as metrics (these feed the backlog signal).
- **Correctness checks:**
  - invariants after every event in debug/test (no crossed book when not in auction, level qty = sum of orders, no zero-size orders);
  - periodic verification against a fresh REST snapshot, compared at matching seq (rate-limited, e.g. every 30 min, in `verify` mode);
  - a 64-bit rolling book-state hash for replay equality.
- **Profiling:** a `perf`/`samply` flamegraph on a replay of 1 h of data; allocation counting in tests (a counting global allocator asserts zero allocations per event in steady state).

## 7. Recorder & replay
- **Record raw:** the exact WS bytes and REST snapshot bytes, plus `RecvTs` (wall + monotonic), a connection id and a record kind. Normalization is re-derived by the same parser at replay, so parser bugs are fixable after the fact. A normalized binary cache is optional (M7) for fast replay.
- **Format:** segment files (`YYYYMMDD-HH.seg`), each record `[len u32][kind u8][crc32 u32][recv_ts][mono_ts][payload]`. Each segment has a header with schema version, product, git hash, host and clock info. A `session.json` manifest goes alongside. Closed segments are zstd-compressed.
- **Gaps and markers:** `SessionStart`, `ConnOpen`/`ConnClose`, `SnapshotBegin/End`, `GapMarker{reason}`, and `ClockSync{ntp_offset}` records.
- **Crash recovery:** on start, scan the last segment and truncate at the first bad CRC or short record. Log the truncation as a gap. fsync policy: per segment rotation plus every N ms (configurable; benchmarked).
- **Laptop 24/7 ops:** run under launchd/systemd with auto-restart; prevent sleep (`caffeinate`/logind); NTP sync status logged hourly; disk usage alarm.
- **Replay:**
  - a `VirtualClock` drives one `BinaryHeap` scheduler keyed `(SimTs, priority, insertion_seq)`;
  - timers are scheduled events;
  - RNG is ChaCha8 seeded per run and per component (`seed ^ hash(component)`).
  - Replay speed (wall time to process) is independent of simulated market time, so we can run as fast as possible or at 1×.
- **Determinism:** two replays of the same log + config + seed produce identical event logs and report hashes (tested in CI).

## 8. Timing & information visibility
Four distinct clocks/views:
1. **Exchange state at `ExchTs`:** the true book from the recording.
2. **Strategy-visible state:** what the strategy has received by sim time `t`. An event becomes visible at `RecvTs + injected_feed_delay` (recorded real latency plus any injected extra). Clock offset between laptop and exchange is estimated as the rolling minimum of `RecvTs − ExchTs` and documented.
3. **Order effect time:** our place/cancel arrives at the exchange at `decision_t + order_latency` / `cancel_latency`. It only interacts with trades whose exchange-time image is ≥ arrival. Fills can happen while a cancel is in flight.
4. **Response time:** acks and fills reach the strategy at `exchange_event_t + response_latency`.

During injected delays, (1) keeps evolving while (2) lags. The strategy API only receives events through the visibility queue, so it has no access to the future. A test asserts that every strategy input has `visible_ts ≤ now`.

v1 latencies default to 0 for order/cancel/response (per D6). The model exists from M4 so later realism is a config change.

## 9. Execution simulator (v1 → later)
**v1 (smallest credible):**
- **Orders:** post-only limit orders only. Reject if the order would cross the book visible at arrival, if size < min, or if it breaks price/tick rules.
- **Fill rule (D6):** a resting order fills fully the instant a real `match` prints at or through our price on our side, at our price, as a maker fee. Partial fills are supported in the data model; v1 caps the fill at the trade size (we never fill more than the real printed qty), which gives natural partials.
- **Double-counting:** our virtual fills never remove real liquidity; each real trade can fill each of our orders at most once.
- **Cancels:** effective at arrival. A fill that occurs before arrival wins, and the cancel returns `TooLateFilled`.
- **Accounting:**
  - cash, inventory, fees, open-order exposure (worst-case inventory if all fill);
  - mark-to-mid each event;
  - end of run: report both mark-to-mid and liquidation-at-touch-plus-taker-fee.
- **Fees:** per scenario (both tiers per D5).

**Later:**
- latency model (constants → empirical distributions measured from our own recorded `RecvTs − ExchTs`);
- L3 queue model (our order joins behind real orders at arrival; fills only after those ahead are matched or cancelled);
- a probabilistic "touch ≠ fill" variant.

**Known limitations (documented in every report):** touch ≠ fill; no market impact from our orders; results are conditional on the fill model.

**Synthetic exchange emulator:** not needed for the experiment. A small deterministic scripted emulator is built in M4 only as a test harness for lifecycle and race cases.

## 10. Strategy & stale-data protection
**Baseline A:**
- reference price = mid (microprice is a later option);
- quote bid/ask at `ref ± max(half_spread, 1 tick)`, joining the touch when the spread is 1 tick;
- fixed size;
- inventory skew `skew = −k·inventory` ticks;
- hard inventory limit → quote only the reducing side;
- requote when ref moves ≥ 1 tick or on a 100 ms timer;
- never more than one live order per side; pending-cancel orders count toward exposure.

**Essential checks in both arms (not under test):**
- cancel all and pause when the book is `Invalid` (gap/disconnect/resync);
- inventory and exposure limits;
- no quoting through the book.

**B = A + StaleGuard. The only extra behavior under test:**
- **Signals:**
  - feed lag `visible_now − ExchTs(last applied)` above a threshold relative to the rolling p99 of healthy lag;
  - heartbeat silence > T_hb (heartbeats arrive about every 1 s even in quiet markets, so silence ≠ quiet);
  - processing backlog (ring depth or event age above a limit).
- **Action:** cancel all quotes when any signal holds for ≥ debounce.
- **Resume:** all signals clear for a hysteresis window.
- **Quiet markets:** lag is measured on heartbeats and events, never on time-since-last-trade, so a calm market doesn't trigger a pause.

Price-pressure signals are deferred to a later experiment.

## 11. Experiment & reporting
- **Data:** contiguous recorded days split chronologically into dev (tune thresholds), validation (pick the config) and test (touched once, at the end).
- **Paired runs:** A and B run on the same log, seed and scenario.
- **Scenarios:**
  - healthy;
  - +X ms feed delay;
  - outages (WS cut for N s → resync);
  - processing backlog (core stall injection);
  - recovery;
  - order/cancel latency grid;
  - conservative fill variant (strict trade-through).
- **Metrics:**
  - net P&L at both fee tiers;
  - inventory path and valuation;
  - fills and volume;
  - markouts (mid change at +100 ms / 1 s / 10 s after each fill = adverse selection);
  - max drawdown;
  - time paused;
  - cancel count and rate.
- **Statistics:** per-hour paired differences B−A with bootstrap CIs.
- **Interpretation guardrails:**
  - report P&L per fill *and* fills lost, so "traded less" can't masquerade as "traded better";
  - natural recorded gaps and injected failures are reported in separate sections.

## 12. Verification & benchmarks
**Tests:**
- *Book:* hand-written L3 fixtures (open/match/done/change sequences) with expected L2 tables; property tests (random valid op streams → invariants); live vs REST snapshot diff.
- *Recorder:* kill -9 mid-write fixture → recovery truncates, and a gap marker is present; corrupt-CRC fixture.
- *Replay:* identical hashes over two runs; live-run book hash = replay hash.
- *Lifecycle:* scripted emulator cases (fill before cancel arrival, cancel before fill, partial then cancel, reject cross).
- *Accounting:* a 3-fill hand-calculated example (cash, inventory, fees, mark) checked to the cent.
- *Fill rule:* trade at / through / not reaching our price; trade qty < our qty.
- *Visibility:* a strategy probe asserts no input has `visible_ts > now` under injected delay.
- *Integration:* fixture → replay → A/B → report golden file.

**Benchmarks (criterion + HDR):**
- parse ns/msg;
- book apply ns/event;
- recorder MB/s at each fsync policy;
- replay events/s;
- tick-to-decision p50/p99/p99.9.

Measurement boundary is stated per benchmark. Each report records CPU, OS, rustc version and power state.

## 13. Milestones (each → GitHub issues)
| M | Deliverable | Acceptance criteria | Size | Main risk |
|---|---|---|---|---|
| **M0** | Wipe repo, workspace scaffold, CI (fmt, clippy -D warnings, test), docs skeleton | `cargo test` green on an empty-ish workspace; old files gone | S | — |
| **M1** | `core` types + **vertical slice on a tiny hand-written fixture**: fixture log → replay → baseline MM → sim v1 → report | The fixture report matches hand-calculated P&L, fills and inventory exactly | M | Interface churn (this milestone is meant to surface it early) |
| **M2** | `coinbase` + `book` + `feed`: live L3 BTC-USD, sync, gap/dup handling, reconnect, health states, `hft verify` | 24 h live run with 0 unexplained divergences vs periodic REST snapshots; forced disconnect recovers to `Valid` | L | L3 access/auth, message volume, REST rate limits |
| **M3** | `recorder` + `replay`: segmented log, crash recovery, gap markers, deterministic replay | Replay of a live session reproduces the live book hash at every checkpoint; kill -9 test passes; two replays are byte-identical | M | Timestamp/clock subtleties |
| **M4** | `sim` v1 full: latency model plumbing, cancel/fill races, fees, accounting, scripted emulator tests | All lifecycle and accounting hand cases pass | M | Subtle race semantics |
| **M5** | `strategy`: Baseline A + StaleGuard B; visibility probe | Visibility test passes; both arms run on 1 day of data | M | Threshold tuning leaking test data (prevented by the splits) |
| **M6** | `experiment` + `report`: scenarios, failure injection, splits, both fee tiers, bootstrap CIs | One command produces the full A/B report from a recording; rerun is identical | M | Too little recorded data (start recording at M3) |
| **M7** | Performance: benches, zero-alloc hot path assertion, profiling, optimization pass | Bench report with before/after and hardware context; zero allocations per event in steady state | M | Laptop noise (pin cores where possible, repeat runs) |
| **M8** | Demo: README, commands, sample dataset, injected outage walkthrough, ASSUMPTIONS.md | A fresh clone runs `hft run fixtures/demo` and reproduces the published tables | S | Data licensing |

Recording starts as soon as M3 lands, so weeks of data accumulate while M4–M6 are built.

## 14. Demonstration
```
hft capture --product BTC-USD --out data/            # laptop, 24/7
hft verify  --product BTC-USD                        # live book vs REST snapshot
hft replay  data/2026-10-xx --check-hash
hft run     --config experiments/stale_guard.toml --split test   # A/B, all scenarios
hft bench                                            # latency/throughput report
```
Outputs land in `reports/<run-id>/`: a summary, A/B tables at both fee tiers, markouts, an outage timeline plot, a bench histogram and the list of assumptions.

## 15. Future extensions (each tied to a question)
- **Latency model / L3 queue fills:** first, once v1 results exist. Question: does B's edge survive realistic queue position and latency?
- **Cloud recorder (us-east):** when laptop gaps or jitter measurably limit the data.
- **Price-pressure signal (order-flow imbalance):** does short-term predictive skew beat stale-guard alone?
- **Second venue (Kraken):** do conclusions generalize across feed designs (L2 + checksum)?
- **Live paper trading:** only after replay results are stable. Question: does the sim's fill rate match reality?
- **CPU optimization beyond M7 / FPGA:** only if M7 shows parsing or book updates dominate tick-to-decision at a level that matters relative to network latency. FPGA would be a JSON-parse/book-update offload experiment with a clear baseline.

## 16. Open questions / risks
- Whether `full` needs auth on ws-feed (resolved by the M2 probe).
- Coinbase data redistribution terms (M8).
- Exact fee schedule from the official source (M6).
- Laptop clock quality (we log the NTP offset, and it may motivate a cloud recorder).

## 17. First concrete implementation steps
1. `git rm -r obj_dir rtl tb`; keep LICENSE, rewrite README; add `.gitignore` (`target/`, `data/`, `reports/*` except examples).
2. Create the workspace `Cargo.toml` with the crates in §4 as stubs, plus `apps/hft` with clap subcommands that print "not yet implemented".
3. `core`: Px/Qty fixed-point with parsing from decimal strings, Instrument, timestamp newtypes, `MdEvent`, `OrderReq`/`OrderEvt`. Unit tests.
4. Add GitHub Actions CI (fmt, clippy, test).
5. Copy this plan to `docs/PLAN.md` and add `docs/DECISIONS.md` (the §1 table).
6. Commit: "Scaffold Rust workspace; remove Verilator prototype". Then start M1 (fixture vertical slice).
