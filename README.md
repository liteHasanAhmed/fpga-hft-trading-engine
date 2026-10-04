# Market-making research engine (Rust)

An end-to-end trading research system: it records real Coinbase BTC-USD market data,
maintains a verified local order book, replays recordings deterministically, and runs a
paired experiment — a baseline market maker (**A**) versus the same market maker with
**stale-data protection** (**B**) — inside a self-built execution simulator, after fees and
under conservative fill assumptions.

Profitability is an experiment, not a promise. The engineering goal is a pipeline whose
every number can be reproduced and whose assumptions are written down.

## Quick start

```sh
cargo build --release
cargo test --workspace

# Fixture demo: A/B report with an injected outage (seconds, no network)
./target/release/hft run --config experiments/demo.toml --data fixtures/stale.log --out reports/demo

# Live: record Coinbase Advanced Trade BTC-USD (public, no account needed)
./target/release/hft capture --out data/btc --duration-s 600
./target/release/hft inspect data/btc            # span, records, gaps
./target/release/hft verify data/btc             # replay == live (book-hash checkpoints)
./target/release/hft run --config experiments/stale_guard.toml --data data/btc --only natural,latency-25ms
./target/release/hft bench data/btc --repeat 20  # tick-to-decision latency
```

Run the recorder 24/7 on a spare laptop: see [`ops/README.md`](ops/README.md).

## Architecture

```
            live (tokio, network only)                       replay (no async)
  ┌───────────────────────────────────┐            ┌───────────────────────────────┐
  │ WebSocket ──raw bytes+recv_ts──▶  │            │ segment files / text fixtures │
  │ bounded channel (drops counted)   │            └───────────────┬───────────────┘
  └───────────────┬───────────────────┘                            │
                  ▼                                                ▼
          recorder (raw, CRC)  ───────── same bytes ─────▶   Decoder (parse + sequence check)
                  │                                                │
                  ▼                                                ▼
           Decoder ──▶ Feed/Book ──checkpoint hashes──▶   Feed/Book (identical decisions)
                                                                   │
                                     experiment runner: one deterministic scheduler
                                     ┌─────────────────────────────┴──────────────┐
                                     ▼                                            ▼
                      exchange-side book @ engine time         strategy-visible book @ recv time
                      simulator: accept / fill / cancel  ◀──▶  strategy A or B (+ injected faults)
                                     └──────────────▶ report (both fee tiers, markouts, CIs)
```

| Crate | Responsibility |
|---|---|
| `core` | Fixed-point `Px`/`Qty`/`Cash`, timestamps, market and order events |
| `coinbase` | Parsers (Advanced Trade L2; Exchange L3), REST snapshot, live WebSocket |
| `book` | Order book: L3 (per order) or L2 (per level), invariants, state hash |
| `feed` | Sync state machine: snapshot, gaps, duplicates, heartbeat gap detection |
| `recorder` | Segmented append-only log, per-record CRC, crash recovery, explicit gaps |
| `replay` | Venue-aware decoder, fixtures, deterministic scheduler, live/REST verification |
| `sim` | Order lifecycle, fill models, fees, accounting, liquidation value |
| `strategy` | Baseline market maker; stale-data guard |
| `experiment` | Paired A/B runner, two market views, fault injection, splits |
| `report` | Metrics, paired bootstrap, Markdown/JSON/CSV |
| `bench` | Tick-to-decision harness, allocation counter, criterion micro-benchmarks |
| `apps/hft` | CLI: `probe`, `capture`, `inspect`, `verify`, `run`, `bench` |

## What is verified

* **Book correctness:** hand-worked L3 and L2 examples; property tests (random operation
  streams never break aggregate consistency); live REST L3 snapshot cross-check (Exchange venue).
* **Replay = live:** the capture records book-hash checkpoints; `hft verify` replays and must match
  every one. Example on live data: `live checkpoints: 2 ok, 0 mismatched … VERIFY OK`.
* **Crash recovery:** torn tails and corrupted payloads are detected (CRC), truncated, and marked as gaps.
* **Determinism:** reruns produce identical digests (CI diffs two full experiment runs).
* **Order lifecycle:** cancel/fill races, partial fills, post-only rejects, trade-before-arrival.
* **Accounting:** a three-fill example hand-calculated to 1e-10 USD, plus liquidation value.
* **Information visibility:** the strategy only sees delayed data; batched trades are applied
  at engine time on the exchange side (tested on an Advanced Trade fixture).

## First measurements (Apple M5, release build)

* Tick-to-decision on recorded Coinbase BTC-USD L2 data, per frame:
  **p50 542 ns, p99 5.3 µs, p99.9 30 µs**, ~9 allocations per frame (the next optimization target).
* Natural feed lag on Coinbase Advanced Trade level2: **p50 ≈ 200–300 ms, p99 ≈ 1 s** after engine
  time (capture clock within ±3 ms of NTP) — large enough that "stale" must be judged per venue.

## Documents

* [`docs/PLAN.md`](docs/PLAN.md) — scope, milestones, verification and benchmark strategy
* [`docs/DECISIONS.md`](docs/DECISIONS.md) — accepted decisions and evidence (incl. the L3 → L2 change)
* [`docs/ASSUMPTIONS.md`](docs/ASSUMPTIONS.md) — timing, fill model, and limitations
* [`fixtures/gen.py`](fixtures/gen.py) — generates the hand-designed fixtures

## Limitations

Fill model v1 ignores queue position (optimistic at the touch; the `trade_through` variant
is the conservative bound), our orders have no market impact, and L2 data cannot reveal
individual orders. Results are conditional on these assumptions. See `docs/ASSUMPTIONS.md`.

## License

MIT
