# Assumptions and limitations

Every experiment report repeats the essentials of this page.

## Market data
* **Coinbase Advanced Trade level2** is aggregated by price level. It cannot tell us how many
  orders form a level, their time priority, or whether a decrease was a cancel or a fill.
* `sequence_num` is per connection, across channels. A jump means frames were lost; the book
  is invalid until a resubscribe delivers a new snapshot.
* Level updates carry engine `event_time`; trades carry engine `time` and are delivered in
  250 ms batches; heartbeats carry server send time.
* Recordings store raw bytes. Normalization is re-derived at replay by the same decoder the live
  capture used, so live and replay make identical decisions (proved by recorded book-hash checkpoints).

## Time
* Simulation time is on the exchange (engine) time axis.
* Strategy-visible time of a message = `recv_ts − offset (+ base latency + injected faults)`,
  where `offset = min(recv_ts − exch_ts)` over the run. Assumes the capture clock is
  NTP-disciplined and drifts negligibly within a run (`sntp` showed ±3 ms).
* Equal-time ordering: exchange market data → order/cancel arrival → injected snapshot →
  strategy-visible data → responses → timers. So an order arriving at the same instant as a
  trade does **not** receive that trade.
* Exchange-side events are applied in true engine-time order within `reorder_window_ms`
  (default 2 s); trades later than that are counted as `late_trades` and applied on arrival.

## Execution (simulator v1)
* Post-only limit orders. Rejected if they would cross the exchange-side book at arrival,
  or if the exchange-side book is unknown (recording gap).
* **Fill rule:** a resting order fills when a real trade prints at (`trade_touch`) or strictly
  through (`trade_through`) its price, on its side, after the order arrived. Fill size =
  min(remaining, trade size), at our limit price, as maker.
* **Touch ≠ fill:** ignoring queue position makes `trade_touch` optimistic at the touch;
  `trade_through` is the conservative bound. Results must be read across both.
* **No market impact:** our orders never change the recorded market, and a real trade's volume
  is not reduced by our fill.
* **No double counting:** trades and book changes are separate inputs; fills come only from trades.
* Latencies (order, cancel, response) default to 0 and are swept as scenarios.
* Fees never affect decisions, so net P&L per fee tier is computed from one set of fills.
* End-of-run valuation: mark-to-mid, and liquidation by walking the book with the taker fee.

## Strategy
* Both arms: cancel all when the book is invalid or no data has arrived for 5 s (watchdog);
  inventory limit; never quote through the visible book; reject cooldown.
* Arm B adds only the stale-data guard (lag vs. adaptive baseline; heartbeat silence; hysteresis).

## Not modeled (yet)
Queue position, hidden/iceberg liquidity, self-trade prevention, rate limits on our orders,
auction/halt states, our own market impact.
