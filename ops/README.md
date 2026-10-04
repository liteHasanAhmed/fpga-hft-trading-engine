# Running the recorder 24/7 on an old laptop

The recorder is designed to survive what laptops do: sleep, Wi-Fi drops, crashes,
power loss. Every one of those becomes an **explicit gap** in the recording, never
silent missing data.

| Event | What happens |
|---|---|
| Wi-Fi drop / server disconnect | `ConnClose` record, reconnect with backoff, fresh snapshot |
| Venue drops a frame (sequence gap) | Replay marks the gap; capture resubscribes for a new snapshot |
| Recorder can't keep up | Frames counted as dropped, `Gap` record, resync |
| Crash / kill -9 / power loss | On restart the torn tail is truncated (CRC) and a `Gap` record starts the new session |

## Setup

1. `cargo build --release -p hft`
2. Check the clock is synchronized: `sntp time.apple.com` (macOS) or `timedatectl` (Linux).
   Feed-lag measurements depend on it. The recorder logs lag percentiles every 10 s.
3. Install the service:
   - macOS: edit `REPO` in `ops/com.hft.capture.plist`, copy to `~/Library/LaunchAgents/`,
     `launchctl load ~/Library/LaunchAgents/com.hft.capture.plist`. `caffeinate` keeps it awake;
     also set *Energy → Prevent automatic sleeping when the display is off* and keep it plugged in.
   - Linux: `ops/hft-capture.service` (instructions inside).
4. Watch it: `tail -f data/capture.log`, and periodically `hft inspect data/coinbase-advanced-btc-usd`.

## Disk use

Coinbase Advanced Trade BTC-USD level2 + trades + heartbeats is roughly 5 MB/minute
of raw JSON in a busy period (measured: 5.3 MB in 60 s), i.e. up to ~7 GB/day. Segments rotate hourly.
Compress closed segments if space is tight (`zstd data/.../*.seg` — the reader does not yet
read `.zst`; decompress before replay).

## Privacy

Recordings never contain the machine's hostname. `HFT_HOST_LABEL` is the only host
description stored (default `unspecified`).
