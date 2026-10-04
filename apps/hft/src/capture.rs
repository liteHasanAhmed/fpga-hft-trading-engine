//! Live capture: record every raw frame while maintaining and verifying a live book.
//!
//! Core loop (single thread) per event:
//! 1. write the raw bytes to the recorder (always first — the recording is the truth),
//! 2. decode with the *same* [`Decoder`] replay uses, and apply to the feed exactly as
//!    replay does — so a replay reproduces every live decision,
//! 3. request a REST snapshot if the feed needs one (with backoff),
//! 4. every `checkpoint_every` applied messages, record `{seq, book hash}` so replay
//!    can prove it rebuilt the identical book,
//! 5. every `verify_every_min`, fetch a REST snapshot purely for verification.

use hft_coinbase::live::{LiveEvent, WsConfig, fetch_l3, http_client, run_ws};
use hft_coinbase::snapshot::parse_snapshot;
use hft_coinbase::{Parsed, Parser, advanced, auth::Credentials};
use hft_core::{Instrument, Ts};
use hft_feed::{Feed, FeedEvent};
use hft_recorder::{Checkpoint, Kind, Recorder, RecorderConfig};
use hft_replay::{Decoder, Item, SnapOutcome, SnapshotVerifier, Venue};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Notify, mpsc};

#[derive(clap::Args)]
pub struct CaptureArgs {
    /// Output recording directory (created; appended to if it exists).
    #[arg(long)]
    out: PathBuf,
    #[arg(long, default_value = "BTC-USD")]
    product: String,
    /// `coinbase-advanced` (public L2, default) or `coinbase-exchange` (L3, needs an Exchange API key).
    #[arg(long, default_value = advanced::VENUE)]
    venue: String,
    /// WebSocket URL (default depends on venue).
    #[arg(long)]
    ws_url: Option<String>,
    #[arg(long, default_value = hft_coinbase::REST_URL)]
    rest_url: String,
    /// Record a book-hash checkpoint every N applied messages.
    #[arg(long, default_value_t = 50_000)]
    checkpoint_every: u64,
    /// Fetch a REST L3 snapshot for verification every N minutes (0 = never; L3 venue only).
    #[arg(long, default_value_t = 30)]
    verify_every_min: u64,
    /// Stop after this many seconds (default: run until Ctrl-C).
    #[arg(long)]
    duration_s: Option<u64>,
    /// Sign the subscription with COINBASE_API_KEY / _SECRET / _PASSPHRASE.
    #[arg(long)]
    auth: bool,
}

fn creds(auth: bool) -> anyhow::Result<Option<Credentials>> {
    if !auth {
        return Ok(None);
    }
    Credentials::from_env()
        .map(Some)
        .ok_or_else(|| anyhow::anyhow!("--auth needs COINBASE_API_KEY, COINBASE_API_SECRET, COINBASE_API_PASSPHRASE"))
}

/// Answer "can we get L3 data?" with a short live session.
pub fn probe(product: &str, n: usize) -> anyhow::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let (tx, mut rx) = mpsc::channel(1024);
        let creds = Credentials::from_env();
        println!("connecting to {} (auth: {})", hft_coinbase::WS_URL, creds.is_some());
        let cfg = WsConfig {
            url: hft_coinbase::WS_URL.into(),
            subscribe: vec![hft_coinbase::auth::subscribe_msg(product, creds.as_ref())?],
            read_timeout: Duration::from_secs(10),
        };
        let task = tokio::spawn(run_ws(cfg, tx, Default::default()));
        let parser = Parser::new(Instrument { symbol: product.into(), ..Instrument::btc_usd() });
        let (mut seen, mut l3) = (0, 0);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        while seen < n {
            let ev = match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(ev)) => ev,
                _ => break,
            };
            match ev {
                LiveEvent::Frame { data, recv_ns } => {
                    let parsed = parser.parse(&data);
                    let s = String::from_utf8_lossy(&data);
                    println!("{} {}", recv_ns, &s[..s.len().min(220)]);
                    match parsed {
                        Ok(Parsed::Md(m)) if m.event.is_sequenced() => l3 += 1,
                        Ok(Parsed::Error(e)) => {
                            println!("\nEXCHANGE ERROR: {e}");
                            println!("If this says authentication is required, set COINBASE_API_KEY/SECRET/PASSPHRASE and use --auth.");
                            break;
                        }
                        Err(e) => println!("  parse error: {e}"),
                        _ => {}
                    }
                    seen += 1;
                }
                other => println!("{other:?}"),
            }
        }
        task.abort();
        println!(
            "\nresult: {}",
            if l3 > 0 { "L3 `full` channel is accessible with this configuration" } else { "no L3 messages received" }
        );
        let client = http_client()?;
        match fetch_l3(&client, hft_coinbase::REST_URL, product).await {
            Ok((_, body)) => {
                let s = parse_snapshot(parser.instrument(), &body)?;
                println!("REST L3 snapshot OK: seq {}, {} bids, {} asks, {:.1} MB", s.seq, s.bids.len(), s.asks.len(), body.len() as f64 / 1e6);
            }
            Err(e) => println!("REST L3 snapshot failed: {e:#}"),
        }
        Ok(())
    })
}

pub fn capture(a: CaptureArgs) -> anyhow::Result<()> {
    let venue = Venue::from_name(&a.venue).ok_or_else(|| anyhow::anyhow!("unknown venue {}", a.venue))?;
    let inst = Instrument { symbol: a.product.clone(), ..Instrument::btc_usd() };
    let (ws_url, subscribe) = match venue {
        Venue::CoinbaseAdvanced => (advanced::WS_URL.to_string(), advanced::subscribe_msgs(&a.product)),
        Venue::CoinbaseExchange => (
            hft_coinbase::WS_URL.to_string(),
            vec![hft_coinbase::auth::subscribe_msg(&a.product, creds(a.auth)?.as_ref())?],
        ),
    };
    let ws_url = a.ws_url.clone().unwrap_or(ws_url);
    let l3 = venue == Venue::CoinbaseExchange;

    let mut rcfg = RecorderConfig::new(venue.name(), &a.product);
    rcfg.git_rev = crate::GIT_REV.into();
    let mut rec = Recorder::open(&a.out, rcfg)?;
    rec.write(
        Kind::Note,
        Ts::wall_now().0,
        serde_json::json!({
            "event": "session_start", "venue": venue.name(),
            "ws_url": ws_url, "rest_url": a.rest_url, "product": a.product,
            "checkpoint_every": a.checkpoint_every, "verify_every_min": a.verify_every_min,
            "auth": a.auth, "git_rev": crate::GIT_REV,
        })
        .to_string()
        .as_bytes(),
    )?;
    tracing::info!(out = %a.out.display(), venue = venue.name(), url = %ws_url, "recording");

    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build()?;
    let (tx, mut rx) = mpsc::channel::<LiveEvent>(1 << 16);
    let (snap_tx, mut snap_rx) = mpsc::unbounded_channel::<bool>();
    let reconnect = Arc::new(Notify::new());
    let ws_cfg = WsConfig { url: ws_url, subscribe, read_timeout: Duration::from_secs(10) };
    rt.spawn(run_ws(ws_cfg, tx.clone(), reconnect.clone()));
    {
        // Snapshot worker (L3 venue): one request at a time.
        let tx = tx.clone();
        let (rest, product) = (a.rest_url.clone(), a.product.clone());
        rt.spawn(async move {
            let client = match http_client() {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!("http client: {e:#}");
                    return;
                }
            };
            while let Some(verify) = snap_rx.recv().await {
                let ev = match fetch_l3(&client, &rest, &product).await {
                    Ok((recv_ns, body)) => LiveEvent::Snapshot { recv_ns, body, verify },
                    Err(e) => LiveEvent::SnapshotFailed { reason: format!("{e:#}"), verify },
                };
                if tx.send(ev).await.is_err() {
                    return;
                }
            }
        });
    }

    let mut dec = Decoder::new(venue, inst.clone());
    let mut items: VecDeque<Item> = VecDeque::new();
    let mut feed = Feed::new(1 << 20);
    let mut verifier = SnapshotVerifier::new(1 << 19);
    let mut fev = Vec::with_capacity(64);
    let mut outcomes = vec![];

    let mut snapshot_inflight = false;
    let mut snapshot_backoff = Duration::from_secs(1);
    let mut next_snapshot_at = Instant::now();
    let mut saw_data_since_reset = false;
    let mut applied_since_cp = 0u64;
    let verify_iv = (l3 && a.verify_every_min > 0).then(|| Duration::from_secs(a.verify_every_min * 60));
    let mut next_verify = Instant::now() + verify_iv.unwrap_or(Duration::from_secs(1 << 40));
    let started = Instant::now();
    let stop_at = a.duration_s.map(|s| started + Duration::from_secs(s));

    // Rolling stats for the status line.
    let mut stat_t = Instant::now();
    let (mut n_frames, mut n_md, mut n_trades) = (0u64, 0u64, 0u64);
    let mut lag = hdrhistogram::Histogram::<u64>::new_with_bounds(1, 60_000_000, 2)?;
    let (mut verify_ok, mut verify_bad, mut gaps) = (0u64, 0u64, 0u64);

    let res: anyhow::Result<()> = rt.block_on(async {
        let mut tick = tokio::time::interval(Duration::from_millis(250));
        let ctrl_c = tokio::signal::ctrl_c();
        tokio::pin!(ctrl_c);
        loop {
            let ev = tokio::select! {
                ev = rx.recv() => match ev { Some(ev) => Some(ev), None => break },
                _ = tick.tick() => None,
                _ = &mut ctrl_c => { tracing::info!("ctrl-c: stopping"); break }
            };
            // 1. Record raw, 2. decode exactly as replay will.
            if let Some(ev) = ev {
                let (kind, recv_ns, bytes): (Kind, i64, std::borrow::Cow<'_, [u8]>) = match &ev {
                    LiveEvent::Connected { recv_ns, url } => (Kind::ConnOpen, *recv_ns, url.as_bytes().into()),
                    LiveEvent::Disconnected { recv_ns, reason } => {
                        tracing::warn!(%reason, "disconnected");
                        (Kind::ConnClose, *recv_ns, reason.as_bytes().into())
                    }
                    LiveEvent::Overflow { recv_ns, dropped } => {
                        let why = format!("core overflow: dropped {dropped} frames");
                        tracing::warn!("{why}");
                        (Kind::Gap, *recv_ns, why.into_bytes().into())
                    }
                    LiveEvent::Frame { recv_ns, data } => (Kind::WsFrame, *recv_ns, data.as_slice().into()),
                    LiveEvent::Snapshot { recv_ns, body, verify } => {
                        (if *verify { Kind::VerifySnapshot } else { Kind::Snapshot }, *recv_ns, body.as_slice().into())
                    }
                    LiveEvent::SnapshotFailed { reason, verify } => {
                        tracing::warn!(%reason, verify, "snapshot fetch failed");
                        if !verify {
                            snapshot_inflight = false;
                            next_snapshot_at = Instant::now() + snapshot_backoff;
                            snapshot_backoff = (snapshot_backoff * 2).min(Duration::from_secs(60));
                        }
                        continue;
                    }
                };
                rec.write(kind, recv_ns, &bytes)?;
                if kind == Kind::WsFrame {
                    n_frames += 1;
                }
                if kind == Kind::Snapshot {
                    snapshot_inflight = false;
                }
                dec.decode(kind, recv_ns, &bytes, &mut items)?;
            }

            // 3. Apply decoded items (same logic as `hft_replay::verify_stream`).
            let mut checkpoint_due = false;
            while let Some(item) = items.pop_front() {
                match item {
                    Item::Md { recv_ts, msg } => {
                        n_md += 1;
                        saw_data_since_reset = true;
                        if msg.event.is_timely() && msg.exch_ts > Ts::ZERO {
                            lag.saturating_record(((recv_ts.0 - msg.exch_ts.0) / 1000).max(1) as u64);
                        }
                        if matches!(msg.event, hft_core::MdEvent::TradePrint { .. } | hft_core::MdEvent::Match { .. }) {
                            n_trades += 1;
                        }
                        fev.clear();
                        feed.on_msg(&msg, &mut fev);
                        verifier.after_msg(&feed, &msg, &fev, &mut outcomes);
                        for e in &fev {
                            match e {
                                FeedEvent::Invalid(r) => tracing::warn!(?r, "book invalid; resyncing"),
                                FeedEvent::BookError(e) => tracing::error!(%e, "book error"),
                                FeedEvent::Synced { seq } => {
                                    tracing::info!(seq, "book synchronized");
                                    snapshot_backoff = Duration::from_secs(1);
                                }
                                FeedEvent::SnapshotRejected { snap_seq, first_buffered } => {
                                    tracing::warn!(snap_seq, ?first_buffered, "snapshot did not connect; retrying");
                                    next_snapshot_at = Instant::now() + snapshot_backoff;
                                    snapshot_backoff = (snapshot_backoff * 2).min(Duration::from_secs(60));
                                }
                                _ => {}
                            }
                        }
                        if feed.is_live() && !matches!(msg.event, hft_core::MdEvent::Heartbeat { .. }) {
                            applied_since_cp += 1;
                            if applied_since_cp >= a.checkpoint_every {
                                applied_since_cp = 0;
                                checkpoint_due = true;
                            }
                        }
                    }
                    Item::Disconnect { reason, .. } => {
                        fev.clear();
                        feed.reset(&mut fev);
                        verifier.reset();
                        saw_data_since_reset = false;
                        if reason.starts_with("sequence gap") {
                            gaps += 1;
                            tracing::warn!(%reason, "frames dropped by the venue; resubscribing for a fresh snapshot");
                            reconnect.notify_one();
                        }
                    }
                    Item::VerifySnapshot { snap, .. } => verifier.submit(&feed, snap, &mut outcomes),
                    Item::ExchangeError { msg, .. } => {
                        tracing::error!("exchange: {msg}");
                        if msg.contains("authentication") {
                            anyhow::bail!("this feed requires authentication: use --auth with COINBASE_API_* set, or --venue coinbase-advanced");
                        }
                    }
                    Item::Connect { .. } => tracing::info!("connected"),
                    Item::Checkpoint { .. } => {}
                }
            }
            // Checkpoints go after the frame that completed them, as replay expects.
            if checkpoint_due {
                let cp = Checkpoint { seq: feed.last_seq(), hash: feed.book().state_hash() };
                rec.write(Kind::Checkpoint, Ts::wall_now().0, serde_json::to_string(&cp)?.as_bytes())?;
            }

            for o in outcomes.drain(..) {
                match &o {
                    SnapOutcome::Match { snap_seq, at_seq } => {
                        verify_ok += 1;
                        tracing::info!(snap_seq, at_seq, "REST verification: book matches");
                    }
                    SnapOutcome::Mismatch { .. } => {
                        verify_bad += 1;
                        tracing::error!(?o, "REST verification MISMATCH");
                    }
                    SnapOutcome::Unverifiable { .. } => tracing::warn!(?o, "REST verification skipped"),
                }
            }

            let now = Instant::now();
            if l3 && feed.needs_snapshot() && saw_data_since_reset && !snapshot_inflight && now >= next_snapshot_at {
                snapshot_inflight = true;
                let _ = snap_tx.send(false);
            }
            if let Some(iv) = verify_iv
                && now >= next_verify
                && feed.is_live()
            {
                next_verify = now + iv;
                let _ = snap_tx.send(true);
            }
            rec.maintain()?;
            if now.duration_since(stat_t) >= Duration::from_secs(10) {
                let dt = now.duration_since(stat_t).as_secs_f64();
                let (bid, ask) = (feed.book().best_bid(), feed.book().best_ask());
                tracing::info!(
                    "{:?} | {:.0} frames/s {:.0} msgs/s {:.1} trades/s | lag p50 {:.1} ms p99 {:.1} ms | gaps {} | verify {}/{} | bbo {} / {}",
                    feed.state(),
                    n_frames as f64 / dt,
                    n_md as f64 / dt,
                    n_trades as f64 / dt,
                    lag.value_at_quantile(0.5) as f64 / 1e3,
                    lag.value_at_quantile(0.99) as f64 / 1e3,
                    gaps + feed.stats().gaps,
                    verify_ok,
                    verify_ok + verify_bad,
                    bid.map_or("-".into(), |b| inst.fmt_px(b.0)),
                    ask.map_or("-".into(), |b| inst.fmt_px(b.0)),
                );
                stat_t = now;
                n_frames = 0;
                n_md = 0;
                n_trades = 0;
                lag.reset();
            }
            if stop_at.is_some_and(|t| now >= t) {
                tracing::info!("duration reached");
                break;
            }
        }
        Ok(())
    });
    rec.write(Kind::Note, Ts::wall_now().0, br#"{"event":"session_end"}"#)?;
    rec.sync()?;
    rt.shutdown_timeout(Duration::from_secs(2));
    res?;
    println!(
        "records written: {}  gaps: {}  snapshots applied: {}  verification: {verify_ok} ok / {verify_bad} mismatched",
        rec.records_written,
        gaps + feed.stats().gaps,
        feed.stats().snapshots_applied
    );
    Ok(())
}
