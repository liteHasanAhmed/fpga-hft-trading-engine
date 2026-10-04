//! Live connectivity (capture only).
//!
//! Network I/O runs on tokio. Raw frames are timestamped the moment they are read
//! and handed to the single-threaded core loop through a bounded channel. If the
//! core falls behind and the channel fills, frames are **dropped and counted**,
//! and an [`LiveEvent::Overflow`] is delivered once space frees up — the core
//! treats that as a gap and resynchronizes. Nothing is dropped silently.

use futures_util::{SinkExt, StreamExt};
use hft_core::Ts;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug)]
pub enum LiveEvent {
    Connected {
        recv_ns: i64,
        url: String,
    },
    Frame {
        recv_ns: i64,
        data: Vec<u8>,
    },
    Disconnected {
        recv_ns: i64,
        reason: String,
    },
    /// `dropped` frames were lost because the core was not keeping up.
    Overflow {
        recv_ns: i64,
        dropped: u64,
    },
    Snapshot {
        recv_ns: i64,
        body: Vec<u8>,
        verify: bool,
    },
    SnapshotFailed {
        reason: String,
        verify: bool,
    },
}

pub struct WsConfig {
    pub url: String,
    /// Sent in order right after connecting.
    pub subscribe: Vec<String>,
    /// Reconnect if no frame arrives for this long (heartbeats come every ~1 s).
    pub read_timeout: Duration,
}

/// Connect, subscribe, forward frames; reconnect forever with jittered backoff.
/// Notifying `reconnect` drops the current session (used to resubscribe after a
/// sequence gap on venues whose book can only be recovered that way).
pub async fn run_ws(cfg: WsConfig, tx: mpsc::Sender<LiveEvent>, reconnect: std::sync::Arc<tokio::sync::Notify>) {
    let mut backoff = Duration::from_millis(500);
    let mut dropped: u64 = 0;
    loop {
        let started = tokio::time::Instant::now();
        let res = tokio::select! {
            r = session(&cfg, &tx, &mut dropped) => r,
            _ = reconnect.notified() => Err(anyhow::anyhow!("reconnect requested")),
        };
        if started.elapsed() > Duration::from_secs(60) {
            backoff = Duration::from_millis(500);
        }
        let reason = match res {
            Ok(()) => "closed by server".to_string(),
            Err(e) => format!("{e:#}"),
        };
        tracing::warn!(%reason, "websocket disconnected");
        if tx.send(LiveEvent::Disconnected { recv_ns: Ts::wall_now().0, reason }).await.is_err() {
            return; // core shut down
        }
        let jitter = Duration::from_millis((Ts::wall_now().0 as u64 / 1000) % 250);
        tokio::time::sleep(backoff + jitter).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

async fn session(cfg: &WsConfig, tx: &mpsc::Sender<LiveEvent>, dropped: &mut u64) -> anyhow::Result<()> {
    let (mut ws, _) = tokio_tungstenite::connect_async(cfg.url.as_str()).await?;
    for m in &cfg.subscribe {
        ws.send(Message::text(m.clone())).await?;
    }
    tx.send(LiveEvent::Connected { recv_ns: Ts::wall_now().0, url: cfg.url.clone() }).await?;
    loop {
        let next = tokio::time::timeout(cfg.read_timeout, ws.next()).await;
        let msg = match next {
            Err(_) => anyhow::bail!("read timeout ({:?} without a frame)", cfg.read_timeout),
            Ok(None) => return Ok(()),
            Ok(Some(m)) => m?,
        };
        let recv_ns = Ts::wall_now().0;
        let data = match msg {
            Message::Text(t) => t.as_bytes().to_vec(),
            Message::Binary(b) => b.to_vec(),
            Message::Close(c) => anyhow::bail!("close frame: {c:?}"),
            _ => continue, // ping/pong handled by tungstenite
        };
        if *dropped > 0 {
            match tx.try_send(LiveEvent::Overflow { recv_ns, dropped: *dropped }) {
                Ok(()) => *dropped = 0,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    *dropped += 1;
                    continue;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => return Ok(()),
            }
        }
        match tx.try_send(LiveEvent::Frame { recv_ns, data }) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => *dropped += 1,
            Err(mpsc::error::TrySendError::Closed(_)) => return Ok(()),
        }
    }
}

/// Fetch the REST level-3 book. Returns the raw body for recording.
pub async fn fetch_l3(client: &reqwest::Client, rest_url: &str, product: &str) -> anyhow::Result<(i64, Vec<u8>)> {
    let url = format!("{rest_url}/products/{product}/book?level=3");
    let resp = client.get(&url).send().await?;
    let status = resp.status();
    let body = resp.bytes().await?;
    let recv_ns = Ts::wall_now().0;
    anyhow::ensure!(
        status.is_success(),
        "GET {url}: {status}: {}",
        String::from_utf8_lossy(&body[..body.len().min(300)])
    );
    Ok((recv_ns, body.to_vec()))
}

pub fn http_client() -> anyhow::Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .user_agent(concat!("hft-research/", env!("CARGO_PKG_VERSION")))
        .timeout(Duration::from_secs(30))
        .build()?)
}
