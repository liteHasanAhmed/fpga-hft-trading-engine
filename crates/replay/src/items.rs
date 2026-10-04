use anyhow::{Context, bail};
use hft_coinbase::advanced::{self, AtParser, FrameInfo, SeqTracker};
use hft_coinbase::{Parsed, Parser, snapshot::parse_snapshot};
use hft_core::{Instrument, L3Snapshot, MdEvent, MdMsg, Ts};
use hft_recorder::{Checkpoint, Kind, Reader};
use std::collections::VecDeque;
use std::io::BufRead;
use std::path::Path;
use std::sync::Arc;

/// A decoded replay item. `recv_ts` is the local receive time recorded at capture.
#[derive(Clone, Debug, PartialEq)]
pub enum Item {
    Md {
        recv_ts: Ts,
        msg: MdMsg,
    },
    Connect {
        recv_ts: Ts,
    },
    /// Connection lost, frames dropped, or recording hole: the book must be resynchronized.
    Disconnect {
        recv_ts: Ts,
        reason: String,
    },
    VerifySnapshot {
        recv_ts: Ts,
        snap: Arc<L3Snapshot>,
    },
    Checkpoint {
        recv_ts: Ts,
        cp: Checkpoint,
    },
    ExchangeError {
        recv_ts: Ts,
        msg: String,
    },
}

impl Item {
    pub fn recv_ts(&self) -> Ts {
        match self {
            Item::Md { recv_ts, .. }
            | Item::Connect { recv_ts }
            | Item::Disconnect { recv_ts, .. }
            | Item::VerifySnapshot { recv_ts, .. }
            | Item::Checkpoint { recv_ts, .. }
            | Item::ExchangeError { recv_ts, .. } => *recv_ts,
        }
    }
}

/// Pull-based stream of raw records → items.
pub trait ItemStream {
    fn next_item(&mut self) -> anyhow::Result<Option<Item>>;
    fn instrument(&self) -> &Instrument;
}

/// Which wire format a recording holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Venue {
    /// Coinbase Exchange `full` channel (L3, per-product sequence).
    CoinbaseExchange,
    /// Coinbase Advanced Trade `level2` + `market_trades` + `heartbeats` (L2, per-connection sequence).
    CoinbaseAdvanced,
}

impl Venue {
    pub fn from_name(s: &str) -> Option<Venue> {
        match s {
            "coinbase" | "coinbase-exchange" => Some(Venue::CoinbaseExchange),
            advanced::VENUE => Some(Venue::CoinbaseAdvanced),
            _ => None,
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Venue::CoinbaseExchange => "coinbase-exchange",
            Venue::CoinbaseAdvanced => advanced::VENUE,
        }
    }
}

/// Raw record → items. Holds the per-connection state that decoding needs
/// (Advanced Trade sequence continuity), so live capture and replay agree.
pub struct Decoder {
    venue: Venue,
    l3: Parser,
    at: AtParser,
    seq: SeqTracker,
    scratch: Vec<MdMsg>,
}

impl Decoder {
    pub fn new(venue: Venue, inst: Instrument) -> Decoder {
        Decoder {
            venue,
            l3: Parser::new(inst.clone()),
            at: AtParser::new(inst),
            seq: SeqTracker::default(),
            scratch: vec![],
        }
    }

    pub fn venue(&self) -> Venue {
        self.venue
    }

    pub fn instrument(&self) -> &Instrument {
        self.l3.instrument()
    }

    pub fn decode(&mut self, kind: Kind, recv_ns: i64, payload: &[u8], out: &mut VecDeque<Item>) -> anyhow::Result<()> {
        let recv_ts = Ts(recv_ns);
        let unparseable = |e: &dyn std::fmt::Display| Item::ExchangeError {
            recv_ts,
            msg: format!("unparseable frame: {e}: {}", String::from_utf8_lossy(&payload[..payload.len().min(200)])),
        };
        match kind {
            // Unparseable frames are kept in the recording and surfaced, not fatal —
            // exactly how the live capture treats them.
            Kind::WsFrame => match self.venue {
                Venue::CoinbaseExchange => match self.l3.parse(payload) {
                    Ok(Parsed::Md(msg)) => out.push_back(Item::Md { recv_ts, msg }),
                    Ok(Parsed::Error(msg)) => out.push_back(Item::ExchangeError { recv_ts, msg }),
                    Ok(Parsed::Subscribed | Parsed::Ignored) => {}
                    Err(e) => out.push_back(unparseable(&e)),
                },
                Venue::CoinbaseAdvanced => {
                    self.scratch.clear();
                    match self.at.parse(payload, &mut self.scratch) {
                        Ok(FrameInfo::Data { seq }) => {
                            if let Some((expected, got)) = self.seq.check(seq) {
                                out.push_back(Item::Disconnect {
                                    recv_ts,
                                    reason: format!("sequence gap: expected {expected}, got {got}"),
                                });
                            }
                            out.extend(self.scratch.drain(..).map(|msg| Item::Md { recv_ts, msg }));
                        }
                        Ok(FrameInfo::Error(msg)) => out.push_back(Item::ExchangeError { recv_ts, msg }),
                        Ok(FrameInfo::Unknown) => {}
                        Err(e) => out.push_back(unparseable(&e)),
                    }
                }
            },
            Kind::Snapshot => {
                let s = parse_snapshot(self.instrument(), payload)?;
                // A snapshot's exchange time is unknown (and older than recv); ZERO keeps
                // it from advancing any exchange clock.
                out.push_back(Item::Md {
                    recv_ts,
                    msg: MdMsg { seq: s.seq, exch_ts: Ts::ZERO, event: MdEvent::Snapshot(Arc::new(s)) },
                });
            }
            Kind::VerifySnapshot => out.push_back(Item::VerifySnapshot {
                recv_ts,
                snap: Arc::new(parse_snapshot(self.instrument(), payload)?),
            }),
            Kind::ConnOpen => {
                self.seq.reset();
                out.push_back(Item::Connect { recv_ts });
            }
            Kind::ConnClose | Kind::Gap => {
                self.seq.reset();
                out.push_back(Item::Disconnect { recv_ts, reason: String::from_utf8_lossy(payload).into_owned() });
            }
            Kind::Checkpoint => out.push_back(Item::Checkpoint { recv_ts, cp: serde_json::from_slice(payload)? }),
            Kind::Note => {}
        }
        Ok(())
    }
}

/// Binary recording directory. The venue comes from the segment header.
pub struct RecordingSource {
    reader: Reader,
    dec: Decoder,
    queue: VecDeque<Item>,
}

impl RecordingSource {
    pub fn open(dir: &Path, inst: Instrument) -> anyhow::Result<RecordingSource> {
        let (venue, product) = recording_meta(dir)?;
        let inst = Instrument { symbol: product, ..inst };
        Ok(RecordingSource { reader: Reader::open(dir)?, dec: Decoder::new(venue, inst), queue: VecDeque::new() })
    }
    pub fn torn_segments(&self) -> &[(std::path::PathBuf, &'static str)] {
        &self.reader.torn
    }
}

impl ItemStream for RecordingSource {
    fn next_item(&mut self) -> anyhow::Result<Option<Item>> {
        loop {
            if let Some(i) = self.queue.pop_front() {
                return Ok(Some(i));
            }
            let Some(r) = self.reader.next_record()? else { return Ok(None) };
            self.dec.decode(r.kind, r.recv_ns, r.payload, &mut self.queue)?;
        }
    }
    fn instrument(&self) -> &Instrument {
        self.dec.instrument()
    }
}

/// Hand-written text fixture. One record per line:
///
/// ```text
/// # venue: coinbase-advanced        (optional header; default coinbase-exchange)
/// # comment
/// <recv_ns> <kind> <payload...>
/// ```
/// `kind` ∈ `ws`, `snapshot`, `verify`, `open`, `close`, `gap`, `checkpoint`, `note`.
/// Payload is the rest of the line (raw JSON for `ws`/`snapshot`).
pub struct FixtureSource {
    pub(crate) lines: Vec<(Kind, i64, String)>,
    pos: usize,
    dec: Decoder,
    queue: VecDeque<Item>,
}

impl FixtureSource {
    pub fn from_reader(r: impl BufRead, inst: Instrument) -> anyhow::Result<FixtureSource> {
        let mut lines = vec![];
        let mut venue = Venue::CoinbaseExchange;
        for (n, line) in r.lines().enumerate() {
            let line = line?;
            let t = line.trim();
            if let Some(v) = t.strip_prefix("# venue:") {
                venue = Venue::from_name(v.trim()).with_context(|| format!("line {}: unknown venue", n + 1))?;
                continue;
            }
            if t.is_empty() || t.starts_with('#') {
                continue;
            }
            let mut parts = t.splitn(3, char::is_whitespace);
            let recv: i64 = parts.next().unwrap().parse().with_context(|| format!("line {}: recv_ns", n + 1))?;
            let kind = match parts.next() {
                Some("ws") => Kind::WsFrame,
                Some("snapshot") => Kind::Snapshot,
                Some("verify") => Kind::VerifySnapshot,
                Some("open") => Kind::ConnOpen,
                Some("close") => Kind::ConnClose,
                Some("gap") => Kind::Gap,
                Some("checkpoint") => Kind::Checkpoint,
                Some("note") => Kind::Note,
                k => bail!("line {}: unknown kind {k:?}", n + 1),
            };
            lines.push((kind, recv, parts.next().unwrap_or("").trim().to_string()));
        }
        Ok(FixtureSource { lines, pos: 0, dec: Decoder::new(venue, inst), queue: VecDeque::new() })
    }

    pub fn open(path: &Path, inst: Instrument) -> anyhow::Result<FixtureSource> {
        let f = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
        Self::from_reader(std::io::BufReader::new(f), inst)
    }

    pub fn venue(&self) -> Venue {
        self.dec.venue()
    }
}

impl ItemStream for FixtureSource {
    fn next_item(&mut self) -> anyhow::Result<Option<Item>> {
        loop {
            if let Some(i) = self.queue.pop_front() {
                return Ok(Some(i));
            }
            let Some((k, recv, p)) = self.lines.get(self.pos) else { return Ok(None) };
            self.pos += 1;
            self.dec.decode(*k, *recv, p.as_bytes(), &mut self.queue)?;
        }
    }
    fn instrument(&self) -> &Instrument {
        self.dec.instrument()
    }
}

/// Open a recording directory or a fixture file.
pub fn open_source(path: &Path, inst: Instrument) -> anyhow::Result<Box<dyn ItemStream>> {
    if path.is_dir() {
        Ok(Box::new(RecordingSource::open(path, inst)?))
    } else {
        Ok(Box::new(FixtureSource::open(path, inst)?))
    }
}

/// (venue, product) from the first segment header of a recording.
fn recording_meta(dir: &Path) -> anyhow::Result<(Venue, String)> {
    let mut r = Reader::open(dir)?;
    let _ = r.next_record()?;
    let h = r.headers.first().with_context(|| format!("no readable segment header in {}", dir.display()))?;
    Ok((Venue::from_name(&h.venue).with_context(|| format!("unknown venue {}", h.venue))?, h.product.clone()))
}

/// Venue of a recording directory or fixture.
pub fn detect_venue(path: &Path) -> anyhow::Result<Venue> {
    if path.is_dir() {
        Ok(recording_meta(path)?.0)
    } else {
        Ok(FixtureSource::open(path, Instrument::btc_usd())?.venue())
    }
}

/// Load every raw record (kind, recv_ns, payload) into memory. Used by benchmarks
/// so that measured work includes parsing but excludes disk I/O.
pub fn load_raw_records(path: &Path) -> anyhow::Result<Vec<(Kind, i64, Vec<u8>)>> {
    if path.is_dir() {
        let mut r = Reader::open(path)?;
        let mut v = vec![];
        while let Some(rec) = r.next_record()? {
            v.push((rec.kind, rec.recv_ns, rec.payload.to_vec()));
        }
        Ok(v)
    } else {
        let f = FixtureSource::open(path, Instrument::btc_usd())?;
        Ok(f.lines.into_iter().map(|(k, t, p)| (k, t, p.into_bytes())).collect())
    }
}
