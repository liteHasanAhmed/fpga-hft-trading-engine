//! `hft` — capture, verify, replay and experiment CLI.

mod capture;
mod run;

use clap::{Parser, Subcommand};
use hft_core::Instrument;
use std::path::PathBuf;

#[global_allocator]
static ALLOC: hft_bench::alloc::Counting = hft_bench::alloc::Counting;

pub const GIT_REV: &str = env!("HFT_GIT_REV");

#[derive(Parser)]
#[command(name = "hft", version, about = "Market-data capture, replay and market-making experiments")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Connect to the live feed, print the first messages, and report whether L3 (`full`) is accessible.
    Probe {
        #[arg(long, default_value = "BTC-USD")]
        product: String,
        #[arg(long, default_value_t = 10)]
        messages: usize,
    },
    /// Record the live feed to a directory, maintaining and verifying a live book.
    Capture(capture::CaptureArgs),
    /// Summarize a recording: segments, time span, record counts, gaps, torn tails.
    Inspect { path: PathBuf },
    /// Replay a recording or fixture through the book; check live checkpoints and snapshots.
    Verify {
        path: PathBuf,
        /// Full invariant check every N messages (0 = off).
        #[arg(long, default_value_t = 10_000)]
        check_every: u64,
    },
    /// Run a paired A/B experiment and write a report.
    Run(run::RunArgs),
    /// Tick-to-decision latency and throughput over a recording or fixture.
    Bench {
        path: PathBuf,
        /// Repeat the input N times (useful for tiny fixtures).
        #[arg(long, default_value_t = 1)]
        repeat: usize,
        #[arg(long)]
        json: Option<PathBuf>,
    },
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_target(false)
        .init();
    match Cli::parse().cmd {
        Cmd::Probe { product, messages } => capture::probe(&product, messages),
        Cmd::Capture(a) => capture::capture(a),
        Cmd::Inspect { path } => inspect(&path),
        Cmd::Verify { path, check_every } => verify(&path, check_every),
        Cmd::Run(a) => run::run(a),
        Cmd::Bench { path, repeat, json } => bench(&path, repeat, json),
    }
}

fn inspect(path: &std::path::Path) -> anyhow::Result<()> {
    use hft_recorder::{Kind, Reader};
    let mut r = Reader::open(path)?;
    let mut counts = std::collections::BTreeMap::<String, u64>::new();
    let (mut first, mut last, mut bytes) = (None, 0i64, 0u64);
    let mut gaps = vec![];
    while let Some(rec) = r.next_record()? {
        *counts.entry(format!("{:?}", rec.kind)).or_default() += 1;
        first.get_or_insert(rec.recv_ns);
        last = rec.recv_ns;
        bytes += rec.payload.len() as u64;
        if matches!(rec.kind, Kind::Gap | Kind::ConnClose) {
            gaps.push((rec.recv_ns, String::from_utf8_lossy(rec.payload).into_owned()));
        }
    }
    let span = (last - first.unwrap_or(last)) as f64 / 1e9;
    println!("segments: {}", r.headers.len());
    if let Some(h) = r.headers.first() {
        println!(
            "venue/product: {}/{}  schema v{}  host {}  git {}",
            h.venue, h.product, h.schema_version, h.host, h.git_rev
        );
    }
    println!("span: {:.1} s ({:.2} h)   payload bytes: {:.1} MB", span, span / 3600.0, bytes as f64 / 1e6);
    for (k, n) in &counts {
        println!("  {k:<15} {n}");
    }
    println!("disconnects/gaps: {}", gaps.len());
    for (t, why) in gaps.iter().take(20) {
        println!("  +{:.1}s {why}", (t - first.unwrap_or(0)) as f64 / 1e9);
    }
    for (p, why) in &r.torn {
        println!("TORN segment {}: {why}", p.display());
    }
    Ok(())
}

fn verify(path: &std::path::Path, check_every: u64) -> anyhow::Result<()> {
    let mut src = hft_replay::open_source(path, Instrument::btc_usd())?;
    let t = std::time::Instant::now();
    let r = hft_replay::verify_stream(&mut *src, check_every)?;
    let el = t.elapsed().as_secs_f64();
    println!("items {}  market msgs {}  ({:.2} s, {:.0} msgs/s)", r.items, r.md_msgs, el, r.md_msgs as f64 / el);
    println!("feed: {:?}", r.feed);
    println!("disconnects in recording: {}", r.disconnects);
    println!("live checkpoints: {} ok, {} mismatched", r.checkpoints_ok, r.checkpoint_mismatches.len());
    for (seq, want, got) in r.checkpoint_mismatches.iter().take(10) {
        println!("  seq {seq}: live {want:016x} replay {got:016x}");
    }
    println!(
        "REST verification snapshots: {} ok, {} mismatched, {} unverifiable",
        r.verify_snapshots_ok, r.verify_snapshot_mismatches, r.verify_snapshots_unverifiable
    );
    for d in &r.mismatch_details {
        println!("  {d}");
    }
    for e in r.exchange_errors.iter().take(5) {
        println!("exchange error: {e}");
    }
    println!("final seq {}  book hash {:016x}  replay digest {:016x}", r.final_seq, r.final_hash, r.digest);
    let ok = r.checkpoint_mismatches.is_empty() && r.verify_snapshot_mismatches == 0 && r.feed.book_errors == 0;
    println!("{}", if ok { "VERIFY OK" } else { "VERIFY FAILED" });
    if !ok {
        std::process::exit(1);
    }
    Ok(())
}

fn bench(path: &std::path::Path, repeat: usize, json: Option<PathBuf>) -> anyhow::Result<()> {
    let one = hft_replay::load_raw_records(path)?;
    let mut recs = Vec::with_capacity(one.len() * repeat);
    for _ in 0..repeat {
        recs.extend(one.iter().cloned());
    }
    let venue = hft_replay::detect_venue(path)?;
    let r = hft_bench::tick_to_decision(&recs, venue, &Instrument::btc_usd())?;
    println!(
        "tick-to-decision [{}] (decode → book → strategy), {} frames, {} market msgs, {:.1} MB",
        r.venue,
        r.frames,
        r.md_msgs,
        r.bytes as f64 / 1e6
    );
    println!(
        "  p50 {} ns   p90 {} ns   p99 {} ns   p99.9 {} ns   max {} ns   mean {:.0} ns",
        r.p50_ns, r.p90_ns, r.p99_ns, r.p999_ns, r.max_ns, r.mean_ns
    );
    println!(
        "  throughput {:.2} M msgs/s (untimed pass)   allocations/frame {:.3}",
        r.throughput_msgs_per_s / 1e6,
        r.allocs_per_frame
    );
    println!("  host: {} | {} | {} | {}", r.host.cpu, r.host.os, r.host.rustc, r.host.profile);
    if let Some(p) = json {
        std::fs::write(&p, serde_json::to_string_pretty(&r)?)?;
        println!("wrote {}", p.display());
    }
    Ok(())
}
