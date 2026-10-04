//! Micro-benchmarks: parse one frame, apply one book update, record one frame.
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use hft_book::Book;
use hft_coinbase::Parser;
use hft_core::{Instrument, OrderId, Px, Qty, Side};
use std::hint::black_box;

const OPEN: &[u8] = br#"{"type":"open","time":"2026-10-01T00:00:00.200000Z","product_id":"BTC-USD","sequence":10101,"order_id":"d50ec984-77a8-460a-b958-66f114b0de9b","price":"62345.67","remaining_size":"0.01250000","side":"sell"}"#;
const MATCH: &[u8] = br#"{"type":"match","trade_id":10,"sequence":50,"maker_order_id":"d50ec984-77a8-460a-b958-66f114b0de9b","taker_order_id":"132fb6ae-456b-4654-b4e0-d681ac05cea1","time":"2026-10-01T00:00:00.200000Z","product_id":"BTC-USD","size":"0.00500000","price":"62345.67","side":"sell"}"#;

fn parse(c: &mut Criterion) {
    let p = Parser::new(Instrument::btc_usd());
    let mut g = c.benchmark_group("parse");
    g.throughput(Throughput::Elements(1));
    g.bench_function("open", |b| b.iter(|| p.parse(black_box(OPEN)).unwrap()));
    g.bench_function("match", |b| b.iter(|| p.parse(black_box(MATCH)).unwrap()));
    g.finish();
}

fn book(c: &mut Criterion) {
    // A book with 20k orders over 2k levels per side, like a busy BTC-USD book.
    let mut base = Book::new();
    let mut id = 0u128;
    for lvl in 0..2000i64 {
        for _ in 0..5 {
            id += 1;
            base.open(OrderId(id), Side::Bid, Px(6_000_000 - lvl), Qty(1_000_000)).unwrap();
            id += 1;
            base.open(OrderId(id), Side::Ask, Px(6_000_001 + lvl), Qty(1_000_000)).unwrap();
        }
    }
    let mut g = c.benchmark_group("book");
    g.throughput(Throughput::Elements(1));
    g.bench_function("open+done near touch", |b| {
        let mut bk = base.clone();
        let mut n = 1u128 << 64;
        b.iter(|| {
            n += 1;
            bk.open(OrderId(n), Side::Bid, Px(5_999_995), Qty(100)).unwrap();
            bk.done(OrderId(n), Some(Qty(100))).unwrap();
        })
    });
    g.bench_function("best bid/ask", |b| b.iter(|| (black_box(&base).best_bid(), base.best_ask())));
    g.bench_function("clone (snapshot-size)", |b| b.iter_batched(|| (), |_| base.clone(), BatchSize::SmallInput));
    g.finish();
}

fn record(c: &mut Criterion) {
    let dir = tempfile_dir();
    let mut cfg = hft_recorder::RecorderConfig::new("coinbase", "BTC-USD");
    cfg.fsync_interval = None;
    let mut r = hft_recorder::Recorder::open(&dir, cfg).unwrap();
    let mut g = c.benchmark_group("recorder");
    g.throughput(Throughput::Bytes(OPEN.len() as u64));
    g.bench_function("append frame (buffered, no fsync)", |b| {
        b.iter(|| r.write(hft_recorder::Kind::WsFrame, 1, black_box(OPEN)).unwrap())
    });
    g.finish();
    drop(r);
    let _ = std::fs::remove_dir_all(&dir);
}

fn tempfile_dir() -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("hft-bench-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

criterion_group!(benches, parse, book, record);
criterion_main!(benches);
