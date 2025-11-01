//! Ring buffer and codec micro-benchmarks.
//!
//! Run with `cargo bench`.

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use ferro_feed::codec::{encode_binary, encode_json};
use ferro_feed::model::{price_from_f64, EventKind, MarketEvent, Side, Symbol};
use ferro_feed::ring::{self, RecvError};

fn sample() -> MarketEvent {
    MarketEvent::new(
        EventKind::Trade,
        Symbol::new("AAPL").unwrap(),
        Side::Buy,
        price_from_f64(189.42),
        100,
    )
}

fn publish_only(c: &mut Criterion) {
    let (mut tx, _sub) = ring::channel::<MarketEvent>(1 << 16);
    let ev = sample();
    let mut g = c.benchmark_group("ring");
    g.throughput(Throughput::Elements(1));
    g.bench_function("publish_no_readers", |b| b.iter(|| tx.publish(black_box(ev))));
    g.finish();
}

fn publish_then_read(c: &mut Criterion) {
    let (mut tx, sub) = ring::channel::<MarketEvent>(1 << 16);
    let mut rx = sub.subscribe();
    let ev = sample();
    let mut g = c.benchmark_group("ring");
    g.throughput(Throughput::Elements(1));
    g.bench_function("publish_then_try_recv", |b| {
        b.iter(|| {
            tx.publish(black_box(ev));
            black_box(rx.try_recv().unwrap())
        })
    });
    g.finish();
}

/// One producer thread, N spinning reader threads; measures how long it
/// takes for every reader to consume a fixed number of events.
fn fan_out(c: &mut Criterion) {
    const EVENTS: u64 = 100_000;
    let mut g = c.benchmark_group("ring_fan_out");
    g.throughput(Throughput::Elements(EVENTS));
    g.sample_size(10);
    for readers in [1usize, 4, 8] {
        g.bench_with_input(BenchmarkId::from_parameter(readers), &readers, |b, &n| {
            b.iter(|| {
                let (mut tx, sub) = ring::channel::<MarketEvent>(1 << 14);
                let go = Arc::new(AtomicBool::new(false));
                let handles: Vec<_> = (0..n)
                    .map(|_| {
                        let mut rx = sub.subscribe();
                        let go = go.clone();
                        std::thread::spawn(move || {
                            while !go.load(Ordering::Acquire) {
                                std::hint::spin_loop();
                            }
                            let mut got = 0u64;
                            loop {
                                match rx.try_recv() {
                                    Ok(_) => got += 1,
                                    Err(RecvError::Lagged(_)) | Err(RecvError::Empty) => {
                                        std::hint::spin_loop()
                                    }
                                    Err(RecvError::Closed) => return got,
                                }
                            }
                        })
                    })
                    .collect();
                go.store(true, Ordering::Release);
                let ev = sample();
                for _ in 0..EVENTS {
                    tx.publish(ev);
                }
                drop(tx);
                for h in handles {
                    black_box(h.join().unwrap());
                }
            })
        });
    }
    g.finish();
}

fn codecs(c: &mut Criterion) {
    let ev = sample();
    let mut g = c.benchmark_group("codec");
    g.throughput(Throughput::Elements(1));
    g.bench_function("encode_binary", |b| b.iter(|| black_box(encode_binary(black_box(&ev))).len()));
    g.bench_function("encode_json", |b| b.iter(|| encode_json(black_box(&ev))));
    g.finish();
}

criterion_group!(benches, publish_only, publish_then_read, fan_out, codecs);
criterion_main!(benches);
