//! Subscribes to a ferro_feed server and reports end-to-end latency
//! (server publish timestamp -> client receive) and throughput.
//!
//! Latency is computed from wall clocks, so run the client on the same host
//! as the server (or on hosts with PTP-synchronised clocks).

use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use tokio_tungstenite::tungstenite::Message;

use ferro_feed::codec::{decode_binary, EventJson};
use ferro_feed::model::{now_ns, EVENT_SIZE};

#[derive(Parser, Debug)]
#[command(about = "ferro_feed latency/throughput client")]
struct Args {
    #[arg(long, default_value = "ws://127.0.0.1:9000")]
    url: String,
    /// Comma-separated symbols, or "*" for all.
    #[arg(long, value_delimiter = ',', default_value = "*")]
    symbols: Vec<String>,
    /// json or binary.
    #[arg(long, default_value = "binary")]
    format: String,
    /// Seconds to run (0 = until Ctrl-C).
    #[arg(long, default_value_t = 10)]
    duration: u64,
    /// Seconds between progress lines.
    #[arg(long, default_value_t = 1)]
    report_every: u64,
    /// Print every event (JSON) instead of measuring.
    #[arg(long)]
    print: bool,
}

struct Stats {
    hist: Histogram<u64>,
    events: u64,
    lag_notices: u64,
    skipped: u64,
    last_seq: u64,
    gaps: u64,
}

impl Stats {
    fn observe(&mut self, seq: u64, publish_ts_ns: u64) {
        let lat = now_ns().saturating_sub(publish_ts_ns).max(1);
        let _ = self.hist.record(lat);
        self.events += 1;
        if self.last_seq != 0 && seq <= self.last_seq {
            self.gaps += 1; // out-of-order would be a server bug
        }
        self.last_seq = seq;
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.format != "json" && args.format != "binary" {
        bail!("--format must be json or binary");
    }

    let (ws, _) = tokio_tungstenite::connect_async(args.url.as_str())
        .await
        .with_context(|| format!("connecting to {}", args.url))?;
    let (mut tx, mut rx) = ws.split();

    tx.send(Message::Text(
        serde_json::json!({"op": "format", "format": args.format}).to_string(),
    ))
    .await?;
    tx.send(Message::Text(
        serde_json::json!({"op": "subscribe", "symbols": args.symbols}).to_string(),
    ))
    .await?;

    let mut stats = Stats {
        hist: Histogram::new_with_bounds(1, 60_000_000_000, 3)?,
        events: 0,
        lag_notices: 0,
        skipped: 0,
        last_seq: 0,
        gaps: 0,
    };

    let start = Instant::now();
    let deadline = (args.duration > 0).then(|| start + Duration::from_secs(args.duration));
    let mut next_report = start + Duration::from_secs(args.report_every.max(1));
    let mut events_at_report = 0;

    loop {
        let timeout = deadline
            .map(|d| d.saturating_duration_since(Instant::now()))
            .unwrap_or(Duration::from_secs(3600));
        let msg = tokio::select! {
            m = rx.next() => m,
            _ = tokio::time::sleep(timeout) => break,
            _ = tokio::signal::ctrl_c() => break,
        };
        let Some(msg) = msg else { break };
        match msg? {
            Message::Binary(buf) => {
                for frame in buf.chunks_exact(EVENT_SIZE) {
                    if let Some(ev) = decode_binary(frame) {
                        if args.print {
                            println!("{ev:?}");
                        }
                        stats.observe(ev.seq, ev.publish_ts_ns);
                    }
                }
            }
            Message::Text(text) => {
                if let Ok(ev) = serde_json::from_str::<EventJson>(&text) {
                    if args.print {
                        println!("{text}");
                    }
                    stats.observe(ev.seq, ev.publish_ts_ns);
                } else {
                    let v: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
                    if v["type"] == "lagged" {
                        stats.lag_notices += 1;
                        stats.skipped += v["skipped"].as_u64().unwrap_or(0);
                    } else {
                        eprintln!("control: {text}");
                    }
                }
            }
            Message::Close(frame) => {
                eprintln!("server closed: {frame:?}");
                break;
            }
            _ => {}
        }

        if !args.print && Instant::now() >= next_report {
            let delta = stats.events - events_at_report;
            events_at_report = stats.events;
            eprintln!(
                "[{:>5.1}s] {:>9} ev/s  p50 {:>7.1}us  p99 {:>7.1}us  lagged {}",
                start.elapsed().as_secs_f64(),
                delta / args.report_every.max(1),
                stats.hist.value_at_quantile(0.50) as f64 / 1e3,
                stats.hist.value_at_quantile(0.99) as f64 / 1e3,
                stats.lag_notices,
            );
            next_report += Duration::from_secs(args.report_every.max(1));
        }
    }

    let _ = tx.send(Message::Close(None)).await;
    let secs = start.elapsed().as_secs_f64();
    let h = &stats.hist;
    println!("events received : {}", stats.events);
    println!("throughput      : {:.0} events/s", stats.events as f64 / secs);
    println!("lag notices     : {} ({} events skipped)", stats.lag_notices, stats.skipped);
    println!("ordering errors : {}", stats.gaps);
    if stats.events > 0 {
        println!("latency (publish -> receive):");
        for (label, q) in [("p50", 0.5), ("p90", 0.9), ("p99", 0.99), ("p99.9", 0.999)] {
            println!("  {label:<6}: {:>9.1} us", h.value_at_quantile(q) as f64 / 1e3);
        }
        println!("  max   : {:>9.1} us", h.max() as f64 / 1e3);
    }
    Ok(())
}
