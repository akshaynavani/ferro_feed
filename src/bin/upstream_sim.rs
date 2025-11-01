//! Stand-in upstream exchange gateway: connects to a ferro_feed server
//! started with `--source ingest` and streams simulated 64-byte binary events.

use std::time::{Duration, Instant};

use anyhow::Context;
use clap::Parser;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use ferro_feed::codec::encode_binary;
use ferro_feed::feed::sim::{SimConfig, Simulator};
use ferro_feed::model::{now_ns, Symbol, EVENT_SIZE};

#[derive(Parser, Debug)]
#[command(about = "Stream simulated binary market events into a ferro_feed ingest port")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:9100")]
    addr: String,
    #[arg(long, value_delimiter = ',', default_value = "AAPL,MSFT,NVDA,BTC-USD,ETH-USD")]
    symbols: Vec<String>,
    /// Events per second.
    #[arg(long, default_value_t = 50_000)]
    rate: u64,
    /// Events per TCP write.
    #[arg(long, default_value_t = 64)]
    batch: usize,
    /// Stop after this many events (0 = forever).
    #[arg(long, default_value_t = 0)]
    count: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let symbols: Vec<Symbol> = args
        .symbols
        .iter()
        .map(|s| Symbol::new(s).with_context(|| format!("invalid symbol {s:?}")))
        .collect::<Result<_, _>>()?;
    let batch = args.batch.max(1);
    let mut sim = Simulator::new(SimConfig {
        symbols,
        events_per_sec: args.rate,
        batch,
        seed: None,
    });

    let mut stream = TcpStream::connect(&args.addr)
        .await
        .with_context(|| format!("connecting to ingest at {}", args.addr))?;
    stream.set_nodelay(true)?;
    eprintln!("connected to {}, streaming at {} ev/s", args.addr, args.rate);

    let period = Duration::from_nanos(batch as u64 * 1_000_000_000 / args.rate.max(1));
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut buf = Vec::with_capacity(batch * EVENT_SIZE);
    let mut sent = 0u64;
    let start = Instant::now();
    loop {
        ticker.tick().await;
        buf.clear();
        for _ in 0..batch {
            let mut ev = sim.next_event();
            ev.exchange_ts_ns = now_ns();
            buf.extend_from_slice(encode_binary(&ev));
        }
        stream.write_all(&buf).await?;
        sent += batch as u64;
        if args.count > 0 && sent >= args.count {
            break;
        }
    }
    stream.shutdown().await?;
    eprintln!(
        "sent {sent} events in {:.2}s",
        start.elapsed().as_secs_f64()
    );
    Ok(())
}
