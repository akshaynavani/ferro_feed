//! TCP ingest for an upstream publisher.
//!
//! The upstream connects and streams back-to-back 64-byte binary
//! [`MarketEvent`]s (see [`crate::codec::encode_binary`]). The server
//! validates each event, overwrites `seq`/`publish_ts_ns`, and publishes in
//! batches of whatever arrived in one socket read. One upstream is served at
//! a time because the ring has a single producer.

use std::net::SocketAddr;

use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use super::FeedPublisher;
use crate::codec::decode_binary;
use crate::model::{MarketEvent, EVENT_SIZE};

const READ_BUF: usize = 64 * 1024;

/// Accepts upstream connections on `addr` until `shutdown` flips to `true`.
pub async fn run(
    addr: SocketAddr,
    mut publisher: FeedPublisher,
    mut shutdown: watch::Receiver<bool>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(%addr, "ingest listening for upstream feed");
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                tracing::info!(%peer, "upstream connected");
                match serve_upstream(stream, &mut publisher, &mut shutdown).await {
                    Ok(stats) => tracing::info!(%peer, ?stats, "upstream disconnected"),
                    Err(e) => tracing::warn!(%peer, error = %e, "upstream error"),
                }
            }
            _ = shutdown.changed() => {}
        }
        if *shutdown.borrow() {
            return Ok(());
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct IngestStats {
    pub accepted: u64,
    pub rejected: u64,
}

/// Reads frames from one upstream connection until EOF or shutdown.
pub async fn serve_upstream(
    mut stream: TcpStream,
    publisher: &mut FeedPublisher,
    shutdown: &mut watch::Receiver<bool>,
) -> std::io::Result<IngestStats> {
    stream.set_nodelay(true)?;
    let mut buf = vec![0u8; READ_BUF];
    let mut filled = 0usize;
    let mut batch: Vec<MarketEvent> = Vec::with_capacity(READ_BUF / EVENT_SIZE);
    let mut stats = IngestStats::default();

    loop {
        let n = tokio::select! {
            r = stream.read(&mut buf[filled..]) => r?,
            _ = shutdown.changed() => return Ok(stats),
        };
        if n == 0 {
            return Ok(stats);
        }
        filled += n;

        let whole = filled - filled % EVENT_SIZE;
        batch.clear();
        let mut rejected = 0;
        for frame in buf[..whole].chunks_exact(EVENT_SIZE) {
            match decode_binary(frame) {
                Some(ev) if ev.is_valid() => batch.push(ev),
                _ => rejected += 1,
            }
        }
        stats.accepted += publisher.publish_batch(&batch) as u64;
        stats.rejected += rejected;
        publisher.record_rejected(rejected);

        // Keep the partial trailing frame for the next read.
        buf.copy_within(whole..filled, 0);
        filled -= whole;
    }
}
