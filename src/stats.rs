//! Tiny HTTP endpoint exposing metrics as JSON, kept off the WebSocket port
//! so monitoring never competes with feed traffic.
//!
//! * `GET /stats`  — [`crate::metrics::MetricsSnapshot`] plus ring state
//! * `GET /health` — `ok`

use std::sync::Arc;

use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use crate::metrics::{Metrics, MetricsSnapshot};
use crate::model::MarketEvent;
use crate::ring::Subscriber;

#[derive(Serialize)]
struct StatsBody {
    ring_capacity: usize,
    ring_head: u64,
    #[serde(flatten)]
    metrics: MetricsSnapshot,
}

pub async fn serve(
    listener: TcpListener,
    metrics: Arc<Metrics>,
    subscriber: Subscriber<MarketEvent>,
    mut shutdown: watch::Receiver<bool>,
) -> std::io::Result<()> {
    tracing::info!(addr = %listener.local_addr()?, "stats endpoint listening");
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else { continue };
                let metrics = metrics.clone();
                let subscriber = subscriber.clone();
                tokio::spawn(async move {
                    if let Err(e) = respond(stream, &metrics, &subscriber).await {
                        tracing::debug!(error = %e, "stats request failed");
                    }
                });
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
        }
    }
}

async fn respond(
    mut stream: TcpStream,
    metrics: &Metrics,
    subscriber: &Subscriber<MarketEvent>,
) -> std::io::Result<()> {
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await?;
    let request = String::from_utf8_lossy(&buf[..n]);
    let mut parts = request.split_whitespace();
    let (method, path) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));

    let (status, content_type, body) = match (method, path) {
        ("GET", "/stats") => {
            let body = StatsBody {
                ring_capacity: subscriber.capacity(),
                ring_head: subscriber.head(),
                metrics: metrics.snapshot(),
            };
            (
                "200 OK",
                "application/json",
                serde_json::to_string_pretty(&body).unwrap_or_default(),
            )
        }
        ("GET", "/health") => ("200 OK", "text/plain", "ok".to_string()),
        _ => ("404 Not Found", "text/plain", "not found".to_string()),
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}
