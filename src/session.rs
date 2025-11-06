//! One WebSocket client session.
//!
//! Each session owns a ring [`Reader`] and runs a single task that selects
//! between client control frames and new ring events. Ring events are drained
//! in bursts (up to `max_batch`), filtered by subscription, encoded, and
//! written with one flush per burst. A session that cannot keep up is lapped
//! by the producer; it gets a `lagged` notice and continues from the recent
//! past instead of slowing the feed for everyone else.

use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use futures_util::{Sink, SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use crate::codec::{encode_binary, encode_json, WireFormat};
use crate::metrics::Metrics;
use crate::model::{now_ns, MarketEvent, EVENT_SIZE};
use crate::protocol::{ClientRequest, ServerMessage};
use crate::ring::{Reader, RecvError};
use crate::subscription::Subscription;

type Ws = WebSocketStream<TcpStream>;
type WsError = tokio_tungstenite::tungstenite::Error;

pub struct SessionConfig {
    pub id: u64,
    pub max_batch: usize,
    pub default_format: WireFormat,
    pub symbols: Vec<String>,
    pub metrics: Arc<Metrics>,
}

pub struct Session {
    cfg: SessionConfig,
    reader: Reader<MarketEvent>,
    sub: Subscription,
    format: WireFormat,
    /// Reused across bursts to avoid per-burst allocation.
    burst: Vec<MarketEvent>,
}

/// Why a session ended.
#[derive(Debug)]
pub enum SessionEnd {
    ClientClosed,
    FeedClosed,
    Shutdown,
    Error(WsError),
}

impl Session {
    pub fn new(cfg: SessionConfig, reader: Reader<MarketEvent>) -> Self {
        let max_batch = cfg.max_batch.max(1);
        Session {
            format: cfg.default_format,
            cfg,
            reader,
            sub: Subscription::default(),
            burst: Vec::with_capacity(max_batch),
        }
    }

    pub async fn run(mut self, ws: Ws, mut shutdown: watch::Receiver<bool>) -> SessionEnd {
        let (mut tx, mut rx) = ws.split();

        let welcome = ServerMessage::Welcome {
            session: self.cfg.id,
            format: self.format,
            symbols: self.cfg.symbols.clone(),
            event_size: EVENT_SIZE,
        };
        if let Err(e) = tx.send(Message::Text(welcome.to_json())).await {
            return SessionEnd::Error(e);
        }

        loop {
            tokio::select! {
                biased;

                _ = shutdown.changed() => {
                    let _ = tx.send(close_frame(CloseCode::Away, "server shutting down")).await;
                    return SessionEnd::Shutdown;
                }

                incoming = rx.next() => {
                    let msg = match incoming {
                        None => return SessionEnd::ClientClosed,
                        Some(Err(e)) => return SessionEnd::Error(e),
                        Some(Ok(m)) => m,
                    };
                    match msg {
                        Message::Text(text) => {
                            let reply = self.handle_request(&text);
                            if let Err(e) = tx.send(Message::Text(reply.to_json())).await {
                                return SessionEnd::Error(e);
                            }
                        }
                        Message::Close(_) => return SessionEnd::ClientClosed,
                        // Pings are answered by tungstenite on the next write.
                        Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
                        Message::Binary(_) => {
                            let reply = ServerMessage::error("binary requests are not supported");
                            if let Err(e) = tx.send(Message::Text(reply.to_json())).await {
                                return SessionEnd::Error(e);
                            }
                        }
                    }
                }

                next = self.reader.recv() => {
                    match next {
                        Ok(first) => {
                            self.drain_burst(first);
                            if let Err(e) = self.write_burst(&mut tx).await {
                                return SessionEnd::Error(e);
                            }
                        }
                        Err(RecvError::Lagged(skipped)) => {
                            tracing::debug!(session = self.cfg.id, skipped, "session lagged");
                            self.cfg.metrics.lag_notices.fetch_add(1, Relaxed);
                            self.cfg.metrics.events_skipped.fetch_add(skipped, Relaxed);
                            let notice = ServerMessage::Lagged { skipped };
                            if let Err(e) = tx.send(Message::Text(notice.to_json())).await {
                                return SessionEnd::Error(e);
                            }
                        }
                        Err(RecvError::Closed) => {
                            let _ = tx.send(close_frame(CloseCode::Away, "feed closed")).await;
                            return SessionEnd::FeedClosed;
                        }
                        Err(RecvError::Empty) => {}
                    }
                }
            }
        }
    }

    /// Collects `first` plus whatever else is already in the ring (up to
    /// `max_batch`) that matches the subscription.
    fn drain_burst(&mut self, first: MarketEvent) {
        self.burst.clear();
        if self.sub.matches(&first) {
            self.burst.push(first);
        }
        for _ in 1..self.cfg.max_batch {
            match self.reader.try_recv() {
                Ok(ev) => {
                    if self.sub.matches(&ev) {
                        self.burst.push(ev);
                    }
                }
                // Lag is reported on the next `recv`; stop the burst here.
                Err(_) => break,
            }
        }
    }

    async fn write_burst<S>(&mut self, tx: &mut S) -> Result<(), WsError>
    where
        S: Sink<Message, Error = WsError> + Unpin,
    {
        if self.burst.is_empty() {
            return Ok(());
        }
        match self.format {
            WireFormat::Binary => {
                // Events are copied byte-for-byte from the ring value into
                // the frame; there is no serialization step.
                // The frame takes ownership of its buffer, so build it at
                // its exact size in one allocation.
                let mut frame = Vec::with_capacity(self.burst.len() * EVENT_SIZE);
                for ev in &self.burst {
                    frame.extend_from_slice(encode_binary(ev));
                }
                let bytes = frame.len() as u64;
                tx.send(Message::Binary(frame)).await?;
                self.record_sent(1, bytes);
            }
            WireFormat::Json => {
                let mut frames = 0;
                let mut bytes = 0;
                for ev in &self.burst {
                    if let Some(text) = encode_json(ev) {
                        frames += 1;
                        bytes += text.len() as u64;
                        tx.feed(Message::Text(text)).await?;
                    }
                }
                tx.flush().await?;
                self.record_sent(frames, bytes);
            }
        }
        Ok(())
    }

    /// Records counters and publish -> write latency for the burst just sent.
    fn record_sent(&self, frames: u64, bytes: u64) {
        let m = &self.cfg.metrics;
        let now = now_ns();
        for ev in &self.burst {
            m.record_latency_ns(now.saturating_sub(ev.publish_ts_ns));
        }
        m.events_sent.fetch_add(self.burst.len() as u64, Relaxed);
        m.frames_sent.fetch_add(frames, Relaxed);
        m.bytes_sent.fetch_add(bytes, Relaxed);
    }

    fn handle_request(&mut self, text: &str) -> ServerMessage {
        let req: ClientRequest = match serde_json::from_str(text) {
            Ok(r) => r,
            Err(e) => return ServerMessage::error(format!("bad request: {e}")),
        };
        match req {
            ClientRequest::Subscribe { symbols } => {
                let change = self.sub.subscribe(&symbols);
                if !change.invalid.is_empty() {
                    return ServerMessage::error(format!("invalid symbols: {:?}", change.invalid));
                }
                ServerMessage::Subscribed {
                    symbols: change.applied,
                }
            }
            ClientRequest::Unsubscribe { symbols } => {
                let change = self.sub.unsubscribe(&symbols);
                if !change.invalid.is_empty() {
                    return ServerMessage::error(format!("invalid symbols: {:?}", change.invalid));
                }
                ServerMessage::Unsubscribed {
                    symbols: change.applied,
                }
            }
            ClientRequest::Format { format } => {
                self.format = format;
                ServerMessage::Format { format }
            }
            ClientRequest::Ping { id } => ServerMessage::Pong {
                id,
                server_ts_ns: now_ns(),
            },
        }
    }
}

fn close_frame(code: CloseCode, reason: &'static str) -> Message {
    Message::Close(Some(CloseFrame {
        code,
        reason: reason.into(),
    }))
}
