//! WebSocket accept loop.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use futures_util::SinkExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;

use crate::codec::WireFormat;
use crate::model::MarketEvent;
use crate::ring::Subscriber;
use crate::session::{Session, SessionConfig, SessionEnd};

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub max_sessions: usize,
    pub max_batch: usize,
    pub default_format: WireFormat,
    /// Symbols advertised in the welcome message.
    pub symbols: Vec<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            max_sessions: 10_000,
            max_batch: 256,
            default_format: WireFormat::Json,
            symbols: Vec::new(),
        }
    }
}

struct ServerState {
    cfg: ServerConfig,
    subscriber: Subscriber<MarketEvent>,
    next_id: AtomicU64,
    active: AtomicUsize,
}

/// Decrements the active-session count when a session task ends, however it ends.
struct ActiveGuard(Arc<ServerState>);

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Accepts WebSocket clients on `listener` until `shutdown` flips to `true`.
pub async fn serve(
    listener: TcpListener,
    subscriber: Subscriber<MarketEvent>,
    cfg: ServerConfig,
    mut shutdown: watch::Receiver<bool>,
) -> std::io::Result<()> {
    let state = Arc::new(ServerState {
        cfg,
        subscriber,
        next_id: AtomicU64::new(1),
        active: AtomicUsize::new(0),
    });
    tracing::info!(addr = %listener.local_addr()?, "websocket server listening");

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, peer)) => {
                        tokio::spawn(handle_connection(state.clone(), stream, peer, shutdown.clone()));
                    }
                    // Transient accept errors (e.g. EMFILE) should not kill the server.
                    Err(e) => tracing::warn!(error = %e, "accept failed"),
                }
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    tracing::info!("websocket server stopping");
                    return Ok(());
                }
            }
        }
    }
}

async fn handle_connection(
    state: Arc<ServerState>,
    stream: TcpStream,
    peer: SocketAddr,
    shutdown: watch::Receiver<bool>,
) {
    if let Err(e) = stream.set_nodelay(true) {
        tracing::debug!(%peer, error = %e, "set_nodelay failed");
    }
    let mut ws = match tokio_tungstenite::accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            tracing::debug!(%peer, error = %e, "websocket handshake failed");
            return;
        }
    };

    let active = state.active.fetch_add(1, Ordering::Relaxed) + 1;
    let _guard = ActiveGuard(state.clone());
    if active > state.cfg.max_sessions {
        let _ = ws
            .send(Message::Close(Some(CloseFrame {
                code: CloseCode::Again,
                reason: "server at session capacity".into(),
            })))
            .await;
        return;
    }

    let id = state.next_id.fetch_add(1, Ordering::Relaxed);
    let session = Session::new(
        SessionConfig {
            id,
            max_batch: state.cfg.max_batch,
            default_format: state.cfg.default_format,
            symbols: state.cfg.symbols.clone(),
        },
        state.subscriber.subscribe(),
    );
    tracing::info!(session = id, %peer, active, "session opened");
    let end = session.run(ws, shutdown).await;
    match end {
        SessionEnd::Error(e) => tracing::info!(session = id, %peer, error = %e, "session ended with error"),
        other => tracing::info!(session = id, %peer, reason = ?other, "session closed"),
    }
}
