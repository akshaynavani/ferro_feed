//! End-to-end: ring -> WebSocket server -> client, plus the TCP ingest path.

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use ferro_feed::codec::{decode_binary, encode_binary, EventJson, WireFormat};
use ferro_feed::feed::{ingest, FeedPublisher};
use ferro_feed::metrics::Metrics;
use ferro_feed::model::{price_from_f64, EventKind, MarketEvent, Side, Symbol, EVENT_SIZE};
use ferro_feed::ring::{self, Subscriber};
use ferro_feed::server::{self, ServerConfig};

type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

fn event(sym: &str, px: f64) -> MarketEvent {
    MarketEvent::new(
        EventKind::Trade,
        Symbol::new(sym).unwrap(),
        Side::Buy,
        price_from_f64(px),
        10,
    )
}

async fn start_server(
    subscriber: Subscriber<MarketEvent>,
    metrics: Arc<Metrics>,
) -> (String, watch::Sender<bool>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (stop_tx, stop_rx) = watch::channel(false);
    let cfg = ServerConfig {
        symbols: vec!["AAPL".into(), "MSFT".into()],
        ..Default::default()
    };
    tokio::spawn(server::serve(listener, subscriber, cfg, metrics, stop_rx));
    (url, stop_tx)
}

async fn next_text(ws: &mut Client) -> serde_json::Value {
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("timed out waiting for message")
            .expect("stream ended")
            .unwrap();
        if let Message::Text(t) = msg {
            return serde_json::from_str(&t).unwrap();
        }
    }
}

async fn request(ws: &mut Client, req: serde_json::Value) -> serde_json::Value {
    ws.send(Message::Text(req.to_string())).await.unwrap();
    next_text(ws).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_session_receives_only_subscribed_symbols() {
    let (producer, sub) = ring::channel(1024);
    let metrics = Arc::new(Metrics::new());
    let mut publisher = FeedPublisher::new(producer).with_metrics(metrics.clone());
    let (url, _stop) = start_server(sub, metrics.clone()).await;

    let (mut ws, _) = tokio_tungstenite::connect_async(url.as_str()).await.unwrap();
    let welcome = next_text(&mut ws).await;
    assert_eq!(welcome["type"], "welcome");
    assert_eq!(welcome["event_size"], EVENT_SIZE);

    let ack = request(&mut ws, serde_json::json!({"op":"subscribe","symbols":["aapl"]})).await;
    assert_eq!(ack["type"], "subscribed");
    assert_eq!(ack["symbols"][0], "AAPL");

    publisher.publish(event("MSFT", 410.0));
    publisher.publish(event("AAPL", 189.5));
    publisher.publish(event("MSFT", 411.0));
    publisher.publish(event("AAPL", 190.25));

    let first: EventJson = serde_json::from_value(next_text(&mut ws).await).unwrap();
    let second: EventJson = serde_json::from_value(next_text(&mut ws).await).unwrap();
    assert_eq!(first.symbol.as_str(), "AAPL");
    assert_eq!(first.price, "189.5");
    assert_eq!(second.price, "190.25");
    assert!(second.seq > first.seq);
    assert!(first.publish_ts_ns > 0);

    let pong = request(&mut ws, serde_json::json!({"op":"ping","id":42})).await;
    assert_eq!(pong["type"], "pong");
    assert_eq!(pong["id"], 42);

    let snap = metrics.snapshot();
    assert_eq!(snap.events_published, 4);
    assert_eq!(snap.sessions_active, 1);
    assert!(snap.events_sent >= 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_session_receives_raw_events() {
    let (producer, sub) = ring::channel(1024);
    let metrics = Arc::new(Metrics::new());
    let mut publisher = FeedPublisher::new(producer);
    let (url, _stop) = start_server(sub, metrics).await;

    let (mut ws, _) = tokio_tungstenite::connect_async(url.as_str()).await.unwrap();
    next_text(&mut ws).await; // welcome
    let ack = request(&mut ws, serde_json::json!({"op":"format","format":"binary"})).await;
    assert_eq!(ack["format"], serde_json::to_value(WireFormat::Binary).unwrap());
    request(&mut ws, serde_json::json!({"op":"subscribe","symbols":["*"]})).await;

    let batch: Vec<_> = (0..10).map(|i| event("AAPL", 100.0 + i as f64)).collect();
    publisher.publish_batch(&batch);

    let mut received = Vec::new();
    while received.len() < 10 {
        let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let Message::Binary(buf) = msg {
            assert_eq!(buf.len() % EVENT_SIZE, 0);
            received.extend(buf.chunks_exact(EVENT_SIZE).map(|f| decode_binary(f).unwrap()));
        }
    }
    let seqs: Vec<u64> = received.iter().map(|e| e.seq).collect();
    assert_eq!(seqs, (1..=10).collect::<Vec<_>>());
    assert_eq!(received[3].price, price_from_f64(103.0));
}

#[tokio::test]
async fn invalid_request_returns_error() {
    let (_producer, sub) = ring::channel::<MarketEvent>(16);
    let (url, _stop) = start_server(sub, Arc::new(Metrics::new())).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(url.as_str()).await.unwrap();
    next_text(&mut ws).await;
    let err = request(&mut ws, serde_json::json!({"op":"subscribe","symbols":["not valid!"]})).await;
    assert_eq!(err["type"], "error");
    let err = request(&mut ws, serde_json::json!({"op":"launch"})).await;
    assert_eq!(err["type"], "error");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ingest_publishes_valid_frames_and_rejects_garbage() {
    let (producer, sub) = ring::channel(1024);
    let metrics = Arc::new(Metrics::new());
    let publisher = FeedPublisher::new(producer).with_metrics(metrics.clone());
    let mut rx = sub.subscribe();

    // Reserve a free port, then hand it to the ingest listener.
    let addr = {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap()
    };
    let (_stop_tx, stop_rx) = watch::channel(false);
    tokio::spawn(ingest::run(addr, publisher, stop_rx));

    let mut upstream = loop {
        match TcpStream::connect(addr).await {
            Ok(s) => break s,
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    };

    let good = event("NVDA", 120.0);
    let mut bad = good;
    bad.kind = 200;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(encode_binary(&good));
    bytes.extend_from_slice(encode_binary(&bad));
    bytes.extend_from_slice(encode_binary(&good));
    // Split a frame across writes to exercise partial-frame buffering.
    upstream.write_all(&bytes[..100]).await.unwrap();
    upstream.flush().await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    upstream.write_all(&bytes[100..]).await.unwrap();
    upstream.flush().await.unwrap();

    let a = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.unwrap().unwrap();
    let b = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.unwrap().unwrap();
    assert_eq!((a.seq, b.seq), (1, 2));
    assert_eq!(a.symbol.as_str(), "NVDA");
    assert_eq!(metrics.snapshot().ingest_rejected, 1);
}
