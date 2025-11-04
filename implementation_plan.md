# ferro_feed — Implementation Plan

Low-latency market data feed server in Rust. A single feed handler publishes normalized
market events (trades, quotes, order adds/cancels) into a lock-free broadcast ring buffer;
any number of WebSocket sessions read from the ring with independent cursors, filter by
their subscribed symbols, and push frames to clients. Events are fixed-size `#[repr(C)]`
POD structs, so passing them through the ring involves no heap allocation and the binary
wire format is the event's bytes verbatim (zero-copy encode).

## Key design decisions

- **Ring buffer**: single-producer / multi-consumer *broadcast* ring (every reader sees every
  event) with per-slot sequence numbers (seqlock style). The producer never blocks: slow
  readers get lapped and receive `Lagged(n)` and skip forward instead of stalling the feed.
  Capacity is a power of two so indexing is a mask.
- **Wake-ups**: readers spin briefly, then park on a `tokio::sync::Notify` that the producer
  signals after each publish batch. The hot path (publish/read) is atomics only.
- **Events**: `MarketEvent` is 64 bytes, `Pod` via `bytemuck`; symbol is a fixed `[u8; 8]`.
  Every event carries the publish timestamp (ns since epoch) so clients can measure latency.
- **Wire formats**: per-session choice of `json` (debuggable) or `binary` (raw 64-byte events,
  zero-copy from the ring slot to the WebSocket frame via `bytemuck::bytes_of`).
- **Feed sources**: built-in simulator (random-walk prices, configurable rate) and a TCP
  ingest that accepts length-free 64-byte binary frames from an upstream publisher.
- **Stack**: Rust 2021, Tokio, tokio-tungstenite, serde/serde_json, bytemuck, clap, tracing,
  hdrhistogram (client). No external services required.

## Phases

### Phase 1 — Crate scaffold and lock-free ring buffer
- `Cargo.toml`, `src/lib.rs`
- `src/ring.rs`: `BroadcastRing<T: Copy>`, `Producer`, `Reader` with `try_read`,
  `Lagged` handling, async `recv` built on `Notify`, cache-padded cursor.
- Unit tests for ordering, wrap-around, lag detection, multi-reader fan-out.

### Phase 2 — Market data model, codec, feed sources
- `src/model.rs`: `Symbol`, `Side`, `EventKind`, `MarketEvent` (Pod, 64 bytes), timestamps.
- `src/codec.rs`: JSON and binary encoding of events.
- `src/feed/mod.rs`, `src/feed/sim.rs` (random-walk simulator with per-symbol books),
  `src/feed/ingest.rs` (TCP binary ingest).

### Phase 3 — WebSocket server and subscriptions
- `src/protocol.rs`: client requests (`subscribe`, `unsubscribe`, `ping`, `format`) and
  server control messages.
- `src/subscription.rs`: per-session symbol filter (hash set of 8-byte symbols + wildcard).
- `src/server.rs`, `src/session.rs`: accept loop, handshake, per-session reader task with
  select over client messages and ring events, lag notifications, clean close.

### Phase 4 — Config, metrics, binary entrypoint
- `src/config.rs` (clap + env), `src/metrics.rs` (atomic counters, publish-to-send latency
  histogram buckets), `src/stats.rs` (`/stats` JSON over a side HTTP port), `src/main.rs`
  with graceful shutdown on Ctrl-C.

### Phase 5 — Client, benchmarks, tests
- `src/bin/feed_client.rs`: subscribes, measures end-to-end latency percentiles with
  hdrhistogram; `src/bin/upstream_sim.rs` streams binary events into the ingest port.
- `benches/ring.rs` (criterion) for publish/read throughput.
- `tests/ring_concurrency.rs`, `tests/ws_end_to_end.rs`.
- `.gitignore`, `README.md` with architecture diagram, run/test instructions.
