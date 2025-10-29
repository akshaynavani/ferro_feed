use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Context};
use clap::Parser;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing_subscriber::EnvFilter;

use ferro_feed::config::{Config, Source};
use ferro_feed::feed::sim::{SimConfig, Simulator};
use ferro_feed::feed::{ingest, FeedPublisher};
use ferro_feed::metrics::Metrics;
use ferro_feed::model::MarketEvent;
use ferro_feed::server::{self, ServerConfig};
use ferro_feed::{ring, stats};

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let cfg = Config::parse();
    let symbols = cfg.validate().map_err(|e| anyhow!(e))?;

    let mut rt = tokio::runtime::Builder::new_multi_thread();
    rt.enable_all().thread_name("ferro-worker");
    if let Some(n) = cfg.workers {
        rt.worker_threads(n);
    }
    rt.build()?.block_on(run(cfg, symbols))
}

async fn run(cfg: Config, symbols: Vec<ferro_feed::model::Symbol>) -> anyhow::Result<()> {
    let metrics = Arc::new(Metrics::new());
    let (producer, subscriber) = ring::channel::<MarketEvent>(cfg.ring_capacity);
    let publisher = FeedPublisher::new(producer).with_metrics(metrics.clone());

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let sim_stop = Arc::new(AtomicBool::new(false));

    // Feed source: owns the ring's single producer.
    let mut sim_thread = None;
    let mut ingest_task = None;
    match cfg.source {
        Source::Sim => {
            let sim = Simulator::new(SimConfig {
                symbols: symbols.clone(),
                events_per_sec: cfg.sim_rate,
                batch: cfg.sim_batch,
                seed: cfg.sim_seed,
            });
            sim_thread = Some(sim.spawn(publisher, sim_stop.clone()));
        }
        Source::Ingest => {
            let addr = cfg.ingest_addr;
            let rx = shutdown_rx.clone();
            ingest_task = Some(tokio::spawn(async move {
                if let Err(e) = ingest::run(addr, publisher, rx).await {
                    tracing::error!(error = %e, "ingest failed");
                }
            }));
        }
    }

    let ws_listener = TcpListener::bind(cfg.ws_addr)
        .await
        .with_context(|| format!("binding websocket address {}", cfg.ws_addr))?;
    let stats_listener = TcpListener::bind(cfg.stats_addr)
        .await
        .with_context(|| format!("binding stats address {}", cfg.stats_addr))?;

    let server_cfg = ServerConfig {
        max_sessions: cfg.max_sessions,
        max_batch: cfg.max_batch,
        default_format: cfg.default_format.into(),
        symbols: symbols.iter().map(|s| s.to_string()).collect(),
    };
    let ws_task = tokio::spawn(server::serve(
        ws_listener,
        subscriber.clone(),
        server_cfg,
        metrics.clone(),
        shutdown_rx.clone(),
    ));
    let stats_task = tokio::spawn(stats::serve(
        stats_listener,
        metrics.clone(),
        subscriber,
        shutdown_rx.clone(),
    ));

    tokio::signal::ctrl_c().await.context("installing Ctrl-C handler")?;
    tracing::info!("shutdown requested");

    sim_stop.store(true, Ordering::Relaxed);
    let _ = shutdown_tx.send(true);
    let _ = ws_task.await;
    let _ = stats_task.await;
    if let Some(t) = ingest_task {
        let _ = t.await;
    }
    if let Some(t) = sim_thread {
        let _ = tokio::task::spawn_blocking(move || t.join()).await;
    }
    // Give sessions a moment to send their close frames.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let snap = metrics.snapshot();
    tracing::info!(
        published = snap.events_published,
        sent = snap.events_sent,
        skipped = snap.events_skipped,
        p99_us = snap.latency.p99_us,
        "ferro_feed stopped"
    );
    Ok(())
}
