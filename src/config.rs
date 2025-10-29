//! Command-line / environment configuration.

use std::net::SocketAddr;

use clap::{Parser, ValueEnum};

use crate::codec::WireFormat;
use crate::model::Symbol;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Source {
    /// Built-in random-walk market simulator.
    Sim,
    /// Accept 64-byte binary events from an upstream over TCP.
    Ingest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum FormatArg {
    Json,
    Binary,
}

impl From<FormatArg> for WireFormat {
    fn from(f: FormatArg) -> Self {
        match f {
            FormatArg::Json => WireFormat::Json,
            FormatArg::Binary => WireFormat::Binary,
        }
    }
}

#[derive(Debug, Clone, Parser)]
#[command(name = "ferro_feed", version, about = "Low-latency market data WebSocket feed server")]
pub struct Config {
    /// WebSocket listen address.
    #[arg(long, env = "FERRO_WS_ADDR", default_value = "0.0.0.0:9000")]
    pub ws_addr: SocketAddr,

    /// HTTP stats listen address.
    #[arg(long, env = "FERRO_STATS_ADDR", default_value = "127.0.0.1:9001")]
    pub stats_addr: SocketAddr,

    /// Where market events come from.
    #[arg(long, env = "FERRO_SOURCE", value_enum, default_value_t = Source::Sim)]
    pub source: Source,

    /// Upstream ingest listen address (when --source ingest).
    #[arg(long, env = "FERRO_INGEST_ADDR", default_value = "127.0.0.1:9100")]
    pub ingest_addr: SocketAddr,

    /// Comma-separated symbol universe for the simulator / welcome message.
    #[arg(
        long,
        env = "FERRO_SYMBOLS",
        value_delimiter = ',',
        default_value = "AAPL,MSFT,NVDA,AMZN,GOOG,BTC-USD,ETH-USD,SOL-USD"
    )]
    pub symbols: Vec<String>,

    /// Simulator target rate in events/second (0 = unthrottled).
    #[arg(long, env = "FERRO_SIM_RATE", default_value_t = 100_000)]
    pub sim_rate: u64,

    /// Simulator events generated per publish.
    #[arg(long, env = "FERRO_SIM_BATCH", default_value_t = 32)]
    pub sim_batch: usize,

    /// Fixed RNG seed for a reproducible simulated feed.
    #[arg(long, env = "FERRO_SIM_SEED")]
    pub sim_seed: Option<u64>,

    /// Ring buffer slots (power of two). Bounds how far a session may fall
    /// behind before it is lapped.
    #[arg(long, env = "FERRO_RING_CAPACITY", default_value_t = 1 << 16)]
    pub ring_capacity: usize,

    /// Max events drained from the ring per session write.
    #[arg(long, env = "FERRO_MAX_BATCH", default_value_t = 256)]
    pub max_batch: usize,

    /// Max concurrent WebSocket sessions.
    #[arg(long, env = "FERRO_MAX_SESSIONS", default_value_t = 10_000)]
    pub max_sessions: usize,

    /// Initial wire format for new sessions.
    #[arg(long, env = "FERRO_DEFAULT_FORMAT", value_enum, default_value_t = FormatArg::Json)]
    pub default_format: FormatArg,

    /// Tokio worker threads (defaults to the number of cores).
    #[arg(long, env = "FERRO_WORKERS")]
    pub workers: Option<usize>,
}

impl Config {
    /// Validates values clap cannot check on its own.
    pub fn validate(&self) -> Result<Vec<Symbol>, String> {
        if !self.ring_capacity.is_power_of_two() || self.ring_capacity < 2 {
            return Err(format!(
                "--ring-capacity must be a power of two >= 2 (got {})",
                self.ring_capacity
            ));
        }
        if self.max_batch == 0 || self.sim_batch == 0 {
            return Err("--max-batch and --sim-batch must be > 0".into());
        }
        let mut symbols = Vec::with_capacity(self.symbols.len());
        for s in &self.symbols {
            let sym = Symbol::new(s.trim()).ok_or_else(|| format!("invalid symbol {s:?}"))?;
            if !symbols.contains(&sym) {
                symbols.push(sym);
            }
        }
        if symbols.is_empty() {
            return Err("at least one symbol is required".into());
        }
        Ok(symbols)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid() {
        let cfg = Config::parse_from(["ferro_feed"]);
        let syms = cfg.validate().unwrap();
        assert_eq!(syms.len(), 8);
        assert_eq!(cfg.source, Source::Sim);
    }

    #[test]
    fn rejects_bad_ring_capacity() {
        let cfg = Config::parse_from(["ferro_feed", "--ring-capacity", "1000"]);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn parses_symbol_list() {
        let cfg = Config::parse_from(["ferro_feed", "--symbols", "aapl,msft,aapl"]);
        let syms = cfg.validate().unwrap();
        assert_eq!(syms.iter().map(|s| s.as_str()).collect::<Vec<_>>(), ["AAPL", "MSFT"]);
    }
}
