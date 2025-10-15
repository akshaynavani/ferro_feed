//! Synthetic exchange feed: random-walk mid prices with order adds, cancels,
//! top-of-book quotes and trades across a configurable symbol universe.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use super::FeedPublisher;
use crate::model::{price_from_f64, EventKind, MarketEvent, Side, Symbol};

const MAX_RESTING: usize = 256;

#[derive(Debug, Clone)]
pub struct SimConfig {
    pub symbols: Vec<Symbol>,
    /// Target events per second; 0 means as fast as possible.
    pub events_per_sec: u64,
    /// Events generated and published per wake-up.
    pub batch: usize,
    pub seed: Option<u64>,
}

impl Default for SimConfig {
    fn default() -> Self {
        SimConfig {
            symbols: ["AAPL", "MSFT", "NVDA", "BTC-USD", "ETH-USD"]
                .iter()
                .filter_map(|s| Symbol::new(s))
                .collect(),
            events_per_sec: 100_000,
            batch: 64,
            seed: None,
        }
    }
}

struct RestingOrder {
    id: u64,
    side: Side,
    price: i64,
    qty: u64,
}

struct SymbolState {
    symbol: Symbol,
    mid: i64,
    tick: i64,
    resting: Vec<RestingOrder>,
}

pub struct Simulator {
    cfg: SimConfig,
    rng: SmallRng,
    books: Vec<SymbolState>,
    next_order_id: u64,
}

impl Simulator {
    pub fn new(cfg: SimConfig) -> Self {
        assert!(!cfg.symbols.is_empty(), "simulator needs at least one symbol");
        let mut rng = match cfg.seed {
            Some(s) => SmallRng::seed_from_u64(s),
            None => SmallRng::from_entropy(),
        };
        let books = cfg
            .symbols
            .iter()
            .map(|&symbol| {
                let start = rng.gen_range(20.0..2_000.0);
                SymbolState {
                    symbol,
                    mid: price_from_f64(start),
                    tick: price_from_f64(0.01),
                    resting: Vec::with_capacity(MAX_RESTING),
                }
            })
            .collect();
        Simulator {
            cfg,
            rng,
            books,
            next_order_id: 1,
        }
    }

    fn random_side(rng: &mut SmallRng) -> Side {
        if rng.gen_bool(0.5) {
            Side::Buy
        } else {
            Side::Sell
        }
    }

    /// Generates the next synthetic event.
    pub fn next_event(&mut self) -> MarketEvent {
        let idx = self.rng.gen_range(0..self.books.len());
        let roll: u32 = self.rng.gen_range(0..100);
        let rng = &mut self.rng;
        let book = &mut self.books[idx];

        match roll {
            // Cancel a resting order (or forced when the book is full).
            _ if book.resting.len() >= MAX_RESTING || (roll < 20 && !book.resting.is_empty()) => {
                let i = rng.gen_range(0..book.resting.len());
                let o = book.resting.swap_remove(i);
                MarketEvent::new(EventKind::OrderCancel, book.symbol, o.side, o.price, o.qty)
                    .with_order_id(o.id)
            }
            0..=49 => {
                let side = Self::random_side(rng);
                let offset = book.tick * rng.gen_range(1..=20);
                let price = match side {
                    Side::Buy => book.mid - offset,
                    _ => book.mid + offset,
                };
                let qty = rng.gen_range(1..=50) * 100;
                let id = self.next_order_id;
                self.next_order_id += 1;
                book.resting.push(RestingOrder { id, side, price, qty });
                MarketEvent::new(EventKind::OrderAdd, book.symbol, side, price, qty).with_order_id(id)
            }
            50..=84 => {
                // Random-walk the mid and publish the new best level on one side.
                let step = rng.gen_range(-2..=2) * book.tick;
                book.mid = (book.mid + step).max(book.tick * 2);
                let side = Self::random_side(rng);
                let price = match side {
                    Side::Buy => book.mid - book.tick,
                    _ => book.mid + book.tick,
                };
                let qty = rng.gen_range(1..=20) * 100;
                MarketEvent::new(EventKind::Quote, book.symbol, side, price, qty)
            }
            _ => {
                let aggressor = Self::random_side(rng);
                let price = match aggressor {
                    Side::Buy => book.mid + book.tick,
                    _ => book.mid - book.tick,
                };
                let qty = rng.gen_range(1..=10) * 10;
                MarketEvent::new(EventKind::Trade, book.symbol, aggressor, price, qty)
            }
        }
    }

    /// Runs the generator on the current thread until `shutdown` is set.
    /// Pacing sleeps when far from the next deadline and spins when close,
    /// which keeps publish jitter low without burning a core at low rates.
    pub fn run(mut self, mut publisher: FeedPublisher, shutdown: Arc<AtomicBool>) {
        let batch = self.cfg.batch.max(1);
        let period = if self.cfg.events_per_sec == 0 {
            Duration::ZERO
        } else {
            Duration::from_nanos(batch as u64 * 1_000_000_000 / self.cfg.events_per_sec)
        };
        tracing::info!(
            symbols = self.books.len(),
            rate = self.cfg.events_per_sec,
            batch,
            "simulator started"
        );

        let mut buf = Vec::with_capacity(batch);
        let mut deadline = Instant::now();
        while !shutdown.load(Ordering::Relaxed) {
            buf.clear();
            buf.extend((0..batch).map(|_| self.next_event()));
            publisher.publish_batch(&buf);

            if period.is_zero() {
                continue;
            }
            deadline += period;
            let now = Instant::now();
            if deadline > now {
                let wait = deadline - now;
                if wait > Duration::from_micros(200) {
                    std::thread::sleep(wait - Duration::from_micros(100));
                }
                while Instant::now() < deadline {
                    std::hint::spin_loop();
                }
            } else if now - deadline > Duration::from_millis(100) {
                // Fell far behind (e.g. machine suspended); don't burst to catch up.
                deadline = now;
            }
        }
        tracing::info!(published = publisher.published(), "simulator stopped");
    }

    /// Spawns the simulator on a dedicated OS thread.
    pub fn spawn(
        self,
        publisher: FeedPublisher,
        shutdown: Arc<AtomicBool>,
    ) -> std::thread::JoinHandle<()> {
        std::thread::Builder::new()
            .name("ferro-sim".into())
            .spawn(move || self.run(publisher, shutdown))
            .expect("failed to spawn simulator thread")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_valid_events_for_configured_symbols() {
        let cfg = SimConfig {
            seed: Some(7),
            ..Default::default()
        };
        let symbols = cfg.symbols.clone();
        let mut sim = Simulator::new(cfg);
        for _ in 0..10_000 {
            let ev = sim.next_event();
            assert!(ev.is_valid());
            assert!(symbols.contains(&ev.symbol));
            assert!(ev.price > 0);
        }
    }

    #[test]
    fn deterministic_with_seed() {
        let mk = || {
            Simulator::new(SimConfig {
                seed: Some(42),
                ..Default::default()
            })
        };
        let (mut a, mut b) = (mk(), mk());
        for _ in 0..100 {
            let (x, y) = (a.next_event(), b.next_event());
            assert_eq!((x.kind, x.symbol, x.price, x.qty), (y.kind, y.symbol, y.price, y.qty));
        }
    }
}
