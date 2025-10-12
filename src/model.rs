//! Normalized market data events.
//!
//! [`MarketEvent`] is a fixed 64-byte, `#[repr(C)]` plain-old-data struct.
//! It moves through the ring by copy (no heap allocation) and its in-memory
//! bytes *are* the binary wire format (little-endian on all supported
//! targets), so encoding a binary frame is a zero-copy `bytes_of` view.

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Prices are fixed-point integers with 8 implied decimals.
pub const PRICE_SCALE: i64 = 100_000_000;

/// Size of one encoded event in bytes.
pub const EVENT_SIZE: usize = std::mem::size_of::<MarketEvent>();

const _: () = assert!(EVENT_SIZE == 64, "MarketEvent must stay exactly one cache line");
#[cfg(not(target_endian = "little"))]
compile_error!("ferro_feed's binary wire format assumes a little-endian target");

/// Up to 8 ASCII bytes, NUL padded (e.g. `b"AAPL\0\0\0\0"`).
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Pod, Zeroable, Default)]
pub struct Symbol(pub [u8; 8]);

impl Symbol {
    /// Builds a symbol from a string, upper-casing it. Returns `None` if it is
    /// empty, longer than 8 bytes, or not ASCII alphanumeric / `.` / `-`.
    pub fn new(s: &str) -> Option<Symbol> {
        let bytes = s.as_bytes();
        if bytes.is_empty()
            || bytes.len() > 8
            || !bytes
                .iter()
                .all(|b| b.is_ascii_alphanumeric() || *b == b'.' || *b == b'-')
        {
            return None;
        }
        let mut out = [0u8; 8];
        for (dst, src) in out.iter_mut().zip(bytes) {
            *dst = src.to_ascii_uppercase();
        }
        Some(Symbol(out))
    }

    pub fn as_str(&self) -> &str {
        let len = self.0.iter().position(|b| *b == 0).unwrap_or(8);
        // Constructed only from validated ASCII, or zeroed.
        std::str::from_utf8(&self.0[..len]).unwrap_or("?")
    }
}

impl fmt::Debug for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Symbol({})", self.as_str())
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for Symbol {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Symbol {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = <std::borrow::Cow<'de, str>>::deserialize(d)?;
        Symbol::new(&s).ok_or_else(|| serde::de::Error::custom(format!("invalid symbol {s:?}")))
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    Trade = 1,
    Quote = 2,
    OrderAdd = 3,
    OrderCancel = 4,
    Heartbeat = 5,
}

impl EventKind {
    pub fn from_u8(v: u8) -> Option<EventKind> {
        Some(match v {
            1 => EventKind::Trade,
            2 => EventKind::Quote,
            3 => EventKind::OrderAdd,
            4 => EventKind::OrderCancel,
            5 => EventKind::Heartbeat,
            _ => return None,
        })
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    None = 0,
    Buy = 1,
    Sell = 2,
}

impl Side {
    pub fn from_u8(v: u8) -> Option<Side> {
        Some(match v {
            0 => Side::None,
            1 => Side::Buy,
            2 => Side::Sell,
            _ => return None,
        })
    }
}

/// One normalized market data event. Field order is chosen so the struct has
/// no implicit padding (required for `Pod`).
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Pod, Zeroable, Default)]
pub struct MarketEvent {
    /// Feed-wide sequence number assigned by the server on publish.
    pub seq: u64,
    /// Nanoseconds since the Unix epoch when the server published the event.
    pub publish_ts_ns: u64,
    /// Nanoseconds since the Unix epoch reported by the upstream source.
    pub exchange_ts_ns: u64,
    pub symbol: Symbol,
    /// Fixed-point price, see [`PRICE_SCALE`].
    pub price: i64,
    pub qty: u64,
    pub order_id: u64,
    /// [`EventKind`] discriminant.
    pub kind: u8,
    /// [`Side`] discriminant.
    pub side: u8,
    pub _reserved: [u8; 6],
}

impl MarketEvent {
    pub fn new(kind: EventKind, symbol: Symbol, side: Side, price: i64, qty: u64) -> Self {
        MarketEvent {
            kind: kind as u8,
            side: side as u8,
            symbol,
            price,
            qty,
            exchange_ts_ns: now_ns(),
            ..Default::default()
        }
    }

    pub fn with_order_id(mut self, id: u64) -> Self {
        self.order_id = id;
        self
    }

    pub fn kind(&self) -> Option<EventKind> {
        EventKind::from_u8(self.kind)
    }

    pub fn side(&self) -> Option<Side> {
        Side::from_u8(self.side)
    }

    pub fn price_f64(&self) -> f64 {
        self.price as f64 / PRICE_SCALE as f64
    }

    /// Basic sanity check applied to events from untrusted upstreams.
    pub fn is_valid(&self) -> bool {
        self.kind().is_some()
            && self.side().is_some()
            && (self.kind() == Some(EventKind::Heartbeat) || self.symbol.0[0] != 0)
    }
}

impl fmt::Debug for MarketEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MarketEvent")
            .field("seq", &self.seq)
            .field("kind", &self.kind())
            .field("symbol", &self.symbol)
            .field("side", &self.side())
            .field("price", &self.price_f64())
            .field("qty", &self.qty)
            .field("order_id", &self.order_id)
            .field("publish_ts_ns", &self.publish_ts_ns)
            .finish()
    }
}

/// Wall-clock nanoseconds since the Unix epoch. Used for cross-process
/// latency measurement, so it must be wall time rather than `Instant`.
#[inline]
pub fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Converts a floating point price to fixed point.
pub fn price_from_f64(p: f64) -> i64 {
    (p * PRICE_SCALE as f64).round() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbol_roundtrip() {
        let s = Symbol::new("btc-usd").unwrap();
        assert_eq!(s.as_str(), "BTC-USD");
        assert!(Symbol::new("").is_none());
        assert!(Symbol::new("TOOLONGSYM").is_none());
        assert!(Symbol::new("A B").is_none());
    }

    #[test]
    fn event_is_one_cache_line() {
        assert_eq!(EVENT_SIZE, 64);
        assert_eq!(std::mem::align_of::<MarketEvent>(), 8);
    }

    #[test]
    fn validity() {
        let ev = MarketEvent::new(EventKind::Trade, Symbol::new("AAPL").unwrap(), Side::Buy, 1, 1);
        assert!(ev.is_valid());
        let mut bad = ev;
        bad.kind = 99;
        assert!(!bad.is_valid());
    }
}
