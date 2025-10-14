//! Wire encodings for [`MarketEvent`].
//!
//! * `Binary` — the raw 64-byte little-endian struct. Encoding is a borrow of
//!   the event's memory (`bytemuck::bytes_of`), no serialization step.
//! * `Json` — a human-readable object, handy for debugging and browsers.

use serde::{Deserialize, Serialize};

use crate::model::{EventKind, MarketEvent, Side, Symbol, EVENT_SIZE, PRICE_SCALE};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WireFormat {
    #[default]
    Json,
    Binary,
}

/// Zero-copy view of an event as wire bytes.
#[inline]
pub fn encode_binary(ev: &MarketEvent) -> &[u8] {
    bytemuck::bytes_of(ev)
}

/// Decodes one binary event. Accepts unaligned input (e.g. a slice of a
/// network read buffer) by copying the 64 bytes into an aligned value.
pub fn decode_binary(buf: &[u8]) -> Option<MarketEvent> {
    if buf.len() != EVENT_SIZE {
        return None;
    }
    bytemuck::try_pod_read_unaligned::<MarketEvent>(buf).ok()
}

/// JSON shape of an event as sent to clients.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventJson {
    #[serde(rename = "type")]
    pub kind: EventKind,
    pub seq: u64,
    pub symbol: Symbol,
    pub side: Side,
    /// Decimal string so no precision is lost to f64.
    pub price: String,
    pub qty: u64,
    #[serde(skip_serializing_if = "is_zero", default)]
    pub order_id: u64,
    pub exchange_ts_ns: u64,
    pub publish_ts_ns: u64,
}

fn is_zero(v: &u64) -> bool {
    *v == 0
}

/// Formats a fixed-point price as a decimal string with trailing zeros trimmed.
pub fn format_price(price: i64) -> String {
    let neg = price < 0;
    let abs = price.unsigned_abs();
    let scale = PRICE_SCALE as u64;
    let whole = abs / scale;
    let frac = abs % scale;
    let mut s = if frac == 0 {
        whole.to_string()
    } else {
        let frac_str = format!("{frac:08}");
        format!("{whole}.{}", frac_str.trim_end_matches('0'))
    };
    if neg {
        s.insert(0, '-');
    }
    s
}

/// Parses a decimal price string into fixed point.
pub fn parse_price(s: &str) -> Option<i64> {
    let (neg, s) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s),
    };
    let (whole, frac) = s.split_once('.').unwrap_or((s, ""));
    if frac.len() > 8 || whole.is_empty() {
        return None;
    }
    let whole: i64 = whole.parse().ok()?;
    let frac_val: i64 = if frac.is_empty() {
        0
    } else {
        format!("{frac:0<8}").parse().ok()?
    };
    let v = whole.checked_mul(PRICE_SCALE)?.checked_add(frac_val)?;
    Some(if neg { -v } else { v })
}

impl EventJson {
    pub fn from_event(ev: &MarketEvent) -> Option<EventJson> {
        Some(EventJson {
            kind: ev.kind()?,
            seq: ev.seq,
            symbol: ev.symbol,
            side: ev.side()?,
            price: format_price(ev.price),
            qty: ev.qty,
            order_id: ev.order_id,
            exchange_ts_ns: ev.exchange_ts_ns,
            publish_ts_ns: ev.publish_ts_ns,
        })
    }

    pub fn to_event(&self) -> Option<MarketEvent> {
        Some(MarketEvent {
            seq: self.seq,
            publish_ts_ns: self.publish_ts_ns,
            exchange_ts_ns: self.exchange_ts_ns,
            symbol: self.symbol,
            price: parse_price(&self.price)?,
            qty: self.qty,
            order_id: self.order_id,
            kind: self.kind as u8,
            side: self.side as u8,
            _reserved: [0; 6],
        })
    }
}

/// Encodes an event as a JSON string. Returns `None` for malformed events.
pub fn encode_json(ev: &MarketEvent) -> Option<String> {
    EventJson::from_event(ev).and_then(|j| serde_json::to_string(&j).ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::price_from_f64;

    fn sample() -> MarketEvent {
        let mut ev = MarketEvent::new(
            EventKind::Trade,
            Symbol::new("ETH-USD").unwrap(),
            Side::Sell,
            price_from_f64(3150.25),
            42,
        );
        ev.seq = 9;
        ev.publish_ts_ns = 123;
        ev
    }

    #[test]
    fn binary_roundtrip() {
        let ev = sample();
        let bytes = encode_binary(&ev);
        assert_eq!(bytes.len(), 64);
        assert_eq!(decode_binary(bytes), Some(ev));
        assert_eq!(decode_binary(&bytes[1..]), None);
    }

    #[test]
    fn binary_decode_unaligned() {
        let ev = sample();
        let mut buf = vec![0u8; 65];
        buf[1..].copy_from_slice(encode_binary(&ev));
        assert_eq!(decode_binary(&buf[1..]), Some(ev));
    }

    #[test]
    fn json_roundtrip() {
        let ev = sample();
        let s = encode_json(&ev).unwrap();
        assert!(s.contains("\"price\":\"3150.25\""));
        assert!(s.contains("\"type\":\"trade\""));
        let back: EventJson = serde_json::from_str(&s).unwrap();
        assert_eq!(back.to_event(), Some(ev));
    }

    #[test]
    fn price_formatting() {
        assert_eq!(format_price(0), "0");
        assert_eq!(format_price(150_000_000), "1.5");
        assert_eq!(format_price(-1), "-0.00000001");
        assert_eq!(parse_price("1.5"), Some(150_000_000));
        assert_eq!(parse_price("-0.00000001"), Some(-1));
        assert_eq!(parse_price("1.123456789"), None);
    }
}
