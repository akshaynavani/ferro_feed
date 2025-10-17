//! Per-session symbol filter.

use std::collections::HashSet;

use crate::model::{EventKind, MarketEvent, Symbol};
use crate::protocol::WILDCARD;

#[derive(Debug, Default, Clone)]
pub struct Subscription {
    all: bool,
    symbols: HashSet<Symbol>,
}

/// Result of applying a subscribe/unsubscribe request.
#[derive(Debug, Default, PartialEq)]
pub struct Change {
    pub applied: Vec<String>,
    pub invalid: Vec<String>,
}

impl Subscription {
    pub fn is_empty(&self) -> bool {
        !self.all && self.symbols.is_empty()
    }

    #[inline]
    pub fn matches(&self, ev: &MarketEvent) -> bool {
        if ev.kind == EventKind::Heartbeat as u8 {
            return !self.is_empty();
        }
        self.all || self.symbols.contains(&ev.symbol)
    }

    pub fn subscribe(&mut self, names: &[String]) -> Change {
        let mut change = Change::default();
        for name in names {
            if name == WILDCARD {
                self.all = true;
                change.applied.push(WILDCARD.to_string());
            } else if let Some(sym) = Symbol::new(name) {
                self.symbols.insert(sym);
                change.applied.push(sym.to_string());
            } else {
                change.invalid.push(name.clone());
            }
        }
        change
    }

    pub fn unsubscribe(&mut self, names: &[String]) -> Change {
        let mut change = Change::default();
        for name in names {
            if name == WILDCARD {
                self.all = false;
                self.symbols.clear();
                change.applied.push(WILDCARD.to_string());
            } else if let Some(sym) = Symbol::new(name) {
                self.symbols.remove(&sym);
                change.applied.push(sym.to_string());
            } else {
                change.invalid.push(name.clone());
            }
        }
        change
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Side;

    fn ev(sym: &str) -> MarketEvent {
        MarketEvent::new(EventKind::Trade, Symbol::new(sym).unwrap(), Side::Buy, 1, 1)
    }

    #[test]
    fn filters_by_symbol() {
        let mut s = Subscription::default();
        assert!(!s.matches(&ev("AAPL")));
        let c = s.subscribe(&["aapl".into(), "bad sym".into()]);
        assert_eq!(c.applied, vec!["AAPL"]);
        assert_eq!(c.invalid, vec!["bad sym"]);
        assert!(s.matches(&ev("AAPL")));
        assert!(!s.matches(&ev("MSFT")));
        s.unsubscribe(&["AAPL".into()]);
        assert!(s.is_empty());
    }

    #[test]
    fn wildcard() {
        let mut s = Subscription::default();
        s.subscribe(&["*".into()]);
        assert!(s.matches(&ev("ANY")));
        s.unsubscribe(&["*".into()]);
        assert!(!s.matches(&ev("ANY")));
    }
}
