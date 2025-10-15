//! Feed sources and the publisher that stamps events into the ring.

pub mod ingest;
pub mod sim;

use crate::model::{now_ns, MarketEvent};
use crate::ring::{Producer, Subscriber};

/// Owns the ring's single producer. Assigns feed-wide sequence numbers and
/// publish timestamps so every consumer sees a gap-free, ordered stream.
pub struct FeedPublisher {
    producer: Producer<MarketEvent>,
    next_seq: u64,
}

impl FeedPublisher {
    pub fn new(producer: Producer<MarketEvent>) -> Self {
        FeedPublisher {
            producer,
            next_seq: 1,
        }
    }

    #[inline]
    fn stamp(&mut self, mut ev: MarketEvent, ts: u64) -> MarketEvent {
        ev.seq = self.next_seq;
        ev.publish_ts_ns = ts;
        self.next_seq += 1;
        ev
    }

    pub fn publish(&mut self, ev: MarketEvent) {
        let ev = self.stamp(ev, now_ns());
        self.producer.publish(ev);
    }

    /// Publishes a batch with a single timestamp read and a single wake-up.
    pub fn publish_batch(&mut self, events: &[MarketEvent]) -> usize {
        let ts = now_ns();
        let mut seq = self.next_seq;
        let n = self.producer.publish_batch(events.iter().map(|ev| {
            let mut ev = *ev;
            ev.seq = seq;
            ev.publish_ts_ns = ts;
            seq += 1;
            ev
        }));
        self.next_seq = seq;
        n
    }

    pub fn published(&self) -> u64 {
        self.next_seq - 1
    }

    pub fn subscriber(&self) -> Subscriber<MarketEvent> {
        self.producer.subscriber()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{EventKind, Side, Symbol};
    use crate::ring;

    #[test]
    fn assigns_contiguous_sequence_numbers() {
        let (tx, sub) = ring::channel(16);
        let mut publisher = FeedPublisher::new(tx);
        let mut rx = sub.subscribe();
        let ev = MarketEvent::new(EventKind::Quote, Symbol::new("X").unwrap(), Side::Buy, 1, 1);
        publisher.publish(ev);
        publisher.publish_batch(&[ev, ev]);
        let seqs: Vec<u64> = std::iter::from_fn(|| rx.try_recv().ok()).map(|e| e.seq).collect();
        assert_eq!(seqs, vec![1, 2, 3]);
        assert_eq!(publisher.published(), 3);
    }
}
