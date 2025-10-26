//! Lock-free server metrics: atomic counters plus a log2-bucketed latency
//! histogram of publish -> socket-write time.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use serde::Serialize;

/// Bucket `i` counts latencies in `[2^i, 2^(i+1))` nanoseconds; the last
/// bucket also absorbs everything larger (~1.1 s and up).
const BUCKETS: usize = 31;

#[derive(Default)]
pub struct Metrics {
    pub events_published: AtomicU64,
    pub ingest_rejected: AtomicU64,
    pub events_sent: AtomicU64,
    pub frames_sent: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub lag_notices: AtomicU64,
    pub events_skipped: AtomicU64,
    pub sessions_total: AtomicU64,
    pub sessions_active: AtomicU64,
    latency: LatencyHistogram,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn record_latency_ns(&self, ns: u64) {
        self.latency.record(ns);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            events_published: self.events_published.load(Relaxed),
            ingest_rejected: self.ingest_rejected.load(Relaxed),
            events_sent: self.events_sent.load(Relaxed),
            frames_sent: self.frames_sent.load(Relaxed),
            bytes_sent: self.bytes_sent.load(Relaxed),
            lag_notices: self.lag_notices.load(Relaxed),
            events_skipped: self.events_skipped.load(Relaxed),
            sessions_total: self.sessions_total.load(Relaxed),
            sessions_active: self.sessions_active.load(Relaxed),
            latency: self.latency.summary(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    pub events_published: u64,
    pub ingest_rejected: u64,
    pub events_sent: u64,
    pub frames_sent: u64,
    pub bytes_sent: u64,
    pub lag_notices: u64,
    pub events_skipped: u64,
    pub sessions_total: u64,
    pub sessions_active: u64,
    pub latency: LatencySummary,
}

pub struct LatencyHistogram {
    buckets: [AtomicU64; BUCKETS],
    count: AtomicU64,
    sum_ns: AtomicU64,
    max_ns: AtomicU64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        LatencyHistogram {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum_ns: AtomicU64::new(0),
            max_ns: AtomicU64::new(0),
        }
    }
}

/// Latency percentiles in microseconds. Percentiles report the upper bound
/// of the bucket they fall in, so they are conservative to within 2x.
#[derive(Debug, Clone, Default, Serialize)]
pub struct LatencySummary {
    pub count: u64,
    pub mean_us: f64,
    pub p50_us: f64,
    pub p90_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    pub max_us: f64,
}

impl LatencyHistogram {
    #[inline]
    fn bucket_for(ns: u64) -> usize {
        let b = 63 - ns.max(1).leading_zeros() as usize;
        b.min(BUCKETS - 1)
    }

    #[inline]
    pub fn record(&self, ns: u64) {
        self.buckets[Self::bucket_for(ns)].fetch_add(1, Relaxed);
        self.count.fetch_add(1, Relaxed);
        self.sum_ns.fetch_add(ns, Relaxed);
        self.max_ns.fetch_max(ns, Relaxed);
    }

    pub fn summary(&self) -> LatencySummary {
        let counts: Vec<u64> = self.buckets.iter().map(|b| b.load(Relaxed)).collect();
        let total: u64 = counts.iter().sum();
        if total == 0 {
            return LatencySummary::default();
        }
        let pct = |q: f64| -> f64 {
            let target = ((total as f64) * q).ceil().max(1.0) as u64;
            let mut seen = 0;
            for (i, c) in counts.iter().enumerate() {
                seen += c;
                if seen >= target {
                    return (1u64 << (i + 1)) as f64 / 1_000.0;
                }
            }
            f64::INFINITY
        };
        let count = self.count.load(Relaxed);
        LatencySummary {
            count,
            mean_us: self.sum_ns.load(Relaxed) as f64 / count.max(1) as f64 / 1_000.0,
            p50_us: pct(0.50),
            p90_us: pct(0.90),
            p99_us: pct(0.99),
            p999_us: pct(0.999),
            max_us: self.max_ns.load(Relaxed) as f64 / 1_000.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_are_log2() {
        assert_eq!(LatencyHistogram::bucket_for(0), 0);
        assert_eq!(LatencyHistogram::bucket_for(1), 0);
        assert_eq!(LatencyHistogram::bucket_for(1024), 10);
        assert_eq!(LatencyHistogram::bucket_for(u64::MAX), BUCKETS - 1);
    }

    #[test]
    fn percentiles() {
        let h = LatencyHistogram::default();
        for _ in 0..99 {
            h.record(1_000); // bucket 9 -> upper bound 1024 ns
        }
        h.record(1_000_000);
        let s = h.summary();
        assert_eq!(s.count, 100);
        assert!((s.p50_us - 1.024).abs() < 1e-9);
        assert!(s.p999_us > 1_000.0);
        assert!((s.max_us - 1_000.0).abs() < 1e-9);
    }
}
