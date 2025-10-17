//! ferro_feed: a low-latency market data feed server.
//!
//! The core of the crate is [`ring::BroadcastRing`], a single-producer,
//! multi-consumer lock-free ring buffer where every reader observes every
//! event through its own cursor.

pub mod ring;
pub mod model;
pub mod codec;
pub mod feed;
pub mod protocol;
pub mod subscription;
