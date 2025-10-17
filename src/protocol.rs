//! WebSocket control protocol.
//!
//! Clients send JSON text frames tagged by `op`:
//!
//! ```json
//! {"op":"subscribe","symbols":["AAPL","BTC-USD"]}   // "*" subscribes to everything
//! {"op":"unsubscribe","symbols":["AAPL"]}
//! {"op":"format","format":"binary"}                  // or "json"
//! {"op":"ping","id":7}
//! ```
//!
//! The server always answers control requests with JSON text frames tagged
//! by `type`. Market events are sent as JSON text frames (see
//! [`crate::codec::EventJson`]) or, in binary mode, as binary frames holding
//! one or more back-to-back 64-byte events.

use serde::{Deserialize, Serialize};

use crate::codec::WireFormat;

pub const WILDCARD: &str = "*";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ClientRequest {
    Subscribe { symbols: Vec<String> },
    Unsubscribe { symbols: Vec<String> },
    Format { format: WireFormat },
    Ping {
        #[serde(default)]
        id: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    Welcome {
        session: u64,
        format: WireFormat,
        /// Symbols the feed currently carries (informational).
        symbols: Vec<String>,
        event_size: usize,
    },
    Subscribed {
        symbols: Vec<String>,
    },
    Unsubscribed {
        symbols: Vec<String>,
    },
    Format {
        format: WireFormat,
    },
    Pong {
        id: u64,
        server_ts_ns: u64,
    },
    /// The session fell behind the ring and `skipped` events were dropped.
    Lagged {
        skipped: u64,
    },
    Error {
        message: String,
    },
}

impl ServerMessage {
    pub fn error(msg: impl Into<String>) -> Self {
        ServerMessage::Error {
            message: msg.into(),
        }
    }

    pub fn to_json(&self) -> String {
        // Serializing these plain enums cannot fail.
        serde_json::to_string(self).expect("server message serializes")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_client_requests() {
        let r: ClientRequest =
            serde_json::from_str(r#"{"op":"subscribe","symbols":["aapl","*"]}"#).unwrap();
        assert_eq!(
            r,
            ClientRequest::Subscribe {
                symbols: vec!["aapl".into(), "*".into()]
            }
        );
        let r: ClientRequest = serde_json::from_str(r#"{"op":"format","format":"binary"}"#).unwrap();
        assert_eq!(
            r,
            ClientRequest::Format {
                format: WireFormat::Binary
            }
        );
        let r: ClientRequest = serde_json::from_str(r#"{"op":"ping"}"#).unwrap();
        assert_eq!(r, ClientRequest::Ping { id: 0 });
        assert!(serde_json::from_str::<ClientRequest>(r#"{"op":"nope"}"#).is_err());
    }

    #[test]
    fn server_messages_are_tagged() {
        let s = ServerMessage::Lagged { skipped: 5 }.to_json();
        assert_eq!(s, r#"{"type":"lagged","skipped":5}"#);
    }
}
