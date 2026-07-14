//! TCP client configuration.

use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use super::types::TcpMessageHandler;

/// Configuration for the internal TCP messaging client.
#[derive(Clone)]
pub struct TcpClientConfig {
    /// Server address, e.g. `127.0.0.1:9000`.
    pub url: String,
    /// Optional inbound message handler (raw framed payload, without length prefix).
    pub message_handler: Option<TcpMessageHandler>,
    /// Optional heartbeat: (interval_secs, payload).
    pub heartbeat: Option<(u64, Vec<u8>)>,
    /// Timeout waiting for ACTIVE before send / reconnect window (ms).
    pub reconnect_timeout_ms: Option<u64>,
    /// Initial reconnect delay (ms).
    pub reconnect_delay_initial_ms: Option<u64>,
    /// Max reconnect delay (ms).
    pub reconnect_delay_max_ms: Option<u64>,
    /// Exponential backoff factor.
    pub reconnect_backoff_factor: Option<f64>,
    /// Max jitter added to reconnect delay (ms).
    pub reconnect_jitter_ms: Option<u64>,
    /// Max attempts for the *initial* connect.
    pub connection_max_retries: Option<u32>,
    /// Max reconnect attempts after drop (`None` = unlimited).
    pub reconnect_max_attempts: Option<u32>,
    /// Raw framed payloads to replay after transport reconnect.
    pub reconnect_payloads: Option<Arc<Mutex<Vec<Vec<u8>>>>>,
}

impl Default for TcpClientConfig {
    fn default() -> Self {
        Self {
            url: "127.0.0.1:9000".into(),
            message_handler: None,
            heartbeat: None,
            reconnect_timeout_ms: Some(10_000),
            reconnect_delay_initial_ms: Some(500),
            reconnect_delay_max_ms: Some(30_000),
            reconnect_backoff_factor: Some(2.0),
            reconnect_jitter_ms: Some(250),
            connection_max_retries: Some(5),
            reconnect_max_attempts: None,
            reconnect_payloads: None,
        }
    }
}

impl TcpClientConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            ..Self::default()
        }
    }

    pub fn with_handler(mut self, handler: impl Fn(&[u8]) + Send + Sync + 'static) -> Self {
        self.message_handler = Some(Arc::new(handler));
        self
    }

    pub fn with_reconnect_payloads(mut self, payloads: Arc<Mutex<Vec<Vec<u8>>>>) -> Self {
        self.reconnect_payloads = Some(payloads);
        self
    }
}

impl Debug for TcpClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpClientConfig")
            .field("url", &self.url)
            .field(
                "message_handler",
                &self.message_handler.as_ref().map(|_| "<function>"),
            )
            .field("heartbeat", &self.heartbeat)
            .field("reconnect_timeout_ms", &self.reconnect_timeout_ms)
            .field(
                "reconnect_delay_initial_ms",
                &self.reconnect_delay_initial_ms,
            )
            .field("reconnect_delay_max_ms", &self.reconnect_delay_max_ms)
            .field("reconnect_backoff_factor", &self.reconnect_backoff_factor)
            .field("reconnect_jitter_ms", &self.reconnect_jitter_ms)
            .field("connection_max_retries", &self.connection_max_retries)
            .field("reconnect_max_attempts", &self.reconnect_max_attempts)
            .field(
                "reconnect_payloads",
                &self.reconnect_payloads.as_ref().map(|_| "<payloads>"),
            )
            .finish()
    }
}
