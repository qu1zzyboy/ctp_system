//! Network clients (nautilus-style).
//!
//! - [`tcp_client`]: internal Client ↔ Server TCP messaging
//! - [`ctp_client`]: external CTP market-data (MD) connection

pub mod backoff;
pub mod ctp_client;
pub mod error;
pub mod mode;
pub mod tcp_client;

pub use backoff::ExponentialBackoff;
pub use ctp_client::{CtpClient, CtpClientConfig, CtpEvent, MarketDataTick};
pub use error::SendError;
pub use mode::ConnectionMode;
pub use tcp_client::{TcpClient, TcpClientConfig, TcpMessageHandler};
