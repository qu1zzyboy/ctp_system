//! Shared infrastructure for assembling client and server.
//!
//! Holds wire protocol messages, logging helpers, network clients, and common
//! errors — analogous to nautilus `common` + `network`.

pub mod error;
pub mod logging;
pub mod network;
pub mod protocol;

pub use error::*;
pub use logging::*;
pub use network::{
    ConnectionMode, CtpClient, CtpClientConfig, ExponentialBackoff, SendError, TcpClient,
    TcpClientConfig, TcpMessageHandler,
};
pub use protocol::*;
