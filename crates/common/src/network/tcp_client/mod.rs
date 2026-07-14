//! Internal TCP client for Client ↔ Server messaging (nautilus `SocketClient` style).
//!
//! Framing: 4-byte big-endian length prefix + payload bytes (typically JSON
//! [`crate::protocol::Envelope`]). Split read/write tasks + controller with
//! exponential-backoff reconnect.

pub mod client;
pub mod config;
pub mod types;

pub use client::TcpClient;
pub use config::TcpClientConfig;
pub use types::{TcpMessageHandler, TcpReader, TcpWriter, WriterCommand};
