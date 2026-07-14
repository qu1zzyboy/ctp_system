//! External CTP market-data client (1 MD session).
//!
//! Wraps [`ctp2rs::v1alpha1::MdApi`] with nautilus-style connection mode
//! tracking and an event channel for depth market data / login callbacks.

pub mod client;
pub mod config;
pub mod types;

pub use client::CtpClient;
pub use config::CtpClientConfig;
pub use types::{CtpEvent, MarketDataTick};
