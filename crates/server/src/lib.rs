//! CTP multi-account trading server.
//!
//! Owns CTP MD (1) + Trade (N) sessions, client connections, and request routing.

pub mod actor;
pub mod adapter;
pub mod config;
pub mod session;

pub use actor::*;
pub use adapter::{
    resolve_dynlib_paths, ExchangeGateway, InternalGateway, MdSession, MdSessionConfig,
    TcpEndpointKind, TcpServer, TradeSession, TradeSessionConfig, TradeSessionEvent,
    MD_DYNLIB_NAME, TD_DYNLIB_NAME,
};
pub use config::*;
pub use session::*;
