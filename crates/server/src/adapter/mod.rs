//! Server adapters (nautilus-style ports).
//!
//! - [`exchange`]: outbound CTP venue connectivity (1 MD + N Trade)
//! - [`internal`]: inbound Client ↔ Server TCP messaging

pub mod exchange;
pub mod internal;

pub use exchange::{
    resolve_dynlib_paths, ExchangeGateway, MdSession, MdSessionConfig, TradeSession,
    TradeSessionConfig, TradeSessionEvent, MD_DYNLIB_NAME, TD_DYNLIB_NAME,
};
pub use internal::{InternalGateway, TcpEndpointKind, TcpServer};
