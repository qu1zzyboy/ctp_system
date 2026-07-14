//! Trading terminal client library.
//!
//! Connects to `ctp-server`, sends login / trade / query / control commands,
//! and receives routed responses and pushes.

pub mod connection;
pub mod credentials;
pub mod market_data;
pub mod trading;

pub use connection::*;
pub use credentials::*;
pub use market_data::*;
pub use trading::*;
