//! Domain enums for orders, positions, and connection state.

use serde::{Deserialize, Serialize};

/// Direction of a futures order / position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Direction {
    Buy,
    Sell,
}

/// Open vs close for CTP futures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OffsetFlag {
    Open,
    Close,
    CloseToday,
    CloseYesterday,
}

/// Order price type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderType {
    Limit,
    Market,
    FAK,
    FOK,
}

/// Lifecycle status of an order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderStatus {
    Submitted,
    Accepted,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
    Unknown,
}

/// CTP account / session connection state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Authenticating,
    LoggedIn,
    Error,
}

/// Client permission relative to an account (bonus item).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ClientPermission {
    #[serde(alias = "full", alias = "read_write", alias = "ReadWrite")]
    Full,
    #[serde(
        alias = "query_only",
        alias = "readonly",
        alias = "read_only",
        alias = "ReadOnly"
    )]
    QueryOnly,
    #[serde(
        alias = "trade_only",
        alias = "writeonly",
        alias = "write_only",
        alias = "WriteOnly"
    )]
    TradeOnly,
}
