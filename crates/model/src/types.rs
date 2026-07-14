//! Core trading value types and aggregates.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::enums::{ConnectionState, Direction, OffsetFlag, OrderStatus, OrderType};
use crate::identifiers::{AccountId, ClientId, ClientOrderId, ExchangeOrderId, InstrumentId};

/// Credentials used when a Client asks Server to log into a CTP account.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountCredentials {
    pub account_id: AccountId,
    pub password: String,
    pub broker_id: String,
    pub app_id: String,
    pub auth_code: String,
}

/// Snapshot of a CTP account connection as seen by the server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountState {
    pub account_id: AccountId,
    pub state: ConnectionState,
    pub front_id: Option<i32>,
    pub session_id: Option<i32>,
    pub updated_at: DateTime<Utc>,
}

/// Order submit request payload (domain level).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub client_order_id: ClientOrderId,
    pub account_id: AccountId,
    pub instrument_id: InstrumentId,
    pub direction: Direction,
    pub offset: OffsetFlag,
    pub order_type: OrderType,
    pub volume: i32,
    pub price: f64,
}

/// Cancel request payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelRequest {
    pub account_id: AccountId,
    pub instrument_id: InstrumentId,
    pub client_order_id: Option<ClientOrderId>,
    pub exchange_order_id: Option<ExchangeOrderId>,
}

/// Live order state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub client_order_id: ClientOrderId,
    pub exchange_order_id: Option<ExchangeOrderId>,
    pub account_id: AccountId,
    pub client_id: ClientId,
    pub instrument_id: InstrumentId,
    pub direction: Direction,
    pub offset: OffsetFlag,
    pub order_type: OrderType,
    pub volume: i32,
    pub volume_traded: i32,
    pub price: f64,
    pub status: OrderStatus,
    pub status_msg: Option<String>,
    pub inserted_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Fill / trade report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub trade_id: String,
    pub exchange_order_id: ExchangeOrderId,
    pub client_order_id: Option<ClientOrderId>,
    pub account_id: AccountId,
    pub instrument_id: InstrumentId,
    pub direction: Direction,
    pub offset: OffsetFlag,
    pub price: f64,
    pub volume: i32,
    pub trade_time: DateTime<Utc>,
}

/// Position snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub account_id: AccountId,
    pub instrument_id: InstrumentId,
    pub direction: Direction,
    pub volume: i32,
    pub yd_volume: i32,
    pub open_cost: f64,
    pub position_cost: f64,
    pub use_margin: f64,
    pub unrealized_pnl: f64,
}

/// Account fund / margin snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountBalance {
    pub account_id: AccountId,
    pub balance: f64,
    pub available: f64,
    pub curr_margin: f64,
    pub frozen_margin: f64,
    pub commission: f64,
    pub close_profit: f64,
    pub position_profit: f64,
}
