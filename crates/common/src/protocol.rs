//! Client ↔ Server wire protocol.
//!
//! All messages are JSON-serializable envelopes. Transport (TCP / WebSocket)
//! is left to the server/client crates.

use chrono::{DateTime, Utc};
use ctp_model::{
    AccountBalance, AccountCredentials, AccountId, AccountState, CancelRequest, ClientId,
    InstrumentId, Order, OrderRequest, Position, RequestId, Trade,
};
use serde::{Deserialize, Serialize};

/// Envelope wrapping every framed message on the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub request_id: RequestId,
    pub client_id: Option<ClientId>,
    pub ts: DateTime<Utc>,
    pub payload: Message,
}

impl Envelope {
    pub fn new(payload: Message) -> Self {
        Self {
            request_id: RequestId::new(uuid::Uuid::new_v4().to_string()),
            client_id: None,
            ts: Utc::now(),
            payload,
        }
    }

    pub fn with_client(mut self, client_id: ClientId) -> Self {
        self.client_id = Some(client_id);
        self
    }
}

/// Top-level message variants exchanged between Client and Server.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum Message {
    // --- Client → Server ---
    Hello(HelloRequest),
    AccountLogin(AccountLoginRequest),
    AccountLogout(AccountLogoutRequest),
    MarketDataHello(MarketDataHelloRequest),
    SubscribeMarketData(SubscribeMarketDataRequest),
    UnsubscribeMarketData(UnsubscribeMarketDataRequest),
    PlaceOrder(OrderRequest),
    CancelOrder(CancelRequest),
    QueryAccount(QueryAccountRequest),
    QueryPosition(QueryPositionRequest),
    QueryOrders(QueryOrdersRequest),
    QueryTrades(QueryTradesRequest),

    // --- Server → Client ---
    HelloAck(HelloAck),
    AccountStateUpdate(AccountState),
    MarketDataTick(MarketDataTickMessage),
    OrderUpdate(Order),
    TradeUpdate(Trade),
    QueryAccountResult(QueryAccountResult),
    QueryPositionResult(QueryPositionResult),
    QueryOrdersResult(QueryOrdersResult),
    QueryTradesResult(QueryTradesResult),
    Error(ErrorResponse),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloRequest {
    pub client_id: ClientId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloAck {
    pub accepted: bool,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountLoginRequest {
    pub credentials: AccountCredentials,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountLogoutRequest {
    pub account_id: AccountId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketDataHelloRequest {
    pub client_id: ClientId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeMarketDataRequest {
    pub instruments: Vec<InstrumentId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnsubscribeMarketDataRequest {
    pub instruments: Vec<InstrumentId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketDataTickMessage {
    pub instrument_id: InstrumentId,
    pub exchange_id: String,
    pub last_price: f64,
    pub volume: i32,
    pub turnover: f64,
    pub open_interest: f64,
    pub bid_price1: f64,
    pub bid_volume1: i32,
    pub ask_price1: f64,
    pub ask_volume1: i32,
    pub update_time: String,
    pub update_millisec: i32,
    pub trading_day: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryAccountRequest {
    pub account_id: AccountId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPositionRequest {
    pub account_id: AccountId,
    pub instrument_id: Option<InstrumentId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryOrdersRequest {
    pub account_id: AccountId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTradesRequest {
    pub account_id: AccountId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryAccountResult {
    pub balance: AccountBalance,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPositionResult {
    pub positions: Vec<Position>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryOrdersResult {
    pub orders: Vec<Order>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTradesResult {
    pub trades: Vec<Trade>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub code: i32,
    pub message: String,
}
