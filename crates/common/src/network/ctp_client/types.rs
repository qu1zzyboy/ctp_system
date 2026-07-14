//! CTP MD event types forwarded from SPI callbacks.

use serde::{Deserialize, Serialize};

/// Normalized depth tick extracted from `CThostFtdcDepthMarketDataField`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketDataTick {
    pub instrument_id: String,
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

/// Events emitted by [`super::CtpClient`] via its event channel.
#[derive(Debug, Clone)]
pub enum CtpEvent {
    FrontConnected,
    FrontDisconnected {
        reason: i32,
    },
    HeartBeatWarning {
        time_lapse: i32,
    },
    LoginOk {
        trading_day: String,
    },
    LoginFailed {
        error_id: i32,
        error_msg: String,
    },
    SubscribeOk {
        instrument_id: String,
    },
    SubscribeFailed {
        instrument_id: String,
        error_id: i32,
        error_msg: String,
    },
    MarketData(MarketDataTick),
    Error {
        error_id: i32,
        error_msg: String,
        request_id: i32,
    },
}
