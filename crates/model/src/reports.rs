//! Exchange report DTOs and order correlation context.
//!
//! Normalized structures produced by server CTP adapters and consumed by
//! routing logic, clients, and persistence. Transport-agnostic.

use chrono::{DateTime, Utc};

use crate::enums::{Direction, OffsetFlag, OrderStatus, OrderType};
use crate::identifiers::{AccountId, ClientId, ClientOrderId, ExchangeOrderId, InstrumentId};
use crate::types::{Order, Trade};

/// Server-side context captured when a client submits an order.
///
/// Used to correlate CTP `OrderRef` callbacks back to the originating client
/// and to fill fields that CTP returns only on the original request.
#[derive(Debug, Clone)]
pub struct OrderContext {
    pub client_id: ClientId,
    pub client_order_id: ClientOrderId,
    pub account_id: AccountId,
    pub instrument_id: InstrumentId,
    pub direction: Direction,
    pub offset: OffsetFlag,
    pub order_type: OrderType,
    pub price: f64,
    pub volume: i32,
    pub inserted_at: DateTime<Utc>,
}

impl OrderContext {
    pub fn order_ref_key(&self) -> String {
        crate::ctp::normalize_order_ref(self.client_order_id.as_str())
    }
}

/// Normalized order status update from the exchange (e.g. CTP `on_rtn_order`).
#[derive(Debug, Clone)]
pub struct OrderReport {
    pub client_order_id: ClientOrderId,
    pub exchange_order_id: Option<ExchangeOrderId>,
    pub instrument_id: InstrumentId,
    pub direction: Direction,
    pub offset: OffsetFlag,
    pub price: f64,
    pub volume_total: i32,
    pub volume_traded: i32,
    pub status: OrderStatus,
    pub status_msg: Option<String>,
    /// Exchange-provided insert/update time string (`YYYYMMDD HH:MM:SS` pieces).
    pub exchange_time: Option<String>,
}

impl OrderReport {
    /// Merge an exchange report with submit-time context into a wire [`Order`].
    pub fn into_order(self, ctx: &OrderContext) -> Order {
        let now = Utc::now();
        Order {
            client_order_id: self.client_order_id,
            exchange_order_id: self.exchange_order_id,
            account_id: ctx.account_id.clone(),
            client_id: ctx.client_id.clone(),
            instrument_id: self.instrument_id,
            direction: self.direction,
            offset: self.offset,
            order_type: ctx.order_type,
            volume: self.volume_total,
            volume_traded: self.volume_traded,
            price: self.price,
            status: self.status,
            status_msg: self.status_msg,
            inserted_at: ctx.inserted_at,
            updated_at: now,
        }
    }
}

/// Normalized trade/fill report from the exchange (e.g. CTP `on_rtn_trade`).
#[derive(Debug, Clone)]
pub struct TradeReport {
    pub trade_id: String,
    pub client_order_id: Option<ClientOrderId>,
    pub exchange_order_id: ExchangeOrderId,
    pub instrument_id: InstrumentId,
    pub direction: Direction,
    pub offset: OffsetFlag,
    pub price: f64,
    pub volume: i32,
    pub trade_date: String,
    pub trade_time: String,
}

impl TradeReport {
    /// Build a wire [`Trade`] for client push.
    pub fn into_trade(self, account_id: AccountId) -> Trade {
        let trade_time = parse_exchange_trade_time(&self.trade_date, &self.trade_time);
        Trade {
            trade_id: self.trade_id,
            exchange_order_id: self.exchange_order_id,
            client_order_id: self.client_order_id,
            account_id,
            instrument_id: self.instrument_id,
            direction: self.direction,
            offset: self.offset,
            price: self.price,
            volume: self.volume,
            trade_time,
        }
    }
}

fn parse_exchange_trade_time(trade_date: &str, trade_time: &str) -> DateTime<Utc> {
    let combined = format!("{trade_date} {trade_time}");
    chrono::NaiveDateTime::parse_from_str(&combined, "%Y%m%d %H:%M:%S")
        .map(|dt| dt.and_utc())
        .unwrap_or_else(|_| Utc::now())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enums::OrderType;

    fn sample_context() -> OrderContext {
        OrderContext {
            client_id: ClientId::new("client-1"),
            client_order_id: ClientOrderId::new("ord001"),
            account_id: AccountId::new("123456"),
            instrument_id: InstrumentId::new("rb2510"),
            direction: Direction::Buy,
            offset: OffsetFlag::Open,
            order_type: OrderType::Limit,
            price: 3500.0,
            volume: 2,
            inserted_at: Utc::now(),
        }
    }

    #[test]
    fn order_report_merges_context() {
        let ctx = sample_context();
        let report = OrderReport {
            client_order_id: ClientOrderId::new("ord001"),
            exchange_order_id: Some(ExchangeOrderId::new("999")),
            instrument_id: InstrumentId::new("rb2510"),
            direction: Direction::Buy,
            offset: OffsetFlag::Open,
            price: 3500.0,
            volume_total: 2,
            volume_traded: 1,
            status: OrderStatus::PartiallyFilled,
            status_msg: Some("partial".into()),
            exchange_time: None,
        };

        let order = report.into_order(&ctx);
        assert_eq!(order.client_id.as_str(), "client-1");
        assert_eq!(order.volume_traded, 1);
        assert_eq!(order.status, OrderStatus::PartiallyFilled);
    }

    #[test]
    fn trade_report_parses_time() {
        let trade = TradeReport {
            trade_id: "t1".into(),
            client_order_id: Some(ClientOrderId::new("ord001")),
            exchange_order_id: ExchangeOrderId::new("999"),
            instrument_id: InstrumentId::new("rb2510"),
            direction: Direction::Buy,
            offset: OffsetFlag::Open,
            price: 3500.0,
            volume: 1,
            trade_date: "20250713".into(),
            trade_time: "09:30:01".into(),
        }
        .into_trade(AccountId::new("123456"));

        assert_eq!(trade.trade_id, "t1");
        assert_eq!(trade.volume, 1);
    }
}
