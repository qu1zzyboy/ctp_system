//! CTP FFI fields → domain model (server adapter).

use chrono::Utc;
use ctp2rs::ffi::WrapToString;
use ctp2rs::v1alpha1::{
    CThostFtdcInvestorPositionField, CThostFtdcOrderField, CThostFtdcTradeField,
    CThostFtdcTradingAccountField,
};
use ctp_model::{
    posi_direction_from_ctp, AccountBalance, AccountId, ClientId, InstrumentId, Order, OrderReport,
    OrderType, Position, Trade,
};

use super::trade::{order_field_to_report, trade_field_to_report};

pub fn trading_account_to_balance(account: &CThostFtdcTradingAccountField) -> AccountBalance {
    AccountBalance {
        account_id: AccountId::new(account.AccountID.try_to_string().unwrap_or_default()),
        balance: account.Balance,
        available: account.Available,
        curr_margin: account.CurrMargin,
        frozen_margin: account.FrozenMargin,
        commission: account.Commission,
        close_profit: account.CloseProfit,
        position_profit: account.PositionProfit,
    }
}

pub fn investor_position_to_position(
    pos: &CThostFtdcInvestorPositionField,
) -> anyhow::Result<Position> {
    let instrument_id = InstrumentId::new(pos.InstrumentID.try_to_string().unwrap_or_default());
    let direction = posi_direction_from_ctp(pos.PosiDirection as u8 as char)
        .ok_or_else(|| anyhow::anyhow!("unknown position direction {}", pos.PosiDirection))?;
    let account_id = AccountId::new(pos.InvestorID.try_to_string().unwrap_or_default());

    Ok(Position {
        account_id,
        instrument_id,
        direction,
        volume: pos.Position,
        yd_volume: pos.YdPosition,
        open_cost: pos.OpenCost,
        position_cost: pos.PositionCost,
        use_margin: pos.UseMargin,
        unrealized_pnl: pos.PositionProfit,
    })
}

pub fn order_field_to_wire_order(
    order: &CThostFtdcOrderField,
    account_id: AccountId,
    client_id: ClientId,
) -> anyhow::Result<Order> {
    let report = order_field_to_report(order)?;
    let now = Utc::now();
    Ok(Order {
        client_order_id: report.client_order_id,
        exchange_order_id: report.exchange_order_id,
        account_id: account_id.clone(),
        client_id,
        instrument_id: report.instrument_id,
        direction: report.direction,
        offset: report.offset,
        order_type: OrderType::Limit,
        volume: report.volume_total,
        volume_traded: report.volume_traded,
        price: report.price,
        status: report.status,
        status_msg: report.status_msg,
        inserted_at: now,
        updated_at: now,
    })
}

pub fn trade_field_to_wire_trade(
    trade: &CThostFtdcTradeField,
    account_id: AccountId,
) -> anyhow::Result<Trade> {
    Ok(trade_field_to_report(trade)?.into_trade(account_id))
}

pub fn order_report_to_wire_order(
    report: OrderReport,
    account_id: AccountId,
    client_id: ClientId,
) -> Order {
    let now = Utc::now();
    Order {
        client_order_id: report.client_order_id,
        exchange_order_id: report.exchange_order_id,
        account_id,
        client_id,
        instrument_id: report.instrument_id,
        direction: report.direction,
        offset: report.offset,
        order_type: OrderType::Limit,
        volume: report.volume_total,
        volume_traded: report.volume_traded,
        price: report.price,
        status: report.status,
        status_msg: report.status_msg,
        inserted_at: now,
        updated_at: now,
    }
}
