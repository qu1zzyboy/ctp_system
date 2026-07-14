//! CTP trade session (one per account: **N Trade**).

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicI32, Ordering},
    Arc, Mutex,
};

use ctp2rs::ffi::{SetString, WrapToString};
use ctp2rs::v1alpha1::{
    CThostFtdcInputOrderActionField, CThostFtdcInputOrderField, CThostFtdcInvestorPositionField,
    CThostFtdcOrderField, CThostFtdcQryInvestorPositionField, CThostFtdcQryOrderField,
    CThostFtdcQryTradeField, CThostFtdcQryTradingAccountField, CThostFtdcReqAuthenticateField,
    CThostFtdcReqUserLoginField, CThostFtdcRspAuthenticateField, CThostFtdcRspInfoField,
    CThostFtdcRspUserLoginField, CThostFtdcSettlementInfoConfirmField, CThostFtdcTradeField,
    CThostFtdcTradingAccountField, THOST_FTDC_AF_Delete, THOST_FTDC_CC_Immediately,
    THOST_FTDC_D_Buy, THOST_FTDC_D_Sell, THOST_FTDC_FCC_NotForceClose, THOST_FTDC_OPT_AnyPrice,
    THOST_FTDC_OPT_LimitPrice, TraderApi, TraderApiBuilder, TraderSpi, THOST_FTDC_TC_GFD,
    THOST_FTDC_TC_IOC, THOST_FTDC_VC_AV, THOST_FTDC_VC_CV, THOST_TE_RESUME_TYPE,
};
use ctp_model::{
    direction_from_ctp, normalize_order_ref, offset_from_ctp, order_status_from_ctp,
    AccountBalance, CancelRequest, ClientOrderId, Direction, ExchangeOrderId, InstrumentId,
    OffsetFlag, OrderReport, OrderRequest, OrderType, Position, TradeReport,
};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// Per-account trade front configuration.
#[derive(Debug, Clone)]
pub struct TradeSessionConfig {
    pub account_id: String,
    pub broker_id: String,
    pub password: String,
    pub app_id: String,
    pub auth_code: String,
    pub trade_front: String,
    pub dynlib_path: std::path::PathBuf,
    pub flow_path: std::path::PathBuf,
}

#[derive(Debug, Clone)]
pub enum TradeSessionEvent {
    FrontConnected,
    FrontDisconnected {
        reason: i32,
    },
    HeartBeatWarning {
        time_lapse: i32,
    },
    AuthenticateOk,
    AuthenticateFailed {
        error_id: i32,
        error_msg: String,
    },
    LoginOk {
        trading_day: String,
        front_id: i32,
        session_id: i32,
    },
    LoginFailed {
        error_id: i32,
        error_msg: String,
    },
    SettlementConfirmed,
    SettlementConfirmFailed {
        error_id: i32,
        error_msg: String,
    },
    Error {
        error_id: i32,
        error_msg: String,
        request_id: i32,
    },
    OrderInsertRejected {
        error_id: i32,
        error_msg: String,
    },
    /// Normalized order status push (`on_rtn_order`).
    OrderReturn(OrderReport),
    /// Normalized trade/fill push (`on_rtn_trade`).
    TradeReturn(TradeReport),
    QueryAccountReady {
        request_id: i32,
        balance: AccountBalance,
    },
    QueryPositionsReady {
        request_id: i32,
        positions: Vec<Position>,
    },
    QueryOrdersReady {
        request_id: i32,
        orders: Vec<OrderReport>,
    },
    QueryTradesReady {
        request_id: i32,
        trades: Vec<TradeReport>,
    },
    QueryFailed {
        request_id: i32,
        error_id: i32,
        error_msg: String,
    },
    CancelActionOk {
        request_id: i32,
    },
    CancelActionFailed {
        request_id: i32,
        error_id: i32,
        error_msg: String,
    },
}

#[derive(Debug)]
enum PendingQuery {
    Account,
    Positions(Vec<Position>),
    Orders(Vec<OrderReport>),
    Trades(Vec<TradeReport>),
    Cancel,
}

#[derive(Debug)]
struct SharedState {
    event_tx: mpsc::UnboundedSender<TradeSessionEvent>,
    pending_queries: Mutex<HashMap<i32, PendingQuery>>,
}

unsafe impl Send for SharedState {}
unsafe impl Sync for SharedState {}

struct TraderSpiHandler {
    shared: Arc<SharedState>,
}

impl TraderSpiHandler {
    fn emit(&self, event: TradeSessionEvent) {
        if self.shared.event_tx.send(event).is_err() {
            warn!("trade session event receiver closed");
        }
    }

    fn query_error(&self, request_id: i32, info: &CThostFtdcRspInfoField) {
        let msg = info.ErrorMsg.try_to_string().unwrap_or_default();
        self.shared.finish_query(
            request_id,
            TradeSessionEvent::QueryFailed {
                request_id,
                error_id: info.ErrorID,
                error_msg: msg,
            },
        );
    }
}

impl SharedState {
    fn register_query(&self, request_id: i32, kind: PendingQuery) {
        self.pending_queries
            .lock()
            .unwrap()
            .insert(request_id, kind);
    }

    fn finish_query(&self, request_id: i32, event: TradeSessionEvent) {
        self.pending_queries.lock().unwrap().remove(&request_id);
        let _ = self.event_tx.send(event);
    }

    fn append_position(&self, request_id: i32, position: Position) {
        let mut guard = self.pending_queries.lock().unwrap();
        if let Some(PendingQuery::Positions(items)) = guard.get_mut(&request_id) {
            items.push(position);
        }
    }

    fn append_order(&self, request_id: i32, order: OrderReport) {
        let mut guard = self.pending_queries.lock().unwrap();
        if let Some(PendingQuery::Orders(items)) = guard.get_mut(&request_id) {
            items.push(order);
        }
    }

    fn append_trade(&self, request_id: i32, trade: TradeReport) {
        let mut guard = self.pending_queries.lock().unwrap();
        if let Some(PendingQuery::Trades(items)) = guard.get_mut(&request_id) {
            items.push(trade);
        }
    }

    fn take_positions(&self, request_id: i32) -> Vec<Position> {
        let mut guard = self.pending_queries.lock().unwrap();
        match guard.remove(&request_id) {
            Some(PendingQuery::Positions(items)) => items,
            _ => Vec::new(),
        }
    }

    fn take_orders(&self, request_id: i32) -> Vec<OrderReport> {
        let mut guard = self.pending_queries.lock().unwrap();
        match guard.remove(&request_id) {
            Some(PendingQuery::Orders(items)) => items,
            _ => Vec::new(),
        }
    }

    fn take_trades(&self, request_id: i32) -> Vec<TradeReport> {
        let mut guard = self.pending_queries.lock().unwrap();
        match guard.remove(&request_id) {
            Some(PendingQuery::Trades(items)) => items,
            _ => Vec::new(),
        }
    }
}

impl TraderSpi for TraderSpiHandler {
    fn on_front_connected(&mut self) {
        info!("ctp trade front connected");
        self.emit(TradeSessionEvent::FrontConnected);
    }

    fn on_front_disconnected(&mut self, n_reason: i32) {
        warn!(reason = n_reason, "ctp trade front disconnected");
        self.emit(TradeSessionEvent::FrontDisconnected { reason: n_reason });
    }

    fn on_heart_beat_warning(&mut self, n_time_lapse: i32) {
        warn!(time_lapse = n_time_lapse, "ctp trade heartbeat warning");
        self.emit(TradeSessionEvent::HeartBeatWarning {
            time_lapse: n_time_lapse,
        });
    }

    fn on_rsp_authenticate(
        &mut self,
        _p_rsp_authenticate_field: Option<&CThostFtdcRspAuthenticateField>,
        p_rsp_info: Option<&CThostFtdcRspInfoField>,
        _n_request_id: i32,
        _b_is_last: bool,
    ) {
        if let Some(info) = p_rsp_info {
            if info.ErrorID != 0 {
                let msg = info.ErrorMsg.try_to_string().unwrap_or_default();
                error!(error_id = info.ErrorID, %msg, "ctp trade authenticate failed");
                self.emit(TradeSessionEvent::AuthenticateFailed {
                    error_id: info.ErrorID,
                    error_msg: msg,
                });
                return;
            }
        }
        info!("ctp trade authenticate ok");
        self.emit(TradeSessionEvent::AuthenticateOk);
    }

    fn on_rsp_user_login(
        &mut self,
        p_rsp_user_login: Option<&CThostFtdcRspUserLoginField>,
        p_rsp_info: Option<&CThostFtdcRspInfoField>,
        _n_request_id: i32,
        _b_is_last: bool,
    ) {
        if let Some(info) = p_rsp_info {
            if info.ErrorID != 0 {
                let msg = info.ErrorMsg.try_to_string().unwrap_or_default();
                error!(error_id = info.ErrorID, %msg, "ctp trade login failed");
                self.emit(TradeSessionEvent::LoginFailed {
                    error_id: info.ErrorID,
                    error_msg: msg,
                });
                return;
            }
        }

        let trading_day = p_rsp_user_login
            .map(|r| r.TradingDay.to_string())
            .unwrap_or_default();
        let front_id = p_rsp_user_login.map(|r| r.FrontID).unwrap_or_default();
        let session_id = p_rsp_user_login.map(|r| r.SessionID).unwrap_or_default();
        info!(%trading_day, front_id, session_id, "ctp trade login ok");
        self.emit(TradeSessionEvent::LoginOk {
            trading_day,
            front_id,
            session_id,
        });
    }

    fn on_rsp_error(
        &mut self,
        p_rsp_info: Option<&CThostFtdcRspInfoField>,
        n_request_id: i32,
        _b_is_last: bool,
    ) {
        if let Some(info) = p_rsp_info {
            let msg = info.ErrorMsg.try_to_string().unwrap_or_default();
            error!(error_id = info.ErrorID, %msg, request_id = n_request_id, "ctp trade error");
            self.emit(TradeSessionEvent::Error {
                error_id: info.ErrorID,
                error_msg: msg,
                request_id: n_request_id,
            });
        }
    }

    fn on_rsp_order_insert(
        &mut self,
        _p_input_order: Option<&CThostFtdcInputOrderField>,
        p_rsp_info: Option<&CThostFtdcRspInfoField>,
        _n_request_id: i32,
        _b_is_last: bool,
    ) {
        if let Some(info) = p_rsp_info {
            if info.ErrorID != 0 {
                let msg = info.ErrorMsg.try_to_string().unwrap_or_default();
                error!(error_id = info.ErrorID, %msg, "ctp trade order insert rejected");
                self.emit(TradeSessionEvent::OrderInsertRejected {
                    error_id: info.ErrorID,
                    error_msg: msg,
                });
            }
        }
    }

    fn on_err_rtn_order_insert(
        &mut self,
        _p_input_order: Option<&CThostFtdcInputOrderField>,
        p_rsp_info: Option<&CThostFtdcRspInfoField>,
    ) {
        if let Some(info) = p_rsp_info {
            let msg = info.ErrorMsg.try_to_string().unwrap_or_default();
            error!(error_id = info.ErrorID, %msg, "ctp trade order insert error return");
            self.emit(TradeSessionEvent::OrderInsertRejected {
                error_id: info.ErrorID,
                error_msg: msg,
            });
        }
    }

    fn on_rsp_settlement_info_confirm(
        &mut self,
        _p_settlement_info_confirm: Option<&CThostFtdcSettlementInfoConfirmField>,
        p_rsp_info: Option<&CThostFtdcRspInfoField>,
        _n_request_id: i32,
        _b_is_last: bool,
    ) {
        if let Some(info) = p_rsp_info {
            if info.ErrorID != 0 {
                let msg = info.ErrorMsg.try_to_string().unwrap_or_default();
                error!(error_id = info.ErrorID, %msg, "ctp trade settlement confirm failed");
                self.emit(TradeSessionEvent::SettlementConfirmFailed {
                    error_id: info.ErrorID,
                    error_msg: msg,
                });
                return;
            }
        }
        info!("ctp trade settlement confirmed");
        self.emit(TradeSessionEvent::SettlementConfirmed);
    }

    fn on_rtn_order(&mut self, p_order: Option<&CThostFtdcOrderField>) {
        let Some(order) = p_order else {
            return;
        };
        match order_field_to_report(order) {
            Ok(report) => {
                info!(
                    client_order_id = %report.client_order_id,
                    ?report.status,
                    volume_traded = report.volume_traded,
                    "ctp trade order return"
                );
                self.emit(TradeSessionEvent::OrderReturn(report));
            }
            Err(e) => warn!(error = %e, "ctp trade order return parse failed"),
        }
    }

    fn on_rtn_trade(&mut self, p_trade: Option<&CThostFtdcTradeField>) {
        let Some(trade) = p_trade else {
            return;
        };
        match trade_field_to_report(trade) {
            Ok(report) => {
                info!(
                    trade_id = %report.trade_id,
                    instrument = %report.instrument_id,
                    volume = report.volume,
                    price = report.price,
                    "ctp trade fill return"
                );
                self.emit(TradeSessionEvent::TradeReturn(report));
            }
            Err(e) => warn!(error = %e, "ctp trade fill return parse failed"),
        }
    }

    fn on_rsp_order_action(
        &mut self,
        _p_input_order_action: Option<&CThostFtdcInputOrderActionField>,
        p_rsp_info: Option<&CThostFtdcRspInfoField>,
        n_request_id: i32,
        _b_is_last: bool,
    ) {
        if let Some(info) = p_rsp_info {
            if info.ErrorID != 0 {
                let msg = info.ErrorMsg.try_to_string().unwrap_or_default();
                self.shared.finish_query(
                    n_request_id,
                    TradeSessionEvent::CancelActionFailed {
                        request_id: n_request_id,
                        error_id: info.ErrorID,
                        error_msg: msg,
                    },
                );
                return;
            }
        }
        self.shared.finish_query(
            n_request_id,
            TradeSessionEvent::CancelActionOk {
                request_id: n_request_id,
            },
        );
    }

    fn on_rsp_qry_trading_account(
        &mut self,
        p_trading_account: Option<&CThostFtdcTradingAccountField>,
        p_rsp_info: Option<&CThostFtdcRspInfoField>,
        n_request_id: i32,
        b_is_last: bool,
    ) {
        if let Some(info) = p_rsp_info {
            if info.ErrorID != 0 {
                self.query_error(n_request_id, info);
                return;
            }
        }
        if b_is_last {
            self.shared
                .pending_queries
                .lock()
                .unwrap()
                .remove(&n_request_id);
            let balance = p_trading_account
                .map(super::mapping::trading_account_to_balance)
                .unwrap_or_else(|| AccountBalance {
                    account_id: ctp_model::AccountId::new(""),
                    balance: 0.0,
                    available: 0.0,
                    curr_margin: 0.0,
                    frozen_margin: 0.0,
                    commission: 0.0,
                    close_profit: 0.0,
                    position_profit: 0.0,
                });
            self.emit(TradeSessionEvent::QueryAccountReady {
                request_id: n_request_id,
                balance,
            });
        }
    }

    fn on_rsp_qry_investor_position(
        &mut self,
        p_investor_position: Option<&CThostFtdcInvestorPositionField>,
        p_rsp_info: Option<&CThostFtdcRspInfoField>,
        n_request_id: i32,
        b_is_last: bool,
    ) {
        if let Some(info) = p_rsp_info {
            if info.ErrorID != 0 {
                self.query_error(n_request_id, info);
                return;
            }
        }
        if let Some(pos) = p_investor_position {
            if let Ok(position) = super::mapping::investor_position_to_position(pos) {
                if position.volume != 0 {
                    self.shared.append_position(n_request_id, position);
                }
            }
        }
        if b_is_last {
            let positions = self.shared.take_positions(n_request_id);
            self.emit(TradeSessionEvent::QueryPositionsReady {
                request_id: n_request_id,
                positions,
            });
        }
    }

    fn on_rsp_qry_order(
        &mut self,
        p_order: Option<&CThostFtdcOrderField>,
        p_rsp_info: Option<&CThostFtdcRspInfoField>,
        n_request_id: i32,
        b_is_last: bool,
    ) {
        if let Some(info) = p_rsp_info {
            if info.ErrorID != 0 {
                self.query_error(n_request_id, info);
                return;
            }
        }
        if let Some(order) = p_order {
            if let Ok(report) = order_field_to_report(order) {
                self.shared.append_order(n_request_id, report);
            }
        }
        if b_is_last {
            let orders = self.shared.take_orders(n_request_id);
            self.emit(TradeSessionEvent::QueryOrdersReady {
                request_id: n_request_id,
                orders,
            });
        }
    }

    fn on_rsp_qry_trade(
        &mut self,
        p_trade: Option<&CThostFtdcTradeField>,
        p_rsp_info: Option<&CThostFtdcRspInfoField>,
        n_request_id: i32,
        b_is_last: bool,
    ) {
        if let Some(info) = p_rsp_info {
            if info.ErrorID != 0 {
                self.query_error(n_request_id, info);
                return;
            }
        }
        if let Some(trade) = p_trade {
            if let Ok(report) = trade_field_to_report(trade) {
                self.shared.append_trade(n_request_id, report);
            }
        }
        if b_is_last {
            let trades = self.shared.take_trades(n_request_id);
            self.emit(TradeSessionEvent::QueryTradesReady {
                request_id: n_request_id,
                trades,
            });
        }
    }
}

/// One CTP TraderApi connection.
#[derive(Debug)]
pub struct TradeSession {
    pub config: TradeSessionConfig,
    api: Option<TraderApi>,
    shared: Option<Arc<SharedState>>,
    event_rx: mpsc::UnboundedReceiver<TradeSessionEvent>,
    event_tx: mpsc::UnboundedSender<TradeSessionEvent>,
    request_id: AtomicI32,
}

impl TradeSession {
    pub fn new(config: TradeSessionConfig) -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        info!(
            account = %config.account_id,
            front = %config.trade_front,
            "exchange.trade session created (not connected)"
        );
        Self {
            config,
            api: None,
            shared: None,
            event_rx,
            event_tx,
            request_id: AtomicI32::new(0),
        }
    }

    pub fn connect(&mut self) -> anyhow::Result<()> {
        if let Some(parent) = self.config.flow_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        ctp2rs::ffi::check_make_dir(
            self.config
                .flow_path
                .parent()
                .and_then(|p| p.to_str())
                .unwrap_or("./flow"),
        );

        let api = TraderApiBuilder::new()
            .with_dynlib(&self.config.dynlib_path)
            .flow_path(&self.config.flow_path)
            .build()
            .map_err(|e| anyhow::anyhow!("TraderApiBuilder failed: {e}"))?;

        let shared = Arc::new(SharedState {
            event_tx: self.event_tx.clone(),
            pending_queries: Mutex::new(HashMap::new()),
        });
        let spi: Box<dyn TraderSpi> = Box::new(TraderSpiHandler {
            shared: shared.clone(),
        });
        api.register_spi(Box::into_raw(spi));
        api.subscribe_private_topic(THOST_TE_RESUME_TYPE::THOST_TERT_QUICK, 0);
        api.subscribe_public_topic(THOST_TE_RESUME_TYPE::THOST_TERT_QUICK);
        api.register_front(&self.config.trade_front);
        api.init();

        info!(
            account = %self.config.account_id,
            front = %self.config.trade_front,
            "ctp trade client started"
        );
        self.shared = Some(shared);
        self.api = Some(api);
        Ok(())
    }

    /// Issue client authentication. Call after [`TradeSessionEvent::FrontConnected`].
    pub fn authenticate(&self) -> anyhow::Result<i32> {
        let mut req = CThostFtdcReqAuthenticateField::default();
        req.BrokerID.set_str(&self.config.broker_id);
        req.UserID.set_str(&self.config.account_id);
        req.UserProductInfo.set_str("quantSystem");
        req.AuthCode.set_str(&self.config.auth_code);
        req.AppID.set_str(&self.config.app_id);

        let rid = self.next_request_id();
        let rc = self.api()?.req_authenticate(&mut req, rid);
        if rc != 0 {
            anyhow::bail!("req_authenticate returned {rc}");
        }
        info!(account = %self.config.account_id, request_id = rid, "ctp trade authenticate requested");
        Ok(rid)
    }

    /// Issue user login. Call after authentication succeeds.
    pub fn login(&self) -> anyhow::Result<i32> {
        let mut req = CThostFtdcReqUserLoginField::default();
        req.BrokerID.set_str(&self.config.broker_id);
        req.UserID.set_str(&self.config.account_id);
        req.Password.set_str(&self.config.password);
        req.UserProductInfo.set_str("quantSystem");

        let rid = self.next_request_id();
        let rc = self.api()?.req_user_login(&mut req, rid);
        if rc != 0 {
            anyhow::bail!("req_user_login returned {rc}");
        }
        info!(account = %self.config.account_id, request_id = rid, "ctp trade login requested");
        Ok(rid)
    }

    pub fn confirm_settlement(&self) -> anyhow::Result<i32> {
        let mut req = CThostFtdcSettlementInfoConfirmField::default();
        req.BrokerID.set_str(&self.config.broker_id);
        req.InvestorID.set_str(&self.config.account_id);
        req.AccountID.set_str(&self.config.account_id);

        let rid = self.next_request_id();
        let rc = self.api()?.req_settlement_info_confirm(&mut req, rid);
        if rc != 0 {
            anyhow::bail!("req_settlement_info_confirm returned {rc}");
        }
        info!(account = %self.config.account_id, request_id = rid, "ctp trade settlement confirm requested");
        Ok(rid)
    }

    pub fn insert_order(&self, order: &OrderRequest) -> anyhow::Result<i32> {
        let mut req = CThostFtdcInputOrderField::default();
        req.BrokerID.set_str(&self.config.broker_id);
        req.InvestorID.set_str(&self.config.account_id);
        req.UserID.set_str(&self.config.account_id);
        req.AccountID.set_str(&self.config.account_id);
        req.InstrumentID.set_str(order.instrument_id.as_str());
        req.OrderRef
            .set_str(&normalize_order_ref(order.client_order_id.as_str()));
        req.OrderPriceType = order_price_type(order.order_type);
        req.Direction = direction(order.direction);
        req.CombOffsetFlag.set_str(offset_flag(order.offset));
        req.CombHedgeFlag.set_str("1");
        req.LimitPrice = order.price;
        req.VolumeTotalOriginal = order.volume;
        req.TimeCondition = time_condition(order.order_type);
        req.VolumeCondition = volume_condition(order.order_type);
        req.MinVolume = 1;
        req.ContingentCondition = THOST_FTDC_CC_Immediately as _;
        req.ForceCloseReason = THOST_FTDC_FCC_NotForceClose as _;
        req.IsAutoSuspend = 0;
        req.UserForceClose = 0;
        req.IsSwapOrder = 0;

        let rid = self.next_request_id();
        req.RequestID = rid;
        let rc = self.api()?.req_order_insert(&mut req, rid);
        if rc != 0 {
            anyhow::bail!("req_order_insert returned {rc}");
        }
        info!(
            account = %self.config.account_id,
            instrument = %order.instrument_id,
            direction = ?order.direction,
            offset = ?order.offset,
            price = order.price,
            volume = order.volume,
            request_id = rid,
            "ctp trade order insert requested"
        );
        Ok(rid)
    }

    pub fn cancel_order(
        &self,
        cancel: &CancelRequest,
        front_id: i32,
        session_id: i32,
    ) -> anyhow::Result<i32> {
        let mut req = CThostFtdcInputOrderActionField::default();
        req.BrokerID.set_str(&self.config.broker_id);
        req.InvestorID.set_str(&self.config.account_id);
        req.UserID.set_str(&self.config.account_id);
        req.FrontID = front_id;
        req.SessionID = session_id;
        req.ActionFlag = THOST_FTDC_AF_Delete as _;
        req.InstrumentID.set_str(cancel.instrument_id.as_str());
        if let Some(exchange_order_id) = &cancel.exchange_order_id {
            req.OrderSysID.set_str(exchange_order_id.as_str());
        }
        if let Some(client_order_id) = &cancel.client_order_id {
            req.OrderRef
                .set_str(&normalize_order_ref(client_order_id.as_str()));
        }

        let rid = self.next_request_id();
        req.RequestID = rid;
        if let Some(shared) = self.shared_state() {
            shared.register_query(rid, PendingQuery::Cancel);
        }
        let rc = self.api()?.req_order_action(&mut req, rid);
        if rc != 0 {
            anyhow::bail!("req_order_action returned {rc}");
        }
        info!(
            account = %self.config.account_id,
            instrument = %cancel.instrument_id,
            request_id = rid,
            "ctp trade cancel requested"
        );
        Ok(rid)
    }

    pub fn query_trading_account(&self) -> anyhow::Result<i32> {
        let mut req = CThostFtdcQryTradingAccountField::default();
        req.BrokerID.set_str(&self.config.broker_id);
        req.InvestorID.set_str(&self.config.account_id);
        req.CurrencyID.set_str("CNY");
        let rid = self.next_request_id();
        if let Some(shared) = self.shared_state() {
            shared.register_query(rid, PendingQuery::Account);
        }
        let rc = self.api()?.req_qry_trading_account(&mut req, rid);
        if rc != 0 {
            anyhow::bail!("req_qry_trading_account returned {rc}");
        }
        Ok(rid)
    }

    pub fn query_investor_position(&self, instrument_id: Option<&str>) -> anyhow::Result<i32> {
        let mut req = CThostFtdcQryInvestorPositionField::default();
        req.BrokerID.set_str(&self.config.broker_id);
        req.InvestorID.set_str(&self.config.account_id);
        if let Some(instrument_id) = instrument_id {
            req.InstrumentID.set_str(instrument_id);
        }
        let rid = self.next_request_id();
        if let Some(shared) = self.shared_state() {
            shared.register_query(rid, PendingQuery::Positions(Vec::new()));
        }
        let rc = self.api()?.req_qry_investor_position(&mut req, rid);
        if rc != 0 {
            anyhow::bail!("req_qry_investor_position returned {rc}");
        }
        Ok(rid)
    }

    pub fn query_orders(&self, instrument_id: Option<&str>) -> anyhow::Result<i32> {
        let mut req = CThostFtdcQryOrderField::default();
        req.BrokerID.set_str(&self.config.broker_id);
        req.InvestorID.set_str(&self.config.account_id);
        if let Some(instrument_id) = instrument_id {
            req.InstrumentID.set_str(instrument_id);
        }
        let rid = self.next_request_id();
        if let Some(shared) = self.shared_state() {
            shared.register_query(rid, PendingQuery::Orders(Vec::new()));
        }
        let rc = self.api()?.req_qry_order(&mut req, rid);
        if rc != 0 {
            anyhow::bail!("req_qry_order returned {rc}");
        }
        Ok(rid)
    }

    pub fn query_trades(&self, instrument_id: Option<&str>) -> anyhow::Result<i32> {
        let mut req = CThostFtdcQryTradeField::default();
        req.BrokerID.set_str(&self.config.broker_id);
        req.InvestorID.set_str(&self.config.account_id);
        if let Some(instrument_id) = instrument_id {
            req.InstrumentID.set_str(instrument_id);
        }
        let rid = self.next_request_id();
        if let Some(shared) = self.shared_state() {
            shared.register_query(rid, PendingQuery::Trades(Vec::new()));
        }
        let rc = self.api()?.req_qry_trade(&mut req, rid);
        if rc != 0 {
            anyhow::bail!("req_qry_trade returned {rc}");
        }
        Ok(rid)
    }

    pub async fn recv(&mut self) -> Option<TradeSessionEvent> {
        self.event_rx.recv().await
    }

    pub fn try_recv(&mut self) -> Option<TradeSessionEvent> {
        self.event_rx.try_recv().ok()
    }

    pub fn account_id(&self) -> &str {
        &self.config.account_id
    }

    fn next_request_id(&self) -> i32 {
        self.request_id.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn api(&self) -> anyhow::Result<&TraderApi> {
        self.api
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Trade session is not connected"))
    }

    fn shared_state(&self) -> Option<&Arc<SharedState>> {
        self.shared.as_ref()
    }
}

fn direction(direction: Direction) -> i8 {
    match direction {
        Direction::Buy => THOST_FTDC_D_Buy as _,
        Direction::Sell => THOST_FTDC_D_Sell as _,
    }
}

fn offset_flag(offset: OffsetFlag) -> &'static str {
    match offset {
        OffsetFlag::Open => "0",
        OffsetFlag::Close => "1",
        OffsetFlag::CloseToday => "3",
        OffsetFlag::CloseYesterday => "4",
    }
}

fn order_price_type(order_type: OrderType) -> i8 {
    match order_type {
        OrderType::Market => THOST_FTDC_OPT_AnyPrice as _,
        OrderType::Limit | OrderType::FAK | OrderType::FOK => THOST_FTDC_OPT_LimitPrice as _,
    }
}

fn time_condition(order_type: OrderType) -> i8 {
    match order_type {
        OrderType::FAK | OrderType::FOK => THOST_FTDC_TC_IOC as _,
        OrderType::Limit | OrderType::Market => THOST_FTDC_TC_GFD as _,
    }
}

fn volume_condition(order_type: OrderType) -> i8 {
    match order_type {
        OrderType::FOK => THOST_FTDC_VC_CV as _,
        OrderType::Limit | OrderType::Market | OrderType::FAK => THOST_FTDC_VC_AV as _,
    }
}

pub(crate) fn order_field_to_report(order: &CThostFtdcOrderField) -> anyhow::Result<OrderReport> {
    let order_ref = order.OrderRef.try_to_string().unwrap_or_default();
    let client_order_id = ClientOrderId::new(order_ref);
    let exchange_order_id = {
        let sys_id = order.OrderSysID.try_to_string().unwrap_or_default();
        if sys_id.is_empty() {
            None
        } else {
            Some(ExchangeOrderId::new(sys_id))
        }
    };
    let instrument_id = InstrumentId::new(order.InstrumentID.try_to_string().unwrap_or_default());
    let direction = direction_from_ctp(order.Direction)
        .ok_or_else(|| anyhow::anyhow!("unknown CTP direction {}", order.Direction))?;
    let offset = offset_from_ctp(first_char(&order.CombOffsetFlag));
    let status = order_status_from_ctp(order.OrderStatus as u8 as char);
    let status_msg = {
        let msg = order.StatusMsg.try_to_string().unwrap_or_default();
        if msg.is_empty() {
            None
        } else {
            Some(msg)
        }
    };
    let insert_date = order.InsertDate.try_to_string().unwrap_or_default();
    let update_time = order.UpdateTime.try_to_string().unwrap_or_default();
    let exchange_time = if insert_date.is_empty() && update_time.is_empty() {
        None
    } else {
        Some(format!("{insert_date} {update_time}"))
    };

    Ok(OrderReport {
        client_order_id,
        exchange_order_id,
        instrument_id,
        direction,
        offset,
        price: order.LimitPrice,
        volume_total: order.VolumeTotalOriginal,
        volume_traded: order.VolumeTraded,
        status,
        status_msg,
        exchange_time,
    })
}

pub(crate) fn trade_field_to_report(trade: &CThostFtdcTradeField) -> anyhow::Result<TradeReport> {
    let order_ref = trade.OrderRef.try_to_string().unwrap_or_default();
    let client_order_id = if order_ref.is_empty() {
        None
    } else {
        Some(ClientOrderId::new(order_ref))
    };
    let exchange_order_id =
        ExchangeOrderId::new(trade.OrderSysID.try_to_string().unwrap_or_default());
    let instrument_id = InstrumentId::new(trade.InstrumentID.try_to_string().unwrap_or_default());
    let direction = direction_from_ctp(trade.Direction)
        .ok_or_else(|| anyhow::anyhow!("unknown CTP direction {}", trade.Direction))?;
    let offset = offset_from_ctp(trade.OffsetFlag as u8 as char);

    Ok(TradeReport {
        trade_id: trade.TradeID.try_to_string().unwrap_or_default(),
        client_order_id,
        exchange_order_id,
        instrument_id,
        direction,
        offset,
        price: trade.Price,
        volume: trade.Volume,
        trade_date: trade.TradeDate.try_to_string().unwrap_or_default(),
        trade_time: trade.TradeTime.try_to_string().unwrap_or_default(),
    })
}

pub(crate) fn first_char(bytes: &[i8]) -> char {
    bytes.first().map(|b| *b as u8 as char).unwrap_or('0')
}
