//! CTP MD client implementation.

use std::{
    fmt::Debug,
    sync::{
        atomic::{AtomicI32, AtomicU8, Ordering},
        Arc, Mutex,
    },
};

use ctp2rs::ffi::{SetString, WrapToString};
use ctp2rs::v1alpha1::{
    CThostFtdcDepthMarketDataField, CThostFtdcReqUserLoginField, CThostFtdcRspInfoField,
    CThostFtdcRspUserLoginField, CThostFtdcSpecificInstrumentField, MdApi, MdApiBuilder, MdSpi,
};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use super::{CtpClientConfig, CtpEvent, MarketDataTick};
use crate::network::ConnectionMode;

/// Shared state visible to the SPI (CTP callback thread).
struct SharedState {
    mode: Arc<AtomicU8>,
    event_tx: mpsc::Sender<CtpEvent>,
    config: CtpClientConfig,
    request_id: AtomicI32,
    pending_instruments: Mutex<Vec<String>>,
}

unsafe impl Send for SharedState {}
unsafe impl Sync for SharedState {}

struct MdSpiHandler {
    shared: Arc<SharedState>,
}

impl MdSpiHandler {
    fn emit(&self, event: CtpEvent) {
        if let Err(e) = self.shared.event_tx.try_send(event) {
            warn!(error = %e, "ctp md event channel full or closed");
        }
    }
}

impl MdSpi for MdSpiHandler {
    fn on_front_connected(&mut self) {
        info!("ctp md front connected");
        self.shared
            .mode
            .store(ConnectionMode::Active.as_u8(), Ordering::SeqCst);
        self.emit(CtpEvent::FrontConnected);
    }

    fn on_front_disconnected(&mut self, n_reason: i32) {
        warn!(reason = n_reason, "ctp md front disconnected");
        self.shared
            .mode
            .store(ConnectionMode::Reconnect.as_u8(), Ordering::SeqCst);
        self.emit(CtpEvent::FrontDisconnected { reason: n_reason });
    }

    fn on_heart_beat_warning(&mut self, n_time_lapse: i32) {
        warn!(time_lapse = n_time_lapse, "ctp md heartbeat warning");
        self.emit(CtpEvent::HeartBeatWarning {
            time_lapse: n_time_lapse,
        });
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
                error!(error_id = info.ErrorID, %msg, "ctp md login failed");
                self.emit(CtpEvent::LoginFailed {
                    error_id: info.ErrorID,
                    error_msg: msg,
                });
                return;
            }
        }

        let trading_day = p_rsp_user_login
            .map(|r| r.TradingDay.try_to_string().unwrap_or_default())
            .unwrap_or_default();
        info!(%trading_day, "ctp md login ok");
        self.shared
            .mode
            .store(ConnectionMode::Active.as_u8(), Ordering::SeqCst);
        self.emit(CtpEvent::LoginOk { trading_day });
    }

    fn on_rsp_sub_market_data(
        &mut self,
        p_specific_instrument: Option<&CThostFtdcSpecificInstrumentField>,
        p_rsp_info: Option<&CThostFtdcRspInfoField>,
        _n_request_id: i32,
        _b_is_last: bool,
    ) {
        let instrument_id = p_specific_instrument
            .map(|i| i.InstrumentID.try_to_string().unwrap_or_default())
            .unwrap_or_default();

        if let Some(info) = p_rsp_info {
            if info.ErrorID != 0 {
                let msg = info.ErrorMsg.try_to_string().unwrap_or_default();
                warn!(%instrument_id, error_id = info.ErrorID, %msg, "subscribe failed");
                self.emit(CtpEvent::SubscribeFailed {
                    instrument_id,
                    error_id: info.ErrorID,
                    error_msg: msg,
                });
                return;
            }
        }

        debug!(%instrument_id, "subscribe ok");
        self.emit(CtpEvent::SubscribeOk { instrument_id });
    }

    fn on_rsp_error(
        &mut self,
        p_rsp_info: Option<&CThostFtdcRspInfoField>,
        n_request_id: i32,
        _b_is_last: bool,
    ) {
        if let Some(info) = p_rsp_info {
            let msg = info.ErrorMsg.try_to_string().unwrap_or_default();
            error!(error_id = info.ErrorID, %msg, request_id = n_request_id, "ctp md error");
            self.emit(CtpEvent::Error {
                error_id: info.ErrorID,
                error_msg: msg,
                request_id: n_request_id,
            });
        }
    }

    fn on_rtn_depth_market_data(
        &mut self,
        p_depth_market_data: Option<&CThostFtdcDepthMarketDataField>,
    ) {
        let Some(d) = p_depth_market_data else {
            return;
        };
        let tick = MarketDataTick {
            instrument_id: d.InstrumentID.try_to_string().unwrap_or_default(),
            exchange_id: d.ExchangeID.try_to_string().unwrap_or_default(),
            last_price: d.LastPrice,
            volume: d.Volume,
            turnover: d.Turnover,
            open_interest: d.OpenInterest,
            bid_price1: d.BidPrice1,
            bid_volume1: d.BidVolume1,
            ask_price1: d.AskPrice1,
            ask_volume1: d.AskVolume1,
            update_time: d.UpdateTime.try_to_string().unwrap_or_default(),
            update_millisec: d.UpdateMillisec,
            trading_day: d.TradingDay.try_to_string().unwrap_or_default(),
        };
        self.emit(CtpEvent::MarketData(tick));
    }
}

/// External CTP market-data client (process-wide: typically **1 MD**).
pub struct CtpClient {
    api: MdApi,
    shared: Arc<SharedState>,
    event_rx: mpsc::Receiver<CtpEvent>,
}

impl Debug for CtpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CtpClient")
            .field("mode", &self.connection_mode())
            .field("front", &self.shared.config.md_front)
            .finish()
    }
}

impl CtpClient {
    /// Create API, register SPI, register front, and `init()`.
    ///
    /// Does **not** block on login — poll [`Self::recv`] / [`Self::try_recv`]
    /// for [`CtpEvent::FrontConnected`], then call [`Self::login`].
    pub fn connect(config: CtpClientConfig) -> anyhow::Result<Self> {
        if let Some(parent) = config.flow_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        ctp2rs::ffi::check_make_dir(
            config
                .flow_path
                .parent()
                .and_then(|p| p.to_str())
                .unwrap_or("./flow"),
        );

        let api = MdApiBuilder::new()
            .with_dynlib(&config.dynlib_path)
            .flow_path(&config.flow_path)
            .using_udp(config.use_udp)
            .multicast(config.use_multicast)
            .build()
            .map_err(|e| anyhow::anyhow!("MdApiBuilder failed: {e}"))?;

        let (event_tx, event_rx) = mpsc::channel(config.event_buffer);
        let mode = Arc::new(AtomicU8::new(ConnectionMode::Reconnect.as_u8()));
        let pending = config.instruments.clone();

        let shared = Arc::new(SharedState {
            mode,
            event_tx,
            config: config.clone(),
            request_id: AtomicI32::new(0),
            pending_instruments: Mutex::new(pending),
        });

        let spi: Box<dyn MdSpi> = Box::new(MdSpiHandler {
            shared: shared.clone(),
        });
        api.register_spi(Box::into_raw(spi));
        api.register_front(&config.md_front);
        api.init();

        info!(front = %config.md_front, "ctp md client started");

        Ok(Self {
            api,
            shared,
            event_rx,
        })
    }

    #[must_use]
    pub fn connection_mode(&self) -> ConnectionMode {
        ConnectionMode::from_atomic(&self.shared.mode)
    }

    #[inline]
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.connection_mode().is_active()
    }

    /// Issue MD user login (call after [`CtpEvent::FrontConnected`]).
    pub fn login(&self) -> anyhow::Result<i32> {
        let mut req = CThostFtdcReqUserLoginField::default();
        req.BrokerID.set_str(&self.shared.config.broker_id);
        req.UserID.set_str(&self.shared.config.user_id);
        req.Password.set_str(&self.shared.config.password);

        let rid = self.shared.request_id.fetch_add(1, Ordering::SeqCst) + 1;
        let rc = self.api.req_user_login(&mut req, rid);
        if rc != 0 {
            anyhow::bail!("req_user_login returned {rc}");
        }
        info!(request_id = rid, "ctp md login requested");
        Ok(rid)
    }

    /// Subscribe market data for instruments.
    pub fn subscribe(&self, instruments: &[impl AsRef<str>]) -> anyhow::Result<i32> {
        let rc = self.api.subscribe_market_data(instruments);
        if rc != 0 {
            anyhow::bail!("subscribe_market_data returned {rc}");
        }
        Ok(rc)
    }

    /// Unsubscribe market data.
    pub fn unsubscribe(&self, instruments: &[impl AsRef<str>]) -> anyhow::Result<i32> {
        let rc = self.api.unsubscribe_market_data(instruments);
        if rc != 0 {
            anyhow::bail!("unsubscribe_market_data returned {rc}");
        }
        Ok(rc)
    }

    /// Subscribe instruments configured at connect time (after login).
    pub fn subscribe_configured(&self) -> anyhow::Result<()> {
        let instruments = self
            .shared
            .pending_instruments
            .lock()
            .map_err(|e| anyhow::anyhow!("lock poisoned: {e}"))?
            .clone();
        if instruments.is_empty() {
            return Ok(());
        }
        self.subscribe(&instruments)?;
        Ok(())
    }

    /// Async receive next CTP event.
    pub async fn recv(&mut self) -> Option<CtpEvent> {
        self.event_rx.recv().await
    }

    /// Non-blocking poll.
    pub fn try_recv(&mut self) -> Option<CtpEvent> {
        self.event_rx.try_recv().ok()
    }

    /// Release the underlying MD API.
    pub fn close(&self) {
        self.shared
            .mode
            .store(ConnectionMode::Disconnect.as_u8(), Ordering::SeqCst);
        self.api.release();
        self.shared
            .mode
            .store(ConnectionMode::Closed.as_u8(), Ordering::SeqCst);
        info!("ctp md client closed");
    }
}

impl Drop for CtpClient {
    fn drop(&mut self) {
        if !self.connection_mode().is_closed() {
            self.close();
        }
    }
}
