//! Shared actor state and command types.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;

use chrono::Utc;
use ctp_common::{Envelope, Message};
use ctp_model::{
    normalize_order_ref, AccountCredentials, AccountId, AccountState, CancelRequest, ClientId,
    ConnectionState, InstrumentId, OrderContext, OrderReport, OrderRequest, TradeReport,
};
use tokio::sync::{mpsc, Mutex};
use tracing::{info, warn};

use crate::config::ServerConfig;
use crate::session::SessionManager;

use super::data::MarketDataSessionActor;
use super::trade::TradeSessionActor;

pub type EnvelopeTx = mpsc::UnboundedSender<Envelope>;
pub type SharedServerActorState = Arc<Mutex<ServerActorState>>;

#[derive(Debug, Clone)]
pub struct TradingClientActor {
    pub client_id: ClientId,
    pub peer: SocketAddr,
    pub outbound: EnvelopeTx,
}

#[derive(Debug, Clone)]
pub struct MarketDataClientActor {
    pub client_id: ClientId,
    pub peer: SocketAddr,
    pub outbound: EnvelopeTx,
}

#[derive(Debug)]
pub enum TradeSessionCommand {
    Login {
        credentials: AccountCredentials,
        reply_to: EnvelopeTx,
    },
    PlaceOrder {
        client_id: ClientId,
        order: OrderRequest,
        reply_to: EnvelopeTx,
    },
    CancelOrder {
        cancel: CancelRequest,
        reply_to: EnvelopeTx,
    },
    QueryAccount {
        account_id: AccountId,
        reply_to: EnvelopeTx,
    },
    QueryPosition {
        account_id: AccountId,
        instrument_id: Option<InstrumentId>,
        reply_to: EnvelopeTx,
    },
    QueryOrders {
        account_id: AccountId,
        reply_to: EnvelopeTx,
    },
    QueryTrades {
        account_id: AccountId,
        reply_to: EnvelopeTx,
    },
    Logout {
        reply_to: EnvelopeTx,
    },
}

#[derive(Debug)]
pub enum MarketDataSessionCommand {
    Subscribe { instruments: Vec<InstrumentId> },
    Unsubscribe { instruments: Vec<InstrumentId> },
}

#[derive(Debug, Default)]
pub struct ServerActorState {
    pub sessions: SessionManager,
    pub trading_clients: HashMap<String, TradingClientActor>,
    pub market_data_clients: HashMap<String, MarketDataClientActor>,
    pub trade_sessions: HashMap<String, TradeSessionActor>,
    pub market_data_session: Option<MarketDataSessionActor>,
    pub market_data_subscriptions: HashMap<String, HashSet<String>>,
}

impl ServerActorState {
    pub fn shared() -> SharedServerActorState {
        Arc::new(Mutex::new(Self::default()))
    }

    pub fn shared_with_config(config: &ServerConfig) -> SharedServerActorState {
        Arc::new(Mutex::new(Self::from_config(config)))
    }

    pub fn from_config(config: &ServerConfig) -> Self {
        let mut state = Self::default();
        for client in &config.clients {
            let client_id = ClientId::new(client.client_id.trim().to_string());
            for account in &client.accounts {
                state.sessions.allow_client_account(
                    client_id.clone(),
                    AccountId::new(account.trim().to_string()),
                    client.permission,
                );
            }
        }
        state
    }

    pub fn register_trading_client(
        &mut self,
        client_id: ClientId,
        peer: SocketAddr,
        outbound: EnvelopeTx,
    ) {
        self.sessions.register_client(client_id.clone());
        self.trading_clients.insert(
            client_id.as_str().to_string(),
            TradingClientActor {
                client_id: client_id.clone(),
                peer,
                outbound,
            },
        );
        info!(client = %client_id, %peer, "registered trading client actor");
    }

    pub fn register_market_data_client(
        &mut self,
        client_id: ClientId,
        peer: SocketAddr,
        outbound: EnvelopeTx,
    ) {
        self.market_data_clients.insert(
            client_id.as_str().to_string(),
            MarketDataClientActor {
                client_id: client_id.clone(),
                peer,
                outbound,
            },
        );
        info!(client = %client_id, %peer, "registered market-data client actor");
    }
}

pub(crate) async fn push_order_report(
    state: &SharedServerActorState,
    order_registry: &HashMap<String, OrderContext>,
    report: OrderReport,
) {
    let key = order_ref_lookup_key(&report.client_order_id);
    let Some(ctx) = order_registry.get(&key) else {
        warn!(
            client_order_id = %report.client_order_id,
            key = %key,
            "order return without registry context"
        );
        return;
    };

    let order = report.into_order(ctx);
    push_envelope_to_client(
        state,
        &ctx.client_id,
        Envelope::new(Message::OrderUpdate(order)),
    )
    .await;
}

pub(crate) async fn push_trade_report(
    state: &SharedServerActorState,
    order_registry: &HashMap<String, OrderContext>,
    report: TradeReport,
) {
    let Some(client_order_id) = report.client_order_id.as_ref() else {
        warn!(
            trade_id = %report.trade_id,
            exchange_order_id = %report.exchange_order_id,
            "trade return missing client order id"
        );
        return;
    };

    let key = order_ref_lookup_key(client_order_id);
    let Some(ctx) = order_registry.get(&key) else {
        warn!(
            trade_id = %report.trade_id,
            client_order_id = %client_order_id,
            key = %key,
            "trade return without registry context"
        );
        return;
    };

    let client_id = ctx.client_id.clone();
    let account_id = ctx.account_id.clone();
    let trade = report.into_trade(account_id);
    push_envelope_to_client(
        state,
        &client_id,
        Envelope::new(Message::TradeUpdate(trade)),
    )
    .await;
}

pub(crate) async fn push_envelope_to_client(
    state: &SharedServerActorState,
    client_id: &ClientId,
    envelope: Envelope,
) {
    let outbound = {
        let guard = state.lock().await;
        guard
            .trading_clients
            .get(client_id.as_str())
            .map(|actor| actor.outbound.clone())
    };

    if let Some(outbound) = outbound {
        if outbound.send(envelope).is_err() {
            warn!(client = %client_id, "failed to push envelope to client");
        }
    } else {
        warn!(client = %client_id, "trading client not connected for push");
    }
}

pub(crate) async fn update_account_connection_state(
    state: &SharedServerActorState,
    account_id: &AccountId,
    connection_state: ConnectionState,
) {
    let mut guard = state.lock().await;
    guard
        .sessions
        .set_account_state(account_id, connection_state);
}

pub(crate) fn order_ref_lookup_key(client_order_id: &ctp_model::ClientOrderId) -> String {
    normalize_order_ref(client_order_id.as_str())
}

pub(crate) fn send_error(outbound_tx: &EnvelopeTx, code: i32, message: &str) {
    let _ = outbound_tx.send(Envelope::new(Message::Error(ctp_common::ErrorResponse {
        code,
        message: message.into(),
    })));
}

pub(crate) fn send_account_state(reply_to: &EnvelopeTx, state: AccountState) {
    let envelope = Envelope::new(Message::AccountStateUpdate(state));
    if reply_to.send(envelope).is_err() {
        warn!("failed to send account state update");
    }
}

pub fn account_state(account_id: AccountId, state: ConnectionState) -> AccountState {
    AccountState {
        account_id,
        state,
        front_id: None,
        session_id: None,
        updated_at: Utc::now(),
    }
}
