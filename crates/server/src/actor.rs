//! Server-side actor handles and in-memory registries.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;

use chrono::Utc;
use ctp_common::{
    network::{CtpEvent, MarketDataTick},
    Envelope, MarketDataTickMessage, Message,
};
use ctp_model::{
    normalize_order_ref, AccountCredentials, AccountId, AccountState, CancelRequest, ClientId,
    ConnectionState, InstrumentId, OrderContext, OrderReport, OrderRequest, TradeReport,
};
use tokio::sync::{mpsc, Mutex};
use tracing::{info, warn};

use crate::adapter::exchange::{
    MdSession, MdSessionConfig, TradeSession, TradeSessionConfig, TradeSessionEvent,
    MD_DYNLIB_NAME, TD_DYNLIB_NAME,
};
use crate::config::{server_md_credentials, ServerConfig};
use crate::session::SessionManager;
use crate::trade_actor_ops::{handle_logged_in_trade_command, TradeSessionCommand};

pub type EnvelopeTx = mpsc::UnboundedSender<Envelope>;

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

#[derive(Debug, Clone)]
pub struct TradeSessionActor {
    pub account_id: AccountId,
    command_tx: mpsc::UnboundedSender<TradeSessionCommand>,
}

#[derive(Debug, Clone)]
pub struct MarketDataSessionActor {
    command_tx: mpsc::UnboundedSender<MarketDataSessionCommand>,
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

    pub fn subscribe_market_data(
        &mut self,
        client_id: &ClientId,
        instruments: impl IntoIterator<Item = InstrumentId>,
    ) -> Vec<InstrumentId> {
        let mut exchange_subscriptions = Vec::new();
        for instrument in instruments {
            let instrument_key = instrument.as_str().to_string();
            let clients = self
                .market_data_subscriptions
                .entry(instrument_key)
                .or_default();
            let was_empty = clients.is_empty();
            let inserted = clients.insert(client_id.as_str().to_string());
            if was_empty && inserted {
                exchange_subscriptions.push(instrument.clone());
            }
            info!(
                client = %client_id,
                instrument = %instrument,
                exchange_subscribe = was_empty && inserted,
                "market-data subscribed"
            );
        }
        exchange_subscriptions
    }

    pub fn unsubscribe_market_data(
        &mut self,
        client_id: &ClientId,
        instruments: impl IntoIterator<Item = InstrumentId>,
    ) -> Vec<InstrumentId> {
        let mut exchange_unsubscriptions = Vec::new();
        for instrument in instruments {
            let instrument_key = instrument.as_str().to_string();
            let mut should_remove = false;
            if let Some(clients) = self.market_data_subscriptions.get_mut(&instrument_key) {
                clients.remove(client_id.as_str());
                should_remove = clients.is_empty();
            }
            if should_remove {
                self.market_data_subscriptions.remove(&instrument_key);
                exchange_unsubscriptions.push(instrument.clone());
            }
            info!(
                client = %client_id,
                instrument = %instrument,
                exchange_unsubscribe = should_remove,
                "market-data unsubscribed"
            );
        }
        exchange_unsubscriptions
    }

    pub fn remove_market_data_client_subscriptions(
        &mut self,
        client_id: &ClientId,
    ) -> Vec<InstrumentId> {
        let mut exchange_unsubscriptions = Vec::new();
        let mut empty_instruments = Vec::new();

        for (instrument, clients) in self.market_data_subscriptions.iter_mut() {
            clients.remove(client_id.as_str());
            if clients.is_empty() {
                empty_instruments.push(instrument.clone());
            }
        }

        for instrument in empty_instruments {
            self.market_data_subscriptions.remove(&instrument);
            exchange_unsubscriptions.push(InstrumentId::new(instrument));
        }

        exchange_unsubscriptions
    }

    pub fn current_market_data_subscriptions(&self) -> Vec<InstrumentId> {
        self.market_data_subscriptions
            .keys()
            .cloned()
            .map(InstrumentId::new)
            .collect()
    }

    pub fn bind_account(
        &mut self,
        client_id: &ClientId,
        credentials: AccountCredentials,
        reply_to: EnvelopeTx,
        shared: SharedServerActorState,
    ) -> anyhow::Result<AccountState> {
        let account_id = credentials.account_id.clone();
        self.sessions
            .ensure_account_access(client_id, &account_id, "account login")?;
        self.sessions.bind_account(client_id, account_id.clone());
        self.sessions
            .set_account_state(&account_id, ConnectionState::Connecting);

        let actor = self
            .trade_sessions
            .entry(account_id.as_str().to_string())
            .or_insert_with(|| TradeSessionActor::spawn(account_id.clone(), shared))
            .clone();

        if actor
            .command_tx
            .send(TradeSessionCommand::Login {
                credentials,
                reply_to,
            })
            .is_err()
        {
            warn!(account = %account_id, "trade session actor command channel closed");
            self.sessions
                .set_account_state(&account_id, ConnectionState::Error);
            return Ok(account_state(account_id, ConnectionState::Error));
        }

        Ok(account_state(account_id, ConnectionState::Connecting))
    }

    pub fn place_order(
        &mut self,
        client_id: ClientId,
        order: OrderRequest,
        reply_to: EnvelopeTx,
    ) -> anyhow::Result<()> {
        self.sessions
            .ensure_write_access(&client_id, &order.account_id, "place order")?;
        let account_id = order.account_id.as_str().to_string();
        let actor = self
            .trade_sessions
            .get(&account_id)
            .ok_or_else(|| anyhow::anyhow!("account {account_id} is not logged in"))?
            .clone();
        actor
            .command_tx
            .send(TradeSessionCommand::PlaceOrder {
                client_id,
                order,
                reply_to,
            })
            .map_err(|e| anyhow::anyhow!("trade session actor command channel closed: {e}"))
    }

    fn send_trade_command(
        &self,
        account_id: &str,
        command: TradeSessionCommand,
    ) -> anyhow::Result<()> {
        let actor = self
            .trade_sessions
            .get(account_id)
            .ok_or_else(|| anyhow::anyhow!("account {account_id} is not logged in"))?
            .clone();
        actor
            .command_tx
            .send(command)
            .map_err(|e| anyhow::anyhow!("trade session actor command channel closed: {e}"))
    }

    pub fn cancel_order(
        &mut self,
        client_id: ClientId,
        cancel: CancelRequest,
        reply_to: EnvelopeTx,
    ) -> anyhow::Result<()> {
        let account_id = cancel.account_id.clone();
        self.sessions
            .ensure_write_access(&client_id, &account_id, "cancel order")?;
        self.send_trade_command(
            account_id.as_str(),
            TradeSessionCommand::CancelOrder { cancel, reply_to },
        )
    }

    pub fn query_account(
        &mut self,
        client_id: ClientId,
        account_id: AccountId,
        reply_to: EnvelopeTx,
    ) -> anyhow::Result<()> {
        self.sessions
            .ensure_read_access(&client_id, &account_id, "query account")?;
        let key = account_id.clone();
        self.send_trade_command(
            key.as_str(),
            TradeSessionCommand::QueryAccount {
                account_id,
                reply_to,
            },
        )
    }

    pub fn query_position(
        &mut self,
        client_id: ClientId,
        account_id: AccountId,
        instrument_id: Option<InstrumentId>,
        reply_to: EnvelopeTx,
    ) -> anyhow::Result<()> {
        self.sessions
            .ensure_read_access(&client_id, &account_id, "query position")?;
        let key = account_id.clone();
        self.send_trade_command(
            key.as_str(),
            TradeSessionCommand::QueryPosition {
                account_id,
                instrument_id,
                reply_to,
            },
        )
    }

    pub fn query_orders(
        &mut self,
        client_id: ClientId,
        account_id: AccountId,
        reply_to: EnvelopeTx,
    ) -> anyhow::Result<()> {
        self.sessions
            .ensure_read_access(&client_id, &account_id, "query orders")?;
        let key = account_id.clone();
        self.send_trade_command(
            key.as_str(),
            TradeSessionCommand::QueryOrders {
                account_id,
                reply_to,
            },
        )
    }

    pub fn query_trades(
        &mut self,
        client_id: ClientId,
        account_id: AccountId,
        reply_to: EnvelopeTx,
    ) -> anyhow::Result<()> {
        self.sessions
            .ensure_read_access(&client_id, &account_id, "query trades")?;
        let key = account_id.clone();
        self.send_trade_command(
            key.as_str(),
            TradeSessionCommand::QueryTrades {
                account_id,
                reply_to,
            },
        )
    }

    pub fn logout_account(
        &mut self,
        client_id: ClientId,
        account_id: AccountId,
        reply_to: EnvelopeTx,
    ) -> anyhow::Result<()> {
        self.sessions
            .ensure_write_access(&client_id, &account_id, "account logout")?;
        self.send_trade_command(
            account_id.as_str(),
            TradeSessionCommand::Logout { reply_to },
        )
    }
}

impl TradeSessionActor {
    pub fn spawn(account_id: AccountId, state: SharedServerActorState) -> Self {
        let (command_tx, mut command_rx) = mpsc::unbounded_channel();
        let actor_account_id = account_id.clone();

        tokio::spawn(async move {
            info!(account = %actor_account_id, "trade session actor started");
            let mut session: Option<TradeSession> = None;
            let mut logged_in_ids: Option<(i32, i32)> = None;
            let mut order_registry: HashMap<String, OrderContext> = HashMap::new();

            loop {
                if logged_in_ids.is_some() {
                    tokio::select! {
                        command = command_rx.recv() => {
                            let Some(command) = command else { break };
                            if let Err(e) = handle_logged_in_trade_command(
                                &actor_account_id,
                                &mut session,
                                &state,
                                &mut logged_in_ids,
                                &mut order_registry,
                                command,
                            ).await {
                                warn!(account = %actor_account_id, error = %e, "trade command failed");
                            }
                        }
                        event = async {
                            match session.as_mut() {
                                Some(trade) => trade.recv().await,
                                None => std::future::pending().await,
                            }
                        } => {
                            if let Some(event) = event {
                                handle_logged_in_trade_event(
                                    &actor_account_id,
                                    &state,
                                    &mut logged_in_ids,
                                    &order_registry,
                                    event,
                                ).await;
                            }
                        }
                    }
                } else {
                    let Some(command) = command_rx.recv().await else {
                        break;
                    };
                    if let Err(e) = handle_logged_out_trade_command(
                        &actor_account_id,
                        &mut session,
                        &mut logged_in_ids,
                        command,
                    )
                    .await
                    {
                        warn!(account = %actor_account_id, error = %e, "trade command failed");
                    }
                }
            }

            info!(account = %actor_account_id, "trade session actor stopped");
        });

        Self {
            account_id,
            command_tx,
        }
    }
}

impl MarketDataSessionActor {
    pub fn spawn(state: SharedServerActorState) -> Self {
        let (command_tx, mut command_rx) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            info!("market-data session actor started");
            let mut session: Option<MdSession> = None;
            let mut logged_in = false;

            loop {
                if let Some(md) = session.as_mut() {
                    tokio::select! {
                        command = command_rx.recv() => {
                            let Some(command) = command else { break; };
                            if let Err(e) = handle_market_data_command(md, &mut logged_in, command).await {
                                warn!(error = %e, "market-data command failed");
                            }
                        }
                        event = md.recv() => {
                            let Some(event) = event else { break; };
                            if let Err(e) = handle_market_data_event(md, &state, &mut logged_in, event).await {
                                warn!(error = %e, "market-data event handling failed");
                            }
                        }
                    }
                } else {
                    let Some(command) = command_rx.recv().await else {
                        break;
                    };
                    match create_md_session() {
                        Ok(mut md) => {
                            if let Err(e) = md.connect() {
                                warn!(error = %e, "market-data session connect failed");
                                continue;
                            }
                            if let Err(e) =
                                handle_market_data_command(&mut md, &mut logged_in, command).await
                            {
                                warn!(error = %e, "market-data command failed");
                            }
                            session = Some(md);
                        }
                        Err(e) => warn!(error = %e, "market-data session config failed"),
                    }
                }
            }

            info!("market-data session actor stopped");
        });

        Self { command_tx }
    }

    pub fn subscribe(&self, instruments: Vec<InstrumentId>) -> anyhow::Result<()> {
        self.command_tx
            .send(MarketDataSessionCommand::Subscribe { instruments })
            .map_err(|e| anyhow::anyhow!("market-data command channel closed: {e}"))
    }

    pub fn unsubscribe(&self, instruments: Vec<InstrumentId>) -> anyhow::Result<()> {
        self.command_tx
            .send(MarketDataSessionCommand::Unsubscribe { instruments })
            .map_err(|e| anyhow::anyhow!("market-data command channel closed: {e}"))
    }
}

pub type SharedServerActorState = Arc<Mutex<ServerActorState>>;

pub async fn subscribe_exchange_market_data(
    state: &SharedServerActorState,
    client_id: &ClientId,
    instruments: Vec<InstrumentId>,
) -> anyhow::Result<()> {
    let (actor, exchange_instruments) = {
        let mut state_guard = state.lock().await;
        let exchange_instruments = state_guard.subscribe_market_data(client_id, instruments);
        if state_guard.market_data_session.is_none() {
            state_guard.market_data_session = Some(MarketDataSessionActor::spawn(state.clone()));
        }
        let actor = state_guard
            .market_data_session
            .clone()
            .ok_or_else(|| anyhow::anyhow!("market-data session actor missing"))?;
        (actor, exchange_instruments)
    };

    if exchange_instruments.is_empty() {
        return Ok(());
    }

    actor.subscribe(exchange_instruments)
}

pub async fn unsubscribe_exchange_market_data(
    state: &SharedServerActorState,
    client_id: &ClientId,
    instruments: Vec<InstrumentId>,
) -> anyhow::Result<()> {
    let (actor, exchange_instruments) = {
        let mut state_guard = state.lock().await;
        let exchange_instruments = state_guard.unsubscribe_market_data(client_id, instruments);
        (
            state_guard.market_data_session.clone(),
            exchange_instruments,
        )
    };

    if let Some(actor) = actor {
        if !exchange_instruments.is_empty() {
            actor.unsubscribe(exchange_instruments)?;
        }
    }
    Ok(())
}

pub async fn remove_exchange_market_data_client(
    state: &SharedServerActorState,
    client_id: &ClientId,
) -> anyhow::Result<()> {
    let (actor, exchange_instruments) = {
        let mut state_guard = state.lock().await;
        state_guard.market_data_clients.remove(client_id.as_str());
        let exchange_instruments = state_guard.remove_market_data_client_subscriptions(client_id);
        (
            state_guard.market_data_session.clone(),
            exchange_instruments,
        )
    };

    if let Some(actor) = actor {
        if !exchange_instruments.is_empty() {
            actor.unsubscribe(exchange_instruments)?;
        }
    }
    Ok(())
}

async fn handle_logged_out_trade_command(
    actor_account_id: &AccountId,
    session: &mut Option<TradeSession>,
    logged_in_ids: &mut Option<(i32, i32)>,
    command: TradeSessionCommand,
) -> anyhow::Result<()> {
    match command {
        TradeSessionCommand::Login {
            credentials,
            reply_to,
        } => {
            info!(
                account = %credentials.account_id,
                broker = %credentials.broker_id,
                "trade session login requested"
            );
            if let Err(e) = drive_trade_login(
                actor_account_id,
                session,
                logged_in_ids,
                credentials,
                &reply_to,
            )
            .await
            {
                send_account_state(
                    &reply_to,
                    account_state(actor_account_id.clone(), ConnectionState::Error),
                );
                return Err(e);
            }
            Ok(())
        }
        TradeSessionCommand::PlaceOrder { reply_to, .. } => {
            send_error(
                &reply_to,
                -11,
                "account is not logged in; place order rejected",
            );
            Ok(())
        }
        TradeSessionCommand::CancelOrder { reply_to, .. }
        | TradeSessionCommand::QueryAccount { reply_to, .. }
        | TradeSessionCommand::QueryPosition { reply_to, .. }
        | TradeSessionCommand::QueryOrders { reply_to, .. }
        | TradeSessionCommand::QueryTrades { reply_to, .. } => {
            send_error(&reply_to, -11, "account is not logged in; command rejected");
            Ok(())
        }
        TradeSessionCommand::Logout { reply_to } => {
            send_account_state(
                &reply_to,
                account_state(actor_account_id.clone(), ConnectionState::Disconnected),
            );
            Ok(())
        }
    }
}

async fn handle_logged_in_trade_event(
    actor_account_id: &AccountId,
    state: &SharedServerActorState,
    logged_in_ids: &mut Option<(i32, i32)>,
    order_registry: &HashMap<String, OrderContext>,
    event: TradeSessionEvent,
) {
    match event {
        TradeSessionEvent::OrderReturn(report) => {
            push_order_report(state, order_registry, report).await;
        }
        TradeSessionEvent::TradeReturn(report) => {
            push_trade_report(state, order_registry, report).await;
        }
        TradeSessionEvent::FrontDisconnected { reason } => {
            warn!(account = %actor_account_id, reason, "trade front disconnected");
            *logged_in_ids = None;
            update_account_connection_state(state, actor_account_id, ConnectionState::Disconnected)
                .await;
        }
        TradeSessionEvent::HeartBeatWarning { time_lapse } => {
            warn!(account = %actor_account_id, time_lapse, "trade heartbeat warning");
        }
        TradeSessionEvent::OrderInsertRejected {
            error_id,
            error_msg,
        } => {
            warn!(account = %actor_account_id, error_id, %error_msg, "order insert rejected by CTP");
        }
        other => {
            warn!(account = %actor_account_id, ?other, "unexpected trade event while logged in");
        }
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

async fn update_account_connection_state(
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

async fn drive_trade_login(
    actor_account_id: &AccountId,
    session: &mut Option<TradeSession>,
    logged_in_ids: &mut Option<(i32, i32)>,
    credentials: AccountCredentials,
    reply_to: &EnvelopeTx,
) -> anyhow::Result<()> {
    if let Some((front_id, session_id)) = *logged_in_ids {
        send_account_state(
            reply_to,
            AccountState {
                account_id: credentials.account_id,
                state: ConnectionState::LoggedIn,
                front_id: Some(front_id),
                session_id: Some(session_id),
                updated_at: Utc::now(),
            },
        );
        return Ok(());
    }

    if session.is_none() {
        let config = trade_session_config(&credentials);
        let mut new_session = TradeSession::new(config);
        new_session.connect()?;
        *session = Some(new_session);
    }

    send_account_state(
        reply_to,
        account_state(credentials.account_id.clone(), ConnectionState::Connecting),
    );

    let session = session
        .as_mut()
        .ok_or_else(|| anyhow::anyhow!("trade session missing after connect"))?;
    let mut login_ids: Option<(i32, i32)> = None;

    loop {
        let event = session
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("trade session event channel closed"))?;
        match event {
            TradeSessionEvent::FrontConnected => {
                send_account_state(
                    reply_to,
                    account_state(
                        credentials.account_id.clone(),
                        ConnectionState::Authenticating,
                    ),
                );
                session.authenticate()?;
            }
            TradeSessionEvent::AuthenticateOk => {
                session.login()?;
            }
            TradeSessionEvent::LoginOk {
                front_id,
                session_id,
                ..
            } => {
                login_ids = Some((front_id, session_id));
                session.confirm_settlement()?;
            }
            TradeSessionEvent::SettlementConfirmed => {
                let (front_id, session_id) = login_ids.unwrap_or_default();
                *logged_in_ids = Some((front_id, session_id));
                send_account_state(
                    reply_to,
                    AccountState {
                        account_id: credentials.account_id.clone(),
                        state: ConnectionState::LoggedIn,
                        front_id: Some(front_id),
                        session_id: Some(session_id),
                        updated_at: Utc::now(),
                    },
                );
                return Ok(());
            }
            TradeSessionEvent::FrontDisconnected { reason } => {
                warn!(account = %actor_account_id, reason, "trade front disconnected during login");
                send_account_state(
                    reply_to,
                    account_state(
                        credentials.account_id.clone(),
                        ConnectionState::Disconnected,
                    ),
                );
            }
            TradeSessionEvent::AuthenticateFailed {
                error_id,
                error_msg,
            } => {
                anyhow::bail!("authenticate failed: {error_id} {error_msg}");
            }
            TradeSessionEvent::LoginFailed {
                error_id,
                error_msg,
            } => {
                anyhow::bail!("login failed: {error_id} {error_msg}");
            }
            TradeSessionEvent::SettlementConfirmFailed {
                error_id,
                error_msg,
            } => {
                anyhow::bail!("settlement confirm failed: {error_id} {error_msg}");
            }
            TradeSessionEvent::Error {
                error_id,
                error_msg,
                request_id,
            } => {
                anyhow::bail!("trade error request_id={request_id}: {error_id} {error_msg}");
            }
            TradeSessionEvent::HeartBeatWarning { time_lapse } => {
                warn!(account = %actor_account_id, time_lapse, "trade heartbeat warning during login");
            }
            TradeSessionEvent::OrderInsertRejected {
                error_id,
                error_msg,
            } => {
                warn!(account = %actor_account_id, error_id, %error_msg, "order insert event received during login");
            }
            TradeSessionEvent::OrderReturn(_) | TradeSessionEvent::TradeReturn(_) => {
                warn!(account = %actor_account_id, "order/trade return received during login, ignored");
            }
            TradeSessionEvent::QueryAccountReady { .. }
            | TradeSessionEvent::QueryPositionsReady { .. }
            | TradeSessionEvent::QueryOrdersReady { .. }
            | TradeSessionEvent::QueryTradesReady { .. }
            | TradeSessionEvent::QueryFailed { .. }
            | TradeSessionEvent::CancelActionOk { .. }
            | TradeSessionEvent::CancelActionFailed { .. } => {
                warn!(account = %actor_account_id, "query/cancel event received during login, ignored");
            }
        }
    }
}

fn trade_session_config(credentials: &AccountCredentials) -> TradeSessionConfig {
    let trade_front =
        std::env::var("CTP_TRADE_FRONT").unwrap_or_else(|_| "tcp://182.254.243.31:30001".into());
    let dynlib_path =
        std::path::PathBuf::from("crates/server/src/adapter/exchange/ctp").join(TD_DYNLIB_NAME);
    let flow_path =
        std::path::PathBuf::from(format!("./flow/trade_{}", credentials.account_id.as_str()));

    TradeSessionConfig {
        account_id: credentials.account_id.as_str().to_string(),
        broker_id: credentials.broker_id.clone(),
        password: credentials.password.clone(),
        app_id: credentials.app_id.clone(),
        auth_code: credentials.auth_code.clone(),
        trade_front,
        dynlib_path,
        flow_path,
    }
}

pub(crate) fn send_account_state(reply_to: &EnvelopeTx, state: AccountState) {
    let envelope = Envelope::new(Message::AccountStateUpdate(state));
    if reply_to.send(envelope).is_err() {
        warn!("failed to send account state update");
    }
}

fn create_md_session() -> anyhow::Result<MdSession> {
    let md_front =
        std::env::var("CTP_MD_FRONT").unwrap_or_else(|_| "tcp://182.254.243.31:30011".into());
    let broker_id = std::env::var("CTP_BROKER_ID").unwrap_or_else(|_| "9999".into());
    let (user_id, password) = server_md_credentials()?;
    let dynlib_path =
        std::path::PathBuf::from("crates/server/src/adapter/exchange/ctp").join(MD_DYNLIB_NAME);

    Ok(MdSession::new(MdSessionConfig {
        dynlib_path,
        flow_path: std::path::PathBuf::from("./flow/md_"),
        md_front,
        broker_id,
        user_id,
        password,
    }))
}

async fn handle_market_data_command(
    md: &mut MdSession,
    logged_in: &mut bool,
    command: MarketDataSessionCommand,
) -> anyhow::Result<()> {
    if !*logged_in {
        wait_market_data_login(md, logged_in).await?;
    }

    match command {
        MarketDataSessionCommand::Subscribe { instruments } => {
            let ids: Vec<String> = instruments
                .into_iter()
                .map(|i| i.as_str().to_string())
                .collect();
            md.subscribe(&ids)?;
            info!(instruments = ?ids, "exchange market-data subscribe requested");
        }
        MarketDataSessionCommand::Unsubscribe { instruments } => {
            let ids: Vec<String> = instruments
                .into_iter()
                .map(|i| i.as_str().to_string())
                .collect();
            md.unsubscribe(&ids)?;
            info!(instruments = ?ids, "exchange market-data unsubscribe requested");
        }
    }
    Ok(())
}

async fn wait_market_data_login(md: &mut MdSession, logged_in: &mut bool) -> anyhow::Result<()> {
    while !*logged_in {
        let event = md
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("market-data event channel closed"))?;
        match event {
            CtpEvent::FrontConnected => {
                md.login()?;
            }
            CtpEvent::LoginOk { trading_day } => {
                info!(%trading_day, "exchange market-data login ok");
                *logged_in = true;
            }
            CtpEvent::LoginFailed {
                error_id,
                error_msg,
            } => {
                anyhow::bail!("market-data login failed: {error_id} {error_msg}");
            }
            CtpEvent::Error {
                error_id,
                error_msg,
                request_id,
            } => {
                anyhow::bail!("market-data error request_id={request_id}: {error_id} {error_msg}");
            }
            CtpEvent::FrontDisconnected { reason } => {
                warn!(reason, "market-data front disconnected during login");
            }
            CtpEvent::HeartBeatWarning { time_lapse } => {
                warn!(time_lapse, "market-data heartbeat warning during login");
            }
            CtpEvent::SubscribeOk { .. }
            | CtpEvent::SubscribeFailed { .. }
            | CtpEvent::MarketData(_) => {}
        }
    }
    Ok(())
}

async fn handle_market_data_event(
    md: &mut MdSession,
    state: &SharedServerActorState,
    logged_in: &mut bool,
    event: CtpEvent,
) -> anyhow::Result<()> {
    match event {
        CtpEvent::FrontConnected => {
            md.login()?;
        }
        CtpEvent::LoginOk { trading_day } => {
            info!(%trading_day, "exchange market-data login ok");
            *logged_in = true;
            resubscribe_market_data(md, state).await?;
        }
        CtpEvent::MarketData(tick) => {
            fanout_market_data_tick(state, tick).await;
        }
        CtpEvent::SubscribeOk { instrument_id } => {
            info!(%instrument_id, "exchange market-data subscribe ok");
        }
        CtpEvent::SubscribeFailed {
            instrument_id,
            error_id,
            error_msg,
        } => {
            warn!(%instrument_id, error_id, %error_msg, "exchange market-data subscribe failed");
        }
        CtpEvent::FrontDisconnected { reason } => {
            *logged_in = false;
            warn!(reason, "exchange market-data front disconnected");
        }
        CtpEvent::HeartBeatWarning { time_lapse } => {
            warn!(time_lapse, "exchange market-data heartbeat warning");
        }
        CtpEvent::LoginFailed {
            error_id,
            error_msg,
        } => {
            *logged_in = false;
            anyhow::bail!("market-data login failed: {error_id} {error_msg}");
        }
        CtpEvent::Error {
            error_id,
            error_msg,
            request_id,
        } => {
            anyhow::bail!("market-data error request_id={request_id}: {error_id} {error_msg}");
        }
    }
    Ok(())
}

async fn resubscribe_market_data(
    md: &mut MdSession,
    state: &SharedServerActorState,
) -> anyhow::Result<()> {
    let instruments = {
        let state_guard = state.lock().await;
        state_guard.current_market_data_subscriptions()
    };
    if instruments.is_empty() {
        return Ok(());
    }

    let ids: Vec<String> = instruments
        .into_iter()
        .map(|instrument| instrument.as_str().to_string())
        .collect();
    md.subscribe(&ids)?;
    info!(
        instruments = ?ids,
        "exchange market-data resubscribe requested after login"
    );
    Ok(())
}

async fn fanout_market_data_tick(state: &SharedServerActorState, tick: MarketDataTick) {
    let envelope = Envelope::new(Message::MarketDataTick(MarketDataTickMessage {
        instrument_id: InstrumentId::new(tick.instrument_id.clone()),
        exchange_id: tick.exchange_id,
        last_price: tick.last_price,
        volume: tick.volume,
        turnover: tick.turnover,
        open_interest: tick.open_interest,
        bid_price1: tick.bid_price1,
        bid_volume1: tick.bid_volume1,
        ask_price1: tick.ask_price1,
        ask_volume1: tick.ask_volume1,
        update_time: tick.update_time,
        update_millisec: tick.update_millisec,
        trading_day: tick.trading_day,
    }));

    let subscribers = {
        let state_guard = state.lock().await;
        state_guard
            .market_data_subscriptions
            .get(&tick.instrument_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|client_id| {
                state_guard
                    .market_data_clients
                    .get(&client_id)
                    .map(|actor| actor.outbound.clone())
            })
            .collect::<Vec<_>>()
    };

    info!(
        instrument = %tick.instrument_id,
        last_price = tick.last_price,
        subscribers = subscribers.len(),
        "fanout market-data tick"
    );

    for outbound in subscribers {
        let _ = outbound.send(envelope.clone());
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn market_data_subscribe_only_requests_exchange_for_first_subscriber() {
        let mut state = ServerActorState::default();
        let cy = ClientId::new("CY");
        let hb = ClientId::new("HB");
        let rb = InstrumentId::new("rb2610");

        let first = state.subscribe_market_data(&cy, [rb.clone()]);
        let second = state.subscribe_market_data(&hb, [rb.clone()]);

        assert_eq!(first, vec![rb]);
        assert!(second.is_empty());
    }

    #[test]
    fn market_data_unsubscribe_only_requests_exchange_after_last_subscriber() {
        let mut state = ServerActorState::default();
        let cy = ClientId::new("CY");
        let hb = ClientId::new("HB");
        let rb = InstrumentId::new("rb2610");

        state.subscribe_market_data(&cy, [rb.clone()]);
        state.subscribe_market_data(&hb, [rb.clone()]);

        let first = state.unsubscribe_market_data(&cy, [rb.clone()]);
        let second = state.unsubscribe_market_data(&hb, [rb.clone()]);

        assert!(first.is_empty());
        assert_eq!(second, vec![rb]);
    }

    #[test]
    fn market_data_disconnect_cleanup_only_unsubscribes_orphaned_instruments() {
        let mut state = ServerActorState::default();
        let cy = ClientId::new("CY");
        let hb = ClientId::new("HB");
        let rb = InstrumentId::new("rb2610");
        let cu = InstrumentId::new("cu2608");

        state.subscribe_market_data(&cy, [rb.clone(), cu.clone()]);
        state.subscribe_market_data(&hb, [rb.clone()]);

        let mut orphaned = state.remove_market_data_client_subscriptions(&cy);
        orphaned.sort_by(|a, b| a.as_str().cmp(b.as_str()));

        assert_eq!(orphaned, vec![cu]);
        assert!(state
            .market_data_subscriptions
            .get(rb.as_str())
            .is_some_and(|clients| clients.contains(hb.as_str())));
    }
}
