//! Trade actor and account command routing.

use std::collections::HashMap;

use chrono::Utc;
use ctp_common::{
    Envelope, Message, QueryAccountResult, QueryOrdersResult, QueryPositionResult,
    QueryTradesResult,
};
use ctp_model::{
    AccountCredentials, AccountId, AccountState, CancelRequest, ClientId, ConnectionState,
    InstrumentId, OrderContext, OrderRequest, OrderStatus,
};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::adapter::exchange::mapping::order_report_to_wire_order;
use crate::adapter::exchange::{
    TradeSession, TradeSessionConfig, TradeSessionEvent, TD_DYNLIB_NAME,
};

use super::common::{
    account_state, order_ref_lookup_key, push_order_report, push_trade_report, send_account_state,
    send_error, update_account_connection_state, EnvelopeTx, ServerActorState,
    SharedServerActorState, TradeSessionCommand,
};

#[derive(Debug, Clone)]
pub struct TradeSessionActor {
    pub account_id: AccountId,
    command_tx: mpsc::UnboundedSender<TradeSessionCommand>,
}

impl ServerActorState {
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

async fn handle_logged_in_trade_command(
    actor_account_id: &AccountId,
    session: &mut Option<TradeSession>,
    state: &SharedServerActorState,
    logged_in_ids: &mut Option<(i32, i32)>,
    order_registry: &mut HashMap<String, OrderContext>,
    command: TradeSessionCommand,
) -> anyhow::Result<()> {
    match command {
        TradeSessionCommand::Login {
            credentials,
            reply_to,
        } => {
            if let Some((front_id, session_id)) = *logged_in_ids {
                send_account_state(
                    &reply_to,
                    AccountState {
                        account_id: credentials.account_id,
                        state: ConnectionState::LoggedIn,
                        front_id: Some(front_id),
                        session_id: Some(session_id),
                        updated_at: Utc::now(),
                    },
                );
            }
            Ok(())
        }
        TradeSessionCommand::PlaceOrder {
            client_id,
            order,
            reply_to,
        } => {
            let trade = session
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("trade session is not connected"))?;
            if let Err(e) = register_and_insert_order(trade, order_registry, client_id, order) {
                send_error(&reply_to, -10, &e.to_string());
                return Err(e);
            }
            info!(account = %actor_account_id, "order insert submitted to CTP");
            Ok(())
        }
        TradeSessionCommand::CancelOrder { cancel, reply_to } => {
            let (front_id, session_id) = (*logged_in_ids)
                .ok_or_else(|| anyhow::anyhow!("trade session is not logged in"))?;
            let trade = session
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("trade session is not connected"))?;
            drive_cancel_order(
                trade,
                state,
                order_registry,
                actor_account_id,
                cancel,
                front_id,
                session_id,
                &reply_to,
            )
            .await
        }
        TradeSessionCommand::QueryAccount {
            account_id,
            reply_to,
        } => {
            let trade = session
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("trade session is not connected"))?;
            drive_query_account(
                trade,
                state,
                order_registry,
                actor_account_id,
                account_id,
                &reply_to,
            )
            .await
        }
        TradeSessionCommand::QueryPosition {
            account_id,
            instrument_id,
            reply_to,
        } => {
            let trade = session
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("trade session is not connected"))?;
            drive_query_positions(
                trade,
                state,
                order_registry,
                actor_account_id,
                account_id,
                instrument_id.as_ref().map(|i| i.as_str()),
                &reply_to,
            )
            .await
        }
        TradeSessionCommand::QueryOrders {
            account_id,
            reply_to,
        } => {
            let trade = session
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("trade session is not connected"))?;
            drive_query_orders(
                trade,
                state,
                order_registry,
                actor_account_id,
                account_id,
                &reply_to,
            )
            .await
        }
        TradeSessionCommand::QueryTrades {
            account_id,
            reply_to,
        } => {
            let trade = session
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("trade session is not connected"))?;
            drive_query_trades(
                trade,
                state,
                order_registry,
                actor_account_id,
                account_id,
                &reply_to,
            )
            .await
        }
        TradeSessionCommand::Logout { reply_to } => {
            session.take();
            *logged_in_ids = None;
            order_registry.clear();
            send_account_state(
                &reply_to,
                account_state(actor_account_id.clone(), ConnectionState::Disconnected),
            );
            info!(account = %actor_account_id, "trade session logged out and released");
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

fn register_and_insert_order(
    session: &mut TradeSession,
    order_registry: &mut HashMap<String, OrderContext>,
    client_id: ClientId,
    order: OrderRequest,
) -> anyhow::Result<()> {
    let ctx = OrderContext {
        client_id,
        client_order_id: order.client_order_id.clone(),
        account_id: order.account_id.clone(),
        instrument_id: order.instrument_id.clone(),
        direction: order.direction,
        offset: order.offset,
        order_type: order.order_type,
        price: order.price,
        volume: order.volume,
        inserted_at: Utc::now(),
    };
    let key = ctx.order_ref_key();
    order_registry.insert(key, ctx);
    session.insert_order(&order).map(|_| ())
}

async fn drive_cancel_order(
    session: &mut TradeSession,
    state: &SharedServerActorState,
    order_registry: &HashMap<String, OrderContext>,
    actor_account_id: &AccountId,
    cancel: CancelRequest,
    front_id: i32,
    session_id: i32,
    reply_to: &EnvelopeTx,
) -> anyhow::Result<()> {
    let rid = session.cancel_order(&cancel, front_id, session_id)?;
    loop {
        let event = session
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("trade session event channel closed"))?;
        match event {
            TradeSessionEvent::OrderReturn(report) => {
                let is_target = cancel
                    .client_order_id
                    .as_ref()
                    .is_some_and(|id| id == &report.client_order_id)
                    || cancel
                        .exchange_order_id
                        .as_ref()
                        .zip(report.exchange_order_id.as_ref())
                        .is_some_and(|(expected, actual)| expected == actual);
                let is_terminal = matches!(
                    report.status,
                    OrderStatus::Cancelled | OrderStatus::Filled | OrderStatus::Rejected
                );
                push_order_report(state, order_registry, report).await;
                if is_target && is_terminal {
                    info!(
                        account = %actor_account_id,
                        request_id = rid,
                        "cancel completed by order return"
                    );
                    return Ok(());
                }
            }
            TradeSessionEvent::TradeReturn(report) => {
                push_trade_report(state, order_registry, report).await;
            }
            TradeSessionEvent::CancelActionOk { request_id } if request_id == rid => {
                info!(account = %actor_account_id, "cancel action accepted by CTP");
                return Ok(());
            }
            TradeSessionEvent::CancelActionFailed {
                request_id,
                error_id,
                error_msg,
            } if request_id == rid => {
                send_error(reply_to, error_id, &error_msg);
                return Ok(());
            }
            TradeSessionEvent::FrontDisconnected { reason } => {
                warn!(account = %actor_account_id, reason, "trade front disconnected during cancel");
                return Ok(());
            }
            _ => {}
        }
    }
}

async fn drive_query_account(
    session: &mut TradeSession,
    state: &SharedServerActorState,
    order_registry: &HashMap<String, OrderContext>,
    actor_account_id: &AccountId,
    _account_id: AccountId,
    reply_to: &EnvelopeTx,
) -> anyhow::Result<()> {
    let rid = session.query_trading_account()?;
    loop {
        match wait_actionable_event(session, state, order_registry, actor_account_id).await? {
            TradeSessionEvent::QueryAccountReady {
                request_id,
                balance,
            } if request_id == rid => {
                let _ = reply_to.send(Envelope::new(Message::QueryAccountResult(
                    QueryAccountResult { balance },
                )));
                return Ok(());
            }
            TradeSessionEvent::QueryFailed {
                request_id,
                error_id,
                error_msg,
            } if request_id == rid => {
                send_error(reply_to, error_id, &error_msg);
                return Ok(());
            }
            _ => {}
        }
    }
}

async fn drive_query_positions(
    session: &mut TradeSession,
    state: &SharedServerActorState,
    order_registry: &HashMap<String, OrderContext>,
    actor_account_id: &AccountId,
    _account_id: AccountId,
    instrument_id: Option<&str>,
    reply_to: &EnvelopeTx,
) -> anyhow::Result<()> {
    let rid = session.query_investor_position(instrument_id)?;
    loop {
        match wait_actionable_event(session, state, order_registry, actor_account_id).await? {
            TradeSessionEvent::QueryPositionsReady {
                request_id,
                positions,
            } if request_id == rid => {
                let _ = reply_to.send(Envelope::new(Message::QueryPositionResult(
                    QueryPositionResult { positions },
                )));
                return Ok(());
            }
            TradeSessionEvent::QueryFailed {
                request_id,
                error_id,
                error_msg,
            } if request_id == rid => {
                send_error(reply_to, error_id, &error_msg);
                return Ok(());
            }
            _ => {}
        }
    }
}

async fn drive_query_orders(
    session: &mut TradeSession,
    state: &SharedServerActorState,
    order_registry: &HashMap<String, OrderContext>,
    actor_account_id: &AccountId,
    account_id: AccountId,
    reply_to: &EnvelopeTx,
) -> anyhow::Result<()> {
    let rid = session.query_orders(None)?;
    loop {
        match wait_actionable_event(session, state, order_registry, actor_account_id).await? {
            TradeSessionEvent::QueryOrdersReady { request_id, orders } if request_id == rid => {
                let client_id = default_query_client_id(order_registry);
                let orders = orders
                    .into_iter()
                    .map(|report| {
                        let cid = lookup_client_id(order_registry, &report.client_order_id)
                            .unwrap_or_else(|| client_id.clone());
                        order_report_to_wire_order(report, account_id.clone(), cid)
                    })
                    .collect();
                let _ = reply_to.send(Envelope::new(Message::QueryOrdersResult(
                    QueryOrdersResult { orders },
                )));
                return Ok(());
            }
            TradeSessionEvent::QueryFailed {
                request_id,
                error_id,
                error_msg,
            } if request_id == rid => {
                send_error(reply_to, error_id, &error_msg);
                return Ok(());
            }
            _ => {}
        }
    }
}

async fn drive_query_trades(
    session: &mut TradeSession,
    state: &SharedServerActorState,
    order_registry: &HashMap<String, OrderContext>,
    actor_account_id: &AccountId,
    account_id: AccountId,
    reply_to: &EnvelopeTx,
) -> anyhow::Result<()> {
    let rid = session.query_trades(None)?;
    loop {
        match wait_actionable_event(session, state, order_registry, actor_account_id).await? {
            TradeSessionEvent::QueryTradesReady { request_id, trades } if request_id == rid => {
                let trades = trades
                    .into_iter()
                    .map(|report| report.into_trade(account_id.clone()))
                    .collect();
                let _ = reply_to.send(Envelope::new(Message::QueryTradesResult(
                    QueryTradesResult { trades },
                )));
                return Ok(());
            }
            TradeSessionEvent::QueryFailed {
                request_id,
                error_id,
                error_msg,
            } if request_id == rid => {
                send_error(reply_to, error_id, &error_msg);
                return Ok(());
            }
            _ => {}
        }
    }
}

async fn wait_actionable_event(
    session: &mut TradeSession,
    state: &SharedServerActorState,
    order_registry: &HashMap<String, OrderContext>,
    actor_account_id: &AccountId,
) -> anyhow::Result<TradeSessionEvent> {
    loop {
        let event = session
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("trade session event channel closed"))?;
        match event {
            TradeSessionEvent::OrderReturn(report) => {
                push_order_report(state, order_registry, report).await;
            }
            TradeSessionEvent::TradeReturn(report) => {
                push_trade_report(state, order_registry, report).await;
            }
            TradeSessionEvent::FrontDisconnected { reason } => {
                warn!(account = %actor_account_id, reason, "trade front disconnected during command");
                return Ok(TradeSessionEvent::FrontDisconnected { reason });
            }
            other => return Ok(other),
        }
    }
}

fn lookup_client_id(
    order_registry: &HashMap<String, OrderContext>,
    client_order_id: &ctp_model::ClientOrderId,
) -> Option<ClientId> {
    let key = order_ref_lookup_key(client_order_id);
    order_registry.get(&key).map(|ctx| ctx.client_id.clone())
}

fn default_query_client_id(order_registry: &HashMap<String, OrderContext>) -> ClientId {
    order_registry
        .values()
        .next()
        .map(|ctx| ctx.client_id.clone())
        .unwrap_or_else(|| ClientId::new("query"))
}
