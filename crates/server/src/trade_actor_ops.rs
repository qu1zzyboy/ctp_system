//! Trade session actor command handling (cancel, query, logout).

use std::collections::HashMap;

use tracing::{info, warn};

use ctp_common::{
    Envelope, Message, QueryAccountResult, QueryOrdersResult, QueryPositionResult,
    QueryTradesResult,
};
use ctp_model::{
    AccountId, CancelRequest, ClientId, InstrumentId, OrderContext, OrderRequest, OrderStatus,
};

use crate::adapter::exchange::mapping::order_report_to_wire_order;
use crate::adapter::exchange::{TradeSession, TradeSessionEvent};

use super::actor::{
    account_state, order_ref_lookup_key, push_order_report, push_trade_report, send_account_state,
    send_error, EnvelopeTx, SharedServerActorState,
};
use ctp_model::ConnectionState;

#[derive(Debug)]
pub enum TradeSessionCommand {
    Login {
        credentials: ctp_model::AccountCredentials,
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

pub async fn handle_logged_in_trade_command(
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
                    ctp_model::AccountState {
                        account_id: credentials.account_id,
                        state: ConnectionState::LoggedIn,
                        front_id: Some(front_id),
                        session_id: Some(session_id),
                        updated_at: chrono::Utc::now(),
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
            let (front_id, session_id) =
                logged_in_ids.ok_or_else(|| anyhow::anyhow!("trade session is not logged in"))?;
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
        inserted_at: chrono::Utc::now(),
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
