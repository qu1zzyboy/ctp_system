use anyhow::Result;
use ctp_client::{client_id_from_env, load_trade_credentials, MarketDataClient, TradingClient};
use ctp_common::{init_logging, Message};
use ctp_model::{
    AccountId, CancelRequest, ClientOrderId, Direction, InstrumentId, OffsetFlag, OrderRequest,
    OrderStatus, OrderType,
};
use std::{pin::Pin, time::Duration};

#[tokio::main]
async fn main() -> Result<()> {
    init_logging("info,ctp_client=debug");

    let client_id = client_id_from_env()?;
    let trading_addr =
        std::env::var("CTP_SERVER_TRADING_ADDR").unwrap_or_else(|_| "127.0.0.1:9000".into());
    let market_data_addr =
        std::env::var("CTP_SERVER_MARKET_DATA_ADDR").unwrap_or_else(|_| "127.0.0.1:9001".into());

    let mut trading = TradingClient::new(client_id.clone(), trading_addr);
    let mut market_data = MarketDataClient::new(client_id.clone(), market_data_addr);
    trading.log_startup();
    market_data.log_startup();

    trading.connect().await?;
    market_data.connect().await?;

    if let Some(envelope) = trading.recv().await {
        tracing::info!(?envelope, "received trading response");
    }
    if let Some(envelope) = market_data.recv().await {
        tracing::info!(?envelope, "received market-data response");
    }

    let credentials = load_trade_credentials()?;
    tracing::info!(
        client = %client_id,
        account = %credentials.account_id,
        "using client trade credentials"
    );
    trading.login_account(credentials.clone()).await?;

    let instruments = parse_instruments(&required_env("CTP_INSTRUMENTS")?);
    market_data.subscribe(instruments.clone()).await?;

    let mut trade_logged_in = false;
    let mut got_tick = false;
    let mut order_sent = false;
    let mut queries_sent = false;
    let mut query_account_done = false;
    let mut query_position_done = false;
    let mut query_orders_done = false;
    let mut query_trades_done = false;
    let mut cancel_sent = false;
    let mut order_finished = false;
    let mut completion_hold_done = false;
    let mut hold_deadline: Option<Pin<Box<tokio::time::Sleep>>> = None;
    let mut logout_sent = false;
    let mut last_tick: Option<(InstrumentId, f64)> = None;
    let mut test_order_id: Option<ClientOrderId> = None;
    let deadline = tokio::time::sleep(Duration::from_secs(120));
    tokio::pin!(deadline);

    loop {
        if trade_logged_in && !queries_sent {
            tracing::info!("sending account/position/order/trade queries");
            trading
                .query_account(credentials.account_id.clone())
                .await?;
            trading
                .query_position(credentials.account_id.clone(), None)
                .await?;
            trading.query_orders(credentials.account_id.clone()).await?;
            trading.query_trades(credentials.account_id.clone()).await?;
            queries_sent = true;
        }

        if trade_logged_in
            && got_tick
            && query_account_done
            && query_position_done
            && query_orders_done
            && query_trades_done
            && !order_sent
        {
            let order =
                build_passive_test_order(credentials.account_id.clone(), last_tick.clone())?;
            test_order_id = Some(order.client_order_id.clone());
            tracing::info!(?order, "sending test order");
            trading.place_order(order).await?;
            order_sent = true;
        }

        if order_finished && !completion_hold_done && hold_deadline.is_none() {
            tracing::info!(
                hold_secs = 20,
                "client flow completed; keeping business connections open for recording"
            );
            hold_deadline = Some(Box::pin(tokio::time::sleep(Duration::from_secs(20))));
        }

        if completion_hold_done && !logout_sent {
            tracing::info!(instruments = ?instruments, "sending market-data unsubscribe");
            market_data.unsubscribe(instruments.clone()).await?;
            tracing::info!(account = %credentials.account_id, "sending account logout");
            trading
                .logout_account(credentials.account_id.clone())
                .await?;
            logout_sent = true;
        }

        if logout_sent && !trade_logged_in {
            break;
        }

        tokio::select! {
            envelope = trading.recv() => {
                let Some(envelope) = envelope else {
                    anyhow::bail!("trading connection closed");
                };
                match envelope.payload {
                    Message::AccountStateUpdate(state) => {
                        tracing::info!(account = %state.account_id, ?state.state, ?state.front_id, ?state.session_id, "received account state");
                        trade_logged_in = matches!(state.state, ctp_model::ConnectionState::LoggedIn);
                    }
                    Message::OrderUpdate(order) => {
                        tracing::info!(
                            client_order_id = %order.client_order_id,
                            account = %order.account_id,
                            instrument = %order.instrument_id,
                            ?order.status,
                            price = order.price,
                            volume = order.volume,
                            "received order update"
                        );
                        if test_order_id.as_ref() == Some(&order.client_order_id) {
                            if !cancel_sent && is_cancelable_status(order.status) {
                                tracing::info!(
                                    client_order_id = %order.client_order_id,
                                    exchange_order_id = ?order.exchange_order_id,
                                    "sending cancel order"
                                );
                                trading
                                    .cancel_order(CancelRequest {
                                        account_id: order.account_id.clone(),
                                        instrument_id: order.instrument_id.clone(),
                                        client_order_id: Some(order.client_order_id.clone()),
                                        exchange_order_id: order.exchange_order_id.clone(),
                                    })
                                    .await?;
                                cancel_sent = true;
                            }

                            if is_terminal_status(order.status) {
                                order_finished = true;
                            }
                        }
                    }
                    Message::QueryAccountResult(result) => {
                        tracing::info!(
                            balance = result.balance.balance,
                            available = result.balance.available,
                            "received account query result"
                        );
                        query_account_done = true;
                    }
                    Message::QueryPositionResult(result) => {
                        tracing::info!(count = result.positions.len(), "received position query result");
                        query_position_done = true;
                    }
                    Message::QueryOrdersResult(result) => {
                        tracing::info!(count = result.orders.len(), "received orders query result");
                        query_orders_done = true;
                    }
                    Message::QueryTradesResult(result) => {
                        tracing::info!(count = result.trades.len(), "received trades query result");
                        query_trades_done = true;
                    }
                    Message::Error(error) => {
                        anyhow::bail!("trading error {}: {}", error.code, error.message);
                    }
                    other => tracing::info!(?other, "received trading message"),
                }
            }
            envelope = market_data.recv() => {
                let Some(envelope) = envelope else {
                    anyhow::bail!("market-data connection closed");
                };
                match envelope.payload {
                    Message::MarketDataTick(tick) => {
                        tracing::info!(
                            instrument = %tick.instrument_id,
                            last = tick.last_price,
                            bid1 = tick.bid_price1,
                            bid_volume1 = tick.bid_volume1,
                            ask1 = tick.ask_price1,
                            ask_volume1 = tick.ask_volume1,
                            time = %tick.update_time,
                            trading_day = %tick.trading_day,
                            "received market-data tick"
                        );
                        got_tick = true;
                        last_tick = Some((tick.instrument_id.clone(), tick.last_price));
                    }
                    Message::Error(error) => {
                        anyhow::bail!("market-data error {}: {}", error.code, error.message);
                    }
                    other => tracing::info!(?other, "received market-data message"),
                }
            }
            () = async {
                match hold_deadline.as_mut() {
                    Some(deadline) => deadline.as_mut().await,
                    None => std::future::pending().await,
                }
            } => {
                completion_hold_done = true;
                hold_deadline = None;
                tracing::info!("recording hold completed; cleaning up client flow");
            }
            () = &mut deadline => {
                anyhow::bail!("timed out waiting for full client flow");
            }
        }
    }

    Ok(())
}

fn required_env(name: &str) -> Result<String> {
    let value = std::env::var(name)?;
    if value.trim().is_empty() || value.contains("your-") || value.contains("your_") {
        anyhow::bail!("env {name} is not configured");
    }
    Ok(value)
}

fn parse_instruments(value: &str) -> Vec<InstrumentId> {
    value
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(InstrumentId::new)
        .collect()
}

fn build_passive_test_order(
    account_id: AccountId,
    last_tick: Option<(InstrumentId, f64)>,
) -> Result<OrderRequest> {
    let instrument_id = std::env::var("CTP_TEST_ORDER_INSTRUMENT")
        .ok()
        .map(InstrumentId::new)
        .or_else(|| last_tick.as_ref().map(|(instrument, _)| instrument.clone()))
        .ok_or_else(|| anyhow::anyhow!("missing CTP_TEST_ORDER_INSTRUMENT and no tick received"))?;

    let direction = parse_direction(
        &std::env::var("CTP_TEST_ORDER_DIRECTION").unwrap_or_else(|_| "Buy".into()),
    )?;
    let last_price = last_tick
        .as_ref()
        .map(|(_, price)| *price)
        .ok_or_else(|| anyhow::anyhow!("missing tick price"))?;
    let price = optional_env_f64("CTP_TEST_ORDER_PRICE")?
        .unwrap_or_else(|| passive_limit_price(last_price, direction));

    Ok(OrderRequest {
        client_order_id: ClientOrderId::new(format!(
            "{}",
            chrono::Utc::now().timestamp_millis() % 1_000_000_000_000
        )),
        account_id,
        instrument_id,
        direction,
        offset: OffsetFlag::Open,
        order_type: OrderType::Limit,
        volume: optional_env_i32("CTP_TEST_ORDER_VOLUME")?.unwrap_or(1),
        price,
    })
}

fn passive_limit_price(last_price: f64, direction: Direction) -> f64 {
    match direction {
        Direction::Buy => (last_price * 0.98).floor().max(1.0),
        Direction::Sell => (last_price * 1.02).ceil(),
    }
}

fn is_cancelable_status(status: OrderStatus) -> bool {
    matches!(
        status,
        OrderStatus::Submitted | OrderStatus::Accepted | OrderStatus::PartiallyFilled
    )
}

fn is_terminal_status(status: OrderStatus) -> bool {
    matches!(
        status,
        OrderStatus::Cancelled | OrderStatus::Filled | OrderStatus::Rejected
    )
}

fn parse_direction(value: &str) -> Result<Direction> {
    match value.to_ascii_lowercase().as_str() {
        "buy" | "b" => Ok(Direction::Buy),
        "sell" | "s" => Ok(Direction::Sell),
        _ => anyhow::bail!("invalid CTP_TEST_ORDER_DIRECTION={value}, expected Buy or Sell"),
    }
}

fn optional_env_f64(name: &str) -> Result<Option<f64>> {
    let Ok(value) = std::env::var(name) else {
        return Ok(None);
    };
    if value.trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(value.parse()?))
}

fn optional_env_i32(name: &str) -> Result<Option<i32>> {
    let Ok(value) = std::env::var(name) else {
        return Ok(None);
    };
    if value.trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(value.parse()?))
}
