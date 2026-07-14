//! Market-data actor and subscription routing.

use ctp_common::{
    network::{CtpEvent, MarketDataTick},
    Envelope, MarketDataTickMessage, Message,
};
use ctp_model::{ClientId, InstrumentId};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::adapter::exchange::{MdSession, MdSessionConfig, MD_DYNLIB_NAME};
use crate::config::server_md_credentials;

use super::common::{MarketDataSessionCommand, ServerActorState, SharedServerActorState};

#[derive(Debug, Clone)]
pub struct MarketDataSessionActor {
    command_tx: mpsc::UnboundedSender<MarketDataSessionCommand>,
}

impl ServerActorState {
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
