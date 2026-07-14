use std::{env, path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use ctp_common::{init_logging, network::CtpEvent, CtpClient, CtpClientConfig};
use ctp_server::config::server_md_credentials;

const DEFAULT_DYNLIB_DIR: &str = "crates/server/src/adapter/exchange/ctp";
const MD_DYNLIB_NAME: &str = "thostmduserapi_se.so";

#[tokio::main]
async fn main() -> Result<()> {
    init_logging("info,ctp_server=debug,ctp_common=debug");

    let md_front = required_env("CTP_MD_FRONT")?;
    let broker_id = required_env("CTP_BROKER_ID")?;
    let (user_id, password) = server_md_credentials()?;
    let instruments = parse_instruments(&required_env("CTP_INSTRUMENTS")?);
    let max_ticks = optional_env_usize("CTP_MARKET_DATA_MAX_TICKS")?.unwrap_or(0);

    if instruments.is_empty() {
        anyhow::bail!("CTP_INSTRUMENTS must contain at least one instrument");
    }

    let dynlib_path = PathBuf::from(DEFAULT_DYNLIB_DIR).join(MD_DYNLIB_NAME);
    let config = CtpClientConfig::new(dynlib_path, md_front)
        .with_credentials(broker_id, user_id, password)
        .with_instruments(instruments.clone());

    let mut client = CtpClient::connect(config).context("connect CTP MD API")?;
    let mut tick_count = 0usize;
    let mut idle_interval = tokio::time::interval(Duration::from_secs(30));
    idle_interval.tick().await;

    loop {
        let event = tokio::select! {
            event = client.recv() => event,
            _ = idle_interval.tick() => {
                println!("waiting for market data... ticks={tick_count}");
                continue;
            }
        };
        let Some(event) = event else {
            std::mem::forget(client);
            anyhow::bail!("CTP MD event channel closed");
        };

        match event {
            CtpEvent::FrontConnected => {
                client.login().context("request CTP MD login")?;
            }
            CtpEvent::LoginOk { trading_day } => {
                println!("login ok, trading_day={trading_day}");
                client
                    .subscribe_configured()
                    .context("subscribe configured instruments")?;
                println!("subscribed: {}", instruments.join(","));
            }
            CtpEvent::MarketData(tick) => {
                tick_count += 1;
                println!(
                    "tick #{tick_count} {} last={} bid1={}x{} ask1={}x{} volume={} open_interest={} time={}.{} trading_day={}",
                    tick.instrument_id,
                    tick.last_price,
                    tick.bid_price1,
                    tick.bid_volume1,
                    tick.ask_price1,
                    tick.ask_volume1,
                    tick.volume,
                    tick.open_interest,
                    tick.update_time,
                    tick.update_millisec,
                    tick.trading_day
                );

                if max_ticks > 0 && tick_count >= max_ticks {
                    std::mem::forget(client);
                    return Ok(());
                }
            }
            CtpEvent::LoginFailed {
                error_id,
                error_msg,
            } => {
                std::mem::forget(client);
                anyhow::bail!("CTP MD login failed: {error_id} {error_msg}");
            }
            CtpEvent::SubscribeFailed {
                instrument_id,
                error_id,
                error_msg,
            } => {
                std::mem::forget(client);
                anyhow::bail!("subscribe {instrument_id} failed: {error_id} {error_msg}");
            }
            CtpEvent::FrontDisconnected { reason } => {
                println!("front disconnected, reason={reason}");
            }
            CtpEvent::HeartBeatWarning { time_lapse } => {
                println!("heartbeat warning, time_lapse={time_lapse}");
            }
            CtpEvent::SubscribeOk { instrument_id } => {
                println!("subscribe ok: {instrument_id}");
            }
            CtpEvent::Error {
                error_id,
                error_msg,
                request_id,
            } => {
                std::mem::forget(client);
                anyhow::bail!("CTP MD error request_id={request_id}: {error_id} {error_msg}");
            }
        }
    }
}

fn required_env(name: &str) -> Result<String> {
    let value = env::var(name).with_context(|| format!("missing env {name}"))?;
    if value.trim().is_empty() || value.contains("your-") || value.contains("your_") {
        anyhow::bail!("env {name} is not configured");
    }
    Ok(value)
}

fn parse_instruments(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn optional_env_usize(name: &str) -> Result<Option<usize>> {
    let Ok(value) = env::var(name) else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    value
        .parse()
        .map(Some)
        .with_context(|| format!("invalid usize env {name}={value}"))
}
