use anyhow::Result;
use ctp_common::init_logging;
use ctp_server::{ExchangeGateway, InternalGateway, ServerActorState, ServerConfig};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    init_logging("info,ctp_server=debug");

    let config = ServerConfig::load()?;
    ExchangeGateway::probe();

    let state = ServerActorState::shared_with_config(&config);
    let internal = InternalGateway::new(&config.listen_addr, &config.market_data_listen_addr);
    info!(
        trading_listen = %config.listen_addr,
        market_data_listen = %config.market_data_listen_addr,
        md_front = %config.md_front,
        dynlib_dir = %config.dynlib_dir,
        access_clients = config.clients.len(),
        "ctp-server starting"
    );

    // ExchangeGateway owns the future CTP MD + Trade actors; internal gateway
    // handles Client <-> Server actors now.
    internal.serve(state).await
}
