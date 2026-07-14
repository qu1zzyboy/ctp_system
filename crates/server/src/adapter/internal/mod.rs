//! Internal adapter — Client ↔ Server TCP messaging.
//!
//! Accepts multiple trading terminals, frames length-prefixed messages, and
//! hands decoded envelopes to the server actor layer.

pub mod tcp_server;

pub use tcp_server::{TcpEndpointKind, TcpServer};

use tracing::info;

use crate::actor::SharedServerActorState;

/// Facade over the internal TCP listener and per-client sessions.
#[derive(Debug, Default)]
pub struct InternalGateway {
    pub trading_listen_addr: String,
    pub market_data_listen_addr: String,
}

impl InternalGateway {
    pub fn new(
        trading_listen_addr: impl Into<String>,
        market_data_listen_addr: impl Into<String>,
    ) -> Self {
        Self {
            trading_listen_addr: trading_listen_addr.into(),
            market_data_listen_addr: market_data_listen_addr.into(),
        }
    }

    /// Bind trading and market-data listeners and accept forever.
    pub async fn serve(&self, state: SharedServerActorState) -> anyhow::Result<()> {
        let trading = TcpServer::bind(&self.trading_listen_addr, TcpEndpointKind::Trading).await?;
        let market_data =
            TcpServer::bind(&self.market_data_listen_addr, TcpEndpointKind::MarketData).await?;

        info!(
            trading = %self.trading_listen_addr,
            market_data = %self.market_data_listen_addr,
            "internal gateway serving"
        );

        tokio::try_join!(trading.run(state.clone()), market_data.run(state))?;
        Ok(())
    }
}
