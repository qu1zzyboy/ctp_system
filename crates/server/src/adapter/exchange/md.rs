//! CTP market-data session (process-wide: **1 MD**).

use std::path::PathBuf;

use ctp_common::{network::CtpEvent, CtpClient, CtpClientConfig};
use tracing::info;

/// Configuration for the shared MD session.
#[derive(Debug, Clone)]
pub struct MdSessionConfig {
    pub dynlib_path: PathBuf,
    pub flow_path: PathBuf,
    pub md_front: String,
    pub broker_id: String,
    pub user_id: String,
    pub password: String,
}

/// Placeholder for the live MD connection owned by the server.
#[derive(Debug)]
pub struct MdSession {
    pub config: MdSessionConfig,
    client: Option<CtpClient>,
}

impl MdSession {
    pub fn new(config: MdSessionConfig) -> Self {
        info!(front = %config.md_front, "exchange.md session created (not connected)");
        Self {
            config,
            client: None,
        }
    }

    /// Create MD API, register SPI/front, and start CTP network thread.
    pub fn connect(&mut self) -> anyhow::Result<()> {
        let config = CtpClientConfig::new(&self.config.dynlib_path, &self.config.md_front)
            .with_credentials(
                &self.config.broker_id,
                &self.config.user_id,
                &self.config.password,
            );
        self.client = Some(CtpClient::connect(config)?);
        info!(front = %self.config.md_front, "exchange.md connected");
        Ok(())
    }

    /// Login after receiving [`CtpEvent::FrontConnected`].
    pub fn login(&self) -> anyhow::Result<i32> {
        self.client()?.login()
    }

    pub fn subscribe(&self, instruments: &[impl AsRef<str>]) -> anyhow::Result<i32> {
        self.client()?.subscribe(instruments)
    }

    pub fn unsubscribe(&self, instruments: &[impl AsRef<str>]) -> anyhow::Result<i32> {
        self.client()?.unsubscribe(instruments)
    }

    pub async fn recv(&mut self) -> Option<CtpEvent> {
        self.client.as_mut()?.recv().await
    }

    pub fn try_recv(&mut self) -> Option<CtpEvent> {
        self.client.as_mut()?.try_recv()
    }

    fn client(&self) -> anyhow::Result<&CtpClient> {
        self.client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("MD session is not connected"))
    }
}
