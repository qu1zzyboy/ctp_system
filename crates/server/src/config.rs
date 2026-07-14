//! Server configuration (accounts, front addresses, listen addr, access control).

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use ctp_model::ClientPermission;

const DEFAULT_CONFIG_PATH: &str = "config/server.toml";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// TCP / WS listen address, e.g. `0.0.0.0:9000`.
    pub listen_addr: String,
    /// Market-data TCP listen address, e.g. `0.0.0.0:9001`.
    pub market_data_listen_addr: String,
    /// Shared MD front address (1 MD for the process).
    pub md_front: String,
    /// Directory containing `thostmduserapi_se.so` / `thosttraderapi_se.so`.
    pub dynlib_dir: String,
    /// Pre-configured CTP accounts (password may also come from Client login).
    pub accounts: Vec<AccountConfig>,
    /// Server-side whitelist: which client may access which account, and how.
    pub clients: Vec<ClientAccessConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountConfig {
    pub account_id: String,
    pub broker_id: String,
    pub password: String,
    pub app_id: String,
    pub auth_code: String,
    pub trade_front: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientAccessConfig {
    pub client_id: String,
    pub accounts: Vec<String>,
    pub permission: ClientPermission,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: std::env::var("CTP_TRADING_LISTEN_ADDR")
                .unwrap_or_else(|_| "0.0.0.0:9000".into()),
            market_data_listen_addr: std::env::var("CTP_MARKET_DATA_LISTEN_ADDR")
                .unwrap_or_else(|_| "0.0.0.0:9001".into()),
            md_front: std::env::var("CTP_MD_FRONT")
                .unwrap_or_else(|_| "tcp://182.254.243.31:30011".into()),
            dynlib_dir: "crates/server/src/adapter/exchange/ctp".into(),
            accounts: vec![],
            clients: vec![],
        }
    }
}

impl ServerConfig {
    pub fn load() -> anyhow::Result<Self> {
        let path = std::env::var("CTP_SERVER_CONFIG")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(DEFAULT_CONFIG_PATH));

        if !path.exists() {
            return Ok(Self::default());
        }

        Self::load_from_path(&path)
    }

    pub fn load_from_path(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path.as_ref())?;
        let config = toml::from_str(&content)?;
        Ok(config)
    }
}

/// Credentials for the server-owned shared MD session (`CTP_SERVER_*`).
pub fn server_md_credentials() -> anyhow::Result<(String, String)> {
    let user_id = std::env::var("CTP_SERVER_USER_ID")
        .or_else(|_| std::env::var("CTP_USER_ID"))
        .map_err(|_| anyhow::anyhow!("missing CTP_SERVER_USER_ID for market-data session"))?;
    let password = std::env::var("CTP_SERVER_PASSWORD")
        .or_else(|_| std::env::var("CTP_PASSWORD"))
        .map_err(|_| anyhow::anyhow!("missing CTP_SERVER_PASSWORD for market-data session"))?;
    if user_id.trim().is_empty() || password.trim().is_empty() {
        anyhow::bail!("CTP_SERVER_USER_ID / CTP_SERVER_PASSWORD must not be empty");
    }
    Ok((user_id, password))
}
