//! Load the current client's trade credentials from environment.
//!
//! Convention: each client process has one logical `CTP_CLIENT_ID` and one
//! trade credential pair: `CTP_CLIENT_USER_ID` / `CTP_CLIENT_PASSWORD`.

use ctp_model::{AccountCredentials, AccountId, ClientId};

/// Resolve the logical client id used on the wire (`Hello`, `PlaceOrder`, …).
pub fn client_id_from_env() -> anyhow::Result<ClientId> {
    let id = std::env::var("CTP_CLIENT_ID")
        .map_err(|_| anyhow::anyhow!("missing CTP_CLIENT_ID (e.g. CY, HB)"))?;
    if id.trim().is_empty() {
        anyhow::bail!("CTP_CLIENT_ID must not be empty");
    }
    Ok(ClientId::new(id.trim()))
}

/// Load trade login credentials for the current client process.
pub fn load_trade_credentials() -> anyhow::Result<AccountCredentials> {
    Ok(AccountCredentials {
        account_id: AccountId::new(required_env("CTP_CLIENT_USER_ID")?),
        password: required_env("CTP_CLIENT_PASSWORD")?,
        broker_id: required_env("CTP_BROKER_ID")?,
        app_id: required_env("CTP_APP_ID")?,
        auth_code: required_env("CTP_AUTH_CODE")?,
    })
}

fn required_env(name: &str) -> anyhow::Result<String> {
    let value = std::env::var(name).map_err(|_| anyhow::anyhow!("missing env {name}"))?;
    if value.trim().is_empty() || value.contains("your-") || value.contains("your_") {
        anyhow::bail!("env {name} is not configured");
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::client_id_from_env;

    #[test]
    fn client_id_from_env_rejects_empty_value() {
        unsafe {
            std::env::set_var("CTP_CLIENT_ID", "  ");
        }
        assert!(client_id_from_env().is_err());
    }
}
