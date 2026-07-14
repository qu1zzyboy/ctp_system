//! Client session bookkeeping on the server.

use ctp_model::{AccountId, ClientId, ClientPermission, ConnectionState};
use std::collections::HashMap;

/// One connected trading terminal.
#[derive(Debug, Clone)]
pub struct ClientSession {
    pub client_id: ClientId,
    /// Accounts this client has logged into via the server.
    pub bound_accounts: Vec<AccountId>,
}

/// Maps clients ↔ accounts and tracks CTP connection state per account.
#[derive(Debug, Default)]
pub struct SessionManager {
    pub clients: HashMap<String, ClientSession>,
    pub account_states: HashMap<String, ConnectionState>,
    /// account_id → client_ids that own / listen to it
    pub account_subscribers: HashMap<String, Vec<ClientId>>,
    /// client_id → account_id → permission, loaded from server-side TOML.
    pub access_rules: HashMap<String, HashMap<String, ClientPermission>>,
}

impl SessionManager {
    pub fn register_client(&mut self, client_id: ClientId) {
        let key = client_id.as_str().to_string();
        self.clients.insert(
            key,
            ClientSession {
                client_id,
                bound_accounts: Vec::new(),
            },
        );
    }

    pub fn allow_client_account(
        &mut self,
        client_id: ClientId,
        account_id: AccountId,
        permission: ClientPermission,
    ) {
        self.access_rules
            .entry(client_id.as_str().to_string())
            .or_default()
            .insert(account_id.as_str().to_string(), permission);
    }

    pub fn bind_account(&mut self, client_id: &ClientId, account_id: AccountId) {
        if let Some(session) = self.clients.get_mut(client_id.as_str()) {
            if !session.bound_accounts.iter().any(|a| a == &account_id) {
                session.bound_accounts.push(account_id.clone());
            }
        }
        let subscribers = self
            .account_subscribers
            .entry(account_id.as_str().to_string())
            .or_default();
        if !subscribers.iter().any(|id| id == client_id) {
            subscribers.push(client_id.clone());
        }
    }

    pub fn set_account_state(&mut self, account_id: &AccountId, state: ConnectionState) {
        self.account_states
            .insert(account_id.as_str().to_string(), state);
    }

    pub fn ensure_account_access(
        &self,
        client_id: &ClientId,
        account_id: &AccountId,
        operation: &str,
    ) -> anyhow::Result<()> {
        self.permission_for(client_id, account_id)
            .map(|_| ())
            .ok_or_else(|| permission_error(client_id, account_id, operation, "whitelist entry"))
    }

    pub fn ensure_read_access(
        &self,
        client_id: &ClientId,
        account_id: &AccountId,
        operation: &str,
    ) -> anyhow::Result<()> {
        match self.permission_for(client_id, account_id) {
            Some(ClientPermission::Full | ClientPermission::QueryOnly) => Ok(()),
            Some(permission) => Err(permission_error_with_mode(
                client_id, account_id, operation, permission, "read",
            )),
            None => Err(permission_error(
                client_id,
                account_id,
                operation,
                "read access",
            )),
        }
    }

    pub fn ensure_write_access(
        &self,
        client_id: &ClientId,
        account_id: &AccountId,
        operation: &str,
    ) -> anyhow::Result<()> {
        match self.permission_for(client_id, account_id) {
            Some(ClientPermission::Full | ClientPermission::TradeOnly) => Ok(()),
            Some(permission) => Err(permission_error_with_mode(
                client_id, account_id, operation, permission, "write",
            )),
            None => Err(permission_error(
                client_id,
                account_id,
                operation,
                "write access",
            )),
        }
    }

    fn permission_for(
        &self,
        client_id: &ClientId,
        account_id: &AccountId,
    ) -> Option<ClientPermission> {
        self.access_rules
            .get(client_id.as_str())
            .and_then(|accounts| accounts.get(account_id.as_str()))
            .copied()
    }
}

fn permission_error(
    client_id: &ClientId,
    account_id: &AccountId,
    operation: &str,
    required: &str,
) -> anyhow::Error {
    anyhow::anyhow!(
        "permission denied: client {} has no {} for account {} while handling {}",
        client_id,
        required,
        account_id,
        operation
    )
}

fn permission_error_with_mode(
    client_id: &ClientId,
    account_id: &AccountId,
    operation: &str,
    actual: ClientPermission,
    required: &str,
) -> anyhow::Error {
    anyhow::anyhow!(
        "permission denied: client {} has {:?} for account {}, requires {} access for {}",
        client_id,
        actual,
        account_id,
        required,
        operation
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_only_allows_read_and_denies_write() {
        let client = ClientId::new("HB");
        let account = AccountId::new("268508");
        let mut sessions = SessionManager::default();
        sessions.allow_client_account(client.clone(), account.clone(), ClientPermission::QueryOnly);

        assert!(sessions
            .ensure_read_access(&client, &account, "query account")
            .is_ok());
        assert!(sessions
            .ensure_write_access(&client, &account, "place order")
            .is_err());
    }

    #[test]
    fn unlisted_account_is_denied_by_default() {
        let client = ClientId::new("HB");
        let account = AccountId::new("268809");
        let sessions = SessionManager::default();

        assert!(sessions
            .ensure_account_access(&client, &account, "account login")
            .is_err());
        assert!(sessions
            .ensure_read_access(&client, &account, "query account")
            .is_err());
        assert!(sessions
            .ensure_write_access(&client, &account, "place order")
            .is_err());
    }
}
