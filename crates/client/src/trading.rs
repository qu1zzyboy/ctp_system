//! Trading command connection to `ctp-server`.

use ctp_common::{
    AccountLoginRequest, AccountLogoutRequest, Envelope, HelloRequest, Message,
    QueryAccountRequest, QueryOrdersRequest, QueryPositionRequest, QueryTradesRequest, SendError,
};
use ctp_model::{
    AccountCredentials, AccountId, CancelRequest, ClientId, InstrumentId, OrderRequest,
};
use tracing::info;

use crate::connection::ClientConnection;

/// Client-side facade for low-frequency trading commands and responses.
#[derive(Debug)]
pub struct TradingClient {
    pub client_id: ClientId,
    pub server_addr: String,
    connection: Option<ClientConnection>,
    login_credentials: Option<AccountCredentials>,
}

impl TradingClient {
    pub fn new(client_id: impl Into<ClientId>, server_addr: impl Into<String>) -> Self {
        Self {
            client_id: client_id.into(),
            server_addr: server_addr.into(),
            connection: None,
            login_credentials: None,
        }
    }

    pub async fn connect(&mut self) -> anyhow::Result<()> {
        let connection = ClientConnection::connect(self.server_addr.clone()).await?;
        self.connection = Some(connection);
        self.send_hello().await?;
        self.update_reconnect_envelopes()?;
        Ok(())
    }

    /// Build the initial server-session hello envelope.
    pub fn hello_envelope(&self) -> Envelope {
        Envelope::new(Message::Hello(HelloRequest {
            client_id: self.client_id.clone(),
        }))
        .with_client(self.client_id.clone())
    }

    pub async fn send_hello(&self) -> Result<(), SendError> {
        self.send_envelope(&self.hello_envelope()).await
    }

    pub async fn login_account(
        &mut self,
        credentials: AccountCredentials,
    ) -> Result<(), SendError> {
        let reconnect_credentials = credentials.clone();
        self.send_envelope(
            &Envelope::new(Message::AccountLogin(AccountLoginRequest { credentials }))
                .with_client(self.client_id.clone()),
        )
        .await?;
        self.login_credentials = Some(reconnect_credentials);
        self.update_reconnect_envelopes()?;
        Ok(())
    }

    pub async fn place_order(&self, order: OrderRequest) -> Result<(), SendError> {
        self.send_envelope(
            &Envelope::new(Message::PlaceOrder(order)).with_client(self.client_id.clone()),
        )
        .await
    }

    pub async fn cancel_order(&self, cancel: CancelRequest) -> Result<(), SendError> {
        self.send_envelope(
            &Envelope::new(Message::CancelOrder(cancel)).with_client(self.client_id.clone()),
        )
        .await
    }

    pub async fn query_account(&self, account_id: AccountId) -> Result<(), SendError> {
        self.send_envelope(
            &Envelope::new(Message::QueryAccount(QueryAccountRequest { account_id }))
                .with_client(self.client_id.clone()),
        )
        .await
    }

    pub async fn query_position(
        &self,
        account_id: AccountId,
        instrument_id: Option<InstrumentId>,
    ) -> Result<(), SendError> {
        self.send_envelope(
            &Envelope::new(Message::QueryPosition(QueryPositionRequest {
                account_id,
                instrument_id,
            }))
            .with_client(self.client_id.clone()),
        )
        .await
    }

    pub async fn query_orders(&self, account_id: AccountId) -> Result<(), SendError> {
        self.send_envelope(
            &Envelope::new(Message::QueryOrders(QueryOrdersRequest { account_id }))
                .with_client(self.client_id.clone()),
        )
        .await
    }

    pub async fn query_trades(&self, account_id: AccountId) -> Result<(), SendError> {
        self.send_envelope(
            &Envelope::new(Message::QueryTrades(QueryTradesRequest { account_id }))
                .with_client(self.client_id.clone()),
        )
        .await
    }

    pub async fn logout_account(&mut self, account_id: AccountId) -> Result<(), SendError> {
        self.send_envelope(
            &Envelope::new(Message::AccountLogout(AccountLogoutRequest { account_id }))
                .with_client(self.client_id.clone()),
        )
        .await?;
        self.login_credentials = None;
        self.update_reconnect_envelopes()?;
        Ok(())
    }

    pub async fn send_envelope(&self, envelope: &Envelope) -> Result<(), SendError> {
        self.connection()?.send(envelope).await
    }

    pub async fn recv(&mut self) -> Option<Envelope> {
        self.connection.as_mut()?.recv().await
    }

    pub fn try_recv(&mut self) -> Option<Envelope> {
        self.connection.as_mut()?.try_recv()
    }

    pub async fn close(&self) {
        if let Some(connection) = &self.connection {
            connection.close().await;
        }
    }

    pub fn log_startup(&self) {
        info!(
            client = %self.client_id,
            server = %self.server_addr,
            "trading client ready"
        );
    }

    fn connection(&self) -> Result<&ClientConnection, SendError> {
        self.connection.as_ref().ok_or(SendError::Closed)
    }

    fn reconnect_envelopes(&self) -> Vec<Envelope> {
        let mut envelopes = vec![self.hello_envelope()];
        if let Some(credentials) = &self.login_credentials {
            envelopes.push(
                Envelope::new(Message::AccountLogin(AccountLoginRequest {
                    credentials: credentials.clone(),
                }))
                .with_client(self.client_id.clone()),
            );
        }
        envelopes
    }

    fn update_reconnect_envelopes(&self) -> Result<(), SendError> {
        self.connection()?
            .set_reconnect_envelopes(&self.reconnect_envelopes())
            .map_err(|e| SendError::Other(e.to_string()))
    }
}
