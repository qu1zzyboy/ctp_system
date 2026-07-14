//! Market-data connection to `ctp-server`.

use std::collections::BTreeMap;

use ctp_common::{
    Envelope, MarketDataHelloRequest, Message, SendError, SubscribeMarketDataRequest,
    UnsubscribeMarketDataRequest,
};
use ctp_model::{ClientId, InstrumentId};
use tracing::info;

use crate::connection::ClientConnection;

/// Client-side facade for market-data subscriptions and tick pushes.
#[derive(Debug)]
pub struct MarketDataClient {
    pub client_id: ClientId,
    pub server_addr: String,
    connection: Option<ClientConnection>,
    subscriptions: BTreeMap<String, InstrumentId>,
}

impl MarketDataClient {
    pub fn new(client_id: impl Into<ClientId>, server_addr: impl Into<String>) -> Self {
        Self {
            client_id: client_id.into(),
            server_addr: server_addr.into(),
            connection: None,
            subscriptions: BTreeMap::new(),
        }
    }

    pub async fn connect(&mut self) -> anyhow::Result<()> {
        let connection = ClientConnection::connect(self.server_addr.clone()).await?;
        self.connection = Some(connection);
        self.send_hello().await?;
        self.update_reconnect_envelopes()?;
        Ok(())
    }

    pub fn hello_envelope(&self) -> Envelope {
        Envelope::new(Message::MarketDataHello(MarketDataHelloRequest {
            client_id: self.client_id.clone(),
        }))
        .with_client(self.client_id.clone())
    }

    pub async fn send_hello(&self) -> Result<(), SendError> {
        self.send_envelope(&self.hello_envelope()).await
    }

    pub async fn subscribe(
        &mut self,
        instruments: impl IntoIterator<Item = InstrumentId>,
    ) -> Result<(), SendError> {
        let instruments = instruments.into_iter().collect::<Vec<_>>();
        self.send_envelope(
            &Envelope::new(Message::SubscribeMarketData(SubscribeMarketDataRequest {
                instruments: instruments.clone(),
            }))
            .with_client(self.client_id.clone()),
        )
        .await?;
        for instrument in instruments {
            self.subscriptions
                .insert(instrument.as_str().to_string(), instrument);
        }
        self.update_reconnect_envelopes()?;
        Ok(())
    }

    pub async fn unsubscribe(
        &mut self,
        instruments: impl IntoIterator<Item = InstrumentId>,
    ) -> Result<(), SendError> {
        let instruments = instruments.into_iter().collect::<Vec<_>>();
        self.send_envelope(
            &Envelope::new(Message::UnsubscribeMarketData(
                UnsubscribeMarketDataRequest {
                    instruments: instruments.clone(),
                },
            ))
            .with_client(self.client_id.clone()),
        )
        .await?;
        for instrument in instruments {
            self.subscriptions.remove(instrument.as_str());
        }
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
            "market-data client ready"
        );
    }

    fn connection(&self) -> Result<&ClientConnection, SendError> {
        self.connection.as_ref().ok_or(SendError::Closed)
    }

    fn reconnect_envelopes(&self) -> Vec<Envelope> {
        let mut envelopes = vec![self.hello_envelope()];
        if !self.subscriptions.is_empty() {
            envelopes.push(
                Envelope::new(Message::SubscribeMarketData(SubscribeMarketDataRequest {
                    instruments: self.subscriptions.values().cloned().collect(),
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
