//! Shared outbound TCP connection wrapper.

use std::sync::{Arc, Mutex};

use ctp_common::{ConnectionMode, Envelope, SendError, TcpClient, TcpClientConfig};
use tokio::sync::mpsc;
use tracing::{info, warn};

/// JSON envelope connection over the shared length-prefixed TCP client.
pub struct ClientConnection {
    transport: TcpClient,
    inbound_rx: mpsc::UnboundedReceiver<Envelope>,
    reconnect_payloads: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl std::fmt::Debug for ClientConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientConnection")
            .field("transport", &self.transport)
            .finish()
    }
}

impl ClientConnection {
    pub async fn connect(server_addr: impl Into<String>) -> anyhow::Result<Self> {
        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
        let reconnect_payloads = Arc::new(Mutex::new(Vec::new()));
        let config = TcpClientConfig::new(server_addr)
            .with_handler(
                move |payload| match serde_json::from_slice::<Envelope>(payload) {
                    Ok(envelope) => {
                        if inbound_tx.send(envelope).is_err() {
                            warn!("inbound envelope receiver closed");
                        }
                    }
                    Err(e) => warn!(error = %e, "failed to decode inbound envelope"),
                },
            )
            .with_reconnect_payloads(reconnect_payloads.clone());

        let transport = TcpClient::connect(config, None, None, None).await?;

        Ok(Self {
            transport,
            inbound_rx,
            reconnect_payloads,
        })
    }

    #[must_use]
    pub fn connection_mode(&self) -> ConnectionMode {
        self.transport.connection_mode()
    }

    pub async fn send(&self, envelope: &Envelope) -> Result<(), SendError> {
        self.transport.send_envelope(envelope).await
    }

    pub fn set_reconnect_envelopes(&self, envelopes: &[Envelope]) -> anyhow::Result<()> {
        let payloads = envelopes
            .iter()
            .map(serde_json::to_vec)
            .collect::<Result<Vec<_>, _>>()?;
        let mut reconnect_payloads = self
            .reconnect_payloads
            .lock()
            .map_err(|e| anyhow::anyhow!("reconnect payload lock poisoned: {e}"))?;
        *reconnect_payloads = payloads;
        info!(
            count = reconnect_payloads.len(),
            "client reconnect envelopes updated"
        );
        Ok(())
    }

    pub async fn recv(&mut self) -> Option<Envelope> {
        self.inbound_rx.recv().await
    }

    pub fn try_recv(&mut self) -> Option<Envelope> {
        self.inbound_rx.try_recv().ok()
    }

    pub async fn close(&self) {
        self.transport.close().await;
    }
}
