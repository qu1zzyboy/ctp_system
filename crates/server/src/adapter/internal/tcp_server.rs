//! TCP accept loops for trading and market-data terminals.
//!
//! Framing matches [`ctp_common::network::TcpClient`]: `u32 BE length + payload`
//! (JSON [`ctp_common::Envelope`] by default).

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{tcp::OwnedReadHalf, tcp::OwnedWriteHalf, TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::actor::{
    remove_exchange_market_data_client, subscribe_exchange_market_data,
    unsubscribe_exchange_market_data, EnvelopeTx, SharedServerActorState,
};

#[derive(Debug, Clone, Copy)]
pub enum TcpEndpointKind {
    Trading,
    MarketData,
}

/// Bound TCP server that accepts Client connections.
#[derive(Debug)]
pub struct TcpServer {
    listener: TcpListener,
    kind: TcpEndpointKind,
}

impl TcpServer {
    pub async fn bind(addr: impl AsRef<str>, kind: TcpEndpointKind) -> anyhow::Result<Self> {
        let listener = TcpListener::bind(addr.as_ref()).await?;
        info!(addr = %addr.as_ref(), ?kind, "internal tcp listening");
        Ok(Self { listener, kind })
    }

    pub fn local_addr(&self) -> anyhow::Result<std::net::SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    /// Accept connections forever, spawning one actor task per client.
    pub async fn run(self, state: SharedServerActorState) -> anyhow::Result<()> {
        loop {
            let (stream, peer) = self.listener.accept().await?;
            info!(%peer, kind = ?self.kind, "client connected");
            let state = state.clone();
            let kind = self.kind;
            tokio::spawn(async move {
                if let Err(e) = handle_client(stream, peer, kind, state).await {
                    warn!(%peer, ?kind, error = %e, "client session ended");
                }
            });
        }
    }
}

async fn handle_client(
    stream: TcpStream,
    peer: std::net::SocketAddr,
    kind: TcpEndpointKind,
    state: SharedServerActorState,
) -> anyhow::Result<()> {
    stream.set_nodelay(true)?;
    let (reader, writer) = stream.into_split();
    let (outbound_tx, outbound_rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        if let Err(e) = write_loop(writer, outbound_rx).await {
            warn!(%peer, error = %e, "client write loop ended");
        }
    });

    read_loop(reader, peer, kind, state, outbound_tx).await
}

async fn read_loop(
    mut reader: OwnedReadHalf,
    peer: std::net::SocketAddr,
    kind: TcpEndpointKind,
    state: SharedServerActorState,
    outbound_tx: EnvelopeTx,
) -> anyhow::Result<()> {
    let mut connected_client_id: Option<ctp_model::ClientId> = None;
    loop {
        let mut len_buf = [0u8; 4];
        if let Err(e) = reader.read_exact(&mut len_buf).await {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                info!(%peer, ?kind, "client disconnected");
                if matches!(kind, TcpEndpointKind::MarketData) {
                    if let Some(client_id) = connected_client_id.as_ref() {
                        if let Err(e) = remove_exchange_market_data_client(&state, client_id).await
                        {
                            warn!(%peer, client = %client_id, error = %e, "market-data client cleanup failed");
                        }
                    }
                }
                break;
            }
            return Err(e.into());
        }

        let len = u32::from_be_bytes(len_buf) as usize;
        if len > 16 * 1024 * 1024 {
            anyhow::bail!("frame too large: {len}");
        }

        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf).await?;

        let envelope: ctp_common::Envelope = serde_json::from_slice(&buf)?;
        if matches!(kind, TcpEndpointKind::MarketData) {
            if let ctp_common::Message::MarketDataHello(req) = &envelope.payload {
                connected_client_id = Some(req.client_id.clone());
            }
        }
        handle_envelope(kind, peer, &state, outbound_tx.clone(), envelope).await?;
    }

    Ok(())
}

async fn write_loop(
    mut writer: OwnedWriteHalf,
    mut outbound_rx: mpsc::UnboundedReceiver<ctp_common::Envelope>,
) -> anyhow::Result<()> {
    while let Some(envelope) = outbound_rx.recv().await {
        let payload = serde_json::to_vec(&envelope)?;
        let len = (payload.len() as u32).to_be_bytes();
        writer.write_all(&len).await?;
        writer.write_all(&payload).await?;
        writer.flush().await?;
    }
    Ok(())
}

async fn handle_envelope(
    kind: TcpEndpointKind,
    peer: std::net::SocketAddr,
    state: &SharedServerActorState,
    outbound_tx: EnvelopeTx,
    envelope: ctp_common::Envelope,
) -> anyhow::Result<()> {
    use ctp_common::{Envelope, HelloAck, Message};

    match (kind, envelope.payload) {
        (TcpEndpointKind::Trading, Message::Hello(req)) => {
            state.lock().await.register_trading_client(
                req.client_id.clone(),
                peer,
                outbound_tx.clone(),
            );
            send(
                &outbound_tx,
                Envelope::new(Message::HelloAck(HelloAck {
                    accepted: true,
                    message: "trading client registered".into(),
                })),
            )?;
        }
        (TcpEndpointKind::Trading, Message::AccountLogin(req)) => {
            let Some(client_id) = envelope.client_id else {
                send_error(&outbound_tx, -1, "missing client_id for account login")?;
                return Ok(());
            };
            let state_update = match state.lock().await.bind_account(
                &client_id,
                req.credentials,
                outbound_tx.clone(),
                state.clone(),
            ) {
                Ok(update) => update,
                Err(e) => {
                    send_error(&outbound_tx, -20, &e.to_string())?;
                    return Ok(());
                }
            };
            send(
                &outbound_tx,
                Envelope::new(Message::AccountStateUpdate(state_update)),
            )?;
        }
        (TcpEndpointKind::Trading, Message::PlaceOrder(req)) => {
            let Some(client_id) = envelope.client_id else {
                send_error(&outbound_tx, -1, "missing client_id for place order")?;
                return Ok(());
            };
            if let Err(e) = state
                .lock()
                .await
                .place_order(client_id, req, outbound_tx.clone())
            {
                send_error(&outbound_tx, -10, &e.to_string())?;
            }
        }
        (TcpEndpointKind::Trading, Message::CancelOrder(req)) => {
            let Some(client_id) = envelope.client_id else {
                send_error(&outbound_tx, -1, "missing client_id for cancel order")?;
                return Ok(());
            };
            if let Err(e) = state
                .lock()
                .await
                .cancel_order(client_id, req, outbound_tx.clone())
            {
                send_error(&outbound_tx, -10, &e.to_string())?;
            }
        }
        (TcpEndpointKind::Trading, Message::QueryAccount(req)) => {
            let Some(client_id) = envelope.client_id else {
                send_error(&outbound_tx, -1, "missing client_id for query account")?;
                return Ok(());
            };
            if let Err(e) =
                state
                    .lock()
                    .await
                    .query_account(client_id, req.account_id, outbound_tx.clone())
            {
                send_error(&outbound_tx, -10, &e.to_string())?;
            }
        }
        (TcpEndpointKind::Trading, Message::QueryPosition(req)) => {
            let Some(client_id) = envelope.client_id else {
                send_error(&outbound_tx, -1, "missing client_id for query position")?;
                return Ok(());
            };
            if let Err(e) = state.lock().await.query_position(
                client_id,
                req.account_id,
                req.instrument_id,
                outbound_tx.clone(),
            ) {
                send_error(&outbound_tx, -10, &e.to_string())?;
            }
        }
        (TcpEndpointKind::Trading, Message::QueryOrders(req)) => {
            let Some(client_id) = envelope.client_id else {
                send_error(&outbound_tx, -1, "missing client_id for query orders")?;
                return Ok(());
            };
            if let Err(e) =
                state
                    .lock()
                    .await
                    .query_orders(client_id, req.account_id, outbound_tx.clone())
            {
                send_error(&outbound_tx, -10, &e.to_string())?;
            }
        }
        (TcpEndpointKind::Trading, Message::QueryTrades(req)) => {
            let Some(client_id) = envelope.client_id else {
                send_error(&outbound_tx, -1, "missing client_id for query trades")?;
                return Ok(());
            };
            if let Err(e) =
                state
                    .lock()
                    .await
                    .query_trades(client_id, req.account_id, outbound_tx.clone())
            {
                send_error(&outbound_tx, -10, &e.to_string())?;
            }
        }
        (TcpEndpointKind::MarketData, Message::MarketDataHello(req)) => {
            state.lock().await.register_market_data_client(
                req.client_id.clone(),
                peer,
                outbound_tx.clone(),
            );
            send(
                &outbound_tx,
                Envelope::new(Message::HelloAck(HelloAck {
                    accepted: true,
                    message: "market-data client registered".into(),
                })),
            )?;
        }
        (TcpEndpointKind::MarketData, Message::SubscribeMarketData(req)) => {
            let Some(client_id) = envelope.client_id else {
                send_error(
                    &outbound_tx,
                    -1,
                    "missing client_id for market-data subscribe",
                )?;
                return Ok(());
            };
            subscribe_exchange_market_data(state, &client_id, req.instruments).await?;
        }
        (TcpEndpointKind::MarketData, Message::UnsubscribeMarketData(req)) => {
            let Some(client_id) = envelope.client_id else {
                send_error(
                    &outbound_tx,
                    -1,
                    "missing client_id for market-data unsubscribe",
                )?;
                return Ok(());
            };
            unsubscribe_exchange_market_data(state, &client_id, req.instruments).await?;
        }
        (TcpEndpointKind::Trading, Message::AccountLogout(req)) => {
            let Some(client_id) = envelope.client_id else {
                send_error(&outbound_tx, -1, "missing client_id for account logout")?;
                return Ok(());
            };
            if let Err(e) =
                state
                    .lock()
                    .await
                    .logout_account(client_id, req.account_id, outbound_tx.clone())
            {
                send_error(&outbound_tx, -10, &e.to_string())?;
            }
        }
        (_, other) => {
            warn!(?kind, ?other, "message received on unsupported endpoint");
            send_error(&outbound_tx, -2, "message unsupported on this endpoint")?;
        }
    }

    Ok(())
}

fn send(outbound_tx: &EnvelopeTx, envelope: ctp_common::Envelope) -> anyhow::Result<()> {
    outbound_tx
        .send(envelope)
        .map_err(|e| anyhow::anyhow!("client outbound channel closed: {e}"))
}

fn send_error(outbound_tx: &EnvelopeTx, code: i32, message: &str) -> anyhow::Result<()> {
    send(
        outbound_tx,
        ctp_common::Envelope::new(ctp_common::Message::Error(ctp_common::ErrorResponse {
            code,
            message: message.into(),
        })),
    )
}
