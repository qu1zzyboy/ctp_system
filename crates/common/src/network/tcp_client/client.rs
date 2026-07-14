//! High-performance internal TCP client with reconnect (nautilus-style).
//!
//! **Design**
//! - Single reader task, writer task fed by an unbounded channel
//! - Controller task drives lifecycle / reconnect
//! - Length-prefixed framing (u32 BE + payload)

use std::{
    fmt::Debug,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, error, info, warn};

use super::{TcpClientConfig, TcpMessageHandler, TcpReader, TcpWriter, WriterCommand};
use crate::network::{ConnectionMode, ExponentialBackoff, SendError};

const CONNECTION_STATE_CHECK_INTERVAL_MS: u64 = 10;
const GRACEFUL_SHUTDOWN_TIMEOUT_SECS: u64 = 5;
const CONNECTION_TIMEOUT_SECS: u64 = 10;
const CONTROLLER_FALLBACK_INTERVAL_MS: u64 = 100;

struct TcpClientInner {
    config: TcpClientConfig,
    read_task: tokio::task::JoinHandle<()>,
    write_task: tokio::task::JoinHandle<()>,
    writer_tx: tokio::sync::mpsc::UnboundedSender<WriterCommand>,
    heartbeat_task: Option<tokio::task::JoinHandle<()>>,
    connection_mode: Arc<AtomicU8>,
    state_notify: Arc<tokio::sync::Notify>,
    reconnect_timeout: Duration,
    backoff: ExponentialBackoff,
    handler: Option<TcpMessageHandler>,
    reconnect_max_attempts: Option<u32>,
    reconnect_attempt_count: u32,
}

impl TcpClientInner {
    async fn connect_url(config: TcpClientConfig) -> anyhow::Result<Self> {
        let max_retries = config.connection_max_retries.unwrap_or(5);
        #[allow(unused_assignments)]
        let mut last_error = String::new();
        let mut attempt = 0u32;

        let stream = loop {
            attempt += 1;
            match tokio::time::timeout(
                Duration::from_secs(CONNECTION_TIMEOUT_SECS),
                TcpStream::connect(&config.url),
            )
            .await
            {
                Ok(Ok(s)) => {
                    if attempt > 1 {
                        info!(attempt, url = %config.url, "tcp connected after retries");
                    }
                    break s;
                }
                Ok(Err(e)) => {
                    last_error = e.to_string();
                    warn!(
                        attempt,
                        max_retries,
                        url = %config.url,
                        error = %last_error,
                        "tcp connect failed"
                    );
                }
                Err(_) => {
                    last_error = format!("connect timeout after {CONNECTION_TIMEOUT_SECS}s");
                    warn!(attempt, max_retries, url = %config.url, "tcp connect timed out");
                }
            }

            if attempt >= max_retries {
                anyhow::bail!(
                    "failed to connect to {} after {} attempts: {}",
                    config.url,
                    max_retries,
                    last_error
                );
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        };

        stream.set_nodelay(true)?;
        let (reader, writer) = stream.into_split();

        let connection_mode = Arc::new(AtomicU8::new(ConnectionMode::Active.as_u8()));
        let state_notify = Arc::new(tokio::sync::Notify::new());
        let (writer_tx, writer_rx) = tokio::sync::mpsc::unbounded_channel();

        let handler = config.message_handler.clone();
        let read_mode = connection_mode.clone();
        let read_notify = state_notify.clone();
        let read_handler = handler.clone();
        let read_task = tokio::spawn(async move {
            Self::run_read_loop(reader, read_handler, read_mode, read_notify).await;
        });

        let write_mode = connection_mode.clone();
        let write_task = tokio::spawn(async move {
            Self::run_write_loop(writer, writer_rx, write_mode).await;
        });

        let heartbeat_task = config.heartbeat.as_ref().map(|(interval_secs, payload)| {
            let tx = writer_tx.clone();
            let mode = connection_mode.clone();
            let interval = Duration::from_secs(*interval_secs);
            let beat = Bytes::from(payload.clone());
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                loop {
                    ticker.tick().await;
                    if !ConnectionMode::from_atomic(&mode).is_active() {
                        continue;
                    }
                    if tx.send(WriterCommand::Send(beat.clone())).is_err() {
                        break;
                    }
                }
            })
        });

        let reconnect_timeout =
            Duration::from_millis(config.reconnect_timeout_ms.unwrap_or(10_000));
        let backoff = ExponentialBackoff::new(
            Duration::from_millis(config.reconnect_delay_initial_ms.unwrap_or(500)),
            Duration::from_millis(config.reconnect_delay_max_ms.unwrap_or(30_000)),
            config.reconnect_backoff_factor.unwrap_or(2.0),
            config.reconnect_jitter_ms.unwrap_or(250),
            true,
        )?;

        Ok(Self {
            reconnect_max_attempts: config.reconnect_max_attempts,
            config,
            read_task,
            write_task,
            writer_tx,
            heartbeat_task,
            connection_mode,
            state_notify,
            reconnect_timeout,
            backoff,
            handler,
            reconnect_attempt_count: 0,
        })
    }

    async fn run_read_loop(
        mut reader: TcpReader,
        handler: Option<TcpMessageHandler>,
        mode: Arc<AtomicU8>,
        notify: Arc<tokio::sync::Notify>,
    ) {
        loop {
            let mut len_buf = [0u8; 4];
            if let Err(e) = reader.read_exact(&mut len_buf).await {
                warn!(error = %e, "tcp read length failed");
                break;
            }
            let len = u32::from_be_bytes(len_buf) as usize;
            if len > 16 * 1024 * 1024 {
                error!(len, "tcp frame too large, closing read");
                break;
            }
            let mut buf = vec![0u8; len];
            if let Err(e) = reader.read_exact(&mut buf).await {
                warn!(error = %e, "tcp read payload failed");
                break;
            }
            if let Some(ref h) = handler {
                h(&buf);
            }
        }

        // Signal reconnect unless we are already disconnecting / closed.
        let current = ConnectionMode::from_atomic(&mode);
        if current.is_active() {
            mode.store(ConnectionMode::Reconnect.as_u8(), Ordering::SeqCst);
            notify.notify_waiters();
        }
    }

    async fn run_write_loop(
        mut writer: TcpWriter,
        mut rx: tokio::sync::mpsc::UnboundedReceiver<WriterCommand>,
        mode: Arc<AtomicU8>,
    ) {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                WriterCommand::Update(new_writer, ack) => {
                    writer = new_writer;
                    let _ = ack.send(true);
                }
                WriterCommand::Send(payload) => {
                    if !ConnectionMode::from_atomic(&mode).is_active() {
                        continue;
                    }
                    let len = (payload.len() as u32).to_be_bytes();
                    if let Err(e) = writer.write_all(&len).await {
                        warn!(error = %e, "tcp write length failed");
                        break;
                    }
                    if let Err(e) = writer.write_all(&payload).await {
                        warn!(error = %e, "tcp write payload failed");
                        break;
                    }
                    if let Err(e) = writer.flush().await {
                        warn!(error = %e, "tcp flush failed");
                        break;
                    }
                }
            }
        }
    }

    async fn reconnect(&mut self) -> anyhow::Result<()> {
        self.reconnect_attempt_count += 1;
        if let Some(max) = self.reconnect_max_attempts {
            if self.reconnect_attempt_count > max {
                anyhow::bail!("reconnect max attempts ({max}) exceeded");
            }
        }

        let delay = self.backoff.next_duration();
        if !delay.is_zero() {
            debug!(
                ?delay,
                attempt = self.reconnect_attempt_count,
                "tcp reconnect backoff"
            );
            tokio::time::sleep(delay).await;
        }

        info!(url = %self.config.url, attempt = self.reconnect_attempt_count, "tcp reconnecting");

        let stream = tokio::time::timeout(
            Duration::from_secs(CONNECTION_TIMEOUT_SECS),
            TcpStream::connect(&self.config.url),
        )
        .await
        .map_err(|_| anyhow::anyhow!("reconnect timeout"))?
        .map_err(|e| anyhow::anyhow!("reconnect failed: {e}"))?;

        stream.set_nodelay(true)?;
        let (reader, writer) = stream.into_split();

        // Swap writer
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        self.writer_tx
            .send(WriterCommand::Update(writer, ack_tx))
            .map_err(|e| anyhow::anyhow!("writer channel closed: {e}"))?;
        let _ = ack_rx.await;

        // Restart reader
        if !self.read_task.is_finished() {
            self.read_task.abort();
        }
        let mode = self.connection_mode.clone();
        let notify = self.state_notify.clone();
        let handler = self.handler.clone();
        self.read_task = tokio::spawn(async move {
            Self::run_read_loop(reader, handler, mode, notify).await;
        });

        self.backoff.reset();
        self.reconnect_attempt_count = 0;
        self.connection_mode
            .store(ConnectionMode::Active.as_u8(), Ordering::SeqCst);
        self.state_notify.notify_waiters();
        if let Some(payloads) = &self.config.reconnect_payloads {
            match payloads.lock() {
                Ok(payloads) => {
                    for payload in payloads.iter().cloned() {
                        if self
                            .writer_tx
                            .send(WriterCommand::Send(payload.into()))
                            .is_err()
                        {
                            warn!("tcp reconnect payload replay failed: writer channel closed");
                            break;
                        }
                    }
                    if !payloads.is_empty() {
                        info!(count = payloads.len(), "tcp reconnect payloads replayed");
                    }
                }
                Err(e) => warn!(error = %e, "tcp reconnect payload lock poisoned"),
            }
        }
        info!(url = %self.config.url, "tcp reconnected");
        Ok(())
    }
}

/// Internal TCP messaging client (Client ↔ Server).
pub struct TcpClient {
    controller_task: tokio::task::JoinHandle<()>,
    connection_mode: Arc<AtomicU8>,
    state_notify: Arc<tokio::sync::Notify>,
    reconnect_timeout: Duration,
    writer_tx: tokio::sync::mpsc::UnboundedSender<WriterCommand>,
}

impl Debug for TcpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpClient")
            .field("mode", &self.connection_mode())
            .finish()
    }
}

impl TcpClient {
    /// Connect to the server.
    pub async fn connect(
        config: TcpClientConfig,
        post_connection: Option<Arc<dyn Fn() + Send + Sync>>,
        post_reconnection: Option<Arc<dyn Fn() + Send + Sync>>,
        post_disconnection: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> anyhow::Result<Self> {
        let inner = TcpClientInner::connect_url(config).await?;
        let writer_tx = inner.writer_tx.clone();
        let connection_mode = inner.connection_mode.clone();
        let state_notify = inner.state_notify.clone();
        let reconnect_timeout = inner.reconnect_timeout;

        let controller_task = Self::spawn_controller(
            inner,
            connection_mode.clone(),
            state_notify.clone(),
            post_reconnection,
            post_disconnection,
        );

        if let Some(handler) = post_connection {
            handler();
            debug!("called post_connection handler");
        }

        Ok(Self {
            controller_task,
            connection_mode,
            state_notify,
            reconnect_timeout,
            writer_tx,
        })
    }

    #[must_use]
    pub fn connection_mode(&self) -> ConnectionMode {
        ConnectionMode::from_atomic(&self.connection_mode)
    }

    #[inline]
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.connection_mode().is_active()
    }

    #[inline]
    #[must_use]
    pub fn is_reconnecting(&self) -> bool {
        self.connection_mode().is_reconnect()
    }

    #[inline]
    #[must_use]
    pub fn is_disconnecting(&self) -> bool {
        self.connection_mode().is_disconnect()
    }

    #[inline]
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.connection_mode().is_closed()
    }

    /// Graceful close.
    pub async fn close(&self) {
        self.connection_mode
            .store(ConnectionMode::Disconnect.as_u8(), Ordering::SeqCst);
        self.state_notify.notify_waiters();

        let result =
            tokio::time::timeout(Duration::from_secs(GRACEFUL_SHUTDOWN_TIMEOUT_SECS), async {
                while !self.is_closed() {
                    tokio::time::sleep(Duration::from_millis(CONNECTION_STATE_CHECK_INTERVAL_MS))
                        .await;
                }
                if !self.controller_task.is_finished() {
                    self.controller_task.abort();
                }
            })
            .await;

        if result.is_err() {
            error!("timeout waiting for tcp controller to finish");
            if !self.controller_task.is_finished() {
                self.controller_task.abort();
            }
            self.connection_mode
                .store(ConnectionMode::Closed.as_u8(), Ordering::SeqCst);
        }
    }

    fn check_not_terminal(&self) -> Result<(), SendError> {
        match self.connection_mode() {
            ConnectionMode::Disconnect | ConnectionMode::Closed => Err(SendError::Closed),
            _ => Ok(()),
        }
    }

    async fn wait_for_active(&self) -> Result<(), SendError> {
        const FALLBACK_INTERVAL_MS: u64 = 100;

        let mode = self.connection_mode();
        if mode.is_active() {
            return Ok(());
        }
        if matches!(mode, ConnectionMode::Disconnect | ConnectionMode::Closed) {
            return Err(SendError::Closed);
        }

        let fallback = Duration::from_millis(FALLBACK_INTERVAL_MS);
        tokio::time::timeout(self.reconnect_timeout, async {
            loop {
                let notified = self.state_notify.notified();
                let mode = self.connection_mode();
                if mode.is_active() {
                    return Ok(());
                }
                if matches!(mode, ConnectionMode::Disconnect | ConnectionMode::Closed) {
                    return Err(());
                }
                tokio::select! {
                    biased;
                    () = notified => {}
                    () = tokio::time::sleep(fallback) => {}
                }
            }
        })
        .await
        .map_err(|_| SendError::Timeout)?
        .map_err(|()| SendError::Closed)
    }

    /// Enqueue a framed payload (length prefix applied by the writer).
    pub async fn send_bytes(&self, data: Vec<u8>) -> Result<(), SendError> {
        self.check_not_terminal()?;
        self.wait_for_active().await?;
        self.writer_tx
            .send(WriterCommand::Send(data.into()))
            .map_err(|e| SendError::BrokenPipe(e.to_string()))
    }

    /// Convenience: serialize and send a protocol [`crate::Envelope`].
    pub async fn send_envelope(&self, envelope: &crate::Envelope) -> Result<(), SendError> {
        let data = serde_json::to_vec(envelope).map_err(|e| SendError::Other(e.to_string()))?;
        self.send_bytes(data).await
    }

    fn spawn_controller(
        mut inner: TcpClientInner,
        connection_mode: Arc<AtomicU8>,
        state_notify: Arc<tokio::sync::Notify>,
        post_reconnection: Option<Arc<dyn Fn() + Send + Sync>>,
        post_disconnection: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let fallback = Duration::from_millis(CONTROLLER_FALLBACK_INTERVAL_MS);
            loop {
                tokio::select! {
                    biased;
                    () = state_notify.notified() => {}
                    () = tokio::time::sleep(fallback) => {}
                }

                let mode = ConnectionMode::from_atomic(&connection_mode);

                if mode.is_disconnect() {
                    if let Some(ref hb) = inner.heartbeat_task {
                        hb.abort();
                    }
                    if !inner.read_task.is_finished() {
                        inner.read_task.abort();
                    }
                    if !inner.write_task.is_finished() {
                        inner.write_task.abort();
                    }
                    connection_mode.store(ConnectionMode::Closed.as_u8(), Ordering::SeqCst);
                    state_notify.notify_waiters();
                    if let Some(ref h) = post_disconnection {
                        h();
                    }
                    break;
                }

                if mode.is_reconnect() {
                    match inner.reconnect().await {
                        Ok(()) => {
                            if let Some(ref h) = post_reconnection {
                                h();
                            }
                        }
                        Err(e) => {
                            error!(error = %e, "tcp reconnect failed");
                            if let Some(max) = inner.reconnect_max_attempts {
                                if inner.reconnect_attempt_count > max {
                                    connection_mode
                                        .store(ConnectionMode::Closed.as_u8(), Ordering::SeqCst);
                                    state_notify.notify_waiters();
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        })
    }
}
