//! TCP client types.

use std::sync::Arc;

use bytes::Bytes;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

pub type TcpReader = OwnedReadHalf;
pub type TcpWriter = OwnedWriteHalf;
pub type TcpMessageHandler = Arc<dyn Fn(&[u8]) + Send + Sync>;

/// Commands for the writer task.
#[derive(Debug)]
pub enum WriterCommand {
    /// Replace the writer after reconnect.
    Update(TcpWriter, tokio::sync::oneshot::Sender<bool>),
    /// Send a framed payload (length prefix applied by writer).
    Send(Bytes),
}
