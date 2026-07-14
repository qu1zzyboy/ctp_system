//! Network send / connection errors.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SendError {
    #[error("connection closed")]
    Closed,

    #[error("timed out waiting for active connection")]
    Timeout,

    #[error("broken pipe: {0}")]
    BrokenPipe(String),

    #[error("{0}")]
    Other(String),
}
