//! Shared error types.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CtpError {
    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("account not found: {0}")]
    AccountNotFound(String),

    #[error("account not logged in: {0}")]
    AccountNotLoggedIn(String),

    #[error("client not found: {0}")]
    ClientNotFound(String),

    #[error("permission denied: {0}")]
    PermissionDenied(String),

    #[error("ctp api error: {0}")]
    CtpApi(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, CtpError>;
