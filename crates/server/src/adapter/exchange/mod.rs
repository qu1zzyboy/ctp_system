//! Exchange (CTP) adapter — external venue connectivity.
//!
//! Server owns **1 MD + N Trade** sessions. Dynamic libraries are loaded at
//! runtime via `libloading`.

pub mod mapping;
pub mod md;
pub mod trade;

use std::path::{Path, PathBuf};

use tracing::info;

pub use md::{MdSession, MdSessionConfig};
pub use trade::{TradeSession, TradeSessionConfig, TradeSessionEvent};

/// Default relative names for official CTP SE shared libraries (Linux).
pub const MD_DYNLIB_NAME: &str = "thostmduserapi_se.so";
pub const TD_DYNLIB_NAME: &str = "thosttraderapi_se.so";

/// Resolve MD / Trade dynlib paths from a directory.
pub fn resolve_dynlib_paths(dir: impl AsRef<Path>) -> (PathBuf, PathBuf) {
    let dir = dir.as_ref();
    (dir.join(MD_DYNLIB_NAME), dir.join(TD_DYNLIB_NAME))
}

/// Process-wide exchange gateway: one shared MD + per-account Trade sessions.
#[derive(Debug, Default)]
pub struct ExchangeGateway {
    pub md: Option<MdSession>,
    pub trades: Vec<TradeSession>,
}

impl ExchangeGateway {
    pub fn new() -> Self {
        Self::default()
    }

    /// Compile-time / startup probe that `ctp2rs` is linked.
    pub fn probe() {
        let _ = std::any::type_name::<ctp2rs::v1alpha1::MdApi>();
        let _ = std::any::type_name::<ctp2rs::v1alpha1::TraderApi>();
        let _ = std::any::type_name::<ctp2rs::v1alpha1::CThostFtdcDepthMarketDataField>();
        info!("exchange adapter: ctp2rs linked (Md/Trade wiring TODO)");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dynlib_names_resolve() {
        let (md, td) = resolve_dynlib_paths("/opt/ctp");
        assert!(md.ends_with(MD_DYNLIB_NAME));
        assert!(td.ends_with(TD_DYNLIB_NAME));
    }
}
