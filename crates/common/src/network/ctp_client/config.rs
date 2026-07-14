//! CTP MD client configuration.

use std::path::PathBuf;

/// Configuration for the external CTP market-data client.
#[derive(Debug, Clone)]
pub struct CtpClientConfig {
    /// Path to `thostmduserapi_se.so` (or OpenCTP equivalent).
    pub dynlib_path: PathBuf,
    /// CTP flow-file directory (created if missing).
    pub flow_path: PathBuf,
    /// MD front address, e.g. `tcp://172.16.0.1:10211`.
    pub md_front: String,
    /// Broker id (SimNow: `9999`).
    pub broker_id: String,
    /// Investor / user id used for MD login.
    pub user_id: String,
    /// Password.
    pub password: String,
    /// Use UDP for MD (usually false).
    pub use_udp: bool,
    /// Use multicast (usually false).
    pub use_multicast: bool,
    /// Instruments to subscribe after successful login.
    pub instruments: Vec<String>,
    /// Event channel capacity.
    pub event_buffer: usize,
}

impl Default for CtpClientConfig {
    fn default() -> Self {
        Self {
            dynlib_path: PathBuf::from("./lib/thostmduserapi_se.so"),
            flow_path: PathBuf::from("./flow/md_"),
            md_front: "tcp://172.16.0.1:10211".into(),
            broker_id: "9999".into(),
            user_id: String::new(),
            password: String::new(),
            use_udp: false,
            use_multicast: false,
            instruments: Vec::new(),
            event_buffer: 4096,
        }
    }
}

impl CtpClientConfig {
    pub fn new(dynlib_path: impl Into<PathBuf>, md_front: impl Into<String>) -> Self {
        Self {
            dynlib_path: dynlib_path.into(),
            md_front: md_front.into(),
            ..Self::default()
        }
    }

    pub fn with_credentials(
        mut self,
        broker_id: impl Into<String>,
        user_id: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.broker_id = broker_id.into();
        self.user_id = user_id.into();
        self.password = password.into();
        self
    }

    pub fn with_instruments(
        mut self,
        instruments: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.instruments = instruments.into_iter().map(Into::into).collect();
        self
    }
}
