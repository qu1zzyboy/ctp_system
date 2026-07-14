//! Logging bootstrap (tracing).

use tracing_subscriber::{fmt, prelude::*, EnvFilter};

/// Initialize stdout logging. `RUST_LOG` overrides the default filter.
pub fn init_logging(default_filter: &str) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().with_target(true).with_thread_ids(false))
        .init();
}
