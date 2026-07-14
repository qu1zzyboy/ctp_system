//! Connection mode enumeration for network clients (nautilus-style).

use std::sync::atomic::{AtomicU8, Ordering};

/// Connection mode for a network client.
///
/// Managed via an atomic flag so reader / writer / controller tasks can
/// coordinate without locks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ConnectionMode {
    /// Fully connected and operational.
    #[default]
    Active = 0,
    /// Lost connection / signalled to reconnect.
    Reconnect = 1,
    /// Explicit disconnect in progress.
    Disconnect = 2,
    /// Permanently closed.
    Closed = 3,
}

impl ConnectionMode {
    #[inline]
    #[must_use]
    pub fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Active,
            1 => Self::Reconnect,
            2 => Self::Disconnect,
            3 => Self::Closed,
            _ => panic!("Invalid ConnectionMode value: {value}"),
        }
    }

    #[inline]
    #[must_use]
    pub fn from_atomic(value: &AtomicU8) -> Self {
        Self::from_u8(value.load(Ordering::SeqCst))
    }

    #[inline]
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    #[inline]
    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Active)
    }

    #[inline]
    #[must_use]
    pub const fn is_reconnect(self) -> bool {
        matches!(self, Self::Reconnect)
    }

    #[inline]
    #[must_use]
    pub const fn is_disconnect(self) -> bool {
        matches!(self, Self::Disconnect)
    }

    #[inline]
    #[must_use]
    pub const fn is_closed(self) -> bool {
        matches!(self, Self::Closed)
    }
}

impl std::fmt::Display for ConnectionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => write!(f, "ACTIVE"),
            Self::Reconnect => write!(f, "RECONNECT"),
            Self::Disconnect => write!(f, "DISCONNECT"),
            Self::Closed => write!(f, "CLOSED"),
        }
    }
}
