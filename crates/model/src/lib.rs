//! Trading domain model (nautilus-style).
//!
//! Agnostic of C/S transport and CTP adapter details. Represents trading
//! state, identifiers, orders, positions, and account balances.

pub mod ctp;
pub mod enums;
pub mod identifiers;
pub mod reports;
pub mod types;

pub use ctp::*;
pub use enums::*;
pub use identifiers::*;
pub use reports::*;
pub use types::*;
