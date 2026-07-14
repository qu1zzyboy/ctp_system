//! CTP field value mapping into domain enums.
//!
//! Keeps `ctp2rs` / FFI types out of the model crate; server adapters parse
//! native structs and call these helpers to produce normalized values.

use crate::enums::{Direction, OffsetFlag, OrderStatus};

/// CTP `OrderRef` is at most 13 ASCII chars; we normalize to a stable lookup key.
pub fn normalize_order_ref(value: &str) -> String {
    value
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .take(12)
        .collect()
}

/// Map CTP `Direction` (`THOST_FTDC_D_Buy` / `THOST_FTDC_D_Sell`).
pub fn direction_from_ctp(value: i8) -> Option<Direction> {
    match value as u8 as char {
        '0' => Some(Direction::Buy),
        '1' => Some(Direction::Sell),
        _ => None,
    }
}

/// Map CTP offset flag (`CombOffsetFlag[0]`).
pub fn offset_from_ctp(flag: char) -> OffsetFlag {
    match flag {
        '0' => OffsetFlag::Open,
        '1' => OffsetFlag::Close,
        '3' => OffsetFlag::CloseToday,
        '4' => OffsetFlag::CloseYesterday,
        _ => OffsetFlag::Open,
    }
}

/// Map CTP `OrderStatus` char into domain lifecycle status.
pub fn order_status_from_ctp(status: char) -> OrderStatus {
    match status {
        '0' => OrderStatus::Filled,
        '1' | '2' => OrderStatus::PartiallyFilled,
        '3' | '4' => OrderStatus::Accepted,
        '5' => OrderStatus::Cancelled,
        'a' | 'b' | 'c' => OrderStatus::Submitted,
        _ => OrderStatus::Unknown,
    }
}

/// Map CTP `PosiDirection` (`2` net long / `3` net short).
pub fn posi_direction_from_ctp(flag: char) -> Option<Direction> {
    match flag {
        '2' => Some(Direction::Buy),
        '3' => Some(Direction::Sell),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_order_ref_truncates() {
        assert_eq!(normalize_order_ref("abc123!@#"), "abc123");
        assert_eq!(normalize_order_ref("123456789012345"), "123456789012");
    }

    #[test]
    fn order_status_mapping() {
        assert_eq!(order_status_from_ctp('0'), OrderStatus::Filled);
        assert_eq!(order_status_from_ctp('1'), OrderStatus::PartiallyFilled);
        assert_eq!(order_status_from_ctp('5'), OrderStatus::Cancelled);
        assert_eq!(order_status_from_ctp('a'), OrderStatus::Submitted);
    }
}
