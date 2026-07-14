//! Strongly-typed identifiers (nautilus-style).

use serde::{Deserialize, Serialize};
use std::fmt;

macro_rules! id_newtype {
    ($name:ident, $doc:expr) => {
        #[doc = $doc]
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub struct $name(pub String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self::new(value)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self::new(value)
            }
        }
    };
}

id_newtype!(ClientId, "Logical trading terminal / client process id.");
id_newtype!(AccountId, "CTP investor / trading account id.");
id_newtype!(BrokerId, "CTP broker id (e.g. SimNow 9999).");
id_newtype!(InstrumentId, "Instrument / contract id (e.g. rb2510).");
id_newtype!(ClientOrderId, "Client-side order id.");
id_newtype!(ExchangeOrderId, "Exchange / CTP order sys id.");
id_newtype!(RequestId, "Request-response correlation id.");
