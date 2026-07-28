use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

macro_rules! string_newtype {
    ($name:ident, $prefix:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl $name {
            pub fn new() -> Self {
                Self(format!("{}{}", $prefix, Uuid::new_v4().simple()))
            }
            pub fn from_string(value: impl Into<String>) -> Self {
                Self(value.into())
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

string_newtype!(ResponseId, "resp_");
string_newtype!(ItemId, "item_");
string_newtype!(CallId, "call_");
string_newtype!(TurnId, "turn_");
string_newtype!(TenantId, "tenant_");
string_newtype!(NodeId, "node_");
string_newtype!(ArtifactHash, "blake3:");

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionVersion(pub u64);

impl Default for SessionVersion {
    fn default() -> Self {
        Self(1)
    }
}
