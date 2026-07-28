use crate::domain::TaggedItem;
use crate::ids::{ResponseId, SessionVersion, TenantId, TurnId};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CanonicalTranscript {
    pub items: Vec<TaggedItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Session {
    pub response_id: ResponseId,
    pub parent_response_id: Option<ResponseId>,
    pub tenant_id: TenantId,
    pub version: SessionVersion,
    pub committed_items: Vec<TaggedItem>,
    pub deterministic_fingerprint: String,
    pub final_response_json: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TurnRecord {
    pub turn_id: TurnId,
    pub response_id: ResponseId,
    pub parent_response_id: Option<ResponseId>,
    pub outcome: TurnOutcome,
    pub request_items: Vec<TaggedItem>,
    pub output_items: Vec<TaggedItem>,
    pub usage: UsageTotals,
    pub error_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    pub deterministic_fingerprint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TurnOutcome {
    Completed,
    Incomplete,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct UsageTotals {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
}
