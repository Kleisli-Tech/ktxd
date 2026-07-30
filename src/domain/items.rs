use crate::ids::{ArtifactHash, CallId, ItemId};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ProvenanceSource {
    User,
    Model,
    ToolOutput,
    Replay,
    Seed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum TrustLevel {
    Trusted,
    Semi,
    Untrusted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProvenanceTag {
    pub source: ProvenanceSource,
    pub trust: TrustLevel,
}

impl ProvenanceTag {
    pub fn user_trusted() -> Self {
        Self {
            source: ProvenanceSource::User,
            trust: TrustLevel::Trusted,
        }
    }

    pub fn model_semi() -> Self {
        Self {
            source: ProvenanceSource::Model,
            trust: TrustLevel::Semi,
        }
    }

    pub fn tool_output_semi() -> Self {
        Self {
            source: ProvenanceSource::ToolOutput,
            trust: TrustLevel::Semi,
        }
    }

    pub fn derive(&self, source: ProvenanceSource) -> Self {
        Self {
            source,
            trust: self.trust,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaggedItem {
    pub id: ItemId,
    pub item: CanonicalItem,
    pub provenance: ProvenanceTag,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact_hash: Option<ArtifactHash>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CanonicalItem {
    Message {
        role: MessageRole,
        text: String,
    },
    FunctionCall {
        call_id: CallId,
        name: String,
        arguments: String,
    },
    FunctionCallOutput {
        call_id: CallId,
        output: FunctionOutput,
    },
    Reasoning {
        raw: Value,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    System,
    Developer,
    User,
    Assistant,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "encoding", rename_all = "snake_case")]
pub enum FunctionOutput {
    Text {
        text: String,
    },
    ContentItems {
        items: Vec<FunctionOutputContentItem>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FunctionOutputContentItem {
    InputText { text: String },
    InputImage { image_url: String },
}

impl FunctionOutput {
    pub fn lower_to_chat_content(&self) -> String {
        match self {
            Self::Text { text } => text.clone(),
            Self::ContentItems { items } => items
                .iter()
                .filter_map(|item| match item {
                    FunctionOutputContentItem::InputText { text } if !text.trim().is_empty() => {
                        Some(text.clone())
                    }
                    FunctionOutputContentItem::InputText { .. }
                    | FunctionOutputContentItem::InputImage { .. } => None,
                })
                .collect::<Vec<_>>()
                .join("\n"),
        }
    }
}

impl TaggedItem {
    pub fn new(item: CanonicalItem, provenance: ProvenanceTag) -> Self {
        Self {
            id: ItemId::new(),
            item,
            provenance,
            artifact_hash: None,
        }
    }
}

pub fn trust_monotone(parent: TrustLevel, child: TrustLevel) -> bool {
    child >= parent
}
