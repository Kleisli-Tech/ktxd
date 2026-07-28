use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Deserialize)]
pub struct ResponsesRequest {
    pub model: String,
    #[serde(default)]
    pub instructions: String,
    pub input: ResponsesInput,
    #[serde(default)]
    pub tools: Vec<Value>,
    #[serde(default = "default_tool_choice")]
    pub tool_choice: String,
    #[serde(default)]
    pub parallel_tool_calls: bool,
    #[serde(default)]
    pub reasoning: Option<Value>,
    #[serde(default)]
    pub store: Option<bool>,
    #[serde(default)]
    pub stream: Option<bool>,
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub service_tier: Option<String>,
    #[serde(default)]
    pub prompt_cache_key: Option<String>,
    #[serde(default)]
    pub text: Option<Value>,
    #[serde(default)]
    pub previous_response_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ResponsesInput {
    String(String),
    Items(Vec<Value>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResponseObject {
    pub id: String,
    #[serde(rename = "object")]
    pub object_type: String,
    pub created_at: i64,
    pub model: String,
    pub status: String,
    pub output: Vec<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<ResponsesUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incomplete_details: Option<IncompleteDetails>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResponsesUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IncompleteDetails {
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelsResponse {
    pub models: Vec<ModelInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelInfo {
    pub slug: String,
    pub display_name: String,
    pub description: Option<String>,
    pub default_reasoning_level: Option<String>,
    pub supported_reasoning_levels: Vec<Value>,
    pub shell_type: String,
    pub visibility: String,
    pub supported_in_api: bool,
    pub priority: i32,
    pub availability_nux: Option<Value>,
    pub upgrade: Option<Value>,
    pub base_instructions: String,
    pub model_messages: Option<Value>,
    pub supports_reasoning_summaries: bool,
    pub default_reasoning_summary: String,
    pub support_verbosity: bool,
    pub default_verbosity: Option<Value>,
    pub apply_patch_tool_type: Option<String>,
    pub web_search_tool_type: String,
    pub truncation_policy: TruncationPolicy,
    pub supports_parallel_tool_calls: bool,
    pub supports_image_detail_original: bool,
    pub context_window: Option<i64>,
    pub auto_compact_token_limit: Option<i64>,
    pub effective_context_window_percent: i64,
    pub experimental_supported_tools: Vec<String>,
    pub input_modalities: Vec<String>,
    pub supports_search_tool: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TruncationPolicy {
    pub mode: String,
    pub limit: i64,
}

fn default_tool_choice() -> String {
    "auto".to_string()
}
