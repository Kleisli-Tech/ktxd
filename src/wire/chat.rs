use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChatCompletionRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub messages: Vec<ChatMessage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ChatTool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallel_tool_calls: Option<bool>,
    pub stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_options: Option<StreamOptions>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamOptions {
    pub include_usage: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChatMessage {
    pub role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ChatToolCall>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChatToolCall {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<u32>,
    pub id: String,
    #[serde(rename = "type")]
    pub tool_type: String,
    pub function: ChatFunctionCall,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChatFunctionCall {
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChatTool {
    #[serde(rename = "type")]
    pub tool_type: String,
    pub function: ChatFunctionTool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChatFunctionTool {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub parameters: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct ChatCompletionResponse {
    pub id: Option<String>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub choices: Vec<ChatChoice>,
    #[serde(default)]
    pub usage: Option<ChatUsage>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct ChatChoice {
    pub index: Option<u32>,
    pub message: Option<ChatResponseMessage>,
    pub delta: Option<ChatDelta>,
    pub finish_reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct ChatResponseMessage {
    pub role: Option<String>,
    pub content: Option<String>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub tool_calls: Vec<ChatToolCall>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct ChatDelta {
    pub role: Option<String>,
    pub content: Option<String>,
    #[serde(default, deserialize_with = "null_to_default")]
    pub tool_calls: Vec<ChatToolCallDelta>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct ChatToolCallDelta {
    pub index: Option<u32>,
    pub id: Option<String>,
    #[serde(rename = "type")]
    pub tool_type: Option<String>,
    pub function: Option<ChatFunctionCallDelta>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct ChatFunctionCallDelta {
    pub name: Option<String>,
    pub arguments: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Default)]
pub struct ChatUsage {
    pub prompt_tokens: Option<u64>,
    pub completion_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
}

pub type Object = Map<String, Value>;

fn null_to_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}
