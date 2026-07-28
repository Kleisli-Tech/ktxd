use crate::config::{InstructionRole, ModelConfig};
use crate::domain::{CanonicalItem, MessageRole, TaggedItem};
use crate::error::{ProxyError, Result};
use crate::translator::NormalizedTurnInput;
use crate::wire::chat::{
    ChatCompletionRequest, ChatFunctionCall, ChatFunctionTool, ChatMessage, ChatTool, ChatToolCall,
    StreamOptions,
};
use serde_json::{Value, json};

pub fn compile_chat_request(
    model_config: &ModelConfig,
    transcript: &[TaggedItem],
    normalized: &NormalizedTurnInput,
    stream: bool,
) -> Result<ChatCompletionRequest> {
    let mut messages = Vec::new();
    if !normalized.instructions.is_empty() {
        messages.push(ChatMessage {
            role: match model_config.instruction_role {
                InstructionRole::System => "system".to_string(),
                InstructionRole::Developer => "developer".to_string(),
            },
            content: Some(normalized.instructions.clone()),
            tool_call_id: None,
            tool_calls: None,
        });
    }

    for tagged_item in transcript {
        match &tagged_item.item {
            CanonicalItem::Message { role, text } => messages.push(ChatMessage {
                role: match role {
                    MessageRole::User => "user".to_string(),
                    MessageRole::Assistant => "assistant".to_string(),
                },
                content: Some(text.clone()),
                tool_call_id: None,
                tool_calls: None,
            }),
            CanonicalItem::FunctionCall {
                call_id,
                name,
                arguments,
            } => messages.push(ChatMessage {
                role: "assistant".to_string(),
                content: None,
                tool_call_id: None,
                tool_calls: Some(vec![ChatToolCall {
                    index: None,
                    id: call_id.to_string(),
                    tool_type: "function".to_string(),
                    function: ChatFunctionCall {
                        name: name.clone(),
                        arguments: arguments.clone(),
                    },
                }]),
            }),
            CanonicalItem::FunctionCallOutput { call_id, output } => messages.push(ChatMessage {
                role: "tool".to_string(),
                content: Some(output.lower_to_chat_content()),
                tool_call_id: Some(call_id.to_string()),
                tool_calls: None,
            }),
            CanonicalItem::Reasoning { .. } => {}
        }
    }

    Ok(ChatCompletionRequest {
        model: model_config
            .send_model_in_body
            .then(|| model_config.upstream_model.clone()),
        messages,
        tools: compile_tools(&normalized.tools)?,
        tool_choice: compile_tool_choice(&normalized.tool_choice),
        parallel_tool_calls: Some(normalized.parallel_tool_calls),
        stream,
        stream_options: (stream && model_config.include_stream_usage).then_some(StreamOptions {
            include_usage: true,
        }),
    })
}

fn compile_tool_choice(tool_choice: &str) -> Option<Value> {
    if tool_choice.is_empty() {
        None
    } else {
        Some(Value::String(tool_choice.to_string()))
    }
}

fn compile_tools(tools: &[Value]) -> Result<Vec<ChatTool>> {
    let mut compiled = Vec::new();
    for tool in tools {
        let tool_type = tool.get("type").and_then(Value::as_str).unwrap_or_default();
        if tool_type != "function" {
            return Err(ProxyError::UnsupportedTool(tool_type.to_string()));
        }
        let function_value = tool.get("function").unwrap_or(tool);
        let name = function_value
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| ProxyError::UnsupportedTool("function_without_name".to_string()))?;
        let description = function_value
            .get("description")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        let parameters = function_value
            .get("parameters")
            .cloned()
            .unwrap_or_else(|| json!({"type":"object","properties":{}}));
        compiled.push(ChatTool {
            tool_type: "function".to_string(),
            function: ChatFunctionTool {
                name: name.to_string(),
                description,
                parameters,
            },
        });
    }
    Ok(compiled)
}
