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
                    MessageRole::System => "system".to_string(),
                    MessageRole::Developer => "developer".to_string(),
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

#[cfg(test)]
mod tests {
    use super::compile_chat_request;
    use crate::config::{InstructionRole, ModelConfig};
    use crate::domain::{
        CanonicalItem, FunctionOutput, FunctionOutputContentItem, MessageRole, ProvenanceTag,
        TaggedItem,
    };
    use crate::error::ProxyError;
    use crate::ids::CallId;
    use crate::translator::{NormalizedTurnInput, PreservedRequestFields};
    use serde_json::{Value, json};

    fn model_config() -> ModelConfig {
        let mut config = ModelConfig::default_deepseek();
        config.upstream_model = "upstream-model".to_string();
        config
    }

    fn normalized(
        instructions: &str,
        tools: Vec<Value>,
        tool_choice: &str,
        parallel_tool_calls: bool,
    ) -> NormalizedTurnInput {
        NormalizedTurnInput {
            model: "public-model".to_string(),
            instructions: instructions.to_string(),
            previous_response_id: None,
            request_items: Vec::new(),
            tools,
            tool_choice: tool_choice.to_string(),
            parallel_tool_calls,
            stream: false,
            preserved: PreservedRequestFields::default(),
        }
    }

    fn item(item: CanonicalItem) -> TaggedItem {
        TaggedItem::new(item, ProvenanceTag::user_trusted())
    }

    fn serialized_request(
        config: &ModelConfig,
        transcript: &[TaggedItem],
        input: &NormalizedTurnInput,
        stream: bool,
    ) -> Value {
        serde_json::to_value(compile_chat_request(config, transcript, input, stream).unwrap())
            .unwrap()
    }

    #[test]
    fn compiles_complete_request_json_with_developer_instructions_and_tools() {
        let mut config = model_config();
        config.instruction_role = InstructionRole::Developer;
        config.send_model_in_body = true;
        config.include_stream_usage = true;
        let input = normalized(
            "Follow the policy.",
            vec![
                json!({
                    "type": "function",
                    "name": "flat_lookup",
                    "description": "Look up a value.",
                    "parameters": {"type": "object", "properties": {"query": {"type": "string"}}}
                }),
                json!({
                    "type": "function",
                    "function": {
                        "name": "nested_lookup",
                        "description": "Look up another value.",
                        "parameters": {"type": "object", "properties": {}}
                    }
                }),
            ],
            "auto",
            true,
        );
        let transcript = vec![
            item(CanonicalItem::Message {
                role: MessageRole::User,
                text: "Find the value.".to_string(),
            }),
            item(CanonicalItem::Message {
                role: MessageRole::Assistant,
                text: "I will look it up.".to_string(),
            }),
            item(CanonicalItem::FunctionCall {
                call_id: CallId::from_string("call_1"),
                name: "flat_lookup".to_string(),
                arguments: r#"{"query":"rust"}"#.to_string(),
            }),
            item(CanonicalItem::FunctionCallOutput {
                call_id: CallId::from_string("call_1"),
                output: FunctionOutput::ContentItems {
                    items: vec![
                        FunctionOutputContentItem::InputText {
                            text: "first".to_string(),
                        },
                        FunctionOutputContentItem::InputImage {
                            image_url: "https://example.test/image.png".to_string(),
                        },
                        FunctionOutputContentItem::InputText {
                            text: "second".to_string(),
                        },
                    ],
                },
            }),
            item(CanonicalItem::Reasoning {
                raw: json!({"summary": "hidden"}),
            }),
        ];

        assert_eq!(
            serialized_request(&config, &transcript, &input, true),
            json!({
                "model": "upstream-model",
                "messages": [
                    {"role": "developer", "content": "Follow the policy."},
                    {"role": "user", "content": "Find the value."},
                    {"role": "assistant", "content": "I will look it up."},
                    {"role": "assistant", "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": {"name": "flat_lookup", "arguments": r#"{"query":"rust"}"#}
                    }]},
                    {"role": "tool", "content": "first\nsecond", "tool_call_id": "call_1"}
                ],
                "tools": [
                    {"type": "function", "function": {
                        "name": "flat_lookup",
                        "description": "Look up a value.",
                        "parameters": {"type": "object", "properties": {"query": {"type": "string"}}}
                    }},
                    {"type": "function", "function": {
                        "name": "nested_lookup",
                        "description": "Look up another value.",
                        "parameters": {"type": "object", "properties": {}}
                    }}
                ],
                "tool_choice": "auto",
                "parallel_tool_calls": true,
                "stream": true,
                "stream_options": {"include_usage": true}
            })
        );
    }

    #[test]
    fn preserves_canonical_system_and_developer_message_roles() {
        let config = model_config();
        let input = normalized("", Vec::new(), "auto", false);
        let transcript = vec![
            item(CanonicalItem::Message {
                role: MessageRole::System,
                text: "System policy".to_string(),
            }),
            item(CanonicalItem::Message {
                role: MessageRole::Developer,
                text: "Developer policy".to_string(),
            }),
            item(CanonicalItem::Message {
                role: MessageRole::User,
                text: "Question".to_string(),
            }),
        ];

        let request = serialized_request(&config, &transcript, &input, false);
        assert_eq!(
            request["messages"],
            json!([
                {"role": "system", "content": "System policy"},
                {"role": "developer", "content": "Developer policy"},
                {"role": "user", "content": "Question"}
            ])
        );
    }

    #[test]
    fn instruction_role_changes_system_message_role() {
        let input = normalized("Instructions", Vec::new(), "", false);
        let transcript = Vec::new();

        let system = serialized_request(&model_config(), &transcript, &input, false);
        assert_eq!(
            system,
            json!({
                "messages": [{"role": "system", "content": "Instructions"}],
                "parallel_tool_calls": false,
                "stream": false
            })
        );

        let mut developer_config = model_config();
        developer_config.instruction_role = InstructionRole::Developer;
        let developer = serialized_request(&developer_config, &transcript, &input, false);
        assert_eq!(
            developer,
            json!({
                "messages": [{"role": "developer", "content": "Instructions"}],
                "parallel_tool_calls": false,
                "stream": false
            })
        );
    }

    #[test]
    fn model_stream_and_stream_usage_toggles_change_complete_json_shape() {
        let input = normalized("", Vec::new(), "", false);
        let transcript = vec![item(CanonicalItem::Message {
            role: MessageRole::User,
            text: "Hello".to_string(),
        })];

        let mut nonstreaming_with_usage_enabled = model_config();
        nonstreaming_with_usage_enabled.include_stream_usage = true;
        assert_eq!(
            serialized_request(&nonstreaming_with_usage_enabled, &transcript, &input, false),
            json!({
                "messages": [{"role": "user", "content": "Hello"}],
                "parallel_tool_calls": false,
                "stream": false
            })
        );

        let mut nonstreaming_with_usage_disabled = model_config();
        nonstreaming_with_usage_disabled.send_model_in_body = true;
        nonstreaming_with_usage_disabled.include_stream_usage = false;
        assert_eq!(
            serialized_request(
                &nonstreaming_with_usage_disabled,
                &transcript,
                &input,
                false
            ),
            json!({
                "model": "upstream-model",
                "messages": [{"role": "user", "content": "Hello"}],
                "parallel_tool_calls": false,
                "stream": false
            })
        );

        let mut streaming_with_usage_disabled = model_config();
        streaming_with_usage_disabled.include_stream_usage = false;
        assert_eq!(
            serialized_request(&streaming_with_usage_disabled, &transcript, &input, true),
            json!({
                "messages": [{"role": "user", "content": "Hello"}],
                "parallel_tool_calls": false,
                "stream": true
            })
        );

        let mut streaming_with_usage_enabled = model_config();
        streaming_with_usage_enabled.send_model_in_body = true;
        streaming_with_usage_enabled.include_stream_usage = true;
        assert_eq!(
            serialized_request(&streaming_with_usage_enabled, &transcript, &input, true),
            json!({
                "model": "upstream-model",
                "messages": [{"role": "user", "content": "Hello"}],
                "parallel_tool_calls": false,
                "stream": true,
                "stream_options": {"include_usage": true}
            })
        );
    }

    #[test]
    fn empty_tool_choice_is_omitted_and_nonempty_choice_is_a_string() {
        let transcript = Vec::new();

        let empty = normalized("", Vec::new(), "", true);
        assert_eq!(
            serialized_request(&model_config(), &transcript, &empty, false),
            json!({
                "messages": [],
                "parallel_tool_calls": true,
                "stream": false
            })
        );

        let selected = normalized("", Vec::new(), "required", true);
        assert_eq!(
            serialized_request(&model_config(), &transcript, &selected, false),
            json!({
                "messages": [],
                "tool_choice": "required",
                "parallel_tool_calls": true,
                "stream": false
            })
        );
    }

    #[test]
    fn missing_function_name_and_unsupported_tool_return_stable_errors() {
        let transcript = Vec::new();
        let missing_name = normalized(
            "",
            vec![json!({"type": "function", "description": "missing name"})],
            "",
            false,
        );
        assert!(matches!(
            compile_chat_request(&model_config(), &transcript, &missing_name, false),
            Err(ProxyError::UnsupportedTool(message)) if message == "function_without_name"
        ));

        let nested_missing_name = normalized(
            "",
            vec![json!({
                "type": "function",
                "function": {"description": "missing name"}
            })],
            "",
            false,
        );
        assert!(matches!(
            compile_chat_request(&model_config(), &transcript, &nested_missing_name, false),
            Err(ProxyError::UnsupportedTool(message)) if message == "function_without_name"
        ));

        let unsupported = normalized("", vec![json!({"type": "web_search"})], "", false);
        assert!(matches!(
            compile_chat_request(&model_config(), &transcript, &unsupported, false),
            Err(ProxyError::UnsupportedTool(message)) if message == "web_search"
        ));
    }

    #[test]
    fn permissive_policy_allows_empty_and_reasoning_only_transcripts() {
        let input = normalized("", Vec::new(), "", false);
        let empty = serialized_request(&model_config(), &[], &input, false);
        assert_eq!(empty["messages"], json!([]));

        let reasoning = vec![item(CanonicalItem::Reasoning {
            raw: json!({"summary": "hidden"}),
        })];
        let reasoning_only = serialized_request(&model_config(), &reasoning, &input, false);
        assert_eq!(reasoning_only["messages"], json!([]));
    }

    #[test]
    fn permissive_policy_preserves_orphan_outputs_and_duplicate_call_ids() {
        let input = normalized("", Vec::new(), "", false);
        let transcript = vec![
            item(CanonicalItem::FunctionCallOutput {
                call_id: CallId::from_string("call_orphan"),
                output: FunctionOutput::Text {
                    text: "orphan output".to_string(),
                },
            }),
            item(CanonicalItem::FunctionCall {
                call_id: CallId::from_string("call_duplicate"),
                name: "first".to_string(),
                arguments: "{}".to_string(),
            }),
            item(CanonicalItem::FunctionCall {
                call_id: CallId::from_string("call_duplicate"),
                name: "second".to_string(),
                arguments: "{}".to_string(),
            }),
        ];

        assert_eq!(
            serialized_request(&model_config(), &transcript, &input, false)["messages"],
            json!([
                {"role": "tool", "content": "orphan output", "tool_call_id": "call_orphan"},
                {"role": "assistant", "tool_calls": [{
                    "id": "call_duplicate",
                    "type": "function",
                    "function": {"name": "first", "arguments": "{}"}
                }]},
                {"role": "assistant", "tool_calls": [{
                    "id": "call_duplicate",
                    "type": "function",
                    "function": {"name": "second", "arguments": "{}"}
                }]}
            ])
        );
    }
}
