use crate::domain::{
    CanonicalItem, FunctionOutput, FunctionOutputContentItem, MessageRole, ProvenanceTag,
    TaggedItem,
};
use crate::error::{ProxyError, Result};
use crate::ids::{CallId, ResponseId};
use crate::wire::responses::{ResponsesInput, ResponsesRequest};
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct NormalizedTurnInput {
    pub model: String,
    pub instructions: String,
    pub previous_response_id: Option<ResponseId>,
    pub request_items: Vec<TaggedItem>,
    pub tools: Vec<Value>,
    pub tool_choice: String,
    pub parallel_tool_calls: bool,
    pub stream: bool,
    pub preserved: PreservedRequestFields,
}

#[derive(Debug, Clone, Default)]
pub struct PreservedRequestFields {
    pub reasoning: Option<Value>,
    pub store: Option<bool>,
    pub include: Vec<String>,
    pub service_tier: Option<String>,
    pub prompt_cache_key: Option<String>,
    pub text: Option<Value>,
}

pub fn normalize_request(request: ResponsesRequest) -> Result<NormalizedTurnInput> {
    let request_items = match request.input {
        ResponsesInput::String(text) => vec![TaggedItem::new(
            CanonicalItem::Message {
                role: MessageRole::User,
                text,
            },
            ProvenanceTag::user_trusted(),
        )],
        ResponsesInput::Items(items) => normalize_items(items)?,
    };

    validate_tools(&request.tools)?;

    Ok(NormalizedTurnInput {
        model: request.model,
        instructions: request.instructions,
        previous_response_id: request.previous_response_id.map(ResponseId::from_string),
        request_items,
        tools: request.tools,
        tool_choice: request.tool_choice,
        parallel_tool_calls: request.parallel_tool_calls,
        stream: request.stream.unwrap_or(false),
        preserved: PreservedRequestFields {
            reasoning: request.reasoning,
            store: request.store,
            include: request.include,
            service_tier: request.service_tier,
            prompt_cache_key: request.prompt_cache_key,
            text: request.text,
        },
    })
}

fn normalize_items(items: Vec<Value>) -> Result<Vec<TaggedItem>> {
    let mut normalized = Vec::new();
    for item in items {
        if !item.is_object() {
            return Err(ProxyError::UnsupportedInputItem(
                "input_item_not_object".to_string(),
            ));
        }
        let item_type = item
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("message");
        match item_type {
            "message" => normalize_message_item(&item, &mut normalized)?,
            "function_call" => normalized.push(normalize_function_call(&item)?),
            "function_call_output" => normalized.push(normalize_function_output(&item)?),
            "reasoning" => normalized.push(TaggedItem::new(
                CanonicalItem::Reasoning { raw: item },
                ProvenanceTag::model_semi(),
            )),
            other => return Err(ProxyError::UnsupportedInputItem(other.to_string())),
        }
    }
    Ok(normalized)
}

fn normalize_message_item(item: &Value, normalized: &mut Vec<TaggedItem>) -> Result<()> {
    let role = item.get("role").and_then(Value::as_str).unwrap_or("user");
    let content = item
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ProxyError::UnsupportedInputItem("message_without_content_array".to_string())
        })?;

    match role {
        "user" => {
            let text = collect_content_text(content, "input_text")?;
            normalized.push(TaggedItem::new(
                CanonicalItem::Message {
                    role: MessageRole::User,
                    text,
                },
                ProvenanceTag::user_trusted(),
            ));
        }
        "developer" => {
            let text = collect_content_text(content, "input_text")?;
            normalized.push(TaggedItem::new(
                CanonicalItem::Message {
                    role: MessageRole::User,
                    text,
                },
                ProvenanceTag::user_trusted(),
            ));
        }
        "assistant" => {
            let text = collect_content_text(content, "output_text")?;
            normalized.push(TaggedItem::new(
                CanonicalItem::Message {
                    role: MessageRole::Assistant,
                    text,
                },
                ProvenanceTag::model_semi(),
            ));
        }
        other => {
            return Err(ProxyError::UnsupportedInputItem(format!(
                "message_role_{other}"
            )));
        }
    }
    Ok(())
}

fn collect_content_text(content: &[Value], text_type: &str) -> Result<String> {
    let mut parts = Vec::new();
    for content_item in content {
        let content_type = content_item
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ProxyError::UnsupportedInputItem("message_content_item_without_type".to_string())
            })?;
        if content_type == text_type {
            let text = content_item
                .get("text")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    ProxyError::UnsupportedInputItem(
                        "message_content_item_without_text".to_string(),
                    )
                })?;
            if !text.is_empty() {
                parts.push(text.to_string());
            }
        } else if content_type == "input_image" {
            continue;
        } else {
            return Err(ProxyError::UnsupportedInputItem(content_type.to_string()));
        }
    }
    Ok(parts.join("\n"))
}

fn normalize_function_call(item: &Value) -> Result<TaggedItem> {
    let name = required_string(item, "name")?;
    let call_id = required_string(item, "call_id")?;
    let arguments = required_string(item, "arguments")?;
    Ok(TaggedItem::new(
        CanonicalItem::FunctionCall {
            call_id: CallId::from_string(call_id),
            name,
            arguments,
        },
        ProvenanceTag::model_semi(),
    ))
}

fn normalize_function_output(item: &Value) -> Result<TaggedItem> {
    let call_id = required_string(item, "call_id")?;
    let output_value = item.get("output").ok_or_else(|| {
        ProxyError::UnsupportedInputItem("function_call_output_without_output".to_string())
    })?;
    let output = if let Some(text) = output_value.as_str() {
        FunctionOutput::Text {
            text: text.to_string(),
        }
    } else if let Some(items) = output_value.as_array() {
        let mut content_items = Vec::new();
        for content_item in items {
            match content_item
                .get("type")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    ProxyError::UnsupportedInputItem(
                        "function_call_output_content_item_without_type".to_string(),
                    )
                })? {
                "input_text" => {
                    let text = content_item
                        .get("text")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            ProxyError::UnsupportedInputItem(
                                "function_call_output_content_item_without_text".to_string(),
                            )
                        })?;
                    content_items.push(FunctionOutputContentItem::InputText {
                        text: text.to_string(),
                    });
                }
                "input_image" => {
                    let image_url = content_item
                        .get("image_url")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            ProxyError::UnsupportedInputItem(
                                "function_call_output_content_item_without_image_url".to_string(),
                            )
                        })?;
                    content_items.push(FunctionOutputContentItem::InputImage {
                        image_url: image_url.to_string(),
                    });
                }
                other => return Err(ProxyError::UnsupportedInputItem(other.to_string())),
            }
        }
        FunctionOutput::ContentItems {
            items: content_items,
        }
    } else {
        return Err(ProxyError::UnsupportedInputItem(
            "function_call_output_output".to_string(),
        ));
    };

    Ok(TaggedItem::new(
        CanonicalItem::FunctionCallOutput {
            call_id: CallId::from_string(call_id),
            output,
        },
        ProvenanceTag::tool_output_semi(),
    ))
}

fn required_string(item: &Value, field: &str) -> Result<String> {
    item.get(field)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| ProxyError::UnsupportedInputItem(format!("missing_{field}")))
}

fn validate_tools(tools: &[Value]) -> Result<()> {
    for tool in tools {
        let tool_type = tool.get("type").and_then(Value::as_str).unwrap_or_default();
        if tool_type != "function" {
            return Err(ProxyError::UnsupportedTool(tool_type.to_string()));
        }
        let function = tool.get("function").unwrap_or(tool);
        let has_name = function
            .get("name")
            .and_then(Value::as_str)
            .is_some_and(|name| !name.is_empty());
        if !has_name {
            return Err(ProxyError::UnsupportedTool(
                "function_without_name".to_string(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{NormalizedTurnInput, normalize_request};
    use crate::domain::{
        CanonicalItem, FunctionOutput, FunctionOutputContentItem, MessageRole, ProvenanceSource,
        TrustLevel,
    };
    use crate::error::ProxyError;
    use crate::wire::responses::{ResponsesInput, ResponsesRequest};
    use serde_json::{Value, json};

    fn request(input: ResponsesInput) -> ResponsesRequest {
        ResponsesRequest {
            model: "public-model".to_string(),
            instructions: String::new(),
            input,
            tools: Vec::new(),
            tool_choice: "auto".to_string(),
            parallel_tool_calls: false,
            reasoning: None,
            store: None,
            stream: None,
            include: Vec::new(),
            service_tier: None,
            prompt_cache_key: None,
            text: None,
            previous_response_id: None,
        }
    }

    fn normalize(input: ResponsesInput) -> NormalizedTurnInput {
        normalize_request(request(input)).expect("request should normalize")
    }

    fn assert_provenance(
        item: &crate::domain::TaggedItem,
        source: ProvenanceSource,
        trust: TrustLevel,
    ) {
        assert!(!item.id.as_str().is_empty());
        assert_eq!(item.provenance.source, source);
        assert_eq!(item.provenance.trust, trust);
        assert!(item.artifact_hash.is_none());
    }

    fn assert_unsupported_input(input: ResponsesInput, expected: &str) {
        assert!(matches!(
            normalize_request(request(input)),
            Err(ProxyError::UnsupportedInputItem(message)) if message == expected
        ));
    }

    fn assert_unsupported_tool(tool: Value, expected: &str) {
        let mut request = request(ResponsesInput::String("lookup".to_string()));
        request.tools = vec![tool];
        assert!(matches!(
            normalize_request(request),
            Err(ProxyError::UnsupportedTool(message)) if message == expected
        ));
    }

    #[test]
    fn string_input_becomes_trusted_user_message() {
        let normalized = normalize(ResponsesInput::String("hello\0world".to_string()));

        assert_eq!(normalized.stream, false);
        assert_eq!(normalized.request_items.len(), 1);
        let item = &normalized.request_items[0];
        assert_provenance(item, ProvenanceSource::User, TrustLevel::Trusted);
        assert!(matches!(
            &item.item,
            CanonicalItem::Message {
                role: MessageRole::User,
                text
            } if text == "hello\0world"
        ));
    }

    #[test]
    fn empty_string_input_becomes_one_trusted_empty_user_message() {
        let normalized = normalize(ResponsesInput::String(String::new()));

        assert_eq!(normalized.request_items.len(), 1);
        let item = &normalized.request_items[0];
        assert_provenance(item, ProvenanceSource::User, TrustLevel::Trusted);
        assert!(matches!(
            &item.item,
            CanonicalItem::Message {
                role: MessageRole::User,
                text
            } if text.is_empty()
        ));
    }

    #[test]
    fn message_items_normalize_roles_join_text_and_omit_images() {
        let normalized = normalize(ResponsesInput::Items(vec![
            json!({
                "content": [
                    {"type": "input_text", "text": "default"},
                    {"type": "input_image", "image_url": "https://example.test/default.png"},
                    {"type": "input_text", "text": "role"}
                ]
            }),
            json!({
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "user"}]
            }),
            json!({
                "type": "message",
                "role": "developer",
                "content": [{"type": "input_text", "text": "developer"}]
            }),
            json!({
                "type": "message",
                "role": "assistant",
                "content": [
                    {"type": "output_text", "text": "first"},
                    {"type": "input_image", "image_url": "https://example.test/assistant.png"},
                    {"type": "output_text", "text": "second"},
                    {"type": "output_text", "text": ""}
                ]
            }),
        ]));

        assert_eq!(normalized.request_items.len(), 4);
        let expected = [
            (MessageRole::User, "default\nrole", ProvenanceSource::User),
            (MessageRole::User, "user", ProvenanceSource::User),
            (MessageRole::User, "developer", ProvenanceSource::User),
            (
                MessageRole::Assistant,
                "first\nsecond",
                ProvenanceSource::Model,
            ),
        ];
        for (item, (role, text, source)) in normalized.request_items.iter().zip(expected) {
            let trust = if source == ProvenanceSource::User {
                TrustLevel::Trusted
            } else {
                TrustLevel::Semi
            };
            assert_provenance(item, source, trust);
            assert!(matches!(
                &item.item,
                CanonicalItem::Message { role: actual_role, text: actual_text }
                    if *actual_role == role && actual_text == text
            ));
        }
    }

    #[test]
    fn empty_input_and_image_only_messages_are_safe() {
        let normalized = normalize(ResponsesInput::Items(vec![
            json!({"type": "message", "content": []}),
            json!({
                "type": "message",
                "content": [{"type": "input_image", "image_url": "https://example.test/image.png"}]
            }),
        ]));

        assert_eq!(normalized.request_items.len(), 2);
        for item in &normalized.request_items {
            assert_provenance(item, ProvenanceSource::User, TrustLevel::Trusted);
            assert!(matches!(
                &item.item,
                CanonicalItem::Message { role: MessageRole::User, text } if text.is_empty()
            ));
        }
        assert!(
            normalize(ResponsesInput::Items(Vec::new()))
                .request_items
                .is_empty()
        );
    }

    #[test]
    fn unsupported_message_shapes_return_typed_errors() {
        assert_unsupported_input(
            ResponsesInput::Items(vec![json!({
                "type": "message",
                "role": "system",
                "content": []
            })]),
            "message_role_system",
        );
        assert_unsupported_input(
            ResponsesInput::Items(vec![json!({
                "type": "message",
                "content": [{"type": "input_audio", "audio": "..."}]
            })]),
            "input_audio",
        );
        assert_unsupported_input(
            ResponsesInput::Items(vec![json!({"type": "message", "content": "not-an-array"})]),
            "message_without_content_array",
        );
        for content in [Value::Null, json!({}), json!("not-an-array")] {
            assert_unsupported_input(
                ResponsesInput::Items(vec![json!({
                    "type": "message",
                    "content": content
                })]),
                "message_without_content_array",
            );
        }
        assert_unsupported_input(
            ResponsesInput::Items(vec![json!({"type": "message"})]),
            "message_without_content_array",
        );
        assert_unsupported_input(
            ResponsesInput::Items(vec![json!({"type": "unsupported_item"})]),
            "unsupported_item",
        );
    }

    #[test]
    fn role_specific_text_types_and_malformed_text_are_rejected() {
        for (role, content_type) in [
            ("user", "output_text"),
            ("developer", "output_text"),
            ("assistant", "input_text"),
        ] {
            assert_unsupported_input(
                ResponsesInput::Items(vec![json!({
                    "type": "message",
                    "role": role,
                    "content": [{"type": content_type, "text": "wrong role"}]
                })]),
                content_type,
            );
        }

        for content in [
            json!({"type": "input_text"}),
            json!({"type": "input_text", "text": null}),
            json!({"type": "input_text", "text": 42}),
        ] {
            assert_unsupported_input(
                ResponsesInput::Items(vec![json!({
                    "type": "message",
                    "content": [content]
                })]),
                "message_content_item_without_text",
            );
        }

        for content in [
            json!({"type": "output_text"}),
            json!({"type": "output_text", "text": null}),
            json!({"type": "output_text", "text": 42}),
        ] {
            assert_unsupported_input(
                ResponsesInput::Items(vec![json!({
                    "type": "message",
                    "role": "assistant",
                    "content": [content]
                })]),
                "message_content_item_without_text",
            );
        }

        assert_unsupported_input(
            ResponsesInput::Items(vec![json!({
                "type": "message",
                "content": [Value::Null]
            })]),
            "message_content_item_without_type",
        );
    }

    #[test]
    fn function_call_requires_name_call_id_and_arguments() {
        for missing in ["name", "call_id", "arguments"] {
            let mut item = json!({
                "type": "function_call",
                "name": "lookup",
                "call_id": "call-1",
                "arguments": "{\"query\":\"rust\"}"
            });
            item.as_object_mut().unwrap().remove(missing);
            assert_unsupported_input(
                ResponsesInput::Items(vec![item]),
                &format!("missing_{missing}"),
            );
        }

        for invalid in ["name", "call_id", "arguments"] {
            let mut item = json!({
                "type": "function_call",
                "name": "lookup",
                "call_id": "call-1",
                "arguments": "{\"query\":\"rust\"}"
            });
            item[invalid] = Value::Null;
            assert_unsupported_input(
                ResponsesInput::Items(vec![item]),
                &format!("missing_{invalid}"),
            );
        }

        let normalized = normalize(ResponsesInput::Items(vec![json!({
            "type": "function_call",
            "name": "lookup",
            "call_id": "call-1",
            "arguments": "{\"query\":\"rust\"}"
        })]));
        let item = &normalized.request_items[0];
        assert_provenance(item, ProvenanceSource::Model, TrustLevel::Semi);
        assert!(matches!(
            &item.item,
            CanonicalItem::FunctionCall { call_id, name, arguments }
                if call_id.as_str() == "call-1"
                    && name == "lookup"
                    && arguments == "{\"query\":\"rust\"}"
        ));
    }

    #[test]
    fn function_call_outputs_normalize_text_content_items_and_images() {
        let normalized = normalize(ResponsesInput::Items(vec![
            json!({
                "type": "function_call_output",
                "call_id": "call-text",
                "output": "plain output\0"
            }),
            json!({
                "type": "function_call_output",
                "call_id": "call-content",
                "output": [
                    {"type": "input_text", "text": "first"},
                    {"type": "input_image", "image_url": "https://example.test/output.png"},
                    {"type": "input_text", "text": "second"}
                ]
            }),
        ]));

        assert_eq!(normalized.request_items.len(), 2);
        for item in &normalized.request_items {
            assert_provenance(item, ProvenanceSource::ToolOutput, TrustLevel::Semi);
        }
        assert!(matches!(
            &normalized.request_items[0].item,
            CanonicalItem::FunctionCallOutput { call_id, output: FunctionOutput::Text { text } }
                if call_id.as_str() == "call-text" && text == "plain output\0"
        ));
        assert!(matches!(
            &normalized.request_items[1].item,
            CanonicalItem::FunctionCallOutput {
                call_id,
                output: FunctionOutput::ContentItems { items }
            }
                if call_id.as_str() == "call-content"
                    && items == &vec![
                        FunctionOutputContentItem::InputText { text: "first".to_string() },
                        FunctionOutputContentItem::InputImage {
                            image_url: "https://example.test/output.png".to_string()
                        },
                        FunctionOutputContentItem::InputText { text: "second".to_string() },
                    ]
        ));
    }

    #[test]
    fn malformed_function_call_outputs_return_typed_errors_without_panicking() {
        assert_unsupported_input(
            ResponsesInput::Items(vec![json!({
                "type": "function_call_output",
                "output": "result"
            })]),
            "missing_call_id",
        );
        assert_unsupported_input(
            ResponsesInput::Items(vec![json!({
                "type": "function_call_output",
                "call_id": "call-1"
            })]),
            "function_call_output_without_output",
        );
        assert_unsupported_input(
            ResponsesInput::Items(vec![json!({
                "type": "function_call_output",
                "call_id": "call-1",
                "output": [{"type": "audio", "data": "..."}]
            })]),
            "audio",
        );
        assert_unsupported_input(
            ResponsesInput::Items(vec![json!({
                "type": "function_call_output",
                "call_id": "call-1",
                "output": {"unexpected": true}
            })]),
            "function_call_output_output",
        );
        assert_unsupported_input(
            ResponsesInput::Items(vec![json!({
                "type": "function_call_output",
                "call_id": "call-1",
                "output": [null]
            })]),
            "function_call_output_content_item_without_type",
        );
        for content in [
            json!({"type": "input_text"}),
            json!({"type": "input_text", "text": null}),
            json!({"type": "input_text", "text": 42}),
        ] {
            assert_unsupported_input(
                ResponsesInput::Items(vec![json!({
                    "type": "function_call_output",
                    "call_id": "call-1",
                    "output": [content]
                })]),
                "function_call_output_content_item_without_text",
            );
        }
        for content in [
            json!({"type": "input_image"}),
            json!({"type": "input_image", "image_url": null}),
            json!({"type": "input_image", "image_url": 42}),
        ] {
            assert_unsupported_input(
                ResponsesInput::Items(vec![json!({
                    "type": "function_call_output",
                    "call_id": "call-1",
                    "output": [content]
                })]),
                "function_call_output_content_item_without_image_url",
            );
        }
    }

    #[test]
    fn reasoning_items_are_preserved_raw_with_model_provenance() {
        let raw = json!({
            "type": "reasoning",
            "id": "rs_1",
            "summary": [{"type": "summary_text", "text": "hidden"}],
            "encrypted_content": null
        });
        let normalized = normalize(ResponsesInput::Items(vec![raw.clone()]));
        let item = &normalized.request_items[0];

        assert_provenance(item, ProvenanceSource::Model, TrustLevel::Semi);
        assert!(matches!(
            &item.item,
            CanonicalItem::Reasoning { raw: actual } if actual == &raw
        ));
    }

    #[test]
    fn flat_and_nested_function_tools_are_accepted() {
        let mut request = request(ResponsesInput::String("lookup".to_string()));
        request.tools = vec![
            json!({
                "type": "function",
                "name": "flat_lookup",
                "parameters": {"type": "object"}
            }),
            json!({
                "type": "function",
                "function": {
                    "name": "nested_lookup",
                    "parameters": {"type": "object"}
                }
            }),
        ];

        let normalized = normalize_request(request).expect("function tools should normalize");
        assert_eq!(normalized.tools.len(), 2);
        assert_eq!(normalized.tools[0]["name"], "flat_lookup");
        assert_eq!(normalized.tools[1]["function"]["name"], "nested_lookup");
    }

    #[test]
    fn non_function_and_incomplete_function_tools_are_rejected() {
        for (tool, expected) in [
            (
                json!({"type": "computer_use_preview"}),
                "computer_use_preview",
            ),
            (json!({"type": "function"}), "function_without_name"),
            (
                json!({"type": "function", "name": null}),
                "function_without_name",
            ),
            (
                json!({"type": "function", "name": 42}),
                "function_without_name",
            ),
            (
                json!({"type": "function", "name": ""}),
                "function_without_name",
            ),
            (
                json!({"type": "function", "function": null}),
                "function_without_name",
            ),
            (
                json!({"type": "function", "function": {}}),
                "function_without_name",
            ),
            (
                json!({"type": "function", "function": {"name": null}}),
                "function_without_name",
            ),
            (
                json!({"type": "function", "function": {"name": 42}}),
                "function_without_name",
            ),
            (
                json!({"type": "function", "function": {"name": ""}}),
                "function_without_name",
            ),
        ] {
            assert_unsupported_tool(tool, expected);
        }
    }

    #[test]
    fn preserved_fields_stream_and_previous_response_id_survive_normalization() {
        let mut request = request(ResponsesInput::String("hello".to_string()));
        request.model = "model-a".to_string();
        request.instructions = "Follow instructions".to_string();
        request.tool_choice = "required".to_string();
        request.parallel_tool_calls = true;
        request.reasoning = Some(json!({"effort": "high"}));
        request.store = Some(true);
        request.stream = Some(true);
        request.include = vec!["reasoning.encrypted_content".to_string()];
        request.service_tier = Some("priority".to_string());
        request.prompt_cache_key = Some("cache-key".to_string());
        request.text = Some(json!({"format": {"type": "text"}}));
        request.previous_response_id = Some("resp_original-value".to_string());

        let normalized = normalize_request(request).expect("request should normalize");
        assert_eq!(normalized.model, "model-a");
        assert_eq!(normalized.instructions, "Follow instructions");
        assert_eq!(normalized.tool_choice, "required");
        assert!(normalized.parallel_tool_calls);
        assert!(normalized.stream);
        assert_eq!(
            normalized.previous_response_id.unwrap().as_str(),
            "resp_original-value"
        );
        assert_eq!(
            normalized.preserved.reasoning,
            Some(json!({"effort": "high"}))
        );
        assert_eq!(normalized.preserved.store, Some(true));
        assert_eq!(
            normalized.preserved.include,
            vec!["reasoning.encrypted_content".to_string()]
        );
        assert_eq!(
            normalized.preserved.service_tier.as_deref(),
            Some("priority")
        );
        assert_eq!(
            normalized.preserved.prompt_cache_key.as_deref(),
            Some("cache-key")
        );
        assert_eq!(
            normalized.preserved.text,
            Some(json!({"format": {"type": "text"}}))
        );
    }

    #[test]
    fn malformed_item_values_return_errors_instead_of_panicking() {
        let malformed_values = [Value::Null, Value::Bool(true), json!(42), json!("item")];
        for value in malformed_values {
            assert_unsupported_input(ResponsesInput::Items(vec![value]), "input_item_not_object");
        }
    }
}
