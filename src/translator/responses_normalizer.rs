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
            .unwrap_or_default();
        if content_type == text_type {
            if let Some(text) = content_item.get("text").and_then(Value::as_str) {
                if !text.is_empty() {
                    parts.push(text.to_string());
                }
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
                .unwrap_or_default()
            {
                "input_text" => content_items.push(FunctionOutputContentItem::InputText {
                    text: content_item
                        .get("text")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                }),
                "input_image" => content_items.push(FunctionOutputContentItem::InputImage {
                    image_url: content_item
                        .get("image_url")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                }),
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
        if tool.get("name").is_none() && tool.get("function").is_none() {
            return Err(ProxyError::UnsupportedTool(
                "function_without_name".to_string(),
            ));
        }
    }
    Ok(())
}
