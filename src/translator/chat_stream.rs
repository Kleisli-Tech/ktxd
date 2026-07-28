use crate::domain::{CanonicalItem, MessageRole, ProvenanceTag, TaggedItem, UsageTotals};
use crate::error::{ProxyError, Result};
use crate::ids::{CallId, ResponseId};
use crate::responses::{
    ResponseEvent, incomplete_event, output_item_added_event, output_item_done_event,
    output_text_delta_event,
};
use crate::wire::chat::{ChatChoice, ChatCompletionResponse, ChatToolCall, ChatUsage};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamTerminal {
    Completed,
    Incomplete(String),
    Failed(String),
}

#[derive(Debug, Clone)]
pub struct StreamTranslation {
    pub events: Vec<ResponseEvent>,
    pub output_items: Vec<TaggedItem>,
    pub usage: UsageTotals,
    pub terminal: StreamTerminal,
}

#[derive(Debug, Default)]
struct ToolCallBuilder {
    id: Option<String>,
    name: Option<String>,
    arguments: String,
}

pub fn translate_stream_chunks(
    response_id: &ResponseId,
    model: &str,
    chunks: Vec<ChatCompletionResponse>,
) -> Result<StreamTranslation> {
    let mut events = Vec::new();
    let mut text_item: Option<TaggedItem> = None;
    let mut text_added = false;
    let mut tool_calls: BTreeMap<u32, ToolCallBuilder> = BTreeMap::new();
    let mut usage = UsageTotals::default();
    let mut finish_reason: Option<String> = None;

    for chunk in chunks {
        if let Some(chunk_usage) = chunk.usage {
            usage = usage_from_chat(&chunk_usage);
        }
        for choice in chunk.choices {
            if let Some(delta) = choice.delta {
                if let Some(content) = delta.content {
                    if content.is_empty() {
                        continue;
                    }
                    if text_item.is_none() {
                        text_item = Some(TaggedItem::new(
                            CanonicalItem::Message {
                                role: MessageRole::Assistant,
                                text: String::new(),
                            },
                            ProvenanceTag::model_semi(),
                        ));
                    }
                    let output_index = 0;
                    if !text_added {
                        events.push(output_item_added_event(
                            response_id,
                            output_index,
                            text_item.as_ref().expect("text item exists"),
                        ));
                        text_added = true;
                    }
                    if let Some(TaggedItem {
                        item: CanonicalItem::Message { text, .. },
                        ..
                    }) = text_item.as_mut()
                    {
                        text.push_str(&content);
                    }
                    events.push(output_text_delta_event(
                        response_id,
                        text_item.as_ref().expect("text item exists").id.as_str(),
                        output_index,
                        &content,
                    ));
                }
                for tool_call in delta.tool_calls {
                    let index = tool_call.index.unwrap_or(0);
                    let builder = tool_calls.entry(index).or_default();
                    if let Some(id) = tool_call.id {
                        latch_field(&mut builder.id, id, "tool_call.id")?;
                    }
                    if let Some(function) = tool_call.function {
                        if let Some(name) = function.name {
                            latch_field(&mut builder.name, name, "tool_call.function.name")?;
                        }
                        if let Some(arguments) = function.arguments {
                            builder.arguments.push_str(&arguments);
                        }
                    }
                }
            }
            if let Some(reason) = choice.finish_reason {
                finish_reason = Some(reason);
            }
        }
    }

    let mut output_items = Vec::new();
    if let Some(item) = text_item {
        let output_index = output_items.len();
        events.push(output_item_done_event(response_id, output_index, &item));
        output_items.push(item);
    }

    let terminal = match finish_reason.as_deref() {
        Some("stop") => StreamTerminal::Completed,
        Some("tool_calls") => {
            let generated_calls = finish_tool_calls(tool_calls)?;
            for item in generated_calls {
                let output_index = output_items.len();
                events.push(output_item_added_event(response_id, output_index, &item));
                events.push(output_item_done_event(response_id, output_index, &item));
                output_items.push(item);
            }
            StreamTerminal::Completed
        }
        Some("length") => StreamTerminal::Incomplete("max_output_tokens".to_string()),
        Some("content_filter") => StreamTerminal::Incomplete("content_filter".to_string()),
        Some(other) => StreamTerminal::Failed(format!("unsupported_finish_reason_{other}")),
        None => StreamTerminal::Failed("done_without_finish_reason".to_string()),
    };

    if let StreamTerminal::Incomplete(reason) = &terminal {
        events.push(incomplete_event(
            response_id,
            model,
            &output_items,
            &usage,
            reason,
        ));
    }

    Ok(StreamTranslation {
        events,
        output_items,
        usage,
        terminal,
    })
}

pub fn translate_non_streaming_response(
    response: ChatCompletionResponse,
) -> Result<(Vec<TaggedItem>, UsageTotals, StreamTerminal)> {
    let usage = response
        .usage
        .as_ref()
        .map(usage_from_chat)
        .unwrap_or_default();
    let choice = response
        .choices
        .into_iter()
        .next()
        .ok_or_else(|| ProxyError::Upstream("missing choice".to_string()))?;
    let finish_reason = choice.finish_reason.unwrap_or_else(|| "stop".to_string());
    let mut output_items = Vec::new();
    if let Some(message) = choice.message {
        if let Some(content) = message.content {
            if !content.is_empty() {
                output_items.push(TaggedItem::new(
                    CanonicalItem::Message {
                        role: MessageRole::Assistant,
                        text: content,
                    },
                    ProvenanceTag::model_semi(),
                ));
            }
        }
        let mut tool_calls = message.tool_calls;
        tool_calls.sort_by(|left, right| {
            left.index
                .unwrap_or(u32::MAX)
                .cmp(&right.index.unwrap_or(u32::MAX))
                .then_with(|| left.id.cmp(&right.id))
        });
        for tool_call in tool_calls {
            output_items.push(tool_call_to_item(tool_call));
        }
    }
    let terminal = match finish_reason.as_str() {
        "stop" | "tool_calls" => StreamTerminal::Completed,
        "length" => StreamTerminal::Incomplete("max_output_tokens".to_string()),
        "content_filter" => StreamTerminal::Incomplete("content_filter".to_string()),
        other => StreamTerminal::Failed(format!("unsupported_finish_reason_{other}")),
    };
    Ok((output_items, usage, terminal))
}

fn finish_tool_calls(tool_calls: BTreeMap<u32, ToolCallBuilder>) -> Result<Vec<TaggedItem>> {
    let mut output_items = Vec::new();
    for (_index, builder) in tool_calls {
        let id = builder
            .id
            .ok_or_else(|| ProxyError::MalformedStream("missing tool call id".to_string()))?;
        let name = builder
            .name
            .ok_or_else(|| ProxyError::MalformedStream("missing tool call name".to_string()))?;
        output_items.push(TaggedItem::new(
            CanonicalItem::FunctionCall {
                call_id: CallId::from_string(id),
                name,
                arguments: builder.arguments,
            },
            ProvenanceTag::model_semi(),
        ));
    }
    Ok(output_items)
}

fn tool_call_to_item(tool_call: ChatToolCall) -> TaggedItem {
    TaggedItem::new(
        CanonicalItem::FunctionCall {
            call_id: CallId::from_string(tool_call.id),
            name: tool_call.function.name,
            arguments: tool_call.function.arguments,
        },
        ProvenanceTag::model_semi(),
    )
}

fn latch_field(target: &mut Option<String>, candidate: String, field: &str) -> Result<()> {
    match target {
        Some(existing) if existing != &candidate => {
            Err(ProxyError::MalformedStream(format!("conflicting {field}")))
        }
        Some(_) => Ok(()),
        None => {
            *target = Some(candidate);
            Ok(())
        }
    }
}

fn usage_from_chat(usage: &ChatUsage) -> UsageTotals {
    UsageTotals {
        input_tokens: usage.prompt_tokens.unwrap_or(0),
        output_tokens: usage.completion_tokens.unwrap_or(0),
        total_tokens: usage.total_tokens.unwrap_or(0),
    }
}

#[allow(dead_code)]
fn _choice_index(choice: &ChatChoice) -> u32 {
    choice.index.unwrap_or(0)
}
