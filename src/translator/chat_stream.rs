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
                    if !content.is_empty() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::chat::{ChatDelta, ChatFunctionCallDelta, ChatToolCallDelta};

    fn response_id() -> ResponseId {
        ResponseId::from_string("resp_test")
    }

    fn chunk(choices: Vec<ChatChoice>, usage: Option<ChatUsage>) -> ChatCompletionResponse {
        ChatCompletionResponse {
            id: None,
            choices,
            usage,
        }
    }

    fn choice(
        index: Option<u32>,
        delta: Option<ChatDelta>,
        finish_reason: Option<&str>,
    ) -> ChatChoice {
        ChatChoice {
            index,
            message: None,
            delta,
            finish_reason: finish_reason.map(str::to_string),
        }
    }

    fn delta(content: Option<&str>, tool_calls: Vec<ChatToolCallDelta>) -> ChatDelta {
        ChatDelta {
            role: None,
            content: content.map(str::to_string),
            tool_calls,
        }
    }

    fn tool_delta(
        index: Option<u32>,
        id: Option<&str>,
        name: Option<&str>,
        arguments: Option<&str>,
    ) -> ChatToolCallDelta {
        ChatToolCallDelta {
            index,
            id: id.map(str::to_string),
            tool_type: Some("function".to_string()),
            function: (name.is_some() || arguments.is_some()).then_some(ChatFunctionCallDelta {
                name: name.map(str::to_string),
                arguments: arguments.map(str::to_string),
            }),
        }
    }

    fn usage(prompt_tokens: u64, completion_tokens: u64, total_tokens: u64) -> ChatUsage {
        ChatUsage {
            prompt_tokens: Some(prompt_tokens),
            completion_tokens: Some(completion_tokens),
            total_tokens: Some(total_tokens),
        }
    }

    #[test]
    fn fragmented_text_emits_one_item_and_one_delta_per_non_empty_fragment() {
        let translated = translate_stream_chunks(
            &response_id(),
            "model-test",
            vec![
                chunk(
                    vec![choice(None, Some(delta(Some("Hel"), Vec::new())), None)],
                    None,
                ),
                chunk(
                    vec![choice(Some(0), Some(delta(Some(""), Vec::new())), None)],
                    None,
                ),
                chunk(
                    vec![choice(None, Some(delta(Some("lo"), Vec::new())), None)],
                    Some(usage(3, 2, 5)),
                ),
                chunk(
                    vec![choice(None, Some(delta(None, Vec::new())), Some("stop"))],
                    None,
                ),
            ],
        )
        .expect("stream should translate");

        assert_eq!(translated.terminal, StreamTerminal::Completed);
        assert_eq!(
            translated.usage,
            UsageTotals {
                input_tokens: 3,
                output_tokens: 2,
                total_tokens: 5,
            }
        );
        assert_eq!(translated.output_items.len(), 1);
        assert!(matches!(
            &translated.output_items[0].item,
            CanonicalItem::Message { text, .. } if text == "Hello"
        ));
        assert_eq!(
            translated
                .events
                .iter()
                .map(|event| event.name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "response.output_item.added",
                "response.output_text.delta",
                "response.output_text.delta",
                "response.output_item.done",
            ]
        );
        for event in &translated.events {
            assert_eq!(event.data["response_id"], "resp_test");
        }
        assert_eq!(translated.events[0].data["output_index"], 0);
        assert_eq!(translated.events[1].data["output_index"], 0);
        assert_eq!(translated.events[2].data["output_index"], 0);
        assert_eq!(translated.events[3].data["output_index"], 0);
        assert_eq!(translated.events[1].data["content_index"], 0);
        assert_eq!(translated.events[2].data["content_index"], 0);
        assert_eq!(translated.events[0].data["item"]["type"], "message");
        assert_eq!(translated.events[0].data["item"]["role"], "assistant");
        assert_eq!(translated.events[3].data["item"]["type"], "message");
        assert_eq!(translated.events[3].data["item"]["role"], "assistant");
        assert_eq!(translated.events[0].data["item"]["content"][0]["text"], "");
        assert_eq!(translated.events[1].data["delta"], "Hel");
        assert_eq!(translated.events[2].data["delta"], "lo");
        assert_eq!(
            translated.events[0].data["item"]["id"],
            translated.events[1].data["item_id"]
        );
        assert_eq!(
            translated.events[1].data["item_id"],
            translated.events[2].data["item_id"]
        );
        assert_eq!(
            translated.events[2].data["item_id"],
            translated.events[3].data["item"]["id"]
        );
        assert_eq!(
            translated.events[3].data["item"]["content"][0]["text"],
            "Hello"
        );
        let item_id = translated.output_items[0].id.to_string();
        assert_eq!(
            translated.events[0].data["item"]["id"].as_str(),
            Some(item_id.as_str())
        );
        assert_eq!(
            translated.events[1].data["item_id"].as_str(),
            Some(item_id.as_str())
        );
        assert_eq!(
            translated.events[2].data["item_id"].as_str(),
            Some(item_id.as_str())
        );
        assert_eq!(
            translated.events[3].data["item"]["id"].as_str(),
            Some(item_id.as_str())
        );
    }

    #[test]
    fn fragmented_tool_calls_are_latched_by_numeric_index() {
        let translated = translate_stream_chunks(
            &response_id(),
            "model-test",
            vec![
                chunk(
                    vec![choice(
                        None,
                        Some(delta(
                            None,
                            vec![tool_delta(
                                Some(0),
                                Some("call_weather"),
                                None,
                                Some(r#"{"city":"#),
                            )],
                        )),
                        None,
                    )],
                    None,
                ),
                chunk(
                    vec![choice(
                        None,
                        Some(delta(
                            None,
                            vec![tool_delta(
                                Some(1),
                                None,
                                Some("search"),
                                Some(r#"{"query":"#),
                            )],
                        )),
                        None,
                    )],
                    None,
                ),
                chunk(
                    vec![choice(
                        None,
                        Some(delta(
                            None,
                            vec![
                                tool_delta(Some(0), None, Some("weather"), Some(r#""Dubai"}"#)),
                                tool_delta(Some(1), Some("call_search"), None, Some(r#""Rust"}"#)),
                            ],
                        )),
                        None,
                    )],
                    None,
                ),
                chunk(
                    vec![choice(
                        None,
                        Some(delta(None, Vec::new())),
                        Some("tool_calls"),
                    )],
                    Some(usage(10, 4, 14)),
                ),
            ],
        )
        .expect("tool stream should translate");

        assert_eq!(translated.terminal, StreamTerminal::Completed);
        assert_eq!(translated.output_items.len(), 2);
        assert_eq!(
            translated.usage,
            UsageTotals {
                input_tokens: 10,
                output_tokens: 4,
                total_tokens: 14,
            }
        );
        assert_eq!(translated.events.len(), 4);
        assert_eq!(
            translated
                .events
                .iter()
                .map(|event| event.name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "response.output_item.added",
                "response.output_item.done",
                "response.output_item.added",
                "response.output_item.done",
            ]
        );
        assert_eq!(
            translated.events[0].data["item"]["id"],
            translated.events[1].data["item"]["id"]
        );
        assert_eq!(
            translated.events[2].data["item"]["id"],
            translated.events[3].data["item"]["id"]
        );
        assert_eq!(translated.events[0].data["output_index"], 0);
        assert_eq!(translated.events[1].data["output_index"], 0);
        assert_eq!(translated.events[2].data["output_index"], 1);
        assert_eq!(translated.events[3].data["output_index"], 1);
        assert_eq!(
            translated.events[1].data["item"],
            serde_json::json!({
                "id": translated.events[1].data["item"]["id"],
                "type": "function_call",
                "name": "weather",
                "arguments": r#"{"city":"Dubai"}"#,
                "call_id": "call_weather"
            })
        );
        assert_eq!(translated.events[3].data["item"]["name"], "search");
        assert_eq!(
            translated.events[3].data["item"]["arguments"],
            r#"{"query":"Rust"}"#
        );
        assert_eq!(translated.events[3].data["item"]["type"], "function_call");
        assert_eq!(translated.events[3].data["item"]["call_id"], "call_search");
        let first_item_id = translated.output_items[0].id.to_string();
        let second_item_id = translated.output_items[1].id.to_string();
        assert_eq!(
            translated.events[0].data["item"]["id"].as_str(),
            Some(first_item_id.as_str())
        );
        assert_eq!(
            translated.events[1].data["item"]["id"].as_str(),
            Some(first_item_id.as_str())
        );
        assert_eq!(
            translated.events[2].data["item"]["id"].as_str(),
            Some(second_item_id.as_str())
        );
        assert_eq!(
            translated.events[3].data["item"]["id"].as_str(),
            Some(second_item_id.as_str())
        );
        assert!(matches!(
            &translated.output_items[0].item,
            CanonicalItem::FunctionCall { call_id, name, arguments }
                if call_id.as_str() == "call_weather"
                    && name == "weather"
                    && arguments == r#"{"city":"Dubai"}"#
        ));
        assert!(matches!(
            &translated.output_items[1].item,
            CanonicalItem::FunctionCall { call_id, name, arguments }
                if call_id.as_str() == "call_search"
                    && name == "search"
                    && arguments == r#"{"query":"Rust"}"#
        ));
    }

    #[test]
    fn malformed_tool_call_latches_return_typed_errors() {
        let cases = [
            (
                "missing id",
                vec![tool_delta(Some(0), None, Some("weather"), Some("{}"))],
            ),
            (
                "missing name",
                vec![tool_delta(Some(0), Some("call_weather"), None, Some("{}"))],
            ),
        ];

        for (label, tool_calls) in cases {
            let error = translate_stream_chunks(
                &response_id(),
                "model-test",
                vec![chunk(
                    vec![choice(
                        Some(0),
                        Some(delta(None, tool_calls)),
                        Some("tool_calls"),
                    )],
                    None,
                )],
            )
            .expect_err(label);
            let expected = if label == "missing id" {
                "missing tool call id"
            } else {
                "missing tool call name"
            };
            assert!(
                matches!(
                    error,
                    ProxyError::MalformedStream(message) if message == expected
                ),
                "{label}"
            );
        }

        let conflicting_id = translate_stream_chunks(
            &response_id(),
            "model-test",
            vec![
                chunk(
                    vec![choice(
                        Some(0),
                        Some(delta(
                            None,
                            vec![tool_delta(Some(0), Some("call_a"), None, None)],
                        )),
                        None,
                    )],
                    None,
                ),
                chunk(
                    vec![choice(
                        Some(0),
                        Some(delta(
                            None,
                            vec![tool_delta(Some(0), Some("call_b"), None, None)],
                        )),
                        None,
                    )],
                    None,
                ),
            ],
        )
        .expect_err("conflicting id should fail");
        assert!(matches!(
            conflicting_id,
            ProxyError::MalformedStream(message) if message == "conflicting tool_call.id"
        ));

        let conflicting_name = translate_stream_chunks(
            &response_id(),
            "model-test",
            vec![
                chunk(
                    vec![choice(
                        Some(0),
                        Some(delta(
                            None,
                            vec![tool_delta(Some(0), None, Some("weather"), None)],
                        )),
                        None,
                    )],
                    None,
                ),
                chunk(
                    vec![choice(
                        Some(0),
                        Some(delta(
                            None,
                            vec![tool_delta(Some(0), None, Some("search"), None)],
                        )),
                        None,
                    )],
                    None,
                ),
            ],
        )
        .expect_err("conflicting name should fail");
        assert!(matches!(
            conflicting_name,
            ProxyError::MalformedStream(message) if message == "conflicting tool_call.function.name"
        ));
    }

    #[test]
    fn terminal_finish_reasons_map_to_explicit_states() {
        let cases = [
            (Some("stop"), StreamTerminal::Completed),
            (
                Some("length"),
                StreamTerminal::Incomplete("max_output_tokens".to_string()),
            ),
            (
                Some("content_filter"),
                StreamTerminal::Incomplete("content_filter".to_string()),
            ),
            (
                Some("provider_reason"),
                StreamTerminal::Failed("unsupported_finish_reason_provider_reason".to_string()),
            ),
            (
                None,
                StreamTerminal::Failed("done_without_finish_reason".to_string()),
            ),
        ];

        for (finish_reason, expected_terminal) in cases {
            let translated = translate_stream_chunks(
                &response_id(),
                "model-test",
                vec![chunk(
                    vec![choice(
                        None,
                        Some(delta(Some("partial"), Vec::new())),
                        finish_reason,
                    )],
                    Some(usage(7, 3, 10)),
                )],
            )
            .expect("terminal state should translate");

            assert_eq!(translated.terminal, expected_terminal);
            assert_eq!(translated.output_items.len(), 1);
            assert_eq!(
                translated.usage,
                UsageTotals {
                    input_tokens: 7,
                    output_tokens: 3,
                    total_tokens: 10,
                }
            );
            assert_eq!(
                translated
                    .events
                    .iter()
                    .map(|event| event.name.as_str())
                    .collect::<Vec<_>>(),
                if matches!(expected_terminal, StreamTerminal::Incomplete(_)) {
                    vec![
                        "response.output_item.added",
                        "response.output_text.delta",
                        "response.output_item.done",
                        "response.incomplete",
                    ]
                } else {
                    vec![
                        "response.output_item.added",
                        "response.output_text.delta",
                        "response.output_item.done",
                    ]
                }
            );
            match expected_terminal {
                StreamTerminal::Incomplete(reason) => {
                    let event = translated.events.last().expect("incomplete event");
                    assert_eq!(event.name, "response.incomplete");
                    assert_eq!(event.data["response"]["status"], "incomplete");
                    assert_eq!(
                        event.data["response"]["incomplete_details"]["reason"],
                        reason
                    );
                    assert_eq!(
                        event.data["response"]["output"][0]["content"][0]["text"],
                        "partial"
                    );
                    assert_eq!(
                        event.data["response"]["usage"],
                        serde_json::json!({
                            "input_tokens": 7,
                            "output_tokens": 3,
                            "total_tokens": 10
                        })
                    );
                }
                StreamTerminal::Completed | StreamTerminal::Failed(_) => {
                    assert_ne!(
                        translated.events.last().map(|event| event.name.as_str()),
                        Some("response.incomplete")
                    );
                }
            }
        }
    }

    #[test]
    fn usage_only_chunks_and_absent_choices_are_safe() {
        let translated = translate_stream_chunks(
            &response_id(),
            "model-test",
            vec![
                chunk(
                    vec![choice(
                        None,
                        Some(delta(Some(""), Vec::new())),
                        Some("stop"),
                    )],
                    None,
                ),
                chunk(Vec::new(), Some(usage(4, 1, 5))),
            ],
        )
        .expect("empty stream deltas should be safe");

        assert_eq!(translated.terminal, StreamTerminal::Completed);
        assert!(translated.output_items.is_empty());
        assert!(translated.events.is_empty());
        assert_eq!(
            translated.usage,
            UsageTotals {
                input_tokens: 4,
                output_tokens: 1,
                total_tokens: 5,
            }
        );
    }

    #[test]
    fn usage_only_stream_without_finish_reason_fails_explicitly() {
        let error_state = translate_stream_chunks(
            &response_id(),
            "model-test",
            vec![chunk(Vec::new(), Some(usage(8, 2, 10)))],
        )
        .expect("provider termination should produce a translation");

        assert_eq!(
            error_state.terminal,
            StreamTerminal::Failed("done_without_finish_reason".to_string())
        );
        assert_eq!(error_state.output_items.len(), 0);
        assert_eq!(error_state.events.len(), 0);
        assert_eq!(
            error_state.usage,
            UsageTotals {
                input_tokens: 8,
                output_tokens: 2,
                total_tokens: 10,
            }
        );
    }
}
