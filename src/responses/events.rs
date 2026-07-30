use crate::domain::{CanonicalItem, TaggedItem, UsageTotals};
use crate::ids::ResponseId;
use crate::wire::responses::{IncompleteDetails, ResponseObject, ResponsesUsage};
use async_trait::async_trait;
use chrono::Utc;
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};

#[derive(Debug, Clone)]
pub struct ResponseEvent {
    pub name: String,
    pub data: Value,
}

#[async_trait]
pub trait ResponseEventSink: Send {
    async fn emit(&mut self, event: ResponseEvent);
}

#[derive(Debug, Default)]
pub struct VecEventSink {
    pub events: Vec<ResponseEvent>,
}

#[async_trait]
impl ResponseEventSink for VecEventSink {
    async fn emit(&mut self, event: ResponseEvent) {
        self.events.push(event);
    }
}

#[derive(Debug, Default)]
pub struct ChannelEventState {
    pub last_sequence_number: Option<u64>,
    pub response_id: Option<String>,
}

pub struct ChannelEventSink {
    sender: mpsc::Sender<ResponseEvent>,
    state: Arc<Mutex<ChannelEventState>>,
}

impl ChannelEventSink {
    pub fn new(sender: mpsc::Sender<ResponseEvent>) -> Self {
        Self::with_state(sender, Arc::new(Mutex::new(ChannelEventState::default())))
    }

    pub fn with_state(
        sender: mpsc::Sender<ResponseEvent>,
        state: Arc<Mutex<ChannelEventState>>,
    ) -> Self {
        Self { sender, state }
    }
}

#[async_trait]
impl ResponseEventSink for ChannelEventSink {
    async fn emit(&mut self, event: ResponseEvent) {
        update_channel_state(&self.state, &event).await;
        let _ = self.sender.send(event).await;
    }
}

async fn update_channel_state(state: &Arc<Mutex<ChannelEventState>>, event: &ResponseEvent) {
    let mut state = state.lock().await;
    if let Some(sequence_number) = event.data.get("sequence_number").and_then(Value::as_u64) {
        state.last_sequence_number = Some(sequence_number);
    }
    if let Some(response_id) = event.data.get("response_id").and_then(Value::as_str) {
        state.response_id = Some(response_id.to_string());
    }
    if let Some(response_id) = event
        .data
        .get("response")
        .and_then(|response| response.get("id"))
        .and_then(Value::as_str)
    {
        state.response_id = Some(response_id.to_string());
    }
}

pub fn created_event(response_id: &ResponseId, model: &str) -> ResponseEvent {
    let response = base_response(response_id, model, "in_progress", Vec::new(), None, None);
    event(
        "response.created",
        json!({"type":"response.created","response": response}),
    )
}

pub fn output_item_added_event(
    response_id: &ResponseId,
    index: usize,
    item: &TaggedItem,
) -> ResponseEvent {
    event(
        "response.output_item.added",
        json!({
            "type":"response.output_item.added",
            "response_id": response_id.to_string(),
            "output_index": index,
            "item": tagged_item_to_response_json(item),
        }),
    )
}

pub fn output_text_delta_event(
    response_id: &ResponseId,
    item_id: &str,
    index: usize,
    delta: &str,
) -> ResponseEvent {
    event(
        "response.output_text.delta",
        json!({
            "type":"response.output_text.delta",
            "response_id": response_id.to_string(),
            "item_id": item_id,
            "output_index": index,
            "content_index": 0,
            "delta": delta,
        }),
    )
}

pub fn output_item_done_event(
    response_id: &ResponseId,
    index: usize,
    item: &TaggedItem,
) -> ResponseEvent {
    event(
        "response.output_item.done",
        json!({
            "type":"response.output_item.done",
            "response_id": response_id.to_string(),
            "output_index": index,
            "item": tagged_item_to_response_json(item),
        }),
    )
}

pub fn completed_event(
    response_id: &ResponseId,
    model: &str,
    output_items: &[TaggedItem],
    usage: &UsageTotals,
) -> ResponseEvent {
    let response = completed_response_object(response_id, model, output_items, usage);
    event(
        "response.completed",
        json!({"type":"response.completed","response": response}),
    )
}

pub fn incomplete_event(
    response_id: &ResponseId,
    model: &str,
    output_items: &[TaggedItem],
    usage: &UsageTotals,
    reason: &str,
) -> ResponseEvent {
    let response = base_response(
        response_id,
        model,
        "incomplete",
        output_items
            .iter()
            .map(tagged_item_to_response_json)
            .collect(),
        Some(usage),
        Some(reason),
    );
    event(
        "response.incomplete",
        json!({"type":"response.incomplete","response": response}),
    )
}

pub fn failed_event(
    response_id: &ResponseId,
    model: &str,
    code: &str,
    message: &str,
) -> ResponseEvent {
    failed_event_with_usage(response_id, model, code, message, None)
}

pub fn failed_event_with_usage(
    response_id: &ResponseId,
    model: &str,
    code: &str,
    message: &str,
    usage: Option<&UsageTotals>,
) -> ResponseEvent {
    let mut response = base_response(response_id, model, "failed", Vec::new(), usage, None);
    response.error = Some(json!({"code": code, "message": message}));
    event(
        "response.failed",
        json!({"type":"response.failed","response": response}),
    )
}

pub fn completed_response_object(
    response_id: &ResponseId,
    model: &str,
    output_items: &[TaggedItem],
    usage: &UsageTotals,
) -> ResponseObject {
    base_response(
        response_id,
        model,
        "completed",
        output_items
            .iter()
            .map(tagged_item_to_response_json)
            .collect(),
        Some(usage),
        None,
    )
}

pub fn base_response(
    response_id: &ResponseId,
    model: &str,
    status: &str,
    output: Vec<Value>,
    usage: Option<&UsageTotals>,
    incomplete_reason: Option<&str>,
) -> ResponseObject {
    ResponseObject {
        id: response_id.to_string(),
        object_type: "response".to_string(),
        created_at: Utc::now().timestamp(),
        model: model.to_string(),
        status: status.to_string(),
        output,
        usage: usage.map(|totals| ResponsesUsage {
            input_tokens: totals.input_tokens,
            output_tokens: totals.output_tokens,
            total_tokens: totals.total_tokens,
        }),
        incomplete_details: incomplete_reason.map(|reason| IncompleteDetails {
            reason: reason.to_string(),
        }),
        error: None,
    }
}

pub fn tagged_item_to_response_json(item: &TaggedItem) -> Value {
    match &item.item {
        CanonicalItem::Message { role, text } => json!({
            "id": item.id.to_string(),
            "type": "message",
            "role": match role {
                crate::domain::MessageRole::System => "system",
                crate::domain::MessageRole::Developer => "developer",
                crate::domain::MessageRole::User => "user",
                crate::domain::MessageRole::Assistant => "assistant",
            },
            "content": [{"type":"output_text", "text": text}],
        }),
        CanonicalItem::FunctionCall {
            call_id,
            name,
            arguments,
        } => json!({
            "id": item.id.to_string(),
            "type": "function_call",
            "name": name,
            "arguments": arguments,
            "call_id": call_id.to_string(),
        }),
        CanonicalItem::FunctionCallOutput { call_id, output } => json!({
            "type": "function_call_output",
            "call_id": call_id.to_string(),
            "output": output.lower_to_chat_content(),
        }),
        CanonicalItem::Reasoning { raw } => raw.clone(),
    }
}

pub fn with_sequence_number(mut event: ResponseEvent, sequence_number: u64) -> ResponseEvent {
    if let Some(object) = event.data.as_object_mut() {
        object.insert("sequence_number".to_string(), json!(sequence_number));
    }
    event
}

pub fn sse_frame(event: &ResponseEvent) -> String {
    format!("event: {}\ndata: {}\n\n", event.name, event.data)
}

fn event(name: &str, data: Value) -> ResponseEvent {
    ResponseEvent {
        name: name.to_string(),
        data,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{
        CanonicalItem, FunctionOutput, FunctionOutputContentItem, MessageRole, ProvenanceTag,
    };
    use crate::ids::CallId;
    use serde_json::json;
    use tokio::sync::mpsc;

    fn response_id() -> ResponseId {
        ResponseId::from_string("resp_test")
    }

    fn usage() -> UsageTotals {
        UsageTotals {
            input_tokens: 11,
            output_tokens: 7,
            total_tokens: 18,
        }
    }

    fn message_item() -> TaggedItem {
        TaggedItem::new(
            CanonicalItem::Message {
                role: MessageRole::Assistant,
                text: "hello".to_string(),
            },
            ProvenanceTag::model_semi(),
        )
    }

    fn assert_event_contract(event: &ResponseEvent, name: &str, expected_response_id: &str) {
        assert_eq!(event.name, name);
        assert_eq!(event.data.get("type").and_then(Value::as_str), Some(name));

        let actual_response_id = event
            .data
            .get("response_id")
            .and_then(Value::as_str)
            .or_else(|| {
                event
                    .data
                    .get("response")
                    .and_then(|response| response.get("id"))
                    .and_then(Value::as_str)
            });
        assert_eq!(actual_response_id, Some(expected_response_id));
    }

    fn assert_response_payload(response: &Value, expected: Value) {
        let created_at = response
            .get("created_at")
            .and_then(Value::as_i64)
            .expect("response should have an integer created_at");
        assert!(created_at > 0);

        let mut normalized = response.clone();
        normalized
            .as_object_mut()
            .expect("response should be an object")
            .remove("created_at");
        assert_eq!(normalized, expected);
    }

    #[test]
    fn constructors_emit_expected_event_contracts() {
        let response_id = response_id();
        let response_id_string = response_id.to_string();
        let item = message_item();
        let usage = usage();
        let item_json = tagged_item_to_response_json(&item);

        let created = created_event(&response_id, "test-model");
        assert_event_contract(&created, "response.created", &response_id_string);
        assert_response_payload(
            &created.data["response"],
            json!({
                "id": response_id_string,
                "object": "response",
                "model": "test-model",
                "status": "in_progress",
                "output": []
            }),
        );

        let added = output_item_added_event(&response_id, 2, &item);
        assert_event_contract(&added, "response.output_item.added", &response_id_string);
        assert_eq!(
            added.data,
            json!({
                "type": "response.output_item.added",
                "response_id": response_id_string,
                "output_index": 2,
                "item": item_json
            })
        );

        let delta = output_text_delta_event(&response_id, &item.id.to_string(), 2, "hel");
        assert_event_contract(&delta, "response.output_text.delta", &response_id_string);
        assert_eq!(
            delta.data,
            json!({
                "type": "response.output_text.delta",
                "response_id": response_id_string,
                "item_id": item.id.to_string(),
                "output_index": 2,
                "content_index": 0,
                "delta": "hel"
            })
        );

        let done = output_item_done_event(&response_id, 2, &item);
        assert_event_contract(&done, "response.output_item.done", &response_id_string);
        assert_eq!(
            done.data,
            json!({
                "type": "response.output_item.done",
                "response_id": response_id_string,
                "output_index": 2,
                "item": item_json
            })
        );

        let completed = completed_event(
            &response_id,
            "test-model",
            std::slice::from_ref(&item),
            &usage,
        );
        assert_event_contract(&completed, "response.completed", &response_id_string);
        assert_response_payload(
            &completed.data["response"],
            json!({
                "id": response_id_string,
                "object": "response",
                "model": "test-model",
                "status": "completed",
                "output": [item_json],
                "usage": {
                    "input_tokens": 11,
                    "output_tokens": 7,
                    "total_tokens": 18
                }
            }),
        );

        let incomplete = incomplete_event(
            &response_id,
            "test-model",
            std::slice::from_ref(&item),
            &usage,
            "max_output_tokens",
        );
        assert_event_contract(&incomplete, "response.incomplete", &response_id_string);
        assert_response_payload(
            &incomplete.data["response"],
            json!({
                "id": response_id_string,
                "object": "response",
                "model": "test-model",
                "status": "incomplete",
                "output": [item_json],
                "usage": {
                    "input_tokens": 11,
                    "output_tokens": 7,
                    "total_tokens": 18
                },
                "incomplete_details": {"reason": "max_output_tokens"}
            }),
        );

        let failed = failed_event(
            &response_id,
            "test-model",
            "upstream_error",
            "backend failed",
        );
        assert_event_contract(&failed, "response.failed", &response_id_string);
        assert_response_payload(
            &failed.data["response"],
            json!({
                "id": response_id_string,
                "object": "response",
                "model": "test-model",
                "status": "failed",
                "output": [],
                "error": {
                    "code": "upstream_error",
                    "message": "backend failed"
                }
            }),
        );
    }

    #[test]
    fn response_status_fields_include_only_applicable_details() {
        let response_id = response_id();
        let item = message_item();
        let usage = usage();

        let created = created_event(&response_id, "test-model");
        assert!(created.data["response"].get("usage").is_none());
        assert!(created.data["response"].get("incomplete_details").is_none());

        let completed = completed_event(
            &response_id,
            "test-model",
            std::slice::from_ref(&item),
            &usage,
        );
        assert_eq!(completed.data["response"]["usage"]["input_tokens"], 11);
        assert_eq!(completed.data["response"]["usage"]["output_tokens"], 7);
        assert_eq!(completed.data["response"]["usage"]["total_tokens"], 18);
        assert!(
            completed.data["response"]
                .get("incomplete_details")
                .is_none()
        );

        let incomplete = incomplete_event(
            &response_id,
            "test-model",
            &[item],
            &usage,
            "max_output_tokens",
        );
        assert_eq!(incomplete.data["response"]["status"], "incomplete");
        assert_eq!(
            incomplete.data["response"]["incomplete_details"]["reason"],
            "max_output_tokens"
        );
        assert_eq!(incomplete.data["response"]["usage"]["total_tokens"], 18);

        let failed = failed_event(&response_id, "test-model", "provider_error", "failed");
        assert!(failed.data["response"].get("usage").is_none());
        assert!(failed.data["response"].get("incomplete_details").is_none());
        assert_eq!(failed.data["response"]["error"]["message"], "failed");

        let failed_with_usage = failed_event_with_usage(
            &response_id,
            "test-model",
            "stream_failed",
            "backend stopped",
            Some(&usage),
        );
        assert_eq!(
            failed_with_usage.data["response"]["usage"],
            json!({"input_tokens": 11, "output_tokens": 7, "total_tokens": 18})
        );
    }

    #[test]
    fn tagged_items_lower_all_supported_response_shapes() {
        let user_item = TaggedItem::new(
            CanonicalItem::Message {
                role: MessageRole::User,
                text: "question".to_string(),
            },
            ProvenanceTag::user_trusted(),
        );
        assert_eq!(
            tagged_item_to_response_json(&user_item),
            json!({
                "id": user_item.id.to_string(),
                "type": "message",
                "role": "user",
                "content": [{"type": "output_text", "text": "question"}]
            })
        );

        let assistant_item = TaggedItem::new(
            CanonicalItem::Message {
                role: MessageRole::Assistant,
                text: "answer".to_string(),
            },
            ProvenanceTag::model_semi(),
        );
        assert_eq!(
            tagged_item_to_response_json(&assistant_item),
            json!({
                "id": assistant_item.id.to_string(),
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "answer"}]
            })
        );

        let function_call_item = TaggedItem::new(
            CanonicalItem::FunctionCall {
                call_id: CallId::from_string("call_test"),
                name: "lookup".to_string(),
                arguments: r#"{"city":"Dubai"}"#.to_string(),
            },
            ProvenanceTag::model_semi(),
        );
        assert_eq!(
            tagged_item_to_response_json(&function_call_item),
            json!({
                "id": function_call_item.id.to_string(),
                "type": "function_call",
                "name": "lookup",
                "arguments": r#"{"city":"Dubai"}"#,
                "call_id": "call_test"
            })
        );

        let text_function_output_item = TaggedItem::new(
            CanonicalItem::FunctionCallOutput {
                call_id: CallId::from_string("call_text"),
                output: FunctionOutput::Text {
                    text: "plain output".to_string(),
                },
            },
            ProvenanceTag::tool_output_semi(),
        );
        assert_eq!(
            tagged_item_to_response_json(&text_function_output_item),
            json!({
                "type": "function_call_output",
                "call_id": "call_text",
                "output": "plain output"
            })
        );

        let content_function_output_item = TaggedItem::new(
            CanonicalItem::FunctionCallOutput {
                call_id: CallId::from_string("call_test"),
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
            },
            ProvenanceTag::tool_output_semi(),
        );
        assert_eq!(
            tagged_item_to_response_json(&content_function_output_item),
            json!({
                "type": "function_call_output",
                "call_id": "call_test",
                "output": "first\nsecond"
            })
        );

        let reasoning_raw = json!({
            "id": "reasoning_test",
            "type": "reasoning",
            "summary": []
        });
        let reasoning = tagged_item_to_response_json(&TaggedItem::new(
            CanonicalItem::Reasoning {
                raw: reasoning_raw.clone(),
            },
            ProvenanceTag::model_semi(),
        ));
        assert_eq!(reasoning, reasoning_raw);
    }

    #[test]
    fn sequence_numbers_are_top_level_and_preserve_event_fields() {
        let original = output_text_delta_event(&response_id(), "item_test", 1, "hello");
        assert!(original.data.get("sequence_number").is_none());

        let inserted = with_sequence_number(original.clone(), 8);
        let mut expected = original.data.clone();
        expected["sequence_number"] = json!(8);
        assert_eq!(inserted.name, original.name);
        assert_eq!(inserted.data, expected);

        let replaced = with_sequence_number(inserted, 9);
        expected["sequence_number"] = json!(9);
        assert_eq!(replaced.data, expected);
    }

    #[tokio::test]
    async fn channel_event_sink_tracks_direct_and_nested_state() {
        let (sender, mut receiver) = mpsc::channel(4);
        let state = Arc::new(Mutex::new(ChannelEventState::default()));
        let mut sink = ChannelEventSink::with_state(sender, Arc::clone(&state));

        sink.emit(ResponseEvent {
            name: "response.output_text.delta".to_string(),
            data: json!({
                "type": "response.output_text.delta",
                "response_id": "resp_direct",
                "sequence_number": 4
            }),
        })
        .await;
        let direct = receiver.recv().await.expect("direct event should be sent");
        assert_eq!(direct.data["response_id"], "resp_direct");
        {
            let state = state.lock().await;
            assert_eq!(state.last_sequence_number, Some(4));
            assert_eq!(state.response_id.as_deref(), Some("resp_direct"));
        }

        sink.emit(ResponseEvent {
            name: "response.completed".to_string(),
            data: json!({
                "type": "response.completed",
                "response": {"id": "resp_nested"},
                "sequence_number": 5
            }),
        })
        .await;
        let nested = receiver.recv().await.expect("nested event should be sent");
        assert_eq!(nested.data["response"]["id"], "resp_nested");
        let state = state.lock().await;
        assert_eq!(state.last_sequence_number, Some(5));
        assert_eq!(state.response_id.as_deref(), Some("resp_nested"));
    }

    #[test]
    fn sse_frame_emits_parseable_event_and_json_data() {
        let event = output_text_delta_event(&response_id(), "item_test", 0, "hello\nworld");
        let frame = sse_frame(&event);

        assert!(frame.starts_with("event: response.output_text.delta\n"));
        assert!(frame.ends_with("\n\n"));
        assert_eq!(frame.matches("\n\n").count(), 1);

        let mut lines = frame.lines();
        assert_eq!(lines.next(), Some("event: response.output_text.delta"));
        let data = lines
            .next()
            .and_then(|line| line.strip_prefix("data: "))
            .expect("frame should contain a data line");
        assert_eq!(
            serde_json::from_str::<Value>(data).expect("data is JSON"),
            event.data
        );
        assert_eq!(lines.next(), Some(""));
        assert_eq!(lines.next(), None);
    }
}
