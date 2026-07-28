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
    let mut response = base_response(response_id, model, "failed", Vec::new(), None, None);
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
            "role": match role { crate::domain::MessageRole::User => "user", crate::domain::MessageRole::Assistant => "assistant" },
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
