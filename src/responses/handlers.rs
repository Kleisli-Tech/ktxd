use crate::app_state::AppState;
use crate::error::{ProxyError, Result};
use crate::ids::ResponseId;
use crate::responses::{
    ChannelEventSink, ChannelEventState, VecEventSink, sse_frame, with_sequence_number,
};
use crate::session::SessionStore;
use crate::translator::normalize_request;
use crate::wire::responses::{ModelInfo, ModelsResponse, ResponsesRequest, TruncationPolicy};
use axum::extract::{Path, State};
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use axum::{
    Json, Router,
    routing::{get, post},
};
use futures_util::Stream;
use serde_json::{Value, json};
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/models", get(models))
        .route("/v1/responses", post(create_response))
        .route("/v1/responses/:response_id", get(get_response))
        .with_state(Arc::new(state))
}

async fn healthz() -> Json<Value> {
    Json(json!({"status":"ok"}))
}

async fn models(State(state): State<Arc<AppState>>) -> Json<ModelsResponse> {
    let models = state
        .config
        .models
        .values()
        .map(|model| ModelInfo {
            slug: model.public_model.clone(),
            display_name: model.display_name.clone(),
            description: Some(model.description.clone()),
            default_reasoning_level: None,
            supported_reasoning_levels: Vec::new(),
            shell_type: "shell_command".to_string(),
            visibility: "list".to_string(),
            supported_in_api: true,
            priority: 0,
            availability_nux: None,
            upgrade: None,
            base_instructions: String::new(),
            model_messages: None,
            supports_reasoning_summaries: false,
            default_reasoning_summary: "none".to_string(),
            support_verbosity: false,
            default_verbosity: None,
            apply_patch_tool_type: Some("function".to_string()),
            web_search_tool_type: "text".to_string(),
            truncation_policy: TruncationPolicy {
                mode: "tokens".to_string(),
                limit: model.context_window,
            },
            supports_parallel_tool_calls: true,
            supports_image_detail_original: false,
            context_window: Some(model.context_window),
            auto_compact_token_limit: None,
            effective_context_window_percent: 90,
            experimental_supported_tools: Vec::new(),
            input_modalities: vec!["text".to_string()],
            supports_search_tool: false,
        })
        .collect();
    Json(ModelsResponse { models })
}

async fn get_response(
    State(state): State<Arc<AppState>>,
    Path(response_id): Path<String>,
) -> Result<Json<Value>> {
    let response_id = ResponseId::from_string(response_id);
    let response = state
        .store
        .get_response_json(&response_id)
        .await?
        .ok_or_else(|| ProxyError::PreviousResponseNotFound(response_id.to_string()))?;
    Ok(Json(response))
}

async fn create_response(
    State(state): State<Arc<AppState>>,
    Json(request): Json<ResponsesRequest>,
) -> Result<Response> {
    let normalized = normalize_request(request)?;
    state.config.model(&normalized.model)?;
    let parent = match normalized.previous_response_id.as_ref() {
        Some(previous_response_id) => Some(
            state
                .store
                .get(previous_response_id)
                .await?
                .ok_or_else(|| {
                    ProxyError::PreviousResponseNotFound(previous_response_id.to_string())
                })?,
        ),
        None => None,
    };

    if normalized.stream {
        Ok(stream_response(state, parent, normalized).into_response())
    } else {
        let model = normalized.model.clone();
        let mut sink = VecEventSink::default();
        let record = state.driver.drive(parent, normalized, &mut sink).await?;
        Ok(Json(state.driver.non_streaming_response(&model, &record)).into_response())
    }
}

fn stream_response(
    state: Arc<AppState>,
    parent: Option<crate::domain::Session>,
    normalized: crate::translator::NormalizedTurnInput,
) -> Sse<impl Stream<Item = std::result::Result<Event, Infallible>>> {
    let (sender, receiver) = mpsc::channel(64);
    let event_state = Arc::new(Mutex::new(ChannelEventState::default()));
    tokio::spawn(async move {
        let model = normalized.model.clone();
        let mut sink = ChannelEventSink::with_state(sender.clone(), event_state.clone());
        if let Err(error) = state.driver.drive(parent, normalized, &mut sink).await {
            let event_state = event_state.lock().await;
            let response_id = event_state
                .response_id
                .clone()
                .map(ResponseId::from_string)
                .unwrap_or_else(ResponseId::new);
            let sequence_number = event_state
                .last_sequence_number
                .map_or(0, |sequence_number| sequence_number + 1);
            let failed = with_sequence_number(
                crate::responses::failed_event(
                    &response_id,
                    &model,
                    error.code(),
                    &error.to_string(),
                ),
                sequence_number,
            );
            let _ = sender.send(failed).await;
        }
    });
    let stream = ReceiverStream::new(receiver).map(|event| {
        let frame = sse_frame(&event);
        let parsed_event = Event::default()
            .event(event.name)
            .data(event.data.to_string());
        let _ = frame;
        Ok(parsed_event)
    });
    Sse::new(stream)
}
