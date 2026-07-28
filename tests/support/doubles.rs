#![allow(dead_code)]

use async_trait::async_trait;
use ktxd::config::ModelConfig;
use ktxd::domain::{Session, TaggedItem, TurnRecord};
use ktxd::error::{ProxyError, Result};
use ktxd::substrate::{NodeSink, SeedResolver};
use ktxd::upstream::ChatCompletions;
use ktxd::wire::chat::{ChatCompletionRequest, ChatCompletionResponse};
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestKind {
    Complete,
    Stream,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordedErrorSource {
    Queued,
    Configured,
    MissingQueue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedError {
    pub source: RecordedErrorSource,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone)]
pub enum RecordedResult {
    Complete(ChatCompletionResponse),
    Stream(Vec<ChatCompletionResponse>),
    Error(RecordedError),
}

#[derive(Debug, Clone)]
pub struct RecordedChatRequest {
    pub kind: RequestKind,
    pub model_config: ModelConfig,
    pub request: ChatCompletionRequest,
    pub serialized_request: Value,
    pub result: RecordedResult,
}

#[derive(Debug, Default)]
struct FakeChatCompletionsState {
    requests: Vec<RecordedChatRequest>,
    complete_results: VecDeque<Result<ChatCompletionResponse>>,
    stream_results: VecDeque<Result<Vec<ChatCompletionResponse>>>,
}

#[derive(Debug, Clone, Default)]
pub struct FakeChatCompletions {
    state: Arc<Mutex<FakeChatCompletionsState>>,
}

impl FakeChatCompletions {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn queue_complete(&self, response: ChatCompletionResponse) {
        self.state
            .lock()
            .await
            .complete_results
            .push_back(Ok(response));
    }

    pub async fn queue_complete_error(&self, error: ProxyError) {
        self.state
            .lock()
            .await
            .complete_results
            .push_back(Err(error));
    }

    pub async fn queue_complete_result(&self, result: Result<ChatCompletionResponse>) {
        self.state.lock().await.complete_results.push_back(result);
    }

    pub async fn queue_stream(&self, responses: Vec<ChatCompletionResponse>) {
        self.state
            .lock()
            .await
            .stream_results
            .push_back(Ok(responses));
    }

    pub async fn queue_stream_error(&self, error: ProxyError) {
        self.state.lock().await.stream_results.push_back(Err(error));
    }

    pub async fn queue_stream_result(&self, result: Result<Vec<ChatCompletionResponse>>) {
        self.state.lock().await.stream_results.push_back(result);
    }

    pub async fn requests(&self) -> Vec<RecordedChatRequest> {
        self.state.lock().await.requests.clone()
    }

    pub async fn complete_requests(&self) -> Vec<ChatCompletionRequest> {
        self.state
            .lock()
            .await
            .requests
            .iter()
            .filter(|request| request.kind == RequestKind::Complete)
            .map(|request| request.request.clone())
            .collect()
    }

    pub async fn stream_requests(&self) -> Vec<ChatCompletionRequest> {
        self.state
            .lock()
            .await
            .requests
            .iter()
            .filter(|request| request.kind == RequestKind::Stream)
            .map(|request| request.request.clone())
            .collect()
    }

    pub async fn request_count(&self) -> usize {
        self.state.lock().await.requests.len()
    }
}

#[async_trait]
impl ChatCompletions for FakeChatCompletions {
    async fn complete(
        &self,
        model_config: &ModelConfig,
        request: ChatCompletionRequest,
    ) -> Result<ChatCompletionResponse> {
        let mut state = self.state.lock().await;
        let serialized_request = serde_json::to_value(&request).expect("request serializes");
        let (result, recorded_result) = match state.complete_results.pop_front() {
            Some(result) => {
                let recorded_result = match &result {
                    Ok(response) => RecordedResult::Complete(response.clone()),
                    Err(error) => {
                        RecordedResult::Error(recorded_error(RecordedErrorSource::Queued, error))
                    }
                };
                (result, recorded_result)
            }
            None => {
                let message = "fake upstream has no queued complete result".to_string();
                let error = ProxyError::Internal(message.clone());
                (
                    Err(ProxyError::Internal(message)),
                    RecordedResult::Error(recorded_error(
                        RecordedErrorSource::MissingQueue,
                        &error,
                    )),
                )
            }
        };
        state.requests.push(RecordedChatRequest {
            kind: RequestKind::Complete,
            model_config: model_config.clone(),
            serialized_request,
            request,
            result: recorded_result,
        });
        result
    }

    async fn stream(
        &self,
        model_config: &ModelConfig,
        request: ChatCompletionRequest,
    ) -> Result<Vec<ChatCompletionResponse>> {
        let mut state = self.state.lock().await;
        let serialized_request = serde_json::to_value(&request).expect("request serializes");
        let (result, recorded_result) = match state.stream_results.pop_front() {
            Some(result) => {
                let recorded_result = match &result {
                    Ok(responses) => RecordedResult::Stream(responses.clone()),
                    Err(error) => {
                        RecordedResult::Error(recorded_error(RecordedErrorSource::Queued, error))
                    }
                };
                (result, recorded_result)
            }
            None => {
                let message = "fake upstream has no queued stream result".to_string();
                let error = ProxyError::Internal(message.clone());
                (
                    Err(ProxyError::Internal(message)),
                    RecordedResult::Error(recorded_error(
                        RecordedErrorSource::MissingQueue,
                        &error,
                    )),
                )
            }
        };
        state.requests.push(RecordedChatRequest {
            kind: RequestKind::Stream,
            model_config: model_config.clone(),
            serialized_request,
            request,
            result: recorded_result,
        });
        result
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodeCommit {
    pub session: Session,
    pub record: TurnRecord,
    pub outcome: RecordedResultStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordedResultStatus {
    Succeeded,
    Failed(RecordedError),
}

#[derive(Debug, Default)]
struct FakeNodeSinkState {
    commits: Vec<NodeCommit>,
    error: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct FakeNodeSink {
    state: Arc<Mutex<FakeNodeSinkState>>,
}

impl FakeNodeSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn set_error(&self, error: impl Into<String>) {
        self.state.lock().await.error = Some(error.into());
    }

    pub async fn clear_error(&self) {
        self.state.lock().await.error = None;
    }

    pub async fn commits(&self) -> Vec<NodeCommit> {
        self.state.lock().await.commits.clone()
    }

    pub async fn call_count(&self) -> usize {
        self.state.lock().await.commits.len()
    }
}

#[async_trait]
impl NodeSink for FakeNodeSink {
    async fn on_turn_committed(&self, session: &Session, record: &TurnRecord) -> Result<()> {
        let mut state = self.state.lock().await;
        let result = match state.error.as_deref() {
            Some(error) => Err(ProxyError::Internal(error.to_string())),
            None => Ok(()),
        };
        let outcome = match &result {
            Ok(()) => RecordedResultStatus::Succeeded,
            Err(error) => {
                RecordedResultStatus::Failed(recorded_error(RecordedErrorSource::Configured, error))
            }
        };
        state.commits.push(NodeCommit {
            session: session.clone(),
            record: record.clone(),
            outcome,
        });
        result
    }
}

#[derive(Debug, Default)]
struct FakeSeedResolverState {
    items: Vec<TaggedItem>,
    calls: Vec<SeedResolution>,
    error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SeedResolution {
    pub session: Option<Session>,
    pub items: Vec<TaggedItem>,
    pub outcome: RecordedResultStatus,
}

#[derive(Debug, Clone, Default)]
pub struct FakeSeedResolver {
    state: Arc<Mutex<FakeSeedResolverState>>,
}

impl FakeSeedResolver {
    pub fn new(items: Vec<TaggedItem>) -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeSeedResolverState {
                items,
                ..FakeSeedResolverState::default()
            })),
        }
    }

    pub fn with_error(error: impl Into<String>) -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeSeedResolverState {
                error: Some(error.into()),
                ..FakeSeedResolverState::default()
            })),
        }
    }

    pub async fn set_items(&self, items: Vec<TaggedItem>) {
        self.state.lock().await.items = items;
    }

    pub async fn set_error(&self, error: impl Into<String>) {
        self.state.lock().await.error = Some(error.into());
    }

    pub async fn clear_error(&self) {
        self.state.lock().await.error = None;
    }

    pub async fn sessions(&self) -> Vec<Option<Session>> {
        self.state
            .lock()
            .await
            .calls
            .iter()
            .map(|call| call.session.clone())
            .collect()
    }

    pub async fn calls(&self) -> Vec<SeedResolution> {
        self.state.lock().await.calls.clone()
    }

    pub async fn call_count(&self) -> usize {
        self.state.lock().await.calls.len()
    }
}

#[async_trait]
impl SeedResolver for FakeSeedResolver {
    async fn resolve_seed_items(&self, session: Option<&Session>) -> Result<Vec<TaggedItem>> {
        let mut state = self.state.lock().await;
        let session = session.cloned();
        let result = match state.error.as_deref() {
            Some(error) => Err(ProxyError::Internal(error.to_string())),
            None => Ok(state.items.clone()),
        };
        let outcome = match &result {
            Ok(_) => RecordedResultStatus::Succeeded,
            Err(error) => {
                RecordedResultStatus::Failed(recorded_error(RecordedErrorSource::Configured, error))
            }
        };
        let items = result.as_ref().cloned().unwrap_or_default();
        state.calls.push(SeedResolution {
            session,
            items,
            outcome,
        });
        result
    }
}

fn recorded_error(source: RecordedErrorSource, error: &ProxyError) -> RecordedError {
    RecordedError {
        source,
        code: error.code().to_string(),
        message: error.to_string(),
    }
}

pub type RecordingChatCompletions = FakeChatCompletions;
pub type RecordingNodeSink = FakeNodeSink;
pub type RecordingSeedResolver = FakeSeedResolver;
