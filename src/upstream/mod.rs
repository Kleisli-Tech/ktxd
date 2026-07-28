use crate::config::{AuthHeaderKind, ModelConfig};
use crate::error::{ProxyError, Result};
use crate::wire::chat::{ChatCompletionRequest, ChatCompletionResponse};
use async_trait::async_trait;
use futures_util::StreamExt;
use reqwest::StatusCode;

#[async_trait]
pub trait ChatCompletions: Send + Sync {
    async fn complete(
        &self,
        model_config: &ModelConfig,
        request: ChatCompletionRequest,
    ) -> Result<ChatCompletionResponse>;

    async fn stream(
        &self,
        model_config: &ModelConfig,
        request: ChatCompletionRequest,
    ) -> Result<Vec<ChatCompletionResponse>>;
}

#[derive(Debug, Clone)]
pub struct ReqwestChatCompletionsClient {
    client: reqwest::Client,
}

impl Default for ReqwestChatCompletionsClient {
    fn default() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl ReqwestChatCompletionsClient {
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }

    async fn send_request(
        &self,
        model_config: &ModelConfig,
        request: &ChatCompletionRequest,
    ) -> Result<reqwest::Response> {
        let auth_value = model_config.auth_value()?;
        let mut builder = self
            .client
            .post(&model_config.chat_completions_url)
            .json(request);
        builder = match model_config.auth_header {
            AuthHeaderKind::ApiKey => builder.header("api-key", auth_value),
            AuthHeaderKind::AuthorizationBearer => builder.bearer_auth(auth_value),
        };
        builder
            .send()
            .await
            .map_err(|error| ProxyError::Upstream(error.to_string()))
    }

    async fn send_with_stream_options_retry(
        &self,
        model_config: &ModelConfig,
        request: ChatCompletionRequest,
    ) -> Result<reqwest::Response> {
        let response = self.send_request(model_config, &request).await?;
        if is_retry_candidate(model_config, &request, response.status()) {
            let status = response.status();
            let body_summary = response.text().await.unwrap_or_default();
            if is_stream_options_incompatibility(&body_summary) {
                let mut retry_request = request;
                retry_request.stream_options = None;
                return self.send_request(model_config, &retry_request).await;
            }
            return Err(ProxyError::Upstream(format!(
                "upstream returned status {status}: {}",
                sanitized_body_summary(&body_summary)
            )));
        }
        Ok(response)
    }
}

#[async_trait]
impl ChatCompletions for ReqwestChatCompletionsClient {
    async fn complete(
        &self,
        model_config: &ModelConfig,
        request: ChatCompletionRequest,
    ) -> Result<ChatCompletionResponse> {
        let response = self.send_request(model_config, &request).await?;
        let response = validate_response(response).await?;
        response
            .json::<ChatCompletionResponse>()
            .await
            .map_err(|error| ProxyError::Upstream(format!("malformed upstream response: {error}")))
    }

    async fn stream(
        &self,
        model_config: &ModelConfig,
        request: ChatCompletionRequest,
    ) -> Result<Vec<ChatCompletionResponse>> {
        let response = self
            .send_with_stream_options_retry(model_config, request)
            .await?;
        let response = validate_response(response).await?;
        parse_sse_response(response).await
    }
}

pub fn should_retry_without_stream_options(
    model_config: &ModelConfig,
    request: &ChatCompletionRequest,
    status: StatusCode,
    body: &str,
) -> bool {
    is_retry_candidate(model_config, request, status) && is_stream_options_incompatibility(body)
}

fn is_retry_candidate(
    model_config: &ModelConfig,
    request: &ChatCompletionRequest,
    status: StatusCode,
) -> bool {
    model_config.retry_without_stream_options_on_4xx
        && request.stream_options.is_some()
        && status.is_client_error()
        && status != StatusCode::UNAUTHORIZED
        && status != StatusCode::FORBIDDEN
}

fn is_stream_options_incompatibility(body: &str) -> bool {
    let body = body.to_ascii_lowercase();
    body.contains("stream_options") || body.contains("stream options")
}

fn sanitized_body_summary(body: &str) -> String {
    const MAX_LEN: usize = 512;
    let collapsed = body.split_whitespace().collect::<Vec<_>>().join(" ");
    collapsed.chars().take(MAX_LEN).collect()
}

pub fn status_to_error(status: StatusCode) -> Option<ProxyError> {
    if status.is_success() {
        return None;
    }
    Some(status_body_to_error(status, ""))
}

pub fn status_body_to_error(status: StatusCode, body: &str) -> ProxyError {
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        ProxyError::UpstreamAuth
    } else if body.trim().is_empty() {
        ProxyError::Upstream(format!("upstream returned status {status}"))
    } else {
        ProxyError::Upstream(format!(
            "upstream returned status {status}: {}",
            sanitized_body_summary(body)
        ))
    }
}

async fn validate_response(response: reqwest::Response) -> Result<reqwest::Response> {
    let status = response.status();
    if status.is_success() {
        Ok(response)
    } else {
        let body = response.text().await.unwrap_or_default();
        Err(status_body_to_error(status, &body))
    }
}

async fn parse_sse_response(response: reqwest::Response) -> Result<Vec<ChatCompletionResponse>> {
    let mut chunks = Vec::new();
    let mut buffer = String::new();
    let mut byte_stream = response.bytes_stream();
    while let Some(next_chunk) = byte_stream.next().await {
        let bytes = next_chunk.map_err(|error| ProxyError::Upstream(error.to_string()))?;
        buffer.push_str(&String::from_utf8_lossy(&bytes));
        while let Some(frame_end) = buffer.find("\n\n") {
            let frame = buffer[..frame_end].to_string();
            buffer = buffer[frame_end + 2..].to_string();
            parse_sse_frame(&frame, &mut chunks)?;
        }
    }
    if !buffer.trim().is_empty() {
        parse_sse_frame(&buffer, &mut chunks)?;
    }
    Ok(chunks)
}

fn parse_sse_frame(frame: &str, chunks: &mut Vec<ChatCompletionResponse>) -> Result<()> {
    for line in frame.lines() {
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let data = data.trim();
        if data == "[DONE]" {
            continue;
        }
        let chunk = serde_json::from_str::<ChatCompletionResponse>(data)
            .map_err(|error| ProxyError::MalformedStream(error.to_string()))?;
        chunks.push(chunk);
    }
    Ok(())
}
