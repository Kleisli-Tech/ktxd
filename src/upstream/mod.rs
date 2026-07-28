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

    fn build_request(
        &self,
        model_config: &ModelConfig,
        request: &ChatCompletionRequest,
    ) -> Result<reqwest::Request> {
        let auth_value = model_config.auth_value()?;
        self.build_request_with_auth_value(model_config, request, &auth_value)
    }

    fn build_request_with_auth_value(
        &self,
        model_config: &ModelConfig,
        request: &ChatCompletionRequest,
        auth_value: &str,
    ) -> Result<reqwest::Request> {
        let mut builder = self
            .client
            .post(&model_config.chat_completions_url)
            .json(request);
        builder = match model_config.auth_header {
            AuthHeaderKind::ApiKey => builder.header("api-key", auth_value),
            AuthHeaderKind::AuthorizationBearer => builder.bearer_auth(auth_value),
        };
        builder
            .build()
            .map_err(|error| ProxyError::Upstream(error.to_string()))
    }

    async fn send_request(
        &self,
        model_config: &ModelConfig,
        request: &ChatCompletionRequest,
    ) -> Result<reqwest::Response> {
        let request = self.build_request(model_config, request)?;
        self.client
            .execute(request)
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
        drain_sse_buffer(&mut buffer, &mut chunks, false)?;
    }
    drain_sse_buffer(&mut buffer, &mut chunks, true)?;
    Ok(chunks)
}

fn drain_sse_buffer(
    buffer: &mut String,
    chunks: &mut Vec<ChatCompletionResponse>,
    flush_remainder: bool,
) -> Result<()> {
    while let Some(frame_end) = buffer.find("\n\n") {
        let frame = buffer[..frame_end].to_string();
        *buffer = buffer[frame_end + 2..].to_string();
        parse_sse_frame(&frame, chunks)?;
    }
    if flush_remainder {
        let frame = std::mem::take(buffer);
        if !frame.trim().is_empty() {
            parse_sse_frame(&frame, chunks)?;
        }
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AuthHeaderKind;
    use crate::wire::chat::{ChatChoice, ChatCompletionRequest, StreamOptions};
    use std::env;

    fn request(stream_options: Option<StreamOptions>) -> ChatCompletionRequest {
        ChatCompletionRequest {
            model: None,
            messages: Vec::new(),
            tools: Vec::new(),
            tool_choice: None,
            parallel_tool_calls: None,
            stream: true,
            stream_options,
        }
    }

    fn model(retry: bool, auth_header: AuthHeaderKind, auth_env_var: &str) -> ModelConfig {
        let mut model = ModelConfig::default_deepseek();
        model.retry_without_stream_options_on_4xx = retry;
        model.auth_header = auth_header;
        model.auth_env_var = auth_env_var.to_string();
        model.chat_completions_url = "http://127.0.0.1:9/chat/completions".to_string();
        model
    }

    fn response_chunk() -> ChatCompletionResponse {
        ChatCompletionResponse {
            id: Some("chunk-1".to_string()),
            choices: vec![ChatChoice {
                index: Some(0),
                message: None,
                delta: None,
                finish_reason: None,
            }],
            usage: None,
        }
    }

    #[test]
    fn status_to_error_maps_success_and_auth_statuses() {
        assert!(status_to_error(StatusCode::OK).is_none());
        assert!(matches!(
            status_to_error(StatusCode::UNAUTHORIZED),
            Some(ProxyError::UpstreamAuth)
        ));
        assert!(matches!(
            status_to_error(StatusCode::FORBIDDEN),
            Some(ProxyError::UpstreamAuth)
        ));
        assert!(matches!(
            status_to_error(StatusCode::BAD_GATEWAY),
            Some(ProxyError::Upstream(message)) if message == "upstream returned status 502 Bad Gateway"
        ));
    }

    #[test]
    fn status_body_to_error_collapses_and_bounds_body_summary() {
        let error = status_body_to_error(StatusCode::BAD_REQUEST, "  first\n\tsecond   third  ");
        assert!(matches!(
            error,
            ProxyError::Upstream(message)
                if message == "upstream returned status 400 Bad Request: first second third"
        ));

        let blank = status_body_to_error(StatusCode::BAD_REQUEST, " \n\t ");
        assert!(matches!(
            blank,
            ProxyError::Upstream(message) if message == "upstream returned status 400 Bad Request"
        ));

        let empty = status_body_to_error(StatusCode::BAD_REQUEST, "");
        assert!(matches!(
            empty,
            ProxyError::Upstream(message) if message == "upstream returned status 400 Bad Request"
        ));

        let error = status_body_to_error(StatusCode::BAD_REQUEST, &"x".repeat(600));
        let ProxyError::Upstream(message) = error else {
            panic!("expected upstream error");
        };
        assert_eq!(
            message,
            format!(
                "upstream returned status 400 Bad Request: {}",
                "x".repeat(512)
            )
        );
    }

    #[test]
    fn retry_predicate_requires_enabled_retry_stream_options_and_compatible_body() {
        let enabled = model(true, AuthHeaderKind::ApiKey, "KTXD_TEST_RETRY_ENABLED");
        let disabled = model(false, AuthHeaderKind::ApiKey, "KTXD_TEST_RETRY_DISABLED");
        let with_options = request(Some(StreamOptions {
            include_usage: true,
        }));
        let without_options = request(None);

        assert!(should_retry_without_stream_options(
            &enabled,
            &with_options,
            StatusCode::BAD_REQUEST,
            "unsupported stream_options field"
        ));
        assert!(!should_retry_without_stream_options(
            &disabled,
            &with_options,
            StatusCode::BAD_REQUEST,
            "unsupported stream_options field"
        ));
        assert!(!should_retry_without_stream_options(
            &enabled,
            &without_options,
            StatusCode::BAD_REQUEST,
            "unsupported stream_options field"
        ));
        assert!(!should_retry_without_stream_options(
            &enabled,
            &with_options,
            StatusCode::UNAUTHORIZED,
            "unsupported stream_options field"
        ));
        assert!(!should_retry_without_stream_options(
            &enabled,
            &with_options,
            StatusCode::FORBIDDEN,
            "unsupported stream_options field"
        ));
        assert!(!should_retry_without_stream_options(
            &enabled,
            &with_options,
            StatusCode::BAD_REQUEST,
            "some other validation error"
        ));
        assert!(!should_retry_without_stream_options(
            &enabled,
            &with_options,
            StatusCode::OK,
            "unsupported stream_options field"
        ));
        assert!(!should_retry_without_stream_options(
            &enabled,
            &with_options,
            StatusCode::INTERNAL_SERVER_ERROR,
            "unsupported stream_options field"
        ));
        assert!(should_retry_without_stream_options(
            &enabled,
            &with_options,
            StatusCode::UNPROCESSABLE_ENTITY,
            "STREAM OPTIONS are not supported"
        ));
    }

    #[test]
    fn build_request_constructs_api_key_and_bearer_auth_headers() {
        let client = ReqwestChatCompletionsClient::default();

        let api_key_request = client
            .build_request_with_auth_value(
                &model(true, AuthHeaderKind::ApiKey, "unused"),
                &request(None),
                "api-secret-value",
            )
            .expect("api-key request builds");
        assert_eq!(
            api_key_request.headers().get("api-key").unwrap(),
            "api-secret-value"
        );
        assert!(api_key_request.headers().get("authorization").is_none());

        let bearer_request = client
            .build_request_with_auth_value(
                &model(true, AuthHeaderKind::AuthorizationBearer, "unused"),
                &request(None),
                "bearer-secret-value",
            )
            .expect("bearer request builds");
        assert_eq!(
            bearer_request.headers().get("authorization").unwrap(),
            "Bearer bearer-secret-value"
        );
        assert!(bearer_request.headers().get("api-key").is_none());
    }

    #[tokio::test]
    async fn send_request_reports_missing_secret_before_url_parsing_or_network_io() {
        let env_var = format!("KTXD_TEST_MISSING_SECRET_{}", uuid::Uuid::new_v4().simple());
        assert!(env::var_os(&env_var).is_none());
        let mut model = model(true, AuthHeaderKind::ApiKey, &env_var);
        model.chat_completions_url = "://invalid-url".to_string();
        let error = ReqwestChatCompletionsClient::default()
            .send_request(&model, &request(None))
            .await
            .expect_err("missing auth must fail before sending");
        assert!(
            matches!(error, ProxyError::Config(ref message) if message == &format!("missing secret environment variable {env_var}"))
        );
        assert!(!error.to_string().contains("invalid-url"));
    }

    #[test]
    fn parse_sse_frame_ignores_done_and_non_data_lines() {
        let payload = serde_json::to_string(&response_chunk()).expect("chunk serializes");
        let frame = format!(": keep-alive\r\nevent: message\r\ndata: {payload}\r\nretry: 1000\r\n");
        let mut chunks = Vec::new();
        parse_sse_frame(&frame, &mut chunks).expect("SSE frame parses");
        parse_sse_frame("data: [DONE]\r\n", &mut chunks).expect("DONE frame parses");
        assert_eq!(chunks, vec![response_chunk()]);
        assert!(chunks[0].choices[0].finish_reason.is_none());
    }

    #[test]
    fn drain_sse_buffer_flushes_unterminated_final_frame() {
        let payload = serde_json::to_string(&response_chunk()).expect("chunk serializes");
        let mut buffer = format!("data: {payload}");
        let mut chunks = Vec::new();
        drain_sse_buffer(&mut buffer, &mut chunks, true).expect("final unterminated frame parses");
        assert!(buffer.is_empty());
        assert_eq!(chunks, vec![response_chunk()]);
    }

    #[test]
    fn parse_sse_frame_returns_malformed_stream_for_invalid_json() {
        let mut chunks = Vec::new();
        let error =
            parse_sse_frame("data: {not-json}", &mut chunks).expect_err("invalid JSON must fail");
        assert!(matches!(error, ProxyError::MalformedStream(_)));
        assert!(chunks.is_empty());
    }
}
