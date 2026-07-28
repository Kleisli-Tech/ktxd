mod support;

use std::{
    env,
    ffi::OsString,
    sync::atomic::{AtomicUsize, Ordering},
};

use ktxd::{
    config::{AuthHeaderKind, ModelConfig},
    error::ProxyError,
    upstream::{ChatCompletions, ReqwestChatCompletionsClient},
    wire::chat::{ChatCompletionRequest, ChatFunctionTool, ChatMessage, ChatTool, StreamOptions},
};
use serde_json::json;
use support::http::{ScriptedHttpServer, ScriptedResponse};
use tokio::sync::{Mutex, MutexGuard};

static NEXT_ENV: AtomicUsize = AtomicUsize::new(0);
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

#[tokio::test]
async fn complete_sends_json_and_configured_auth_headers() {
    let _env_lock = lock_environment().await;
    let response_body = serde_json::to_vec(&json!({"id": "complete-1", "choices": []})).unwrap();
    let mut server = ScriptedHttpServer::spawn(vec![
        ScriptedResponse::new(200, response_body.clone()),
        ScriptedResponse::new(200, response_body),
    ])
    .await
    .unwrap();

    let api_key_var = unique_env("API_KEY");
    let bearer_var = unique_env("BEARER");
    let _api_key = ScopedEnv::set(&api_key_var, "api-secret");
    let _bearer = ScopedEnv::set(&bearer_var, "bearer-secret");
    let api_model = model(&server, &api_key_var, AuthHeaderKind::ApiKey, false);
    let bearer_model = model(
        &server,
        &bearer_var,
        AuthHeaderKind::AuthorizationBearer,
        false,
    );
    let request = request(false, None);
    let client = client();

    let api_response = client.complete(&api_model, request.clone()).await.unwrap();
    let bearer_response = client
        .complete(&bearer_model, request.clone())
        .await
        .unwrap();
    server.finish().await.unwrap();

    assert_eq!(api_response.id.as_deref(), Some("complete-1"));
    assert_eq!(bearer_response.id.as_deref(), Some("complete-1"));
    let requests = server.requests().await;
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].target, "/chat/completions");
    assert_eq!(requests[0].header("api-key"), Some("api-secret"));
    assert_eq!(requests[0].header_values("api-key").count(), 1);
    assert_eq!(requests[0].header("authorization"), None);
    assert_eq!(
        requests[1].header("authorization"),
        Some("Bearer bearer-secret")
    );
    assert_eq!(requests[1].header_values("authorization").count(), 1);
    assert_eq!(requests[1].header("api-key"), None);
    let expected = expected_request_json(false, None);
    assert_eq!(request_json(&requests[0].body), expected);
    assert_eq!(request_json(&requests[1].body), expected);
}

#[tokio::test]
async fn missing_secret_is_checked_before_any_network_request() {
    let _env_lock = lock_environment().await;
    let mut server = ScriptedHttpServer::spawn(Vec::new()).await.unwrap();
    let missing_var = unique_env("MISSING");
    let _missing = ScopedEnv::unset(&missing_var);
    let client = client();

    let error = client
        .complete(
            &model(&server, &missing_var, AuthHeaderKind::ApiKey, false),
            request(false, None),
        )
        .await
        .expect_err("missing secret should fail before sending");
    server.finish().await.unwrap();

    assert!(
        matches!(error, ProxyError::Config(message) if message == format!("missing secret environment variable {missing_var}"))
    );
    assert!(server.requests().await.is_empty());
}

#[tokio::test]
async fn non_success_statuses_map_auth_and_bounded_body_errors() {
    let _env_lock = lock_environment().await;
    let long_body = format!("  first\n second\t{}", "x".repeat(700));
    let mut server = ScriptedHttpServer::spawn(vec![
        ScriptedResponse::new(401, "unauthorized"),
        ScriptedResponse::new(403, "forbidden"),
        ScriptedResponse::new(418, long_body.clone()),
    ])
    .await
    .unwrap();
    let secret_var = unique_env("STATUS");
    let _secret = ScopedEnv::set(&secret_var, "status-secret");
    let client = client();
    let model = model(&server, &secret_var, AuthHeaderKind::ApiKey, false);

    let unauthorized = client
        .complete(&model, request(false, None))
        .await
        .unwrap_err();
    let forbidden = client
        .complete(&model, request(false, None))
        .await
        .unwrap_err();
    let other = client
        .complete(&model, request(false, None))
        .await
        .unwrap_err();
    server.finish().await.unwrap();

    assert!(matches!(unauthorized, ProxyError::UpstreamAuth));
    assert!(matches!(forbidden, ProxyError::UpstreamAuth));
    let ProxyError::Upstream(message) = other else {
        panic!("expected bounded upstream error");
    };
    let collapsed = long_body.split_whitespace().collect::<Vec<_>>().join(" ");
    let expected_summary = collapsed.chars().take(512).collect::<String>();
    assert_eq!(expected_summary.chars().count(), 512);
    assert_eq!(
        message,
        format!("upstream returned status 418 I'm a teapot: {expected_summary}")
    );
    assert!(!expected_summary.contains(['\n', '\r', '\t']));
    assert_eq!(server.requests().await.len(), 3);
}

#[tokio::test]
async fn stream_retries_once_without_stream_options_and_records_bodies() {
    let _env_lock = lock_environment().await;
    let first_error = serde_json::to_vec(&json!({"error": "unsupported stream_options"})).unwrap();
    let stream_body = b"data: {\"id\":\"chunk-1\",\"choices\":[]}\n\ndata: [DONE]\n\n".to_vec();
    let mut server = ScriptedHttpServer::spawn(vec![
        ScriptedResponse::new(400, first_error),
        ScriptedResponse::new(200, stream_body).with_header("content-type", "text/event-stream"),
    ])
    .await
    .unwrap();
    let secret_var = unique_env("RETRY");
    let _secret = ScopedEnv::set(&secret_var, "retry-secret");
    let mut model = model(&server, &secret_var, AuthHeaderKind::ApiKey, true);
    model.include_stream_usage = true;
    let client = client();

    let chunks = client
        .stream(
            &model,
            request(
                true,
                Some(StreamOptions {
                    include_usage: true,
                }),
            ),
        )
        .await
        .unwrap();
    server.finish().await.unwrap();

    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].id.as_deref(), Some("chunk-1"));
    let requests = server.requests().await;
    assert_eq!(requests.len(), 2);
    let first = request_json(&requests[0].body);
    let second = request_json(&requests[1].body);
    let expected_first = expected_request_json(true, Some(true));
    let mut expected_second = expected_first.clone();
    expected_second
        .as_object_mut()
        .unwrap()
        .remove("stream_options");
    assert_eq!(first, expected_first);
    assert_eq!(second, expected_second);
    assert_eq!(requests[0].header("api-key"), Some("retry-secret"));
    assert_eq!(requests[1].header("api-key"), Some("retry-secret"));
}

#[tokio::test]
async fn stream_does_not_retry_when_disabled_missing_options_or_auth_fails() {
    let _env_lock = lock_environment().await;
    let mut server = ScriptedHttpServer::spawn(vec![
        ScriptedResponse::new(400, "unsupported stream_options"),
        ScriptedResponse::new(400, "unsupported stream_options"),
        ScriptedResponse::new(401, "unsupported stream_options"),
        ScriptedResponse::new(403, "unsupported stream_options"),
    ])
    .await
    .unwrap();
    let secret_var = unique_env("NO_RETRY");
    let _secret = ScopedEnv::set(&secret_var, "no-retry-secret");
    let client = client();
    let enabled = model(&server, &secret_var, AuthHeaderKind::ApiKey, true);
    let disabled = model(&server, &secret_var, AuthHeaderKind::ApiKey, false);

    let disabled_error = client
        .stream(
            &disabled,
            request(
                true,
                Some(StreamOptions {
                    include_usage: true,
                }),
            ),
        )
        .await
        .unwrap_err();
    let absent_error = client
        .stream(&enabled, request(true, None))
        .await
        .unwrap_err();
    let unauthorized = client
        .stream(
            &enabled,
            request(
                true,
                Some(StreamOptions {
                    include_usage: true,
                }),
            ),
        )
        .await
        .unwrap_err();
    let forbidden = client
        .stream(
            &enabled,
            request(
                true,
                Some(StreamOptions {
                    include_usage: true,
                }),
            ),
        )
        .await
        .unwrap_err();
    server.finish().await.unwrap();

    let expected_error = "upstream returned status 400 Bad Request: unsupported stream_options";
    assert!(matches!(disabled_error, ProxyError::Upstream(message) if message == expected_error));
    assert!(matches!(absent_error, ProxyError::Upstream(message) if message == expected_error));
    assert!(matches!(unauthorized, ProxyError::UpstreamAuth));
    assert!(matches!(forbidden, ProxyError::UpstreamAuth));
    let requests = server.requests().await;
    assert_eq!(requests.len(), 4);
    assert_eq!(
        request_json(&requests[0].body),
        expected_request_json(true, Some(true))
    );
    assert_eq!(
        request_json(&requests[1].body),
        expected_request_json(true, None)
    );
    assert_eq!(
        request_json(&requests[2].body),
        expected_request_json(true, Some(true))
    );
    assert_eq!(
        request_json(&requests[3].body),
        expected_request_json(true, Some(true))
    );
}

#[tokio::test]
async fn stream_parses_chunked_sse_with_crlf_done_comments_and_unterminated_frame() {
    let _env_lock = lock_environment().await;
    let first = json!({"id": "chunk-1", "choices": []});
    let second = json!({"id": "chunk-2", "choices": []});
    let body = format!(
        "event: first\r\ndata: {}\r\n\r\n: keep-alive\n\ndata: [DONE]\n\nnon-data: ignored\n\ndata: {}",
        first, second
    );
    let split_at = body.len() / 3;
    let split_at_two = split_at * 2;
    let mut server = ScriptedHttpServer::spawn(vec![
        ScriptedResponse::new(200, Vec::new())
            .with_header("content-type", "text/event-stream")
            .with_chunks(vec![
                body.as_bytes()[..split_at].to_vec(),
                body.as_bytes()[split_at..split_at_two].to_vec(),
                body.as_bytes()[split_at_two..].to_vec(),
            ]),
    ])
    .await
    .unwrap();
    let secret_var = unique_env("SSE");
    let _secret = ScopedEnv::set(&secret_var, "sse-secret");
    let client = client();

    let chunks = client
        .stream(
            &model(&server, &secret_var, AuthHeaderKind::ApiKey, false),
            request(true, None),
        )
        .await
        .unwrap();
    server.finish().await.unwrap();

    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].id.as_deref(), Some("chunk-1"));
    assert_eq!(chunks[1].id.as_deref(), Some("chunk-2"));
}

#[tokio::test]
async fn malformed_sse_json_returns_malformed_stream_error() {
    let _env_lock = lock_environment().await;
    let mut server = ScriptedHttpServer::spawn(vec![
        ScriptedResponse::new(200, b"event: chunk\r\ndata: {not-json}\r\n\r\n".to_vec())
            .with_header("content-type", "text/event-stream"),
    ])
    .await
    .unwrap();
    let secret_var = unique_env("MALFORMED_SSE");
    let _secret = ScopedEnv::set(&secret_var, "malformed-secret");
    let client = client();

    let error = client
        .stream(
            &model(&server, &secret_var, AuthHeaderKind::ApiKey, false),
            request(true, None),
        )
        .await
        .unwrap_err();
    server.finish().await.unwrap();

    assert!(matches!(error, ProxyError::MalformedStream(message) if !message.is_empty()));
}

fn model(
    server: &ScriptedHttpServer,
    auth_env_var: &str,
    auth_header: AuthHeaderKind,
    retry: bool,
) -> ModelConfig {
    let mut model = ModelConfig::default_deepseek();
    model.chat_completions_url = server.url("/chat/completions");
    model.auth_env_var = auth_env_var.to_string();
    model.auth_header = auth_header;
    model.retry_without_stream_options_on_4xx = retry;
    model
}

fn request(stream: bool, stream_options: Option<StreamOptions>) -> ChatCompletionRequest {
    ChatCompletionRequest {
        model: Some("request-model".to_string()),
        messages: vec![ChatMessage {
            role: "user".to_string(),
            content: Some("hello".to_string()),
            tool_call_id: None,
            tool_calls: None,
        }],
        tools: vec![ChatTool {
            tool_type: "function".to_string(),
            function: ChatFunctionTool {
                name: "lookup".to_string(),
                description: Some("Lookup data".to_string()),
                parameters: json!({"type": "object"}),
            },
        }],
        tool_choice: Some(json!("auto")),
        parallel_tool_calls: Some(true),
        stream,
        stream_options,
    }
}

fn expected_request_json(stream: bool, include_usage: Option<bool>) -> serde_json::Value {
    let mut expected = json!({
        "model": "request-model",
        "messages": [{"role": "user", "content": "hello"}],
        "tools": [{
            "type": "function",
            "function": {
                "name": "lookup",
                "description": "Lookup data",
                "parameters": {"type": "object"}
            }
        }],
        "tool_choice": "auto",
        "parallel_tool_calls": true,
        "stream": stream
    });
    if let Some(include_usage) = include_usage {
        expected["stream_options"] = json!({"include_usage": include_usage});
    }
    expected
}

fn request_json(body: &[u8]) -> serde_json::Value {
    serde_json::from_slice(body).expect("recorded request body is valid JSON")
}

fn client() -> ReqwestChatCompletionsClient {
    ReqwestChatCompletionsClient::new(
        reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("test HTTP client builds"),
    )
}

async fn lock_environment() -> MutexGuard<'static, ()> {
    ENV_LOCK.lock().await
}

struct ScopedEnv {
    key: String,
    previous: Option<OsString>,
}

impl ScopedEnv {
    fn set(key: &str, value: &str) -> Self {
        let guard = Self {
            key: key.to_string(),
            previous: env::var_os(key),
        };
        unsafe { env::set_var(key, value) };
        guard
    }

    fn unset(key: &str) -> Self {
        let guard = Self {
            key: key.to_string(),
            previous: env::var_os(key),
        };
        unsafe { env::remove_var(key) };
        guard
    }
}

impl Drop for ScopedEnv {
    fn drop(&mut self) {
        if let Some(previous) = &self.previous {
            unsafe { env::set_var(&self.key, previous) };
        } else {
            unsafe { env::remove_var(&self.key) };
        }
    }
}

fn unique_env(prefix: &str) -> String {
    let number = NEXT_ENV.fetch_add(1, Ordering::Relaxed);
    format!("KTXD_UPSTREAM_{prefix}_{}_{}", std::process::id(), number)
}
