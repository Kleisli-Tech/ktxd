mod support;

use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode},
    response::Response,
};
use ktxd::app_state::AppState;
use ktxd::config::AppConfig;
use ktxd::driver::TurnDriver;
use ktxd::error::ProxyError;
use ktxd::responses::router;
use ktxd::session::MemoryStore;
use ktxd::substrate::{NullSeedResolver, NullSink};
use ktxd::wire::chat::{
    ChatChoice, ChatCompletionResponse, ChatDelta, ChatFunctionCall, ChatFunctionCallDelta,
    ChatResponseMessage, ChatToolCall, ChatToolCallDelta, ChatUsage,
};
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;

struct TestApp {
    router: Router,
    upstream: support::FakeChatCompletions,
    store: Arc<MemoryStore>,
}

fn test_app() -> TestApp {
    test_app_with_config(AppConfig::default())
}

fn test_app_with_config(config: AppConfig) -> TestApp {
    let config = Arc::new(config);
    let store = MemoryStore::shared();
    let upstream = support::FakeChatCompletions::new();
    let driver = Arc::new(TurnDriver::new(
        Arc::clone(&config),
        Arc::new(upstream.clone()),
        Arc::clone(&store),
        Arc::new(NullSink),
        Arc::new(NullSeedResolver),
    ));
    let router = router(AppState {
        config,
        store: Arc::clone(&store),
        driver,
    });
    TestApp {
        router,
        upstream,
        store,
    }
}

async fn send(router: Router, method: Method, uri: &str, body: Option<Value>) -> Response {
    let request = Request::builder().method(method).uri(uri);
    let request = match body {
        Some(body) => request
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&body).expect("request serializes"),
            )),
        None => request.body(Body::empty()),
    }
    .expect("request builds");
    router.oneshot(request).await.expect("router responds")
}

async fn send_malformed_json(router: Router, method: Method, uri: &str) -> Response {
    send_raw(router, method, uri, "application/json", "{").await
}

async fn send_raw(
    router: Router,
    method: Method,
    uri: &str,
    content_type: &str,
    body: &str,
) -> Response {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", content_type)
        .body(Body::from(body.to_string()))
        .expect("request builds");
    router.oneshot(request).await.expect("router responds")
}

async fn json_body(response: Response) -> (StatusCode, String, Value) {
    let status = response.status();
    let content_type = response
        .headers()
        .get("content-type")
        .expect("response content type")
        .to_str()
        .expect("content type is valid UTF-8")
        .to_string();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body collects");
    let value = serde_json::from_slice(&body).expect("response body is JSON");
    (status, content_type, value)
}

async fn text_body(response: Response) -> (StatusCode, String, String) {
    let status = response.status();
    let content_type = response
        .headers()
        .get("content-type")
        .expect("response content type")
        .to_str()
        .expect("content type is valid UTF-8")
        .to_string();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body collects");
    (
        status,
        content_type,
        String::from_utf8(body.to_vec()).expect("response body is UTF-8"),
    )
}

async fn assert_proxy_error(response: Response, status: StatusCode, code: &str, message: &str) {
    let (actual_status, content_type, body) = json_body(response).await;
    assert_eq!(actual_status, status);
    assert!(content_type.starts_with("application/json"));
    assert_eq!(
        body,
        json!({
            "error": {
                "message": message,
                "type": "invalid_request_error",
                "code": code
            }
        })
    );
}

fn assert_sse_frames_terminated(frames: &[support::SseFrame]) {
    assert!(!frames.is_empty());
    assert!(frames.iter().all(|frame| frame.terminated));
    for frame in frames {
        let payload = frame.payload.as_ref().expect("SSE JSON payload");
        assert_eq!(frame.event.as_deref(), payload["type"].as_str());
    }
}

fn sequence_numbers(frames: &[support::SseFrame]) -> Vec<u64> {
    frames
        .iter()
        .map(|frame| {
            frame.payload.as_ref().expect("SSE JSON payload")["sequence_number"]
                .as_u64()
                .expect("SSE sequence number")
        })
        .collect()
}

fn event_response_id(payload: &Value) -> &str {
    payload
        .get("response_id")
        .and_then(Value::as_str)
        .or_else(|| {
            payload
                .get("response")
                .and_then(|response| response["id"].as_str())
        })
        .expect("event response ID")
}

fn without_created_at(mut value: Value) -> Value {
    if let Some(object) = value.as_object_mut() {
        object.remove("created_at");
    }
    value
}

fn text_response(content: &str, usage: (u64, u64, u64)) -> ChatCompletionResponse {
    ChatCompletionResponse {
        id: Some("chatcmpl_test".to_string()),
        choices: vec![ChatChoice {
            index: Some(0),
            message: Some(ChatResponseMessage {
                role: Some("assistant".to_string()),
                content: Some(content.to_string()),
                tool_calls: Vec::new(),
            }),
            delta: None,
            finish_reason: Some("stop".to_string()),
        }],
        usage: Some(ChatUsage {
            prompt_tokens: Some(usage.0),
            completion_tokens: Some(usage.1),
            total_tokens: Some(usage.2),
        }),
    }
}

fn tool_response() -> ChatCompletionResponse {
    ChatCompletionResponse {
        id: Some("chatcmpl_tool".to_string()),
        choices: vec![ChatChoice {
            index: Some(0),
            message: Some(ChatResponseMessage {
                role: Some("assistant".to_string()),
                content: None,
                tool_calls: vec![ChatToolCall {
                    index: Some(0),
                    id: "call_weather".to_string(),
                    tool_type: "function".to_string(),
                    function: ChatFunctionCall {
                        name: "get_weather".to_string(),
                        arguments: r#"{"city":"Dubai"}"#.to_string(),
                    },
                }],
            }),
            delta: None,
            finish_reason: Some("tool_calls".to_string()),
        }],
        usage: Some(ChatUsage {
            prompt_tokens: Some(4),
            completion_tokens: Some(6),
            total_tokens: Some(10),
        }),
    }
}

fn text_stream() -> Vec<ChatCompletionResponse> {
    vec![
        ChatCompletionResponse {
            id: None,
            choices: vec![ChatChoice {
                index: Some(0),
                message: None,
                delta: Some(ChatDelta {
                    role: None,
                    content: Some("hel".to_string()),
                    tool_calls: Vec::new(),
                }),
                finish_reason: None,
            }],
            usage: None,
        },
        ChatCompletionResponse {
            id: None,
            choices: vec![ChatChoice {
                index: Some(0),
                message: None,
                delta: Some(ChatDelta {
                    role: None,
                    content: Some("lo".to_string()),
                    tool_calls: Vec::new(),
                }),
                finish_reason: Some("stop".to_string()),
            }],
            usage: Some(ChatUsage {
                prompt_tokens: Some(3),
                completion_tokens: Some(2),
                total_tokens: Some(5),
            }),
        },
    ]
}

fn tool_stream() -> Vec<ChatCompletionResponse> {
    vec![
        ChatCompletionResponse {
            id: None,
            choices: vec![ChatChoice {
                index: Some(0),
                message: None,
                delta: Some(ChatDelta {
                    role: None,
                    content: None,
                    tool_calls: vec![ChatToolCallDelta {
                        index: Some(0),
                        id: Some("call_stream".to_string()),
                        tool_type: Some("function".to_string()),
                        function: Some(ChatFunctionCallDelta {
                            name: Some("lookup".to_string()),
                            arguments: Some(r#"{"q":"a""#.to_string()),
                        }),
                    }],
                }),
                finish_reason: None,
            }],
            usage: None,
        },
        ChatCompletionResponse {
            id: None,
            choices: vec![ChatChoice {
                index: Some(0),
                message: None,
                delta: Some(ChatDelta {
                    role: None,
                    content: None,
                    tool_calls: vec![ChatToolCallDelta {
                        index: Some(0),
                        id: None,
                        tool_type: None,
                        function: Some(ChatFunctionCallDelta {
                            name: None,
                            arguments: Some("}".to_string()),
                        }),
                    }],
                }),
                finish_reason: Some("tool_calls".to_string()),
            }],
            usage: Some(ChatUsage {
                prompt_tokens: Some(2),
                completion_tokens: Some(4),
                total_tokens: Some(6),
            }),
        },
    ]
}

#[tokio::test]
async fn public_health_models_retrieval_and_error_routes_are_stable() {
    let mut config = AppConfig::default();
    let mut second_model = config
        .models
        .get("DeepSeek-V4-Pro")
        .expect("default model")
        .clone();
    second_model.public_model = "Second-Test-Model".to_string();
    second_model.display_name = "Second Test Model".to_string();
    second_model.description = "A second deterministic model".to_string();
    second_model.context_window = 12_345;
    config
        .models
        .insert(second_model.public_model.clone(), second_model);
    let app = test_app_with_config(config);

    let response = send(app.router.clone(), Method::GET, "/healthz", None).await;
    let (status, content_type, body) = json_body(response).await;
    assert_eq!(status, StatusCode::OK);
    assert!(content_type.starts_with("application/json"));
    assert_eq!(body, json!({"status": "ok"}));

    let response = send(app.router.clone(), Method::GET, "/v1/models", None).await;
    let (status, content_type, body) = json_body(response).await;
    assert_eq!(status, StatusCode::OK);
    assert!(content_type.starts_with("application/json"));
    let models = body["models"].as_array().expect("models array");
    assert_eq!(models.len(), 2);
    assert_eq!(models[0]["slug"], "DeepSeek-V4-Pro");
    assert_eq!(models[0]["display_name"], "DeepSeek V4 Pro");
    assert_eq!(
        models[0]["description"],
        "DeepSeek-V4-Pro via Azure AI Foundry Chat Completions"
    );
    assert_eq!(models[0]["shell_type"], "shell_command");
    assert_eq!(models[0]["visibility"], "list");
    assert_eq!(models[0]["supported_in_api"], true);
    assert_eq!(models[0]["priority"], 0);
    assert_eq!(models[0]["base_instructions"], "");
    assert_eq!(models[0]["supports_reasoning_summaries"], false);
    assert_eq!(models[0]["default_reasoning_summary"], "none");
    assert_eq!(models[0]["support_verbosity"], false);
    assert_eq!(models[0]["apply_patch_tool_type"], "function");
    assert_eq!(models[0]["web_search_tool_type"], "text");
    assert_eq!(
        models[0]["truncation_policy"],
        json!({"mode": "tokens", "limit": 1_000_000})
    );
    assert_eq!(models[0]["supports_parallel_tool_calls"], true);
    assert_eq!(models[0]["supports_image_detail_original"], false);
    assert_eq!(models[0]["context_window"], 1_000_000);
    assert_eq!(models[0]["effective_context_window_percent"], 90);
    assert_eq!(models[0]["input_modalities"], json!(["text"]));
    assert_eq!(models[0]["supports_search_tool"], false);
    assert_eq!(models[1]["slug"], "Second-Test-Model");
    assert_eq!(models[1]["display_name"], "Second Test Model");
    assert_eq!(models[1]["description"], "A second deterministic model");
    assert_eq!(models[1]["context_window"], 12_345);
    assert_eq!(app.upstream.request_count().await, 0);

    assert_proxy_error(
        send(
            app.router.clone(),
            Method::GET,
            "/v1/responses/resp_missing",
            None,
        )
        .await,
        StatusCode::NOT_FOUND,
        "previous_response_not_found",
        "unknown previous_response_id: resp_missing",
    )
    .await;
    assert_proxy_error(
        send(
            app.router.clone(),
            Method::POST,
            "/v1/responses",
            Some(json!({"model": "missing", "input": "hello"})),
        )
        .await,
        StatusCode::BAD_REQUEST,
        "unknown_model",
        "unknown model: missing",
    )
    .await;
    assert_proxy_error(
        send(
            app.router.clone(),
            Method::POST,
            "/v1/responses",
            Some(json!({
                "model": "DeepSeek-V4-Pro",
                "input": [{"type": "unsupported_item"}]
            })),
        )
        .await,
        StatusCode::BAD_REQUEST,
        "unsupported_input_item",
        "unsupported input item: unsupported_item",
    )
    .await;
    assert_proxy_error(
        send(
            app.router.clone(),
            Method::POST,
            "/v1/responses",
            Some(json!({
                "model": "DeepSeek-V4-Pro",
                "input": "hello",
                "tools": [{"type": "web_search"}]
            })),
        )
        .await,
        StatusCode::BAD_REQUEST,
        "unsupported_tool",
        "unsupported tool: web_search",
    )
    .await;

    let response = send_raw(
        app.router.clone(),
        Method::POST,
        "/v1/responses",
        "text/plain",
        "{}",
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);

    let response = send_malformed_json(app.router.clone(), Method::POST, "/v1/responses").await;
    let (status, content_type, body) = text_body(response).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(content_type.starts_with("text/plain"));
    assert!(body.contains("JSON"));

    assert_eq!(
        send(app.router.clone(), Method::GET, "/v1/responses", None)
            .await
            .status(),
        StatusCode::METHOD_NOT_ALLOWED
    );
    assert_eq!(
        send(app.router, Method::GET, "/v1/not-a-route", None)
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(app.upstream.request_count().await, 0);
}

#[tokio::test]
async fn non_streaming_text_is_persisted_and_parent_transcript_is_forwarded() {
    let app = test_app();
    app.upstream
        .queue_complete(text_response("first answer", (3, 4, 7)))
        .await;
    app.upstream
        .queue_complete(text_response("second answer", (8, 4, 12)))
        .await;

    let first_request = json!({
        "model": "DeepSeek-V4-Pro",
        "input": "first question"
    });
    let response = send(
        app.router.clone(),
        Method::POST,
        "/v1/responses",
        Some(first_request),
    )
    .await;
    let (status, content_type, first) = json_body(response).await;
    assert_eq!(status, StatusCode::OK);
    assert!(content_type.starts_with("application/json"));
    assert_eq!(first["object"], "response");
    assert_eq!(first["status"], "completed");
    assert_eq!(first["model"], "DeepSeek-V4-Pro");
    assert_eq!(
        first["usage"],
        json!({"input_tokens": 3, "output_tokens": 4, "total_tokens": 7})
    );
    assert_eq!(first["output"][0]["content"][0]["text"], "first answer");
    let first_id = first["id"].as_str().expect("first response ID").to_string();
    support::assert_generated_id(&first_id, "resp_");

    let response = send(
        app.router.clone(),
        Method::GET,
        &format!("/v1/responses/{first_id}"),
        None,
    )
    .await;
    let (status, _, retrieved) = json_body(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(retrieved, first);

    let response = send(
        app.router.clone(),
        Method::POST,
        "/v1/responses",
        Some(json!({
            "model": "DeepSeek-V4-Pro",
            "input": "second question",
            "previous_response_id": first_id
        })),
    )
    .await;
    let (status, _, second) = json_body(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(second["status"], "completed");

    let requests = app.upstream.complete_requests().await;
    assert_eq!(requests.len(), 2);
    let messages = &requests[1].messages;
    assert_eq!(
        messages
            .iter()
            .map(|message| message.role.as_str())
            .collect::<Vec<_>>(),
        vec!["user", "assistant", "user"]
    );
    assert_eq!(messages[0].content.as_deref(), Some("first question"));
    assert_eq!(messages[1].content.as_deref(), Some("first answer"));
    assert_eq!(messages[2].content.as_deref(), Some("second question"));

    let upstream_count = app.upstream.request_count().await;
    assert_proxy_error(
        send(
            app.router,
            Method::POST,
            "/v1/responses",
            Some(json!({
                "model": "DeepSeek-V4-Pro",
                "input": "orphan",
                "previous_response_id": "resp_unknown"
            })),
        )
        .await,
        StatusCode::NOT_FOUND,
        "previous_response_not_found",
        "unknown previous_response_id: resp_unknown",
    )
    .await;
    assert_eq!(app.upstream.request_count().await, upstream_count);
}

#[tokio::test]
async fn non_streaming_tool_calls_and_terminal_payloads_are_retrievable() {
    let app = test_app();
    app.upstream.queue_complete(tool_response()).await;
    let response = send(
        app.router.clone(),
        Method::POST,
        "/v1/responses",
        Some(json!({
            "model": "DeepSeek-V4-Pro",
            "input": "call the weather tool",
            "tools": [{
                "type": "function",
                "name": "get_weather",
                "description": "Get weather",
                "parameters": {"type": "object"}
            }]
        })),
    )
    .await;
    let (status, content_type, body) = json_body(response).await;
    assert_eq!(status, StatusCode::OK);
    assert!(content_type.starts_with("application/json"));
    assert_eq!(body["object"], "response");
    assert_eq!(body["model"], "DeepSeek-V4-Pro");
    assert_eq!(body["status"], "completed");
    assert_eq!(body["output"].as_array().expect("tool output").len(), 1);
    assert_eq!(
        body["usage"],
        json!({"input_tokens": 4, "output_tokens": 6, "total_tokens": 10})
    );
    assert_eq!(body["output"][0]["type"], "function_call");
    support::assert_generated_id(
        body["output"][0]["id"].as_str().expect("tool item ID"),
        "item_",
    );
    assert_eq!(body["output"][0]["call_id"], "call_weather");
    assert_eq!(body["output"][0]["name"], "get_weather");
    assert_eq!(body["output"][0]["arguments"], r#"{"city":"Dubai"}"#);
    let response_id = body["id"].as_str().expect("tool response ID").to_string();
    support::assert_generated_id(&response_id, "resp_");
    let response = send(
        app.router.clone(),
        Method::GET,
        &format!("/v1/responses/{response_id}"),
        None,
    )
    .await;
    let (status, _, retrieved) = json_body(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(retrieved, body);

    let incomplete_id = ktxd::ids::ResponseId::from_string("resp_incomplete");
    let failed_id = ktxd::ids::ResponseId::from_string("resp_failed");
    let incomplete_response = json!({
        "id": incomplete_id,
        "object": "response",
        "created_at": 1,
        "model": "DeepSeek-V4-Pro",
        "status": "incomplete",
        "output": [{"type": "message", "content": [{"type": "output_text", "text": "partial"}]}],
        "usage": {"input_tokens": 1, "output_tokens": 2, "total_tokens": 3},
        "incomplete_details": {"reason": "max_output_tokens"}
    });
    let failed_response = json!({
        "id": failed_id,
        "object": "response",
        "created_at": 1,
        "model": "DeepSeek-V4-Pro",
        "status": "failed",
        "output": [],
        "error": {"code": "upstream_error", "message": "upstream down"}
    });
    app.store
        .put_response_json(incomplete_id, incomplete_response.clone())
        .await
        .expect("store incomplete response");
    app.store
        .put_response_json(failed_id, failed_response.clone())
        .await
        .expect("store failed response");

    for (id, expected) in [
        ("resp_incomplete", incomplete_response),
        ("resp_failed", failed_response),
    ] {
        let response = send(
            app.router.clone(),
            Method::GET,
            &format!("/v1/responses/{id}"),
            None,
        )
        .await;
        let (status, _, body) = json_body(response).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, expected);
    }
}

#[tokio::test]
async fn streaming_text_and_tool_calls_emit_framed_ordered_events() {
    let app = test_app();
    app.upstream.queue_stream(text_stream()).await;
    app.upstream.queue_stream(tool_stream()).await;

    let response = send(
        app.router.clone(),
        Method::POST,
        "/v1/responses",
        Some(json!({"model": "DeepSeek-V4-Pro", "input": "stream text", "stream": true})),
    )
    .await;
    let collected = support::collect_sse_response(response)
        .await
        .expect("text SSE parses");
    assert_eq!(collected.status, StatusCode::OK);
    assert!(
        collected.headers["content-type"]
            .to_str()
            .expect("SSE content type")
            .starts_with("text/event-stream")
    );
    assert!(collected.terminal_blank_line);
    let frames = collected.frames;
    assert_sse_frames_terminated(&frames);
    assert_eq!(
        frames
            .iter()
            .map(|frame| frame.event.as_deref())
            .collect::<Vec<_>>(),
        vec![
            Some("response.created"),
            Some("response.output_item.added"),
            Some("response.output_text.delta"),
            Some("response.output_text.delta"),
            Some("response.output_item.done"),
            Some("response.completed"),
        ]
    );
    assert_eq!(sequence_numbers(&frames), (0..6).collect::<Vec<_>>());
    let response_id = frames[0].payload.as_ref().expect("created payload")["response"]["id"]
        .as_str()
        .expect("created response ID")
        .to_string();
    support::assert_generated_id(&response_id, "resp_");
    for frame in &frames {
        assert_eq!(
            event_response_id(frame.payload.as_ref().expect("SSE payload")),
            response_id
        );
    }
    let created = &frames[0].payload.as_ref().expect("created payload")["response"];
    assert_eq!(created["object"], "response");
    assert_eq!(created["status"], "in_progress");
    assert_eq!(created["model"], "DeepSeek-V4-Pro");
    assert_eq!(created["output"], json!([]));
    let added = &frames[1].payload.as_ref().expect("added payload")["item"];
    let item_id = added["id"].as_str().expect("text item ID").to_string();
    support::assert_generated_id(&item_id, "item_");
    assert_eq!(added["type"], "message");
    assert_eq!(added["role"], "assistant");
    assert_eq!(
        added["content"],
        json!([{"type": "output_text", "text": ""}])
    );
    assert_eq!(frames[1].payload.as_ref().unwrap()["output_index"], 0);
    assert_eq!(frames[2].payload.as_ref().unwrap()["item_id"], item_id);
    assert_eq!(frames[2].payload.as_ref().unwrap()["output_index"], 0);
    assert_eq!(frames[2].payload.as_ref().unwrap()["content_index"], 0);
    assert_eq!(frames[2].payload.as_ref().unwrap()["delta"], "hel");
    assert_eq!(frames[3].payload.as_ref().unwrap()["item_id"], item_id);
    assert_eq!(frames[3].payload.as_ref().unwrap()["output_index"], 0);
    assert_eq!(frames[3].payload.as_ref().unwrap()["content_index"], 0);
    assert_eq!(frames[3].payload.as_ref().unwrap()["delta"], "lo");
    let done = &frames[4].payload.as_ref().expect("done payload")["item"];
    assert_eq!(done["id"], item_id);
    assert_eq!(done["type"], "message");
    assert_eq!(
        done["content"],
        json!([{"type": "output_text", "text": "hello"}])
    );
    assert_eq!(frames[4].payload.as_ref().unwrap()["output_index"], 0);
    let completed = &frames[5].payload.as_ref().expect("completed payload")["response"];
    assert_eq!(completed["status"], "completed");
    assert_eq!(completed["output"][0], *done);
    assert_eq!(
        completed["usage"],
        json!({"input_tokens": 3, "output_tokens": 2, "total_tokens": 5})
    );

    let response = send(
        app.router.clone(),
        Method::GET,
        &format!("/v1/responses/{response_id}"),
        None,
    )
    .await;
    let (status, _, retrieved) = json_body(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(retrieved["id"], response_id);
    assert_eq!(retrieved["status"], "completed");
    assert_eq!(retrieved["output"], completed["output"]);
    assert_eq!(retrieved["usage"], completed["usage"]);

    let response = send(
        app.router.clone(),
        Method::POST,
        "/v1/responses",
        Some(json!({"model": "DeepSeek-V4-Pro", "input": "stream tool", "stream": true})),
    )
    .await;
    let collected = support::collect_sse_response(response)
        .await
        .expect("tool SSE parses");
    assert_eq!(collected.status, StatusCode::OK);
    assert!(
        collected.headers["content-type"]
            .to_str()
            .expect("SSE content type")
            .starts_with("text/event-stream")
    );
    assert!(collected.terminal_blank_line);
    let frames = collected.frames;
    assert_sse_frames_terminated(&frames);
    assert_eq!(
        frames
            .iter()
            .map(|frame| frame.event.as_deref())
            .collect::<Vec<_>>(),
        vec![
            Some("response.created"),
            Some("response.output_item.added"),
            Some("response.output_item.done"),
            Some("response.completed"),
        ]
    );
    assert_eq!(sequence_numbers(&frames), (0..4).collect::<Vec<_>>());
    let response_id = frames[0].payload.as_ref().expect("created payload")["response"]["id"]
        .as_str()
        .expect("created response ID")
        .to_string();
    for frame in &frames {
        assert_eq!(
            event_response_id(frame.payload.as_ref().expect("SSE payload")),
            response_id
        );
    }
    let added = &frames[1].payload.as_ref().expect("added payload")["item"];
    let done = &frames[2].payload.as_ref().expect("done payload")["item"];
    let item_id = added["id"].as_str().expect("tool item ID");
    support::assert_generated_id(item_id, "item_");
    assert_eq!(done["id"], item_id);
    assert_eq!(frames[1].payload.as_ref().unwrap()["output_index"], 0);
    assert_eq!(frames[2].payload.as_ref().unwrap()["output_index"], 0);
    assert_eq!(added["type"], "function_call");
    assert_eq!(added["call_id"], "call_stream");
    assert_eq!(added["name"], "lookup");
    assert_eq!(added["arguments"], r#"{"q":"a"}"#);
    assert_eq!(done, added);
    let completed = &frames[3].payload.as_ref().expect("completed payload")["response"];
    assert_eq!(completed["status"], "completed");
    assert_eq!(completed["output"][0], *done);
    assert_eq!(
        completed["usage"],
        json!({"input_tokens": 2, "output_tokens": 4, "total_tokens": 6})
    );

    let response = send(
        app.router,
        Method::GET,
        &format!("/v1/responses/{response_id}"),
        None,
    )
    .await;
    let (status, _, retrieved) = json_body(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        without_created_at(retrieved),
        without_created_at(completed.clone())
    );
}

#[tokio::test]
async fn streaming_upstream_failure_is_a_parseable_failed_event_and_is_stored() {
    let app = test_app();
    app.upstream
        .queue_stream_error(ProxyError::Upstream("service unavailable".to_string()))
        .await;
    let response = send(
        app.router.clone(),
        Method::POST,
        "/v1/responses",
        Some(json!({"model": "DeepSeek-V4-Pro", "input": "fail", "stream": true})),
    )
    .await;
    let collected = support::collect_sse_response(response)
        .await
        .expect("failure SSE parses");
    assert_eq!(collected.status, StatusCode::OK);
    assert!(
        collected.headers["content-type"]
            .to_str()
            .expect("SSE content type")
            .starts_with("text/event-stream")
    );
    assert!(collected.terminal_blank_line);
    let frames = collected.frames;
    assert_sse_frames_terminated(&frames);
    assert_eq!(
        frames
            .iter()
            .map(|frame| frame.event.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("response.created"), Some("response.failed"),]
    );
    assert_eq!(sequence_numbers(&frames), vec![0, 1]);
    let response_id = frames[0].payload.as_ref().unwrap()["response"]["id"]
        .as_str()
        .expect("created response ID")
        .to_string();
    support::assert_generated_id(&response_id, "resp_");
    for frame in &frames {
        assert_eq!(
            event_response_id(frame.payload.as_ref().expect("SSE payload")),
            response_id
        );
    }
    let created = &frames[0].payload.as_ref().expect("created payload")["response"];
    assert_eq!(created["object"], "response");
    assert_eq!(created["status"], "in_progress");
    assert_eq!(created["model"], "DeepSeek-V4-Pro");
    let failed = &frames[1].payload.as_ref().expect("failed payload")["response"];
    assert_eq!(failed["object"], "response");
    assert_eq!(failed["status"], "failed");
    assert_eq!(failed["error"]["code"], "upstream_error");
    assert_eq!(
        failed["error"]["message"],
        "upstream request failed: service unavailable"
    );

    let response = send(
        app.router,
        Method::GET,
        &format!("/v1/responses/{response_id}"),
        None,
    )
    .await;
    let (status, _, body) = json_body(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        without_created_at(body),
        without_created_at(json!({
            "id": response_id,
            "object": "response",
            "created_at": 0,
            "model": "DeepSeek-V4-Pro",
            "status": "failed",
            "output": [],
            "usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
            "error": failed["error"].clone()
        }))
    );
}
