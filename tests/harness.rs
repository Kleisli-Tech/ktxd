mod support;

use axum::body::Body;
use axum::response::Response;
use ktxd::config::AppConfig;
use ktxd::domain::{Session, TurnOutcome, TurnRecord, UsageTotals};
use ktxd::ids::{ResponseId, SessionVersion, TenantId, TurnId};
use ktxd::responses::ResponseEvent;
use ktxd::substrate::{NodeSink, SeedResolver};
use ktxd::upstream::ChatCompletions;
use ktxd::wire::chat::ChatCompletionRequest;
use ktxd::wire::chat::ChatCompletionResponse;
use serde_json::json;

#[tokio::test]
async fn shared_harness_support_is_hermetic_and_compiles() {
    let upstream = support::FakeChatCompletions::new();
    upstream
        .queue_complete(ChatCompletionResponse {
            id: Some("chatcmpl-test".to_string()),
            choices: Vec::new(),
            usage: None,
        })
        .await;
    let model = AppConfig::default()
        .model("DeepSeek-V4-Pro")
        .expect("default model exists")
        .clone();
    let request = ChatCompletionRequest {
        model: Some("test-model".to_string()),
        messages: Vec::new(),
        tools: Vec::new(),
        tool_choice: None,
        parallel_tool_calls: None,
        stream: false,
        stream_options: None,
    };
    let response = upstream
        .complete(&model, request.clone())
        .await
        .expect("queued fake response");
    assert_eq!(response.id.as_deref(), Some("chatcmpl-test"));
    let recorded = upstream.requests().await;
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].request, request);
    assert_eq!(recorded[0].serialized_request["stream"], false);
    assert!(matches!(
        recorded[0].result,
        support::RecordedResult::Complete(_)
    ));

    let body = b"event: response.created\ndata: {\"type\":\"response.created\"}\n\n";
    let frames = support::parse_sse_frames(body).expect("valid SSE fixture");
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].event.as_deref(), Some("response.created"));
    assert_eq!(frames[0].payload, Some(json!({"type": "response.created"})));
    assert!(frames[0].terminated);
}

#[test]
fn sse_support_preserves_order_and_terminal_framing() {
    let body = concat!(
        ": keep-alive\r\n",
        "event: first\r\n",
        "data: {\"value\":\r\n",
        "data: 1}\r\n\r\n",
        "event: done\r\n",
        "data: [DONE]"
    );
    let frames = support::parse_sse_frames(body.as_bytes()).expect("valid SSE fixture");

    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].event.as_deref(), Some("first"));
    assert_eq!(frames[0].payload, Some(json!({"value": 1})));
    assert!(frames[0].terminated);
    assert_eq!(frames[1].event.as_deref(), Some("done"));
    assert!(frames[1].is_done());
    assert!(!frames[1].terminated);
}

#[tokio::test]
async fn fake_upstream_keeps_atomic_request_result_pairs_and_error_sources() {
    let upstream = support::FakeChatCompletions::new();
    let model = default_model();
    upstream
        .queue_complete(ChatCompletionResponse {
            id: Some("complete-a".to_string()),
            choices: Vec::new(),
            usage: None,
        })
        .await;
    upstream
        .queue_complete_error(ktxd::error::ProxyError::Upstream(
            "queued failure".to_string(),
        ))
        .await;

    let first = upstream
        .complete(&model, request("request-a"))
        .await
        .expect("first queued response");
    assert_eq!(first.id.as_deref(), Some("complete-a"));
    let second = upstream.complete(&model, request("request-b")).await;
    assert!(second.is_err());
    let missing = upstream.complete(&model, request("request-c")).await;
    assert!(missing.is_err());

    let recorded = upstream.requests().await;
    assert_eq!(recorded.len(), 3);
    assert_eq!(recorded[0].request.model.as_deref(), Some("request-a"));
    assert!(matches!(
        &recorded[0].result,
        support::RecordedResult::Complete(response) if response.id.as_deref() == Some("complete-a")
    ));
    assert!(matches!(
        &recorded[1].result,
        support::RecordedResult::Error(error)
            if error.source == support::RecordedErrorSource::Queued
                && error.code == "upstream_error"
                && error.message.contains("queued failure")
    ));
    assert!(matches!(
        &recorded[2].result,
        support::RecordedResult::Error(error)
            if error.source == support::RecordedErrorSource::MissingQueue
                && error.code == "internal_error"
    ));
}

#[tokio::test]
async fn fake_upstream_pairs_concurrent_calls_without_reordering_results() {
    let upstream = support::FakeChatCompletions::new();
    let model = default_model();
    for id in ["response-a", "response-b"] {
        upstream
            .queue_complete(ChatCompletionResponse {
                id: Some(id.to_string()),
                choices: Vec::new(),
                usage: None,
            })
            .await;
    }

    let first_upstream = upstream.clone();
    let first_model = model.clone();
    let first = tokio::spawn(async move {
        (
            "request-a".to_string(),
            first_upstream
                .complete(&first_model, request("request-a"))
                .await,
        )
    });
    let second_upstream = upstream.clone();
    let second_model = model.clone();
    let second = tokio::spawn(async move {
        (
            "request-b".to_string(),
            second_upstream
                .complete(&second_model, request("request-b"))
                .await,
        )
    });
    let returned = [
        first.await.expect("first task"),
        second.await.expect("second task"),
    ]
    .into_iter()
    .map(|(request_model, result)| {
        (
            request_model,
            result.expect("queued response").id.expect("response ID"),
        )
    })
    .collect::<Vec<_>>();

    let recorded = upstream.requests().await;
    assert_eq!(recorded.len(), 2);
    for record in recorded {
        let request_model = record.request.model.expect("request model");
        let response_id = match record.result {
            support::RecordedResult::Complete(response) => response.id.expect("response ID"),
            other => panic!("unexpected recorded result: {other:?}"),
        };
        assert!(returned.contains(&(request_model, response_id)));
    }
}

#[tokio::test]
async fn sink_and_seed_doubles_record_each_call_outcome() {
    let session = sample_session();
    let record = sample_record();
    let sink = support::FakeNodeSink::new();
    sink.set_error("sink failure").await;
    let sink_error = sink
        .on_turn_committed(&session, &record)
        .await
        .expect_err("configured sink failure");
    sink.clear_error().await;
    sink.on_turn_committed(&session, &record)
        .await
        .expect("cleared sink");
    let commits = sink.commits().await;
    assert!(matches!(
        &commits[0].outcome,
        support::RecordedResultStatus::Failed(error)
            if error.source == support::RecordedErrorSource::Configured
                && error.code == sink_error.code()
                && error.message == sink_error.to_string()
    ));
    assert!(matches!(
        commits[1].outcome,
        support::RecordedResultStatus::Succeeded
    ));

    let resolver = support::FakeSeedResolver::with_error("resolver failure");
    let resolver_error = resolver
        .resolve_seed_items(None)
        .await
        .expect_err("configured resolver failure");
    resolver.clear_error().await;
    assert!(resolver.resolve_seed_items(Some(&session)).await.is_ok());
    let calls = resolver.calls().await;
    assert!(matches!(
        &calls[0].outcome,
        support::RecordedResultStatus::Failed(error)
            if error.source == support::RecordedErrorSource::Configured
                && error.code == resolver_error.code()
                && error.message == resolver_error.to_string()
    ));
    assert!(matches!(
        calls[1].outcome,
        support::RecordedResultStatus::Succeeded
    ));
    assert_eq!(calls[1].session.as_ref(), Some(&session));
}

#[tokio::test]
async fn sse_collector_preserves_http_metadata_and_exact_framing() {
    let body = b"event: first\r\ndata: {\"value\":1}\r\n\r\n";
    let response = Response::builder()
        .status(207)
        .header("x-test", "yes")
        .body(Body::from(body.to_vec()))
        .expect("response builds");
    let collected = support::collect_sse_response(response)
        .await
        .expect("response collects");
    assert_eq!(collected.status, 207);
    assert_eq!(collected.headers["x-test"], "yes");
    assert_eq!(collected.body, body);
    assert!(collected.terminal_blank_line);
    assert_eq!(
        collected.frames[0].raw,
        "event: first\r\ndata: {\"value\":1}"
    );

    let unterminated =
        support::parse_sse_frames(b"event: final\ndata: {}").expect("unterminated frame parses");
    assert!(!unterminated[0].terminated);
    assert!(!support::has_terminal_blank_line(b"event: final\ndata: {}").expect("valid UTF-8"));
}

#[test]
fn sse_parser_rejects_incomplete_application_records_and_skips_comments() {
    assert!(
        support::parse_sse_frames(b": keep-alive\n\n")
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        support::parse_sse_frames(b"event: missing-data\n\n"),
        Err(support::SseError::MissingData { .. })
    ));
    assert!(matches!(
        support::parse_sse_frames(b"data: not-json\n\n"),
        Err(support::SseError::Json(_))
    ));
    assert!(matches!(
        support::parse_sse_frames(b"retry: 1000\n\n"),
        Err(support::SseError::MissingData { .. })
    ));
    let bare_cr = support::parse_sse_frames(b": keep-alive\revent: bare-cr\rdata: {}\r\r")
        .expect("bare-CR comments do not hide application fields");
    assert_eq!(bare_cr.len(), 1);
    assert_eq!(bare_cr[0].event.as_deref(), Some("bare-cr"));
    assert_eq!(bare_cr[0].payload, Some(json!({})));
    assert!(bare_cr[0].terminated);
}

#[test]
fn assertion_helpers_reject_invalid_contract_values() {
    support::assert_generated_id("resp_0123456789abcdef0123456789abcdef", "resp_");
    support::assert_fingerprint("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
    support::assert_artifact_hash(
        "blake3:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    );
    assert!(std::panic::catch_unwind(|| support::assert_fingerprint("A".repeat(64))).is_err());
    assert!(
        std::panic::catch_unwind(|| support::assert_artifact_hash(format!(
            "blake3:{}",
            "z".repeat(64)
        )))
        .is_err()
    );
    assert!(std::panic::catch_unwind(|| support::assert_contiguous_sequence_numbers(&[])).is_err());

    let event = ResponseEvent {
        name: "response.output_text.delta".to_string(),
        data: json!({"id": "wrong", "response": {"id": "wrong"}}),
    };
    assert!(
        std::panic::catch_unwind(|| support::assert_event_response_id(&event, "wrong")).is_err()
    );
}

fn default_model() -> ktxd::config::ModelConfig {
    AppConfig::default()
        .model("DeepSeek-V4-Pro")
        .expect("default model exists")
        .clone()
}

fn request(model: &str) -> ChatCompletionRequest {
    ChatCompletionRequest {
        model: Some(model.to_string()),
        messages: Vec::new(),
        tools: Vec::new(),
        tool_choice: None,
        parallel_tool_calls: None,
        stream: false,
        stream_options: None,
    }
}

fn sample_session() -> Session {
    Session {
        response_id: ResponseId::from_string("resp_parent"),
        parent_response_id: None,
        tenant_id: TenantId::from_string("tenant_test"),
        version: SessionVersion(1),
        committed_items: Vec::new(),
        deterministic_fingerprint: "0".repeat(64),
        final_response_json: json!({}),
    }
}

fn sample_record() -> TurnRecord {
    TurnRecord {
        turn_id: TurnId::from_string("turn_test"),
        response_id: ResponseId::from_string("resp_test"),
        parent_response_id: None,
        outcome: TurnOutcome::Completed,
        request_items: Vec::new(),
        output_items: Vec::new(),
        usage: UsageTotals::default(),
        error_code: None,
        error_message: None,
        deterministic_fingerprint: Some("0".repeat(64)),
    }
}
