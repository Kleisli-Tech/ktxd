mod support;

use ktxd::config::AppConfig;
use ktxd::domain::{
    CanonicalItem, MessageRole, ProvenanceTag, Session, TaggedItem, TurnOutcome, TurnRecord,
    UsageTotals,
};
use ktxd::driver::TurnDriver;
use ktxd::error::ProxyError;
use ktxd::ids::{ArtifactHash, ResponseId, SessionVersion, TenantId, TurnId};
use ktxd::responses::{ResponseEvent, VecEventSink, tagged_item_to_response_json};
use ktxd::session::{MemoryStore, SessionStore, TurnRecordStore};
use ktxd::translator::{NormalizedTurnInput, PreservedRequestFields};
use ktxd::wire::chat::{
    ChatChoice, ChatCompletionResponse, ChatDelta, ChatFunctionCallDelta, ChatResponseMessage,
    ChatToolCallDelta, ChatUsage,
};
use serde_json::{Value, json};
use std::sync::Arc;

#[tokio::test]
async fn non_streaming_success_commits_response_and_notifies_node_sink() {
    let (driver, store, upstream, node_sink, seed_resolver) = make_driver();
    upstream
        .queue_complete(non_streaming_response("hello", "stop", Some((3, 2, 5))))
        .await;
    let request_item = message_item(MessageRole::User, "question");
    let normalized = normalized(
        "DeepSeek-V4-Pro",
        false,
        vec![request_item.clone()],
        Vec::new(),
    );
    let mut sink = VecEventSink::default();

    let record = driver
        .drive(None, normalized, &mut sink)
        .await
        .expect("driver succeeds");

    assert!(sink.events.is_empty());
    assert_eq!(record.outcome, TurnOutcome::Completed);
    assert_eq!(record.request_items, vec![request_item]);
    assert_eq!(record.output_items.len(), 1);
    assert_eq!(
        record.output_items[0].artifact_hash,
        Some(ArtifactHash::from_string(
            ktxd::domain::blake3_hash(&record.output_items[0].item).expect("artifact hash"),
        ))
    );
    support::assert_record_fingerprint(&record);
    assert_eq!(record.usage.total_tokens, 5);
    assert_eq!(upstream.request_count().await, 1);
    assert_eq!(seed_resolver.call_count().await, 1);

    let request = upstream
        .complete_requests()
        .await
        .pop()
        .expect("compiled request");
    assert_eq!(request.messages.len(), 1);
    assert_eq!(request.messages[0].role, "user");
    assert_eq!(request.messages[0].content.as_deref(), Some("question"));
    assert!(!request.stream);

    let stored_record = TurnRecordStore::get(store.as_ref(), &record.turn_id)
        .await
        .expect("turn lookup")
        .expect("stored turn");
    assert_eq!(stored_record, record);
    let stored_response = store
        .get_response_json(&record.response_id)
        .await
        .expect("response lookup")
        .expect("stored response");
    assert_eq!(
        stored_response,
        driver.non_streaming_response("DeepSeek-V4-Pro", &record)
    );
    assert_eq!(stored_response["id"], record.response_id.to_string());
    assert_eq!(stored_response["status"], "completed");

    let session = SessionStore::get(store.as_ref(), &record.response_id)
        .await
        .expect("session lookup")
        .expect("completed session");
    assert_eq!(session.committed_items.len(), 2);
    assert_eq!(node_sink.call_count().await, 1);
    assert_eq!(node_sink.commits().await[0].record, record);
}

#[tokio::test]
async fn streaming_success_emits_ordered_events_and_commits_logical_output() {
    let (driver, store, upstream, node_sink, _) = make_driver();
    let request_item = message_item(MessageRole::User, "question");
    let tool = json!({
        "type": "function",
        "function": {
            "name": "lookup",
            "description": "Find a value",
            "parameters": {"type": "object", "properties": {"query": {"type": "string"}}}
        }
    });
    upstream
        .queue_stream(vec![
            stream_chunk(Some("hel"), None, None),
            stream_chunk(Some("lo"), Some("stop"), Some((4, 3, 7))),
        ])
        .await;
    let mut sink = VecEventSink::default();

    let record = driver
        .drive(
            None,
            normalized_with_options(
                "DeepSeek-V4-Pro",
                true,
                "instruction",
                vec![request_item.clone()],
                vec![tool],
                "auto",
                true,
            ),
            &mut sink,
        )
        .await
        .expect("driver succeeds");

    assert_eq!(record.outcome, TurnOutcome::Completed);
    assert_eq!(record.output_items.len(), 1);
    assert_eq!(
        record.output_items[0].item,
        CanonicalItem::Message {
            role: MessageRole::Assistant,
            text: "hello".to_string(),
        }
    );
    assert_eq!(
        sink.events
            .iter()
            .map(|event| event.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "response.created",
            "response.output_item.added",
            "response.output_text.delta",
            "response.output_text.delta",
            "response.output_item.done",
            "response.completed",
        ]
    );
    support::assert_contiguous_sequence_numbers(&sink.events);
    assert_event_response_ids(&sink.events, &record.response_id);

    let stream_requests = upstream.stream_requests().await;
    assert_eq!(stream_requests.len(), 1);
    assert!(upstream.complete_requests().await.is_empty());
    let request = &stream_requests[0];
    assert!(request.stream);
    assert_eq!(
        request
            .stream_options
            .as_ref()
            .map(|options| options.include_usage),
        Some(true)
    );
    assert_eq!(request.parallel_tool_calls, Some(true));
    assert_eq!(request.tool_choice, Some(Value::String("auto".to_string())));
    assert_eq!(request.messages.len(), 2);
    assert_eq!(request.messages[0].role, "system");
    assert_eq!(request.messages[0].content.as_deref(), Some("instruction"));
    assert_eq!(request.messages[1].role, "user");
    assert_eq!(request.messages[1].content.as_deref(), Some("question"));
    assert_eq!(request.tools.len(), 1);
    assert_eq!(request.tools[0].function.name, "lookup");

    let added = &sink.events[1].data;
    let first_delta = &sink.events[2].data;
    let second_delta = &sink.events[3].data;
    let done = &sink.events[4].data;
    let completed = &sink.events[5].data["response"];
    let item_id = added["item"]["id"].as_str().expect("added item ID");
    assert_eq!(added["output_index"], 0);
    assert_eq!(first_delta["output_index"], 0);
    assert_eq!(second_delta["output_index"], 0);
    assert_eq!(first_delta["item_id"], item_id);
    assert_eq!(second_delta["item_id"], item_id);
    assert_eq!(first_delta["delta"], "hel");
    assert_eq!(second_delta["delta"], "lo");
    assert_eq!(done["output_index"], 0);
    assert_eq!(
        done["item"],
        tagged_item_to_response_json(&record.output_items[0])
    );
    assert_eq!(completed["output"].as_array().unwrap().len(), 1);
    assert_eq!(completed["usage"]["total_tokens"], 7);
    assert_eq!(
        record.output_items[0].artifact_hash,
        Some(ArtifactHash::from_string(
            ktxd::domain::blake3_hash(&record.output_items[0].item).expect("artifact hash"),
        ))
    );

    let stored_response = store
        .get_response_json(&record.response_id)
        .await
        .expect("response lookup")
        .expect("stored response");
    assert_eq!(
        without_created_at(completed.clone()),
        without_created_at(stored_response.clone())
    );
    let session = SessionStore::get(store.as_ref(), &record.response_id)
        .await
        .expect("session lookup")
        .expect("completed session");
    assert_eq!(
        session.committed_items,
        vec![request_item, record.output_items[0].clone()]
    );
    assert_eq!(node_sink.call_count().await, 1);
    assert_eq!(node_sink.commits().await[0].session, session);
    assert_eq!(node_sink.commits().await[0].record, record);
}

#[tokio::test]
async fn unknown_model_fails_before_seed_or_upstream() {
    let (driver, store, upstream, node_sink, seed_resolver) = make_driver();
    let mut sink = VecEventSink::default();

    let record = driver
        .drive(
            None,
            normalized(
                "missing-model",
                true,
                vec![message_item(MessageRole::User, "question")],
                Vec::new(),
            ),
            &mut sink,
        )
        .await
        .expect("terminal record is returned");

    assert_failed_record(&record, "unknown_model");
    assert_eq!(
        record.error_message.as_deref(),
        Some("unknown model: missing-model")
    );
    assert_eq!(upstream.request_count().await, 0);
    assert_eq!(seed_resolver.call_count().await, 0);
    assert_eq!(sink.events.len(), 1);
    assert_eq!(sink.events[0].name, "response.failed");
    assert_event_response_ids(&sink.events, &record.response_id);
    assert_failed_event_matches_record(&sink.events[0], &record);
    support::assert_contiguous_sequence_numbers(&sink.events);
    assert_terminal_storage(&store, &record).await;
    assert_no_completed_side_effects(&store, &node_sink, &record).await;
}

#[tokio::test]
async fn seed_failure_emits_failed_event_without_calling_upstream() {
    let (driver, store, upstream, node_sink, seed_resolver) =
        make_driver_with_seed_error("seed failed");
    let mut sink = VecEventSink::default();

    let record = driver
        .drive(
            None,
            normalized(
                "DeepSeek-V4-Pro",
                true,
                vec![message_item(MessageRole::User, "question")],
                Vec::new(),
            ),
            &mut sink,
        )
        .await
        .expect("terminal record is returned");

    assert_failed_record(&record, "internal_error");
    assert!(
        record
            .error_message
            .as_deref()
            .unwrap()
            .contains("seed failed")
    );
    assert_eq!(upstream.request_count().await, 0);
    assert_eq!(seed_resolver.call_count().await, 1);
    assert_eq!(
        event_names(&sink.events),
        vec!["response.created", "response.failed"]
    );
    assert_event_response_ids(&sink.events, &record.response_id);
    assert_failed_event_matches_record(sink.events.last().expect("failed event"), &record);
    support::assert_contiguous_sequence_numbers(&sink.events);
    assert_terminal_storage(&store, &record).await;
    assert_no_completed_side_effects(&store, &node_sink, &record).await;
}

#[tokio::test]
async fn unsupported_tool_fails_during_compilation_without_upstream_call() {
    let (driver, store, upstream, node_sink, seed_resolver) = make_driver();
    let mut sink = VecEventSink::default();
    let unsupported_tool = json!({"type": "computer", "name": "click"});

    let record = driver
        .drive(
            None,
            normalized(
                "DeepSeek-V4-Pro",
                false,
                vec![message_item(MessageRole::User, "question")],
                vec![unsupported_tool],
            ),
            &mut sink,
        )
        .await
        .expect("terminal record is returned");

    assert_failed_record(&record, "unsupported_tool");
    assert_eq!(upstream.request_count().await, 0);
    assert_eq!(seed_resolver.call_count().await, 1);
    assert!(sink.events.is_empty());
    assert_terminal_storage(&store, &record).await;
    assert_no_completed_side_effects(&store, &node_sink, &record).await;
}

#[tokio::test]
async fn upstream_complete_failure_is_stored_as_failed_terminal_record() {
    let (driver, store, upstream, node_sink, _) = make_driver();
    upstream
        .queue_complete_error(ProxyError::Upstream("complete failed".to_string()))
        .await;
    let mut sink = VecEventSink::default();

    let record = driver
        .drive(
            None,
            normalized(
                "DeepSeek-V4-Pro",
                false,
                vec![message_item(MessageRole::User, "question")],
                Vec::new(),
            ),
            &mut sink,
        )
        .await
        .expect("terminal record is returned");

    assert_failed_record(&record, "upstream_error");
    assert!(
        record
            .error_message
            .as_deref()
            .unwrap()
            .contains("complete failed")
    );
    assert!(sink.events.is_empty());
    assert_eq!(upstream.request_count().await, 1);
    assert_terminal_storage(&store, &record).await;
    assert_no_completed_side_effects(&store, &node_sink, &record).await;
}

#[tokio::test]
async fn upstream_stream_failure_emits_failed_event_and_stores_record() {
    let (driver, store, upstream, node_sink, _) = make_driver();
    upstream
        .queue_stream_error(ProxyError::Upstream("stream failed".to_string()))
        .await;
    let mut sink = VecEventSink::default();

    let record = driver
        .drive(
            None,
            normalized(
                "DeepSeek-V4-Pro",
                true,
                vec![message_item(MessageRole::User, "question")],
                Vec::new(),
            ),
            &mut sink,
        )
        .await
        .expect("terminal record is returned");

    assert_failed_record(&record, "upstream_error");
    assert_eq!(
        event_names(&sink.events),
        vec!["response.created", "response.failed"]
    );
    assert_event_response_ids(&sink.events, &record.response_id);
    assert_failed_event_matches_record(sink.events.last().expect("failed event"), &record);
    support::assert_contiguous_sequence_numbers(&sink.events);
    assert_terminal_storage(&store, &record).await;
    assert_no_completed_side_effects(&store, &node_sink, &record).await;
}

#[tokio::test]
async fn malformed_stream_fails_without_completed_session() {
    let (driver, store, upstream, node_sink, _) = make_driver();
    upstream
        .queue_stream(vec![
            stream_tool_chunk("call_a", "lookup", None, None),
            stream_tool_chunk("call_b", "lookup", None, None),
        ])
        .await;
    let mut sink = VecEventSink::default();

    let record = driver
        .drive(
            None,
            normalized(
                "DeepSeek-V4-Pro",
                true,
                vec![message_item(MessageRole::User, "question")],
                Vec::new(),
            ),
            &mut sink,
        )
        .await
        .expect("terminal record is returned");

    assert_failed_record(&record, "malformed_stream");
    assert_eq!(record.output_items.len(), 0);
    assert_eq!(
        event_names(&sink.events),
        vec!["response.created", "response.failed"]
    );
    assert_event_response_ids(&sink.events, &record.response_id);
    assert_failed_event_matches_record(sink.events.last().expect("failed event"), &record);
    support::assert_contiguous_sequence_numbers(&sink.events);
    assert_eq!(upstream.request_count().await, 1);
    assert_eq!(node_sink.call_count().await, 0);
    assert!(
        SessionStore::get(store.as_ref(), &record.response_id)
            .await
            .expect("session lookup")
            .is_none()
    );
    assert_terminal_storage(&store, &record).await;
}

#[tokio::test]
async fn incomplete_output_is_stored_with_reason_without_session_transcript() {
    let (driver, store, upstream, node_sink, _) = make_driver();
    upstream
        .queue_complete(non_streaming_response(
            "partial",
            "length",
            Some((2, 8, 10)),
        ))
        .await;
    let mut sink = VecEventSink::default();

    let record = driver
        .drive(
            None,
            normalized(
                "DeepSeek-V4-Pro",
                false,
                vec![message_item(MessageRole::User, "question")],
                Vec::new(),
            ),
            &mut sink,
        )
        .await
        .expect("terminal record is returned");

    assert_eq!(record.outcome, TurnOutcome::Incomplete);
    assert_eq!(record.error_code.as_deref(), Some("max_output_tokens"));
    assert_eq!(record.output_items.len(), 1);
    assert_eq!(
        record.output_items[0].item,
        CanonicalItem::Message {
            role: MessageRole::Assistant,
            text: "partial".to_string(),
        }
    );
    let response = store
        .get_response_json(&record.response_id)
        .await
        .expect("response lookup")
        .expect("stored response");
    assert_eq!(response["status"], "incomplete");
    assert_eq!(response["id"], record.response_id.to_string());
    assert_eq!(response["usage"]["total_tokens"], 10);
    assert_eq!(
        response["incomplete_details"]["reason"],
        "max_output_tokens"
    );
    assert_eq!(response["output"].as_array().unwrap().len(), 1);
    assert!(
        SessionStore::get(store.as_ref(), &record.response_id)
            .await
            .expect("session lookup")
            .is_none()
    );
    assert_eq!(node_sink.call_count().await, 0);
    assert_eq!(
        TurnRecordStore::get(store.as_ref(), &record.turn_id)
            .await
            .expect("turn lookup"),
        Some(record.clone())
    );
}

#[tokio::test]
async fn streaming_incomplete_output_emits_terminal_event_and_persists_exact_state() {
    let (driver, store, upstream, node_sink, _) = make_driver();
    upstream
        .queue_stream(vec![
            stream_chunk(Some("part"), None, None),
            stream_chunk(None, Some("length"), Some((2, 4, 6))),
        ])
        .await;
    let mut sink = VecEventSink::default();

    let record = driver
        .drive(
            None,
            normalized(
                "DeepSeek-V4-Pro",
                true,
                vec![message_item(MessageRole::User, "question")],
                Vec::new(),
            ),
            &mut sink,
        )
        .await
        .expect("terminal record is returned");

    assert_eq!(record.outcome, TurnOutcome::Incomplete);
    assert_eq!(record.error_code.as_deref(), Some("max_output_tokens"));
    assert_eq!(record.output_items.len(), 1);
    assert_eq!(record.usage.total_tokens, 6);
    assert_eq!(
        event_names(&sink.events),
        vec![
            "response.created",
            "response.output_item.added",
            "response.output_text.delta",
            "response.output_item.done",
            "response.incomplete",
        ]
    );
    assert_event_response_ids(&sink.events, &record.response_id);
    support::assert_contiguous_sequence_numbers(&sink.events);
    assert_eq!(sink.events[2].data["delta"], "part");
    assert_eq!(
        sink.events[4].data["response"]["incomplete_details"]["reason"],
        "max_output_tokens"
    );
    let stored_response = assert_terminal_storage(&store, &record).await;
    assert_eq!(
        without_created_at(sink.events[4].data["response"].clone()),
        without_created_at(stored_response)
    );
    assert_no_completed_side_effects(&store, &node_sink, &record).await;
}

#[tokio::test]
async fn non_streaming_failed_terminal_discards_partial_output() {
    let (driver, store, upstream, node_sink, _) = make_driver();
    upstream
        .queue_complete(non_streaming_response("partial", "other", None))
        .await;
    let mut sink = VecEventSink::default();

    let record = driver
        .drive(
            None,
            normalized(
                "DeepSeek-V4-Pro",
                false,
                vec![message_item(MessageRole::User, "question")],
                Vec::new(),
            ),
            &mut sink,
        )
        .await
        .expect("terminal record is returned");

    assert_failed_record(&record, "unsupported_finish_reason_other");
    assert!(record.output_items.is_empty());
    assert!(sink.events.is_empty());
    let response = assert_terminal_storage(&store, &record).await;
    assert_eq!(response["status"], "failed");
    assert!(
        response["output"]
            .as_array()
            .expect("output array")
            .is_empty()
    );
    assert_no_completed_side_effects(&store, &node_sink, &record).await;
}

#[tokio::test]
async fn failed_terminal_stream_discards_partial_output_and_never_commits_session() {
    let (driver, store, upstream, node_sink, _) = make_driver();
    upstream
        .queue_stream(vec![
            stream_chunk(Some("partial"), None, None),
            stream_chunk(None, Some("other"), None),
        ])
        .await;
    let mut sink = VecEventSink::default();

    let record = driver
        .drive(
            None,
            normalized(
                "DeepSeek-V4-Pro",
                true,
                vec![message_item(MessageRole::User, "question")],
                Vec::new(),
            ),
            &mut sink,
        )
        .await
        .expect("terminal record is returned");

    assert_failed_record(&record, "stream_failed");
    assert_eq!(
        record.error_message.as_deref(),
        Some("unsupported_finish_reason_other")
    );
    assert!(record.output_items.is_empty());
    assert_eq!(
        event_names(&sink.events),
        vec![
            "response.created",
            "response.output_item.added",
            "response.output_text.delta",
            "response.output_item.done",
            "response.failed",
        ]
    );
    assert_event_response_ids(&sink.events, &record.response_id);
    assert_failed_event_matches_record(sink.events.last().expect("failed event"), &record);
    support::assert_contiguous_sequence_numbers(&sink.events);
    assert_terminal_storage(&store, &record).await;
    assert!(
        SessionStore::get(store.as_ref(), &record.response_id)
            .await
            .expect("session lookup")
            .is_none()
    );
    assert_eq!(node_sink.call_count().await, 0);
}

#[tokio::test]
async fn node_sink_failure_does_not_roll_back_completed_commit() {
    let (driver, store, upstream, node_sink, _) = make_driver();
    node_sink.set_error("node unavailable").await;
    upstream
        .queue_complete(non_streaming_response("hello", "stop", None))
        .await;
    let mut sink = VecEventSink::default();

    let record = driver
        .drive(
            None,
            normalized(
                "DeepSeek-V4-Pro",
                false,
                vec![message_item(MessageRole::User, "question")],
                Vec::new(),
            ),
            &mut sink,
        )
        .await
        .expect("commit succeeds despite sink failure");

    assert_eq!(record.outcome, TurnOutcome::Completed);
    assert_eq!(node_sink.call_count().await, 1);
    assert!(matches!(
        node_sink.commits().await[0].outcome,
        support::RecordedResultStatus::Failed(_)
    ));
    let session = SessionStore::get(store.as_ref(), &record.response_id)
        .await
        .expect("session lookup")
        .expect("completed session");
    let stored_record = TurnRecordStore::get(store.as_ref(), &record.turn_id)
        .await
        .expect("turn lookup")
        .expect("stored turn");
    let response = store
        .get_response_json(&record.response_id)
        .await
        .expect("response lookup")
        .expect("stored response");
    assert_eq!(stored_record, record);
    assert_eq!(session.response_id, record.response_id);
    assert_eq!(response["id"], record.response_id.to_string());
    assert_eq!(response["status"], "completed");
    assert_eq!(node_sink.commits().await[0].session, session);
    assert_eq!(node_sink.commits().await[0].record, record);
}

#[tokio::test]
async fn child_turn_uses_parent_transcript_identity_and_incremented_version() {
    let (driver, store, upstream, node_sink, seed_resolver) = make_driver();
    let parent_response_id = ResponseId::from_string("resp_parent");
    let parent_item = message_item(MessageRole::User, "parent");
    let parent_output = message_item(MessageRole::Assistant, "answer");
    let seed_item = message_item(MessageRole::User, "seed");
    let child_request = message_item(MessageRole::User, "child request");
    let parent = Session {
        response_id: parent_response_id.clone(),
        parent_response_id: None,
        tenant_id: TenantId::from_string("tenant_parent"),
        version: SessionVersion(4),
        committed_items: vec![parent_item.clone(), parent_output.clone()],
        deterministic_fingerprint: "0".repeat(64),
        final_response_json: json!({"id": parent_response_id, "status": "completed"}),
    };
    let parent_record = TurnRecord {
        turn_id: TurnId::from_string("turn_parent"),
        response_id: parent.response_id.clone(),
        parent_response_id: None,
        outcome: TurnOutcome::Completed,
        request_items: vec![parent_item.clone()],
        output_items: vec![parent_output.clone()],
        usage: UsageTotals::default(),
        error_code: None,
        error_message: None,
        deterministic_fingerprint: Some("0".repeat(64)),
    };
    store
        .commit_completed(parent.clone(), parent_record)
        .await
        .expect("parent commit");
    seed_resolver.set_items(vec![seed_item.clone()]).await;
    upstream
        .queue_complete(non_streaming_response("child", "stop", None))
        .await;
    let mut sink = VecEventSink::default();

    let record = driver
        .drive(
            Some(parent.clone()),
            normalized(
                "DeepSeek-V4-Pro",
                false,
                vec![child_request.clone()],
                Vec::new(),
            ),
            &mut sink,
        )
        .await
        .expect("child commit");

    assert_eq!(record.parent_response_id, Some(parent.response_id.clone()));
    let request = upstream
        .complete_requests()
        .await
        .pop()
        .expect("compiled child request");
    let contents = request
        .messages
        .iter()
        .filter_map(|message| message.content.as_deref())
        .collect::<Vec<_>>();
    assert_eq!(contents, vec!["parent", "answer", "seed", "child request"]);
    assert_eq!(seed_resolver.sessions().await, vec![Some(parent.clone())]);
    let child = SessionStore::get(store.as_ref(), &record.response_id)
        .await
        .expect("session lookup")
        .expect("child session");
    assert_eq!(child.parent_response_id, Some(parent.response_id));
    assert_eq!(child.tenant_id, parent.tenant_id);
    assert_eq!(child.version, SessionVersion(5));
    assert_eq!(
        child.committed_items,
        vec![
            parent_item,
            parent_output,
            seed_item,
            child_request,
            record.output_items[0].clone(),
        ]
    );
    assert_eq!(
        child.deterministic_fingerprint,
        record.deterministic_fingerprint.clone().unwrap()
    );
    assert!(
        store
            .get_response_json(&record.response_id)
            .await
            .expect("response lookup")
            .is_some()
    );
    assert_eq!(node_sink.commits().await[0].session, child);
}

#[tokio::test]
async fn equivalent_tool_key_order_keeps_fingerprint_and_changed_transcript_changes_it() {
    let request_item = message_item(MessageRole::User, "same");
    let tools_one = vec![json!({
        "type": "function",
        "function": {"name": "lookup", "parameters": {"b": 2, "a": 1}}
    })];
    let tools_two = vec![json!({
        "function": {"parameters": {"a": 1, "b": 2}, "name": "lookup"},
        "type": "function"
    })];
    let mut changed_item = request_item.clone();
    if let CanonicalItem::Message { text, .. } = &mut changed_item.item {
        *text = "changed".to_string();
    }

    let first = run_fingerprint_turn(request_item.clone(), tools_one.clone(), "answer").await;
    let equivalent =
        run_fingerprint_turn(message_item(MessageRole::User, "same"), tools_two, "answer").await;
    let changed = run_fingerprint_turn(changed_item, tools_one, "answer").await;

    assert_eq!(first, equivalent);
    assert_ne!(first, changed);
    support::assert_fingerprint(&first);
}

fn make_driver() -> (
    TurnDriver,
    Arc<MemoryStore>,
    support::FakeChatCompletions,
    support::FakeNodeSink,
    support::FakeSeedResolver,
) {
    let store = MemoryStore::shared();
    let upstream = support::FakeChatCompletions::new();
    let node_sink = support::FakeNodeSink::new();
    let seed_resolver = support::FakeSeedResolver::new(Vec::new());
    let driver = TurnDriver::new(
        Arc::new(AppConfig::default()),
        Arc::new(upstream.clone()),
        store.clone(),
        Arc::new(node_sink.clone()),
        Arc::new(seed_resolver.clone()),
    );
    (driver, store, upstream, node_sink, seed_resolver)
}

fn make_driver_with_seed_error(
    error: &str,
) -> (
    TurnDriver,
    Arc<MemoryStore>,
    support::FakeChatCompletions,
    support::FakeNodeSink,
    support::FakeSeedResolver,
) {
    let store = MemoryStore::shared();
    let upstream = support::FakeChatCompletions::new();
    let node_sink = support::FakeNodeSink::new();
    let seed_resolver = support::FakeSeedResolver::with_error(error);
    let driver = TurnDriver::new(
        Arc::new(AppConfig::default()),
        Arc::new(upstream.clone()),
        store.clone(),
        Arc::new(node_sink.clone()),
        Arc::new(seed_resolver.clone()),
    );
    (driver, store, upstream, node_sink, seed_resolver)
}

async fn run_fingerprint_turn(request_item: TaggedItem, tools: Vec<Value>, output: &str) -> String {
    let (driver, _, upstream, _, _) = make_driver();
    upstream
        .queue_complete(non_streaming_response(output, "stop", None))
        .await;
    let mut sink = VecEventSink::default();
    let record = driver
        .drive(
            None,
            normalized("DeepSeek-V4-Pro", false, vec![request_item], tools),
            &mut sink,
        )
        .await
        .expect("fingerprint turn");
    record
        .deterministic_fingerprint
        .expect("completed turn has fingerprint")
}

fn normalized(
    model: &str,
    stream: bool,
    request_items: Vec<TaggedItem>,
    tools: Vec<Value>,
) -> NormalizedTurnInput {
    normalized_with_options(model, stream, "", request_items, tools, "", true)
}

fn normalized_with_options(
    model: &str,
    stream: bool,
    instructions: &str,
    request_items: Vec<TaggedItem>,
    tools: Vec<Value>,
    tool_choice: &str,
    parallel_tool_calls: bool,
) -> NormalizedTurnInput {
    NormalizedTurnInput {
        model: model.to_string(),
        instructions: instructions.to_string(),
        previous_response_id: None,
        request_items,
        tools,
        tool_choice: tool_choice.to_string(),
        parallel_tool_calls,
        stream,
        preserved: PreservedRequestFields::default(),
    }
}

fn message_item(role: MessageRole, text: &str) -> TaggedItem {
    let provenance = match role {
        MessageRole::User => ProvenanceTag::user_trusted(),
        MessageRole::Assistant => ProvenanceTag::model_semi(),
    };
    TaggedItem::new(
        CanonicalItem::Message {
            role,
            text: text.to_string(),
        },
        provenance,
    )
}

fn non_streaming_response(
    content: &str,
    finish_reason: &str,
    usage: Option<(u64, u64, u64)>,
) -> ChatCompletionResponse {
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
            finish_reason: Some(finish_reason.to_string()),
        }],
        usage: usage.map(chat_usage),
    }
}

fn stream_chunk(
    content: Option<&str>,
    finish_reason: Option<&str>,
    usage: Option<(u64, u64, u64)>,
) -> ChatCompletionResponse {
    ChatCompletionResponse {
        id: None,
        choices: vec![ChatChoice {
            index: Some(0),
            message: None,
            delta: Some(ChatDelta {
                role: None,
                content: content.map(str::to_string),
                tool_calls: Vec::new(),
            }),
            finish_reason: finish_reason.map(str::to_string),
        }],
        usage: usage.map(chat_usage),
    }
}

fn stream_tool_chunk(
    id: &str,
    name: &str,
    arguments: Option<&str>,
    finish_reason: Option<&str>,
) -> ChatCompletionResponse {
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
                    id: Some(id.to_string()),
                    tool_type: Some("function".to_string()),
                    function: Some(ChatFunctionCallDelta {
                        name: Some(name.to_string()),
                        arguments: arguments.map(str::to_string),
                    }),
                }],
            }),
            finish_reason: finish_reason.map(str::to_string),
        }],
        usage: None,
    }
}

fn chat_usage((prompt_tokens, completion_tokens, total_tokens): (u64, u64, u64)) -> ChatUsage {
    ChatUsage {
        prompt_tokens: Some(prompt_tokens),
        completion_tokens: Some(completion_tokens),
        total_tokens: Some(total_tokens),
    }
}

fn event_names(events: &[ResponseEvent]) -> Vec<&str> {
    events.iter().map(|event| event.name.as_str()).collect()
}

fn assert_event_response_ids(events: &[ResponseEvent], response_id: &ResponseId) {
    for event in events {
        support::assert_event_response_id(event, response_id.to_string());
    }
}

fn assert_failed_record(record: &TurnRecord, code: &str) {
    assert_eq!(record.outcome, TurnOutcome::Failed);
    assert_eq!(record.error_code.as_deref(), Some(code));
    assert!(record.deterministic_fingerprint.is_none());
}

fn assert_failed_event_matches_record(event: &ResponseEvent, record: &TurnRecord) {
    assert_eq!(event.name, "response.failed");
    assert_eq!(event.data["response"]["id"], record.response_id.to_string());
    assert_eq!(event.data["response"]["status"], "failed");
    assert_eq!(
        event.data["response"]["error"]["code"],
        record.error_code.as_deref().expect("failed code")
    );
    assert_eq!(
        event.data["response"]["error"]["message"],
        record
            .error_message
            .as_deref()
            .or(record.error_code.as_deref())
            .expect("failed message")
    );
}

async fn assert_terminal_storage(store: &MemoryStore, record: &TurnRecord) -> Value {
    assert_eq!(
        TurnRecordStore::get(store, &record.turn_id)
            .await
            .expect("turn lookup")
            .as_ref(),
        Some(record)
    );
    let response = store
        .get_response_json(&record.response_id)
        .await
        .expect("response lookup")
        .expect("stored response");
    assert_eq!(response["id"], record.response_id.to_string());
    match &record.outcome {
        TurnOutcome::Incomplete => {
            assert_eq!(response["status"], "incomplete");
            assert_eq!(
                response["incomplete_details"]["reason"],
                record.error_code.as_deref().expect("incomplete reason")
            );
        }
        TurnOutcome::Failed => {
            assert_eq!(response["status"], "failed");
            assert_eq!(
                response["error"]["code"],
                record.error_code.as_deref().expect("failed code")
            );
            assert_eq!(
                response["error"]["message"],
                record
                    .error_message
                    .as_deref()
                    .or(record.error_code.as_deref())
                    .expect("failed message")
            );
        }
        other => panic!("unexpected terminal outcome: {other:?}"),
    }
    response
}

async fn assert_no_completed_side_effects(
    store: &MemoryStore,
    node_sink: &support::FakeNodeSink,
    record: &TurnRecord,
) {
    assert!(
        SessionStore::get(store, &record.response_id)
            .await
            .expect("session lookup")
            .is_none()
    );
    assert_eq!(node_sink.call_count().await, 0);
}

fn without_created_at(mut value: Value) -> Value {
    value
        .as_object_mut()
        .expect("response object")
        .remove("created_at");
    value
}
