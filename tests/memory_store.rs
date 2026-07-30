use ktxd::domain::{
    CanonicalItem, MessageRole, ProvenanceTag, Session, TaggedItem, TurnOutcome, TurnRecord,
    UsageTotals,
};
use ktxd::error::ProxyError;
use ktxd::ids::{ResponseId, SessionVersion, TenantId, TurnId};
use ktxd::session::{MemoryStore, SessionStore, TurnRecordStore};
use serde_json::{Value, json};

#[tokio::test]
async fn completed_commit_keeps_session_response_and_turn_consistent() {
    let store = MemoryStore::shared();
    let response_id = ResponseId::from_string("resp_completed");
    let turn_id = TurnId::from_string("turn_completed");
    let request_item = message_item(MessageRole::User, "hello");
    let output_item = message_item(MessageRole::Assistant, "world");
    let final_response = json!({
        "id": response_id,
        "status": "completed",
        "output": [{"type": "message", "text": "world"}],
    });
    let session = Session {
        response_id: response_id.clone(),
        parent_response_id: Some(ResponseId::from_string("resp_parent")),
        tenant_id: TenantId::from_string("tenant_test"),
        version: SessionVersion(2),
        committed_items: vec![request_item.clone(), output_item.clone()],
        deterministic_fingerprint: "session-fingerprint".to_string(),
        final_response_json: final_response.clone(),
    };
    let record = turn_record(
        turn_id.clone(),
        response_id.clone(),
        session.parent_response_id.clone(),
        TurnOutcome::Completed,
        vec![request_item],
        vec![output_item],
    );

    store
        .commit_completed(session.clone(), record.clone())
        .await
        .expect("completed commit");

    assert_eq!(session_get(&store, &response_id).await, Some(session));
    assert_eq!(
        store
            .get_response_json(&response_id)
            .await
            .expect("response lookup"),
        Some(final_response)
    );
    assert_eq!(turn_get(&store, &turn_id).await, Some(record));
    assert_eq!(store.count().await, 1);
}

#[tokio::test]
async fn reads_return_clones_instead_of_mutable_store_state() {
    let store = MemoryStore::shared();
    let response_id = ResponseId::from_string("resp_clone");
    let turn_id = TurnId::from_string("turn_clone");
    let session = Session {
        response_id: response_id.clone(),
        parent_response_id: None,
        tenant_id: TenantId::from_string("tenant_test"),
        version: SessionVersion(1),
        committed_items: vec![message_item(MessageRole::User, "original")],
        deterministic_fingerprint: "fingerprint".to_string(),
        final_response_json: json!({"status": "completed"}),
    };
    let record = turn_record(
        turn_id.clone(),
        response_id.clone(),
        None,
        TurnOutcome::Completed,
        Vec::new(),
        vec![message_item(MessageRole::Assistant, "original")],
    );
    store
        .commit_completed(session.clone(), record.clone())
        .await
        .expect("completed commit");

    let mut returned_session = session_get(&store, &response_id)
        .await
        .expect("stored session");
    returned_session.committed_items.clear();
    returned_session.final_response_json["status"] = json!("mutated");

    let mut returned_response = store
        .get_response_json(&response_id)
        .await
        .expect("response lookup")
        .expect("stored response");
    returned_response["status"] = json!("mutated");

    let mut returned_record = turn_get(&store, &turn_id).await.expect("stored turn");
    returned_record.output_items.clear();

    assert_eq!(session_get(&store, &response_id).await, Some(session));
    assert_eq!(
        store
            .get_response_json(&response_id)
            .await
            .expect("response lookup"),
        Some(json!({"status": "completed"}))
    );
    assert_eq!(turn_get(&store, &turn_id).await, Some(record));
}

#[tokio::test]
async fn unknown_ids_return_none() {
    let store = MemoryStore::shared();

    assert_eq!(
        session_get(&store, &ResponseId::from_string("resp_unknown")).await,
        None
    );
    assert_eq!(
        store
            .get_response_json(&ResponseId::from_string("resp_unknown"))
            .await
            .expect("response lookup"),
        None
    );
    assert_eq!(
        turn_get(&store, &TurnId::from_string("turn_unknown")).await,
        None
    );
}

#[tokio::test]
async fn duplicate_turn_id_is_rejected_without_overwriting_existing_records() {
    let store = MemoryStore::shared();
    let turn_id = TurnId::from_string("turn_duplicate");
    let first_response_id = ResponseId::from_string("resp_first");
    let second_response_id = ResponseId::from_string("resp_second");
    let first_session = session(first_response_id.clone(), json!({"value": "first"}));
    let first_record = turn_record(
        turn_id.clone(),
        first_response_id.clone(),
        None,
        TurnOutcome::Completed,
        Vec::new(),
        vec![message_item(MessageRole::Assistant, "first")],
    );
    store
        .commit_completed(first_session.clone(), first_record.clone())
        .await
        .expect("first commit");

    let second_session = session(second_response_id.clone(), json!({"value": "second"}));
    let second_record = turn_record(
        turn_id.clone(),
        second_response_id.clone(),
        None,
        TurnOutcome::Completed,
        Vec::new(),
        vec![message_item(MessageRole::Assistant, "second")],
    );
    let error = store
        .commit_completed(second_session, second_record)
        .await
        .expect_err("duplicate turn must fail");
    assert_internal(error, "duplicate turn record");

    assert_eq!(
        session_get(&store, &first_response_id).await,
        Some(first_session)
    );
    assert_eq!(
        store
            .get_response_json(&first_response_id)
            .await
            .expect("response lookup"),
        Some(json!({"value": "first"}))
    );
    assert_eq!(turn_get(&store, &turn_id).await, Some(first_record));
    assert_eq!(session_get(&store, &second_response_id).await, None);
    assert_eq!(
        store
            .get_response_json(&second_response_id)
            .await
            .expect("response lookup"),
        None
    );
    assert_eq!(store.count().await, 1);
}

#[tokio::test]
async fn completed_commit_rejects_mismatched_ids_without_partial_writes() {
    let store = MemoryStore::shared();
    let session_response_id = ResponseId::from_string("resp_session");
    let record_response_id = ResponseId::from_string("resp_record");
    let mismatched_response_record = turn_record(
        TurnId::from_string("turn_mismatched_response"),
        record_response_id.clone(),
        None,
        TurnOutcome::Completed,
        Vec::new(),
        Vec::new(),
    );

    let error = store
        .commit_completed(
            session(
                session_response_id.clone(),
                json!({"id": session_response_id, "status": "completed"}),
            ),
            mismatched_response_record,
        )
        .await
        .expect_err("mismatched response IDs must fail");
    assert_internal(error, "session and turn response IDs do not match");

    let response_id = ResponseId::from_string("resp_parent_mismatch");
    let mut parent_mismatched_session = session(
        response_id.clone(),
        json!({"id": response_id, "status": "completed"}),
    );
    parent_mismatched_session.parent_response_id =
        Some(ResponseId::from_string("resp_session_parent"));
    let parent_mismatched_record = turn_record(
        TurnId::from_string("turn_mismatched_parent"),
        response_id.clone(),
        Some(ResponseId::from_string("resp_record_parent")),
        TurnOutcome::Completed,
        Vec::new(),
        Vec::new(),
    );
    let error = store
        .commit_completed(parent_mismatched_session, parent_mismatched_record)
        .await
        .expect_err("mismatched parent IDs must fail");
    assert_internal(error, "session and turn parent response IDs do not match");

    assert_eq!(session_get(&store, &session_response_id).await, None);
    assert_eq!(session_get(&store, &record_response_id).await, None);
    assert_eq!(session_get(&store, &response_id).await, None);
    assert_eq!(
        store
            .get_response_json(&session_response_id)
            .await
            .expect("response lookup"),
        None
    );
    assert_eq!(
        turn_get(&store, &TurnId::from_string("turn_mismatched_response")).await,
        None
    );
    assert_eq!(
        turn_get(&store, &TurnId::from_string("turn_mismatched_parent")).await,
        None
    );
    assert_eq!(store.count().await, 0);
}

#[tokio::test]
async fn completed_commit_rejects_duplicate_response_id_without_overwriting() {
    let store = MemoryStore::shared();
    let response_id = ResponseId::from_string("resp_duplicate");
    let first_turn_id = TurnId::from_string("turn_first");
    let second_turn_id = TurnId::from_string("turn_second");
    let first_session = session(response_id.clone(), json!({"value": "first"}));
    let first_record = turn_record(
        first_turn_id.clone(),
        response_id.clone(),
        None,
        TurnOutcome::Completed,
        Vec::new(),
        vec![message_item(MessageRole::Assistant, "first")],
    );
    store
        .commit_completed(first_session.clone(), first_record.clone())
        .await
        .expect("first commit");

    let second_session = session(response_id.clone(), json!({"value": "second"}));
    let second_record = turn_record(
        second_turn_id.clone(),
        response_id.clone(),
        None,
        TurnOutcome::Completed,
        Vec::new(),
        vec![message_item(MessageRole::Assistant, "second")],
    );
    let error = store
        .commit_completed(second_session, second_record)
        .await
        .expect_err("duplicate response must fail");
    assert_internal(error, "duplicate response record");

    assert_eq!(session_get(&store, &response_id).await, Some(first_session));
    assert_eq!(
        store
            .get_response_json(&response_id)
            .await
            .expect("response lookup"),
        Some(json!({"value": "first"}))
    );
    assert_eq!(turn_get(&store, &first_turn_id).await, Some(first_record));
    assert_eq!(turn_get(&store, &second_turn_id).await, None);
    assert_eq!(store.count().await, 1);
}

#[tokio::test]
async fn direct_response_json_round_trips_exact_value() {
    let store = MemoryStore::shared();
    let response_id = ResponseId::from_string("resp_direct");
    let response = json!({
        "id": response_id,
        "status": "failed",
        "error": {"code": "upstream_error", "details": [null, 3, true]},
    });
    store
        .put_response_json(response_id.clone(), response.clone())
        .await
        .expect("response JSON put");

    assert_eq!(
        store
            .get_response_json(&response_id)
            .await
            .expect("response JSON lookup"),
        Some(response)
    );
}

#[tokio::test]
async fn failed_and_incomplete_terminal_commits_round_trip() {
    let store = MemoryStore::shared();
    let failed_response_id = ResponseId::from_string("resp_failed");
    let incomplete_response_id = ResponseId::from_string("resp_incomplete");
    let failed_response = json!({
        "id": failed_response_id,
        "status": "failed",
        "error": {"code": "upstream_error", "message": "provider failed"},
    });
    let incomplete_response = json!({
        "id": incomplete_response_id,
        "status": "incomplete",
        "output": [{"type": "message", "text": "partial"}],
        "incomplete_details": {"reason": "max_output_tokens"},
    });

    let mut failed_record = turn_record(
        TurnId::from_string("turn_failed"),
        failed_response_id.clone(),
        None,
        TurnOutcome::Failed,
        Vec::new(),
        Vec::new(),
    );
    failed_record.error_code = Some("upstream_error".to_string());
    failed_record.error_message = Some("provider failed".to_string());
    failed_record.deterministic_fingerprint = None;
    let mut incomplete_record = turn_record(
        TurnId::from_string("turn_incomplete"),
        incomplete_response_id.clone(),
        None,
        TurnOutcome::Incomplete,
        Vec::new(),
        vec![message_item(MessageRole::Assistant, "partial")],
    );
    incomplete_record.error_code = Some("max_output_tokens".to_string());
    incomplete_record.deterministic_fingerprint = None;
    store
        .commit_terminal(
            failed_response_id.clone(),
            failed_response.clone(),
            failed_record.clone(),
        )
        .await
        .expect("failed terminal commit");
    store
        .commit_terminal(
            incomplete_response_id.clone(),
            incomplete_response.clone(),
            incomplete_record.clone(),
        )
        .await
        .expect("incomplete terminal commit");

    assert_eq!(
        store
            .get_response_json(&failed_response_id)
            .await
            .expect("response JSON lookup"),
        Some(failed_response)
    );
    assert_eq!(
        store
            .get_response_json(&incomplete_response_id)
            .await
            .expect("response JSON lookup"),
        Some(incomplete_response)
    );
    assert_eq!(
        turn_get(&store, &failed_record.turn_id).await,
        Some(failed_record)
    );
    assert_eq!(
        turn_get(&store, &incomplete_record.turn_id).await,
        Some(incomplete_record)
    );
    assert_eq!(store.count().await, 2);
}

#[tokio::test]
async fn duplicate_terminal_turn_is_rejected_atomically() {
    let store = MemoryStore::shared();
    let turn_id = TurnId::from_string("turn_terminal_duplicate");
    let first_response_id = ResponseId::from_string("resp_terminal_first");
    let second_response_id = ResponseId::from_string("resp_terminal_second");
    let first_response = json!({"id": first_response_id, "status": "failed"});
    let first_record = turn_record(
        turn_id.clone(),
        first_response_id.clone(),
        None,
        TurnOutcome::Failed,
        Vec::new(),
        Vec::new(),
    );
    store
        .commit_terminal(
            first_response_id.clone(),
            first_response.clone(),
            first_record.clone(),
        )
        .await
        .expect("first terminal commit");

    let second_record = turn_record(
        turn_id.clone(),
        second_response_id.clone(),
        None,
        TurnOutcome::Incomplete,
        Vec::new(),
        vec![message_item(MessageRole::Assistant, "partial")],
    );
    let error = store
        .commit_terminal(
            second_response_id.clone(),
            json!({"id": second_response_id, "status": "incomplete"}),
            second_record,
        )
        .await
        .expect_err("duplicate terminal turn must fail");
    assert_internal(error, "duplicate turn record");

    assert_eq!(turn_get(&store, &turn_id).await, Some(first_record));
    assert_eq!(
        store
            .get_response_json(&first_response_id)
            .await
            .expect("response lookup"),
        Some(first_response)
    );
    assert_eq!(
        store
            .get_response_json(&second_response_id)
            .await
            .expect("response lookup"),
        None
    );
    assert_eq!(store.count().await, 1);
}

#[tokio::test]
async fn terminal_commit_rejects_mismatched_response_id_without_writes() {
    let store = MemoryStore::shared();
    let response_id = ResponseId::from_string("resp_terminal_key");
    let record_response_id = ResponseId::from_string("resp_terminal_record");
    let turn_id = TurnId::from_string("turn_terminal_mismatch");
    let record = turn_record(
        turn_id.clone(),
        record_response_id.clone(),
        None,
        TurnOutcome::Failed,
        Vec::new(),
        Vec::new(),
    );

    let error = store
        .commit_terminal(
            response_id.clone(),
            json!({"id": response_id, "status": "failed"}),
            record,
        )
        .await
        .expect_err("mismatched terminal response IDs must fail");
    assert_internal(error, "response JSON and turn response IDs do not match");

    assert_eq!(turn_get(&store, &turn_id).await, None);
    assert_eq!(
        store
            .get_response_json(&response_id)
            .await
            .expect("response lookup"),
        None
    );
    assert_eq!(
        store
            .get_response_json(&record_response_id)
            .await
            .expect("response lookup"),
        None
    );
    assert_eq!(store.count().await, 0);
}

#[tokio::test]
async fn direct_turn_put_rejects_duplicate_without_overwriting() {
    let store = MemoryStore::shared();
    let turn_id = TurnId::from_string("turn_direct_duplicate");
    let first_record = turn_record(
        turn_id.clone(),
        ResponseId::from_string("resp_direct_first"),
        None,
        TurnOutcome::Failed,
        Vec::new(),
        Vec::new(),
    );
    let second_record = turn_record(
        turn_id.clone(),
        ResponseId::from_string("resp_direct_second"),
        None,
        TurnOutcome::Incomplete,
        Vec::new(),
        vec![message_item(MessageRole::Assistant, "partial")],
    );
    TurnRecordStore::put(store.as_ref(), first_record.clone())
        .await
        .expect("first direct turn put");

    let error = TurnRecordStore::put(store.as_ref(), second_record)
        .await
        .expect_err("duplicate direct turn put must fail");
    assert_internal(error, "duplicate turn record");

    assert_eq!(turn_get(&store, &turn_id).await, Some(first_record));
    assert_eq!(store.count().await, 1);
}

#[tokio::test]
async fn continuation_session_preserves_parent_transcript_and_final_response() {
    let store = MemoryStore::shared();
    let parent_response_id = ResponseId::from_string("resp_parent");
    let child_response_id = ResponseId::from_string("resp_child");
    let parent_request = message_item(MessageRole::User, "first turn");
    let parent_output = message_item(MessageRole::Assistant, "first answer");
    let child_item = message_item(MessageRole::User, "second turn");
    let child_output = message_item(MessageRole::Assistant, "second answer");
    let parent_final_response = json!({
        "id": parent_response_id,
        "status": "completed",
        "output": [{"type": "message", "text": "first answer"}],
    });
    let mut parent = session(parent_response_id.clone(), parent_final_response.clone());
    parent.committed_items = vec![parent_request.clone(), parent_output.clone()];
    let parent_record = turn_record(
        TurnId::from_string("turn_parent"),
        parent_response_id.clone(),
        None,
        TurnOutcome::Completed,
        vec![parent_request],
        vec![parent_output],
    );
    store
        .commit_completed(parent.clone(), parent_record.clone())
        .await
        .expect("parent commit");

    let stored_parent = session_get(&store, &parent_response_id)
        .await
        .expect("stored parent");
    assert_eq!(stored_parent, parent);
    assert_eq!(
        store
            .get_response_json(&parent_response_id)
            .await
            .expect("parent response lookup"),
        Some(parent_final_response)
    );
    assert_eq!(
        turn_get(&store, &parent_record.turn_id).await,
        Some(parent_record)
    );

    let final_response = json!({
        "id": child_response_id,
        "status": "completed",
        "output": [{"type": "message", "text": "second answer"}],
    });
    let child = Session {
        response_id: child_response_id.clone(),
        parent_response_id: Some(parent_response_id.clone()),
        tenant_id: TenantId::from_string("tenant_test"),
        version: SessionVersion(2),
        committed_items: stored_parent
            .committed_items
            .iter()
            .cloned()
            .chain([child_item.clone(), child_output.clone()])
            .collect(),
        deterministic_fingerprint: "child-fingerprint".to_string(),
        final_response_json: final_response.clone(),
    };
    let child_record = turn_record(
        TurnId::from_string("turn_child"),
        child_response_id.clone(),
        Some(parent_response_id),
        TurnOutcome::Completed,
        vec![child_item],
        vec![child_output],
    );
    store
        .commit_completed(child.clone(), child_record.clone())
        .await
        .expect("child commit");

    let stored_child = session_get(&store, &child_response_id)
        .await
        .expect("stored child");
    assert_eq!(
        stored_child.parent_response_id,
        Some(ResponseId::from_string("resp_parent"))
    );
    assert_eq!(stored_child.committed_items, child.committed_items);
    assert_eq!(stored_child.final_response_json, final_response);
    assert_eq!(
        store
            .get_response_json(&child_response_id)
            .await
            .expect("child response lookup"),
        Some(child.final_response_json)
    );
    assert_eq!(
        turn_get(&store, &child_record.turn_id).await,
        Some(child_record)
    );
    assert_eq!(store.count().await, 2);
}

fn session(response_id: ResponseId, final_response_json: Value) -> Session {
    Session {
        response_id,
        parent_response_id: None,
        tenant_id: TenantId::from_string("tenant_test"),
        version: SessionVersion(1),
        committed_items: Vec::new(),
        deterministic_fingerprint: "fingerprint".to_string(),
        final_response_json,
    }
}

async fn session_get(store: &MemoryStore, response_id: &ResponseId) -> Option<Session> {
    SessionStore::get(store, response_id)
        .await
        .expect("session lookup")
}

async fn turn_get(store: &MemoryStore, turn_id: &TurnId) -> Option<TurnRecord> {
    TurnRecordStore::get(store, turn_id)
        .await
        .expect("turn lookup")
}

fn turn_record(
    turn_id: TurnId,
    response_id: ResponseId,
    parent_response_id: Option<ResponseId>,
    outcome: TurnOutcome,
    request_items: Vec<TaggedItem>,
    output_items: Vec<TaggedItem>,
) -> TurnRecord {
    TurnRecord {
        turn_id,
        response_id,
        parent_response_id,
        outcome,
        request_items,
        output_items,
        usage: UsageTotals {
            input_tokens: 3,
            output_tokens: 2,
            total_tokens: 5,
        },
        error_code: None,
        error_message: None,
        deterministic_fingerprint: Some("fingerprint".to_string()),
    }
}

fn message_item(role: MessageRole, text: &str) -> TaggedItem {
    let provenance = match role {
        MessageRole::System | MessageRole::Developer | MessageRole::User => {
            ProvenanceTag::user_trusted()
        }
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

fn assert_internal(error: ProxyError, expected_message: &str) {
    match error {
        ProxyError::Internal(message) => assert_eq!(message, expected_message),
        other => panic!("expected internal error, got {other}"),
    }
}
