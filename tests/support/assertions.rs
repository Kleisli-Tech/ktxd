#![allow(dead_code)]

use ktxd::domain::{Session, TaggedItem, TurnRecord};
use ktxd::responses::ResponseEvent;
use serde::Serialize;
use serde_json::Value;
use std::fmt::Display;

pub fn assert_id_prefix(id: impl Display, prefix: &str) {
    let id = id.to_string();
    assert!(
        id.starts_with(prefix),
        "expected ID {id:?} to start with {prefix:?}"
    );
}

pub fn assert_generated_id(id: impl Display, prefix: &str) {
    let id = id.to_string();
    assert_id_prefix(&id, prefix);
    let suffix = &id[prefix.len()..];
    assert_eq!(
        suffix.len(),
        32,
        "generated ID suffix should be 32 characters"
    );
    assert!(
        suffix
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "generated ID suffix should be lowercase hexadecimal: {suffix:?}"
    );
}

pub fn assert_timestamp_present(value: &Value, field: &str) {
    let timestamp = value
        .get(field)
        .unwrap_or_else(|| panic!("missing timestamp field {field:?}"));
    let timestamp = timestamp
        .as_i64()
        .unwrap_or_else(|| panic!("timestamp field {field:?} was not an integer: {timestamp}"));
    let now = chrono::Utc::now().timestamp();
    assert!((946_684_800..=now + 300).contains(&timestamp));
}

pub fn assert_response_id_reference(value: &Value, expected: impl AsRef<str>) {
    let expected = expected.as_ref();
    let actual = value
        .as_str()
        .or_else(|| value.get("id").and_then(Value::as_str))
        .or_else(|| value.get("response_id").and_then(Value::as_str))
        .or_else(|| {
            value
                .get("response")
                .and_then(|response| response.get("id"))
                .and_then(Value::as_str)
        })
        .unwrap_or_else(|| panic!("could not find a response ID in {value}"));
    assert_eq!(actual, expected);
}

pub fn assert_event_response_id(event: &ResponseEvent, expected: impl AsRef<str>) {
    let expected = expected.as_ref();
    let actual = match event.name.as_str() {
        "response.created" | "response.completed" | "response.incomplete" | "response.failed" => {
            event
                .data
                .get("response")
                .and_then(|response| response.get("id"))
                .and_then(Value::as_str)
        }
        "response.output_item.added"
        | "response.output_item.done"
        | "response.output_text.delta" => event.data.get("response_id").and_then(Value::as_str),
        name => panic!("unsupported event name for response-ID assertion: {name}"),
    }
    .unwrap_or_else(|| {
        panic!(
            "event {} has no response ID in its production field",
            event.name
        )
    });
    assert_eq!(actual, expected);
}

pub fn assert_fingerprint(value: impl AsRef<str>) {
    let value = value.as_ref();
    assert_eq!(
        value.len(),
        64,
        "fingerprint should be a SHA-256 hex digest"
    );
    assert!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "fingerprint should contain only hexadecimal characters: {value:?}"
    );
}

pub fn assert_optional_fingerprint(value: Option<&str>) {
    assert_fingerprint(value.expect("expected a deterministic fingerprint"));
}

pub fn assert_artifact_hash(value: impl AsRef<str>) {
    let value = value.as_ref();
    assert!(
        value.starts_with("blake3:"),
        "artifact hash should use the blake3: prefix: {value:?}"
    );
    assert_eq!(value.len(), "blake3:".len() + 64);
    assert!(
        value["blake3:".len()..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
}

pub fn assert_item_artifact_hash(item: &TaggedItem) {
    assert_artifact_hash(
        item.artifact_hash
            .as_ref()
            .expect("expected an artifact hash on the item")
            .to_string(),
    );
}

pub fn assert_record_fingerprint(record: &TurnRecord) {
    assert_optional_fingerprint(record.deterministic_fingerprint.as_deref());
}

pub fn assert_session_fingerprint(session: &Session) {
    assert_fingerprint(&session.deterministic_fingerprint);
}

pub fn sequence_numbers(events: &[ResponseEvent]) -> Vec<u64> {
    events
        .iter()
        .map(|event| {
            event
                .data
                .get("sequence_number")
                .and_then(Value::as_u64)
                .unwrap_or_else(|| panic!("event {} has no sequence_number", event.name))
        })
        .collect()
}

pub fn assert_contiguous_sequence_numbers(events: &[ResponseEvent]) {
    assert!(!events.is_empty(), "expected at least one response event");
    let numbers = sequence_numbers(events);
    assert_eq!(numbers, (0..numbers.len() as u64).collect::<Vec<_>>());
}

pub fn assert_serialized_id_reference<T: Serialize>(value: &T, expected: impl AsRef<str>) {
    let value = serde_json::to_value(value).expect("value should serialize");
    assert_response_id_reference(&value, expected);
}
