mod support;

use axum::body::Body;
use axum::response::Response;
use ktxd::ids::ResponseId;
use ktxd::responses::{output_text_delta_event, sse_frame};
use serde_json::Value;

#[tokio::test]
async fn sse_frame_round_trips_through_shared_collector() {
    let event = output_text_delta_event(
        &ResponseId::from_string("resp_test"),
        "item_test",
        0,
        "hello\nworld",
    );
    let body = sse_frame(&event);
    let collected = support::collect_sse_response(Response::new(Body::from(body)))
        .await
        .expect("generated frame should parse through the shared collector");

    assert_eq!(collected.frames.len(), 1);
    let frame = &collected.frames[0];
    assert_eq!(frame.event.as_deref(), Some(event.name.as_str()));
    assert_eq!(frame.payload.as_ref(), Some(&event.data));
    assert_eq!(
        frame.json::<Value>().expect("frame data should be JSON"),
        event.data
    );
    assert!(frame.terminated);
    assert!(collected.terminal_blank_line);
}
