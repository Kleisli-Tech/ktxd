# Test Harness

The test harness is intentionally hermetic. Component tests use the real
`Arc<ktxd::session::MemoryStore>` because `TurnDriver` owns that concrete store,
while upstream, seed-resolution, and node-commit boundaries use the doubles in
`tests/support/doubles.rs`.

Fast pure behavior belongs beside its implementation in an inline
`#[cfg(test)] mod tests`; cross-module and public-router behavior belongs under
`tests/`. Shared JSON or provider wire data belongs under `tests/fixtures/`, and
small one-off values stay inline in the test that uses them.

HTTP tests should use `collect_sse_response` when validating status, headers,
body bytes, or terminal framing; use `collect_sse_frames` when only parsed SSE
events are relevant. The helpers preserve event names, data payloads, frame
order, and terminal blank-line information. Typed driver tests should use the
production `VecEventSink`.

Baseline recorded on July 28, 2026: before this harness was added, `cargo test`
passed with zero unit, integration, and doc tests. The harness smoke test makes
support compilation explicit while the remaining suites are added.
