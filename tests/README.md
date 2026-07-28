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

The local upstream suite in `tests/upstream_contract.rs` uses the scripted
server in `tests/support/http.rs`. It binds only to an ephemeral loopback port,
records complete request headers and bodies, and serves queued responses. Test
clients disable environment-configured proxies, and environment-backed secrets
are scoped and serialized, so the suite cannot escape loopback or race over the
process environment. SSE integration cases use multiple socket writes, while
the production decoder is also tested at every individual byte boundary.

Baseline recorded on July 28, 2026: before this harness was added, `cargo test`
passed with zero unit, integration, and doc tests. The harness smoke test and
local upstream contract suite make support and HTTP behavior explicit.
