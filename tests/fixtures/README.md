# Shared Fixtures

Keep reusable JSON fixtures here only when more than one test group consumes
them. Small values should remain inline with the test for readability.

Provider-specific wire fixtures belong under `tests/fixtures/upstream/` and
must document whether they are valid, malformed, or intentionally incompatible.
