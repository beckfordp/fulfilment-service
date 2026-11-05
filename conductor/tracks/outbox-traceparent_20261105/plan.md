# Plan: traceparent into the outbox (gluon PERF-5.4)

## Phase 1: Write trace_parent
- [x] Task: purerestlib/purekafka → 0.11.0 in `build.sbt` af28304
- [x] Task: Test: outbox row inside a span carries its traceparent; outside, NULL af28304
- [x] Task: `insertOutboxEvent` + `trace_parent`; `insertOutbox` helper; store takes the tracer; `Main` passes it af28304
- [x] Task: Full `sbt scalafmtCheck test` (97 tests, green)
- [ ] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md):
      redeploy, row and Kafka header carry the traceparent
