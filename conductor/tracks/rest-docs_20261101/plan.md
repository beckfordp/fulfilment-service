# Plan — fulfilment-service: REST docs convention (gluon TD-20.5)

## Phase 1: purerestlib 0.7.3
- [x] Task: bump `purerestlibVersion` 0.4.0 → 0.7.3 (+ aligned pins); fix API drift until suite green; sync tech-stack.md — tapir/purekafka/otel4s already aligned, no drift, 68/68 ba97a08
- [x] Task: Conductor - User Manual Verification 'Phase 1: purerestlib 0.7.3' (Protocol in workflow.md) — covered by suite

## Phase 2: Per-endpoint error outputs
- [x] Task: tests first — `FulfilmentDocsSuite` walks each endpoint's errorOutput statuses (FR2 subsets, green) → the shared `errorOutput` split into per-endpoint `oneOf`s over the shared variants (`invalidRequestOutput`, `notFoundOrInvalidTransitionOutput`, `fullErrorOutput`); existing error-response tests stay green — fc08517
- [x] Task: Conductor - User Manual Verification 'Phase 2: Per-endpoint error outputs' (Protocol in workflow.md) — covered by suite

## Phase 3: Descriptions and examples — BLOCKED on user approval of the spec Appendix
- [ ] Task: coverage test first in `FulfilmentDocsSuite` (`Docs.undocumented == Nil` over all served endpoints) → approved summaries/descriptions/tags, param descriptions/examples, DTO/Address/ErrorResponse annotations + examples; green — RED 61 gaps 2026-11-01 (suite left uncommitted); awaiting Appendix approval
- [ ] Task: Conductor - User Manual Verification 'Phase 3: Descriptions and examples' (Protocol in workflow.md)

## Phase 4: Smoke test
- [ ] Task: `scripts/verify-rest-docs.sh` — run service, fetch `/docs/docs.yaml`, assert summaries/descriptions/examples + narrowed errors; scalafmtCheck; CI green
- [ ] Task: Conductor - User Manual Verification 'Phase 4: Smoke test' (Protocol in workflow.md)

## Preparation status (2026-11-01)

Approval-safe work done: purerestlib bump (ba97a08), per-endpoint error
outputs (fc08517), `FulfilmentDocsSuite` added and run. Red count: 61 gaps —
8 summaries, 8 operation descriptions, 6 parameter descriptions (5× path
`orderId`, 1× query `state`), 14 non-2xx response descriptions, 20 body
schema property descriptions (TaskResponse ×9, Address ×5, ErrorResponse ×1,
DispatchRequest ×2, FailRequest ×1, SimulateRequest ×2), and 5 body examples
(TaskResponse, ErrorResponse, DispatchRequest, FailRequest, SimulateRequest).
`FulfilmentItem` fields already carry `@description`/`@encodedExample`.

No description annotation has been applied to code. The red
`FulfilmentDocsSuite` is intentionally uncommitted until the Appendix is
approved.
