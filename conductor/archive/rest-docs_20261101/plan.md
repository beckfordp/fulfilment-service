# Plan — fulfilment-service: REST docs convention (gluon TD-20.5)

## Phase 1: purerestlib 0.7.3
- [x] Task: bump `purerestlibVersion` 0.4.0 → 0.7.3 (+ aligned pins); fix API drift until suite green; sync tech-stack.md — tapir/purekafka/otel4s already aligned, no drift, 68/68 ba97a08
- [x] Task: Conductor - User Manual Verification 'Phase 1: purerestlib 0.7.3' (Protocol in workflow.md) — covered by suite

## Phase 2: Per-endpoint error outputs
- [x] Task: tests first — `FulfilmentDocsSuite` walks each endpoint's errorOutput statuses (FR2 subsets, green) → the shared `errorOutput` split into per-endpoint `oneOf`s over the shared variants (`invalidRequestOutput`, `notFoundOrInvalidTransitionOutput`, `fullErrorOutput`); existing error-response tests stay green — fc08517
- [x] Task: Conductor - User Manual Verification 'Phase 2: Per-endpoint error outputs' (Protocol in workflow.md) — covered by suite

## Phase 3: Descriptions and examples (Appendix approved 2026-11-01)
- [x] Task: coverage test first in `FulfilmentDocsSuite` (`Docs.undocumented == Nil` over all served endpoints) → approved summaries/descriptions/tags, param descriptions/examples, DTO/Address/ErrorResponse annotations + examples; green — red 61 gaps → 0; 70/70 2921a49
- [x] Task: Conductor - User Manual Verification 'Phase 3: Descriptions and examples' (Protocol in workflow.md) — covered by `FulfilmentDocsSuite` + Phase 4's verify script

## Phase 4: Smoke test
- [x] Task: `scripts/verify-rest-docs.sh` — run service, fetch `/docs/docs.yaml`, assert summaries/descriptions/examples + narrowed errors; scalafmtCheck; CI green — PASS 72 checks; scalafmtCheckAll + 70/70 d3aa22a
- [x] Task: Conductor - User Manual Verification 'Phase 4: Smoke test' (Protocol in workflow.md) — `scripts/verify-rest-docs.sh` PASS

## Outcome (2026-11-01)

The user approved the spec Appendix exactly as proposed. All 61
`Docs.undocumented` gaps are closed: 8 summaries, 8 operation descriptions,
6 parameter descriptions, 14 non-2xx response descriptions, 20 body schema
property descriptions and 5 body examples. Per-endpoint error outputs list
only reachable variants. `FulfilmentDocsSuite` asserts zero gaps, the full
suite is 70/70, `scalafmtCheckAll` is clean, and
`scripts/verify-rest-docs.sh` passes 72 served-spec checks (it starts the
service with `FULFILMENT_SIMULATION_ENABLED=true` so `simulate-next` is in
the spec).

## Phase: Review Fixes
- [x] Task: Apply review suggestions a944cef
