# Plan — fulfilment-service: REST docs convention (gluon TD-20.5)

## Phase 1: purerestlib 0.7.3
- [ ] Task: bump `purerestlibVersion` 0.4.0 → 0.7.3 (+ aligned pins); fix API drift until suite green; sync tech-stack.md
- [ ] Task: Conductor - User Manual Verification 'Phase 1: purerestlib 0.7.3' (Protocol in workflow.md) — covered by suite

## Phase 2: Per-endpoint error outputs
- [ ] Task: tests first — `FulfilmentDocsSuite` walks each endpoint's errorOutput statuses (FR2 subsets, red) → split the shared `errorOutput` into per-endpoint `oneOf`s over the shared variants; existing error-response tests stay green
- [ ] Task: Conductor - User Manual Verification 'Phase 2: Per-endpoint error outputs' (Protocol in workflow.md)

## Phase 3: Descriptions and examples — BLOCKED on user approval of the spec Appendix
- [ ] Task: coverage test first in `FulfilmentDocsSuite` (`Docs.undocumented == Nil` over all served endpoints, red) → approved summaries/descriptions/tags, param descriptions/examples, DTO/Address/ErrorResponse annotations + examples; green
- [ ] Task: Conductor - User Manual Verification 'Phase 3: Descriptions and examples' (Protocol in workflow.md)

## Phase 4: Smoke test
- [ ] Task: `scripts/verify-rest-docs.sh` — run service, fetch `/docs/docs.yaml`, assert summaries/descriptions/examples + narrowed errors; scalafmtCheck; CI green
- [ ] Task: Conductor - User Manual Verification 'Phase 4: Smoke test' (Protocol in workflow.md)
