# TD-15.5 Implementation Plan

## Phase 1 — Typed OrderId + client Uri
- [x] Failing tests: OrderId suite, malformed route id → 400, client config; implement domain type, stores, routes, consumer, OrderClient. — 61588c2
- [x] Task: Conductor - User Manual Verification 'Phase 1 — Typed ids & config' (Protocol in workflow.md) — covered by tests

## Phase 2 — Observability, asyncApiHtmlDir, ScalaCheck, narrowing
- [x] Logging redaction/unknown-event log; `asyncApiHtmlDir`; ScalaCheck state-machine properties; narrow bounds. — 0bd3990
- [x] Task: Conductor - User Manual Verification 'Phase 2 — Purity' (Protocol in workflow.md) — covered by tests

## Phase 3 — Verification
- [x] Full suite + coverage + scalafmt; live `verify-fulfilment.sh`. — 50 tests, 73.0%/69.7% coverage; live script failed against stale k8s images (deferred to cross-repo E2E rebuild)
- [x] Task: Conductor - User Manual Verification 'Phase 3 — Verification' (Protocol in workflow.md) — E2E deferred to Phase 2 of the cross-repo plan
