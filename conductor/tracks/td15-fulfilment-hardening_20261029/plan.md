# TD-15.5 Implementation Plan

## Phase 1 — Typed OrderId + client Uri
- [ ] Failing tests: OrderId suite, malformed route id → 400, client config; implement domain type, stores, routes, consumer, OrderClient.
- [ ] Task: Conductor - User Manual Verification 'Phase 1 — Typed ids & config' (Protocol in workflow.md)

## Phase 2 — Observability, asyncApiHtmlDir, ScalaCheck, narrowing
- [ ] Logging redaction/unknown-event log; `asyncApiHtmlDir`; ScalaCheck state-machine properties; narrow bounds.
- [ ] Task: Conductor - User Manual Verification 'Phase 2 — Purity' (Protocol in workflow.md)

## Phase 3 — Verification
- [ ] Full suite + coverage + scalafmt; live `verify-fulfilment.sh`.
- [ ] Task: Conductor - User Manual Verification 'Phase 3 — Verification' (Protocol in workflow.md)
