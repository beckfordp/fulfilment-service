# Fulfilment review fixes — Implementation Plan

## Phase 1 — Pure lifecycle (H1, H3, M1, M2, L2)
- [ ] Write failing tests: `FulfilmentLifecycleSuite` (transition table, command parsing, simulatedNext) + property "no command leaves a terminal state"; confirm failure.
- [ ] Implement `domain/FulfilmentLifecycle` (state, failure, commands, transition, simulatedNext); single `store.transition(orderId, command)` in both interpreters; routes parse commands at the boundary; exhaustive simulate-next.
- [ ] Task: Conductor - User Manual Verification 'Phase 1 — Pure lifecycle' (Protocol in workflow.md)

## Phase 2 — Effects and capabilities (H2, M5)
- [ ] Write failing tests: in-memory timestamps from `Clock` (TestControl epoch); simulate-next tracking from `UUIDGen`; envelope id/time from `UUIDGen`/`Clock`; confirm failure.
- [ ] Implement `Clock`/`UUIDGen` in store, routes, events; narrow bounds (`OrderClient.apply` → `Concurrent`).
- [ ] Task: Conductor - User Manual Verification 'Phase 2 — Effects' (Protocol in workflow.md)

## Phase 3 — Consumer and order lookup (M3, M4)
- [ ] Write failing tests: `OrderClient` no-shipping-address → `NoShippingAddress`; `PaymentAuthorizedHandlerSuite` (happy path, unknown order, no address, invalid orderId, redelivery idempotent); confirm failure.
- [ ] Implement `OrderLookup` ADT and extracted `PaymentAuthorizedConsumer.handle`.
- [ ] Task: Conductor - User Manual Verification 'Phase 3 — Consumer' (Protocol in workflow.md)

## Phase 4 — Low-severity cleanup (L1, L3)
- [ ] SQL state filter for `list` (Postgres suite covers); tech-stack versions.
- [ ] Task: Conductor - User Manual Verification 'Phase 4 — Cleanup' (Protocol in workflow.md)
