# Fulfilment review fixes — Implementation Plan

## Phase 1 — Pure lifecycle (H1, H3, M1, M2, L2) [checkpoint: 0683d43]
- [x] Write failing tests: `FulfilmentLifecycleSuite` (transition table, command parsing, simulatedNext) + property "no command leaves a terminal state"; confirm failure. — 7dda4b4 (red: compile, types missing)
- [x] Implement `domain/FulfilmentLifecycle` (state, failure, commands, transition, simulatedNext); single `store.transition(orderId, command)` in both interpreters; routes parse commands at the boundary; exhaustive simulate-next. — 7dda4b4 (also: in-memory transition timestamps via `Clock`; test-only `StoreCommands` shorthands)
- [x] Task: Conductor - User Manual Verification 'Phase 1 — Pure lifecycle' (Protocol in workflow.md) — covered by suite (61 green, incl. Postgres testcontainers)

## Phase 2 — Effects and capabilities (H2, M5) [checkpoint: 3166e41]
- [x] Write failing tests: in-memory timestamps from `Clock` (TestControl epoch); simulate-next tracking from `UUIDGen`; envelope id/time from `UUIDGen`/`Clock`; confirm failure. — 5fa6468 (3 red)
- [x] Implement `Clock`/`UUIDGen` in store, routes, events; narrow bounds (`OrderClient.apply` → `Concurrent`). — 5fa6468 (in-memory store `Concurrent`+`Clock`+`UUIDGen`; added cats-effect-testkit, tech-stack noted)
- [x] Task: Conductor - User Manual Verification 'Phase 2 — Effects' (Protocol in workflow.md) — covered by suite (64 green)

## Phase 3 — Consumer and order lookup (M3, M4)
- [x] Write failing tests: `OrderClient` no-shipping-address → `NoShippingAddress`; `PaymentAuthorizedHandlerSuite` (happy path, unknown order, no address, invalid orderId, redelivery idempotent); confirm failure. — 58ffebd (red: compile)
- [x] Implement `OrderLookup` ADT and extracted `PaymentAuthorizedConsumer.handle`. — 58ffebd
- [x] Task: Conductor - User Manual Verification 'Phase 3 — Consumer' (Protocol in workflow.md) — covered by suite (70 green)

## Phase 4 — Low-severity cleanup (L1, L3)
- [ ] SQL state filter for `list` (Postgres suite covers); tech-stack versions.
- [ ] Task: Conductor - User Manual Verification 'Phase 4 — Cleanup' (Protocol in workflow.md)
