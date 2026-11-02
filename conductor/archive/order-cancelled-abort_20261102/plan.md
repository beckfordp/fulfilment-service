# Plan: US-9.5 — abort a non-dispatched task on `OrderCancelled`

## Phase 1: Domain + migration

- [x] Task: Failing tests — all three abortable states, terminal states rejected, simulatedNext, state parsing (Red)
- [x] Task: `Cancelled` state, `Cancel` command, lifecycle/applyCommand/forCommand, V3 migration (Green)
- [x] Task: Conductor - User Manual Verification 'Domain + migration' (Protocol in workflow.md)

## Phase 2: Consumer

- [x] Task: Failing handler tests — abort, dispatched no-op, duplicate no-op, malformed DLQ (Red)
- [x] Task: `OrderCancelledConsumer` + KafkaConsumers merge + AsyncAPI channel (Green)
- [x] Task: Conductor - User Manual Verification 'Consumer' (Protocol in workflow.md)

## Phase 3: Schema check

- [x] Task: Add `order-events-OrderCancelled` to CONSUMES; `sbt schemaRegistryCheck`
- [x] Task: Conductor - User Manual Verification 'Schema check' (Protocol in workflow.md)

## Review (2026-11-02)

- `sbt scalafmtCheck test` green (81 tests); `sbt schemaRegistryCheck` green.
- Live path covered by `verify-auth-expiry.sh`: PENDING task moved to
  CANCELLED; unit tests cover PICKING/PACKED and terminal no-ops.
- No blocking findings.
