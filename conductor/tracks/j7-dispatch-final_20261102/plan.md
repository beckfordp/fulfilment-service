# Plan: Checkout Journey 7 — dispatch is final

## Phase 1: Carry the rejected state

- [x] Task: Failing tests for `InvalidTransition.from` and the handler flag
  (dispatched vs duplicate cancel) (Red). (0d82a54)
- [x] Task: Add `from` to `InvalidTransition`, populate it in `applyCommand`,
  update matches; flag `cancel_after_dispatch` in the consumer (Green). (0d82a54)
- [x] Task: `sbt scalafmtCheck test`.

## Phase 2: Reconciliation store and races

- [x] Task: Failing store/migration tests for the reconciliation table and
  first-wins flag (Red). (0d82a54)
- [x] Task: `V4__create_fulfilment_reconciliation.sql`; add
  `flagReconciliation`/`reconciliationReason` to both interpreters (Green). (0d82a54)
- [x] Task: Postgres abort-after-dispatch test; barrier-based concurrent
  cancel vs dispatch test asserting one winner and one outbox event.
- [x] Task: `sbt scalafmtCheck test`.
- [x] Task: Conductor - User Manual Verification 'Dispatch is final'
  (Protocol in workflow.md)

## Review

- Reviewed 2026-11-02: plan compliance yes; style pass; tests cover the
  carried `from` state, the dispatched flag, duplicate cancel, in-memory and
  Postgres races, and the reconciliation table. Full suite 88 passed. No
  blocking findings.
