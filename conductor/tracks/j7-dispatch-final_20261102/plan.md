# Plan: Checkout Journey 7 — dispatch is final

## Phase 1: Carry the rejected state

- [ ] Task: Failing tests for `InvalidTransition.from` and the handler flag
  (dispatched vs duplicate cancel) (Red).
- [ ] Task: Add `from` to `InvalidTransition`, populate it in `applyCommand`,
  update matches; flag `cancel_after_dispatch` in the consumer (Green).
- [ ] Task: `sbt scalafmtCheck test`.

## Phase 2: Reconciliation store and races

- [ ] Task: Failing store/migration tests for the reconciliation table and
  first-wins flag (Red).
- [ ] Task: `V4__create_fulfilment_reconciliation.sql`; add
  `flagReconciliation`/`reconciliationReason` to both interpreters (Green).
- [ ] Task: Postgres abort-after-dispatch test; barrier-based concurrent
  cancel vs dispatch test asserting one winner and one outbox event.
- [ ] Task: `sbt scalafmtCheck test`.
- [ ] Task: Conductor - User Manual Verification 'Dispatch is final'
  (Protocol in workflow.md)

## Review

- Filled after `/conductor:review`.
