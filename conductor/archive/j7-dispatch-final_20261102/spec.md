# Specification: Checkout Journey 7 — dispatch is final

## Overview

`FulfilmentLifecycle` already makes `DISPATCHED` terminal (US-21.3), so an
`OrderCancelled` arriving after dispatch is rejected. The consumer only logs
it, so a cancellation that lost the race leaves no manual-reconciliation
record. This track carries the current state on the rejection, flags
`cancel_after_dispatch`, and adds the missing Postgres/concurrency tests.

## Functional Requirements

- `FulfilmentFailure.InvalidTransition` carries `from: Option[FulfilmentState]`;
  `FulfilmentTask.applyCommand` populates it. Route-decoded errors keep `None`.
- `Cancel` from `DISPATCHED` → flag `cancel_after_dispatch`, log, commit
  (no DLQ). `Cancel` on `CANCELLED`/`DISPATCH_FAILED` stays a benign no-op.
- `fulfilment_reconciliation(order_id uuid primary key references
  fulfilment_task(order_id) on delete cascade, reason text, created_at)`;
  `flagReconciliation`/`reconciliationReason`, first-wins (insert returns
  whether this call wrote the row).
- No transition-rule changes: `DISPATCHED`/`DISPATCH_FAILED`/`CANCELLED` stay
  terminal.

## Acceptance Criteria

- Handler tests: dispatched → flag; duplicate cancel → no flag; pending →
  no flag.
- Concurrent cancel vs dispatch (barrier on the in-memory store; two pooled
  sessions in Postgres) yields exactly one `Right`, the final state matches the
  winner, and only dispatch writes its outbox event.
- Postgres abort-after-dispatch and reconciliation rows covered; migration
  suite asserts the table.
- `sbt scalafmtCheck test` green.

## Out of Scope

Order/payment/inventory reconciliation (their tracks), gshop.
