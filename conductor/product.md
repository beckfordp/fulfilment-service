# Product Guide — fulfilment-service

## Context
Full cross-repo vision lives in the `gluon` platform repo, not here:
[`../../../docs/product.md`](../../../docs/product.md),
[`../../../docs/system-design.md`](../../../docs/system-design.md),
[`../../../docs/user-stories.md`](../../../docs/user-stories.md),
[`../../../docs/PLAN.md`](../../../docs/PLAN.md) (Checkout Journey 3).

## What this service does
Track A pick/pack/dispatch for paid orders: it consumes
`payment-events: PaymentAuthorized`, creates a pick/pack task for the order,
tracks `PENDING -> PICKING -> PACKED -> DISPATCHED`, and publishes
`fulfilment-events: DispatchConfirmed` (with tracking number + items) or
`DispatchFailed` (with a reason). Downstream, order-service moves the order to
`Dispatched`/`DispatchFailed`, payment-service voids the authorization on
failure, and notification-service sends the shipped/failed email.

The repo was scaffolded from `pure-service-generator` with a generic CRUD
placeholder (removed by the first track). No field-spec was used — the task
model (state machine, JSONB items, events) isn't flat codegen material (see
TD-12 in `gluon/TECHNICAL_DEBT.md`).

## Domain model
- **Fulfilment task** — one per `orderId`: current state, order snapshot
  (items + shipping address, fetched from order-service when the task is
  created), timestamps, and on completion `trackingNo`/`carrier` or
  `failureReason`.
- **Operator API** — the internal endpoints the gshop admin fulfilment screen
  uses to list tasks and drive transitions (decided in the US-17 track).

## User stories in scope
From `../../../docs/user-stories.md`:
- **US-17.1** — consume `PaymentAuthorized`, create the pick/pack task
- **US-17.2** — state machine `PENDING -> PICKING -> PACKED -> DISPATCHED`
- **US-17.3** — publish `DispatchConfirmed {orderId, trackingNo, items}`
- **US-18.1** — publish `DispatchFailed {orderId, reason}`
- Later: **US-20.4** — cancel in-flight tasks on `PaymentAuthorizationExpired`
  (Checkout Journey 5).

## Sequencing (`../../../PLAN.md`)
Checkout Journey 3; depends on Journey 1's `PaymentAuthorized`. Its
`DispatchConfirmed` unlocks Journey 4's capture; Journey 5 cancels tasks on
authorization expiry.

## Events
- Consumes: `payment-events` (`PaymentAuthorized`).
- Publishes: `fulfilment-events` (`DispatchConfirmed`/`DispatchFailed`) via the
  transactional outbox + Debezium (ADR 0008/0009), shared envelope (ADR 0009).

## Out of scope
- Real warehouse/carrier integrations; stock/inventory decisions (order-service
  releases reservations); payment capture (payment-service); customer identity.
