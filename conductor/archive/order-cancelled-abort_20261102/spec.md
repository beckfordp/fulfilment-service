# Specification: US-9.5 — abort a non-dispatched task on `OrderCancelled`

## Overview

Journey 5's expiry path and US-9's shared cancellation path need
fulfilment-service to stop an in-flight pick/pack task when order-service
publishes `order-events: OrderCancelled`. Add a terminal `CANCELLED` state
reachable from `PENDING`/`PICKING`/`PACKED`; `DISPATCHED` and
`DISPATCH_FAILED` stay final.

## Functional Requirements

- FR-1: `FulfilmentState` gains `Cancelled` (`"CANCELLED"`); V3 alters the
  state CHECK constraint (V1 is never edited).
- FR-2: `FulfilmentCommand.Cancel` maps `PENDING`/`PICKING`/`PACKED ->
  CANCELLED`; any terminal state rejects it. `simulatedNext` returns `None`
  for `CANCELLED`.
- FR-3: a new `order-events` consumer (group
  `fulfilment-service-order-events`, DLQ `order-events-dlq`) decodes the
  envelope and calls `store.transition(orderId, Cancel)`. `NotFound` and
  `InvalidTransition` are logged no-ops; a store error is retried/DLQ'd.
- FR-4: cancelling publishes no fulfilment event.
- FR-5: AsyncAPI registers the `OrderCancelled` subscription.

## Non-Functional Requirements

- Both store interpreters share the pure lifecycle; no duplicate transition
  logic.
- `sbt scalafmtCheck test` and `schemaRegistryCheck` green.

## Acceptance Criteria

- A synthetic `OrderCancelled` moves PENDING/PICKING/PACKED to `CANCELLED`.
- A `DISPATCHED` or `DISPATCH_FAILED` task is untouched.
- A duplicate cancel is a no-op; malformed input dead-letters.

## Out of Scope

gshop rendering (separate track), US-21 dispatch race, payment/inventory.
