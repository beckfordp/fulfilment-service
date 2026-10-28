# Specification: fulfilment-service core (US-17.1–17.3, 18.1)

## Overview

New fulfilment-service: consume `PaymentAuthorized`, create a pick/pack task
for the order (snapshotting items + shipping address from order-service), drive
it through `PENDING → PICKING → PACKED → DISPATCHED` (or `DISPATCH_FAILED`),
and publish `DispatchConfirmed`/`DispatchFailed` through the transactional
outbox. Operator REST drives tasks; a config-gated `simulate-next` endpoint
gives deterministic demos.

## Functional Requirements

- **FR-1 — Task creation.** Consume `payment-events: PaymentAuthorized`
  (null-safe, envelope/eventType filtered, DLQ-guarded); look up the order via
  order-service `GET /orders/{orderId}` (resilience-wrapped client); create one
  PENDING task with an items + shipping-address snapshot. Idempotent per
  orderId. A 404 order is logged/skipped; transient order-service failures use
  the DLQ retry path.
- **FR-2 — State machine.** Persist `fulfilment_task` (orderId PK, state, JSONB
  items/address, trackingNo/carrier/failureReason, timestamps). Allowed:
  PENDING→PICKING→PACKED→DISPATCHED; any non-terminal→DISPATCH_FAILED; terminal
  states never transition. Invalid transitions return 409.
- **FR-3 — Operator REST.** `GET /fulfilment-tasks` (list, optional state
  filter), `POST /fulfilment-tasks/{orderId}/start-picking|pack|dispatch|fail`.
  Dispatch requires trackingNo+carrier; fail requires a reason (400 on blank).
  Unknown order 404.
- **FR-4 — Simulate next (dev-only).** `POST /fulfilment-tasks/{orderId}/simulate-next`,
  registered only when `fulfilment.simulation.enabled`; advances one legal
  step, generates a `GLN-…` tracking number + default carrier, or forces
  `DispatchFailed` with a supplied reason; 409 on terminal.
- **FR-5 — Events.** Dispatch writes `DispatchConfirmed {orderId, trackingNo,
  items}` (full item snapshot) and failure writes `DispatchFailed {orderId,
  reason}`, each in the same transaction as the state change, envelope
  correlated by orderId; AsyncAPI registers Subscribe `PaymentAuthorized` and
  Publish(Cdc) both events; schemaRegistryCheck passes; Debezium connector +
  local k8s infra added.
- **FR-6 — Live proof.** Authorized order → task PENDING; drive to DISPATCHED →
  one `DispatchConfirmed` on `fulfilment-events`; forced failure likewise
  produces `DispatchFailed`.

## Non-Functional Requirements

- No new runtime deps; PostgreSQL + skunk/Flyway; tests via Testcontainers.
- Simulation endpoint gated off by default outside local config.
- `sbt scalafmtCheck test` green.

## Acceptance Criteria

- Tasks are idempotent, transitions validated, events emitted exactly on
  terminal actions, AsyncAPI/registry aligned, live script passes.
- gshop's prototype UI can later point at these endpoints unchanged.

## Out of Scope

- gshop admin wiring, order/payment/notification consumers (separate tracks),
  US-20.4 cancellation, real warehouse/carrier integration.
