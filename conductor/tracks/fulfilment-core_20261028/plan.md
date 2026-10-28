# Plan: fulfilment-service core (US-17.1–17.3, 18.1)

## Phase 1: Domain + schema

- [ ] Task: Failing tests for the state machine — legal/illegal transitions, terminal rejection, dispatch/fail field validation; event envelope builders (`DispatchConfirmed` full item snapshot, `DispatchFailed` reason) (Red)
- [ ] Task: Implement `FulfilmentTask`, `FulfilmentState`, transition ADT errors, `FulfilmentEvents`, config (`order-client` resilience + `simulation.enabled`), and migration `V1` (`fulfilment_task` + `outbox_events`) replacing the scaffold (Green)
- [ ] Task: Refactor, `sbt scalafmtCheck test` green
- [ ] Task: Conductor - User Manual Verification 'Domain + schema' (Protocol in workflow.md)

## Phase 2: Store

- [ ] Task: Failing store tests (in-memory + Postgres): create is idempotent per orderId; each transition persists and, for dispatch/fail, writes the matching outbox row in the same transaction; invalid transitions return `InvalidTransition`; list filters by state (Red)
- [ ] Task: Implement `FulfilmentStore` (replace generated CRUD) in both interpreters (Green)
- [ ] Task: Refactor, tests green
- [ ] Task: Conductor - User Manual Verification 'Store' (Protocol in workflow.md)

## Phase 3: Order client + PaymentAuthorized consumer

- [ ] Task: Failing tests for `OrderClient.get` (200 parse, 404 → not-found, transient → error) and the consumer (valid event creates one PENDING task with the snapshot; duplicate event no-ops; unknown order skipped; malformed logged) (Red)
- [ ] Task: Implement the resilience-wrapped `OrderClient` and `PaymentAuthorizedConsumer` (null-safe, DLQ-guarded, group `fulfilment-service-payment-events`), wire in `KafkaConsumers`/`Main` (Green)
- [ ] Task: Refactor, full suite green
- [ ] Task: Conductor - User Manual Verification 'Order client + consumer' (Protocol in workflow.md)

## Phase 4: Operator REST + simulate-next

- [ ] Task: Failing `FulfilmentRoutesSuite`: list (with state filter), start-picking/pack/dispatch/fail happy paths, 400 validation, 404 unknown, 409 invalid transition; simulate-next gated by config, advances one step, generates tracking, forceFailure path, 409 terminal (Red)
- [ ] Task: Implement `FulfilmentRoutes` + conditional simulate endpoint, replacing the generated CRUD, and wire `AppRoutes`/`Main` (Green)
- [ ] Task: Refactor, full suite green
- [ ] Task: Conductor - User Manual Verification 'Operator REST + simulate-next' (Protocol in workflow.md)

## Phase 5: AsyncAPI, infra, live proof

- [ ] Task: `AsyncApiChannels` (Subscribe `PaymentAuthorized`; Publish(Cdc) `DispatchConfirmed`/`DispatchFailed`); `schemaRegistryCheck`; Debezium connector + `postgres-fulfilment.yaml` + `environments/local/fulfilment-service.values.yaml` + `bin/k8s-local-up`
- [ ] Task: Update `gluon/docs/system-design.md` (service status, contracts incl. items snapshot) and README/product/tech-stack
- [ ] Task: Rebuild/redeploy; `scripts/verify-fulfilment.sh` — authorized order leaves a PENDING task, simulate to DISPATCHED → one `DispatchConfirmed`, forced failure → `DispatchFailed`, cleanup
- [ ] Task: Conductor - User Manual Verification 'AsyncAPI, infra, live proof' (Protocol in workflow.md)

## Risks / notes

- order-service `GET /orders/{id}` is the snapshot source; a transient failure uses the consumer DLQ path, a 404 is logged and skipped.
- Items are stored as JSONB and echoed as a full snapshot in `DispatchConfirmed`.
- Simulation is registered only when `fulfilment.simulation.enabled` (local values true); no scheduler.
- gshop admin wiring and order/payment/notification consumers are follow-up tracks.
