# Spec: fulfilment-service review fixes (gluon TD-16.3)

## Overview
Fix the findings from the 2026-11-01 full review of fulfilment-service main + test code
against `code_styleguides/scala-fp.md` (Review checklist) and `general.md`.
Baseline: 52 tests green at `ed0ddde`. No REST/Kafka wire change: the operator API
used by gshop's admin screen keeps every path, status code, body and error message.

## Findings

### High
- **H1 No pure lifecycle; rules duplicated.** Transition rules (PENDING→PICKING,
  PICKING→PACKED, PACKED→DISPATCHED, any non-terminal→DISPATCH_FAILED) are written
  twice, inline in both `FulfilmentStore` interpreters; no `transition` function
  (laws 1, 2).
- **H2 Side effects outside F.** `UUID.randomUUID()` in
  `FulfilmentRoutes.generatedTrackingNo`; `Instant.now()` (5x) inside the in-memory
  store, partly inside `Ref.modify` (law 7).
- **H3 `case _` over sealed `FulfilmentState`** in simulate-next; the "next simulated
  step" decision lives in the route shell (laws 1, 6).

### Medium
- **M1** `FulfilmentState`/`FulfilmentFailure` live outside `domain/` (purity not
  checkable by location).
- **M2** Request validation (trim + non-blank `trackingNo`/`carrier`/`reason`)
  duplicated in both stores, done inside the store instead of parsed once at the
  boundary (laws 1, 5).
- **M3** `OrderClient.get` returns `Option`, collapsing "order not found" and "order
  has no shipping address"; the consumer logs both as "unknown order" (law 4).
- **M4** `PaymentAuthorizedConsumer`'s per-event logic is inline in the Kafka stream,
  so it has no tests (invalid orderId, unknown order, happy path).
- **M5** Over-broad capabilities: `FulfilmentEvents` builders and the in-memory store
  take `Sync` (need `Clock`+`UUIDGen`); `OrderClient.apply` takes `Async` (needs
  `Concurrent`) (law 3).

### Low
- **L1** Postgres `list` reads every row and filters by state in Scala; filter in SQL.
- **L2** No ScalaCheck property that no command leaves a terminal state (law 8).
- **L3** `tech-stack.md` stale: purerestlib listed as 0.1.0 (is 0.4.0), purekafka as
  0.5.0 in Messaging (is 0.6.1).
- **L4** (skip) `.toOption.get` in test fixtures: test harness code, not service code.
- **L5** (skip) simulate-next reads the task then transitions; a concurrent operator
  step makes the store's atomic check return 409. Benign, documented.

## Functional Requirements
- FR-1 (H1, H3, M1, M2, L2): `domain/FulfilmentLifecycle.scala` holds
  `FulfilmentState`, `FulfilmentFailure`, `FulfilmentCommand` (smart constructors
  trim/validate once) and pure `transition`/`simulatedNext` (exhaustive). Stores
  expose one `transition(orderId, command)`; both interpreters call the pure core
  and one shared event mapping.
- FR-2 (H2, M5): time via `Clock`, ids via `UUIDGen`; narrowest bounds that compile.
- FR-3 (M3, M4): `OrderClient.get` returns an `OrderLookup` ADT; consumer per-event
  `handle` extracted and tested.
- FR-4 (L1, L3): SQL state filter; tech-stack versions.

## Acceptance Criteria
- Failing-first tests for each fix, now green; full suite green; scalafmtCheck green.
- Error messages/status codes byte-identical (existing route suite unchanged).

## Out of Scope
- US-20.4 (cancel on PaymentAuthorizationExpired); gshop admin screen wiring.
