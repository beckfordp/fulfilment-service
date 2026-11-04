# Spec: Journey telemetry (gluon PERF-4.1–4.3, 4.7, 4.11)

## Overview
gluon PLAN.md Performance 4 for fulfilment-service: adopt purerestlib/purekafka 0.8.0 and
expose the *people* backlog (dispatch queue) separately from consumer lag — the Q13/Q14
signal for the Christmas Rush.

## Functional Requirements
1. Bump purerestlib + purekafka to 0.8.0; `Main` passes `runtime.metrics` (PERF-4.1).
2. `PoolMetrics` on the pool `fulfilment` (PERF-4.3).
3. `EventAge` in both consumers (payment-events, order-events) (PERF-4.2).
4. `FulfilmentJourneyMetrics` store decorator (PERF-4.7): `fulfilment.tasks.created`;
   `fulfilment.transitions{from,to}` (dispatch rate = rate of `to=DISPATCHED`);
   `fulfilment.step.duration{from,to}` (s, time spent in `from`, read via `get` before
   the transition). `Milestones` line `order_dispatched` / `fulfilment_<to>` (PERF-4.11).
5. Backlog gauges: new store query `backlog` (count + oldest `updated_at` per open state
   PENDING/PICKING/PACKED) → observable gauges `fulfilment.backlog.size{state}` and
   `fulfilment.backlog.oldest_age{state}` (s).

## Non-Functional Requirements
- No order ids on labels. Coverage > 80% on new code. scalafmt clean.

## Acceptance Criteria
- Decorator + backlog tests (in-memory and Postgres). Live: metrics port shows the series.

## Out of Scope
- Outbox gauge (PERF-4.9): Debezium `MilliSecondsBehindSource` instead. Consumer spans (Perf 5).
