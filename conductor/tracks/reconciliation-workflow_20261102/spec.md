# Specification: ADM-4 — reconciliation workflow

## Overview

fulfilment-service records first-wins `cancel_after_dispatch` flags with no
API or resolve action. This track adds the open/resolved queue and operator
resolve, per ADR 0026 and the contract in `gluon/docs/system-design.md`.

## Functional Requirements

- `V5__extend_fulfilment_reconciliation.sql`: status/operator/note/resolved_at
  columns, named status + resolution checks, `(status, created_at desc)` index.
- `domain/Reconciliation.scala`: status ADT/parse, row, pure resolve rule.
- `FulfilmentStore`: list newest first; `resolveReconciliation` conditional
  update in one transaction + same-session read → Resolved/AlreadyResolved/
  NotFound; first resolver wins. Insert-only OTel counter
  `reconciliation.flags.recorded`.
- `ReconciliationRoutes.scala`: `GET /reconciliations` (filters, 400 unknown
  status, empty on malformed orderId) and `PATCH /reconciliations/{orderId}`
  (typed UUID path, dev token required → 401, only `resolved` → 400, 404
  unknown, idempotent 200).
- Copy `OperatorAuth`; `operator-auth` config (`OPERATOR_TOKEN`/
  `OPERATOR_ID`) + startup warning; add `Authorization` to CORS; register
  routes in `AppRoutes`.
- Update stubs, `FulfilmentDocsSuite`, `MigrationsSuite`, config suite and
  `scripts/verify-rest-docs.sh`.

## Non-Functional
- TDD; `sbt scalafmtCheck test` green; >80% coverage on new code; no
  Kafka/registry changes.

## Out of Scope
- gshop UI (ADM-7); live demo (ADM-9); reopening resolved flags.
