# Spec: REST docs convention (gluon TD-20.5)

## Overview
fulfilment-service `/docs` shows bare paths and unexplained schemas. Apply the
TD-20 convention (purerest README "Documenting endpoints"), mirroring
order-service `rest-docs_20261101`, and assert it in a test. The endpoint set is
the one `AppRoutes` passes to `Docs.routes`: the five operator endpoints,
config-gated `simulate-next`, and health/ready.

## Functional Requirements
- FR1: purerestlib 0.4.0 → 0.7.3 (+ aligned pins); suite green; tech-stack.md
  synced.
- FR2: Per-endpoint error outputs — variants defined once, each endpoint's
  `oneOf` lists only what it can return: GET list 400 (unknown `state`);
  start-picking/pack 404, 409; dispatch/fail 400 (blank field), 404, 409;
  simulate-next 404, 409; health none; ready 503. Pin with a
  `FulfilmentDocsSuite` test walking each endpoint's `errorOutput` statuses.
- FR3: Every endpoint `.summary`/`.description`/`.tag` (`fulfilment-tasks`,
  `health`) — see Appendix.
- FR4: `orderId` path and `state` query parameters described + example. There
  are no header inputs.
- FR5: Every request/response DTO + field `@description`; each class an
  `@encodedExample` (incl. Address, ErrorResponse); body `.example` on every
  request/response (`FulfilmentItem` fields already carry both).
- FR6: Every error variant described + example body; the fixed not-found
  message shared between example and mapping.
- FR7: `FulfilmentDocsSuite`: `Docs.undocumented(all served endpoints) ==
  Nil` over the same list `AppRoutes` passes to `Docs.routes` (simulation
  enabled → 8 endpoints).
- FR8: `scripts/verify-rest-docs.sh` checks served `/docs/docs.yaml` for
  summaries/descriptions/examples and narrowed error statuses.

## Non-Functional
- Wire format unchanged (encode/decode identical); scalafmtCheck clean.
- Examples use UUID-shaped order ids and the real wire state values; no real
  ids, credentials or tokens.
- `simulate-next` remains config-gated; its docs describe it as demo-only.

## Acceptance Criteria
- Full suite green, zero `Docs.undocumented` gaps, verify script PASS, CI
  green.
- `/docs` no longer lists impossible errors per endpoint.
- The Appendix is approved by the user before any description annotation is
  applied.

## Out of Scope
- Behaviour changes (state machine, event payloads, outbox).
- AsyncAPI side (TD-19).
- Client-side mirrors (order-service/notification-service DTOs).
- Wiring gshop's admin fulfilment screen.

## Appendix: approved descriptions (2026-11-01)

Approved by the user on 2026-11-01 exactly as proposed below. These are now
applied to the code.

### Endpoints

Tag: `fulfilment-tasks` for the operator API, `health` for health/ready.

| Endpoint | summary | description |
|---|---|---|
| GET /fulfilment-tasks | List fulfilment tasks | Every pick/pack task, oldest first (createdAt). Optional `state` filter; an unknown value is 400. Read directly from Postgres — no cache, so results are always current. |
| POST /fulfilment-tasks/{orderId}/start-picking | Start picking a task | Moves the task PENDING → PICKING. No event published. 404 if no task exists; 409 from any other state (so repeating it while PICKING is a 409). |
| POST /fulfilment-tasks/{orderId}/pack | Mark a task packed | Moves the task PICKING → PACKED. No event published. 404 if no task exists; 409 from any other state. |
| POST /fulfilment-tasks/{orderId}/dispatch | Dispatch a task | Moves the task PACKED → DISPATCHED, records the tracking number and carrier, and publishes `DispatchConfirmed` through the transactional outbox. Blank `trackingNo`/`carrier` → 400. 404 if no task exists; 409 from any other state. Not idempotent — a repeat is 409. |
| POST /fulfilment-tasks/{orderId}/fail | Fail a task | Moves the task to DISPATCH_FAILED from PENDING, PICKING or PACKED, records the reason, and publishes `DispatchFailed` through the transactional outbox. Blank `reason` → 400. 404 if no task exists; 409 from a terminal state. |
| POST /fulfilment-tasks/{orderId}/simulate-next | Advance a task one step (simulation) | Demo-only; registered only when `fulfilment.simulation.enabled`. Advances PENDING → PICKING → PACKED → DISPATCHED, minting a `GLN-XXXXXXXX` tracking number and carrier `DHL Express`; with `forceFailure` it fails the task instead. 404 if no task exists; 409 from a terminal state. |
| GET /health | Liveness check | 200 while the process is up. |
| GET /health/ready | Readiness check | 200 when Postgres answers a ping, else 503. |

### Parameters / inputs

| Endpoint(s) | In | Name | description | example |
|---|---|---|---|---|
| start-picking, pack, dispatch, fail, simulate-next | path | `orderId` | Order id (UUID) the task was created for. A malformed id returns tapir's default 400. | `3f2b8c1e-7a4d-4e9b-9c2a-1d5e6f7a8b90` |
| GET /fulfilment-tasks | query | `state` (optional) | Filter by lifecycle state: `PENDING`, `PICKING`, `PACKED`, `DISPATCHED` or `DISPATCH_FAILED`. | `PENDING` |

No header inputs.

### DTO fields

`TaskResponse` — class: "A fulfilment task as stored: order snapshot,
lifecycle state and dispatch outcome." Class example:

```json
{
  "orderId": "3f2b8c1e-7a4d-4e9b-9c2a-1d5e6f7a8b90",
  "state": "DISPATCHED",
  "items": [
    {"sku": "SKU-0042", "productName": "Espresso cup", "quantity": 2, "unitPriceCents": 1250}
  ],
  "shippingAddress": {
    "line1": "1 High Street", "line2": "Flat 2", "city": "London",
    "postalCode": "SW1A 1AA", "country": "GB"
  },
  "trackingNo": "GLN-1A2B3C4D",
  "carrier": "DHL Express",
  "failureReason": null,
  "createdAt": "2026-10-31T10:15:30Z",
  "updatedAt": "2026-10-31T10:20:00Z"
}
```

| Field | description | example |
|---|---|---|
| orderId | Order id (UUID) from order-service. | `3f2b8c1e-7a4d-4e9b-9c2a-1d5e6f7a8b90` |
| state | Lifecycle state. | `PICKING` |
| items | Order lines snapshotted when the task was created. | (see FulfilmentItem) |
| shippingAddress | Where the order ships. | (see Address) |
| trackingNo | Carrier tracking number; present once DISPATCHED. | `GLN-1A2B3C4D` |
| carrier | Carrier name; present once DISPATCHED. | `DHL Express` |
| failureReason | Why dispatch failed; present once DISPATCH_FAILED. | `carrier_unavailable` |
| createdAt | When the task was created (UTC). | `2026-10-31T10:15:30Z` |
| updatedAt | When the task last changed (UTC). | `2026-10-31T10:20:00Z` |

`FulfilmentItem` — fields already annotated; add the class description only:
"One order line as snapshotted when the task was created." Existing field
descriptions/examples: sku "Product SKU." (`SKU-0042`); productName "Product
name at dispatch time." (`Espresso cup`); quantity "Units dispatched." (`2`);
unitPriceCents "Unit price, in cents." (`1250`).

`Address` — class: "Ship-to address snapshotted from the order when the task
was created." Class example:
`{"line1":"1 High Street","line2":"Flat 2","city":"London","postalCode":"SW1A 1AA","country":"GB"}`

| Field | description | example |
|---|---|---|
| line1 | First address line. | `1 High Street` |
| line2 | Second address line, if any. | `Flat 2` |
| city | City or town. | `London` |
| postalCode | Postcode / ZIP. | `SW1A 1AA` |
| country | ISO 3166-1 alpha-2 country code. | `GB` |

`DispatchRequest` — class: "Dispatch details: who is carrying the order and its
tracking number." Example: `{"trackingNo":"GLN-1A2B3C4D","carrier":"DHL Express"}`

| Field | description | example |
|---|---|---|
| trackingNo | Carrier tracking number; non-blank. | `GLN-1A2B3C4D` |
| carrier | Carrier name; non-blank. | `DHL Express` |

`FailRequest` — class: "Why the task could not be dispatched." Example:
`{"reason":"carrier_unavailable"}`

| Field | description | example |
|---|---|---|
| reason | Failure reason; non-blank. | `carrier_unavailable` |

`SimulateRequest` — class: "Demo simulation options; both optional (send `{}`
to advance one step)." Example: `{"forceFailure":true,"reason":"simulated dispatch failure"}`

| Field | description | example |
|---|---|---|
| forceFailure | Fail the task instead of advancing it. | `true` |
| reason | Failure reason when `forceFailure` is true; defaults to `Simulated dispatch failure`. | `simulated dispatch failure` |

`ErrorResponse` — class: "Error body." Example:
`{"error":"Fulfilment task not found"}`

| Field | description | example |
|---|---|---|
| error | Human-readable reason. | `Fulfilment task not found` |

### Error variants

| Status | When | Example body |
|---|---|---|
| 404 | No task exists for `orderId` (any operator endpoint, incl. simulate-next). | `{"error":"Fulfilment task not found"}` |
| 409 | start-picking: task is not PENDING. | `{"error":"Cannot transition order 3f2b8c1e-7a4d-4e9b-9c2a-1d5e6f7a8b90 from PICKING"}` |
| 409 | pack: task is not PICKING. | `{"error":"Cannot transition order 3f2b8c1e-7a4d-4e9b-9c2a-1d5e6f7a8b90 from PENDING"}` |
| 409 | dispatch: task is not PACKED. | `{"error":"Cannot transition order 3f2b8c1e-7a4d-4e9b-9c2a-1d5e6f7a8b90 from PICKING"}` |
| 409 | fail: task is already DISPATCHED/DISPATCH_FAILED. | `{"error":"Cannot transition order 3f2b8c1e-7a4d-4e9b-9c2a-1d5e6f7a8b90 from DISPATCHED"}` |
| 409 | simulate-next: task is terminal. | `{"error":"Order 3f2b8c1e-7a4d-4e9b-9c2a-1d5e6f7a8b90 is already DISPATCHED"}` |
| 400 | GET list: `state` is not one of the wire values. | `{"error":"Unknown state: SHIPPED"}` |
| 400 | dispatch: blank (or whitespace-only) `trackingNo` or `carrier`. | `{"error":"trackingNo and carrier are required"}` |
| 400 | fail: blank (or whitespace-only) `reason`. | `{"error":"reason is required"}` |
| 400 | Malformed `orderId` path or unparseable JSON body — tapir's default decode response, description "Invalid value for: …" (text/plain, no `ErrorResponse`). | `Invalid value for: path parameter orderId` |
| 503 | ready: Postgres did not answer the ping. | (no body) |

Error variant bodies get `.description(...)` (when they happen) plus
`.example(ErrorResponse(...))`. Note from TD-20.3: tapir folds a single-use
nested class's field description into its component, so describe the class,
not the field.
