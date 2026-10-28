# Product Guidelines — fulfilment-service

## Code & engineering conventions
Canonical source: [`development-guidelines.md`](../development-guidelines.md)
(pure-FP, tagless-final, ADT error modeling, no partial functions, no
null/var, explicit DI, exhaustive pattern matching).

## API design conventions
- Plain resource JSON responses, standard HTTP status codes — no envelope.
- Errors: small JSON error object mapped 1:1 from the domain error ADT.
- Operator endpoints are internal (`/fulfilment-tasks...`), used by the gshop
  admin screen; no customer-facing API.
- One dedicated Postgres database per service (`fulfilment`).
- Swagger/OpenAPI + AsyncAPI generated from the same definitions as the real
  routes/registrations.

## Domain conventions
- State machine transitions are validated in the store and rejected with an
  ADT error; terminal states (`DISPATCHED`, `DISPATCH_FAILED`) never transition.
- Task creation is idempotent per `orderId` (redelivered `PaymentAuthorized`
  must not duplicate a task).
- Events are written to the outbox in the same transaction as the state change.
- Never log tokens/PII beyond ids already in event payloads.

## Testing
- scalafmt + munit + munit-cats-effect; Testcontainers Postgres for
  integration tests; Kafka consumers tested with synthetic envelopes.
- `sbt scalafmtCheck test` before every commit (CI enforces the same).
