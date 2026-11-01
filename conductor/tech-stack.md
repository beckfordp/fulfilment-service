# Tech Stack — fulfilment-service

## Language / runtime
- Scala 3.9.0
- JDK 21

## Effects / HTTP
- Cats Effect 3.7.0
- http4s 0.23.37 (ember-server)
- circe 0.14.16
- CORS middleware (`Cors.scala`) from the platform template (TD-1)

## API layer
- tapir 1.11.25 — operator endpoints + Swagger/OpenAPI docs (`/docs`)
- purekafka.docs — AsyncAPI docs for consumed/produced events; schemaRegistryCheck
  against Apicurio at build/CI time (ADR 0003)
- Schema registry baseline (gluon TD-18, purekafka 0.5.0 / sbt-purekafka-docs
  0.5.0-dev): only producer channels register (draft-07, closed, labelled
  `producer=<service>`, `BACKWARD_TRANSITIVE`); subscribe channels dry-run
  their mirror. CI restores gluon's `registry/` git export into the throwaway
  Apicurio first, so the check compares against the real baseline;
  `scripts/verify-schema-registry-check.sh` does the same locally.
- AsyncAPI 3.0 (gluon TD-19, purekafka 0.6.0 / sbt-purekafka-docs 0.6.0-dev):
  `send`/`receive` from this service's side. Consumed-event mirrors come from
  gluon's `registry/` via `bin/registry-scala` (see gluon `registry/README.md`).

## Persistence
- skunk 1.0.0 + skunk-circe (JSONB task snapshot/outbox payload)
- Flyway 11.8.2; PostgreSQL 16, database `fulfilment`

## Messaging
- fs2-kafka 3.6.0 + purekafka 0.5.0 — `PaymentAuthorizedConsumer` (null-safe,
  envelope/eventType filtered, StreamSupervisor + DeadLetter, group
  `fulfilment-service-payment-events`); transactional outbox + Debezium for
  `DispatchConfirmed`/`DispatchFailed`
- `OrderClient` — resilience-wrapped `GET /orders/{id}` snapshot (items +
  shipping address), retry + circuit breaker from `order-client` config
- `simulate-next` is registered only when `fulfilment.simulation.enabled`
  (local values true)

## Testing
- munit 1.3.6 + munit-cats-effect 2.2.1, log4cats-testing, testcontainers
  (Postgres + Kafka modules)
- scalafmt (Scala 3 default) — `sbt scalafmtCheck test` in CI

## Shared platform library
- `purerestlib` 0.1.0 (`io.github.beckfordp`) — tracing, logging, metrics,
  resilience, tapir docs.

## Packaging / local deploy
- sbt-native-packager; deployed to local OrbStack k8s via gluon's Helm chart
  (Postgres + Debezium connector added to `infra/k8s/local-infra`).
