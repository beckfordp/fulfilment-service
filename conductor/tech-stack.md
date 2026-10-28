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

## Persistence
- skunk 1.0.0 + skunk-circe (JSONB task snapshot/outbox payload)
- Flyway 11.8.2; PostgreSQL 16, database `fulfilment`

## Messaging
- fs2-kafka 3.6.0 + purekafka 0.3.0 — consumer (`PaymentAuthorized`) with
  StreamSupervisor + DeadLetter; transactional outbox + Debezium for
  `DispatchConfirmed`/`DispatchFailed`
- Optional order-service client (`GET /orders/{id}`) for the task snapshot,
  resilience-wrapped like other service clients

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
