# fulfilment-service

A production-quality microservice built on
[purerest](https://github.com/beckfordp/purerest), generated from the
[pure-service-generator](https://github.com/beckfordp/pure-service-generator) giter8 template.
Its own `build.sbt` resolves `purerestlib` as a published GitHub Packages dependency.

## Prerequisites

- sbt / JDK 21 (for building and running)
- Docker Desktop (or another Docker engine) with Docker Compose v2
- A GitHub [personal access token](https://github.com/settings/tokens) with `read:packages`
  scope, exported as `GITHUB_TOKEN` (and `GITHUB_ACTOR` set to your GitHub username) — needed to
  resolve `purerestlib` from GitHub Packages. GitHub Packages requires authentication to *read*
  Maven artifacts even from a public repo.

## Quickstart

```
export GITHUB_ACTOR=<your-github-username>
export GITHUB_TOKEN=<your-PAT-with-read:packages>

docker compose up -d   # starts Postgres
sbt run                # runs migrations, then starts the service on :8080
```

Then, in another terminal:

```
# Operator queue (tasks are created by PaymentAuthorized events)
curl http://localhost:8080/fulfilment-tasks
curl "http://localhost:8080/fulfilment-tasks?state=PACKED"

# Drive a task (404 unknown, 409 invalid transition)
curl -X POST http://localhost:8080/fulfilment-tasks/<orderId>/start-picking
curl -X POST http://localhost:8080/fulfilment-tasks/<orderId>/pack
curl -X POST http://localhost:8080/fulfilment-tasks/<orderId>/dispatch \
  -H "Content-Type: application/json" \
  -d '{"trackingNo":"GLN-1234","carrier":"DHL Express"}'
curl -X POST http://localhost:8080/fulfilment-tasks/<orderId>/fail \
  -H "Content-Type: application/json" -d '{"reason":"Item damaged"}'

# Dev-only (simulation.enabled): advance one step / force a failure
curl -X POST http://localhost:8080/fulfilment-tasks/<orderId>/simulate-next \
  -H "Content-Type: application/json" -d '{}'
curl -X POST http://localhost:8080/fulfilment-tasks/<orderId>/simulate-next \
  -H "Content-Type: application/json" \
  -d '{"forceFailure":true,"reason":"Simulated failure"}'

# Liveness / readiness
curl http://localhost:8080/health
curl http://localhost:8080/health/ready
```

Swagger UI (generated from the same tapir endpoint definitions as the real routes — see
`purerest.docs.Docs`) is browsable at **http://localhost:8080/docs**.

## View Kafka AsyncAPI docs

This service consumes `PaymentAuthorized` on `payment-events` and publishes
`DispatchConfirmed`/`DispatchFailed` on `fulfilment-events` (outbox + Debezium), registering all
three for [AsyncAPI](https://www.asyncapi.com/) documentation via `purekafka.docs` (see
`AsyncApiChannels`).

Generation is handled by the `sbt-purekafka-docs` plugin (`asyncApiChannelsClass` in
`build.sbt`), auto-chained after `Compile / compile` — every `sbt compile`/`run`/
`Docker/publishLocal` regenerates both files locally, no running server or manual script
needed. Run it directly with:

```
sbt asyncApiGenerate
```

- **Raw spec** — generated at `target/asyncapi-docs-spec/asyncapi.json`; also served live at
  http://localhost:8080/docs/asyncapi.json while the service is running.
- **Rendered HTML** — generated at `target/asyncapi-docs/index.html`
  (`open target/asyncapi-docs/index.html`); also served live at
  http://localhost:8080/docs/asyncapi/index.html while the service is running.

The HTML renderer needs Node.js/npm. If the Node toolchain is unavailable, the plugin logs a
warning and skips HTML generation (the JSON spec is still generated), so
`/docs/asyncapi/index.html` returns 404 until a successful `sbt asyncApiGenerate`.

## Development guidelines

Extending this service? See [`development-guidelines.md`](./development-guidelines.md) for the
pure-FP/tagless-final standards it follows — error modeling as ADTs, patterns to follow (and
avoid) — before adding new code.

## Testing

```
sbt scalafmtCheck test
```

Unit tests use an in-memory `FulfilmentStore`; Postgres-backed tests spin up a
real, ephemeral container via Testcontainers — no local Postgres or manual setup needed to run
`sbt test`.

## One database per service

This service owns a single dedicated Postgres database, named after its domain: a database named
`fulfilment` (singular), with a `fulfilment` table (quoted throughout in SQL — several
plausible domain names, e.g. "user"/"group"/"order", are reserved PostgreSQL keywords). REST
paths stay pluralized (`/fulfilments`, `/fulfilments/{id}`) per ordinary
resource-collection convention; only the database/table naming reflects the one-db-per-service
rule. No service reads or writes another service's database directly.

## Calling other services with resilience

This service doesn't call any other service out of the box, so purerest's retry + circuit-breaker
middleware (`purerest.resilience.Resilience.middleware`) isn't wired into `Main`. When you adapt
it to call a real downstream service, wrap its http4s `Client[F]` with `Resilience.middleware`
before building your own client on top of it — see
[`ClientResilienceExampleSuite`](./src/test/scala/fulfilmentservice/examples/ClientResilienceExampleSuite.scala)
for the pattern, tested against a dummy client so it stays correct as purerest evolves.
