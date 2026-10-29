# TD-15.5 — Cross-repo consistency hardening (fulfilment-service)

## Overview

Fulfilment-service slice of gluon TD-15: typed order ids, `Uri` config for the
order-service client, raw-log redaction, unknown-event logging,
`asyncApiHtmlDir`, ScalaCheck, narrower effect bounds.

## Functional Requirements

1. **Typed ids** — opaque `OrderId` parsed once at HTTP/Kafka boundaries;
   store and routes take `OrderId`; malformed route id → 400.
2. **Client config** — `OrderClient` takes a parsed `Uri`; no
   `Uri.unsafeFromString`.
3. **Observability** — unknown event types logged; raw payloads redacted.
4. **Config** — `asyncApiHtmlDir` with `${?ASYNCAPI_HTML_DIR}`; no `sys.env`.
5. **Tests** — `munit-scalacheck` + state-machine properties.
6. **Effect constraints** — `Cors`/`HealthRoutes`/route handlers narrowed where
   possible.

## Acceptance Criteria

- Malformed order id → 400; no raw UUID parsing in stores.
- Full suite + scalafmt green; live script passes.
