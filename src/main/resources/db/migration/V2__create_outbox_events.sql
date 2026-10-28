-- Transactional outbox (ADR 0008, TD-3.2): the Kafka-bound event is written
-- here in the same transaction as the state change it announces, instead of
-- a separate direct Kafka publish call. `payload` holds the complete
-- EventEnvelope JSON (eventId/eventType/timestamp/source/correlationId/
-- payload) already assembled by the app - a future Kafka Connect + Debezium
-- connector (TD-3.1) streams new rows here straight to Kafka, unchanged.
-- No sent/processed column: cleanup is time-based (a CronJob, TD-3.1), not
-- consumption-tracked. See docs/examples.md for how this ties into
-- OutboxExampleSuite, and how to remove this table if you don't adopt the
-- outbox-CDC pattern (Flyway migrations are append-only history once
-- applied anywhere real - don't delete this file after that point).
CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    aggregate_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    trace_parent TEXT,
    correlation_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
