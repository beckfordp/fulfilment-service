#!/usr/bin/env bash
set -euo pipefail

# Phase 3 verification for journey-telemetry_20261103 (gluon PERF-4): after
# real traffic (gluon bin/run-acceptance drives every journey), fulfilment-service's
# metrics port in gluon-local exports the runtime, pool and business series
# this track adds. `fulfilment.tasks.created` exports as fulfilment_tasks_total:
# Prometheus drops the OpenMetrics-reserved _created suffix.
#
# Usage: ./scripts/verify-journey-telemetry.sh   (after bin/k8s-local-up + traffic)

NAMESPACE="gluon-local"
LOCAL_PORT="${LOCAL_PORT:-19091}"
EXPECTED=(jvm_memory_used_bytes jvm_thread_count cats_effect_runtime db_client_connection_count db_client_connection_wait_time_seconds_bucket fulfilment_tasks_total fulfilment_transitions_total fulfilment_step_duration_seconds_bucket fulfilment_backlog_size fulfilment_backlog_oldest_age_seconds purekafka_consumer_event_age_seconds_bucket)

kubectl -n "$NAMESPACE" port-forward "svc/fulfilment-service" "$LOCAL_PORT:9090" >/dev/null 2>&1 &
PF_PID=$!
trap 'kill "$PF_PID" >/dev/null 2>&1 || true' EXIT
body=""
for _ in $(seq 1 20); do
  body="$(curl -fs "http://localhost:$LOCAL_PORT/metrics" || true)"
  [ -n "$body" ] && break
  sleep 0.5
done
[ -n "$body" ] || { echo "FAIL: fulfilment-service /metrics unreachable" >&2; exit 1; }

failed=0
for metric in "${EXPECTED[@]}"; do
  if grep -q "^$metric" <<< "$body"; then
    echo "  PASS $metric"
  else
    echo "  FAIL $metric missing"
    failed=1
  fi
done
[ "$failed" = 0 ] || { echo "FAIL: journey telemetry series missing" >&2; exit 1; }
echo "PASS: fulfilment-service journey telemetry live"
