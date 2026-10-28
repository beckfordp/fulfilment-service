#!/usr/bin/env bash
# Live verification for track fulfilment-core_20261028 (US-17.1-17.3, 18.1).
# Real flow: authorize an order -> fulfilment task PENDING -> simulate to
# DISPATCHED + one DispatchConfirmed on fulfilment-events; a second order is
# failed via simulate forceFailure -> DispatchFailed. Cleans up test data.
set -euo pipefail

NAMESPACE="${NAMESPACE:-gluon-local}"
ORDER_URL="${ORDER_URL:-http://localhost:18080}"
PAYMENT_URL="${PAYMENT_URL:-http://localhost:18081}"
INVENTORY_URL="${INVENTORY_URL:-http://localhost:18082}"
FULFILMENT_URL="${FULFILMENT_URL:-http://localhost:18085}"
PIDS=()
TMP_DIR="$(mktemp -d)"
cleanup() {
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

start_pf() {
  kubectl -n "$NAMESPACE" port-forward "svc/$1" "$2:8080" >/dev/null 2>&1 &
  PIDS+=("$!")
}
wait_ready() {
  for _ in $(seq 1 30); do
    curl -sf "$1/health/ready" >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "FAIL: $2 never became ready" >&2
  exit 1
}
post_json() {
  curl -sS -o "$3" -w '%{http_code}' -X POST "$1" \
    -H 'Content-Type: application/json' -d "$2"
}
json_field() { python3 -c "import json,sys; print(json.load(sys.stdin)$1)" < "$2"; }

echo "== fulfilment-service live verification =="
start_pf order-service 18080
start_pf payment-service 18081
start_pf inventory-service 18082
start_pf fulfilment-service 18085
wait_ready "$ORDER_URL" order-service
wait_ready "$PAYMENT_URL" payment-service
wait_ready "$INVENTORY_URL" inventory-service
wait_ready "$FULFILMENT_URL" fulfilment-service

ADDRESS='{"line1":"1 Verification Lane","city":"London","postalCode":"EC1A 1BB","country":"GB"}'
CUSTOMER="$(python3 -c 'import uuid; print(uuid.uuid4())')"
SKU="$(curl -sf "$INVENTORY_URL/inventorys" | python3 -c '
import json,sys
rows=json.load(sys.stdin)
stocked=[r for r in rows if r["quantityAvailable"]>0]
if not stocked: sys.exit("no stocked sku")
print(stocked[0]["sku"])')"
echo "-- using sku $SKU"

make_order() {
  local out="$1"
  local pm="$TMP_DIR/pm.json"
  local status
  status="$(post_json "$PAYMENT_URL/payments/methods" \
    "{\"customerId\":\"$CUSTOMER\",\"cardNumber\":\"4242424242424242\",\"expiry\":\"12/30\",\"billingAddress\":$ADDRESS}" "$pm")"
  [ "$status" = "201" ] || { echo "FAIL: payment method -> $status" >&2; cat "$pm" >&2; exit 1; }
  local pm_id; pm_id="$(json_field '["id"]' "$pm")"
  status="$(post_json "$ORDER_URL/orders" \
    "{\"customerId\":\"$CUSTOMER\",\"items\":[{\"sku\":\"$SKU\",\"productName\":\"Verify Watch\",\"unitPriceCents\":100,\"quantity\":1}],\"shippingAddress\":$ADDRESS,\"paymentMethodId\":\"$pm_id\"}" "$out")"
  [ "$status" = "201" ] || { echo "FAIL: order -> $status" >&2; cat "$out" >&2; exit 1; }
  json_field '["id"]' "$out"
}

wait_task() {
  local order_id="$1" expected="$2"
  for _ in $(seq 1 30); do
    local body; body="$(curl -sf "$FULFILMENT_URL/fulfilment-tasks?state=$expected")"
    if echo "$body" | grep -q "$order_id"; then return 0; fi
    sleep 1
  done
  echo "FAIL: task for $order_id never reached $expected" >&2
  curl -s "$FULFILMENT_URL/fulfilment-tasks" >&2 || true
  exit 1
}

echo "-- order 1 -> PENDING task with snapshot --"
ORDER1="$(make_order "$TMP_DIR/order1.json")"
wait_task "$ORDER1" PENDING
TASK="$(curl -sf "$FULFILMENT_URL/fulfilment-tasks?state=PENDING" | python3 -c "
import json,sys
rows=json.load(sys.stdin)
t=[r for r in rows if r['orderId']=='$ORDER1'][0]
print(t['items'][0]['sku'], t['shippingAddress']['line1'])")"
[ -n "$TASK" ] || { echo "FAIL: snapshot missing" >&2; exit 1; }
echo "   task $ORDER1 snapshot: $TASK"

echo "-- simulate to DISPATCHED --"
for _ in 1 2 3; do
  STATUS="$(post_json "$FULFILMENT_URL/fulfilment-tasks/$ORDER1/simulate-next" '{}' "$TMP_DIR/sim.json")"
  [ "$STATUS" = "200" ] || { echo "FAIL: simulate -> $STATUS" >&2; cat "$TMP_DIR/sim.json" >&2; exit 1; }
done
STATE="$(json_field '["state"]' "$TMP_DIR/sim.json")"
[ "$STATE" = "DISPATCHED" ] || { echo "FAIL: expected DISPATCHED, got $STATE" >&2; exit 1; }
TRACKING="$(json_field '["trackingNo"]' "$TMP_DIR/sim.json")"
echo "   DISPATCHED via $TRACKING"

echo "-- one DispatchConfirmed on fulfilment-events --"
MESSAGES="$(kubectl exec -n "$NAMESPACE" deploy/kafka -- \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic fulfilment-events --from-beginning --timeout-ms 10000 2>/dev/null || true)"
echo "$MESSAGES" | grep -q "$ORDER1" || { echo "FAIL: no event for $ORDER1" >&2; exit 1; }
echo "$MESSAGES" | grep "$ORDER1" | grep -q 'DispatchConfirmed' || {
  echo "FAIL: no DispatchConfirmed for $ORDER1" >&2; exit 1; }
echo "   observed DispatchConfirmed for $ORDER1"

echo "-- order 2 -> forced DispatchFailed --"
ORDER2="$(make_order "$TMP_DIR/order2.json")"
wait_task "$ORDER2" PENDING
STATUS="$(post_json "$FULFILMENT_URL/fulfilment-tasks/$ORDER2/simulate-next" \
  '{"forceFailure":true,"reason":"Simulated warehouse failure"}' "$TMP_DIR/fail.json")"
[ "$STATUS" = "200" ] || { echo "FAIL: forced failure -> $STATUS" >&2; cat "$TMP_DIR/fail.json" >&2; exit 1; }
STATE="$(json_field '["state"]' "$TMP_DIR/fail.json")"
[ "$STATE" = "DISPATCH_FAILED" ] || { echo "FAIL: expected DISPATCH_FAILED, got $STATE" >&2; exit 1; }
MESSAGES="$(kubectl exec -n "$NAMESPACE" deploy/kafka -- \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic fulfilment-events --from-beginning --timeout-ms 10000 2>/dev/null || true)"
echo "$MESSAGES" | grep "$ORDER2" | grep -q 'DispatchFailed' || {
  echo "FAIL: no DispatchFailed for $ORDER2" >&2; exit 1; }
echo "   DISPATCH_FAILED + DispatchFailed for $ORDER2"

echo "-- cleaning up --"
for id in "$ORDER1" "$ORDER2"; do
  curl -sf -X POST "$INVENTORY_URL/inventorys/order-reservations/$id/release" >/dev/null || true
  curl -sf -X DELETE "$ORDER_URL/orders/$id" >/dev/null || true
done
kubectl exec -n "$NAMESPACE" deploy/fulfilment-postgres -- psql -U fulfilment -d fulfilment -q -c \
  "delete from outbox_events where aggregate_id in ('$ORDER1','$ORDER2'); delete from fulfilment_task where order_id in ('$ORDER1','$ORDER2')" >/dev/null

echo "All fulfilment live checks passed."
