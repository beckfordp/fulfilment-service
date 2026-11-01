#!/usr/bin/env bash
set -euo pipefail

# Verifies the REST docs convention end-to-end (track rest-docs_20261101,
# gluon TD-20.5): brings up Postgres and a real sbt run instance, fetches the
# served OpenAPI spec at /docs/docs.yaml, and checks it carries every
# endpoint's summary/description/tag, the parameter and error descriptions,
# DTO field descriptions and body examples, and that each endpoint lists only
# the error statuses it can return - not just that FulfilmentDocsSuite's
# in-process Docs.undocumented check passes.
#
# Requires: sbt, Docker, python3 with PyYAML, GITHUB_TOKEN/GITHUB_ACTOR set
# (purerestlib resolution).
#
# Usage: ./scripts/verify-rest-docs.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

SBT_PID=""
SPEC="$(mktemp)"

cleanup() {
  echo
  echo "Cleaning up..."
  if [ -n "$SBT_PID" ]; then
    pkill -9 -P "$SBT_PID" >/dev/null 2>&1 || true
    kill -9 "$SBT_PID" >/dev/null 2>&1 || true
  fi
  docker compose down -v >/dev/null 2>&1 || true
  rm -f "$SPEC"
}
trap cleanup EXIT

wait_ready() {
  for _ in $(seq 1 120); do
    status="$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/health || true)"
    [ "$status" = "200" ] && return 0
    sleep 1
  done
  echo "FAIL: fulfilment-service did not become ready in time." >&2
  return 1
}

echo "1. docker compose up -d (fresh Postgres volume)..."
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d
echo "   OK: postgres starting"

echo
echo "2. Starting fulfilment-service (sbt run) in the background..."
if [ -z "${GITHUB_TOKEN:-}" ] || [ -z "${GITHUB_ACTOR:-}" ]; then
  echo "WARN: GITHUB_TOKEN / GITHUB_ACTOR not set - resolving purerestlib from" >&2
  echo "      GitHub Packages will fail without a read:packages token." >&2
fi
FULFILMENT_SIMULATION_ENABLED=true sbt run </dev/null \
  >/tmp/fulfilment-service-verify-rest-docs.log 2>&1 &
SBT_PID=$!
wait_ready
echo "   OK: fulfilment-service ready on :8080"

echo
echo "3. GET /docs/docs.yaml..."
status="$(curl -s -o "$SPEC" -w '%{http_code}' http://localhost:8080/docs/docs.yaml)"
[ "$status" = "200" ] || { echo "FAIL: /docs/docs.yaml returned $status" >&2; exit 1; }
echo "   OK: served (200)"

echo
echo "4. Checking the spec..."
python3 -I - "$SPEC" <<'PY'
import sys, yaml

spec = yaml.safe_load(open(sys.argv[1]))
paths, schemas = spec["paths"], spec["components"]["schemas"]
failed = False

def check(label, ok):
    global failed
    print(("   OK: " if ok else "   FAIL: ") + label, file=sys.stdout if ok else sys.stderr)
    failed |= not ok

# Statuses each endpoint may list (success + its narrowed errors; tapir adds
# 400 for undecodable inputs on its own). simulate-next is served because the
# script starts the service with FULFILMENT_SIMULATION_ENABLED=true.
allowed = {
    ("/fulfilment-tasks", "get"): {"200", "400"},
    ("/fulfilment-tasks/{orderId}/start-picking", "post"): {"200", "400", "404", "409"},
    ("/fulfilment-tasks/{orderId}/pack", "post"): {"200", "400", "404", "409"},
    ("/fulfilment-tasks/{orderId}/dispatch", "post"): {"200", "400", "404", "409"},
    ("/fulfilment-tasks/{orderId}/fail", "post"): {"200", "400", "404", "409"},
    ("/fulfilment-tasks/{orderId}/simulate-next", "post"): {"200", "400", "404", "409"},
    ("/health", "get"): {"200"},
    ("/health/ready", "get"): {"200", "503"},
}
for (path, method), statuses in allowed.items():
    op = paths.get(path, {}).get(method)
    name = f"{method.upper()} {path}"
    if op is None:
        check(f"{name} documented", False)
        continue
    check(f"{name} has summary/description/tag",
          bool(op.get("summary")) and bool(op.get("description")) and bool(op.get("tags")))
    for p in op.get("parameters", []):
        check(f"{name} parameter '{p['name']}' described with example",
              bool(p.get("description")) and ("example" in p or "example" in p.get("schema", {})))
    responses = op.get("responses", {})
    check(f"{name} lists only {sorted(statuses)} (got {sorted(responses)})",
          set(responses) <= statuses)
    for code, r in responses.items():
        if code.startswith("2"):
            continue
        check(f"{name} {code} described", bool(r.get("description")))
        # tapir's own decode-failure response is text/plain with no example;
        # every JSON error body (our ErrorResponse variants) needs one.
        json_bodies = {ct: c for ct, c in r.get("content", {}).items() if "json" in ct}
        if json_bodies:
            check(f"{name} {code} JSON body has an example",
                  any(("example" in c or "examples" in c) for c in json_bodies.values()))

for path in ["/fulfilment-tasks/{orderId}/dispatch", "/fulfilment-tasks/{orderId}/fail",
             "/fulfilment-tasks/{orderId}/simulate-next"]:
    post = paths[path]["post"]
    check(f"POST {path} request body has an example",
          any(("example" in c or "examples" in c)
              for c in post["requestBody"]["content"].values()))

for path in ["/fulfilment-tasks", "/fulfilment-tasks/{orderId}/dispatch",
             "/fulfilment-tasks/{orderId}/fail", "/fulfilment-tasks/{orderId}/simulate-next"]:
    ok = paths[path]["post" if path != "/fulfilment-tasks" else "get"]["responses"]["200"]
    check(f"200 {path} response has an example body",
          any(("example" in c or "examples" in c) for c in ok["content"].values()))

check("GET /health/ready 503 described", bool(paths["/health/ready"]["get"]["responses"]["503"].get("description")))
check("start-picking lists 404/409",
      {"404", "409"} <= set(paths["/fulfilment-tasks/{orderId}/start-picking"]["post"]["responses"]))
check("pack lists 404/409",
      {"404", "409"} <= set(paths["/fulfilment-tasks/{orderId}/pack"]["post"]["responses"]))
check("dispatch lists 400/404/409",
      {"400", "404", "409"} <= set(paths["/fulfilment-tasks/{orderId}/dispatch"]["post"]["responses"]))
check("fail lists 400/404/409",
      {"400", "404", "409"} <= set(paths["/fulfilment-tasks/{orderId}/fail"]["post"]["responses"]))
check("simulate-next documented (simulation enabled)",
      "/fulfilment-tasks/{orderId}/simulate-next" in paths)

for name in ["TaskResponse", "Address", "DispatchRequest", "FailRequest",
             "SimulateRequest", "ErrorResponse"]:
    props = schemas.get(name, {}).get("properties", {})
    # A field that only references another component is described by it
    # (tapir folds a single-use field's description into the component).
    def described(s):
        ref = s.get("$ref", "").rsplit("/", 1)[-1]
        return bool(s.get("description")) or bool(ref and schemas.get(ref, {}).get("description"))
    missing = [f for f, s in props.items() if not described(s)]
    check(f"schema {name} fields all described"
          + (f" (missing: {missing}: {[props[f] for f in missing]})" if missing else ""),
          bool(props) and not missing)

sys.exit(1 if failed else 0)
PY

echo
echo "All checks passed."
