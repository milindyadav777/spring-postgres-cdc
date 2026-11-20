#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
namespace=cdc-lab
k=(kubectl --context "kind-${KIND_CLUSTER_NAME:-spring-labs}" -n "$namespace")
"${k[@]}" wait --for=condition=Ready scaledobject/cdc-projector --timeout=180s
"${k[@]}" port-forward svc/cdc 8080:8080 >/dev/null &
port_forward=$!
trap 'kill "$port_forward" 2>/dev/null || true' EXIT
base=http://127.0.0.1:8080
curl -fsS --retry 25 --retry-all-errors --retry-delay 1 "$base/actuator/health/readiness" >/dev/null
id=$("${k[@]}" exec statefulset/source -- psql -XAtq -v ON_ERROR_STOP=1 -U postgres -d orders \
  -c "INSERT INTO orders(status) VALUES ('PAID') RETURNING id")
for i in {1..120}; do
  if curl -fsS "$base/orders/$id" 2>/dev/null | jq -e '.status == "PAID"' >/dev/null; then
    echo "PASS source WAL reached the Kubernetes projection"
    exit 0
  fi
  sleep 2
done
echo "No projection for source order $id" >&2
exit 1
