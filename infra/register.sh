#!/bin/sh
set -eu
curl -fsS --retry 90 --retry-delay 2 --retry-all-errors --max-time 10 \
  -X PUT -H 'Content-Type: application/json' --data-binary @/config/connector.json \
  http://connect:8083/connectors/orders-cdc/config
