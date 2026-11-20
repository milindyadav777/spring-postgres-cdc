#!/bin/sh
set -eu
exec /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-0:9092,kafka-1:9092,kafka-2:9092 \
  --create --if-not-exists --topic orders.public.orders --partitions 6 --replication-factor 3 \
  --config min.insync.replicas=2 --config cleanup.policy=delete --config retention.ms=604800000
