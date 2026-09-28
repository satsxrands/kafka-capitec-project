#!/usr/bin/env bash
# Creates the three topics. Retention for payment + fraud events: 5 years
# (5 x 365.25 days = 1826 days = 157,766,400,000 ms). Notifications: 7 days.
# Partitions = peak topic throughput / 10 Mb/s per consumer, rounded up with headroom:
#   payments 1000 Mb/s -> 100 | fraud ~193 Mb/s -> 20 -> 24 | notifications ~273 Mb/s -> 28 -> 30
set -euo pipefail

kubectl exec kafka-0 -- kafka-topics \
  --bootstrap-server kafka-service:9092 \
  --create \
  --topic payments.payment-lifecycle.v1 \
  --partitions 100 \
  --replication-factor 3 \
  --config retention.ms=157766400000 \
  --config retention.bytes=-1 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config compression.type=zstd \
  --config max.message.bytes=262144

kubectl exec kafka-0 -- kafka-topics \
  --bootstrap-server kafka-service:9092 \
  --create \
  --topic fraud.fraud-score.v1 \
  --partitions 24 \
  --replication-factor 3 \
  --config retention.ms=157766400000 \
  --config retention.bytes=-1 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config compression.type=lz4 \
  --config max.message.bytes=262144

kubectl exec kafka-0 -- kafka-topics \
  --bootstrap-server kafka-service:9092 \
  --create \
  --topic notifications.payment-notification.v1 \
  --partitions 30 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config retention.bytes=-1 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config compression.type=lz4 \
  --config max.message.bytes=262144
