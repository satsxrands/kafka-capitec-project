#!/usr/bin/env bash
# Audit / reconciliation consumer: reads EVERY payment event and archives it.
# Lag SLA is minutes, so it fetches in large batches (fetch.min.bytes=1 MB, wait 5 s).
set -euo pipefail

kubectl exec kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic payments.payment-lifecycle.v1 \
  --group payment-audit-archiver \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true \
  --property key.separator='|' \
  --consumer-property auto.offset.reset=earliest \
  --consumer-property enable.auto.commit=true \
  --consumer-property auto.commit.interval.ms=5000 \
  --consumer-property max.poll.records=1000 \
  --consumer-property session.timeout.ms=30000 \
  --consumer-property heartbeat.interval.ms=10000 \
  --consumer-property max.poll.interval.ms=300000 \
  --consumer-property fetch.min.bytes=1048576 \
  --consumer-property fetch.max.wait.ms=5000 \
  --consumer-property max.partition.fetch.bytes=1048576 \
  --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor \
  --timeout-ms 20000 2>/dev/null > /tmp/audit-archive.txt || true
echo "Audit records archived: $(wc -l < /tmp/audit-archive.txt | tr -d ' ')" >&2
