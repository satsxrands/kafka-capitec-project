#!/usr/bin/env bash
# Notification consumer + notification producer, chained:
#   payments topic -> [notification-service group] -> notify.py -> notifications topic
# A 2 s SLA allows small fetch batching (fetch.min.bytes=16384, fetch.max.wait.ms=500).
set -euo pipefail
cd "$(dirname "$0")"

kubectl exec kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic payments.payment-lifecycle.v1 \
  --group notification-service \
  --property print.key=true \
  --property key.separator='|' \
  --consumer-property auto.offset.reset=earliest \
  --consumer-property enable.auto.commit=true \
  --consumer-property auto.commit.interval.ms=1000 \
  --consumer-property max.poll.records=500 \
  --consumer-property session.timeout.ms=15000 \
  --consumer-property heartbeat.interval.ms=5000 \
  --consumer-property max.poll.interval.ms=60000 \
  --consumer-property fetch.min.bytes=16384 \
  --consumer-property fetch.max.wait.ms=500 \
  --consumer-property max.partition.fetch.bytes=1048576 \
  --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor \
  --timeout-ms 20000 2>/dev/null \
| python3 -u notify.py \
| tee /tmp/notification-events.txt \
| kubectl exec -i kafka-2 -- kafka-console-producer \
  --bootstrap-server kafka-service:9092 \
  --topic notifications.payment-notification.v1 \
  --property parse.key=true \
  --property key.separator=: \
  --producer-property acks=all \
  --producer-property enable.idempotence=true \
  --producer-property retries=2147483647 \
  --producer-property max.in.flight.requests.per.connection=5 \
  --producer-property compression.type=lz4 \
  --producer-property linger.ms=10 \
  --producer-property delivery.timeout.ms=30000 \
  --producer-property request.timeout.ms=10000
echo "Notification events produced: $(wc -l < /tmp/notification-events.txt | tr -d ' ')" >&2
