#!/usr/bin/env bash
# Notification dispatcher: reads the notifications topic and "sends" each one
# (the POC prints it; production would call the push/SMS provider).
set -euo pipefail

kubectl exec kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic notifications.payment-notification.v1 \
  --group notification-dispatcher \
  --property print.key=true \
  --property key.separator='|' \
  --consumer-property auto.offset.reset=earliest \
  --consumer-property enable.auto.commit=true \
  --consumer-property auto.commit.interval.ms=1000 \
  --consumer-property max.poll.records=500 \
  --consumer-property fetch.min.bytes=1 \
  --consumer-property fetch.max.wait.ms=500 \
  --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor \
  --timeout-ms 20000 2>/dev/null > /tmp/dispatched.txt || true
echo "Notifications dispatched: $(wc -l < /tmp/dispatched.txt | tr -d ' ')" >&2
