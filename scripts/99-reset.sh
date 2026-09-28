#!/usr/bin/env bash
# Deletes the POC topics and consumer groups so the demo can be rerun from scratch.
set -uo pipefail
for g in fraud-scoring-service notification-service payment-audit-archiver notification-dispatcher; do
  kubectl exec kafka-0 -- kafka-consumer-groups --bootstrap-server kafka-service:9092 --delete --group "$g" 2>/dev/null
done
for t in payments.payment-lifecycle.v1 fraud.fraud-score.v1 notifications.payment-notification.v1; do
  kubectl exec kafka-0 -- kafka-topics --bootstrap-server kafka-service:9092 --delete --topic "$t"
done
