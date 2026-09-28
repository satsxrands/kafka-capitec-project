#!/usr/bin/env bash
# End-to-end demo from a clean cluster. Evidence lands in ../evidence/.
# Order matters: the fraud and notification services start FIRST and wait, the way
# they would in production, so record timestamps measure real processing latency.
set -euo pipefail
cd "$(dirname "$0")"
E=../evidence
mkdir -p "$E"

{ ./00-create-topics.sh
  for t in payments.payment-lifecycle.v1 fraud.fraud-score.v1 notifications.payment-notification.v1; do
    kubectl exec kafka-0 -- kafka-topics --bootstrap-server kafka-service:9092 --describe --topic "$t"
  done; } > "$E/01-topics.txt" 2>&1

./20-fraud-service.sh > "$E/03-fraud-service.txt" 2>&1 &
./30-notification-service.sh > "$E/04-notification-service.txt" 2>&1 &
sleep 12   # let both groups join and get partitions assigned
./10-produce-payments.sh 60 20 > "$E/02-produce.txt" 2>&1
wait
./40-audit-consumer.sh > "$E/05-audit-consumer.txt" 2>&1
./50-notification-dispatcher.sh > "$E/06-dispatcher.txt" 2>&1

cp /tmp/payment-events.txt /tmp/fraud-events.txt /tmp/notification-events.txt \
   /tmp/audit-archive.txt /tmp/dispatched.txt "$E/"
cat "$E"/0[2-6]-*.txt
./90-verify.sh
