#!/usr/bin/env bash
# Fraud consumer + fraud producer, chained:
#   payments topic -> [fraud-scoring-service group] -> fraud_score.py -> fraud topic
# Low-latency fetch (fetch.min.bytes=1, fetch.max.wait.ms=10) for the < 50 ms SLA.
# --timeout-ms stops the demo once the topic has been idle for 20 s.
set -euo pipefail
cd "$(dirname "$0")"

kubectl exec kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic payments.payment-lifecycle.v1 \
  --group fraud-scoring-service \
  --property print.key=true \
  --property key.separator='|' \
  --consumer-property auto.offset.reset=earliest \
  --consumer-property enable.auto.commit=true \
  --consumer-property auto.commit.interval.ms=1000 \
  --consumer-property max.poll.records=100 \
  --consumer-property session.timeout.ms=10000 \
  --consumer-property heartbeat.interval.ms=3000 \
  --consumer-property max.poll.interval.ms=30000 \
  --consumer-property fetch.min.bytes=1 \
  --consumer-property fetch.max.wait.ms=10 \
  --consumer-property max.partition.fetch.bytes=1048576 \
  --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor \
  --timeout-ms 20000 2>/dev/null \
| python3 -u fraud_score.py \
| tee /tmp/fraud-events.txt \
| kubectl exec -i kafka-1 -- kafka-console-producer \
  --bootstrap-server kafka-service:9092 \
  --topic fraud.fraud-score.v1 \
  --property parse.key=true \
  --property key.separator=: \
  --producer-property acks=all \
  --producer-property enable.idempotence=true \
  --producer-property retries=2147483647 \
  --producer-property max.in.flight.requests.per.connection=5 \
  --producer-property compression.type=lz4 \
  --producer-property linger.ms=0 \
  --producer-property delivery.timeout.ms=30000 \
  --producer-property request.timeout.ms=10000
echo "Fraud events produced: $(wc -l < /tmp/fraud-events.txt | tr -d ' ')" >&2
