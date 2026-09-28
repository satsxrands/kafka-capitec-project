#!/usr/bin/env bash
# Payment producer: pipes generated lifecycle events into the payments topic.
# Client settings use --producer-property (NOT --property, which the console
# producer silently ignores for client configs; see Issues Encountered).
set -euo pipefail
cd "$(dirname "$0")"

# 60 payments, one event every 20 ms (~50 events/s): a steady stream, not one burst.
python3 -u generate_payments.py "${1:-60}" "${2:-20}" \
| tee /tmp/payment-events.txt \
| kubectl exec -i kafka-0 -- kafka-console-producer \
  --bootstrap-server kafka-service:9092 \
  --topic payments.payment-lifecycle.v1 \
  --property parse.key=true \
  --property key.separator=: \
  --producer-property acks=all \
  --producer-property enable.idempotence=true \
  --producer-property retries=2147483647 \
  --producer-property max.in.flight.requests.per.connection=5 \
  --producer-property compression.type=zstd \
  --producer-property linger.ms=5 \
  --producer-property batch.size=65536 \
  --producer-property delivery.timeout.ms=30000 \
  --producer-property request.timeout.ms=10000
echo "Produced $(wc -l < /tmp/payment-events.txt | tr -d ' ') payment events" >&2
