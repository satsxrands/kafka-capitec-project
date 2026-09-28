# Fraud producer, consumer, topic/s and event/s

The fraud consumer reads `PAYMENT_INITIATED` events from the payments topic, scores each
payment, and the fraud producer publishes one `FRAUD_SCORE_LOW`, `FRAUD_SCORE_MEDIUM` or
`FRAUD_SCORE_HIGH` event per payment, keyed by `payment_id`. Chained in
`scripts/20-fraud-service.sh`; scoring rules in `scripts/fraud_score.py`.

## Topic configs

```bash
kubectl exec -it kafka-0 -- kafka-topics \
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
```

### Consumer configs

Reads the payments topic. Tuned for the < 50 ms SLA: a fetch returns as soon as one byte is
available (`fetch.min.bytes=1`) and waits at most 10 ms (`fetch.max.wait.ms=10`).

```bash
kubectl exec -it kafka-0 -- kafka-console-consumer \
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
  --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

### Producer configs

No batching delay (`linger.ms=0`) and fast lz4 compression, again for the 50 ms SLA.

```bash
kubectl exec -i kafka-1 -- kafka-console-producer \
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
```
