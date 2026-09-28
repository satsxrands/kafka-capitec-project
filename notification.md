# Notification producer, consumer, topic/s and event/s

The notification consumer reads `PAYMENT_VALIDATED` and the three failure events
(`PAYMENT_NOT_AUTHORISED`, `PAYMENT_INVALIDATED`, `PAYMENT_NOT_COMPLETED`) from the payments
topic. The notification producer publishes `NOTIFICATION_PAYMENT_APPROVED` or
`NOTIFICATION_PAYMENT_FAILED`, keyed by `payment_id`. Chained in
`scripts/30-notification-service.sh`; logic in `scripts/notify.py`. A separate
`notification-dispatcher` group reads this topic and sends the message
(`scripts/50-notification-dispatcher.sh`).

## Topic configs

```bash
kubectl exec -it kafka-0 -- kafka-topics \
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
```

### Consumer configs

Reads the payments topic. The 2 s SLA leaves room to batch: a fetch waits for 16 KB or
500 ms, whichever comes first, which cuts request count at peak load.

```bash
kubectl exec -it kafka-0 -- kafka-console-consumer \
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
  --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

### Producer configs

```bash
kubectl exec -i kafka-2 -- kafka-console-producer \
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
```
