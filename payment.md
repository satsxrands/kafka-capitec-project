# Payment producer, topic/s and event/s

One topic holds the whole payment lifecycle, keyed by `payment_id`, so every event of one
payment lands on the same partition in order. Full rationale: `payment_events_setup.md`.

Events published (brief section 1):
`PAYMENT_INITIATED` -> `PAYMENT_AUTHORISED` | `PAYMENT_NOT_AUTHORISED`
-> `PAYMENT_VALIDATED` | `PAYMENT_INVALIDATED` -> `PAYMENT_COMPLETED` | `PAYMENT_NOT_COMPLETED`

## Topic configs

```bash
kubectl exec -it kafka-0 -- kafka-topics \
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
```

### Consumer configs

The audit / reconciliation consumer reads every payment event (`scripts/40-audit-consumer.sh`).
The fraud and notification consumers of this topic are in `fraud.md` and `notification.md`.

Client settings are passed with `--consumer-property`. The template's `--property` only
configures the console tool's output formatter; client settings given that way are
silently ignored (proved in `payment_events_setup.md`, Issues Encountered).

```bash
kubectl exec -it kafka-0 -- kafka-console-consumer \
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
  --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

### Producer configs

`scripts/10-produce-payments.sh` pipes `scripts/generate_payments.py` into this command.

```bash
kubectl exec -i kafka-0 -- kafka-console-producer \
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
```
