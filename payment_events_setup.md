# Payment Events Streaming: Design and Setup

Proof of concept for the Digital Payments event streaming system (`project.md`): a payment
producer, a fraud consumer/producer, a notification consumer/producer, an audit consumer
and a notification dispatcher, on a 3-broker Kafka cluster.

Everything below was run for real. Raw output is in `evidence/`, and the whole demo reruns
from a clean cluster with `scripts/99-reset.sh && scripts/80-run-demo.sh`.

## Contents

1. [Architecture](#1-architecture)
2. [Design Decisions](#2-design-decisions)
3. [Cluster Setup](#3-cluster-setup)
4. [Topic Creation](#4-topic-creation)
5. [Producer Setup](#5-producer-setup)
6. [Consumer Groups](#6-consumer-groups)
7. [Verification](#7-verification)
8. [Trade-offs and Justifications](#8-trade-offs-and-justifications)
9. [Issues Encountered](#9-issues-encountered)
10. [Conclusions](#10-conclusions)

---

## 1. Architecture

```
                        payments.payment-lifecycle.v1  (100 partitions, key = payment_id)
 payment producer ───►  INITIATED, AUTHORISED / NOT_AUTHORISED, VALIDATED / INVALIDATED,
                        COMPLETED / NOT_COMPLETED
                               │                    │                         │
             fraud-scoring-service      notification-service       payment-audit-archiver
             (INITIATED only)           (VALIDATED + failures)     (every event)
                               │                    │                         │
                               ▼                    ▼                         ▼
                 fraud.fraud-score.v1   notifications.payment-      archive / reconciliation
                 (24 partitions)        notification.v1 (30)
                 SCORE_LOW/MEDIUM/HIGH  PAYMENT_APPROVED / FAILED
                                                    │
                                          notification-dispatcher
                                          (sends push / SMS)
```

Four consumer groups (the brief asks for at least 3), each independent: every group gets
its own copy of the stream and its own offsets, so a slow audit job never delays fraud
scoring.

---

## 2. Design Decisions

### 2.1 Sizing from the requirements

| Requirement (brief) | Value |
|---|---|
| Peak throughput | 1000 Mb/s |
| One producer instance | 100 Mb/s |
| One consumer instance | 10 Mb/s |

- **Producers needed at peak:** 1000 / 100 = **10 producer instances**.
- **Payments topic partitions:** a partition is consumed by at most one member of a group,
  so to keep up at peak a group needs 1000 / 10 = **100 consumers, hence 100 partitions**.
- **Units:** "Mb/s" is read as megabits. The partition count does not depend on it, because
  peak and per-consumer rates use the same unit and only the ratio matters. The unit only
  changes storage estimates (x8 if megabytes).
- **Fraud and notification topics** carry less data. Measured on the mock stream
  (`evidence/*-events.txt`):

  | Topic | Events vs payments | Avg size | Share of payment volume | Peak | Min partitions | Chosen |
  |---|---|---|---|---|---|---|
  | payments | 219 | 423 B | 100% | 1000 Mb/s | 100 | **100** |
  | fraud | 60 (one per payment) | 299 B | 19.3% | ~193 Mb/s | 20 | **24** |
  | notifications | 66 | 384 B | 27.3% | ~273 Mb/s | 28 | **30** |

### 2.2 Topic design

| Setting | payments.payment-lifecycle.v1 | fraud.fraud-score.v1 | notifications.payment-notification.v1 |
|---|---|---|---|
| Partitions | 100 | 24 | 30 |
| Replication factor | 3 | 3 | 3 |
| min.insync.replicas | 2 | 2 | 2 |
| retention.ms | 220,924,800,000 (7 years) | 220,924,800,000 (7 years) | 604,800,000 (7 days) |
| retention.bytes | -1 (no size cap) | -1 | -1 |
| cleanup.policy | delete | delete | delete |
| compression.type | zstd | lz4 | lz4 |
| max.message.bytes | 262,144 (256 KB) | 262,144 | 262,144 |

**Topic names.** `<domain>.<entity>.v<schema-major>`: `payments.payment-lifecycle.v1`.
Domain first groups topics by owning team for ACLs and dashboards; the version suffix
lets a breaking schema change ship as `.v2` alongside `.v1` while consumers migrate.

**Partitions.** From 2.1. Different payment types (EFT, RTC, PayShap, card) share one
topic: fraud and audit need them all, and splitting by type would split one customer's
activity across topics and lose the cross-type view fraud relies on.

**Replication factor 3.** The requirement is zero data loss. With 3 copies, one broker can
fail (or be patched) and every partition still has 2 in-sync copies. RF 2 would leave a
single copy during any maintenance window; RF 4+ adds storage and replication traffic
for a failure mode (3 brokers down at once) that needs a different answer (multi-site).

**min.insync.replicas 2.** Together with the producer's `acks=all`, a write is only
confirmed once 2 brokers have it. So a confirmed payment survives the loss of any one
broker. If 2 brokers are down the topic refuses writes (`NotEnoughReplicasException`)
rather than accepting data it could lose: for payments, unavailable is better than lost.
Setting it to 3 would make every broker restart an outage.

**Retention: 7 years, no size cap, for payment and fraud events.** The regulatory hold is
7 years. 7 x 365.25 = 2556.75 days, rounded up to 2557 days (220,924,800,000 ms) so leap
years can never make a record expire a day early. Retention is also a live topic setting:
if the regulation changes, one `kafka-configs --alter` command updates it with no data
rewrite. Time-based only, with `retention.bytes=-1`, because
a size cap deletes the oldest data when the disk fills, which could silently break the
retention promise; storage is managed by capacity planning (or tiered storage), not by
deleting records. Notifications are operational only: 7 days covers replay after an
outage, and the audit trail lives in the payments topic.

**Cleanup policy delete, not compact.** Payment events are immutable facts: "initiated at
09:11" never changes, a later event records the next state. The topic is an event
stream, not a state store. Compaction keeps only the latest record per key, so it would
erase the INITIATED and AUTHORISED history that audit and reconciliation need.

**Compression.** Payment events are repetitive JSON (the same field names every time),
which compresses well. Payments use **zstd**: the best ratio, which matters most on the
topic holding 7 years of data, at a CPU cost that fits the 150 ms budget. Fraud and
notification topics use **lz4**, the fastest codec, because their SLAs are tighter and
their retention is short or small. Setting the codec on the topic as well as the
producer means the broker never recompresses.

**max.message.bytes 256 KB.** A payment event is ~0.4 KB. A much lower ceiling than the
1 MB default turns a bug (someone attaching a document to an event) into a loud
rejection instead of a slow topic.

### 2.3 Message schema

JSON, one object per event. Shared envelope plus payment fields:

| Field | Type | Req. | Purpose |
|---|---|---|---|
| event_id | UUID string | yes | Unique per event; consumers de-duplicate on it |
| event_type | enum string | yes | Lifecycle state, e.g. `PAYMENT_AUTHORISED` |
| schema_version | int | yes | Lets consumers handle old and new shapes |
| occurred_at | ISO-8601 UTC | yes | Business time of the state change |
| payment_id | string | yes | Partition key; ties a payment's events together |
| customer_id | string | yes | Notifications and fraud velocity checks |
| source_account_masked | string | yes | Last 4 digits only, for display and reconciliation |
| beneficiary_account_masked | string | yes | Last 4 digits only |
| amount_cents | int | yes | Integer cents: no floating-point rounding on money |
| currency | ISO-4217 string | yes | `ZAR` |
| channel | enum | yes | APP / WEB / USSD / CARD |
| payment_type | enum | yes | EFT / RTC / PAYSHAP / CARD_PURCHASE |
| device_id | string | yes | Fraud: new-device signal |
| geo_country | ISO-3166 string | yes | Fraud: location signal |
| reason_code | enum | failures only | Why a payment failed, e.g. `INSUFFICIENT_FUNDS` |

**PII.** No names, full account numbers, ID numbers or card numbers go on the topic:
accounts are masked to the last 4 digits and customers are referenced by an internal id.
A topic kept for years is copied into many consumers and archives, so the less
identifiable data it holds, the smaller the breach and POPIA exposure.

Fraud and notification events carry `payment_id`, `source_event_id` (the payment event
they were derived from, for tracing) and their own fields: `score`, `band`, `reasons` for
fraud; `customer_id`, `channel`, `message`, `reason_code` for notifications.

### 2.4 Producer design

| Setting | Payments | Fraud | Notifications | Why |
|---|---|---|---|---|
| acks | all | all | all | Confirmed only when min.insync (2) brokers have it |
| enable.idempotence | true | true | true | Broker drops duplicates caused by retries |
| retries | 2147483647 | same | same | Retry until the delivery timeout, not a fixed count |
| max.in.flight.requests.per.connection | 5 | 5 | 5 | Highest value that keeps order with idempotence |
| delivery.timeout.ms | 30000 | 30000 | 30000 | Longest a payment can be "stuck" in the producer |
| request.timeout.ms | 10000 | 10000 | 10000 | One broker request |
| linger.ms | 5 | 0 | 10 | Batching delay, sized to each SLA |
| batch.size | 65536 | default | default | Bigger batches compress better on the busy topic |
| compression.type | zstd | lz4 | lz4 | See 2.2 |

**Event types.** The seven lifecycle events in `payment.md`. All state transitions are
captured, failures included: a missing failure event means a customer is never told
their payment failed and reconciliation cannot close the day.

**Partition key: `payment_id`.** Kafka orders records only within a partition, and a key
always maps to the same partition. Keying by `payment_id` guarantees INITIATED is stored
before AUTHORISED before COMPLETED for each payment, which audit and reconciliation need.
It also spreads load evenly (millions of distinct payments). `customer_id` was
considered, for per-customer fraud velocity, but a few very active customers or
merchants would make hot partitions; velocity is better computed inside the fraud
service with a keyed state store.

**Durability: `acks=all`.** Losing a payment event means money moved with no record of it.
That cost is far higher than a few milliseconds of latency.

**Idempotency.** If a network blip hides a successful write, the producer retries and
could store the event twice. With `enable.idempotence=true` the broker tracks a
producer id and sequence number per partition and silently discards the duplicate.
This covers producer retries only; a payment submitted twice by the application gets
two different `event_id`s, which is why consumers also de-duplicate on `event_id`.

**Serialization: JSON for the POC, Avro in production.** The console tools read and
write text, so JSON keeps the POC runnable and readable. In production, Avro with a
Schema Registry: records are much smaller (no field names repeated in every record,
which matters over 7 years of retention), and the registry rejects incompatible schema
changes before they reach consumers. Protobuf is equally valid; Avro fits the
Kafka/Confluent tooling best.

**Error handling.** Retry quietly for up to 30 s (`delivery.timeout.ms`), which covers a
leader election or a broker restart. After that the send fails, and the payment
service must fail the payment visibly to the customer rather than leave it pending
forever. A payment silently queued for minutes is worse than a clear "try again".

**Scale of the test.** 60 payments, 219 events, paced at one event every 20 ms, mixing
every outcome: 39 completed, 6 not authorised, 9 invalidated, 6 not completed, plus a
spread of fraud risk (49 low, 6 medium, 5 high).

### 2.5 Consumer design

| Group | Reads | Filters to | Lag SLA | Output |
|---|---|---|---|---|
| fraud-scoring-service | payments | PAYMENT_INITIATED | < 50 ms | FRAUD_SCORE_* to fraud topic |
| notification-service | payments | VALIDATED + 3 failure types | < 2 s | NOTIFICATION_PAYMENT_* to notifications topic |
| payment-audit-archiver | payments | none (all events) | minutes | Archive for audit and reconciliation |
| notification-dispatcher | notifications | none | < 2 s end to end | Push / SMS send |

**fraud-scoring-service.** Scores every initiation before the payment is authorised, so
latency is on the payment's critical path: 50 ms. Fetch settings return as soon as data
arrives. Scoring (`scripts/fraud_score.py`): +40 amount >= R20,000, +30 new device, +30
foreign location; >= 60 HIGH, >= 30 MEDIUM, else LOW. Unparseable records are logged and
skipped so one bad record cannot stop scoring. Offsets: manual commit after the fraud
event is written, in production (see 8.4). Duplicates are harmless (scoring is
deterministic, output keyed by payment); a missed event is not acceptable (a payment goes
unscored). Scales to 100 instances, one per partition.

**notification-service.** Customers expect a message within seconds, not milliseconds:
2 s. That allows fetch batching (16 KB or 500 ms) to cut broker requests at peak.
Failures (not authorised, invalidated, not completed) and validations produce a
notification. A duplicate notification is annoying but tolerable; `event_id` de-dup in
the dispatcher prevents most of them. Scales to 100 instances.

**payment-audit-archiver.** Reads everything for the audit and reconciliation store. Lag of
minutes is fine; completeness is what matters. Large fetches (1 MB or 5 s) favour
throughput. Can never skip an event, so manual commit after the archive write in
production. Scales to 100 instances but in practice runs fewer, since it can batch.

**notification-dispatcher.** Sends the notification. Separated from notification-service so
a slow SMS provider backs up only this group, not the reading of payment events.

**Common settings.** `auto.offset.reset=earliest` so a new group starts from the beginning
rather than skipping existing events. `CooperativeStickyAssignor` so scaling a group up
or down moves only the partitions that must move, instead of pausing the whole group
(which would breach the fraud SLA during every deploy).

---

## 3. Cluster Setup

3 brokers in KRaft mode (Kafka's built-in controller quorum; no Zookeeper needed from
Kafka 3.3), image `confluentinc/cp-kafka:7.9.2` (Kafka 3.9), on Rancher Desktop's
Kubernetes (k3s on containerd). Names match the course templates: pod `kafka-0`, service
`kafka-service:9092`. Manifest: `infra/kafka.yaml`.

```bash
rdctl start --container-engine.name=containerd --kubernetes.enabled=true \
  --virtual-machine.memory-in-gb=8 --virtual-machine.number-cpus=4
kubectl config use-context rancher-desktop
kubectl apply -f infra/kafka.yaml
kubectl rollout status statefulset/kafka
kubectl exec kafka-0 -- kafka-metadata-quorum --bootstrap-server kafka-service:9092 describe --status
```

```
LeaderId:               2
CurrentVoters:          [{"id": 0, ... "CONTROLLER://kafka-0.kafka-headless:9093"},
                         {"id": 1, ... "CONTROLLER://kafka-1.kafka-headless:9093"},
                         {"id": 2, ... "CONTROLLER://kafka-2.kafka-headless:9093"}]
```

---

## 4. Topic Creation

Commands: `scripts/00-create-topics.sh` (also in `payment.md`, `fraud.md`,
`notification.md`). Describe output (`evidence/01-topics.txt`, first partitions shown):

```
Topic: payments.payment-lifecycle.v1  PartitionCount: 100  ReplicationFactor: 3
  Configs: compression.type=zstd,min.insync.replicas=2,cleanup.policy=delete,
           retention.ms=220924800000,max.message.bytes=262144,retention.bytes=-1
  Partition: 0  Leader: 1  Replicas: 1,2,0  Isr: 1,2,0
  Partition: 1  Leader: 2  Replicas: 2,0,1  Isr: 2,0,1
  Partition: 2  Leader: 0  Replicas: 0,1,2  Isr: 0,1,2
Topic: fraud.fraud-score.v1  PartitionCount: 24  ReplicationFactor: 3
  Configs: compression.type=lz4,min.insync.replicas=2,cleanup.policy=delete,
           retention.ms=220924800000,max.message.bytes=262144,retention.bytes=-1
Topic: notifications.payment-notification.v1  PartitionCount: 30  ReplicationFactor: 3
  Configs: compression.type=lz4,min.insync.replicas=2,cleanup.policy=delete,
           retention.ms=604800000,max.message.bytes=262144,retention.bytes=-1
```

Leaders are spread across all 3 brokers and every ISR has 3 members.

---

## 5. Producer Setup

`scripts/10-produce-payments.sh` pipes `scripts/generate_payments.py` (fixed random seed,
so reruns reproduce the same payments) into the console producer (command in
`payment.md`). Line format is `key:json`; `parse.key=true` makes `payment_id` the key.

**Events produced:** 219 for 60 payments.

| Event type | Count |
|---|---|
| PAYMENT_INITIATED | 60 |
| PAYMENT_AUTHORISED | 54 |
| PAYMENT_NOT_AUTHORISED | 6 |
| PAYMENT_VALIDATED | 45 |
| PAYMENT_INVALIDATED | 9 |
| PAYMENT_COMPLETED | 39 |
| PAYMENT_NOT_COMPLETED | 6 |

**Example events:**

```json
{"event_id":"37f8a88b-17fc-695a-07a0-ca6e0822e8f3","event_type":"PAYMENT_INITIATED",
 "schema_version":1,"occurred_at":"2026-09-28T14:36:50.807Z","payment_id":"PAY-20260928-00000",
 "customer_id":"CUST-5506","source_account_masked":"****5012","beneficiary_account_masked":"****4657",
 "amount_cents":1220528,"currency":"ZAR","channel":"APP","payment_type":"EFT",
 "device_id":"DEV-NEW-704","geo_country":"RU"}
```

```json
{"event_id":"05628059-568c-c69b-1064-005c3985c3cf","event_type":"PAYMENT_INVALIDATED",
 "schema_version":1,"occurred_at":"2026-09-28T14:36:51.212Z","payment_id":"PAY-20260928-00009",
 "customer_id":"CUST-4470","source_account_masked":"****9835","beneficiary_account_masked":"****4295",
 "amount_cents":187895,"currency":"ZAR","channel":"USSD","payment_type":"CARD_PURCHASE",
 "device_id":"DEV-787","geo_country":"ZA","reason_code":"BENEFICIARY_ACCOUNT_CLOSED"}
```

**Evidence of production:** the audit consumer read back all 219 records with their
partition and offset (`evidence/audit-archive.txt`), e.g. payment `PAY-20260928-00000`:

```
Partition:51|Offset:0|PAY-20260928-00000|{..."event_type":"PAYMENT_INITIATED"...}
Partition:51|Offset:1|PAY-20260928-00000|{..."event_type":"PAYMENT_AUTHORISED"...}
```

The 60 payments landed on 43 of the 100 partitions (with 60 keys and 100 partitions some
share a partition and some partitions stay empty, as expected from key hashing).

**Decisions made during testing:** the producer is paced (one event per 20 ms) rather than
dumping all events in one burst; see 9.3 for why.

---

## 6. Consumer Groups

Commands in `fraud.md`, `notification.md` and `payment.md`; scripts `20-` to `50-` in
`scripts/`. The fraud and notification services start first and wait for events, as they
would in production (`scripts/80-run-demo.sh`).

| Group | Consumed | Output | Result |
|---|---|---|---|
| fraud-scoring-service | 219 payment events | 60 fraud events | 49 LOW, 6 MEDIUM, 5 HIGH |
| notification-service | 219 payment events | 66 notifications | 45 APPROVED, 21 FAILED |
| payment-audit-archiver | 219 payment events | 219 archived | all events, with partition + offset |
| notification-dispatcher | 66 notifications | 66 dispatched | |

Checks: 60 fraud events = 60 initiated payments. 45 APPROVED = 45 validated. 21 FAILED =
6 not authorised + 9 invalidated + 6 not completed.

**Sample output: fraud** (`evidence/fraud-events.txt`):

```json
{"event_id":"fraud-37f8a88b-17fc-695a-07a0-ca6e0822e8f3","event_type":"FRAUD_SCORE_HIGH",
 "payment_id":"PAY-20260928-00000","source_event_id":"37f8a88b-17fc-695a-07a0-ca6e0822e8f3",
 "score":60,"band":"HIGH","reasons":["NEW_DEVICE","FOREIGN_GEO"]}
```

**Sample output: notification** (`evidence/notification-events.txt`):

```json
{"event_id":"notif-05628059-568c-c69b-1064-005c3985c3cf","event_type":"NOTIFICATION_PAYMENT_FAILED",
 "payment_id":"PAY-20260928-00009","customer_id":"CUST-4470","channel":"PUSH",
 "message":"Your payment of R1878.95 could not be processed.","reason_code":"BENEFICIARY_ACCOUNT_CLOSED"}
```

**Filtering** happens in the consumer (`fraud_score.py`, `notify.py`): each group reads
the whole payments topic and keeps only the event types it needs. **Error handling:**
a record that is not valid JSON is logged as `SKIP` and skipped, so one bad record cannot
block the partition behind it.

---

## 7. Verification

`scripts/90-verify.sh`. Output in `evidence/07-*` to `09-*`.

### 7.1 Consumer lag

```bash
kubectl exec kafka-0 -- kafka-consumer-groups --bootstrap-server kafka-service:9092 \
  --describe --group fraud-scoring-service
```

```
GROUP                 TOPIC                         PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
fraud-scoring-service payments.payment-lifecycle.v1 16         4               4               0
fraud-scoring-service payments.payment-lifecycle.v1 24         11              11              0
fraud-scoring-service payments.payment-lifecycle.v1 32         4               4               0
...
```

Summary over every partition (`evidence/07-lag-summary.txt`):

```
fraud-scoring-service    partitions=100  total lag=0
notification-dispatcher  partitions=30   total lag=0
notification-service     partitions=100  total lag=0
payment-audit-archiver   partitions=100  total lag=0
```

All four groups are fully caught up.

### 7.2 Event ordering

`scripts/verify_ordering.py` checks, for every payment, that all its events are on one
partition, at increasing offsets, in lifecycle order (initiated, then authorised, then
validated, then completed).

```
payments checked: 60, events: 219, violations: 0

Journey (success) PAY-20260928-00000: partition 51
  offset  0  PAYMENT_INITIATED
  offset  1  PAYMENT_AUTHORISED
  offset  2  PAYMENT_VALIDATED
  offset  3  PAYMENT_COMPLETED

Journey (failure) PAY-20260928-00009: partition 5
  offset  0  PAYMENT_INITIATED
  offset  1  PAYMENT_AUTHORISED
  offset  2  PAYMENT_INVALIDATED
```

The check was proven able to fail: swapping two offsets of one payment in a copy of the
archive makes it report `ORDER VIOLATION PAY-20260928-00000` and `violations: 1`.

### 7.3 End-to-end latency against the SLAs

Measured from Kafka record timestamps: payment event written, to the derived fraud or
notification event written (`scripts/verify_latency.py`, `evidence/09-latency.txt`).

| Path | Scope | Median | p95 | Max | SLA |
|---|---|---|---|---|---|
| Fraud | steady state | 10 ms | 12 ms | 15 ms | < 50 ms, met |
| Fraud | all 60, incl. cold start | 11 ms | 50 ms | 50 ms | first 7 at the limit |
| Notification | steady state | 260 ms | 480 ms | 493 ms | < 2000 ms, met |
| Notification | all 66 | 298 ms | 514 ms | 514 ms | met |

The first 7 fraud events took 49-50 ms, right at the limit, and every later one 7-15 ms: the fraud producer's
first send has to fetch topic metadata and register a producer id (needed for
idempotence). A production service does both at startup, before taking traffic.
The notification figures sit around 500 ms by design: that is `fetch.max.wait.ms=500`
batching, which a 2 s SLA can afford.

---

## 8. Trade-offs and Justifications

1. **Durability over latency.** `acks=all` + `min.insync.replicas=2` costs latency on every
   write. Measured: in steady state the fraud path still meets 50 ms with it, so there is
   no reason to weaken it. `acks=1` would lose confirmed payments if a leader died before
   replicating.
2. **One lifecycle topic vs a topic per event type.** One topic keeps each payment's
   events ordered in one partition. The cost: fraud and notification consumers read and
   discard events they do not need (fraud keeps ~27% of records). At 1000 Mb/s that is
   real network cost; if it became the bottleneck, a small router could republish only
   INITIATED events to a dedicated topic, keyed the same way.
3. **Partitions: exactly 100 has no headroom.** 100 partitions exactly matches peak
   / per-consumer throughput. Adding partitions later changes which partition a key
   hashes to, which breaks ordering for payments in flight. A production cluster would
   start with headroom (e.g. 120) or plan partition changes for a quiet window.
4. **Manual vs auto offset commit.** The design calls for manual commit after
   processing for fraud and audit (at-least-once: an event is never marked done before
   its output is written). The console consumer cannot commit manually, so the POC uses
   auto-commit every 1 s; a crash could skip up to 1 s of events. The production
   consumer would commit after the output write, and de-duplicate on `event_id`
   downstream to make redelivery harmless.
5. **zstd vs lz4.** zstd on the 7-year topic for storage, lz4 on the latency-sensitive
   topics for speed.
6. **JSON vs Avro.** JSON for a readable CLI POC; Avro + Schema Registry in production for
   size and enforced compatibility.

---

## 9. Issues Encountered

### 9.1 Brokers crash-looped on first start

All three pods restarted repeatedly with `java.net.UnknownHostException:
kafka-1.kafka-headless`. Kubernetes only publishes a pod's DNS name once it is Ready, but a
KRaft broker only opens port 9092 (the readiness check) after it has reached the other
controllers by DNS name. Neither could happen first. **Fix:** `publishNotReadyAddresses:
true` on the headless service, so brokers can find each other before they are ready.

### 9.2 The template's `--property` flags are silently ignored

The provided templates pass client settings as `--property acks=...`. `--property`
configures only the console tool's line reader and formatter. Proved by logging the
producer's effective configuration:

```
# --property acks=0 --property enable.idempotence=false
	acks = -1
	enable.idempotence = true
# --producer-property acks=0 --producer-property enable.idempotence=false
	acks = 0
	enable.idempotence = false
```

With `--property`, every tuned client setting is dropped and the defaults apply. Because
the defaults happen to be safe (`acks=all`), the mistake is invisible on payments, but
the fraud consumer's low-latency fetch settings would never take effect. **Fix:** all
commands use `--producer-property` / `--consumer-property`.

### 9.3 Fraud latency looked like 92 ms against a 50 ms SLA

The first run measured a steady ~92 ms on the fraud path. Narrowed down one variable at a
time with a probe pipeline:

| Probe | Median |
|---|---|
| Whole pipeline inside the broker pod (no kubectl, no Python) | 93 ms |
| Source topic with replication factor 1 | 35 ms |
| Replication factor 3, producer `acks=1` | 36 ms |
| Replication factor 3, `acks=all`, events 200 ms apart | 9 ms |

So neither the tooling nor replication itself was the cause. The first runs piped all
events into the producer in a single burst, and with `acks=all` each batch waited behind
the previous batch's replication round trip. **Fix:** the producer is paced like real
traffic (one event per 20 ms). In production, peak volume is spread over 10 producer
instances and 100 partitions rather than queued in one process.

### 9.4 Partition counts for fraud and notifications were first set too low

The first draft gave the fraud and notification topics 12 partitions each by judgement.
Applying the brief's 10 Mb/s-per-consumer rule to measured event sizes (2.1) showed they
need at least 20 and 28. **Fix:** 24 and 30.

### 9.5 No MEDIUM fraud scores in the first run

The first mock data produced only LOW and HIGH scores, but the brief requires low, medium
and high events. **Fix:** the generator adds a segment of ordinary payments from a new
device (score 30, MEDIUM).

---

## 10. Conclusions

- The design meets every acceptance criterion: topics created and verified, 219 events
  produced across full success and failure journeys, 4 independent consumer groups each
  producing output, ordering verified with a check that is proven able to fail, and zero
  lag on every group.
- Both latency SLAs are met in steady state, with `acks=all` durability kept.
- **Lessons:** Kafka's defaults can hide a broken configuration (9.2), so check the
  effective config rather than the command line. Measure latency under realistic traffic,
  not a burst (9.3). Derive every partition count from the throughput rule, not only the
  obvious topic (9.4).
- **Would do differently in production:** a real client (Java or Python) instead of the
  console tools, for manual commits, a startup warm-up and proper metrics; Avro + Schema
  Registry; partition headroom above 100; a broker-failure test showing writes continue
  with one broker down and are refused with two down.
