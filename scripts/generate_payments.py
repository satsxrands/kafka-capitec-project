#!/usr/bin/env python3
"""Generates mock payment lifecycle events as `key:json` lines for the console producer.

Every event of one payment carries the same key (payment_id), so all of them land on
the same partition and stay in order. Journeys (per brief section 1):
  initiated -> authorised | not authorised
  authorised -> validated | invalidated
  validated -> completed | not completed
"""
import json, random, sys, time, uuid
from datetime import datetime, timedelta, timezone

N = int(sys.argv[1]) if len(sys.argv) > 1 else 60
# Gap between events on stdout. 0 = one burst. A burst queues every event behind the
# previous one's acks=all round trip and inflates latency (see Issues Encountered).
PACE_MS = int(sys.argv[2]) if len(sys.argv) > 2 else 0
rng = random.Random(42)  # fixed seed: the same run reproduces the same evidence

CHANNELS = ["APP", "WEB", "USSD", "CARD"]
TYPES = ["EFT", "RTC", "PAYSHAP", "CARD_PURCHASE"]
# Outcome mix: most complete; the rest fail at each possible step.
OUTCOMES = ["COMPLETED"] * 7 + ["NOT_AUTHORISED", "INVALIDATED", "NOT_COMPLETED"]
REASONS = {
    "NOT_AUTHORISED": "INSUFFICIENT_FUNDS",
    "INVALIDATED": "BENEFICIARY_ACCOUNT_CLOSED",
    "NOT_COMPLETED": "CLEARING_TIMEOUT",
}

def event(payment, event_type, at, reason=None):
    e = {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
        "event_type": event_type,
        "schema_version": 1,
        "occurred_at": at.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        **payment,
    }
    if reason:
        e["reason_code"] = reason
    return f"{payment['payment_id']}:{json.dumps(e, separators=(',', ':'))}"

start = datetime.now(timezone.utc)
lines = []
for i in range(N):
    outcome = rng.choice(OUTCOMES)
    risky = rng.random() < 0.15
    # A normal payment from a device we have not seen before: medium risk, not high.
    new_device = risky or rng.random() < 0.15
    payment = {
        "payment_id": f"PAY-{start:%Y%m%d}-{i:05d}",
        "customer_id": f"CUST-{rng.randint(1000, 9999)}",
        "source_account_masked": f"****{rng.randint(1000, 9999)}",
        "beneficiary_account_masked": f"****{rng.randint(1000, 9999)}",
        "amount_cents": rng.randint(50_000, 5_000_000) if risky else rng.randint(1_000, 250_000),
        "currency": "ZAR",
        "channel": rng.choice(CHANNELS),
        "payment_type": rng.choice(TYPES),
        "device_id": f"DEV-NEW-{rng.randint(100, 999)}" if new_device else f"DEV-{rng.randint(100, 999)}",
        "geo_country": "ZA" if not risky else rng.choice(["NG", "RU", "ZA"]),
    }
    t = start + timedelta(milliseconds=i * 40)
    step = lambda ms: t + timedelta(milliseconds=ms)
    lines.append(event(payment, "PAYMENT_INITIATED", step(0)))
    if outcome == "NOT_AUTHORISED":
        lines.append(event(payment, "PAYMENT_NOT_AUTHORISED", step(20), REASONS[outcome]))
        continue
    lines.append(event(payment, "PAYMENT_AUTHORISED", step(20)))
    if outcome == "INVALIDATED":
        lines.append(event(payment, "PAYMENT_INVALIDATED", step(45), REASONS[outcome]))
        continue
    lines.append(event(payment, "PAYMENT_VALIDATED", step(45)))
    if outcome == "NOT_COMPLETED":
        lines.append(event(payment, "PAYMENT_NOT_COMPLETED", step(90), REASONS[outcome]))
    else:
        lines.append(event(payment, "PAYMENT_COMPLETED", step(90)))

for line in lines:
    print(line, flush=True)
    if PACE_MS:
        time.sleep(PACE_MS / 1000)
