#!/usr/bin/env python3
"""Fraud consumer logic: reads `key|json` payment events on stdin, keeps only
PAYMENT_INITIATED, scores each one and writes `key:json` fraud events on stdout."""
import json, sys
from datetime import datetime, timezone

for line in sys.stdin:
    key, _, value = line.rstrip("\n").partition("|")
    try:
        e = json.loads(value)
    except json.JSONDecodeError:
        print(f"SKIP unparseable record key={key}", file=sys.stderr)
        continue
    if e.get("event_type") != "PAYMENT_INITIATED":
        continue  # filter: the fraud consumer only scores initiations

    score, reasons = 0, []
    if e["amount_cents"] >= 2_000_000:
        score += 40; reasons.append("HIGH_AMOUNT")
    if e["device_id"].startswith("DEV-NEW-"):
        score += 30; reasons.append("NEW_DEVICE")
    if e["geo_country"] != "ZA":
        score += 30; reasons.append("FOREIGN_GEO")
    band = "HIGH" if score >= 60 else "MEDIUM" if score >= 30 else "LOW"

    out = {
        "event_id": f"fraud-{e['event_id']}",
        "event_type": f"FRAUD_SCORE_{band}",
        "schema_version": 1,
        "occurred_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "payment_id": e["payment_id"],
        "source_event_id": e["event_id"],
        "score": score,
        "band": band,
        "reasons": reasons,
    }
    print(f"{e['payment_id']}:{json.dumps(out, separators=(',', ':'))}", flush=True)
