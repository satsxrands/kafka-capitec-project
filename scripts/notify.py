#!/usr/bin/env python3
"""Notification consumer logic: reads `key|json` payment events, keeps validated and
failed ones, writes `key:json` notification events."""
import json, sys
from datetime import datetime, timezone

FAILED = {"PAYMENT_NOT_AUTHORISED", "PAYMENT_INVALIDATED", "PAYMENT_NOT_COMPLETED"}

for line in sys.stdin:
    key, _, value = line.rstrip("\n").partition("|")
    try:
        e = json.loads(value)
    except json.JSONDecodeError:
        print(f"SKIP unparseable record key={key}", file=sys.stderr)
        continue
    t = e.get("event_type")
    if t == "PAYMENT_VALIDATED":
        kind, text = "APPROVED", "Your payment of R{:.2f} has been approved."
    elif t in FAILED:
        kind, text = "FAILED", "Your payment of R{:.2f} could not be processed."
    else:
        continue  # filter: only validated + failed events notify the customer

    out = {
        "event_id": f"notif-{e['event_id']}",
        "event_type": f"NOTIFICATION_PAYMENT_{kind}",
        "schema_version": 1,
        "occurred_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "payment_id": e["payment_id"],
        "customer_id": e["customer_id"],
        "source_event_id": e["event_id"],
        "channel": "PUSH",
        "message": text.format(e["amount_cents"] / 100),
        **({"reason_code": e["reason_code"]} if "reason_code" in e else {}),
    }
    print(f"{e['payment_id']}:{json.dumps(out, separators=(',', ':'))}", flush=True)
