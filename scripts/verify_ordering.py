#!/usr/bin/env python3
"""Checks, from the audit consumer's output, that every payment's events sit in ONE
partition, in lifecycle order, at increasing offsets. Prints two full journeys."""
import json, sys
from collections import defaultdict

RANK = {"PAYMENT_INITIATED": 0, "PAYMENT_AUTHORISED": 1, "PAYMENT_NOT_AUTHORISED": 1,
        "PAYMENT_VALIDATED": 2, "PAYMENT_INVALIDATED": 2,
        "PAYMENT_COMPLETED": 3, "PAYMENT_NOT_COMPLETED": 3}

by_payment = defaultdict(list)
for line in open(sys.argv[1]):
    part, off, key, value = line.rstrip("\n").split("|", 3)
    e = json.loads(value)
    by_payment[key].append((int(part.split(":")[1]), int(off.split(":")[1]), e["event_type"]))

bad = 0
for pid, evs in by_payment.items():
    evs.sort(key=lambda x: x[1])
    parts = {p for p, _, _ in evs}
    ranks = [RANK[t] for _, _, t in evs]
    if len(parts) != 1 or ranks != sorted(ranks) or ranks[0] != 0:
        bad += 1
        print("ORDER VIOLATION", pid, evs)

print(f"payments checked: {len(by_payment)}, events: {sum(map(len, by_payment.values()))}, "
      f"violations: {bad}")
shown = set()
for pid, evs in sorted(by_payment.items()):
    last = evs[-1][2]
    kind = "success" if last == "PAYMENT_COMPLETED" else "failure"
    if kind in shown or len(evs) < 3:
        continue
    shown.add(kind)
    print(f"\nJourney ({kind}) {pid}: partition {evs[0][0]}")
    for p, o, t in evs:
        print(f"  offset {o:>2}  {t}")
sys.exit(1 if bad else 0)
