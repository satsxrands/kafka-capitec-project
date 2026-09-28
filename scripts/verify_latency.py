#!/usr/bin/env python3
"""Measures end-to-end latency from Kafka record timestamps (CreateTime):
payment event written -> derived fraud / notification event written.
Input files: `CreateTime:<ms>|<key>|<json>` lines from the console consumer."""
import json, statistics, sys

def load(path):
    out = []
    for line in open(path):
        ts, key, value = line.rstrip("\n").split("|", 2)
        out.append((int(ts.split(":")[1]), json.loads(value)))
    return out

payments = {e["event_id"]: ts for ts, e in load(sys.argv[1])}
for label, path, sla in (("fraud", sys.argv[2], 50), ("notification", sys.argv[3], 2000)):
    # In send order, so the cold start (first sends of a fresh producer fetch metadata
    # and a producer id) can be reported separately from steady state.
    rows = sorted((payments[e["source_event_id"]], ts - payments[e["source_event_id"]])
                  for ts, e in load(path))
    in_order = [l for _, l in rows]
    for scope, lat in (("all", in_order), ("steady state (after first 10)", in_order[10:])):
        lat = sorted(lat)
        p95 = lat[int(0.95 * (len(lat) - 1))]
        print(f"{label:>12} {scope:<30} n={len(lat):<3} min={lat[0]} ms  median={statistics.median(lat):.0f} ms  "
              f"p95={p95} ms  max={lat[-1]} ms  (SLA < {sla} ms)")
    print(f"{label:>12} first 10 in send order: {in_order[:10]}")
