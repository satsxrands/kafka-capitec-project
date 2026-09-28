#!/usr/bin/env bash
# Verification: consumer lag for every group, ordering per payment, end-to-end latency.
set -euo pipefail
cd "$(dirname "$0")"
E=../evidence

echo "## Consumer groups and lag" > "$E/07-consumer-lag.txt"
for g in fraud-scoring-service notification-service payment-audit-archiver notification-dispatcher; do
  kubectl exec kafka-0 -- kafka-consumer-groups --bootstrap-server kafka-service:9092 \
    --describe --group "$g" 2>/dev/null >> "$E/07-consumer-lag.txt"
done
awk '/^[a-z]/ && $6 ~ /^[0-9]+$/ {lag[$1]+=$6; n[$1]++} END{for (g in lag) printf "%-24s partitions=%-4d total lag=%d\n", g, n[g], lag[g]}' \
  "$E/07-consumer-lag.txt" | sort | tee "$E/07-lag-summary.txt"

python3 verify_ordering.py "$E/audit-archive.txt" | tee "$E/08-ordering.txt"

dump() {  # read a whole topic with record timestamps, without joining a group
  kubectl exec kafka-0 -- kafka-console-consumer --bootstrap-server kafka-service:9092 \
    --topic "$1" --from-beginning --timeout-ms 10000 \
    --property print.timestamp=true --property print.key=true --property key.separator='|' 2>/dev/null \
    | sed 's/\t/|/'
}
dump payments.payment-lifecycle.v1 > /tmp/ts-payments.txt
dump fraud.fraud-score.v1 > /tmp/ts-fraud.txt
dump notifications.payment-notification.v1 > /tmp/ts-notif.txt
python3 verify_latency.py /tmp/ts-payments.txt /tmp/ts-fraud.txt /tmp/ts-notif.txt | tee "$E/09-latency.txt"
