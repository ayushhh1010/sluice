#!/usr/bin/env bash
# kcat (librdkafka) round trip: 10,000 messages produced and consumed back.
# Usage: BOOTSTRAP=host:9092 ./kcat.sh   (requires kcat and sluicectl or Docker)
set -euo pipefail
B=${BOOTSTRAP:-localhost:9092}
T=compat-kcat-$RANDOM$RANDOM
N=10000
KCAT=${KCAT:-kcat}
SLUICECTL=${SLUICECTL:-sluicectl}

$KCAT -b "$B" -L >/dev/null
$SLUICECTL create -b "$B" -topic "$T" -partitions 3
seq 1 $N | $KCAT -b "$B" -t "$T" -P -X linger.ms=50
got=$($KCAT -b "$B" -t "$T" -C -o beginning -e -q | sort -n | uniq | wc -l | tr -d ' ')
if [ "$got" != "$N" ]; then
  echo "kcat: consumed $got unique messages, want $N" >&2
  exit 1
fi
echo "kcat: PASS ($N messages round-tripped through $T)"
