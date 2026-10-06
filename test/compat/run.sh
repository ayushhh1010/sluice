#!/usr/bin/env bash
# Client compatibility matrix. Starts a local cluster (sluice-dev over a
# directory store) unless BOOTSTRAP is set, then runs every client in Docker
# so CI needs nothing but Go and Docker.
set -euo pipefail
cd "$(dirname "$0")/../.."
if [ -z "${BOOTSTRAP:-}" ]; then
  go build -o bin/ ./cmd/...
  rm -rf /tmp/sluice-compat
  ./bin/sluice-dev -store file:///tmp/sluice-compat -http-base 0 >/tmp/sluice-compat.log 2>&1 &
  trap 'kill $! 2>/dev/null || true' EXIT
  export BOOTSTRAP=127.0.0.1:9092
  for _ in $(seq 1 50); do (echo > /dev/tcp/127.0.0.1/9092) 2>/dev/null && break; sleep 0.2; done
fi
export SLUICECTL=${SLUICECTL:-./bin/sluicectl}

echo "== franz-go"
go test ./test/e2e/ -count=1

echo "== kcat (librdkafka)"
KCAT="docker run --rm -i --network host edenhill/kcat:1.7.1" test/compat/kcat.sh

echo "== kafka-python and confluent-kafka-python"
docker run --rm --network host -e BOOTSTRAP -v "$PWD/test/compat:/t" python:3.12-slim \
  sh -c "pip install -q kafka-python confluent-kafka && python /t/kafka_python_test.py && python /t/confluent_test.py"
echo "compatibility matrix: PASS"
