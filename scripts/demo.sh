#!/usr/bin/env bash
# Sends a batch of slow jobs, waits until they all succeed, and shows which workers ran them.
# Usage: scripts/demo.sh [job-count]      (needs: docker compose up --build --scale worker=3)
# Runs in Git Bash or WSL on Windows.
set -euo pipefail
cd "$(dirname "$0")/.."

BASE_URL="${BASE_URL:-http://localhost:8080}"
COUNT="${1:-12}"
PSQL="docker compose exec -T postgres psql -U ${POSTGRES_USER:-jobqueue} -d ${POSTGRES_DB:-jobqueue} -At"

# Each job sleeps 1 s, so with several workers the jobs visibly spread across them.
jobs=""
for i in $(seq 1 "$COUNT"); do
  jobs="${jobs}{\"queue\":\"default\",\"type\":\"sleep\",\"payload\":{\"millis\":1000}},"
done
response=$(curl -sf -X POST "$BASE_URL/jobs/batch" -H 'Content-Type: application/json' \
  -d "{\"jobs\":[${jobs%,}]}")

# The response is {"ids":["...","..."]}; turn it into a SQL list: 'id1','id2',...
ids=$(echo "$response" | grep -oE '[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}' | sed "s/.*/'&'/" | paste -sd, -)
echo "Enqueued $COUNT jobs. Waiting for them to succeed..."

for _ in $(seq 1 60); do
  done_count=$($PSQL -c "SELECT count(*) FROM jobs WHERE status = 'SUCCEEDED' AND id IN ($ids)")
  echo "  SUCCEEDED: $done_count / $COUNT"
  [ "$done_count" -eq "$COUNT" ] && break
  sleep 1
done

echo
echo "Jobs executed per worker:"
$PSQL -F ' : ' -c "SELECT worker_id, count(*) FROM job_attempts WHERE job_id IN ($ids) GROUP BY worker_id ORDER BY worker_id"

if [ "$done_count" -ne "$COUNT" ]; then
  echo "Not all jobs finished within 60 s." >&2
  exit 1
fi
