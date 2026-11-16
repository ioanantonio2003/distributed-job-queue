#!/usr/bin/env bash
# Sends waves of jobs so the Grafana dashboard has something to show: the queue fills, workers drain it, and a few
# jobs fail on purpose. Usage: scripts/load-wave.sh        (needs: docker compose up --build --scale worker=3)
# Tunables (environment): WAVES, JOBS (per wave, at most 1000), PAUSE (seconds between waves), MILLIS (work per job)
set -euo pipefail
body_file="$(mktemp)"
trap 'rm -f "$body_file"' EXIT

BASE_URL="${BASE_URL:-http://localhost:8080}"
WAVES="${WAVES:-6}"
JOBS="${JOBS:-400}"
PAUSE="${PAUSE:-8}"
MILLIS="${MILLIS:-250}"

# JOBS normal jobs, plus 10 that fail once and succeed on the retry, plus 3 that always fail and end up DEAD.
normal="{\"queue\":\"default\",\"type\":\"sleep\",\"payload\":{\"millis\":$MILLIS}}"
flaky='{"queue":"default","type":"flaky","payload":{"failTimes":1}}'
broken='{"queue":"default","type":"always-fail","payload":{},"maxAttempts":2}'

for wave in $(seq 1 "$WAVES"); do
  jobs=$(seq 1 "$JOBS" | sed "s|.*|$normal,|" | tr -d '\n')
  jobs="$jobs$(seq 1 10 | sed "s|.*|$flaky,|" | tr -d '\n')$(seq 1 3 | sed "s|.*|$broken,|" | tr -d '\n')"
  # The body is too large for a command-line argument on Windows, so it goes through a file.
  echo "{\"jobs\":[${jobs%,}]}" > "$body_file"
  curl -sf -X POST "$BASE_URL/jobs/batch" -H 'Content-Type: application/json' --data-binary "@$body_file" >/dev/null
  echo "wave $wave/$WAVES sent ($((JOBS + 13)) jobs)"
  [ "$wave" -lt "$WAVES" ] && sleep "$PAUSE"
done
echo "done: watch http://localhost:3000"
