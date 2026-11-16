#!/usr/bin/env bash
# Reproducible benchmark of the queue itself: processing throughput and queue latency, with 1 and 3 workers.
# Usage: scripts/benchmark/run.sh        (Git Bash or WSL; needs Docker; takes about 20-30 minutes with the defaults)
# Tunables (environment): WORKERS_LIST ("1 3"), REPS (3), JOBS (20000), WARMUP_JOBS (5000),
#                         LATENCY_RATE (100 jobs/s), LATENCY_SECONDS (30)
#
# What is measured, all computed from the database (the source of truth), never from the client:
#   throughput  jobs/s = jobs / (last finished_at - first started_at) while draining a backlog that was loaded
#               while the workers were frozen (docker pause), so loading and draining do not overlap.
#   latency     created_at -> started_at of jobs sent at a steady LATENCY_RATE, as p50 / p95 / p99.
# Handler: "noop" (does nothing), so the queue machinery is what is measured. Raw output: scripts/benchmark/out/.
set -euo pipefail
cd "$(dirname "$0")/../.."

WORKERS_LIST="${WORKERS_LIST:-1 3}"
REPS="${REPS:-3}"
JOBS="${JOBS:-20000}"
WARMUP_JOBS="${WARMUP_JOBS:-5000}"
LATENCY_RATE="${LATENCY_RATE:-100}"
LATENCY_SECONDS="${LATENCY_SECONDS:-30}"
BASE_URL="${BASE_URL:-http://localhost:8080}"

RUN_ID="bench-$(date +%Y%m%d-%H%M%S)"
OUT="scripts/benchmark/out/$RUN_ID"
mkdir -p "$OUT"
CSV="$OUT/results.csv"

log() { echo "$(date +%T) $*" | tee -a "$OUT/run.log" >&2; }
sql() { docker compose exec -T postgres psql -U "${POSTGRES_USER:-jobqueue}" -d "${POSTGRES_DB:-jobqueue}" -At -c "$1"; }
not_finished() { sql "SELECT count(*) FROM jobs WHERE queue = '$1' AND status IN ('QUEUED', 'RUNNING')"; }

cleanup() {
  docker compose unpause worker >/dev/null 2>&1 || true
  docker compose stop server worker >/dev/null 2>&1 || true
  docker compose rm -f server worker >/dev/null 2>&1 || true
}
trap cleanup EXIT

# --- environment, written next to the results ------------------------------------------------------------------
capture_env() {
  {
    echo "date:        $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "commit:      $(git rev-parse --short HEAD 2>/dev/null || echo 'n/a')$(git diff --quiet 2>/dev/null || echo ' (+ uncommitted changes)')"
    echo "host OS:     $(cmd //c ver 2>/dev/null | tr -d '\r' | grep -v '^$' || uname -a)"
    echo "host CPU:    $(powershell -NoProfile -Command "(Get-CimInstance Win32_Processor | Select-Object -First 1).Name" 2>/dev/null | tr -d '\r' || echo 'n/a')"
    echo "host cores:  $(powershell -NoProfile -Command "(Get-CimInstance Win32_Processor | Measure-Object -Property NumberOfLogicalProcessors -Sum).Sum" 2>/dev/null | tr -d '\r' || echo 'n/a') logical"
    echo "host RAM:    $(powershell -NoProfile -Command "[math]::Round((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory / 1GB, 1)" 2>/dev/null | tr -d '\r' || echo 'n/a') GB"
    echo "docker:      $(docker version --format '{{.Server.Version}}') (engine OS: $(docker info --format '{{.OperatingSystem}}'), $(docker info --format '{{.NCPU}}') CPUs and $(docker info --format '{{.MemTotal}}' | awk '{ printf "%.1f", $1 / 1073741824 }') GB visible to containers)"
    echo "postgres:    $(sql 'SHOW server_version') (default configuration, shared_buffers $(sql 'SHOW shared_buffers'))"
    echo "java:        $(docker run --rm eclipse-temurin:21-jre-alpine java -version 2>&1 | head -1)"
    echo "worker cfg:  4 slots per queue, poll 500 ms backing off to 5 s, lease 30 s, heartbeat 10 s, default connection pool"
    echo "parameters:  workers=[$WORKERS_LIST] reps=$REPS jobs=$JOBS warmup=$WARMUP_JOBS latency=${LATENCY_RATE}/s for ${LATENCY_SECONDS}s"
  } | tee "$OUT/environment.txt" >&2
}

# --- helpers -----------------------------------------------------------------------------------------------------
make_body() { # make_body queue count file
  local job="{\"queue\":\"$1\",\"type\":\"noop\"}"
  echo "{\"jobs\":[$(seq 1 "$2" | sed "s|.*|$job,|" | tr -d '\n' | sed 's/,$//')]}" > "$3"
}
post_batch() { curl -sf -X POST "$BASE_URL/jobs/batch" -H 'Content-Type: application/json' --data-binary "@$1" >/dev/null; }

enqueue_backlog() { # enqueue_backlog queue total   (1000 per request, the API maximum)
  make_body "$1" 1000 "$OUT/batch.json"
  for _ in $(seq 1 $(($2 / 1000))); do post_batch "$OUT/batch.json"; done
}

wait_drain() { # wait_drain queue
  local started=$SECONDS
  while [ "$(not_finished "$1")" -gt 0 ]; do
    (( SECONDS - started < 900 )) || { log "timeout waiting for $1 to drain"; exit 2; }
    sleep 1
  done
}

start_workers() { # start_workers n queue
  export JOBQUEUE_WORKER_QUEUES="$2"
  docker compose stop prometheus grafana >/dev/null 2>&1 || true   # no monitoring load during measurements
  docker compose up -d --scale worker="$1" server worker >"$OUT/compose-up-$1.log" 2>&1
  for _ in $(seq 1 80); do
    [ "$(docker compose ps --format '{{.Name}} {{.Health}}' | grep -cE '(server|worker).* healthy')" -ge $(($1 + 1)) ] && break
    sleep 3
  done
}

throughput_run() { # throughput_run queue total  -> sets TPUT, DRAIN_SECONDS
  local queue=$1 total=$2 since
  since=$(sql "SELECT now()")
  docker compose pause worker >/dev/null 2>&1    # frozen: they cannot start draining while the backlog loads
  enqueue_backlog "$queue" "$total"
  docker compose unpause worker >/dev/null 2>&1  # from here, the workers drain the backlog
  wait_drain "$queue"
  IFS='|' read -r done_count DRAIN_SECONDS <<<"$(sql "
    SELECT count(*), extract(epoch FROM max(finished_at) - min(started_at))
    FROM jobs WHERE queue = '$queue' AND created_at >= '$since' AND status = 'SUCCEEDED'")"
  [ "$done_count" -eq "$total" ] || { log "expected $total SUCCEEDED jobs, found $done_count"; exit 3; }
  TPUT=$(awk -v n="$total" -v s="$DRAIN_SECONDS" 'BEGIN { printf "%.1f", n / s }')
  DRAIN_SECONDS=$(awk -v s="$DRAIN_SECONDS" 'BEGIN { printf "%.2f", s }')
}

latency_run() { # latency_run queue  -> sets LAT_N, LAT_P50, LAT_P95, LAT_P99 (seconds)
  local queue=$1 since total=$((LATENCY_RATE * LATENCY_SECONDS)) sent=0 start_ms elapsed due
  since=$(sql "SELECT now()")
  start_ms=$(date +%s%3N)
  while [ "$sent" -lt "$total" ]; do   # send at a steady rate: whatever is due by now goes out in one batch
    elapsed=$(( $(date +%s%3N) - start_ms ))
    due=$(( elapsed * LATENCY_RATE / 1000 - sent ))
    (( due > total - sent )) && due=$((total - sent))
    if [ "$due" -gt 0 ]; then
      make_body "$queue" "$due" "$OUT/latency.json"
      post_batch "$OUT/latency.json"
      sent=$((sent + due))
    fi
    sleep 0.05
  done
  wait_drain "$queue"
  IFS='|' read -r LAT_N LAT_P50 LAT_P95 LAT_P99 <<<"$(sql "
    SELECT count(*),
           round(percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM started_at - created_at))::numeric, 3),
           round(percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM started_at - created_at))::numeric, 3),
           round(percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM started_at - created_at))::numeric, 3)
    FROM jobs WHERE queue = '$queue' AND created_at >= '$since' AND status = 'SUCCEEDED'")"
}

median_of() { # median_of workers column   (median over the repetitions, column of results.csv)
  awk -F, -v n="$1" -v c="$2" 'NR > 1 && $1 == n { print $c }' "$CSV" | sort -n | sed -n "$(( (REPS + 1) / 2 ))p"
}

summary() {
  {
    echo "| Workers | Throughput per repetition (jobs/s) | Throughput, median | Latency p50 (s), median | p95 (s), median | p99 (s), median |"
    echo "|--------:|------------------------------------|-------------------:|------------------------:|----------------:|----------------:|"
    for n in $WORKERS_LIST; do
      reps=$(awk -F, -v n="$n" 'NR > 1 && $1 == n { printf "%s%s", sep, $4; sep = " / " }' "$CSV")
      echo "| $n | $reps | $(median_of "$n" 4) | $(median_of "$n" 7) | $(median_of "$n" 8) | $(median_of "$n" 9) |"
    done
  } | tee "$OUT/summary.md" >&2
}

# --- the measurement ---------------------------------------------------------------------------------------------
echo "workers,rep,jobs,throughput_jobs_per_s,drain_seconds,latency_jobs,latency_p50_s,latency_p95_s,latency_p99_s" > "$CSV"
docker compose up -d postgres >/dev/null 2>&1
for _ in $(seq 1 30); do sql "SELECT 1" >/dev/null 2>&1 && break; sleep 2; done
capture_env

for n in $WORKERS_LIST; do
  queue="bench-w$n"
  log "=== $n worker(s), queue $queue"
  # The same starting point for every configuration: no leftovers from earlier runs, vacuumed and analyzed tables.
  sql "DELETE FROM jobs WHERE queue LIKE 'bench-%'" >/dev/null
  sql "VACUUM (ANALYZE) jobs" >/dev/null
  sql "VACUUM (ANALYZE) job_attempts" >/dev/null
  start_workers "$n" "$queue"
  log "warm-up: $WARMUP_JOBS jobs (not counted: lets the JVMs and the database warm up)"
  throughput_run "$queue" "$WARMUP_JOBS"
  for rep in $(seq 1 "$REPS"); do
    throughput_run "$queue" "$JOBS"
    log "rep $rep throughput: $TPUT jobs/s ($JOBS jobs in ${DRAIN_SECONDS}s)"
    tput=$TPUT drain=$DRAIN_SECONDS
    latency_run "$queue"
    log "rep $rep latency at ${LATENCY_RATE} jobs/s (n=$LAT_N): p50 ${LAT_P50}s, p95 ${LAT_P95}s, p99 ${LAT_P99}s"
    echo "$n,$rep,$JOBS,$tput,$drain,$LAT_N,$LAT_P50,$LAT_P95,$LAT_P99" >> "$CSV"
  done
done

log "=== summary (also in $OUT/summary.md)"
summary
log "raw output: $OUT"
