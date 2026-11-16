#!/usr/bin/env bash
# Chaos test with real processes: kills and freezes real worker containers while 10,000 jobs are processed,
# then checks from the database that nothing was lost or run twice to completion.
#
# Usage: scripts/chaos/chaos-test.sh            (Git Bash or WSL; PowerShell version: chaos-test.ps1)
# Tunables (environment): JOBS, WORKERS, JOB_MILLIS, CHAOS_SECONDS, LEASE_SECONDS, PAUSE_SECONDS,
#                         DRAIN_TIMEOUT, SEED
set -euo pipefail
cd "$(dirname "$0")/../.."

JOBS="${JOBS:-10000}"
WORKERS="${WORKERS:-3}"
JOB_MILLIS="${JOB_MILLIS:-150}"           # each job sleeps this long, so workers are usually busy when killed
CHAOS_SECONDS="${CHAOS_SECONDS:-90}"      # at most this long spent killing workers
LEASE_SECONDS="${LEASE_SECONDS:-6}"       # short lease: a dead worker's jobs come back within seconds
PAUSE_SECONDS="${PAUSE_SECONDS:-$((LEASE_SECONDS * 3))}"   # must be longer than the lease
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-300}"     # seconds allowed for the queue to empty after the chaos
SEED="${SEED:-$RANDOM}"
RANDOM="$SEED"                            # same seed = same kill schedule (not the same thread timing)

BASE_URL="${BASE_URL:-http://localhost:8080}"
RUN_ID="chaos-$(date +%Y%m%d-%H%M%S)"     # also the queue name: this run's jobs are isolated from any others
OUT="scripts/chaos/out/$RUN_ID"
mkdir -p "$OUT"
EVENTS="$OUT/events.log"

log() { echo "$(date +%T) $*" | tee -a "$EVENTS"; }
sql() { docker compose exec -T postgres psql -U "${POSTGRES_USER:-jobqueue}" -d "${POSTGRES_DB:-jobqueue}" -At -c "$1"; }
not_finished() { sql "SELECT count(*) FROM jobs WHERE queue = '$RUN_ID' AND status IN ('QUEUED', 'RUNNING')"; }
# Jobs a container's worker currently holds: a worker id is the container hostname (the first 12 characters of
# the container id) plus a random suffix, and locked_by stores it.
held_by() { sql "SELECT count(*) FROM jobs WHERE queue = '$RUN_ID' AND status = 'RUNNING' AND locked_by LIKE '${1:0:12}-%'"; }

# Passed through docker-compose.yml to the containers: workers take jobs only from this run's queue,
# and a short lease and reaper interval make recoveries quick.
export JOBQUEUE_WORKER_QUEUES="$RUN_ID"
export JOBQUEUE_WORKER_LEASE="${LEASE_SECONDS}s"
export JOBQUEUE_REAPER_INTERVAL="2s"

cleanup() {
  # Leave nothing frozen or dead behind, then stop the application containers (Postgres stays up).
  for id in "${CONTAINERS[@]:-}"; do
    [ -n "$id" ] && { docker unpause "$id" >/dev/null 2>&1 || true; }
  done
  docker compose stop server worker >/dev/null 2>&1 || true
  docker compose rm -f server worker >/dev/null 2>&1 || true
}
CONTAINERS=()
trap cleanup EXIT

log "run=$RUN_ID jobs=$JOBS workers=$WORKERS job_millis=$JOB_MILLIS lease=${LEASE_SECONDS}s seed=$SEED"

# ---- 1. start the stack -------------------------------------------------------------------------------------
docker compose up --build -d --scale worker="$WORKERS" >"$OUT/compose-up.log" 2>&1
for _ in $(seq 1 60); do
  [ "$(docker compose ps --format '{{.Health}}' | grep -c healthy)" -ge $((WORKERS + 2)) ] && break
  sleep 3
done
mapfile -t CONTAINERS < <(docker compose ps -q worker)
[ "${#CONTAINERS[@]}" -eq "$WORKERS" ] || { log "expected $WORKERS workers, found ${#CONTAINERS[@]}"; exit 2; }
log "stack up: $(docker compose ps --format '{{.Name}}' | paste -sd' ' -)"

# ---- 2. enqueue the jobs, 1000 per batch request (the API maximum) -------------------------------------------
STARTED_AT=$SECONDS
BATCH=1000
job="{\"queue\":\"$RUN_ID\",\"type\":\"sleep\",\"payload\":{\"millis\":$JOB_MILLIS},\"maxAttempts\":20}"
for _ in $(seq 1 $((JOBS / BATCH))); do
  # seq | sed repeats the job BATCH times (yes | head would die of SIGPIPE under "set -o pipefail").
  body="{\"jobs\":[$(seq 1 "$BATCH" | sed "s|.*|$job,|" | tr -d '\n' | sed 's/,$//')]}"
  echo "$body" > "$OUT/batch.json"
  curl -sf -X POST "$BASE_URL/jobs/batch" -H 'Content-Type: application/json' --data-binary "@$OUT/batch.json" >/dev/null
done
rm -f "$OUT/batch.json"
log "enqueued $(sql "SELECT count(*) FROM jobs WHERE queue = '$RUN_ID'") jobs"

# ---- 3. chaos: kill and restart workers at random times; once, freeze one past its lease -----------------------
kills=0
pauses=0
paused_held=0   # how many jobs the frozen worker was holding when it was frozen
chaos_start=$SECONDS
while (( SECONDS - chaos_start < CHAOS_SECONDS )) && [ "$(not_finished)" -gt 0 ]; do
  sleep $((3 + RANDOM % 6))
  [ "$(not_finished)" -gt 0 ] || break
  victim="${CONTAINERS[$((RANDOM % WORKERS))]}"
  name="$(docker inspect -f '{{.Name}}' "$victim" | tr -d '/')"

  if (( pauses == 0 && SECONDS - chaos_start > CHAOS_SECONDS / 3 )); then
    # A real zombie: the process is frozen (not dead), its heartbeat stops, its lease expires, the reaper gives
    # its jobs to others, and then it wakes up and tries to finish work that is no longer its own.
    # Freeze the worker that holds the most jobs, otherwise the zombie could have nothing to be a zombie about.
    best=-1
    for id in "${CONTAINERS[@]}"; do
      held=$(held_by "$id")
      if (( held > best )); then best=$held; victim=$id; fi
    done
    name="$(docker inspect -f '{{.Name}}' "$victim" | tr -d '/')"
    paused_held=$best
    log "PAUSE  $name for ${PAUSE_SECONDS}s (lease is ${LEASE_SECONDS}s), holding $paused_held running job(s)"
    docker pause "$victim" >/dev/null
    sleep "$PAUSE_SECONDS"
    docker unpause "$victim" >/dev/null
    log "UNPAUSE $name"
    pauses=$((pauses + 1))
  else
    log "KILL   $name   (not finished: $(not_finished))"
    docker kill "$victim" >/dev/null
    kills=$((kills + 1))
    sleep $((1 + RANDOM % 3))
    docker start "$victim" >/dev/null
    log "START  $name"
  fi
done

# Whatever state the chaos left behind, make sure every worker is alive for the final stretch.
for id in "${CONTAINERS[@]}"; do
  docker unpause "$id" >/dev/null 2>&1 || true
  docker start "$id" >/dev/null 2>&1 || true
done
log "chaos over after $((SECONDS - chaos_start))s: $kills kill(s), $pauses pause(s); waiting for the queue to drain"

# ---- 4. wait for the queue to empty ---------------------------------------------------------------------------
drain_start=$SECONDS
while [ "$(not_finished)" -gt 0 ] && (( SECONDS - drain_start < DRAIN_TIMEOUT )); do
  sleep 5
done
ELAPSED=$((SECONDS - STARTED_AT))

# ---- 5. check the invariants in the database ------------------------------------------------------------------
docker compose logs --no-color >"$OUT/compose.log" 2>&1 || true
IFS='|' read -r total succeeded dead queued running recovered bad_success_rows bad_history expired_rows <<<"$(sql "
  SELECT count(*),
         count(*) FILTER (WHERE status = 'SUCCEEDED'),
         count(*) FILTER (WHERE status = 'DEAD'),
         count(*) FILTER (WHERE status = 'QUEUED'),
         count(*) FILTER (WHERE status = 'RUNNING'),
         count(*) FILTER (WHERE attempts > 1),
         count(*) FILTER (WHERE (SELECT count(*) FROM job_attempts a WHERE a.job_id = j.id AND a.outcome = 'SUCCEEDED')
                                <> CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END),
         count(*) FILTER (WHERE attempts <> (SELECT count(*) FROM job_attempts a WHERE a.job_id = j.id)),
         (SELECT count(*) FROM job_attempts a JOIN jobs x ON x.id = a.job_id
           WHERE x.queue = '$RUN_ID' AND a.outcome = 'LEASE_EXPIRED')
  FROM jobs j WHERE queue = '$RUN_ID'" | tr -d '[:space:]')"
lease_lost_logs=$(grep -ci "lease lost" "$OUT/compose.log" || true)

failures=0
check() { # check "description" "actual" "expected"
  if [ "$2" = "$3" ]; then log "OK    $1 ($2)"; else log "FAIL  $1: got $2, expected $3"; failures=$((failures + 1)); fi
}
log "---- invariants (queue $RUN_ID)"
check "jobs in the database equal jobs sent" "$total" "$JOBS"
check "SUCCEEDED + DEAD equals jobs sent" "$((succeeded + dead))" "$JOBS"
check "jobs still QUEUED" "$queued" "0"
check "jobs still RUNNING" "$running" "0"
check "jobs without exactly one SUCCEEDED history row (SUCCEEDED jobs) / any (others)" "$bad_success_rows" "0"
check "jobs whose attempts differ from their history rows" "$bad_history" "0"
if (( paused_held > 0 )); then
  # The frozen worker held jobs that were given to others; on waking up it must notice (log) that it lost them.
  if (( lease_lost_logs >= 1 )); then
    log "OK    the frozen worker ($paused_held job(s) in hand) noticed it lost them ($lease_lost_logs log line(s))"
  else
    log "FAIL  the frozen worker held $paused_held job(s) but no 'lease lost' line was logged"
    failures=$((failures + 1))
  fi
elif (( pauses > 0 )); then
  log "NOTE  the frozen worker held no jobs, so this run did not really exercise a zombie"
fi

{
  echo "run:                       $RUN_ID (seed $SEED)"
  echo "jobs sent:                 $JOBS    workers: $WORKERS    lease: ${LEASE_SECONDS}s"
  echo "workers killed:            $kills"
  echo "workers frozen (pause):    $pauses (for ${PAUSE_SECONDS}s), holding $paused_held job(s) when frozen"
  echo "SUCCEEDED / DEAD:          $succeeded / $dead"
  echo "jobs run more than once:   $recovered"
  echo "LEASE_EXPIRED recoveries:  $expired_rows"
  echo "'lease lost' log lines:    $lease_lost_logs (workers noticing they lost a job)"
  echo "total time:                ${ELAPSED}s"
  echo "invariant failures:        $failures"
} | tee "$OUT/summary.txt"
log "raw output in $OUT"

[ "$failures" -eq 0 ] || exit 1
