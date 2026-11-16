# Chaos test with real processes: kills and freezes real worker containers while 10,000 jobs are processed,
# then checks from the database that nothing was lost or run twice to completion.
#
# Usage: .\scripts\chaos\chaos-test.ps1 [-Jobs 10000] [-Workers 3] [-JobMillis 150] [-ChaosSeconds 90]
#                                       [-LeaseSeconds 6] [-PauseSeconds 18] [-DrainTimeout 300] [-Seed 1234]
# (Bash version: chaos-test.sh)
param(
    [int]$Jobs = 10000,
    [int]$Workers = 3,
    [int]$JobMillis = 150,           # each job sleeps this long, so workers are usually busy when killed
    [int]$ChaosSeconds = 90,         # at most this long spent killing workers
    [int]$LeaseSeconds = 6,          # short lease: a dead worker's jobs come back within seconds
    [int]$PauseSeconds = 0,          # 0 = three times the lease; must be longer than the lease
    [int]$DrainTimeout = 300,        # seconds allowed for the queue to empty after the chaos
    [int]$Seed = (Get-Random -Maximum 32768),
    [string]$BaseUrl = "http://localhost:8080"
)
# Native commands such as docker write progress to stderr; do not treat that as a terminating error.
$ErrorActionPreference = "Continue"
Set-Location (Join-Path $PSScriptRoot "..\..")

if ($PauseSeconds -le 0) { $PauseSeconds = $LeaseSeconds * 3 }
$rng = [System.Random]::new($Seed)   # same seed = same kill schedule (not the same thread timing)
$runId = "chaos-" + (Get-Date -Format "yyyyMMdd-HHmmss")   # also the queue name: isolates this run's jobs
$out = "scripts/chaos/out/$runId"
New-Item -ItemType Directory -Force $out | Out-Null
$dbUser = if ($env:POSTGRES_USER) { $env:POSTGRES_USER } else { "jobqueue" }
$dbName = if ($env:POSTGRES_DB) { $env:POSTGRES_DB } else { "jobqueue" }

function Log([string]$message) {
    $line = "{0} {1}" -f (Get-Date -Format "HH:mm:ss"), $message
    Write-Host $line
    Add-Content -Path "$out/events.log" -Value $line
}
function Sql([string]$query) {
    $oneLine = $query -replace '\s+', ' '
    (docker compose exec -T postgres psql -U $dbUser -d $dbName -At -c $oneLine | Out-String).Trim()
}
function NotFinished { [int](Sql "SELECT count(*) FROM jobs WHERE queue = '$runId' AND status IN ('QUEUED', 'RUNNING')") }
# Jobs a container's worker currently holds: a worker id is the container hostname (the first 12 characters of
# the container id) plus a random suffix, and locked_by stores it.
function HeldBy([string]$containerId) {
    $prefix = $containerId.Substring(0, 12)
    [int](Sql "SELECT count(*) FROM jobs WHERE queue = '$runId' AND status = 'RUNNING' AND locked_by LIKE '$prefix-%'")
}

# Passed through docker-compose.yml to the containers: workers take jobs only from this run's queue,
# and a short lease and reaper interval make recoveries quick.
$env:JOBQUEUE_WORKER_QUEUES = $runId
$env:JOBQUEUE_WORKER_LEASE = "${LeaseSeconds}s"
$env:JOBQUEUE_REAPER_INTERVAL = "2s"

$containers = @()
$exitCode = 0
try {
    Log "run=$runId jobs=$Jobs workers=$Workers job_millis=$JobMillis lease=${LeaseSeconds}s seed=$Seed"

    # ---- 1. start the stack ---------------------------------------------------------------------------------
    cmd /c "docker compose up --build -d --scale worker=$Workers > `"$out/compose-up.log`" 2>&1"
    for ($i = 0; $i -lt 60; $i++) {
        $healthy = @(docker compose ps --format '{{.Health}}' | Where-Object { $_ -eq 'healthy' }).Count
        if ($healthy -ge ($Workers + 2)) { break }
        Start-Sleep -Seconds 3
    }
    $containers = @(docker compose ps -q worker)
    if ($containers.Count -ne $Workers) { Log "expected $Workers workers, found $($containers.Count)"; exit 2 }
    Log "stack up: $((docker compose ps --format '{{.Name}}') -join ' ')"

    # ---- 2. enqueue the jobs, 1000 per batch request (the API maximum) ---------------------------------------
    $clock = [System.Diagnostics.Stopwatch]::StartNew()
    $batch = 1000
    $job = @{ queue = $runId; type = "sleep"; payload = @{ millis = $JobMillis }; maxAttempts = 20 }
    $body = @{ jobs = @(1..$batch | ForEach-Object { $job }) } | ConvertTo-Json -Depth 6 -Compress
    for ($i = 0; $i -lt [int]($Jobs / $batch); $i++) {
        Invoke-RestMethod -Method Post -Uri "$BaseUrl/jobs/batch" -ContentType "application/json" -Body $body | Out-Null
    }
    Log "enqueued $(Sql "SELECT count(*) FROM jobs WHERE queue = '$runId'") jobs"

    # ---- 3. chaos: kill and restart workers at random times; once, freeze one past its lease -----------------
    $kills = 0
    $pauses = 0
    $pausedHeld = 0   # how many jobs the frozen worker was holding when it was frozen
    $chaosClock = [System.Diagnostics.Stopwatch]::StartNew()
    while ($chaosClock.Elapsed.TotalSeconds -lt $ChaosSeconds -and (NotFinished) -gt 0) {
        Start-Sleep -Seconds $rng.Next(3, 9)
        if ((NotFinished) -eq 0) { break }
        $victim = $containers[$rng.Next(0, $Workers)]
        $name = (docker inspect -f '{{.Name}}' $victim).TrimStart('/')

        if ($pauses -eq 0 -and $chaosClock.Elapsed.TotalSeconds -gt ($ChaosSeconds / 3)) {
            # A real zombie: the process is frozen (not dead), its heartbeat stops, its lease expires, the reaper
            # gives its jobs to others, and then it wakes up and tries to finish work that is no longer its own.
            # Freeze the worker that holds the most jobs, otherwise the zombie could have nothing to be a zombie about.
            $best = -1
            foreach ($id in $containers) {
                $held = HeldBy $id
                if ($held -gt $best) { $best = $held; $victim = $id }
            }
            $name = (docker inspect -f '{{.Name}}' $victim).TrimStart('/')
            $pausedHeld = $best
            Log "PAUSE  $name for ${PauseSeconds}s (lease is ${LeaseSeconds}s), holding $pausedHeld running job(s)"
            docker pause $victim | Out-Null
            Start-Sleep -Seconds $PauseSeconds
            docker unpause $victim | Out-Null
            Log "UNPAUSE $name"
            $pauses++
        }
        else {
            Log "KILL   $name   (not finished: $(NotFinished))"
            docker kill $victim | Out-Null
            $kills++
            Start-Sleep -Seconds $rng.Next(1, 4)
            docker start $victim | Out-Null
            Log "START  $name"
        }
    }

    # Whatever state the chaos left behind, make sure every worker is alive for the final stretch.
    foreach ($id in $containers) {
        docker unpause $id 2>$null | Out-Null
        docker start $id 2>$null | Out-Null
    }
    Log ("chaos over after {0:N0}s: {1} kill(s), {2} pause(s); waiting for the queue to drain" -f $chaosClock.Elapsed.TotalSeconds, $kills, $pauses)

    # ---- 4. wait for the queue to empty -------------------------------------------------------------------------
    $drainClock = [System.Diagnostics.Stopwatch]::StartNew()
    while ((NotFinished) -gt 0 -and $drainClock.Elapsed.TotalSeconds -lt $DrainTimeout) { Start-Sleep -Seconds 5 }
    $elapsed = [int]$clock.Elapsed.TotalSeconds

    # ---- 5. check the invariants in the database ----------------------------------------------------------------
    cmd /c "docker compose logs --no-color > `"$out/compose.log`" 2>&1"
    $row = (Sql @"
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
                 WHERE x.queue = '$runId' AND a.outcome = 'LEASE_EXPIRED')
        FROM jobs j WHERE queue = '$runId'
"@).Split('|') | ForEach-Object { [int]$_ }
    $total, $succeeded, $dead, $queued, $running, $recovered, $badSuccessRows, $badHistory, $expiredRows = $row
    $leaseLostLogs = @(Select-String -Path "$out/compose.log" -Pattern "lease lost" -SimpleMatch).Count

    $failures = 0
    function Check([string]$what, $actual, $expected) {
        if ($actual -eq $expected) { Log "OK    $what ($actual)" }
        else { Log "FAIL  ${what}: got $actual, expected $expected"; $script:failures++ }
    }
    Log "---- invariants (queue $runId)"
    Check "jobs in the database equal jobs sent" $total $Jobs
    Check "SUCCEEDED + DEAD equals jobs sent" ($succeeded + $dead) $Jobs
    Check "jobs still QUEUED" $queued 0
    Check "jobs still RUNNING" $running 0
    Check "jobs without exactly one SUCCEEDED history row (SUCCEEDED jobs) / any (others)" $badSuccessRows 0
    Check "jobs whose attempts differ from their history rows" $badHistory 0
    if ($pausedHeld -gt 0) {
        # The frozen worker held jobs that were given to others; on waking up it must notice (log) that it lost them.
        if ($leaseLostLogs -ge 1) {
            Log "OK    the frozen worker ($pausedHeld job(s) in hand) noticed it lost them ($leaseLostLogs log line(s))"
        }
        else {
            Log "FAIL  the frozen worker held $pausedHeld job(s) but no 'lease lost' line was logged"
            $failures++
        }
    }
    elseif ($pauses -gt 0) {
        Log "NOTE  the frozen worker held no jobs, so this run did not really exercise a zombie"
    }

    $summary = @(
        "run:                       $runId (seed $Seed)",
        "jobs sent:                 $Jobs    workers: $Workers    lease: ${LeaseSeconds}s",
        "workers killed:            $kills",
        "workers frozen (pause):    $pauses (for ${PauseSeconds}s), holding $pausedHeld job(s) when frozen",
        "SUCCEEDED / DEAD:          $succeeded / $dead",
        "jobs run more than once:   $recovered",
        "LEASE_EXPIRED recoveries:  $expiredRows",
        "'lease lost' log lines:    $leaseLostLogs (workers noticing they lost a job)",
        "total time:                ${elapsed}s",
        "invariant failures:        $failures"
    )
    $summary | Tee-Object -FilePath "$out/summary.txt"
    Log "raw output in $out"
    if ($failures -gt 0) { $exitCode = 1 }
}
finally {
    # Leave nothing frozen or dead behind, then stop the application containers (Postgres stays up).
    foreach ($id in $containers) { docker unpause $id 2>$null | Out-Null }
    docker compose stop server worker 2>$null | Out-Null
    docker compose rm -f server worker 2>$null | Out-Null
}
exit $exitCode
