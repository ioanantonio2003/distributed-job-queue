# Sends a batch of slow jobs, waits until they all succeed, and shows which workers ran them.
# Usage: .\scripts\demo.ps1 [-Count 12]      (needs: docker compose up --build --scale worker=3)
param(
    [int]$Count = 12,
    [string]$BaseUrl = "http://localhost:8080"
)
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

$dbUser = if ($env:POSTGRES_USER) { $env:POSTGRES_USER } else { "jobqueue" }
$dbName = if ($env:POSTGRES_DB) { $env:POSTGRES_DB } else { "jobqueue" }
function Invoke-Sql([string]$sql, [string[]]$extra = @()) {
    docker compose exec -T postgres psql -U $dbUser -d $dbName -At @extra -c $sql
}

# Each job sleeps 1 s, so with several workers the jobs visibly spread across them.
$job = @{ queue = "default"; type = "sleep"; payload = @{ millis = 1000 } }
$body = @{ jobs = @(1..$Count | ForEach-Object { $job }) } | ConvertTo-Json -Depth 5
$response = Invoke-RestMethod -Method Post -Uri "$BaseUrl/jobs/batch" -ContentType "application/json" -Body $body

$ids = ($response.ids | ForEach-Object { "'$_'" }) -join ","
Write-Host "Enqueued $Count jobs. Waiting for them to succeed..."

$done = 0
for ($i = 0; $i -lt 60; $i++) {
    $done = [int](Invoke-Sql "SELECT count(*) FROM jobs WHERE status = 'SUCCEEDED' AND id IN ($ids)")
    Write-Host "  SUCCEEDED: $done / $Count"
    if ($done -eq $Count) { break }
    Start-Sleep -Seconds 1
}

Write-Host ""
Write-Host "Jobs executed per worker:"
Invoke-Sql "SELECT worker_id, count(*) FROM job_attempts WHERE job_id IN ($ids) GROUP BY worker_id ORDER BY worker_id" @("-F", " : ")

if ($done -ne $Count) {
    Write-Error "Not all jobs finished within 60 s."
}
