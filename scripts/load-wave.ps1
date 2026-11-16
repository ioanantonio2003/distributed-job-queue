# Sends waves of jobs so the Grafana dashboard has something to show: the queue fills, workers drain it, and a few
# jobs fail on purpose. Usage: .\scripts\load-wave.ps1 [-Waves 6] [-Jobs 400] [-Pause 8] [-Millis 250]
# (needs: docker compose up --build --scale worker=3)
param(
    [int]$Waves = 6,
    [int]$Jobs = 400,      # per wave, at most 1000
    [int]$Pause = 8,       # seconds between waves
    [int]$Millis = 250,    # work per job
    [string]$BaseUrl = "http://localhost:8080"
)
$ErrorActionPreference = "Stop"

# $Jobs normal jobs, plus 10 that fail once and succeed on the retry, plus 3 that always fail and end up DEAD.
$normal = @{ queue = "default"; type = "sleep"; payload = @{ millis = $Millis } }
$flaky = @{ queue = "default"; type = "flaky"; payload = @{ failTimes = 1 } }
$broken = @{ queue = "default"; type = "always-fail"; payload = @{}; maxAttempts = 2 }
$batch = @(1..$Jobs | ForEach-Object { $normal }) + @(1..10 | ForEach-Object { $flaky }) + @(1..3 | ForEach-Object { $broken })
$body = @{ jobs = $batch } | ConvertTo-Json -Depth 6 -Compress

for ($wave = 1; $wave -le $Waves; $wave++) {
    Invoke-RestMethod -Method Post -Uri "$BaseUrl/jobs/batch" -ContentType "application/json" -Body $body | Out-Null
    Write-Host "wave $wave/$Waves sent ($($batch.Count) jobs)"
    if ($wave -lt $Waves) { Start-Sleep -Seconds $Pause }
}
Write-Host "done: watch http://localhost:3000"
