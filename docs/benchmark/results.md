# Benchmark results

Throughput and queue latency of the job queue with 1 and with 3 workers. These numbers describe **one laptop and
an untuned PostgreSQL**, not production capacity. Everything below was measured with
[`scripts/benchmark/run.sh`](../../scripts/benchmark/run.sh); nothing is estimated.

## What is measured

All figures are computed from the database after the fact (the source of truth), not from the load generator.

- **Throughput (jobs/s):** 20,000 jobs are loaded into the queue while the workers are frozen (`docker pause`), then the
  workers are released and drain the backlog. Throughput is `20000 / (last finished_at - first started_at)`.
  Loading and draining therefore do not overlap.
- **Latency (seconds):** jobs are sent at a steady 100 jobs/s for 30 seconds (3,000 jobs) to workers that are already
  running. Latency is `started_at - created_at` per job, reported as p50 / p95 / p99 (`percentile_cont`). Jobs sent in
  the same request share one `created_at`, so the resolution of the sending side is roughly one request interval.
- **Handler:** `noop` (does nothing), so the queue machinery is what is measured: claim, heartbeat, completion and
  the history row for each job. Real handlers add their own time.
- **Method:** workers are started, then 5,000 warm-up jobs are drained and discarded (JIT and caches), then
  **3 repetitions** of throughput followed by latency. Between configurations the benchmark tables are emptied and
  vacuumed. Medians are used for the summary because a single repetition can be disturbed by other activity on the
  machine; all individual values are listed below.

## Environment

| | |
|---|---|
| Date | 2026-11-16 |
| Code | commit `6400bc5` plus uncommitted changes (the working tree contained the benchmark script and the `noop` handler themselves) |
| Machine | laptop: 12th Gen Intel Core i7-1255U, 12 logical cores, 15.7 GB RAM |
| OS | Windows 11 (10.0.26200) with Docker Desktop 29.9.2 |
| Containers | 12 CPUs and 7.6 GB visible to Docker; PostgreSQL, server, workers and the load generator all on this one machine |
| PostgreSQL | 17.11, default configuration (`shared_buffers` 128 MB) |
| Java | OpenJDK 21.0.13 (workers and server in containers) |
| Worker settings | 4 concurrent handlers per queue (slots), poll interval 500 ms backing off to 5 s when idle, lease 30 s, heartbeat 10 s, default connection pool |
| Monitoring | Prometheus and Grafana stopped during the measurement |

The full environment printed by the run is in `environment.txt` next to the raw results (`scripts/benchmark/out/`,
not published).

## Results

20,000 jobs per throughput repetition, `noop` handler.

| Workers | Throughput per repetition (jobs/s) | Median |
|--------:|------------------------------------|-------:|
| 1 | 767.1 / 397.5 / 799.4 | **767.1** |
| 3 | 1686.4 / 1234.5 / 1667.4 | **1667.4** |

Latency at a steady 100 jobs/s (3,000 jobs per repetition), in seconds:

| Workers | Repetition | p50 | p95 | p99 |
|--------:|-----------:|----:|----:|----:|
| 1 | 1 | 1.414 | 3.600 | 3.930 |
| 1 | 2 | 1.826 | 3.834 | 4.005 |
| 1 | 3 | 1.485 | 3.734 | 4.000 |
| 1 | **median** | **1.485** | **3.734** | **4.000** |
| 3 | 1 | 0.911 | 1.871 | 1.924 |
| 3 | 2 | 0.687 | 1.966 | 2.802 |
| 3 | 3 | 0.738 | 1.772 | 1.952 |
| 3 | **median** | **0.738** | **1.871** | **1.952** |

## What the numbers say

- **More workers, more throughput:** the median with 3 workers is about 2.2 times the median with 1 worker
  (1667.4 / 767.1). That is below 3 times, which fits workers competing for one database on one machine that also
  runs the load generator; this was not investigated further.
- **The repetitions vary.** Repetition 2 was the slowest in both configurations (397.5 and 1234.5 jobs/s). The cause
  was not investigated (other activity on the machine, or PostgreSQL background work such as checkpoints or
  autovacuum, are possible). All three values are reported instead of discarding any.
- **Latency is dominated by polling, not by work.** With a `noop` handler, a job takes milliseconds to run, yet the
  median wait before it starts is 0.7 to 1.5 seconds and p99 is close to the 5 s maximum polling interval. This is what
  the design predicts: workers poll, and an idle worker waits longer after each empty poll (500 ms doubling up to
  5 s). It was **not** verified by a separate experiment that varies the polling settings. These latencies are a
  property of the default configuration, not a lower bound of the approach.

## How to reproduce

Requires Docker and Git Bash (or WSL). From the repository root:

```bash
scripts/benchmark/run.sh
```

It started its own containers, ran for about 10 minutes on the machine above, and wrote `results.csv`,
`environment.txt`, `summary.md` and a log to `scripts/benchmark/out/<run>/`. Parameters can be changed with
environment variables, for example:

```bash
WORKERS_LIST="1 2 4" REPS=3 JOBS=20000 LATENCY_RATE=100 LATENCY_SECONDS=30 scripts/benchmark/run.sh
```

Numbers will differ on other hardware; compare the relative behavior, not the absolute values.

## Not measured

Handlers that do real work, payload sizes other than an empty one, more than 3 workers, tuned PostgreSQL settings,
and different polling settings were not measured.
