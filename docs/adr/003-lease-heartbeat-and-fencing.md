# ADR 003: Leases, heartbeats and fencing tokens

Status: accepted

## Context
A worker that takes a job must be able to die without blocking the job forever, and a worker that is slow (a long
garbage-collection pause, a frozen container) must not be able to overwrite the work of the worker that replaced it.
A plain lock solves neither: a dead owner never releases it, and a revived one still believes it holds it.

## Decision
- **Lease.** Claiming a job sets `locked_until = now() + lease` (30 s by default). The time is the database's clock,
  never an application clock, so machines with drifting clocks cannot disagree.
- **Heartbeat.** While a job runs, its worker extends the lease of all its running jobs in one statement
  (`UPDATE ... FROM unnest(...)`), every lease/3. The application refuses to start unless lease is at least
  3 times the heartbeat interval, so one or two late heartbeats do not cost a healthy worker its jobs.
- **Reaper.** A job whose lease expired is taken back by a periodic reaper (a `QUEUED` job again, or `DEAD` if its
  attempts are used up) and the expiry is recorded in the attempt history.
- **Fencing token.** Every write a worker makes to a job (complete, fail, heartbeat, release) is conditional:
  `WHERE id = :id AND locked_by = :worker AND attempts = :attempt AND status = 'RUNNING'`. `attempts` is
  incremented when the job is claimed, so (`locked_by`, `attempts`) identifies one specific lease. If the statement
  changes zero rows, the worker has lost the job: it logs that and ignores its own result.
- **Crashes count as attempts.** Because `attempts` goes up at claim time, a job that kills its worker every time
  eventually reaches `max_attempts` and becomes `DEAD`. The exception is a graceful shutdown, which gives the job
  back and takes the attempt back with it: the worker left because it was asked to, not because the job failed.

## Consequences
- A worker that stops heartbeating (killed, frozen, partitioned) loses its jobs after at most one lease, and anything
  it later tries to write is rejected. Tests: a zombie that wakes up after its job was reassigned and finished
  ([`ReliabilityScenariosIntegrationTest`](../../jobqueue-server/src/test/java/dev/jobqueue/server/reliability/ReliabilityScenariosIntegrationTest.java)),
  fencing at the SQL level
  ([`JobCompletionRepositoryIntegrationTest`](../../jobqueue-worker/src/test/java/dev/jobqueue/worker/completion/JobCompletionRepositoryIntegrationTest.java)),
  lease renewal and loss
  ([`JobHeartbeatIntegrationTest`](../../jobqueue-worker/src/test/java/dev/jobqueue/worker/heartbeat/JobHeartbeatIntegrationTest.java)),
  and the real-process [chaos test](../../scripts/chaos/chaos-test.sh).
- Recovery of a dead worker's jobs takes up to one lease plus one reaper interval. Shorter leases recover faster and
  tolerate less heartbeat delay.
- A worker cut off from the database for longer than the lease does not stop on its own; fencing only keeps it from
  writing, and the reaper recovers its jobs.
- Job timeouts interrupt the handler thread, which only works for code that responds to interruption. A handler that
  ignores it keeps running in the background, though its slot is freed and its result is fenced out.

## Alternatives considered
- **Advisory locks or `SELECT ... FOR UPDATE` held while the handler runs:** would pin a database connection and a
  transaction for the whole duration of a job, which is slow and fragile.
- **Heartbeat-only liveness with no fencing token:** a revived worker could still complete a job someone else now owns.
