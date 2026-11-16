# ADR 004: No leader, `SKIP LOCKED` instead

Status: accepted

## Context
Some work must be done by exactly one party at a time: handing out jobs, and recovering the jobs of dead workers.
The usual way is to elect a leader that does it. Leader election needs its own protocol, and the leader is a single
point of failure until a new one is elected.

## Decision
No component is elected. Every worker and every server instance runs the same loops, and the database arbitrates
with `FOR UPDATE SKIP LOCKED`: a statement that finds a row already locked by another one skips it and takes the
next. So concurrent claimers receive different jobs, and concurrent reapers recover different jobs, without waiting
for each other.

- **Claim.** Each worker claims from each of its queues with one short statement, for as many jobs as that queue has
  free slots. The statement filters with `queue = :queue` (equality), not `queue = ANY(...)`: with equality,
  PostgreSQL can read the partial index `(queue, priority DESC, run_at) WHERE status = 'QUEUED'` in the order asked for
  and stop after `LIMIT` rows. With `ANY(array)` it read and sorted every runnable job first, so the cost of a claim
  grew with the size of the backlog. This was seen in the query plans with `EXPLAIN (ANALYZE)` on a large backlog;
  it was checked by hand and is not part of the published benchmark.
- **Reaper.** Every server instance runs a reaper. It picks expired leases in batches with `SKIP LOCKED`, so running
  several at once is safe (a test runs two against 300 expired jobs and checks each is recovered exactly once).

## Consequences
- Any number of workers and servers can run, and any of them can die, with no election and no failover gap.
- Per-queue claiming costs one small statement per queue with free slots per polling pass, instead of one statement
  for all queues. Each is cheap, and it also lets each queue have its own concurrency limit.
- `SKIP LOCKED` gives no fairness guarantee between workers; it spreads work, it does not schedule it.
- Recurring (cron) jobs would need the same treatment (one scheduler instance acting per due schedule). That feature
  is **not implemented**.

## Alternatives considered
- **A leader (for example through an advisory lock or ZooKeeper-style election):** more moving parts and a recovery
  window when the leader dies.
- **`SELECT ... FOR UPDATE` without `SKIP LOCKED`:** correct, but workers queue up behind each other on the same rows.
