# ADR 002: At-least-once delivery with idempotent handlers

Status: accepted

## Context
A job can be interrupted at any moment: the worker's process dies, the machine loses power, the network drops, a
deploy stops the process. After that, the system cannot know whether the handler's side effects (an e-mail sent, a
payment captured) happened before the interruption. Delivering a job *exactly once* would require the queue and the
handler's side effects to commit atomically, which is impossible for effects outside the database.

## Decision
Deliver **at least once**. A job whose worker disappears is given to another worker and run again. The system never
claims exactly-once delivery. Handlers must be idempotent: running them twice must be safe.

Two tools help handlers cope:
- `JobContext` tells a handler which `attempt` it is and passes the job's `idempotencyKey`, so it can pass a stable
  key to the downstream service.
- Enqueue itself is idempotent: a request with the same `(queue, idempotencyKey)` returns the job that already
  exists instead of creating a second one. The rule is enforced by a unique index in the database, not by a check in
  application code.

## Consequences
- A job interrupted by a crash or a lost lease runs more than once. This is visible in the data: the attempt history
  (`job_attempts`) shows every attempt, including those that ended with `LEASE_EXPIRED`.
- What *is* guaranteed, and tested: a job ends as `SUCCEEDED` by exactly one attempt, no job is lost, and a result
  written by a worker that no longer owns the job is rejected (see [ADR 003](003-lease-heartbeat-and-fencing.md)).
  The [chaos test](../../scripts/chaos/chaos-test.sh) checks these properties from the database after killing and
  freezing real worker processes.
- Fencing stops a stale worker from *writing to the queue*. It cannot undo effects the handler already had outside
  the queue; that is the handler's idempotency to deal with.
- A job that crashes its worker every time consumes an attempt per crash and ends up `DEAD`, instead of looping
  forever.

## Alternatives considered
- **At-most-once** (never retry after a crash): loses work silently; unacceptable for most uses.
- **Exactly-once:** not achievable end to end without distributed transactions with the handler's side effects.
  Claiming it would be dishonest.
