# ADR 001: PostgreSQL as the only broker

Status: accepted

## Context
A job queue needs a place to store jobs and a way for several workers to take them without taking the same job
twice. The usual answer is a dedicated broker (Redis, RabbitMQ, Kafka). Each one is another system to run, secure,
monitor and keep consistent with the database that already holds the application's data.

## Decision
Use PostgreSQL as the only broker. Jobs are rows in a `jobs` table. Workers take them with
`UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED) RETURNING ...`, which atomically picks and marks a batch
of runnable jobs while skipping rows another worker has already locked.

## Consequences
- **One system to operate.** The jobs, their history and the state of the queue live in one place, are backed up
  together and can be inspected with plain SQL. The schema is managed by Flyway migrations.
- **Strong guarantees come cheap.** Atomic state changes, unique indexes (idempotent enqueue) and transactions are
  features of the database, not things to rebuild on top of a message protocol.
- **Throughput is bounded by what one PostgreSQL can do**, and every job costs several writes (claim, heartbeat,
  completion, history row). Measured numbers are in [the benchmark](../benchmark/results.md); they are for one
  small machine and an untuned database, not a ceiling.
- **Workers poll.** There is no push from the database, so a job waits, on average, about half a polling interval
  before it is noticed (poll interval 500 ms, backing off to 5 s when idle). `LISTEN/NOTIFY` could shorten this
  and is listed under future work in the README; it is not implemented.
- **Finished jobs accumulate.** There is no retention job yet, so the table grows until someone deletes old rows.
  The claim path is protected by partial indexes that only contain `QUEUED` rows, so a large table of finished
  jobs does not slow claiming down.

## Alternatives considered
- **Redis or RabbitMQ:** faster per message and push-based, but a second system with its own failure modes and no
  shared transaction with the data. Not needed at the scale this project targets.
- **Kafka:** built for ordered event logs, not for per-job retries, delays and dead-lettering.
