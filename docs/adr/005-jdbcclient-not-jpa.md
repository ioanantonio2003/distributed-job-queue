# ADR 005: `JdbcClient` with explicit SQL, not JPA, on the critical path

Status: accepted

## Context
The correctness of the queue lives in a handful of SQL statements: the claim (`UPDATE ... WHERE id IN (SELECT ...
FOR UPDATE SKIP LOCKED) RETURNING`), the fenced completion and failure writes that update a job and insert a history
row in one atomic statement (a CTE), the batched heartbeat (`UPDATE ... FROM unnest(...)`), and the reaper. An ORM
such as JPA/Hibernate is designed around entities and a persistence context; it hides the statements, may flush them
at moments of its own choosing and has no natural way to say `SKIP LOCKED`, `RETURNING` or a data-modifying CTE.

## Decision
Use Spring's `JdbcClient` with hand-written SQL, in `*Repository` classes, for everything that touches the
`jobs` table. SQL lives in Java text blocks, with comments explaining *why* each clause is there. Rows are mapped
by hand into immutable `record`s. There is no JPA, no Lombok and no query generated from method names.

## Consequences
- **Every statement is visible and reviewable**, including the exact fencing predicate on each write. That is the
  point: an interviewer, a reviewer or a future maintainer can read what the database is asked to do.
- **Atomicity is a property of one statement**, not of a transaction boundary managed elsewhere; most paths need no
  `@Transactional` at all. A handler never runs inside a transaction and never holds a connection.
- It is more verbose: mapping and parameter binding are written out, and the columns have to be kept in step across
  the migration, the record and the mapper by hand.
- Schema changes go through Flyway migrations, which are immutable once applied (a change is a new migration).
- The tests run against a real PostgreSQL (Testcontainers), never an in-memory substitute, because the behavior that
  matters (`SKIP LOCKED`, partial indexes, `jsonb`) is PostgreSQL's.

## Alternatives considered
- **JPA/Hibernate:** convenient for ordinary CRUD, but would hide or fight the statements that make the queue correct.
- **jOOQ or another SQL builder:** a reasonable choice; rejected here to keep the SQL as plain text that can be read
  and run directly in `psql`.
