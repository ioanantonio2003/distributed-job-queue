package dev.jobqueue.server.reaper;

import dev.jobqueue.common.AttemptOutcome;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Takes back jobs whose worker stopped renewing the lease.
 *
 * <p>Invariant: a job is reaped by at most one reaper, and only while it is RUNNING with an expired lease.
 * {@code FOR UPDATE SKIP LOCKED} makes concurrent reapers (one per server instance) take different rows, so no
 * leader election is needed. A worker finishing the same job at the same moment cannot both win: whichever
 * locks the row first wins, and the other one's update matches nothing.
 */
@Repository
public class ReaperRepository {

    /** How many jobs went back to the queue and how many were declared DEAD. */
    public record ReapResult(int requeued, int dead) {

        public static final ReapResult NONE = new ReapResult(0, 0);

        public int total() {
            return requeued + dead;
        }

        public ReapResult plus(ReapResult other) {
            return new ReapResult(requeued + other.requeued, dead + other.dead);
        }
    }

    private final JdbcClient jdbc;

    public ReaperRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Recovers up to {@code batchSize} jobs with an expired lease, in one statement.
     */
    public ReapResult reap(int batchSize) {
        // "expired" picks and locks the rows and keeps their OLD values (locked_by, attempts, started_at):
        // "reaped" resets the jobs, "recorded" writes the history row from the old values. All three see the
        // same snapshot, so the history names the worker that actually lost the job. Crashes count as attempts
        // because attempts was incremented at claim time: a job that kills its worker every time runs out of
        // attempts and ends up DEAD instead of looping forever.
        List<String> statuses = jdbc.sql("""
                        WITH expired AS (
                            SELECT id, locked_by, attempts, max_attempts, started_at
                            FROM jobs
                            -- The database clock decides what is expired, and jobs_lease_idx covers this filter.
                            WHERE status = 'RUNNING' AND locked_until < now()
                            ORDER BY locked_until
                            LIMIT :batchSize
                            FOR UPDATE SKIP LOCKED
                        ),
                        reaped AS (
                            UPDATE jobs j
                            SET status = CASE WHEN e.attempts >= e.max_attempts THEN 'DEAD' ELSE 'QUEUED' END,
                                -- Back in the queue right away: a crash is not the fault of the job, so no backoff.
                                run_at = now(),
                                locked_by = NULL,
                                locked_until = NULL,
                                last_error = 'lease expired',
                                finished_at = CASE WHEN e.attempts >= e.max_attempts THEN now() END
                            FROM expired e
                            WHERE j.id = e.id
                            RETURNING j.status
                        ),
                        recorded AS (
                            INSERT INTO job_attempts (job_id, attempt, worker_id, started_at, finished_at, outcome, error)
                            SELECT id, attempts, locked_by, started_at, now(), :outcome, 'lease expired' FROM expired
                        )
                        SELECT status FROM reaped
                        """)
                .param("batchSize", batchSize)
                .param("outcome", AttemptOutcome.LEASE_EXPIRED.name())
                .query(String.class)
                .list();
        int dead = (int) statuses.stream().filter("DEAD"::equals).count();
        return new ReapResult(statuses.size() - dead, dead);
    }
}
