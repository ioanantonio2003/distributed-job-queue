package dev.jobqueue.worker.completion;

import dev.jobqueue.common.AttemptOutcome;
import dev.jobqueue.common.JobStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/**
 * Records the end of a job attempt, successful or failed.
 *
 * <p>Invariant (fencing): only the worker that currently holds the lease can finish the job. The lease is
 * identified by (locked_by, attempts) while the status is RUNNING; a worker that lost the job, for example
 * because its lease expired and another worker claimed it, matches nothing and changes nothing.
 */
@Repository
public class JobCompletionRepository {

    private final JdbcClient jdbc;

    public JobCompletionRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Marks the job SUCCEEDED and writes its job_attempts row, in one statement.
     *
     * @return true if this worker still held the job and it is now SUCCEEDED; false if the lease was lost
     *         (nothing was changed and the caller must ignore its result)
     */
    public boolean complete(UUID jobId, String workerId, int attempt) {
        // One statement = one implicit transaction: the UPDATE and the INSERT both happen or neither does,
        // without an explicit @Transactional. The INSERT selects from "done", so it produces a row only when
        // the fenced UPDATE matched one. The affected-row count is therefore 1 on success and 0 when fenced out.
        int rows = jdbc.sql("""
                        WITH done AS (
                            UPDATE jobs
                            SET status = 'SUCCEEDED', finished_at = now(), locked_by = NULL, locked_until = NULL
                            -- The fencing check: same worker AND same attempt number AND still RUNNING.
                            WHERE id = :jobId AND locked_by = :workerId AND attempts = :attempt AND status = 'RUNNING'
                            RETURNING id, attempts, started_at
                        )
                        INSERT INTO job_attempts (job_id, attempt, worker_id, started_at, finished_at, outcome)
                        SELECT id, attempts, :workerId, started_at, now(), 'SUCCEEDED' FROM done
                        """)
                .param("jobId", jobId)
                .param("workerId", workerId)
                .param("attempt", attempt)
                .update();
        return rows == 1;
    }

    /**
     * Records that the attempt failed, in one statement and under the same fencing check as {@link #complete}.
     * The job goes to DEAD if the failure is permanent or its attempts are used up; otherwise back to QUEUED,
     * to be claimed again once {@code delay} has passed (run_at, on the database clock).
     * {@code outcome} is what job_attempts records: FAILED for an exception, TIMED_OUT for a time limit.
     *
     * @return the job's new status (QUEUED or DEAD), or empty if the lease was lost and nothing was changed
     */
    public Optional<JobStatus> fail(UUID jobId, String workerId, int attempt, String error,
            boolean permanent, Duration delay, AttemptOutcome outcome) {
        // "recorded" is a data-modifying CTE that nothing selects from: PostgreSQL still runs it exactly once and
        // to completion, so the job update and the history row are atomic. The final SELECT only reports the
        // new status; it returns no row when the fenced UPDATE matched nothing.
        return jdbc.sql("""
                        WITH failed AS (
                            UPDATE jobs
                            SET status = CASE WHEN (:permanent OR attempts >= max_attempts) THEN 'DEAD' ELSE 'QUEUED' END,
                                run_at = now() + :delaySeconds * interval '1 second',
                                last_error = :error,
                                locked_by = NULL,
                                locked_until = NULL,
                                -- Only a finished job has a finish time; a job waiting for a retry has not finished.
                                finished_at = CASE WHEN (:permanent OR attempts >= max_attempts) THEN now() END
                            -- The fencing check, identical to complete().
                            WHERE id = :jobId AND locked_by = :workerId AND attempts = :attempt AND status = 'RUNNING'
                            RETURNING id, attempts, started_at, status
                        ),
                        recorded AS (
                            INSERT INTO job_attempts (job_id, attempt, worker_id, started_at, finished_at, outcome, error)
                            SELECT id, attempts, :workerId, started_at, now(), :outcome, :error FROM failed
                        )
                        SELECT status FROM failed
                        """)
                .param("jobId", jobId)
                .param("workerId", workerId)
                .param("attempt", attempt)
                .param("error", error)
                .param("permanent", permanent)
                .param("delaySeconds", delay.toMillis() / 1000.0)
                .param("outcome", outcome.name())
                .query(String.class)
                .optional()
                .map(JobStatus::valueOf);
    }
}
